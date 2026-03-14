//! Tests filling coverage gaps identified during the test-coverage audit.
//!
//! Organised by fix / area:
//! - Fix 1 (monotonic cursor): full `apply_single_event` path + DB read-back
//! - Fix 2 (proof race): happy-path token issuance regression + feed-deleted 404
//! - Fix 3 (search keyset): malformed cursor and NaN/Inf rank
//! - Proof module: `recompute_binding` edge cases, `prune_expired` atomicity
//! - Verifiers: trait-level pass/fail through `IngestContext`
//! - Community push: duplicate event, bad-signature event

#![expect(clippy::significant_drop_tightening, reason = "MutexGuard<Connection> must be held for the full scope in test setup")]

mod common;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use rusqlite::params;

// ============================================================================
// Fix 1: Monotonic cursor through full `apply_single_event` call
// ============================================================================

/// Build a minimal `ArtistUpserted` event for testing `apply_single_event`.
fn make_artist_event(
    event_id: &str,
    artist_id: &str,
    seq: i64,
    now: i64,
) -> stophammer::event::Event {
    use stophammer::event::{ArtistUpsertedPayload, Event, EventPayload, EventType};
    use stophammer::model::Artist;

    let artist = Artist {
        artist_id:  artist_id.into(),
        name:       format!("Artist {artist_id}"),
        name_lower: format!("artist {artist_id}"),
        sort_name:  None,
        type_id:    None,
        area:       None,
        img_url:    None,
        url:        None,
        begin_year: None,
        end_year:   None,
        created_at: now,
        updated_at: now,
    };
    let inner = ArtistUpsertedPayload { artist: artist.clone() };
    let payload_json = serde_json::to_string(&inner).expect("serialize");

    Event {
        event_id:     event_id.into(),
        event_type:   EventType::ArtistUpserted,
        payload:      EventPayload::ArtistUpserted(ArtistUpsertedPayload { artist }),
        subject_guid: artist_id.into(),
        signed_by:    "deadbeef".into(),
        signature:    "cafebabe".into(),
        seq,
        created_at:   now,
        warnings:     vec![],
        payload_json,
    }
}

// ---------------------------------------------------------------------------
// 1. Monotonic cursor maintained through `apply_single_event` (not just DB fn)
// ---------------------------------------------------------------------------

#[test]
fn cursor_monotonic_through_apply_single_event() {
    use stophammer::apply::{apply_single_event, ApplyOutcome};

    let db = common::test_db_arc();
    let now = common::now();

    // Apply event with seq=20.
    let ev20 = make_artist_event("mono-full-evt-20", "mono-full-art-1", 20, now);
    let r1 = apply_single_event(&db, "mono-full-node", &ev20).expect("apply seq=20");
    assert!(matches!(r1, ApplyOutcome::Applied(_)));

    // Apply event with seq=10 (lower — should NOT regress cursor).
    let ev10 = make_artist_event("mono-full-evt-10", "mono-full-art-2", 10, now);
    let r2 = apply_single_event(&db, "mono-full-node", &ev10).expect("apply seq=10");
    assert!(matches!(r2, ApplyOutcome::Applied(_)));

    // Read cursor from DB — must still be 20.
    let cursor: i64 = {
        let conn = db.lock().expect("lock");
        conn.query_row(
            "SELECT last_seq FROM node_sync_state WHERE node_pubkey = 'mono-full-node'",
            [],
            |r| r.get(0),
        )
        .expect("query cursor")
    };
    assert_eq!(
        cursor, 20,
        "cursor must not regress from 20 to 10 through apply_single_event"
    );

    // Apply event with seq=25 (higher — cursor must advance).
    let ev25 = make_artist_event("mono-full-evt-25", "mono-full-art-3", 25, now);
    let r3 = apply_single_event(&db, "mono-full-node", &ev25).expect("apply seq=25");
    assert!(matches!(r3, ApplyOutcome::Applied(_)));

    let cursor2: i64 = {
        let conn = db.lock().expect("lock");
        conn.query_row(
            "SELECT last_seq FROM node_sync_state WHERE node_pubkey = 'mono-full-node'",
            [],
            |r| r.get(0),
        )
        .expect("query cursor after advance")
    };
    assert_eq!(
        cursor2, 25,
        "cursor must advance from 20 to 25"
    );
}

// ---------------------------------------------------------------------------
// 2. Cursor survives simulated restart (read-back via get_node_sync_cursor)
// ---------------------------------------------------------------------------

#[test]
fn cursor_survives_simulated_restart() {
    use stophammer::apply::{apply_single_event, ApplyOutcome};

    let db = common::test_db_arc();
    let now = common::now();

    // Apply several events.
    let ev1 = make_artist_event("restart-evt-1", "restart-art-1", 5, now);
    let ev2 = make_artist_event("restart-evt-2", "restart-art-2", 10, now);
    let ev3 = make_artist_event("restart-evt-3", "restart-art-3", 15, now);

    for ev in [&ev1, &ev2, &ev3] {
        let r = apply_single_event(&db, "restart-node", ev).expect("apply");
        assert!(matches!(r, ApplyOutcome::Applied(_)));
    }

    // Simulate restart: read cursor via the production function.
    let cursor = {
        let conn = db.lock().expect("lock");
        stophammer::db::get_node_sync_cursor(&conn, "restart-node")
            .expect("get cursor")
    };
    assert_eq!(cursor, 15, "cursor should be 15 after applying seq 5, 10, 15");

    // Apply a seq=12 event (out-of-order, lower than persisted cursor).
    let ev4 = make_artist_event("restart-evt-4", "restart-art-4", 12, now);
    let r4 = apply_single_event(&db, "restart-node", &ev4).expect("apply");
    assert!(matches!(r4, ApplyOutcome::Applied(_)));

    // Re-read cursor — must still be 15.
    let cursor2 = {
        let conn = db.lock().expect("lock");
        stophammer::db::get_node_sync_cursor(&conn, "restart-node")
            .expect("get cursor after out-of-order")
    };
    assert_eq!(
        cursor2, 15,
        "cursor must remain 15 after applying out-of-order seq=12"
    );
}

// ============================================================================
// Fix 2: Proof race — happy-path regression: URL unchanged -> token IS issued
// ============================================================================

// ---------------------------------------------------------------------------
// 3. Happy-path: assert issues token and it validates (regression guard)
// ---------------------------------------------------------------------------

#[test]
fn proof_happy_path_token_issued_and_valid() {
    let conn = common::test_db();
    let nonce = "happy-nonce-12345";

    // Create challenge.
    let (challenge_id, _token_binding) =
        stophammer::proof::create_challenge(&conn, "feed-happy", "feed:write", nonce)
            .unwrap();

    // Resolve as valid (simulating successful RSS verification).
    let rows = stophammer::proof::resolve_challenge(&conn, &challenge_id, "valid")
        .unwrap();
    assert_eq!(rows, 1, "challenge should transition to valid");

    // Issue token.
    let token = stophammer::proof::issue_token(&conn, "feed:write", "feed-happy")
        .unwrap();
    assert!(!token.is_empty(), "token should be non-empty");

    // Validate token — must succeed with correct scope and feed.
    let result = stophammer::proof::validate_token(&conn, &token, "feed:write")
        .unwrap();
    assert_eq!(
        result,
        Some("feed-happy".to_string()),
        "token must validate and return the correct feed_guid"
    );
}

// ---------------------------------------------------------------------------
// 4. Assert with feed deleted between phases -> 404 (unit-level simulation)
// ---------------------------------------------------------------------------

#[test]
fn proof_feed_deleted_between_phases_returns_none() {
    let conn = common::test_db();

    // Create a feed, then delete it (simulating deletion between phases).
    let now = common::now();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES ('art-del', 'Del Artist', 'del artist', ?1, ?2)",
        params![now, now],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES ('Del Artist', ?1)",
        params![now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, 'art-del', 0, 'Del Artist', '')",
        params![credit_id],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, \
         explicit, episode_count, created_at, updated_at) \
         VALUES ('feed-del', 'https://example.com/del.xml', 'Del Feed', 'del feed', ?1, 0, 0, ?2, ?3)",
        params![credit_id, now, now],
    )
    .unwrap();

    // Create challenge for this feed.
    let (_cid, _binding) =
        stophammer::proof::create_challenge(&conn, "feed-del", "feed:write", "del-nonce-12345")
            .unwrap();

    // Simulate feed deletion between phase 1 and phase 3.
    conn.execute("DELETE FROM feeds WHERE feed_guid = 'feed-del'", [])
        .unwrap();

    // Phase 3 would call get_feed_by_guid and get None.
    let feed = stophammer::db::get_feed_by_guid(&conn, "feed-del").unwrap();
    assert!(
        feed.is_none(),
        "feed should be gone after deletion — would produce 404 in the handler"
    );
}

// ============================================================================
// Fix 3: Search keyset cursor — malformed and NaN/Inf inputs
// ============================================================================

fn test_search_app_state() -> Arc<stophammer::api::AppState> {
    let db = common::test_db_arc();

    // Insert a searchable entity so non-cursor queries work.
    {
        let conn = db.lock().unwrap();
        stophammer::search::populate_search_index(
            &conn, "feed", "f-cursor-test", "podcast cursor test", "", "", "",
        )
        .unwrap();
    }

    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create("/tmp/test-coverage-gap-signer.key")
            .unwrap(),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db,
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex: pubkey,
        admin_token: "test-admin-token".into(),
        sync_token: None,
        push_client: reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry: Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

// ---------------------------------------------------------------------------
// 5. Malformed cursor (not valid base64) returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_malformed_cursor_returns_400() {
    use http::Request;
    use tower::ServiceExt;

    let state = test_search_app_state();
    let app = stophammer::api::build_router(state);

    // Send a search request with an invalid base64 cursor.
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/search?q=podcast&cursor=!!!not-valid-base64!!!")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "malformed base64 cursor should return 400, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 6. NaN cursor rank returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_nan_cursor_rank_returns_400() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use http::Request;
    use tower::ServiceExt;

    let state = test_search_app_state();
    let app = stophammer::api::build_router(state);

    // Encode a cursor with NaN rank bits.
    let nan_bits = f64::NAN.to_bits();
    let cursor_value = format!("{nan_bits}\0123");
    let cursor_b64 = URL_SAFE_NO_PAD.encode(cursor_value.as_bytes());

    let uri = format!("/v1/search?q=podcast&cursor={cursor_b64}");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "NaN cursor rank should return 400, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 7. Inf cursor rank returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_inf_cursor_rank_returns_400() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use http::Request;
    use tower::ServiceExt;

    let state = test_search_app_state();
    let app = stophammer::api::build_router(state);

    // Encode a cursor with +Inf rank bits.
    let inf_bits = f64::INFINITY.to_bits();
    let cursor_value = format!("{inf_bits}\0456");
    let cursor_b64 = URL_SAFE_NO_PAD.encode(cursor_value.as_bytes());

    let uri = format!("/v1/search?q=podcast&cursor={cursor_b64}");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "Inf cursor rank should return 400, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 8. Cursor with invalid format (missing separator) returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_cursor_missing_separator_returns_400() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use http::Request;
    use tower::ServiceExt;

    let state = test_search_app_state();
    let app = stophammer::api::build_router(state);

    // Encode a cursor with no NUL separator.
    let cursor_value = "justanumber";
    let cursor_b64 = URL_SAFE_NO_PAD.encode(cursor_value.as_bytes());

    let uri = format!("/v1/search?q=podcast&cursor={cursor_b64}");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        400,
        "cursor with no separator should return 400, got {}",
        resp.status()
    );
}

// ============================================================================
// Proof module: recompute_binding edge cases
// ============================================================================

// ---------------------------------------------------------------------------
// 9. recompute_binding with valid input returns correct binding
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_valid_input() {
    // Create a challenge to get a real token_binding.
    let conn = common::test_db();
    let nonce = "recompute-nonce-1";
    let (_cid, token_binding) =
        stophammer::proof::create_challenge(&conn, "feed-rc", "feed:write", nonce)
            .unwrap();

    // Recompute with the same nonce should produce the same binding.
    let recomputed = stophammer::proof::recompute_binding(&token_binding, nonce);
    assert_eq!(
        recomputed.as_deref(),
        Some(token_binding.as_str()),
        "recomputed binding must match original when nonce is correct"
    );
}

// ---------------------------------------------------------------------------
// 10. recompute_binding with wrong nonce produces different binding
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_wrong_nonce_differs() {
    let conn = common::test_db();
    let nonce = "recompute-nonce-2";
    let (_cid, token_binding) =
        stophammer::proof::create_challenge(&conn, "feed-rc2", "feed:write", nonce)
            .unwrap();

    let recomputed = stophammer::proof::recompute_binding(&token_binding, "wrong-nonce-val");
    assert!(recomputed.is_some(), "should still produce a binding");
    assert_ne!(
        recomputed.unwrap(),
        token_binding,
        "wrong nonce must produce different binding"
    );
}

// ---------------------------------------------------------------------------
// 11. recompute_binding with malformed input (no dot) returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_malformed_no_dot() {
    let result = stophammer::proof::recompute_binding("nodothere", "any-nonce");
    assert!(result.is_none(), "malformed binding with no dot should return None");
}

// ---------------------------------------------------------------------------
// 12. recompute_binding with empty token part returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_empty_token_part() {
    let result = stophammer::proof::recompute_binding(".hashpart", "any-nonce");
    assert!(result.is_none(), "empty token part should return None");
}

// ---------------------------------------------------------------------------
// 13. recompute_binding with empty hash part returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_empty_hash_part() {
    let result = stophammer::proof::recompute_binding("tokenpart.", "any-nonce");
    assert!(result.is_none(), "empty hash part should return None");
}

// ============================================================================
// Verifiers: trait-level pass/fail through IngestContext
// ============================================================================

/// Build an `IngestContext` for verifier tests.
const fn verifier_ctx<'a>(
    req: &'a stophammer::ingest::IngestFeedRequest,
    conn: &'a rusqlite::Connection,
) -> stophammer::verify::IngestContext<'a> {
    stophammer::verify::IngestContext {
        request: req,
        db: conn,
        existing: None,
    }
}

// ---------------------------------------------------------------------------
// 14. CrawlTokenVerifier pass and fail
// ---------------------------------------------------------------------------

#[test]
fn crawl_token_verifier_pass() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::crawl_token::CrawlTokenVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   "secret-token".into(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     None,
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = CrawlTokenVerifier { expected: "secret-token".into() };
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn crawl_token_verifier_fail() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::crawl_token::CrawlTokenVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   "wrong-token".into(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     None,
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = CrawlTokenVerifier { expected: "correct-token".into() };
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

// ---------------------------------------------------------------------------
// 15. ContentHashVerifier pass (new hash) and fail (same hash = no-change)
// ---------------------------------------------------------------------------

#[test]
fn content_hash_verifier_pass_new_hash() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::content_hash::ContentHashVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: "https://example.com/feed.xml".into(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  "newhash123".into(),
        feed_data:     None,
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = ContentHashVerifier;
    // No cached hash exists, so it should pass.
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn content_hash_verifier_fail_unchanged() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::content_hash::{ContentHashVerifier, NO_CHANGE_SENTINEL};

    let conn = common::test_db();
    let now = common::now();

    // Insert a cached hash.
    conn.execute(
        "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
         VALUES ('https://example.com/cached.xml', 'samehash', ?1)",
        params![now],
    )
    .unwrap();

    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: "https://example.com/cached.xml".into(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  "samehash".into(),
        feed_data:     None,
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = ContentHashVerifier;
    match v.verify(&ctx) {
        VerifyResult::Fail(msg) => {
            assert_eq!(msg, NO_CHANGE_SENTINEL, "should return NO_CHANGE sentinel");
        }
        other => panic!("expected Fail with NO_CHANGE, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 16. MediumMusicVerifier pass and fail
// ---------------------------------------------------------------------------

#[test]
fn medium_music_verifier_pass() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::medium_music::MediumMusicVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-1".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          Some("music".into()),
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = MediumMusicVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn medium_music_verifier_fail_podcast() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::medium_music::MediumMusicVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-2".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          Some("podcast".into()),
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = MediumMusicVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

#[test]
fn medium_music_verifier_fail_absent() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::medium_music::MediumMusicVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-3".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = MediumMusicVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

// ---------------------------------------------------------------------------
// 17. FeedGuidVerifier pass and fail
// ---------------------------------------------------------------------------

#[test]
fn feed_guid_verifier_pass() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::feed_guid::FeedGuidVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "917393e3-1b1e-5f2c-a927-9e29e2d26b32".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = FeedGuidVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn feed_guid_verifier_fail_bad_guid() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::feed_guid::FeedGuidVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "c9c7bad3-4712-514e-9ebd-d1e208fa1b76".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = FeedGuidVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

#[test]
fn feed_guid_verifier_fail_invalid_uuid() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::feed_guid::FeedGuidVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "not-a-valid-uuid".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = FeedGuidVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

// ---------------------------------------------------------------------------
// 18. V4VPaymentVerifier pass and fail
// ---------------------------------------------------------------------------

#[test]
fn v4v_payment_verifier_pass() {
    use stophammer::model::RouteType;
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::v4v_payment::V4VPaymentVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-v4v".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![stophammer::ingest::IngestPaymentRoute {
                recipient_name: Some("Artist".into()),
                route_type:     RouteType::Keysend,
                address:        "03e7156ae33b0a208d".into(),
                custom_key:     None,
                custom_value:   None,
                split:          100,
                fee:            false,
            }],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = V4VPaymentVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn v4v_payment_verifier_fail_no_routes() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::v4v_payment::V4VPaymentVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-v4v-fail".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = V4VPaymentVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

// ---------------------------------------------------------------------------
// 19. EnclosureTypeVerifier pass (audio) and warn (video)
// ---------------------------------------------------------------------------

#[test]
fn enclosure_type_verifier_pass_audio() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::enclosure_type::EnclosureTypeVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-enc".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![stophammer::ingest::IngestTrackData {
                track_guid:        "t1".into(),
                title:             "Song".into(),
                pub_date:          None,
                duration_secs:     None,
                enclosure_url:     None,
                enclosure_type:    Some("audio/mpeg".into()),
                enclosure_bytes:   None,
                track_number:      None,
                season:            None,
                explicit:          false,
                description:       None,
                author_name:       None,
                payment_routes:    vec![],
                value_time_splits: vec![],
            }],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = EnclosureTypeVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn enclosure_type_verifier_warn_video() {
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::enclosure_type::EnclosureTypeVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-enc-vid".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![stophammer::ingest::IngestTrackData {
                track_guid:        "t-vid".into(),
                title:             "Video Song".into(),
                pub_date:          None,
                duration_secs:     None,
                enclosure_url:     None,
                enclosure_type:    Some("video/mp4".into()),
                enclosure_bytes:   None,
                track_number:      None,
                season:            None,
                explicit:          false,
                description:       None,
                author_name:       None,
                payment_routes:    vec![],
                value_time_splits: vec![],
            }],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = EnclosureTypeVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Warn(_)));
}

// ---------------------------------------------------------------------------
// 20. PaymentRouteSumVerifier pass and fail
// ---------------------------------------------------------------------------

#[test]
fn payment_route_sum_verifier_pass() {
    use stophammer::model::RouteType;
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::payment_route_sum::PaymentRouteSumVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-prs".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![stophammer::ingest::IngestTrackData {
                track_guid:        "t-prs".into(),
                title:             "Song".into(),
                pub_date:          None,
                duration_secs:     None,
                enclosure_url:     None,
                enclosure_type:    None,
                enclosure_bytes:   None,
                track_number:      None,
                season:            None,
                explicit:          false,
                description:       None,
                author_name:       None,
                payment_routes:    vec![
                    stophammer::ingest::IngestPaymentRoute {
                        recipient_name: None,
                        route_type:     RouteType::Keysend,
                        address:        "addr1".into(),
                        custom_key:     None,
                        custom_value:   None,
                        split:          60,
                        fee:            false,
                    },
                    stophammer::ingest::IngestPaymentRoute {
                        recipient_name: None,
                        route_type:     RouteType::Keysend,
                        address:        "addr2".into(),
                        custom_key:     None,
                        custom_value:   None,
                        split:          40,
                        fee:            false,
                    },
                ],
                value_time_splits: vec![],
            }],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = PaymentRouteSumVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Pass));
}

#[test]
fn payment_route_sum_verifier_fail_not_100() {
    use stophammer::model::RouteType;
    use stophammer::verify::{Verifier, VerifyResult};
    use stophammer::verifiers::payment_route_sum::PaymentRouteSumVerifier;

    let conn = common::test_db();
    let req = stophammer::ingest::IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: String::new(),
        source_url:    String::new(),
        http_status:   200,
        content_hash:  String::new(),
        feed_data:     Some(stophammer::ingest::IngestFeedData {
            feed_guid:           "guid-prs-fail".into(),
            title:               "Test".into(),
            description:         None,
            image_url:           None,
            language:            None,
            explicit:            false,
            itunes_type:         None,
            raw_medium:          None,
            author_name:         None,
            owner_name:          None,
            pub_date:            None,
            feed_payment_routes: vec![],
            tracks:              vec![stophammer::ingest::IngestTrackData {
                track_guid:        "t-prs-fail".into(),
                title:             "Song".into(),
                pub_date:          None,
                duration_secs:     None,
                enclosure_url:     None,
                enclosure_type:    None,
                enclosure_bytes:   None,
                track_number:      None,
                season:            None,
                explicit:          false,
                description:       None,
                author_name:       None,
                payment_routes:    vec![
                    stophammer::ingest::IngestPaymentRoute {
                        recipient_name: None,
                        route_type:     RouteType::Keysend,
                        address:        "addr1".into(),
                        custom_key:     None,
                        custom_value:   None,
                        split:          60,
                        fee:            false,
                    },
                    stophammer::ingest::IngestPaymentRoute {
                        recipient_name: None,
                        route_type:     RouteType::Keysend,
                        address:        "addr2".into(),
                        custom_key:     None,
                        custom_value:   None,
                        split:          60,
                        fee:            false,
                    },
                ],
                value_time_splits: vec![],
            }],
        }),
    };
    let ctx = verifier_ctx(&req, &conn);
    let v = PaymentRouteSumVerifier;
    assert!(matches!(v.verify(&ctx), VerifyResult::Fail(_)));
}

// ============================================================================
// Community push: duplicate event returns Duplicate outcome
// ============================================================================

// ---------------------------------------------------------------------------
// 21. Duplicate push event returns Duplicate, not error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn community_push_duplicate_event_returns_duplicate() {
    use http::Request;
    use http_body_util::BodyExt;
    use std::sync::atomic::AtomicI64;
    use tower::ServiceExt;

    let db = common::test_db_arc();

    let signer = stophammer::signing::NodeSigner::load_or_create("/tmp/test-dup-push-signer.key")
        .expect("create signer");
    let primary_pubkey = signer.pubkey_hex().to_string();
    let now = stophammer::db::unix_now();

    // Build a valid ArtistUpserted event.
    let event_id = uuid::Uuid::new_v4().to_string();
    let artist_payload = serde_json::json!({
        "artist": {
            "artist_id": "art-dup-push",
            "name": "Dup Push Artist",
            "name_lower": "dup push artist",
            "created_at": now,
            "updated_at": now
        }
    });
    let payload_json = serde_json::to_string(&artist_payload).expect("serialize");

    let (signed_by, signature) = signer.sign_event(
        &event_id,
        &stophammer::event::EventType::ArtistUpserted,
        &payload_json,
        "art-dup-push",
        now,
    );

    let push_body = serde_json::json!({
        "events": [{
            "event_id": event_id,
            "event_type": "artist_upserted",
            "payload": {
                "type": "artist_upserted",
                "data": artist_payload
            },
            "subject_guid": "art-dup-push",
            "signed_by": signed_by,
            "signature": signature,
            "seq": 1,
            "created_at": now,
            "warnings": [],
            "payload_json": payload_json
        }]
    });

    let state = Arc::new(stophammer::community::CommunityState {
        db:                 Arc::clone(&db),
        primary_pubkey_hex: primary_pubkey,
        last_push_at:       Arc::new(AtomicI64::new(0)),
        sse_registry:       None,
    });

    let app = stophammer::community::build_community_push_router(state);

    // First push: should be applied.
    let req1 = Request::builder()
        .method("POST")
        .uri("/sync/push")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&push_body).unwrap()))
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), 200);
    let bytes1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let body1: serde_json::Value = serde_json::from_slice(&bytes1).unwrap();
    assert_eq!(body1["applied"].as_u64().unwrap(), 1);

    // Second push with same event_id: should be duplicate.
    let req2 = Request::builder()
        .method("POST")
        .uri("/sync/push")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&push_body).unwrap()))
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), 200);
    let bytes2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let body2: serde_json::Value = serde_json::from_slice(&bytes2).unwrap();
    assert_eq!(body2["applied"].as_u64().unwrap(), 0, "duplicate should not be applied");
    assert_eq!(body2["duplicate"].as_u64().unwrap(), 1, "should be counted as duplicate");
    assert_eq!(body2["rejected"].as_u64().unwrap(), 0, "should not be rejected");
}

// ============================================================================
// Community push: bad signature -> rejected
// ============================================================================

// ---------------------------------------------------------------------------
// 22. Push event with bad signature is rejected
// ---------------------------------------------------------------------------

#[tokio::test]
async fn community_push_bad_signature_rejected() {
    use http::Request;
    use http_body_util::BodyExt;
    use std::sync::atomic::AtomicI64;
    use tower::ServiceExt;

    let db = common::test_db_arc();

    let signer = stophammer::signing::NodeSigner::load_or_create("/tmp/test-badsig-push-signer.key")
        .expect("create signer");
    let primary_pubkey = signer.pubkey_hex().to_string();
    let now = stophammer::db::unix_now();

    // Build an event with the correct signer pubkey but a garbage signature.
    let event_id = uuid::Uuid::new_v4().to_string();
    let artist_payload = serde_json::json!({
        "artist": {
            "artist_id": "art-badsig",
            "name": "BadSig Artist",
            "name_lower": "badsig artist",
            "created_at": now,
            "updated_at": now
        }
    });
    let payload_json = serde_json::to_string(&artist_payload).expect("serialize");

    let push_body = serde_json::json!({
        "events": [{
            "event_id": event_id,
            "event_type": "artist_upserted",
            "payload": {
                "type": "artist_upserted",
                "data": artist_payload
            },
            "subject_guid": "art-badsig",
            "signed_by": primary_pubkey,
            "signature": "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "seq": 1,
            "created_at": now,
            "warnings": [],
            "payload_json": payload_json
        }]
    });

    let state = Arc::new(stophammer::community::CommunityState {
        db:                 Arc::clone(&db),
        primary_pubkey_hex: primary_pubkey,
        last_push_at:       Arc::new(AtomicI64::new(0)),
        sse_registry:       None,
    });

    let app = stophammer::community::build_community_push_router(state);

    let req = Request::builder()
        .method("POST")
        .uri("/sync/push")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&push_body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), 200);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    assert_eq!(body["applied"].as_u64().unwrap(), 0, "bad signature should not be applied");
    assert_eq!(body["rejected"].as_u64().unwrap(), 1, "bad signature should be rejected");

    // Verify artist was NOT inserted.
    let conn = db.lock().expect("lock");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artists WHERE artist_id = 'art-badsig'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "artist from bad-signature event must not be in DB");
}

// ============================================================================
// SSRF guard: validate_feed_url unit tests
// ============================================================================

// ---------------------------------------------------------------------------
// 23. validate_feed_url rejects non-HTTP schemes
// ---------------------------------------------------------------------------

#[test]
fn validate_feed_url_rejects_ftp() {
    let result = stophammer::proof::validate_feed_url("ftp://example.com/feed.xml");
    assert!(result.is_err(), "ftp:// should be rejected");
    assert!(
        result.unwrap_err().contains("disallowed URL scheme"),
        "error should mention disallowed scheme"
    );
}

#[test]
fn validate_feed_url_rejects_file() {
    let result = stophammer::proof::validate_feed_url("file:///etc/passwd");
    assert!(result.is_err(), "file:// should be rejected");
}

// ---------------------------------------------------------------------------
// 24. validate_feed_url rejects private IPs
// ---------------------------------------------------------------------------

#[test]
fn validate_feed_url_rejects_localhost() {
    let result = stophammer::proof::validate_feed_url("http://127.0.0.1/feed.xml");
    assert!(result.is_err(), "127.0.0.1 should be rejected");
    assert!(
        result.unwrap_err().contains("private/reserved IP"),
        "error should mention private IP"
    );
}

#[test]
fn validate_feed_url_rejects_private_10() {
    let result = stophammer::proof::validate_feed_url("http://10.0.0.1/feed.xml");
    assert!(result.is_err(), "10.0.0.1 should be rejected");
}

// ---------------------------------------------------------------------------
// 25. validate_feed_url accepts public HTTPS
// ---------------------------------------------------------------------------

#[test]
fn validate_feed_url_accepts_public_https() {
    // We can't guarantee DNS resolution in CI, but the scheme+IP check passes.
    // Use a literal public IP so DNS resolution is not needed.
    let result = stophammer::proof::validate_feed_url("https://8.8.8.8/feed.xml");
    assert!(result.is_ok(), "public IP over HTTPS should be accepted");
}
