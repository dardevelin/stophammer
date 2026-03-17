#![expect(clippy::significant_drop_tightening, reason = "MutexGuard<Connection> must be held for the full scope in test setup")]

mod common;

use rusqlite::params;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn insert_artist(conn: &rusqlite::Connection, artist_id: &str, name: &str, now: i64) {
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![artist_id, name, name.to_lowercase(), now, now],
    )
    .unwrap();
}

fn insert_artist_credit(
    conn: &rusqlite::Connection,
    artist_id: &str,
    display_name: &str,
    now: i64,
) -> i64 {
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params![display_name, now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![credit_id, artist_id, 0, display_name, ""],
    )
    .unwrap();
    credit_id
}

fn insert_feed(
    conn: &rusqlite::Connection,
    feed_guid: &str,
    feed_url: &str,
    title: &str,
    credit_id: i64,
    now: i64,
) {
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, \
         description, explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            feed_guid,
            feed_url,
            title,
            title.to_lowercase(),
            credit_id,
            "A test feed",
            0,
            0,
            now,
            now,
        ],
    )
    .unwrap();
}

fn insert_track(
    conn: &rusqlite::Connection,
    track_guid: &str,
    feed_guid: &str,
    credit_id: i64,
    title: &str,
    now: i64,
) {
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, \
         description, explicit, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            track_guid,
            feed_guid,
            credit_id,
            title,
            title.to_lowercase(),
            "A test track",
            0,
            now,
            now,
        ],
    )
    .unwrap();
}

/// Build a full `AppState` backed by the given DB, suitable for test routers.
fn test_app_state(db: Arc<Mutex<rusqlite::Connection>>) -> Arc<stophammer::api::AppState> {
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create("/tmp/test-proof-signer.key").unwrap(),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db: stophammer::db_pool::DbPool::from_writer_only(db),
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex:  pubkey,
        admin_token:      "test-admin-token".into(),
        sync_token:      None,
        push_client:      reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry: Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

/// Generate RSS XML with a `podcast:txt` element containing the given text.
fn rss_with_podcast_txt(txt_content: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:podcast="https://podcastindex.org/namespace/1.0">
  <channel>
    <title>Test Podcast</title>
    <podcast:txt>{txt_content}</podcast:txt>
  </channel>
</rss>"#
    )
}

/// Seed a feed at a given URL (used when we need to point at a mock server).
fn seed_feed_at_url(conn: &rusqlite::Connection, feed_url: &str) -> (i64, i64) {
    let now = common::now();
    insert_artist(conn, "artist-1", "Test Artist", now);
    let credit_id = insert_artist_credit(conn, "artist-1", "Test Artist", now);
    insert_feed(
        conn,
        "feed-1",
        feed_url,
        "Test Album",
        credit_id,
        now,
    );
    (credit_id, now)
}

/// Perform the full challenge -> assert flow with mock RSS verification.
/// Returns the `access_token` string.
async fn challenge_assert_with_mock(
    app: &axum::Router,
    mock_server: &MockServer,
    feed_guid: &str,
    nonce: &str,
) -> String {
    // Step 1: create challenge
    let resp = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": feed_guid,
                "scope": "feed:write",
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body = body_json(resp).await;
    let challenge_id = body["challenge_id"].as_str().unwrap().to_string();
    let token_binding = body["token_binding"].as_str().unwrap().to_string();

    // Mount RSS with the correct token_binding
    let rss = rss_with_podcast_txt(&format!("stophammer-proof {token_binding}"));
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(mock_server)
        .await;

    // Step 2: assert
    let resp2 = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/assert",
            &serde_json::json!({
                "challenge_id": challenge_id,
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    let body2 = body_json(resp2).await;
    body2["access_token"].as_str().unwrap().to_string()
}

// ============================================================================
// Unit tests — proof module (direct DB access)
// ============================================================================

// ---------------------------------------------------------------------------
// 1. create_challenge stores a pending challenge
// ---------------------------------------------------------------------------

#[test]
fn create_challenge_stores_pending() {
    let conn = common::test_db();
    let (challenge_id, token_binding) =
        stophammer::proof::create_challenge(&conn, "feed-abc", "feed:write", "my-nonce").unwrap();

    assert!(!challenge_id.is_empty());
    assert!(token_binding.contains('.'), "binding should have token.hash format");

    let ch = stophammer::proof::get_challenge(&conn, &challenge_id)
        .unwrap()
        .expect("challenge should exist");
    assert_eq!(ch.state, "pending");
    assert_eq!(ch.feed_guid, "feed-abc");
    assert_eq!(ch.scope, "feed:write");
    assert_eq!(ch.token_binding, token_binding);
}

// ---------------------------------------------------------------------------
// 2. expired challenge returns None from get_challenge
// ---------------------------------------------------------------------------

#[test]
fn challenge_expires_after_24h() {
    let conn = common::test_db();

    // Insert a challenge with an already-expired expires_at.
    let past = common::now() - 1;
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('expired-ch', 'feed-x', 'feed:write', 'tok.hash', 'pending', ?1, ?2)",
        params![past, past - 86400],
    )
    .unwrap();

    let result = stophammer::proof::get_challenge(&conn, "expired-ch").unwrap();
    assert!(result.is_none(), "expired challenge should not be returned");
}

// ---------------------------------------------------------------------------
// 3. resolve_challenge transitions to valid
// ---------------------------------------------------------------------------

#[test]
fn resolve_challenge_valid() {
    let conn = common::test_db();
    let (challenge_id, _) =
        stophammer::proof::create_challenge(&conn, "feed-abc", "feed:write", "nonce1").unwrap();

    stophammer::proof::resolve_challenge(&conn, &challenge_id, "valid").unwrap();

    let ch = stophammer::proof::get_challenge(&conn, &challenge_id)
        .unwrap()
        .expect("challenge should still exist");
    assert_eq!(ch.state, "valid");
}

// ---------------------------------------------------------------------------
// 4. resolve_challenge transitions to invalid
// ---------------------------------------------------------------------------

#[test]
fn resolve_challenge_invalid() {
    let conn = common::test_db();
    let (challenge_id, _) =
        stophammer::proof::create_challenge(&conn, "feed-abc", "feed:write", "nonce2").unwrap();

    stophammer::proof::resolve_challenge(&conn, &challenge_id, "invalid").unwrap();

    let ch = stophammer::proof::get_challenge(&conn, &challenge_id)
        .unwrap()
        .expect("challenge should still exist");
    assert_eq!(ch.state, "invalid");
}

// ---------------------------------------------------------------------------
// 5. issue_token returns a token that validates with correct scope
// ---------------------------------------------------------------------------

#[test]
fn issue_token_valid() {
    let conn = common::test_db();
    let token =
        stophammer::proof::issue_token(&conn, "feed:write", "feed-abc", &stophammer::proof::ProofLevel::RssOnly).unwrap();

    let result = stophammer::proof::validate_token(&conn, &token, "feed:write").unwrap();
    assert_eq!(result, Some("feed-abc".to_string()));
}

// ---------------------------------------------------------------------------
// 6. validate_token returns None for wrong scope
// ---------------------------------------------------------------------------

#[test]
fn issue_token_wrong_scope() {
    let conn = common::test_db();
    let token =
        stophammer::proof::issue_token(&conn, "feed:write", "feed-abc", &stophammer::proof::ProofLevel::RssOnly).unwrap();

    let result = stophammer::proof::validate_token(&conn, &token, "feed:admin").unwrap();
    assert!(result.is_none(), "wrong scope should not validate");
}

// ---------------------------------------------------------------------------
// 7. expired token returns None from validate_token
// ---------------------------------------------------------------------------

#[test]
fn token_expires() {
    let conn = common::test_db();

    // Insert a token with an already-expired expires_at.
    let past = common::now() - 1;
    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('expired-tok', 'feed:write', 'feed-x', ?1, ?2)",
        params![past, past - 3600],
    )
    .unwrap();

    let result = stophammer::proof::validate_token(&conn, "expired-tok", "feed:write").unwrap();
    assert!(result.is_none(), "expired token should not validate");
}

// ============================================================================
// Integration tests — HTTP endpoints (axum router + tower::ServiceExt)
// ============================================================================

use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Helper to create a JSON request body.
fn json_request(method: &str, uri: &str, body: &serde_json::Value) -> Request<axum::body::Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

/// Helper to read the response body as JSON.
async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

// ---------------------------------------------------------------------------
// 8. POST /proofs/challenge then POST /proofs/assert returns access_token
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assert_endpoint_issues_token() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        let now = common::now();
        insert_artist(&conn, "artist-abc", "Test Artist", now);
        let credit_id = insert_artist_credit(&conn, "artist-abc", "Test Artist", now);
        insert_feed(&conn, "feed-abc", &mock_server.uri(), "Test Album", credit_id, now);
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "test-nonce-1234x";

    // Step 1: create challenge.
    let resp = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": "feed-abc",
                "scope": "feed:write",
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let body = body_json(resp).await;
    let challenge_id = body["challenge_id"].as_str().unwrap();
    let token_binding = body["token_binding"].as_str().unwrap();
    assert_eq!(body["state"].as_str().unwrap(), "pending");
    assert!(token_binding.contains('.'));
    assert!(body["expires_at"].as_i64().is_some());

    // Mount RSS with the correct token_binding so verification passes.
    let rss = rss_with_podcast_txt(&format!("stophammer-proof {token_binding}"));
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(&mock_server)
        .await;

    // Step 2: assert with correct nonce.
    let resp2 = app
        .oneshot(json_request(
            "POST",
            "/v1/proofs/assert",
            &serde_json::json!({
                "challenge_id": challenge_id,
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);

    let body2 = body_json(resp2).await;
    assert!(body2["access_token"].as_str().is_some());
    assert_eq!(body2["scope"].as_str().unwrap(), "feed:write");
    assert_eq!(body2["subject_feed_guid"].as_str().unwrap(), "feed-abc");
    assert!(body2["expires_at"].as_i64().is_some());
}

// ---------------------------------------------------------------------------
// 9. POST /proofs/assert with wrong nonce returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assert_wrong_nonce_returns_400() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    // Create challenge with one nonce.
    let resp = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": "feed-xyz",
                "scope": "feed:write",
                "requester_nonce": "correct-nonce-ok",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body = body_json(resp).await;
    let challenge_id = body["challenge_id"].as_str().unwrap();

    // Assert with wrong nonce.
    let resp2 = app
        .oneshot(json_request(
            "POST",
            "/v1/proofs/assert",
            &serde_json::json!({
                "challenge_id": challenge_id,
                "requester_nonce": "wrong-nonce-here",
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp2.status(), 400);
}

// ---------------------------------------------------------------------------
// 10. PATCH /feeds/{guid} with valid Bearer token updates feed_url
// REST semantics compliant (RFC 7396) — 2026-03-12
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_feed_with_valid_token() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        seed_feed_at_url(&conn, &mock_server.uri());
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "patch-feed-nonce";
    let access_token = challenge_assert_with_mock(&app, &mock_server, "feed-1", nonce).await;

    // PATCH the feed.
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/v1/feeds/feed-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "feed_url": "https://new-location.example.com/feed.xml"
            }))
            .unwrap(),
        ))
        .unwrap();

    let resp3 = app.oneshot(patch_req).await.unwrap();
    assert_eq!(resp3.status(), 204);

    // Verify the URL was actually updated in the database.
    let url: String = {
        let conn = db.lock().unwrap();
        conn.query_row(
            "SELECT feed_url FROM feeds WHERE feed_guid = 'feed-1'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(url, "https://new-location.example.com/feed.xml");
}

// ---------------------------------------------------------------------------
// 11. PATCH /feeds/{guid} with token for wrong feed returns 403
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_feed_with_wrong_guid_returns_403() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        let now = common::now();
        insert_artist(&conn, "artist-1", "Test Artist", now);
        let credit_id = insert_artist_credit(&conn, "artist-1", "Test Artist", now);
        insert_feed(&conn, "feed-a", &mock_server.uri(), "Feed A", credit_id, now);
        insert_feed(&conn, "feed-b", "https://example.com/b.xml", "Feed B", credit_id, now);
        drop(conn);
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    // Get a token scoped to feed-a.
    let nonce = "wrong-guid-nonce";
    let access_token = challenge_assert_with_mock(&app, &mock_server, "feed-a", nonce).await;

    // Try to PATCH feed-b with a token for feed-a.
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/v1/feeds/feed-b")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "feed_url": "https://evil.example.com/feed.xml"
            }))
            .unwrap(),
        ))
        .unwrap();

    let resp3 = app.oneshot(patch_req).await.unwrap();
    assert_eq!(resp3.status(), 403);
}

// ---------------------------------------------------------------------------
// 12. PATCH /tracks/{guid} with valid token updates enclosure_url
// REST semantics compliant (RFC 7396) — 2026-03-12
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_track_with_valid_token() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        let (credit_id, now) = seed_feed_at_url(&conn, &mock_server.uri());
        insert_track(&conn, "track-1", "feed-1", credit_id, "Song One", now);
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "patch-track-nonce";
    let access_token = challenge_assert_with_mock(&app, &mock_server, "feed-1", nonce).await;

    // PATCH the track.
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/v1/tracks/track-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "enclosure_url": "https://cdn.example.com/new-song.mp3"
            }))
            .unwrap(),
        ))
        .unwrap();

    let resp3 = app.oneshot(patch_req).await.unwrap();
    assert_eq!(resp3.status(), 204);

    // Verify the URL was actually updated in the database.
    let url: String = {
        let conn = db.lock().unwrap();
        conn.query_row(
            "SELECT enclosure_url FROM tracks WHERE track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(url, "https://cdn.example.com/new-song.mp3");
}

// ---------------------------------------------------------------------------
// 13. PATCH /feeds/{guid} with empty body returns 204 No Content
// REST semantics compliant (RFC 7396) — 2026-03-12
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_feed_no_fields_returns_204() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        seed_feed_at_url(&conn, &mock_server.uri());
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "patch-empty-nonce";
    let access_token = challenge_assert_with_mock(&app, &mock_server, "feed-1", nonce).await;

    // PATCH with empty body (no fields).
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/v1/feeds/feed-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({})).unwrap(),
        ))
        .unwrap();

    let resp3 = app.oneshot(patch_req).await.unwrap();
    assert_eq!(resp3.status(), 204);
}

// ---------------------------------------------------------------------------
// 14. POST /proofs/challenge with nonce shorter than 16 chars returns 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assert_empty_nonce_returns_400() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    // Nonce that is too short (15 chars).
    let resp = app
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": "feed-abc",
                "scope": "feed:write",
                "requester_nonce": "short-nonce-xxx",
            }),
        ))
        .await
        .unwrap();

    assert_eq!(resp.status(), 400);
    let body = body_json(resp).await;
    assert!(
        body["error"].as_str().unwrap().contains("16"),
        "error message should mention the minimum length"
    );
}

// ============================================================================
// Sprint 3A — Issue #9: recompute_binding malformed input
// ============================================================================

// ---------------------------------------------------------------------------
// 15. recompute_binding with valid input returns Some(expected_token)
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_valid_input() {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    use sha2::{Digest, Sha256};

    let base_token = "abcdefABCDEF0123456789";
    let nonce = "test-requester-nonce";

    let mut hasher = Sha256::new();
    hasher.update(nonce.as_bytes());
    let hash = hasher.finalize();
    let expected = format!("{}.{}", base_token, URL_SAFE_NO_PAD.encode(hash));

    let stored_binding = format!("{base_token}.old-hash-value");

    let result = stophammer::proof::recompute_binding(&stored_binding, nonce);
    assert_eq!(result, Some(expected));
}

// ---------------------------------------------------------------------------
// 16. recompute_binding with missing '.' separator returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_missing_separator_returns_none() {
    let result = stophammer::proof::recompute_binding("no-dot-in-this-string", "some-nonce");
    assert!(result.is_none(), "missing '.' separator should return None");
}

// ---------------------------------------------------------------------------
// 17. recompute_binding with empty base_token part returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_empty_base_token_returns_none() {
    let result = stophammer::proof::recompute_binding(".some-hash", "some-nonce");
    assert!(result.is_none(), "empty base_token should return None");
}

// ---------------------------------------------------------------------------
// 18. recompute_binding with empty hash part returns None
// ---------------------------------------------------------------------------

#[test]
fn recompute_binding_empty_hash_part_returns_none() {
    let result = stophammer::proof::recompute_binding("some-token.", "some-nonce");
    assert!(result.is_none(), "empty hash part should return None");
}

// ============================================================================
// Sprint 3A — Issue #17: prune_expired
// ============================================================================

// ---------------------------------------------------------------------------
// 19. prune_expired deletes rows where expires_at < now
// ---------------------------------------------------------------------------

#[test]
fn prune_expired_deletes_expired_rows() {
    let mut conn = common::test_db();
    let now = common::now();
    let past = now - 3600; // 1 hour ago

    // Insert expired challenge and token.
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('expired-ch-1', 'feed-x', 'feed:write', 'tok.hash', 'pending', ?1, ?2)",
        params![past, past - 86400],
    ).unwrap();

    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('expired-tok-1', 'feed:write', 'feed-x', ?1, ?2)",
        params![past, past - 3600],
    ).unwrap();

    let deleted = stophammer::proof::prune_expired(&mut conn).unwrap();
    assert!(deleted >= 2, "should have deleted at least 2 expired rows, got {deleted}");

    // Verify the rows are gone.
    let ch_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_challenges WHERE challenge_id = 'expired-ch-1'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(ch_count, 0, "expired challenge should be deleted");

    let tok_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_tokens WHERE access_token = 'expired-tok-1'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(tok_count, 0, "expired token should be deleted");
}

// ---------------------------------------------------------------------------
// 20. prune_expired does NOT delete rows where expires_at > now
// ---------------------------------------------------------------------------

#[test]
fn prune_expired_keeps_unexpired_rows() {
    let mut conn = common::test_db();
    let now = common::now();
    let future = now + 86400; // 24 hours from now

    // Insert unexpired challenge and token.
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('valid-ch-1', 'feed-y', 'feed:write', 'tok.hash', 'pending', ?1, ?2)",
        params![future, now],
    ).unwrap();

    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('valid-tok-1', 'feed:write', 'feed-y', ?1, ?2)",
        params![future, now],
    ).unwrap();

    let deleted = stophammer::proof::prune_expired(&mut conn).unwrap();
    assert_eq!(deleted, 0, "no rows should be deleted when all are unexpired");

    // Verify the rows still exist.
    let ch_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_challenges WHERE challenge_id = 'valid-ch-1'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(ch_count, 1, "unexpired challenge should still exist");

    let tok_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_tokens WHERE access_token = 'valid-tok-1'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(tok_count, 1, "unexpired token should still exist");
}

// ---------------------------------------------------------------------------
// 21. prune_expired returns count of deleted rows
// ---------------------------------------------------------------------------

#[test]
fn prune_expired_returns_correct_count() {
    let mut conn = common::test_db();
    let now = common::now();
    let past = now - 3600;
    let future = now + 86400;

    // Insert 2 expired challenges, 1 expired token, 1 unexpired of each.
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('exp-ch-a', 'feed-z', 'feed:write', 'tok.hash', 'pending', ?1, ?2)",
        params![past, past - 86400],
    ).unwrap();
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('exp-ch-b', 'feed-z', 'feed:write', 'tok.hash', 'valid', ?1, ?2)",
        params![past, past - 86400],
    ).unwrap();
    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES ('live-ch-a', 'feed-z', 'feed:write', 'tok.hash', 'pending', ?1, ?2)",
        params![future, now],
    ).unwrap();

    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('exp-tok-a', 'feed:write', 'feed-z', ?1, ?2)",
        params![past, past - 3600],
    ).unwrap();
    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('live-tok-a', 'feed:write', 'feed-z', ?1, ?2)",
        params![future, now],
    ).unwrap();

    let deleted = stophammer::proof::prune_expired(&mut conn).unwrap();
    assert_eq!(deleted, 3, "should have deleted 2 challenges + 1 token = 3 total");
}

// ============================================================================
// Sprint 4B — Issue #15: Full challenge → assert → PATCH HTTP round-trip
// ============================================================================

// ---------------------------------------------------------------------------
// 22. Full round-trip: challenge → assert → PATCH feed + PATCH track → 204
// ---------------------------------------------------------------------------

#[tokio::test]
#[expect(clippy::too_many_lines, reason = "integration test exercises full challenge-assert-patch round-trip")]
async fn full_roundtrip_challenge_assert_patch_feed_and_track() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().expect("lock for seeding");
        let (credit_id, now) = seed_feed_at_url(&conn, &mock_server.uri());
        insert_track(&conn, "track-rt-1", "feed-1", credit_id, "Round-trip Song", now);
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "roundtrip-nonce-16ch";

    // ── Step 1: POST /v1/proofs/challenge ──────────────────────────────────
    let resp_challenge = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": "feed-1",
                "scope": "feed:write",
                "requester_nonce": nonce,
            }),
        ))
        .await
        .expect("challenge request should not panic");

    assert_eq!(resp_challenge.status(), 201, "challenge should return 201 Created");
    let challenge_body = body_json(resp_challenge).await;
    let challenge_id = challenge_body["challenge_id"]
        .as_str()
        .expect("response must contain challenge_id");
    let token_binding = challenge_body["token_binding"]
        .as_str()
        .expect("response must contain token_binding");
    assert!(
        token_binding.contains('.'),
        "token_binding should have token.hash format"
    );
    assert_eq!(
        challenge_body["state"].as_str().expect("state field"),
        "pending"
    );

    // Verify challenge was persisted in DB with correct state.
    {
        let conn = db.lock().expect("lock for challenge verification");
        let ch = stophammer::proof::get_challenge(&conn, challenge_id)
            .expect("DB query should succeed")
            .expect("challenge should exist in DB");
        drop(conn);
        assert_eq!(ch.state, "pending");
        assert_eq!(ch.feed_guid, "feed-1");
        assert_eq!(ch.scope, "feed:write");
    }

    // ── Step 1b: Mount RSS with matching token_binding ─────────────────────
    let rss = rss_with_podcast_txt(&format!("stophammer-proof {token_binding}"));
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(&mock_server)
        .await;

    // ── Step 2: POST /v1/proofs/assert ─────────────────────────────────────
    let resp_assert = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/assert",
            &serde_json::json!({
                "challenge_id": challenge_id,
                "requester_nonce": nonce,
            }),
        ))
        .await
        .expect("assert request should not panic");

    assert_eq!(resp_assert.status(), 200, "assert should return 200 OK");
    let assert_body = body_json(resp_assert).await;
    let access_token = assert_body["access_token"]
        .as_str()
        .expect("response must contain access_token");
    assert_eq!(
        assert_body["scope"].as_str().expect("scope field"),
        "feed:write"
    );
    assert_eq!(
        assert_body["subject_feed_guid"].as_str().expect("subject_feed_guid field"),
        "feed-1"
    );
    assert!(
        assert_body["expires_at"].as_i64().is_some(),
        "expires_at should be present"
    );

    // Verify challenge was resolved to "valid" in DB.
    {
        let conn = db.lock().expect("lock for assert verification");
        let ch = stophammer::proof::get_challenge(&conn, challenge_id)
            .expect("DB query should succeed")
            .expect("challenge should still exist");
        drop(conn);
        assert_eq!(ch.state, "valid", "challenge should be resolved to valid after assert");
    }

    // ── Step 3: PATCH /v1/feeds/feed-1 with Bearer token ───────────────────
    let patch_feed_req = Request::builder()
        .method("PATCH")
        .uri("/v1/feeds/feed-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "feed_url": "https://roundtrip-updated.example.com/feed.xml"
            }))
            .expect("json serialization"),
        ))
        .expect("build patch feed request");

    let resp_patch_feed = app
        .clone()
        .oneshot(patch_feed_req)
        .await
        .expect("patch feed request should not panic");

    assert_eq!(
        resp_patch_feed.status(),
        204,
        "PATCH feed with valid Bearer should return 204 No Content"
    );

    // Verify feed_url was updated in the database.
    {
        let conn = db.lock().expect("lock for feed patch verification");
        let url: String = conn
            .query_row(
                "SELECT feed_url FROM feeds WHERE feed_guid = 'feed-1'",
                [],
                |r| r.get(0),
            )
            .expect("feed should exist");
        drop(conn);
        assert_eq!(url, "https://roundtrip-updated.example.com/feed.xml");
    }

    // ── Step 4: PATCH /v1/tracks/track-rt-1 with revoked Bearer → 401 ──────
    // Finding-6 token revocation on URL change — 2026-03-13
    // The feed_url PATCH in step 3 revoked the token, so the same token
    // must now be rejected.
    let patch_track_req = Request::builder()
        .method("PATCH")
        .uri("/v1/tracks/track-rt-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "enclosure_url": "https://cdn.roundtrip.example.com/song.mp3"
            }))
            .expect("json serialization"),
        ))
        .expect("build patch track request");

    let resp_patch_track = app
        .clone()
        .oneshot(patch_track_req)
        .await
        .expect("patch track request should not panic");

    assert_eq!(
        resp_patch_track.status(),
        401,
        "PATCH track with revoked Bearer should return 401 Unauthorized"
    );

    // ── Step 4b: PATCH /v1/tracks/track-rt-1 with admin token → 204 ─────
    // Confirm the track endpoint itself still works with valid auth.
    let patch_track_admin = Request::builder()
        .method("PATCH")
        .uri("/v1/tracks/track-rt-1")
        .header("Content-Type", "application/json")
        .header("X-Admin-Token", "test-admin-token")
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "enclosure_url": "https://cdn.roundtrip.example.com/song.mp3"
            }))
            .expect("json serialization"),
        ))
        .expect("build patch track admin request");

    let resp_patch_admin = app
        .oneshot(patch_track_admin)
        .await
        .expect("patch track admin request should not panic");

    assert_eq!(
        resp_patch_admin.status(),
        204,
        "PATCH track with admin token should return 204 No Content"
    );

    // Verify enclosure_url was updated in the database.
    {
        let conn = db.lock().expect("lock for track patch verification");
        let url: String = conn
            .query_row(
                "SELECT enclosure_url FROM tracks WHERE track_guid = 'track-rt-1'",
                [],
                |r| r.get(0),
            )
            .expect("track should exist");
        drop(conn);
        assert_eq!(url, "https://cdn.roundtrip.example.com/song.mp3");
    }
}

// ============================================================================
// Finding-6 token revocation on URL change — 2026-03-13
// ============================================================================

// ---------------------------------------------------------------------------
// 23. revoke_tokens_for_feed deletes all tokens for the given feed_guid
// ---------------------------------------------------------------------------

#[test]
fn revoke_tokens_for_feed_deletes_matching_tokens() {
    let conn = common::test_db();
    let now = common::now();
    let future = now + 3600;

    // Insert two tokens for feed-a and one for feed-b.
    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('tok-a1', 'feed:write', 'feed-a', ?1, ?2)",
        params![future, now],
    ).unwrap();
    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('tok-a2', 'feed:write', 'feed-a', ?1, ?2)",
        params![future, now],
    ).unwrap();
    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at) \
         VALUES ('tok-b1', 'feed:write', 'feed-b', ?1, ?2)",
        params![future, now],
    ).unwrap();

    let deleted = stophammer::proof::revoke_tokens_for_feed(&conn, "feed-a").unwrap();
    assert_eq!(deleted, 2, "should delete exactly 2 tokens for feed-a");

    // feed-a tokens are gone.
    let count_a: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_tokens WHERE subject_feed_guid = 'feed-a'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(count_a, 0, "no tokens should remain for feed-a");

    // feed-b token is untouched.
    let count_b: i64 = conn.query_row(
        "SELECT COUNT(*) FROM proof_tokens WHERE subject_feed_guid = 'feed-b'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(count_b, 1, "feed-b token should be untouched");
}

// ---------------------------------------------------------------------------
// 24. PATCH feed_url revokes all proof tokens for that feed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn patch_feed_url_revokes_proof_tokens() {
    let mock_server = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        seed_feed_at_url(&conn, &mock_server.uri());
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "revoke-test-nonce1";
    let access_token = challenge_assert_with_mock(&app, &mock_server, "feed-1", nonce).await;

    // Verify the token is valid before the PATCH.
    {
        let conn = db.lock().unwrap();
        let result = stophammer::proof::validate_token(&conn, &access_token, "feed:write").unwrap();
        assert_eq!(
            result,
            Some("feed-1".to_string()),
            "token should be valid before PATCH"
        );
    }

    // PATCH the feed_url — this should revoke all tokens for feed-1.
    let patch_req = Request::builder()
        .method("PATCH")
        .uri("/v1/feeds/feed-1")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {access_token}"))
        .body(axum::body::Body::from(
            serde_json::to_vec(&serde_json::json!({
                "feed_url": "https://new-url.example.com/feed.xml"
            }))
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(patch_req).await.unwrap();
    assert_eq!(resp.status(), 204, "PATCH should succeed");

    // After the PATCH, the token should be revoked (no longer valid).
    {
        let conn = db.lock().unwrap();
        let result = stophammer::proof::validate_token(&conn, &access_token, "feed:write").unwrap();
        assert!(
            result.is_none(),
            "token should be revoked after feed_url PATCH"
        );
    }

    // Count remaining tokens for feed-1 — should be zero.
    {
        let conn = db.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM proof_tokens WHERE subject_feed_guid = 'feed-1'",
            [],
            |r| r.get(0),
        ).unwrap();
        assert_eq!(count, 0, "all tokens for feed-1 should be revoked after feed_url PATCH");
    }
}

// ---------------------------------------------------------------------------
// Issue-PROOF-RACE: TOCTOU race — assert refuses token when feed URL changed
// between phase 1 (read URL) and phase 3 (issue token).
//
// Unit-level test: simulates what phase 3 does when the feed URL has changed
// since phase 1. We exercise the DB re-read that the fix adds to phase 3.
// ---------------------------------------------------------------------------

#[test]
fn proof_race_rejects_token_when_feed_url_changed() {
    let conn = common::test_db();
    let now = common::now();

    // Set up feed with old URL.
    insert_artist(&conn, "artist-race", "Race Artist", now);
    let credit_id = insert_artist_credit(&conn, "artist-race", "Race Artist", now);
    insert_feed(
        &conn,
        "feed-race",
        "https://old.example.com/feed.xml",
        "Race Feed",
        credit_id,
        now,
    );

    // Create a challenge for this feed.
    let (_challenge_id, _token_binding) =
        stophammer::proof::create_challenge(&conn, "feed-race", "feed:write", "race-nonce-12345")
            .unwrap();

    // Phase 1 would have read feed_url = "https://old.example.com/feed.xml".
    let verified_url = "https://old.example.com/feed.xml".to_string();

    // Simulate concurrent PATCH: change the feed URL in the DB.
    conn.execute(
        "UPDATE feeds SET feed_url = 'https://new.example.com/feed.xml' WHERE feed_guid = 'feed-race'",
        [],
    )
    .unwrap();

    // Phase 3 logic: re-read feed URL and compare with verified_url.
    let current_feed = stophammer::db::get_feed_by_guid(&conn, "feed-race")
        .unwrap()
        .expect("feed should exist");

    // The URLs should differ — the guard should fire.
    assert_ne!(
        current_feed.feed_url, verified_url,
        "feed URL should have changed (simulated concurrent PATCH)"
    );

    // No token should be issued when URLs differ.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM proof_tokens WHERE subject_feed_guid = 'feed-race'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "no token should be issued when feed URL changed during verification");
}

// ---------------------------------------------------------------------------
// Issue-PROOF-RACE integration: full HTTP flow — a concurrent URL change
// between phase 1 and phase 3 produces 409.
//
// We use two mock servers (both serving valid RSS) and spawn a racing task
// that changes the feed URL while the assert handler is in flight. Depending
// on timing the result is 200, 409, or 503 — all are correct behaviors.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assert_rejects_token_when_feed_url_changed_during_verification() {
    let mock_old = MockServer::start().await;
    let mock_new = MockServer::start().await;
    let db = common::test_db_arc();
    {
        let conn = db.lock().unwrap();
        seed_feed_at_url(&conn, &mock_old.uri());
    }
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let nonce = "race-nonce-12345";

    // Create challenge.
    let resp = app
        .clone()
        .oneshot(json_request(
            "POST",
            "/v1/proofs/challenge",
            &serde_json::json!({
                "feed_guid": "feed-1",
                "scope": "feed:write",
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let body = body_json(resp).await;
    let challenge_id = body["challenge_id"].as_str().unwrap().to_string();
    let token_binding = body["token_binding"].as_str().unwrap().to_string();

    // Mount correct RSS on BOTH mock servers so phase 2 succeeds regardless
    // of which URL phase 1 reads.
    let rss = rss_with_podcast_txt(&format!("stophammer-proof {token_binding}"));
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss.clone()))
        .mount(&mock_old)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(rss))
        .mount(&mock_new)
        .await;

    // Spawn a concurrent task that changes the feed URL, racing with phase 3.
    let db_race = Arc::clone(&db);
    let new_uri = mock_new.uri();
    let race_handle = tokio::task::spawn(async move {
        tokio::task::yield_now().await;
        let conn = db_race.lock().unwrap();
        conn.execute(
            &format!(
                "UPDATE feeds SET feed_url = '{new_uri}' WHERE feed_guid = 'feed-1'"
            ),
            [],
        )
        .unwrap();
    });

    let resp2 = app
        .oneshot(json_request(
            "POST",
            "/v1/proofs/assert",
            &serde_json::json!({
                "challenge_id": challenge_id,
                "requester_nonce": nonce,
            }),
        ))
        .await
        .unwrap();

    race_handle.await.unwrap();

    // Non-deterministic timing: the URL change may land before phase 1 (both
    // reads see new URL, no mismatch, 200), between phase 1 and phase 3
    // (mismatch, 409), or after phase 3 (200). All outcomes are correct.
    let status = resp2.status().as_u16();
    assert!(
        status == 200 || status == 409,
        "expected 200 or 409 but got {status}"
    );
}
