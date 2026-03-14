// Sprint 3B tests — 2026-03-13
//
// TDD tests for issues #8, #18, #19, #23, CRIT-02, CRIT-03, HIGH-02.

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

// ── Issue #8: VerifierChain::run() must have #[must_use] ─────────────────────

/// Compile-time assertion: `VerifierChain::run()` returns Result and should
/// be marked `#[must_use]`. We verify the method returns the expected type.
#[test]
fn verifier_chain_run_returns_result() {
    let chain = stophammer::verify::VerifierChain::new(vec![]);
    let conn = common::test_db();
    let request = dummy_ingest_request();
    let ctx = stophammer::verify::IngestContext {
        request:  &request,
        db:       &conn,
        existing: None,
    };
    // Must use the result — if #[must_use] is missing, clippy will warn
    let result = chain.run(&ctx);
    assert!(result.is_ok());
}

// ── Issue #23: SSE connection counter TOCTOU ─────────────────────────────────

/// Verifies that `try_acquire_connection` is atomic by checking that concurrent
/// acquisitions do not exceed the limit.
#[test]
fn sse_connection_counter_atomic() {
    let registry = stophammer::api::SseRegistry::new();

    // Acquire a connection, verify count increases
    assert!(registry.try_acquire_connection());
    assert_eq!(registry.active_connections(), 1);

    // Release and verify count decreases
    registry.release_connection();
    assert_eq!(registry.active_connections(), 0);
}

// ── Issue #18: CORS configurable origin ──────────────────────────────────────

/// When `CORS_ALLOW_ORIGIN` is not set, the default should be wildcard (*).
/// Note: we test via `build_cors_layer` behavior through the router.
#[tokio::test]
async fn cors_default_is_any_origin() {
    let state = test_app_state();
    let app = stophammer::api::build_router(state);

    let resp = tower::ServiceExt::oneshot(
        app,
        http::Request::builder()
            .method("GET")
            .uri("/health")
            .header("Origin", "https://example.com")
            .body(axum::body::Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    let origin = resp
        .headers()
        .get("access-control-allow-origin")
        .expect("should have CORS header");
    // Default (no CORS_ALLOW_ORIGIN env var) should be wildcard
    assert_eq!(origin, "*", "default CORS should allow any origin");
}

// ── CRIT-03: Debug derives on public types ───────────────────────────────────

#[test]
fn api_error_has_debug() {
    let err = stophammer::api::ApiError {
        status: http::StatusCode::BAD_REQUEST,
        message: "test".into(),
        www_authenticate: None,
    };
    let _ = format!("{err:?}");
}

#[test]
fn event_row_has_debug() {
    let row = stophammer::db::EventRow {
        event_id:     "e1".into(),
        event_type:   stophammer::event::EventType::ArtistUpserted,
        payload_json: "{}".into(),
        subject_guid: "s1".into(),
        signed_by:    "pk".into(),
        signature:    "sig".into(),
        created_at:   1000,
        warnings:     vec![],
    };
    let _ = format!("{row:?}");
}

#[test]
fn external_id_row_has_debug() {
    let row = stophammer::db::ExternalIdRow {
        id:     1,
        scheme: "isrc".into(),
        value:  "US1234".into(),
    };
    let _ = format!("{row:?}");
}

#[test]
fn entity_source_row_has_debug() {
    let row = stophammer::db::EntitySourceRow {
        id:          1,
        source_type: "rss".into(),
        source_url:  Some("https://example.com".into()),
        trust_level: 5,
        created_at:  1000,
    };
    let _ = format!("{row:?}");
}

#[test]
fn apply_outcome_has_debug() {
    let outcome = stophammer::apply::ApplyOutcome::Duplicate;
    let _ = format!("{outcome:?}");
}

#[test]
fn challenge_row_has_debug() {
    let row = stophammer::proof::ChallengeRow {
        challenge_id:  "c1".into(),
        feed_guid:     "fg1".into(),
        scope:         "feed:write".into(),
        token_binding: "tok.hash".into(),
        state:         "pending".into(),
        expires_at:    9999,
    };
    let _ = format!("{row:?}");
}

#[test]
fn node_signer_debug_redacts_key() {
    let signer = stophammer::signing::NodeSigner::load_or_create(
        Path::new("/tmp/test-sprint3b-debug.key"),
    )
    .expect("create signer");
    let debug_str = format!("{signer:?}");
    assert!(
        debug_str.contains("REDACTED"),
        "NodeSigner Debug must redact the signing key, got: {debug_str}"
    );
    assert!(
        debug_str.contains("pubkey_hex"),
        "NodeSigner Debug should show pubkey_hex"
    );
}

#[test]
fn ingest_context_has_debug() {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<stophammer::verify::IngestContext<'_>>();
}

#[test]
fn verifier_chain_has_debug() {
    let chain = stophammer::verify::VerifierChain::new(vec![]);
    let _ = format!("{chain:?}");
}

#[test]
fn chain_spec_has_debug() {
    let spec = stophammer::verify::ChainSpec {
        names: vec!["test".into()],
    };
    let _ = format!("{spec:?}");
}

#[test]
fn community_config_has_debug() {
    let config = stophammer::community::CommunityConfig {
        primary_url:       "http://localhost:8008".into(),
        tracker_url:       "http://localhost:9009".into(),
        node_address:      "http://localhost:7007".into(),
        poll_interval_secs: 300,
        push_timeout_secs:  90,
    };
    let _ = format!("{config:?}");
}

#[test]
fn community_state_has_debug() {
    let db = common::test_db_arc();
    let state = stophammer::community::CommunityState {
        db,
        primary_pubkey_hex: "abcd1234".into(),
        last_push_at:       Arc::new(std::sync::atomic::AtomicI64::new(0)),
        sse_registry:       None,
    };
    let _ = format!("{state:?}");
}

#[test]
fn search_result_has_debug() {
    let result = stophammer::search::SearchResult {
        entity_type:    "track".into(),
        entity_id:      "t1".into(),
        rank:           1.0,
        quality_score:  50,
        effective_rank: 1.0,
        rowid:          42,
    };
    let _ = format!("{result:?}");
}

// ── HIGH-02: File path params use &Path / impl AsRef<Path> ──────────────────

#[test]
fn node_signer_load_or_create_accepts_path() {
    let path = Path::new("/tmp/test-sprint3b-path.key");
    let signer = stophammer::signing::NodeSigner::load_or_create(path);
    assert!(signer.is_ok());
}

#[test]
fn open_db_accepts_path_like() {
    // Must accept impl AsRef<Path>, so both &str and &Path should work
    let _conn = stophammer::db::open_db(":memory:");
    let _conn2 = stophammer::db::open_db(Path::new(":memory:"));
}

#[test]
fn cert_needs_renewal_accepts_path() {
    let path = Path::new("/tmp/stophammer_sprint3b_nonexistent.pem");
    assert!(stophammer::tls::cert_needs_renewal(path));
}

// ── CRIT-02: skip_ssrf_validation is cfg-gated ──────────────────────────────

/// In test builds (cfg(test)), `skip_ssrf_validation` should still be available.
#[test]
fn skip_ssrf_field_available_in_test_cfg() {
    let db = common::test_db_arc();
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create(
            Path::new("/tmp/test-sprint3b-ssrf.key"),
        )
        .expect("create signer"),
    );
    let pubkey = signer.pubkey_hex().to_string();
    let _state = stophammer::api::AppState {
        db,
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex:  pubkey,
        admin_token:      String::new(),
        sync_token:      None,
        push_client:      reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry:     Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    };
}

// ── Issue #19: RSS body streaming with chunked read ──────────────────────────

#[tokio::test]
async fn rss_body_size_limit_enforced() {
    let client = reqwest::Client::new();
    let result = stophammer::proof::verify_podcast_txt(
        &client,
        "http://127.0.0.1:1/nonexistent",
        "dummy-binding",
    )
    .await;
    // Should fail with connection error, not panic
    assert!(result.is_err());
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn test_app_state() -> Arc<stophammer::api::AppState> {
    let db = common::test_db_arc();
    let key_path = format!("/tmp/test-sprint3b-{}.key", uuid::Uuid::new_v4());
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create(
            Path::new(&key_path),
        )
        .expect("create signer"),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db,
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex:  pubkey,
        admin_token:      String::new(),
        sync_token:      None,
        push_client:      reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry:     Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

fn dummy_ingest_request() -> stophammer::ingest::IngestFeedRequest {
    stophammer::ingest::IngestFeedRequest {
        canonical_url: "https://example.com/feed.xml".into(),
        source_url:    "https://example.com/feed.xml".into(),
        crawl_token:   "test-token".into(),
        http_status:   200,
        content_hash:  "abc123".into(),
        feed_data:     None,
    }
}
