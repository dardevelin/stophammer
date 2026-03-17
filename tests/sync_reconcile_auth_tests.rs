// Issue-RECONCILE-AUTH — 2026-03-16
//
// POST /sync/reconcile must require the same SYNC_TOKEN / ADMIN_TOKEN
// authentication that POST /sync/register uses.  Without it, any
// unauthenticated caller can write to `node_sync_state` and force the
// endpoint onto the writer lock.
//
// After the fix, reconcile is also read-only (no upsert_node_sync_state),
// so we verify that no writes occur.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn test_app_state(
    db: Arc<Mutex<rusqlite::Connection>>,
    admin_token: &str,
    sync_token: Option<&str>,
) -> Arc<stophammer::api::AppState> {
    let key_dir = tempfile::tempdir().expect("create temp dir");
    let key_path = key_dir.path().join("test.key");
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create(
            key_path.to_str().expect("path to str"),
        )
        .expect("create signer"),
    );
    // Leak the tempdir so the key file persists for the signer's lifetime.
    std::mem::forget(key_dir);
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db: stophammer::db_pool::DbPool::from_writer_only(db),
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex:  pubkey,
        admin_token:      admin_token.into(),
        sync_token:       sync_token.map(String::from),
        push_client:      reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry:     Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

fn reconcile_body() -> serde_json::Value {
    serde_json::json!({
        "node_pubkey": "deadbeef01234567890abcdef01234567890abcdef01234567890abcdef012345",
        "have": [],
        "since_seq": 0,
    })
}

// ── Test 1: reconcile without auth token returns 403 ─────────────────────────

#[tokio::test]
async fn reconcile_without_auth_returns_403() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db), "admin-secret", None);
    let app = stophammer::api::build_router(state);

    let body = reconcile_body();
    let req = Request::builder()
        .method("POST")
        .uri("/sync/reconcile")
        .header("Content-Type", "application/json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("serialize"),
        ))
        .expect("build request");

    let resp = app.oneshot(req).await.expect("call handler");
    assert_eq!(
        resp.status(),
        403,
        "POST /sync/reconcile without auth must return 403"
    );
}

// ── Test 2: reconcile with wrong token returns 403 ───────────────────────────

#[tokio::test]
async fn reconcile_with_wrong_token_returns_403() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db), "admin-secret", Some("sync-secret"));
    let app = stophammer::api::build_router(state);

    let body = reconcile_body();
    let req = Request::builder()
        .method("POST")
        .uri("/sync/reconcile")
        .header("Content-Type", "application/json")
        .header("X-Sync-Token", "wrong-token")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("serialize"),
        ))
        .expect("build request");

    let resp = app.oneshot(req).await.expect("call handler");
    assert_eq!(
        resp.status(),
        403,
        "POST /sync/reconcile with wrong X-Sync-Token must return 403"
    );
}

// ── Test 3: reconcile with valid sync token returns 200 ──────────────────────

#[tokio::test]
async fn reconcile_with_valid_sync_token_returns_200() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db), "admin-secret", Some("sync-secret"));
    let app = stophammer::api::build_router(state);

    let body = reconcile_body();
    let req = Request::builder()
        .method("POST")
        .uri("/sync/reconcile")
        .header("Content-Type", "application/json")
        .header("X-Sync-Token", "sync-secret")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("serialize"),
        ))
        .expect("build request");

    let resp = app.oneshot(req).await.expect("call handler");
    assert_eq!(
        resp.status(),
        200,
        "POST /sync/reconcile with valid X-Sync-Token must return 200"
    );

    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("read body")
        .to_bytes();
    let json: serde_json::Value =
        serde_json::from_slice(&bytes).expect("parse json");
    assert!(
        json.get("send_to_node").is_some(),
        "response must include send_to_node field"
    );
}

// ── Test 4: reconcile with valid admin token (legacy fallback) returns 200 ───

#[tokio::test]
async fn reconcile_with_admin_token_legacy_returns_200() {
    let db = common::test_db_arc();
    // sync_token is None — falls back to admin_token.
    let state = test_app_state(Arc::clone(&db), "admin-secret", None);
    let app = stophammer::api::build_router(state);

    let body = reconcile_body();
    let req = Request::builder()
        .method("POST")
        .uri("/sync/reconcile")
        .header("Content-Type", "application/json")
        .header("X-Admin-Token", "admin-secret")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("serialize"),
        ))
        .expect("build request");

    let resp = app.oneshot(req).await.expect("call handler");
    assert_eq!(
        resp.status(),
        200,
        "POST /sync/reconcile with valid X-Admin-Token (legacy) must return 200"
    );
}

// ── Test 5: reconcile does not write to node_sync_state (read-only) ──────────

#[tokio::test]
async fn reconcile_does_not_write_node_sync_state() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db), "admin-secret", None);
    let app = stophammer::api::build_router(state);

    let body = serde_json::json!({
        "node_pubkey": "probe-pubkey-no-write",
        "have": [],
        "since_seq": 0,
    });
    let req = Request::builder()
        .method("POST")
        .uri("/sync/reconcile")
        .header("Content-Type", "application/json")
        .header("X-Admin-Token", "admin-secret")
        .body(axum::body::Body::from(
            serde_json::to_vec(&body).expect("serialize"),
        ))
        .expect("build request");

    let resp = app.oneshot(req).await.expect("call handler");
    assert_eq!(resp.status(), 200);

    // Verify no row was written for the probe pubkey.
    let conn = db.lock().expect("lock db");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM node_sync_state WHERE node_pubkey = ?1",
            rusqlite::params!["probe-pubkey-no-write"],
            |row| row.get(0),
        )
        .expect("query node_sync_state");

    assert_eq!(
        count, 0,
        "reconcile must not write to node_sync_state (read-only endpoint)"
    );
}
