// Issue-SYNC-SSRF — 2026-03-16
//
// POST /sync/register must reject node_url values that target private,
// loopback, or link-local addresses, and must reject non-HTTP schemes.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Build an `AppState` with SSRF validation **enabled** (skip_ssrf_validation = false).
fn state_with_ssrf_enabled(
    db: Arc<Mutex<rusqlite::Connection>>,
) -> Arc<stophammer::api::AppState> {
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create("/tmp/test-sync-ssrf.key")
            .expect("create signer"),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db: stophammer::db_pool::DbPool::from_writer_only(db),
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex: pubkey,
        admin_token: "test-token".into(),
        sync_token: None,
        push_client: reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry: Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: false,
    })
}

/// Build an `AppState` with SSRF validation **disabled** (for acceptance tests
/// where the public hostname may not resolve in CI).
fn state_with_ssrf_disabled(
    db: Arc<Mutex<rusqlite::Connection>>,
) -> Arc<stophammer::api::AppState> {
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create("/tmp/test-sync-ssrf-accept.key")
            .expect("create signer"),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db: stophammer::db_pool::DbPool::from_writer_only(db),
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex: pubkey,
        admin_token: "test-token".into(),
        sync_token: None,
        push_client: reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry: Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

fn register_body(node_url: &str) -> serde_json::Value {
    serde_json::json!({
        "node_pubkey": "deadbeef01234567890abcdef01234567890abcdef01234567890abcdef012345",
        "node_url":    node_url
    })
}

async fn post_register(
    app: axum::Router,
    node_url: &str,
) -> http::Response<axum::body::Body> {
    let body = register_body(node_url);
    let req = Request::builder()
        .method("POST")
        .uri("/sync/register")
        .header("Content-Type", "application/json")
        .header("X-Admin-Token", "test-token")
        .body(axum::body::Body::from(serde_json::to_vec(&body).expect("serialize")))
        .expect("build request");
    app.oneshot(req).await.expect("call handler")
}

// ── Integration tests: rejected URLs ─────────────────────────────────────────

#[tokio::test]
async fn register_loopback_rejected() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_enabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "http://127.0.0.1:8080/events").await;
    assert_eq!(resp.status(), 422, "loopback IPv4 must be rejected");
}

#[tokio::test]
async fn register_private_range_rejected() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_enabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "http://10.0.0.1:8080/events").await;
    assert_eq!(resp.status(), 422, "10.x private range must be rejected");
}

#[tokio::test]
async fn register_link_local_rejected() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_enabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "http://169.254.1.1:8080/events").await;
    assert_eq!(resp.status(), 422, "link-local address must be rejected");
}

#[tokio::test]
async fn register_ipv6_loopback_rejected() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_enabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "http://[::1]:8080/events").await;
    assert_eq!(resp.status(), 422, "IPv6 loopback must be rejected");
}

#[tokio::test]
async fn register_ftp_scheme_rejected() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_enabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "ftp://example.com/events").await;
    assert_eq!(resp.status(), 422, "non-HTTP scheme must be rejected");
}

// ── Integration tests: accepted URLs ─────────────────────────────────────────
// These use skip_ssrf_validation: true to avoid DNS dependency in CI,
// but still exercise the full handler path.

#[tokio::test]
async fn register_http_public_accepted() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_disabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "http://example.com/events").await;
    assert_eq!(resp.status(), 200, "public HTTP URL must be accepted");

    let bytes = resp.into_body().collect().await.expect("read body").to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("parse json");
    assert_eq!(json["ok"], true);
}

#[tokio::test]
async fn register_https_public_accepted() {
    let db = common::test_db_arc();
    let state = state_with_ssrf_disabled(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = post_register(app, "https://example.com/events").await;
    assert_eq!(resp.status(), 200, "public HTTPS URL must be accepted");

    let bytes = resp.into_body().collect().await.expect("read body").to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("parse json");
    assert_eq!(json["ok"], true);
}

// ── Unit tests: validate_node_url directly ───────────────────────────────────

#[test]
fn validate_node_url_rejects_loopback() {
    let result = stophammer::proof::validate_node_url("http://127.0.0.1:8080/events");
    assert!(result.is_err(), "loopback must be rejected");
}

#[test]
fn validate_node_url_rejects_private_10() {
    let result = stophammer::proof::validate_node_url("http://10.0.0.1:8080/events");
    assert!(result.is_err(), "10.x private must be rejected");
}

#[test]
fn validate_node_url_rejects_private_172() {
    let result = stophammer::proof::validate_node_url("http://172.16.0.1:8080/events");
    assert!(result.is_err(), "172.16.x private must be rejected");
}

#[test]
fn validate_node_url_rejects_private_192() {
    let result = stophammer::proof::validate_node_url("http://192.168.1.1:8080/events");
    assert!(result.is_err(), "192.168.x private must be rejected");
}

#[test]
fn validate_node_url_rejects_link_local() {
    let result = stophammer::proof::validate_node_url("http://169.254.1.1:8080/events");
    assert!(result.is_err(), "link-local must be rejected");
}

#[test]
fn validate_node_url_rejects_ipv6_loopback() {
    let result = stophammer::proof::validate_node_url("http://[::1]:8080/events");
    assert!(result.is_err(), "IPv6 loopback must be rejected");
}

#[test]
fn validate_node_url_rejects_ftp_scheme() {
    let result = stophammer::proof::validate_node_url("ftp://example.com/events");
    assert!(result.is_err(), "ftp scheme must be rejected");
}

#[test]
fn validate_node_url_rejects_file_scheme() {
    let result = stophammer::proof::validate_node_url("file:///etc/passwd");
    assert!(result.is_err(), "file scheme must be rejected");
}

#[test]
fn validate_node_url_rejects_unspecified() {
    let result = stophammer::proof::validate_node_url("http://0.0.0.0:8080/events");
    assert!(result.is_err(), "unspecified address must be rejected");
}

#[test]
fn validate_node_url_accepts_http_public_ip() {
    // 93.184.216.34 is example.com's IP — a clearly public address.
    let result = stophammer::proof::validate_node_url("http://93.184.216.34:8080/events");
    assert!(result.is_ok(), "public IP must be accepted");
}

#[test]
fn validate_node_url_accepts_https_public_ip() {
    let result = stophammer::proof::validate_node_url("https://93.184.216.34:8080/events");
    assert!(result.is_ok(), "public IP over HTTPS must be accepted");
}

// ── Unit tests: is_url_ssrf_safe directly ────────────────────────────────────

#[test]
fn is_url_ssrf_safe_rejects_private_ip() {
    let url = url::Url::parse("http://10.0.0.1:8080/events").expect("parse");
    assert!(!stophammer::proof::is_url_ssrf_safe(&url));
}

#[test]
fn is_url_ssrf_safe_rejects_loopback() {
    let url = url::Url::parse("http://127.0.0.1:8080/events").expect("parse");
    assert!(!stophammer::proof::is_url_ssrf_safe(&url));
}

#[test]
fn is_url_ssrf_safe_rejects_ftp() {
    let url = url::Url::parse("ftp://example.com/events").expect("parse");
    assert!(!stophammer::proof::is_url_ssrf_safe(&url));
}

#[test]
fn is_url_ssrf_safe_accepts_public_ip() {
    let url = url::Url::parse("http://93.184.216.34:8080/events").expect("parse");
    assert!(stophammer::proof::is_url_ssrf_safe(&url));
}
