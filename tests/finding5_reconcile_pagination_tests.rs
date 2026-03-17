// Finding-5 reconcile pagination — 2026-03-13
//
// Reconcile must return `has_more` and `next_seq` so callers can paginate.
// `get_event_refs_since` must be bounded to prevent unbounded memory usage.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use http::Request;
use http_body_util::BodyExt;
use tower::ServiceExt;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn test_app_state(db: Arc<Mutex<rusqlite::Connection>>) -> Arc<stophammer::api::AppState> {
    let signer = Arc::new(
        stophammer::signing::NodeSigner::load_or_create("/tmp/test-finding5-reconcile.key")
            .expect("create signer"),
    );
    let pubkey = signer.pubkey_hex().to_string();
    Arc::new(stophammer::api::AppState {
        db: stophammer::db_pool::DbPool::from_writer_only(db),
        chain: Arc::new(stophammer::verify::VerifierChain::new(vec![])),
        signer,
        node_pubkey_hex:  pubkey,
        admin_token:      "test-admin-token".into(),
        sync_token:       None,
        push_client:      reqwest::Client::new(),
        push_subscribers: Arc::new(RwLock::new(HashMap::new())),
        sse_registry:     Arc::new(stophammer::api::SseRegistry::new()),
        skip_ssrf_validation: true,
    })
}

fn json_request(method: &str, uri: &str, body: &serde_json::Value) -> Request<axum::body::Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("Content-Type", "application/json")
        // Issue-RECONCILE-AUTH — 2026-03-16: reconcile now requires auth.
        .header("X-Admin-Token", "test-admin-token")
        .body(axum::body::Body::from(serde_json::to_vec(body).expect("serialize")))
        .expect("build request")
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let bytes = resp.into_body().collect().await.expect("read body").to_bytes();
    serde_json::from_slice(&bytes).expect("parse json")
}

/// Insert a dummy `ArtistUpserted` event into the events table.
// Issue-SEQ-INTEGRITY — 2026-03-14: pass signer instead of signed_by/signature.
fn insert_dummy_event(conn: &rusqlite::Connection, event_id: &str, created_at: i64) -> i64 {
    let payload_json = r#"{"artist":{"artist_id":"a-1","name":"Test","name_lower":"test","created_at":0,"updated_at":0}}"#;
    let signer = stophammer::signing::NodeSigner::load_or_create("/tmp/finding5-test.key")
        .expect("signer");
    let (seq, _, _) = stophammer::db::insert_event(
        conn,
        event_id,
        &stophammer::event::EventType::ArtistUpserted,
        payload_json,
        "a-1",
        &signer,
        created_at,
        &[],
    )
    .expect("insert event");
    seq
}

// ── Test: ReconcileResponse includes has_more and next_seq fields ─────────

#[tokio::test]
async fn reconcile_response_has_pagination_fields() {
    let db = common::test_db_arc();

    // Insert a few events so reconcile has data
    {
        let conn = db.lock().expect("lock db");
        for i in 0..5 {
            insert_dummy_event(&conn, &format!("evt-{i}"), 1_700_000_000 + i);
        }
    }

    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = app
        .oneshot(json_request(
            "POST",
            "/sync/reconcile",
            &serde_json::json!({
                "node_pubkey": "test-pubkey",
                "have": [],
                "since_seq": 0,
            }),
        ))
        .await
        .expect("call handler");

    let status = resp.status();
    let json = body_json(resp).await;
    assert_eq!(status, 200, "reconcile failed: {json}");

    // Must contain pagination fields
    assert!(
        json.get("has_more").is_some(),
        "ReconcileResponse must include 'has_more' field"
    );
    assert!(
        json.get("next_seq").is_some(),
        "ReconcileResponse must include 'next_seq' field"
    );
}

// ── Test: has_more=false when all events fit within the limit ─────────────

#[tokio::test]
async fn reconcile_has_more_false_when_all_events_fit() {
    let db = common::test_db_arc();

    {
        let conn = db.lock().expect("lock db");
        for i in 0..5 {
            insert_dummy_event(&conn, &format!("small-{i}"), 1_700_000_000 + i);
        }
    }

    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = app
        .oneshot(json_request(
            "POST",
            "/sync/reconcile",
            &serde_json::json!({
                "node_pubkey": "test-pubkey",
                "have": [],
                "since_seq": 0,
            }),
        ))
        .await
        .expect("call handler");

    assert_eq!(resp.status(), 200);
    let json = body_json(resp).await;

    assert_eq!(
        json["has_more"], false,
        "has_more should be false when all events fit"
    );
    // next_seq should be the last event's seq
    assert!(
        json["next_seq"].as_i64().expect("next_seq is i64") > 0,
        "next_seq should reflect the last event sequence"
    );
}

// ── Test: next_seq equals since_seq when no events exist ─────────────────

#[tokio::test]
async fn reconcile_next_seq_equals_since_seq_when_no_events() {
    let db = common::test_db_arc();
    let state = test_app_state(Arc::clone(&db));
    let app = stophammer::api::build_router(state);

    let resp = app
        .oneshot(json_request(
            "POST",
            "/sync/reconcile",
            &serde_json::json!({
                "node_pubkey": "test-pubkey",
                "have": [],
                "since_seq": 42,
            }),
        ))
        .await
        .expect("call handler");

    assert_eq!(resp.status(), 200);
    let json = body_json(resp).await;

    assert_eq!(
        json["has_more"], false,
        "has_more should be false when there are no events"
    );
    assert_eq!(
        json["next_seq"].as_i64().expect("next_seq is i64"),
        42,
        "next_seq should equal since_seq when there are no events"
    );
}

// ── Test: get_event_refs_since accepts a limit and returns truncation flag ─

#[test]
fn get_event_refs_since_respects_limit() {
    let conn = common::test_db();

    // Insert 20 events
    for i in 0..20 {
        insert_dummy_event(&conn, &format!("ref-{i}"), 1_700_000_000 + i);
    }

    // Request with a small limit
    let (refs, truncated) = stophammer::db::get_event_refs_since(&conn, 0, 10)
        .expect("get_event_refs_since");

    assert_eq!(refs.len(), 10, "should return at most the limit");
    assert!(truncated, "should indicate truncation when there are more rows");
}

#[test]
fn get_event_refs_since_not_truncated_when_all_fit() {
    let conn = common::test_db();

    // Insert 5 events
    for i in 0..5 {
        insert_dummy_event(&conn, &format!("fit-{i}"), 1_700_000_000 + i);
    }

    let (refs, truncated) = stophammer::db::get_event_refs_since(&conn, 0, 100)
        .expect("get_event_refs_since");

    assert_eq!(refs.len(), 5, "should return all events");
    assert!(!truncated, "should not indicate truncation when all rows fit");
}
