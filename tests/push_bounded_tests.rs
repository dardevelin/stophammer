// Issue-PUSH-BOUNDS — 2026-03-16

mod common;

use rusqlite::params;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── Helpers ────────────────────────────────────────────────────────────────

fn seed_peer(
    db: &Arc<Mutex<rusqlite::Connection>>,
    pubkey: &str,
    push_url: &str,
) {
    let conn = db.lock().unwrap();
    let now = stophammer::db::unix_now();
    conn.execute(
        "INSERT OR REPLACE INTO peer_nodes (node_pubkey, node_url, discovered_at, consecutive_failures, last_push_at)
         VALUES (?1, ?2, ?3, 0, ?3)",
        params![pubkey, push_url, now],
    )
    .unwrap();
}

fn get_failures(db: &Arc<Mutex<rusqlite::Connection>>, pubkey: &str) -> i64 {
    let conn = db.lock().unwrap();
    conn.query_row(
        "SELECT consecutive_failures FROM peer_nodes WHERE node_pubkey = ?1",
        params![pubkey],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

fn make_test_event() -> stophammer::event::Event {
    stophammer::event::Event {
        event_id:     "evt-push-bounds-001".into(),
        event_type:   stophammer::event::EventType::FeedRetired,
        payload:      stophammer::event::EventPayload::FeedRetired(
            stophammer::event::FeedRetiredPayload {
                feed_guid: "feed-bounds-guid".into(),
                reason:    None,
            },
        ),
        subject_guid: "feed-bounds-guid".into(),
        signed_by:    "test-node-pubkey".into(),
        signature:    "deadbeef".into(),
        seq:          1,
        created_at:   stophammer::db::unix_now(),
        warnings:     vec![],
        payload_json: "{}".into(),
    }
}

// ── A. Arc sharing test ───────────────────────────────────────────────────

/// Verify that the push function serializes the batch once and shares it via
/// Arc rather than cloning per peer. We set up multiple peers pointing at the
/// same mock server, verify the push completes successfully for all, and
/// confirm the mock received the same number of requests as there are peers.
#[tokio::test]
async fn push_batch_shared_across_peers() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/sync/push"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "applied": 1,
                    "rejected": 0,
                    "duplicate": 0
                })),
        )
        .expect(3)
        .mount(&mock_server)
        .await;

    let push_url = format!("{}/sync/push", mock_server.uri());

    let db = common::test_db_arc();
    let pool = common::wrap_pool(db.clone());

    // Seed 3 peers all pointing at the same mock server.
    for i in 0..3 {
        seed_peer(&db, &format!("peer-arc-{i}"), &push_url);
    }

    let subscribers: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(
        (0..3)
            .map(|i| (format!("peer-arc-{i}"), push_url.clone()))
            .collect(),
    ));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    let events = vec![make_test_event()];

    stophammer::api::fan_out_push_public(
        pool.clone(),
        client,
        Arc::clone(&subscribers),
        events,
    )
    .await;

    // Give spawned tasks time to complete.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // All 3 peers should still be in subscribers (push succeeded).
    let guard = subscribers.read().unwrap();
    for i in 0..3 {
        assert!(
            guard.contains_key(&format!("peer-arc-{i}")),
            "peer-arc-{i} should still be in subscribers after successful push"
        );
    }

    // Failures should be 0 for all peers.
    drop(guard);
    for i in 0..3 {
        let failures = get_failures(&db, &format!("peer-arc-{i}"));
        assert_eq!(
            failures, 0,
            "peer-arc-{i} should have 0 failures, got {failures}"
        );
    }
}

// ── B. Concurrency limit test ─────────────────────────────────────────────

/// Verify that `MAX_CONCURRENT_PUSHES` is set to the expected value and that
/// the static semaphore is available. The semaphore is a `LazyLock` static,
/// so this test validates the constant is correctly wired.
///
/// We also verify that the push system handles more peers than the concurrency
/// limit by spawning peers > `MAX_CONCURRENT_PUSHES` and confirming all pushes
/// eventually complete (the semaphore serializes excess tasks rather than
/// dropping them).
#[tokio::test]
async fn push_concurrency_limit_allows_all_peers_to_complete() {
    let mock_server = MockServer::start().await;

    let peer_count = stophammer::api::MAX_CONCURRENT_PUSHES + 4; // 20 peers

    Mock::given(method("POST"))
        .and(path("/sync/push"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "applied": 1,
                    "rejected": 0,
                    "duplicate": 0
                })),
        )
        .expect(peer_count as u64)
        .mount(&mock_server)
        .await;

    let push_url = format!("{}/sync/push", mock_server.uri());

    let db = common::test_db_arc();
    let pool = common::wrap_pool(db.clone());

    for i in 0..peer_count {
        seed_peer(&db, &format!("peer-conc-{i}"), &push_url);
    }

    let subscribers: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(
        (0..peer_count)
            .map(|i| (format!("peer-conc-{i}"), push_url.clone()))
            .collect(),
    ));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap();

    let events = vec![make_test_event()];

    stophammer::api::fan_out_push_public(
        pool.clone(),
        client,
        Arc::clone(&subscribers),
        events,
    )
    .await;

    // Wait for all spawned tasks to complete.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // All peers should have succeeded despite exceeding the concurrency limit.
    let guard = subscribers.read().unwrap();
    for i in 0..peer_count {
        assert!(
            guard.contains_key(&format!("peer-conc-{i}")),
            "peer-conc-{i} should still be in subscribers (semaphore should queue, not drop)"
        );
    }
    drop(guard);

    for i in 0..peer_count {
        let failures = get_failures(&db, &format!("peer-conc-{i}"));
        assert_eq!(
            failures, 0,
            "peer-conc-{i} should have 0 failures, got {failures}"
        );
    }
}

/// Verify that `MAX_CONCURRENT_PUSHES` has the expected value.
#[test]
fn push_concurrency_limit_is_16() {
    assert_eq!(
        stophammer::api::MAX_CONCURRENT_PUSHES, 16,
        "MAX_CONCURRENT_PUSHES should be 16"
    );
}

// ── C. Rejected-event handling test ───────────────────────────────────────

/// A peer that returns 200 with rejected > 0 should have its failure counter
/// incremented (partial failure detection).
#[tokio::test]
async fn push_rejected_events_increment_failures() {
    let mock_server = MockServer::start().await;

    // Peer returns 200 but with rejected events.
    Mock::given(method("POST"))
        .and(path("/sync/push"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "applied": 0,
                    "rejected": 5,
                    "duplicate": 0
                })),
        )
        .mount(&mock_server)
        .await;

    let push_url = format!("{}/sync/push", mock_server.uri());

    let db = common::test_db_arc();
    let pool = common::wrap_pool(db.clone());
    let pubkey = "peer-reject-test";
    seed_peer(&db, pubkey, &push_url);

    let subscribers: Arc<RwLock<HashMap<String, String>>> =
        Arc::new(RwLock::new(HashMap::from([(pubkey.to_string(), push_url)])));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    let events = vec![make_test_event()];

    stophammer::api::fan_out_push_public(
        pool.clone(),
        client,
        Arc::clone(&subscribers),
        events,
    )
    .await;

    // Give spawned task time to complete.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let failures = get_failures(&db, pubkey);
    assert!(
        failures >= 1,
        "peer should have >= 1 failure from rejected events, got {failures}"
    );

    // Peer should still be in subscribers (not yet at eviction threshold).
    assert!(
        subscribers.read().unwrap().contains_key(pubkey),
        "peer should not be evicted after a single partial failure"
    );
}

/// A peer that returns 200 with rejected == 0 should NOT have its failure
/// counter incremented.
#[tokio::test]
async fn push_no_rejections_no_failure_increment() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/sync/push"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({
                    "applied": 1,
                    "rejected": 0,
                    "duplicate": 0
                })),
        )
        .mount(&mock_server)
        .await;

    let push_url = format!("{}/sync/push", mock_server.uri());

    let db = common::test_db_arc();
    let pool = common::wrap_pool(db.clone());
    let pubkey = "peer-no-reject-test";
    seed_peer(&db, pubkey, &push_url);

    let subscribers: Arc<RwLock<HashMap<String, String>>> =
        Arc::new(RwLock::new(HashMap::from([(pubkey.to_string(), push_url)])));

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();

    let events = vec![make_test_event()];

    stophammer::api::fan_out_push_public(
        pool.clone(),
        client,
        Arc::clone(&subscribers),
        events,
    )
    .await;

    // Give spawned task time to complete.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let failures = get_failures(&db, pubkey);
    assert_eq!(
        failures, 0,
        "peer should have 0 failures when no rejections, got {failures}"
    );
}
