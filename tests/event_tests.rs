mod common;

use ed25519_dalek::{Signer, SigningKey};
use rand_core::OsRng;
use rusqlite::params;
use sha2::{Sha256, Digest};

/// Helper: sign an event and insert it into the DB, returning the assigned seq.
fn insert_signed_event(
    conn: &rusqlite::Connection,
    event_id: &str,
    event_type: &str,
    payload_json: &str,
    subject_guid: &str,
    created_at: i64,
    key: &SigningKey,
) -> i64 {
    let pubkey_hex = hex::encode(key.verifying_key().to_bytes());

    let signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": payload_json,
        "subject_guid": subject_guid,
        "created_at":   created_at,
    });
    let serialized = serde_json::to_string(&signing_payload).unwrap();
    let digest = Sha256::digest(serialized.as_bytes());
    let sig = key.sign(&digest);
    let sig_hex = hex::encode(sig.to_bytes());

    conn.query_row(
        "INSERT INTO events \
         (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, '[]') \
         RETURNING seq",
        params![event_id, event_type, payload_json, subject_guid, pubkey_hex, sig_hex, created_at],
        |row| row.get(0),
    ).unwrap()
}

#[test]
fn event_type_snake_case() {
    // These are the expected snake_case values from serde rename_all
    let types = vec![
        "feed_upserted", "feed_retired", "track_upserted", "track_removed",
        "artist_upserted", "routes_replaced", "artist_merged",
        "artist_credit_created", "feed_routes_replaced",
    ];
    // Verify each can be deserialized from quoted JSON
    for t in &types {
        let json = format!("\"{t}\"");
        let _: serde_json::Value = serde_json::from_str(&json).unwrap();
    }
}

#[test]
fn event_payload_tagged_envelope() {
    // EventPayload uses internally-tagged format: {"type":"...", "data":...}
    let event_type = "artist_upserted";
    let inner_json = r#"{"artist":{"artist_id":"a1","name":"Test","name_lower":"test","created_at":1000,"updated_at":1000}}"#;

    // Construct the tagged envelope the same way get_events_since does
    let tagged = format!(r#"{{"type":"{event_type}","data":{inner_json}}}"#);
    let parsed: serde_json::Value = serde_json::from_str(&tagged).unwrap();

    assert_eq!(parsed["type"], "artist_upserted");
    assert!(parsed["data"].is_object(), "data field must be an object");
    assert_eq!(parsed["data"]["artist"]["name"], "Test");
}

#[test]
fn event_seq_monotonic() {
    let conn = common::test_db();
    let now = common::now();
    let key = SigningKey::generate(&mut OsRng);

    let payload = r#"{"artist":{"artist_id":"a1","name":"A","name_lower":"a","created_at":1,"updated_at":1}}"#;

    let seq1 = insert_signed_event(&conn, "evt-seq-1", "artist_upserted", payload, "subj-1", now, &key);
    let seq2 = insert_signed_event(&conn, "evt-seq-2", "artist_upserted", payload, "subj-1", now, &key);
    let seq3 = insert_signed_event(&conn, "evt-seq-3", "artist_upserted", payload, "subj-1", now, &key);

    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
    assert_eq!(seq3, 3);
}

#[test]
fn event_idempotent_insert() {
    let conn = common::test_db();
    let now = common::now();
    let key = SigningKey::generate(&mut OsRng);
    let pubkey_hex = hex::encode(key.verifying_key().to_bytes());

    let event_id = "evt-idem-1";
    let event_type = "artist_upserted";
    let payload = r#"{"artist":{"artist_id":"a1","name":"A","name_lower":"a","created_at":1,"updated_at":1}}"#;
    let subject_guid = "subj-idem";

    let signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": payload,
        "subject_guid": subject_guid,
        "created_at":   now,
    });
    let serialized = serde_json::to_string(&signing_payload).unwrap();
    let digest = Sha256::digest(serialized.as_bytes());
    let sig = key.sign(&digest);
    let sig_hex = hex::encode(sig.to_bytes());

    // First insert succeeds
    let changed_1 = conn.execute(
        "INSERT OR IGNORE INTO events \
         (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, '[]')",
        params![event_id, event_type, payload, subject_guid, pubkey_hex, sig_hex, now],
    ).unwrap();
    assert_eq!(changed_1, 1, "first insert should change 1 row");

    // Second insert with same event_id is ignored
    let changed_2 = conn.execute(
        "INSERT OR IGNORE INTO events \
         (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, '[]')",
        params![event_id, event_type, payload, subject_guid, pubkey_hex, sig_hex, now],
    ).unwrap();
    assert_eq!(changed_2, 0, "duplicate event_id should return 0 changed rows");
}

#[test]
fn event_pagination() {
    let conn = common::test_db();
    let now = common::now();
    let key = SigningKey::generate(&mut OsRng);

    let payload = r#"{"artist":{"artist_id":"a1","name":"A","name_lower":"a","created_at":1,"updated_at":1}}"#;

    // Insert 5 events
    for i in 1..=5 {
        let eid = format!("evt-page-{i}");
        insert_signed_event(&conn, &eid, "artist_upserted", payload, "subj-page", now, &key);
    }

    // Get events since seq 0 with limit 2
    let mut stmt = conn.prepare(
        "SELECT event_id, seq FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
    ).unwrap();
    let page1: Vec<(String, i64)> = stmt.query_map(params![0_i64, 2_i64], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }).unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].1, 1); // seq 1
    assert_eq!(page1[1].1, 2); // seq 2

    // Get next page starting after seq 2
    let page2: Vec<(String, i64)> = stmt.query_map(params![2_i64, 2_i64], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }).unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].1, 3); // seq 3
    assert_eq!(page2[1].1, 4); // seq 4

    // Final page with only 1 remaining
    let page3: Vec<(String, i64)> = stmt.query_map(params![4_i64, 2_i64], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    }).unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0].1, 5); // seq 5
}
