mod common;

use ed25519_dalek::{Signer, SigningKey, VerifyingKey, Signature, Verifier};
use rand_core::OsRng;
use rusqlite::params;
use sha2::{Sha256, Digest};

#[test]
fn signing_key_roundtrip() {
    let key = SigningKey::generate(&mut OsRng);
    let path = "/tmp/stophammer-test-key";
    std::fs::write(path, key.to_bytes()).unwrap();
    let loaded_bytes = std::fs::read(path).unwrap();
    let loaded_arr: [u8; 32] = loaded_bytes.try_into().unwrap();
    let loaded = SigningKey::from_bytes(&loaded_arr);
    assert_eq!(
        hex::encode(key.verifying_key().to_bytes()),
        hex::encode(loaded.verifying_key().to_bytes()),
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn event_signature_stored_correctly() {
    let conn = common::test_db();
    let now = common::now();

    // Generate a signing key and sign a dummy event
    let key = SigningKey::generate(&mut OsRng);
    let pubkey_hex = hex::encode(key.verifying_key().to_bytes());

    let event_id = "evt-sig-store-1";
    let event_type = "artist_upserted";
    let payload_json = r#"{"artist":{"artist_id":"a1","name":"Test","name_lower":"test","created_at":1000,"updated_at":1000}}"#;
    let subject_guid = "subj-1";

    // Build the signing payload the same way NodeSigner does
    let signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": payload_json,
        "subject_guid": subject_guid,
        "created_at":   now,
    });
    let serialized = serde_json::to_string(&signing_payload).unwrap();
    let digest = Sha256::digest(serialized.as_bytes());
    let sig: Signature = key.sign(&digest);
    let sig_hex = hex::encode(sig.to_bytes());

    // Insert the event into the database
    conn.execute(
        "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, '[]')",
        params![event_id, event_type, payload_json, subject_guid, pubkey_hex, sig_hex, now],
    ).unwrap();

    // Read back and verify the signature column is hex-encoded and 128 chars (64 bytes)
    let stored_sig: String = conn.query_row(
        "SELECT signature FROM events WHERE event_id = ?1",
        params![event_id],
        |row| row.get(0),
    ).unwrap();

    assert_eq!(stored_sig.len(), 128, "signature hex should be 128 chars (64 bytes)");
    assert!(
        hex::decode(&stored_sig).is_ok(),
        "signature column should be valid hex",
    );
}

#[test]
fn wrong_pubkey_rejects() {
    let now = common::now();

    // Sign with key A
    let key_a = SigningKey::generate(&mut OsRng);
    let event_id = "evt-wrong-pk";
    let event_type = "artist_upserted";
    let payload_json = r#"{"artist":{"artist_id":"a2","name":"A","name_lower":"a","created_at":1,"updated_at":1}}"#;
    let subject_guid = "subj-2";

    let signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": payload_json,
        "subject_guid": subject_guid,
        "created_at":   now,
    });
    let serialized = serde_json::to_string(&signing_payload).unwrap();
    let digest = Sha256::digest(serialized.as_bytes());
    let sig: Signature = key_a.sign(&digest);

    // Attempt verification with key B (different key)
    let key_b = SigningKey::generate(&mut OsRng);
    let verifier_b: VerifyingKey = key_b.verifying_key();

    let result = verifier_b.verify(&digest, &sig);
    assert!(result.is_err(), "verification with wrong pubkey must fail");
}

#[test]
fn tampered_payload_rejects() {
    let now = common::now();

    let key = SigningKey::generate(&mut OsRng);
    let verifier: VerifyingKey = key.verifying_key();

    let event_id = "evt-tamper";
    let event_type = "artist_upserted";
    let original_payload = r#"{"artist":{"artist_id":"a3","name":"Original","name_lower":"original","created_at":1,"updated_at":1}}"#;
    let subject_guid = "subj-3";

    // Sign the original payload
    let signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": original_payload,
        "subject_guid": subject_guid,
        "created_at":   now,
    });
    let serialized = serde_json::to_string(&signing_payload).unwrap();
    let digest = Sha256::digest(serialized.as_bytes());
    let sig: Signature = key.sign(&digest);

    // Tamper with the payload_json
    let tampered_payload = r#"{"artist":{"artist_id":"a3","name":"Tampered","name_lower":"tampered","created_at":1,"updated_at":1}}"#;

    // Recompute digest with tampered content
    let tampered_signing_payload = serde_json::json!({
        "event_id":     event_id,
        "event_type":   event_type,
        "payload_json": tampered_payload,
        "subject_guid": subject_guid,
        "created_at":   now,
    });
    let tampered_serialized = serde_json::to_string(&tampered_signing_payload).unwrap();
    let tampered_digest = Sha256::digest(tampered_serialized.as_bytes());

    // Original signature must not verify against the tampered digest
    let result = verifier.verify(&tampered_digest, &sig);
    assert!(result.is_err(), "tampered payload must fail signature verification");
}
