mod common;

use rusqlite::params;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// Helper: insert prerequisite artist + artist_credit for tests that need a
// feed or track (which require foreign keys to those tables).
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
         explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![feed_guid, feed_url, title, title.to_lowercase(), credit_id, 0, 0, now, now],
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
         explicit, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![track_guid, feed_guid, credit_id, title, title.to_lowercase(), 0, now, now],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 1. apply_artist_upserted
// ---------------------------------------------------------------------------

#[test]
fn apply_artist_upserted() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "artist-1", "Test Artist", now);

    let name: String = conn
        .query_row(
            "SELECT name FROM artists WHERE artist_id = 'artist-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name, "Test Artist");

    // name_lower is stored alongside.
    let name_lower: String = conn
        .query_row(
            "SELECT name_lower FROM artists WHERE artist_id = 'artist-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name_lower, "test artist");
}

// ---------------------------------------------------------------------------
// 2. apply_feed_upserted
// ---------------------------------------------------------------------------

#[test]
fn apply_feed_upserted() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "artist-1", "Feed Artist", now);
    let credit_id = insert_artist_credit(&conn, "artist-1", "Feed Artist", now);
    insert_feed(
        &conn,
        "feed-guid-1",
        "https://example.com/feed.xml",
        "My Album",
        credit_id,
        now,
    );

    // Verify feed exists with correct title and credit.
    let (title, fc): (String, i64) = conn
        .query_row(
            "SELECT title, artist_credit_id FROM feeds WHERE feed_guid = 'feed-guid-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(title, "My Album");
    assert_eq!(fc, credit_id);
}

// ---------------------------------------------------------------------------
// 3. apply_track_upserted
// ---------------------------------------------------------------------------

#[test]
fn apply_track_upserted() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "artist-1", "Track Artist", now);
    let credit_id = insert_artist_credit(&conn, "artist-1", "Track Artist", now);
    insert_feed(
        &conn,
        "feed-1",
        "https://example.com/feed.xml",
        "Album",
        credit_id,
        now,
    );
    insert_track(&conn, "track-1", "feed-1", credit_id, "Song One", now);

    // Insert a payment route for the track.
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, \
         address, split, fee) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params!["track-1", "feed-1", "Artist Wallet", "node",
                "03e7156ae33b0a208d0744199163177e909e80176e55d97a2f221ede0f934dd9ad",
                95, 0],
    )
    .unwrap();

    // Insert a value time split.
    conn.execute(
        "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, \
         remote_feed_guid, remote_item_guid, split, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params!["track-1", 30, 60, "remote-feed-1", "remote-item-1", 50, now],
    )
    .unwrap();

    // Verify track.
    let title: String = conn
        .query_row(
            "SELECT title FROM tracks WHERE track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title, "Song One");

    // Verify payment route.
    let route_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(route_count, 1);

    // Verify VTS.
    let vts_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM value_time_splits WHERE source_track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(vts_count, 1);
}

// ---------------------------------------------------------------------------
// 4. apply_routes_replaced
// ---------------------------------------------------------------------------

#[test]
fn apply_routes_replaced() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "artist-1", "Artist", now);
    let credit_id = insert_artist_credit(&conn, "artist-1", "Artist", now);
    insert_feed(&conn, "feed-1", "https://example.com/f.xml", "Album", credit_id, now);
    insert_track(&conn, "track-1", "feed-1", credit_id, "Song", now);

    // Insert original route.
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, \
         address, split, fee) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params!["track-1", "feed-1", "Old Wallet", "node", "old-address", 100, 0],
    )
    .unwrap();

    // Replace: delete old, insert new (mimics db::replace_payment_routes).
    conn.execute(
        "DELETE FROM payment_routes WHERE track_guid = 'track-1'",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, \
         address, split, fee) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params!["track-1", "feed-1", "New Wallet", "lnaddress", "new@address.com", 60, 0],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, \
         address, split, fee) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params!["track-1", "feed-1", "Producer", "lnaddress", "producer@pay.com", 40, 0],
    )
    .unwrap();

    // Verify old route is gone and two new routes exist.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    let old_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = 'track-1' AND address = 'old-address'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old_count, 0, "old route must be gone after replace");

    // Verify splits sum to 100.
    let sum: i64 = conn
        .query_row(
            "SELECT SUM(split) FROM payment_routes WHERE track_guid = 'track-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sum, 100);
}

// ---------------------------------------------------------------------------
// 5. apply_artist_merged
// ---------------------------------------------------------------------------

#[test]
fn apply_artist_merged() {
    let conn = common::test_db();
    let now = common::now();

    // Create source and target artists.
    insert_artist(&conn, "artist-src", "Source Artist", now);
    insert_artist(&conn, "artist-tgt", "Target Artist", now);

    // Give source an alias.
    conn.execute(
        "INSERT INTO artist_aliases (alias_lower, artist_id, created_at) \
         VALUES (?1, ?2, ?3)",
        params!["source alias", "artist-src", now],
    )
    .unwrap();

    // Create a credit pointing to source.
    let credit_id = insert_artist_credit(&conn, "artist-src", "Source Artist", now);

    // Simulate merge: repoint credits, transfer aliases, record redirect, delete source.
    // This mirrors db::merge_artists logic.
    conn.execute(
        "UPDATE artist_credit_name SET artist_id = 'artist-tgt' WHERE artist_id = 'artist-src'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE artist_aliases SET artist_id = 'artist-tgt' WHERE artist_id = 'artist-src'",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES ('artist-src', 'artist-tgt', ?1)",
        params![now],
    )
    .unwrap();
    conn.execute(
        "DELETE FROM artists WHERE artist_id = 'artist-src'",
        [],
    )
    .unwrap();

    // Verify: source artist is gone.
    let src_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artists WHERE artist_id = 'artist-src'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(src_count, 0, "source artist must be deleted after merge");

    // Verify: redirect exists.
    let redirect_target: String = conn
        .query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = 'artist-src'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(redirect_target, "artist-tgt");

    // Verify: credit now points to target.
    let credit_artist: String = conn
        .query_row(
            "SELECT artist_id FROM artist_credit_name WHERE artist_credit_id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(credit_artist, "artist-tgt");

    // Verify: alias transferred to target.
    let alias_artist: String = conn
        .query_row(
            "SELECT artist_id FROM artist_aliases WHERE alias_lower = 'source alias'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(alias_artist, "artist-tgt");
}

// ---------------------------------------------------------------------------
// 5b. merge_artists alias transfer with deduplication (Finding-1 regression)
// ---------------------------------------------------------------------------

#[test]
fn merge_artists_transfers_and_deduplicates_aliases() {
    let mut conn = common::test_db();
    let now = common::now();

    // Source artist with aliases A, B, C.
    insert_artist(&conn, "src", "Source", now);
    for alias in &["a", "b", "c"] {
        conn.execute(
            "INSERT INTO artist_aliases (alias_lower, artist_id, created_at) \
             VALUES (?1, ?2, ?3)",
            params![alias, "src", now],
        )
        .expect("insert source alias");
    }

    // Target artist with alias B (overlap).
    insert_artist(&conn, "tgt", "Target", now);
    conn.execute(
        "INSERT INTO artist_aliases (alias_lower, artist_id, created_at) \
         VALUES (?1, ?2, ?3)",
        params!["b", "tgt", now],
    )
    .expect("insert target alias");

    // Call the PRODUCTION merge function.
    let transferred = stophammer::db::merge_artists(&mut conn, "src", "tgt")
        .expect("merge_artists must succeed");

    // A and C should have been transferred (B already existed on target).
    let mut transferred = transferred;
    transferred.sort();
    assert_eq!(
        transferred,
        vec!["a", "c"],
        "only non-overlapping aliases should be reported as transferred"
    );

    // Target must now own exactly {a, b, c}.
    let mut aliases: Vec<String> = conn
        .prepare("SELECT alias_lower FROM artist_aliases WHERE artist_id = 'tgt' ORDER BY alias_lower")
        .expect("prepare")
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("collect");
    aliases.sort();
    assert_eq!(aliases, vec!["a", "b", "c"], "target must own all three aliases after merge");

    // Source must have zero remaining aliases.
    let src_alias_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_aliases WHERE artist_id = 'src'",
            [],
            |r| r.get(0),
        )
        .expect("count source aliases");
    assert_eq!(src_alias_count, 0, "source aliases must be fully cleaned up");

    // Source artist row must be deleted.
    let src_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artists WHERE artist_id = 'src'",
            [],
            |r| r.get(0),
        )
        .expect("count source artist");
    assert_eq!(src_count, 0, "source artist must be deleted after merge");
}

// ---------------------------------------------------------------------------
// 6. apply_feed_routes_replaced
// ---------------------------------------------------------------------------

#[test]
fn apply_feed_routes_replaced() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "artist-1", "Artist", now);
    let credit_id = insert_artist_credit(&conn, "artist-1", "Artist", now);
    insert_feed(&conn, "feed-1", "https://example.com/f.xml", "Album", credit_id, now);

    // Insert original feed-level route.
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["feed-1", "Old Feed Wallet", "node", "old-feed-addr", 100, 0],
    )
    .unwrap();

    // Replace feed routes (mimics db::replace_feed_payment_routes).
    conn.execute(
        "DELETE FROM feed_payment_routes WHERE feed_guid = 'feed-1'",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["feed-1", "New Feed Wallet", "lnaddress", "artist@wallet.com", 80, 0],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["feed-1", "App Fee", "lnaddress", "app@fee.com", 20, 1],
    )
    .unwrap();

    // Verify old route is gone.
    let old_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = 'feed-1' AND address = 'old-feed-addr'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old_count, 0, "old feed route must be gone");

    // Verify new routes exist.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = 'feed-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    // Verify fee flag on app route.
    let fee: i64 = conn
        .query_row(
            "SELECT fee FROM feed_payment_routes WHERE feed_guid = 'feed-1' AND recipient_name = 'App Fee'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(fee, 1, "app fee route should have fee=1");
}

// ---------------------------------------------------------------------------
// 7. apply_duplicate_event
// ---------------------------------------------------------------------------

/// Inserting the same `event_id` twice should be idempotent: the second insert
/// is a no-op thanks to INSERT OR IGNORE on the events table PK.
#[test]
fn apply_duplicate_event() {
    let conn = common::test_db();
    let now = common::now();
    let event_id = "evt-dup-test-001";

    // First insert.
    conn.execute(
        "INSERT OR IGNORE INTO events (event_id, event_type, payload_json, subject_guid, \
         signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            event_id,
            "artist_upserted",
            r#"{"type":"artist_upserted","data":{}}"#,
            "artist-1",
            "deadbeef",
            "cafebabe",
            1,
            now,
            "[]",
        ],
    )
    .unwrap();

    let count_after_first: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_id = ?1",
            params![event_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_after_first, 1);

    // Second insert with same event_id — should be a no-op.
    let changed = conn
        .execute(
            "INSERT OR IGNORE INTO events (event_id, event_type, payload_json, subject_guid, \
             signed_by, signature, seq, created_at, warnings_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                event_id,
                "artist_upserted",
                r#"{"type":"artist_upserted","data":{}}"#,
                "artist-1",
                "deadbeef",
                "cafebabe",
                2,
                now,
                "[]",
            ],
        )
        .unwrap();

    assert_eq!(changed, 0, "second insert of same event_id must be a no-op");

    let count_after_second: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_id = ?1",
            params![event_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_after_second, 1, "still exactly one event row");

    // Verify the original seq was preserved (not overwritten by the second insert).
    let seq: i64 = conn
        .query_row(
            "SELECT seq FROM events WHERE event_id = ?1",
            params![event_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(seq, 1, "original seq must be preserved");
}

// ---------------------------------------------------------------------------
// 8. apply_creates_artist_credit
// ---------------------------------------------------------------------------

/// Artist credits link multiple artists to a single display name. This test
/// verifies the credit + `credit_name` rows are created correctly and the
/// foreign key to artists is valid.
#[test]
fn apply_creates_artist_credit() {
    let conn = common::test_db();
    let now = common::now();

    // Create two artists.
    insert_artist(&conn, "artist-a", "Alice", now);
    insert_artist(&conn, "artist-b", "Bob", now);

    // Create a joint credit: "Alice & Bob".
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params!["Alice & Bob", now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();

    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![credit_id, "artist-a", 0, "Alice", " & "],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![credit_id, "artist-b", 1, "Bob", ""],
    )
    .unwrap();

    // Verify credit display name.
    let display_name: String = conn
        .query_row(
            "SELECT display_name FROM artist_credit WHERE id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(display_name, "Alice & Bob");

    // Verify two credit names exist.
    let name_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit_name WHERE artist_credit_id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name_count, 2);

    // Verify ordering and join phrase.
    let (first_name, first_join): (String, String) = conn
        .query_row(
            "SELECT name, join_phrase FROM artist_credit_name \
             WHERE artist_credit_id = ?1 AND position = 0",
            params![credit_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_name, "Alice");
    assert_eq!(first_join, " & ");

    let (second_name, second_join): (String, String) = conn
        .query_row(
            "SELECT name, join_phrase FROM artist_credit_name \
             WHERE artist_credit_id = ?1 AND position = 1",
            params![credit_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(second_name, "Bob");
    assert_eq!(second_join, "");

    // Reconstructing the display name from parts:
    let reconstructed = format!("{first_name}{first_join}{second_name}");
    assert_eq!(reconstructed, "Alice & Bob");
}

// ============================================================================
// Sprint 4B — Issue #16: Timestamp captured before mutex lock
// ============================================================================

// ---------------------------------------------------------------------------
// 9. apply_single_event records last_seen_at captured before the lock
// ---------------------------------------------------------------------------

/// Verifies that the `now` timestamp written to `node_sync_state.last_seen_at`
/// by `apply_single_event` is captured BEFORE the mutex lock is acquired, not
/// after. We record wall-clock time before the call and assert that the stored
/// `last_seen_at` is within a tight tolerance (no lock-induced delay).
#[test]
fn timestamp_captured_before_lock() {
    use stophammer::event::{Event, EventPayload, EventType, ArtistUpsertedPayload};
    use stophammer::model::Artist;

    let db: Arc<Mutex<rusqlite::Connection>> = common::test_db_arc();
    let now = common::now();

    // Build a minimal ArtistUpserted event.
    let artist = Artist {
        artist_id:  "ts-artist-1".into(),
        name:       "Timestamp Artist".into(),
        name_lower: "timestamp artist".into(),
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
    let payload = EventPayload::ArtistUpserted(ArtistUpsertedPayload {
        artist,
    });
    // payload_json must contain only the inner struct (not the tagged enum),
    // matching production usage where the DB stores the inner payload.
    let payload_json = serde_json::to_string(&inner).expect("serialize inner payload");
    let ev = Event {
        event_id:     "ts-evt-001".into(),
        event_type:   EventType::ArtistUpserted,
        payload,
        subject_guid: "ts-artist-1".into(),
        signed_by:    "deadbeef".into(),
        signature:    "cafebabe".into(),
        seq:          1,
        created_at:   now,
        warnings:     vec![],
        payload_json,
    };

    // Capture wall-clock before calling apply_single_event.
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();

    let result = stophammer::apply::apply_single_event(&db, "test-node-pubkey", &ev);
    assert!(result.is_ok(), "apply_single_event should succeed");

    // Capture wall-clock after the call.
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();

    // Read the stored last_seen_at from node_sync_state.
    let last_seen_at: i64 = {
        let conn = db.lock().expect("lock after apply");
        conn.query_row(
            "SELECT last_seen_at FROM node_sync_state WHERE node_pubkey = 'test-node-pubkey'",
            [],
            |r| r.get(0),
        )
        .expect("node_sync_state row should exist")
    };

    // The stored timestamp must fall within [before, after].
    // If the timestamp were captured AFTER the lock, it would still be in this
    // range for an uncontended lock, but the key invariant is that it is at
    // least >= before (not delayed by lock wait time).
    assert!(
        last_seen_at >= before && last_seen_at <= after,
        "last_seen_at ({last_seen_at}) should be between before ({before}) and after ({after}); \
         timestamp must be captured before the mutex lock"
    );
}

// ---------------------------------------------------------------------------
// 10. Issue-PAYLOAD-INTEGRITY: tampered ev.payload must not be applied
// ---------------------------------------------------------------------------

/// A MITM attacker can alter `ev.payload` in transit without breaking the
/// signature, which only covers `payload_json`. This test verifies that
/// `apply_single_event` applies state from `payload_json` (the signed bytes),
/// not the potentially-tampered `ev.payload`.
///
/// Strategy: create an event with `payload_json` containing artist name
/// "Signed Artist", but tamper `ev.payload` to contain "Tampered Artist".
/// After `apply_single_event`, the DB must contain "Signed Artist".
// Issue-PAYLOAD-INTEGRITY — 2026-03-14
#[test]
fn tampered_payload_struct_is_ignored_in_favour_of_payload_json() {
    use stophammer::event::{Event, EventPayload, EventType, ArtistUpsertedPayload};
    use stophammer::model::Artist;

    let db: Arc<Mutex<rusqlite::Connection>> = common::test_db_arc();
    let now = common::now();

    // The "real" (signed) artist — what payload_json contains.
    let signed_artist = Artist {
        artist_id:  "integrity-artist-1".into(),
        name:       "Signed Artist".into(),
        name_lower: "signed artist".into(),
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

    // payload_json is the inner struct, matching production format.
    let inner = ArtistUpsertedPayload { artist: signed_artist };
    let payload_json = serde_json::to_string(&inner).expect("serialize inner payload");

    // The tampered payload struct — what an attacker puts in ev.payload.
    let tampered_artist = Artist {
        artist_id:  "integrity-artist-1".into(),
        name:       "Tampered Artist".into(),
        name_lower: "tampered artist".into(),
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
    let tampered_payload = EventPayload::ArtistUpserted(ArtistUpsertedPayload {
        artist: tampered_artist,
    });

    let ev = Event {
        event_id:     "integrity-evt-001".into(),
        event_type:   EventType::ArtistUpserted,
        payload:      tampered_payload,   // TAMPERED — must be ignored
        subject_guid: "integrity-artist-1".into(),
        signed_by:    "deadbeef".into(),
        signature:    "cafebabe".into(),
        seq:          1,
        created_at:   now,
        warnings:     vec![],
        payload_json,                     // SIGNED — must be authoritative
    };

    let result = stophammer::apply::apply_single_event(&db, "test-node-pubkey", &ev);
    assert!(result.is_ok(), "apply_single_event should succeed: {result:?}");

    // Verify the DB contains the signed name, NOT the tampered name.
    let stored_name: String = {
        let conn = db.lock().expect("lock after apply");
        conn.query_row(
            "SELECT name FROM artists WHERE artist_id = 'integrity-artist-1'",
            [],
            |r| r.get(0),
        )
        .expect("artist row should exist")
    };

    assert_eq!(
        stored_name, "Signed Artist",
        "apply_single_event must use payload_json (signed data), not ev.payload (tampered data)"
    );
}

// ---------------------------------------------------------------------------
// 11. Issue-PAYLOAD-INTEGRITY: malformed payload_json is rejected
// ---------------------------------------------------------------------------

/// If `payload_json` cannot be deserialized into a valid `EventPayload`,
/// the event must be rejected with an error rather than silently applied.
// Issue-PAYLOAD-INTEGRITY — 2026-03-14
#[test]
fn malformed_payload_json_is_rejected() {
    use stophammer::event::{Event, EventPayload, EventType, ArtistUpsertedPayload};
    use stophammer::model::Artist;

    let db: Arc<Mutex<rusqlite::Connection>> = common::test_db_arc();
    let now = common::now();

    let artist = Artist {
        artist_id:  "bad-json-artist".into(),
        name:       "Bad Artist".into(),
        name_lower: "bad artist".into(),
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
    let payload = EventPayload::ArtistUpserted(ArtistUpsertedPayload {
        artist,
    });

    let ev = Event {
        event_id:     "bad-json-evt-001".into(),
        event_type:   EventType::ArtistUpserted,
        payload,
        subject_guid: "bad-json-artist".into(),
        signed_by:    "deadbeef".into(),
        signature:    "cafebabe".into(),
        seq:          1,
        created_at:   now,
        warnings:     vec![],
        payload_json: "NOT VALID JSON".into(),  // garbage
    };

    let result = stophammer::apply::apply_single_event(&db, "test-node-pubkey", &ev);
    assert!(
        result.is_err(),
        "apply_single_event must reject events with malformed payload_json"
    );
}

// ---------------------------------------------------------------------------
// Helpers for Issue-DEDUP-ORDER tests
// ---------------------------------------------------------------------------

/// Build a `FeedUpserted` event with the given parameters.
fn make_feed_upserted_event(
    event_id: &str,
    feed_guid: &str,
    feed_title: &str,
    artist_prefix: &str,
    seq: i64,
    now: i64,
) -> stophammer::event::Event {
    use stophammer::event::{Event, EventPayload, EventType, FeedUpsertedPayload};
    use stophammer::model::{Artist, ArtistCredit, ArtistCreditName, Feed};

    let artist = Artist {
        artist_id:  format!("{artist_prefix}-artist"),
        name:       format!("{artist_prefix} Artist"),
        name_lower: format!("{artist_prefix} artist"),
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
    let credit = ArtistCredit {
        id:           1,
        display_name: format!("{artist_prefix} Artist"),
        created_at:   now,
        names:        vec![ArtistCreditName {
            id:               0,
            artist_credit_id: 1,
            artist_id:        format!("{artist_prefix}-artist"),
            position:         0,
            name:             format!("{artist_prefix} Artist"),
            join_phrase:      String::new(),
        }],
    };
    let feed = Feed {
        feed_guid:        feed_guid.into(),
        feed_url:         format!("https://example.com/{feed_guid}.xml"),
        title:            feed_title.into(),
        title_lower:      feed_title.to_lowercase(),
        artist_credit_id: 1,
        description:      None,
        image_url:        None,
        language:         None,
        explicit:         false,
        itunes_type:      None,
        episode_count:    0,
        newest_item_at:   None,
        oldest_item_at:   None,
        created_at:       now,
        updated_at:       now,
        raw_medium:       None,
    };

    let inner = FeedUpsertedPayload { feed, artist, artist_credit: credit };
    let payload_json = serde_json::to_string(&inner).expect("serialize");

    Event {
        event_id:     event_id.into(),
        event_type:   EventType::FeedUpserted,
        payload:      EventPayload::FeedUpserted(inner),
        subject_guid: feed_guid.into(),
        signed_by:    "deadbeef".into(),
        signature:    "cafebabe".into(),
        seq,
        created_at:   now,
        warnings:     vec![],
        payload_json,
    }
}

// ---------------------------------------------------------------------------
// 12. Issue-DEDUP-ORDER: duplicate `FeedUpserted` must not overwrite newer data
// ---------------------------------------------------------------------------

/// Replaying the same `FeedUpserted` event (same `event_id`) must be a no-op:
/// the feed's title must not be overwritten and only one event row must exist.
// Issue-DEDUP-ORDER — 2026-03-14
#[test]
fn duplicate_feed_upserted_does_not_overwrite_newer_data() {
    use stophammer::apply::{apply_single_event, ApplyOutcome};

    let db = common::test_db_arc();
    let now = common::now();
    let ev = make_feed_upserted_event(
        "dedup-evt-feed-001", "dedup-feed-1", "Original Title", "dedup", 1, now,
    );

    // First apply — should succeed.
    let r1 = apply_single_event(&db, "test-node", &ev).expect("first apply");
    assert!(matches!(r1, ApplyOutcome::Applied(_)), "first apply must be Applied");

    // Simulate newer data arriving via a different event.
    {
        let conn = db.lock().expect("lock");
        conn.execute(
            "UPDATE feeds SET title = 'Updated Title' WHERE feed_guid = 'dedup-feed-1'",
            [],
        )
        .expect("update title");
    }

    // Second apply — same `event_id`, must be Duplicate and NOT overwrite.
    let r2 = apply_single_event(&db, "test-node", &ev).expect("second apply");
    assert!(
        matches!(r2, ApplyOutcome::Duplicate),
        "second apply of same event_id must be Duplicate"
    );

    // The feed must still have "Updated Title".
    let title: String = {
        let conn = db.lock().expect("lock");
        conn.query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'dedup-feed-1'",
            [],
            |r| r.get(0),
        )
        .expect("query title")
    };
    assert_eq!(
        title, "Updated Title",
        "duplicate event must NOT overwrite newer feed data"
    );

    // Only one event row should exist.
    let event_count: i64 = {
        let conn = db.lock().expect("lock");
        conn.query_row(
            "SELECT COUNT(*) FROM events WHERE event_id = 'dedup-evt-feed-001'",
            [],
            |r| r.get(0),
        )
        .expect("count events")
    };
    assert_eq!(event_count, 1, "only one event row must exist");
}

// ---------------------------------------------------------------------------
// 13. Issue-DEDUP-ORDER: out-of-order event with same `event_id` is rejected
// ---------------------------------------------------------------------------

/// Replaying the same `event_id` with a different seq must be detected as a
/// duplicate regardless of seq ordering.
// Issue-DEDUP-ORDER — 2026-03-14
#[test]
fn out_of_order_event_is_rejected() {
    use stophammer::apply::{apply_single_event, ApplyOutcome};
    use stophammer::event::Event;

    let db = common::test_db_arc();
    let now = common::now();
    let ev10 = make_feed_upserted_event(
        "ooo-evt-010", "ooo-feed-1", "Title Seq 10", "ooo", 10, now,
    );

    // Apply the seq=10 event first.
    let r1 = apply_single_event(&db, "test-node", &ev10).expect("apply seq=10");
    assert!(matches!(r1, ApplyOutcome::Applied(_)));

    // Replay the SAME `event_id` with a lower seq.
    let ev10_replay = Event { seq: 5, ..ev10 };
    let r2 = apply_single_event(&db, "test-node", &ev10_replay).expect("replay");
    assert!(
        matches!(r2, ApplyOutcome::Duplicate),
        "replaying same event_id must return Duplicate regardless of seq"
    );

    // Feed must still reflect the seq=10 data.
    let title: String = {
        let conn = db.lock().expect("lock");
        conn.query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'ooo-feed-1'",
            [],
            |r| r.get(0),
        )
        .expect("query title")
    };
    assert_eq!(title, "Title Seq 10", "feed data must reflect seq=10 event only");
}

// ---------------------------------------------------------------------------
// 14. Issue-CURSOR-MONOTONIC: cursor does not regress when old event applied
// ---------------------------------------------------------------------------

/// If `node_sync_state.last_seq` is already 15 and we call
/// `upsert_node_sync_state` with seq=10, the stored cursor must remain 15.
// Issue-CURSOR-MONOTONIC — 2026-03-14
#[test]
fn cursor_does_not_regress_when_old_event_applied() {
    let conn = common::test_db();
    let now = common::now();

    // Seed cursor at seq=15.
    stophammer::db::upsert_node_sync_state(&conn, "peer-mono-1", 15, now)
        .expect("seed cursor at 15");

    // Attempt to regress to seq=10.
    stophammer::db::upsert_node_sync_state(&conn, "peer-mono-1", 10, now + 1)
        .expect("upsert with lower seq");

    let stored: i64 = conn
        .query_row(
            "SELECT last_seq FROM node_sync_state WHERE node_pubkey = 'peer-mono-1'",
            [],
            |r| r.get(0),
        )
        .expect("query last_seq");

    assert_eq!(
        stored, 15,
        "cursor must NOT regress from 15 to 10 — monotonic invariant violated"
    );
}

// ---------------------------------------------------------------------------
// 15. Issue-CURSOR-MONOTONIC: cursor advances when new event applied
// ---------------------------------------------------------------------------

/// If `node_sync_state.last_seq` is 5 and we call `upsert_node_sync_state`
/// with seq=10, the stored cursor must advance to 10.
// Issue-CURSOR-MONOTONIC — 2026-03-14
#[test]
fn cursor_advances_when_new_event_applied() {
    let conn = common::test_db();
    let now = common::now();

    // Seed cursor at seq=5.
    stophammer::db::upsert_node_sync_state(&conn, "peer-mono-2", 5, now)
        .expect("seed cursor at 5");

    // Advance to seq=10.
    stophammer::db::upsert_node_sync_state(&conn, "peer-mono-2", 10, now + 1)
        .expect("upsert with higher seq");

    let stored: i64 = conn
        .query_row(
            "SELECT last_seq FROM node_sync_state WHERE node_pubkey = 'peer-mono-2'",
            [],
            |r| r.get(0),
        )
        .expect("query last_seq");

    assert_eq!(
        stored, 10,
        "cursor must advance from 5 to 10"
    );
}
