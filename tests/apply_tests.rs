mod common;

use rusqlite::params;

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

/// Inserting the same event_id twice should be idempotent: the second insert
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
/// verifies the credit + credit_name rows are created correctly and the
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
    let reconstructed = format!("{}{}{}", first_name, first_join, second_name);
    assert_eq!(reconstructed, "Alice & Bob");
}
