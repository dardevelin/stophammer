mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// Helper: insert an artist and return its artist_id.
// ---------------------------------------------------------------------------

fn insert_artist(conn: &rusqlite::Connection, id: &str, name: &str) -> String {
    let now = common::now();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5)",
        params![id, name, name.to_lowercase(), name, now],
    )
    .unwrap();
    id.to_string()
}

/// Create an artist credit for a single artist and return the credit id.
fn insert_single_credit(conn: &rusqlite::Connection, artist_id: &str, display: &str) -> i64 {
    let now = common::now();
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params![display, now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name)
         VALUES (?1, ?2, 0, ?3)",
        params![credit_id, artist_id, display],
    )
    .unwrap();
    credit_id
}

/// Insert a minimal feed and return its feed_guid.
fn insert_feed(conn: &rusqlite::Connection, guid: &str, credit_id: i64) -> String {
    let now = common::now();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![
            guid,
            format!("https://example.com/{guid}"),
            "Test Feed",
            "test feed",
            credit_id,
            now,
        ],
    )
    .unwrap();
    guid.to_string()
}

/// Insert a minimal track and return its track_guid.
fn insert_track(
    conn: &rusqlite::Connection,
    track_guid: &str,
    feed_guid: &str,
    credit_id: i64,
) -> String {
    let now = common::now();
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![track_guid, feed_guid, credit_id, "Test Track", "test track", now],
    )
    .unwrap();
    track_guid.to_string()
}

// ---------------------------------------------------------------------------
// 1. Artist with no feeds
// ---------------------------------------------------------------------------

#[test]
fn artist_with_no_feeds() {
    let conn = common::test_db();
    insert_artist(&conn, "art-lonely", "Lonely Artist");
    let cid = insert_single_credit(&conn, "art-lonely", "Lonely Artist");

    // LEFT JOIN to feeds: artist exists but no feeds.
    let mut stmt = conn
        .prepare(
            "SELECT a.artist_id, f.feed_guid
             FROM artists a
             LEFT JOIN artist_credit_name acn ON acn.artist_id = a.artist_id
             LEFT JOIN feeds f ON f.artist_credit_id = acn.artist_credit_id
             WHERE a.artist_id = 'art-lonely'",
        )
        .unwrap();
    let results: Vec<(String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "art-lonely");
    assert!(results[0].1.is_none(), "feed_guid should be NULL for artist with no feeds");

    // Direct feed query returns nothing.
    let feed_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feeds WHERE artist_credit_id = ?1",
            params![cid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(feed_count, 0);

    // Direct track query returns nothing.
    let track_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tracks t
             JOIN feeds f ON f.feed_guid = t.feed_guid
             WHERE f.artist_credit_id = ?1",
            params![cid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(track_count, 0);
}

// ---------------------------------------------------------------------------
// 2. Feed with no tracks
// ---------------------------------------------------------------------------

#[test]
fn feed_with_no_tracks() {
    let conn = common::test_db();
    insert_artist(&conn, "art-empty-feed", "Empty Feed Artist");
    let cid = insert_single_credit(&conn, "art-empty-feed", "Empty Feed Artist");
    insert_feed(&conn, "feed-empty", cid);

    let track_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tracks WHERE feed_guid = 'feed-empty'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(track_count, 0);

    // LEFT JOIN from feed to tracks returns the feed with NULL track fields.
    let mut stmt = conn
        .prepare(
            "SELECT f.feed_guid, t.track_guid
             FROM feeds f
             LEFT JOIN tracks t ON t.feed_guid = f.feed_guid
             WHERE f.feed_guid = 'feed-empty'",
        )
        .unwrap();
    let results: Vec<(String, Option<String>)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "feed-empty");
    assert!(results[0].1.is_none(), "track_guid should be NULL for feed with no tracks");
}

// ---------------------------------------------------------------------------
// 3. Duplicate event idempotent (INSERT OR IGNORE)
// ---------------------------------------------------------------------------

#[test]
fn duplicate_event_idempotent() {
    let conn = common::test_db();
    let now = common::now();

    // Insert first event.
    conn.execute(
        "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-dup-reg', 'feed.created', '{\"title\":\"original\"}', 'feed-001', 'node-a', 'sig-a', 1, ?1)",
        params![now],
    )
    .unwrap();

    // Insert same event_id with INSERT OR IGNORE — should be a no-op.
    conn.execute(
        "INSERT OR IGNORE INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-dup-reg', 'feed.updated', '{\"title\":\"modified\"}', 'feed-001', 'node-a', 'sig-b', 2, ?1)",
        params![now],
    )
    .unwrap();

    // Only one event should exist.
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM events WHERE event_id = 'evt-dup-reg'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    // Original payload should be preserved (not overwritten by the second insert).
    let payload: String = conn
        .query_row(
            "SELECT payload_json FROM events WHERE event_id = 'evt-dup-reg'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(payload, "{\"title\":\"original\"}");

    // Seq should remain 1.
    let seq: i64 = conn
        .query_row(
            "SELECT seq FROM events WHERE event_id = 'evt-dup-reg'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(seq, 1);
}

// ---------------------------------------------------------------------------
// 4. Artist credit display name not unique
// ---------------------------------------------------------------------------

#[test]
fn artist_credit_display_name_not_unique() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "art-dup-name-1", "Same Name");
    insert_artist(&conn, "art-dup-name-2", "Same Name Too");

    // Two credits with the same display_name should both succeed.
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES ('Same Display', ?1)",
        params![now],
    )
    .unwrap();
    let cid1 = conn.last_insert_rowid();

    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES ('Same Display', ?1)",
        params![now],
    )
    .unwrap();
    let cid2 = conn.last_insert_rowid();

    assert_ne!(cid1, cid2, "IDs should be different for credits with same display_name");

    // Both should be retrievable.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit WHERE display_name = 'Same Display'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// 5. Feed payment routes replaced
// ---------------------------------------------------------------------------

#[test]
fn feed_payment_routes_replaced() {
    let conn = common::test_db();
    insert_artist(&conn, "art-fpr-reg", "FPR Reg Artist");
    let cid = insert_single_credit(&conn, "art-fpr-reg", "FPR Reg Artist");
    let fg = insert_feed(&conn, "feed-fpr-reg", cid);

    // Insert initial routes.
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'Original Host', 'keysend', 'node-original', 80)",
        params![&fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'Original App', 'keysend', 'node-app-original', 20)",
        params![&fg],
    )
    .unwrap();

    let initial_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = ?1",
            params![&fg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(initial_count, 2);

    // Replace: delete all, then insert new set.
    conn.execute(
        "DELETE FROM feed_payment_routes WHERE feed_guid = ?1",
        params![&fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'New Host', 'keysend', 'node-new-host', 90)",
        params![&fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'New App', 'keysend', 'node-new-app', 10)",
        params![&fg],
    )
    .unwrap();

    // Verify old routes are gone.
    let old_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = ?1 AND recipient_name LIKE 'Original%'",
            params![&fg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(old_count, 0, "old routes should be deleted");

    // Verify new routes are present.
    let mut stmt = conn
        .prepare(
            "SELECT recipient_name, address, split FROM feed_payment_routes WHERE feed_guid = ?1 ORDER BY split DESC",
        )
        .unwrap();
    let routes: Vec<(String, String, i64)> = stmt
        .query_map(params![&fg], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].0, "New Host");
    assert_eq!(routes[0].1, "node-new-host");
    assert_eq!(routes[0].2, 90);
    assert_eq!(routes[1].0, "New App");
    assert_eq!(routes[1].1, "node-new-app");
    assert_eq!(routes[1].2, 10);
}

// ---------------------------------------------------------------------------
// 6. Value time splits ordering
// ---------------------------------------------------------------------------

#[test]
fn value_time_splits_ordering() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "art-vts-ord", "VTS Order Artist");
    let cid = insert_single_credit(&conn, "art-vts-ord", "VTS Order Artist");
    let fg = insert_feed(&conn, "feed-vts-ord", cid);
    let tg = insert_track(&conn, "track-vts-ord", &fg, cid);

    // Insert VTS entries with non-sequential start_time_secs (out of order).
    let times = [120, 0, 60, 240, 180];
    for (i, &start) in times.iter().enumerate() {
        conn.execute(
            "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at)
             VALUES (?1, ?2, 60, ?3, ?4, 50, ?5)",
            params![&tg, start, format!("remote-feed-{i}"), format!("remote-item-{i}"), now],
        )
        .unwrap();
    }

    // Query ordered by start_time_secs.
    let mut stmt = conn
        .prepare(
            "SELECT start_time_secs FROM value_time_splits
             WHERE source_track_guid = ?1
             ORDER BY start_time_secs ASC",
        )
        .unwrap();
    let ordered: Vec<i64> = stmt
        .query_map(params![&tg], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(ordered, vec![0, 60, 120, 180, 240]);
}

// ---------------------------------------------------------------------------
// 7. Tag case normalization
// ---------------------------------------------------------------------------

#[test]
fn tag_case_normalization() {
    let conn = common::test_db();
    let now = common::now();

    // The tags table has a UNIQUE constraint on name. So storing tags in a
    // case-normalized fashion (lowercased) means "Rock", "ROCK", "rock" all
    // map to the single row "rock".
    let variants = ["Rock", "ROCK", "rock"];
    for v in &variants {
        conn.execute(
            "INSERT OR IGNORE INTO tags (name, created_at) VALUES (?1, ?2)",
            params![v.to_lowercase(), now],
        )
        .unwrap();
    }

    // Should be exactly one tag row.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tags WHERE name = 'rock'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "all case variants should collapse to one tag");

    // Retrieve the tag.
    let tag_name: String = conn
        .query_row(
            "SELECT name FROM tags WHERE name = 'rock'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tag_name, "rock");
}

// ---------------------------------------------------------------------------
// 8. External ID cross-entity (same scheme+value, different entity types)
// ---------------------------------------------------------------------------

#[test]
fn external_id_cross_entity() {
    let conn = common::test_db();
    let now = common::now();

    // Insert an external ID for an artist.
    conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at)
         VALUES ('artist', 'art-ext-1', 'isrc', 'USRC17607839', ?1)",
        params![now],
    )
    .unwrap();

    // Insert the same scheme for a track (different entity_type + entity_id).
    // The UNIQUE constraint is (entity_type, entity_id, scheme), so this is fine.
    conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at)
         VALUES ('track', 'track-ext-1', 'isrc', 'USRC17607839', ?1)",
        params![now],
    )
    .unwrap();

    // Both should exist.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM external_ids WHERE scheme = 'isrc' AND value = 'USRC17607839'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    // Verify they link to different entities.
    let artist_eid: String = conn
        .query_row(
            "SELECT entity_id FROM external_ids WHERE entity_type = 'artist' AND scheme = 'isrc'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(artist_eid, "art-ext-1");

    let track_eid: String = conn
        .query_row(
            "SELECT entity_id FROM external_ids WHERE entity_type = 'track' AND scheme = 'isrc'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(track_eid, "track-ext-1");

    // Duplicate (same entity_type, entity_id, scheme) should fail.
    let dup_result = conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at)
         VALUES ('artist', 'art-ext-1', 'isrc', 'DIFFERENT_VALUE', ?1)",
        params![now],
    );
    assert!(dup_result.is_err(), "duplicate entity_type+entity_id+scheme should fail");
}

// ---------------------------------------------------------------------------
// 9. Large batch insert (100 feeds)
// ---------------------------------------------------------------------------

#[test]
fn large_batch_insert() {
    let conn = common::test_db();
    let now = common::now();

    insert_artist(&conn, "art-batch", "Batch Artist");
    let cid = insert_single_credit(&conn, "art-batch", "Batch Artist");

    // Insert 100 feeds in a loop.
    for i in 0..100 {
        let guid = format!("feed-batch-{i:04}");
        conn.execute(
            "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
            params![
                &guid,
                format!("https://example.com/batch/{i}"),
                format!("Batch Album {i}"),
                format!("batch album {i}"),
                cid,
                now,
            ],
        )
        .unwrap();
    }

    // Verify all 100 are queryable.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feeds WHERE feed_guid LIKE 'feed-batch-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 100);

    // Spot-check a few by index.
    let first: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'feed-batch-0000'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(first, "Batch Album 0");

    let last: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'feed-batch-0099'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(last, "Batch Album 99");

    let middle: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'feed-batch-0050'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(middle, "Batch Album 50");
}

// ---------------------------------------------------------------------------
// 10. Event seq monotonic
// ---------------------------------------------------------------------------

#[test]
fn event_seq_monotonic() {
    let conn = common::test_db();
    let now = common::now();

    // Insert 5 events with explicit seq values.
    for i in 1..=5 {
        conn.execute(
            "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
             VALUES (?1, 'feed.created', '{}', ?2, 'node-mono', 'sig-mono', ?3, ?4)",
            params![format!("evt-mono-{i}"), format!("feed-mono-{i}"), i, now],
        )
        .unwrap();
    }

    // Verify seq values are strictly monotonic (1, 2, 3, 4, 5).
    let mut stmt = conn
        .prepare("SELECT seq FROM events WHERE event_id LIKE 'evt-mono-%' ORDER BY seq ASC")
        .unwrap();
    let seqs: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);

    // Verify strict monotonicity: each seq is exactly one greater than the previous.
    for window in seqs.windows(2) {
        assert_eq!(
            window[1] - window[0],
            1,
            "seq values should increase by exactly 1: got {} -> {}",
            window[0],
            window[1]
        );
    }
}
