mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// 1. Schema creation on fresh :memory: DB
// ---------------------------------------------------------------------------

#[test]
fn schema_creates_all_tables() {
    let conn = common::test_db();
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap();
    let tables: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    let expected = [
        "artist_aliases",
        "artist_artist_rel",
        "artist_credit",
        "artist_credit_name",
        "artist_id_redirect",
        "artist_location",
        "artist_tag",
        "artist_type",
        "artists",
        "entity_field_status",
        "entity_quality",
        "entity_source",
        "events",
        "external_ids",
        "feed_crawl_cache",
        "feed_payment_routes",
        "feed_rel",
        "feed_tag",
        "feed_type",
        "feeds",
        "manifest_source",
        "node_sync_state",
        "payment_routes",
        "peer_nodes",
        "rel_type",
        "tags",
        "track_rel",
        "track_tag",
        "tracks",
        "value_time_splits",
    ];
    for name in &expected {
        assert!(
            tables.contains(&name.to_string()),
            "missing table: {name}"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Lookup table seeding
// ---------------------------------------------------------------------------

#[test]
fn lookup_tables_seeded() {
    let conn = common::test_db();

    let artist_type_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM artist_type", [], |r| r.get(0))
        .unwrap();
    assert_eq!(artist_type_count, 6);

    let feed_type_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM feed_type", [], |r| r.get(0))
        .unwrap();
    assert_eq!(feed_type_count, 8);

    let rel_type_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM rel_type", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rel_type_count, 35);
}

// ---------------------------------------------------------------------------
// 3. Schema idempotency
// ---------------------------------------------------------------------------

#[test]
fn schema_idempotent() {
    let conn = common::test_db();
    const SCHEMA: &str = include_str!("../src/schema.sql");
    // Applying schema a second time must not error.
    conn.execute_batch(SCHEMA).unwrap();

    // Seed counts should remain unchanged (INSERT OR IGNORE).
    let artist_type_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM artist_type", [], |r| r.get(0))
        .unwrap();
    assert_eq!(artist_type_count, 6);
}

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
// 4. Artist insert + alias auto-registration
// ---------------------------------------------------------------------------

#[test]
fn artist_insert_and_alias() {
    let conn = common::test_db();
    let now = common::now();
    let id = "art-001";
    insert_artist(&conn, id, "Alice Band");

    // Manually register an alias (production code does this on insert).
    conn.execute(
        "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at)
         VALUES (?1, ?2, ?3)",
        params!["alice band", id, now],
    )
    .unwrap();

    let alias_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_aliases WHERE artist_id = ?1",
            params![id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(alias_count, 1);
}

// ---------------------------------------------------------------------------
// 5. Artist resolve via alias
// ---------------------------------------------------------------------------

#[test]
fn artist_resolve_via_alias() {
    let conn = common::test_db();
    let now = common::now();
    let id = "art-002";
    insert_artist(&conn, id, "The Rolling Stones");

    // Register a shortened alias.
    conn.execute(
        "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at)
         VALUES (?1, ?2, ?3)",
        params!["rolling stones", id, now],
    )
    .unwrap();

    let resolved: String = conn
        .query_row(
            "SELECT artist_id FROM artist_aliases WHERE alias_lower = ?1",
            params!["rolling stones"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(resolved, id);
}

// ---------------------------------------------------------------------------
// 6. Artist resolve via name_lower
// ---------------------------------------------------------------------------

#[test]
fn artist_resolve_via_name_lower() {
    let conn = common::test_db();
    insert_artist(&conn, "art-003", "Portishead");

    let resolved: String = conn
        .query_row(
            "SELECT artist_id FROM artists WHERE name_lower = ?1",
            params!["portishead"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(resolved, "art-003");
}

// ---------------------------------------------------------------------------
// 7. Artist resolve creates new
// ---------------------------------------------------------------------------

#[test]
fn artist_resolve_creates_new() {
    let conn = common::test_db();
    let name = "Brand New Artist";
    let name_lower = name.to_lowercase();

    // Lookup — should find nothing.
    let existing = conn.query_row(
        "SELECT artist_id FROM artists WHERE name_lower = ?1",
        params![&name_lower],
        |r| r.get::<_, String>(0),
    );
    assert!(existing.is_err());

    // Create on miss.
    let new_id = "art-new-001";
    insert_artist(&conn, new_id, name);

    let resolved: String = conn
        .query_row(
            "SELECT artist_id FROM artists WHERE name_lower = ?1",
            params![&name_lower],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(resolved, new_id);
}

// ---------------------------------------------------------------------------
// 8. Artist merge
// ---------------------------------------------------------------------------

#[test]
fn artist_merge_repoints_credits() {
    let conn = common::test_db();
    let now = common::now();

    let old_id = "art-old";
    let new_id = "art-new";
    insert_artist(&conn, old_id, "Old Name");
    insert_artist(&conn, new_id, "New Name");

    // Create a credit pointing at old_id.
    let credit_id = insert_single_credit(&conn, old_id, "Old Name");

    // Merge: repoint credit names.
    conn.execute(
        "UPDATE artist_credit_name SET artist_id = ?1 WHERE artist_id = ?2",
        params![new_id, old_id],
    )
    .unwrap();

    // Record redirect.
    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at)
         VALUES (?1, ?2, ?3)",
        params![old_id, new_id, now],
    )
    .unwrap();

    // Verify credit now points at new_id.
    let pointed: String = conn
        .query_row(
            "SELECT artist_id FROM artist_credit_name WHERE artist_credit_id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pointed, new_id);

    // Verify redirect exists.
    let redirect: String = conn
        .query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![old_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(redirect, new_id);
}

// ---------------------------------------------------------------------------
// 9. Artist credit creation — single and multi-artist
// ---------------------------------------------------------------------------

#[test]
fn artist_credit_single() {
    let conn = common::test_db();
    insert_artist(&conn, "art-s1", "Solo");
    let cid = insert_single_credit(&conn, "art-s1", "Solo");

    let display: String = conn
        .query_row(
            "SELECT display_name FROM artist_credit WHERE id = ?1",
            params![cid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(display, "Solo");
}

#[test]
fn artist_credit_multi() {
    let conn = common::test_db();
    let now = common::now();
    insert_artist(&conn, "art-m1", "Alice");
    insert_artist(&conn, "art-m2", "Bob");

    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params!["Alice & Bob", now],
    )
    .unwrap();
    let cid = conn.last_insert_rowid();

    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase)
         VALUES (?1, 'art-m1', 0, 'Alice', ' & ')",
        params![cid],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase)
         VALUES (?1, 'art-m2', 1, 'Bob', '')",
        params![cid],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit_name WHERE artist_credit_id = ?1",
            params![cid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------------
// 10. Feed upsert
// ---------------------------------------------------------------------------

#[test]
fn feed_upsert() {
    let conn = common::test_db();
    let now = common::now();
    insert_artist(&conn, "art-f1", "Feed Artist");
    let cid = insert_single_credit(&conn, "art-f1", "Feed Artist");
    let guid = "feed-001";

    // Initial insert.
    insert_feed(&conn, guid, cid);
    let title: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = ?1",
            params![guid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title, "Test Feed");

    // Upsert: update title.
    conn.execute(
        "UPDATE feeds SET title = ?1, title_lower = ?2, updated_at = ?3 WHERE feed_guid = ?4",
        params!["Updated Feed", "updated feed", now, guid],
    )
    .unwrap();

    let updated_title: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = ?1",
            params![guid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(updated_title, "Updated Feed");
}

// ---------------------------------------------------------------------------
// 11. Track upsert
// ---------------------------------------------------------------------------

#[test]
fn track_upsert() {
    let conn = common::test_db();
    let now = common::now();
    insert_artist(&conn, "art-t1", "Track Artist");
    let cid = insert_single_credit(&conn, "art-t1", "Track Artist");
    let fg = insert_feed(&conn, "feed-t1", cid);
    let tg = "track-001";

    insert_track(&conn, tg, &fg, cid);

    let title: String = conn
        .query_row(
            "SELECT title FROM tracks WHERE track_guid = ?1",
            params![tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title, "Test Track");

    // Update.
    conn.execute(
        "UPDATE tracks SET title = ?1, title_lower = ?2, updated_at = ?3 WHERE track_guid = ?4",
        params!["Updated Track", "updated track", now, tg],
    )
    .unwrap();

    let updated: String = conn
        .query_row(
            "SELECT title FROM tracks WHERE track_guid = ?1",
            params![tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(updated, "Updated Track");
}

// ---------------------------------------------------------------------------
// 12. Payment route replace (delete + insert cycle)
// ---------------------------------------------------------------------------

#[test]
fn payment_route_replace() {
    let conn = common::test_db();
    insert_artist(&conn, "art-pr", "PR Artist");
    let cid = insert_single_credit(&conn, "art-pr", "PR Artist");
    let fg = insert_feed(&conn, "feed-pr", cid);
    let tg = insert_track(&conn, "track-pr", &fg, cid);

    // Insert initial routes.
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, ?2, 'Alice', 'keysend', 'node-abc', 90)",
        params![&tg, &fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, ?2, 'App', 'keysend', 'node-xyz', 10)",
        params![&tg, &fg],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = ?1",
            params![&tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    // Replace: delete all, then insert new set.
    conn.execute(
        "DELETE FROM payment_routes WHERE track_guid = ?1",
        params![&tg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, ?2, 'Bob', 'keysend', 'node-bob', 100)",
        params![&tg, &fg],
    )
    .unwrap();

    let new_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = ?1",
            params![&tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_count, 1);

    let recipient: String = conn
        .query_row(
            "SELECT recipient_name FROM payment_routes WHERE track_guid = ?1",
            params![&tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(recipient, "Bob");
}

// ---------------------------------------------------------------------------
// 13. Feed payment route replace
// ---------------------------------------------------------------------------

#[test]
fn feed_payment_route_replace() {
    let conn = common::test_db();
    insert_artist(&conn, "art-fpr", "FPR Artist");
    let cid = insert_single_credit(&conn, "art-fpr", "FPR Artist");
    let fg = insert_feed(&conn, "feed-fpr", cid);

    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'Host', 'keysend', 'node-host', 95)",
        params![&fg],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = ?1",
            params![&fg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);

    // Replace.
    conn.execute(
        "DELETE FROM feed_payment_routes WHERE feed_guid = ?1",
        params![&fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'New Host', 'keysend', 'node-new', 80)",
        params![&fg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES (?1, 'App', 'keysend', 'node-app', 20)",
        params![&fg],
    )
    .unwrap();

    let new_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = ?1",
            params![&fg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_count, 2);
}

// ---------------------------------------------------------------------------
// 14. Value time split replace (delete + insert cycle)
// ---------------------------------------------------------------------------

#[test]
fn value_time_split_replace() {
    let conn = common::test_db();
    let now = common::now();
    insert_artist(&conn, "art-vts", "VTS Artist");
    let cid = insert_single_credit(&conn, "art-vts", "VTS Artist");
    let fg = insert_feed(&conn, "feed-vts", cid);
    let tg = insert_track(&conn, "track-vts", &fg, cid);

    // Insert two VTS entries.
    conn.execute(
        "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at)
         VALUES (?1, 0, 60, 'remote-feed-1', 'remote-item-1', 50, ?2)",
        params![&tg, now],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at)
         VALUES (?1, 60, 120, 'remote-feed-2', 'remote-item-2', 50, ?2)",
        params![&tg, now],
    )
    .unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM value_time_splits WHERE source_track_guid = ?1",
            params![&tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    // Replace cycle.
    conn.execute(
        "DELETE FROM value_time_splits WHERE source_track_guid = ?1",
        params![&tg],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at)
         VALUES (?1, 0, 180, 'remote-feed-3', 'remote-item-3', 100, ?2)",
        params![&tg, now],
    )
    .unwrap();

    let new_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM value_time_splits WHERE source_track_guid = ?1",
            params![&tg],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_count, 1);
}

// ---------------------------------------------------------------------------
// 15. Event insert monotonic seq
// ---------------------------------------------------------------------------

#[test]
fn event_insert_monotonic_seq() {
    let conn = common::test_db();
    let now = common::now();

    for i in 1..=5 {
        conn.execute(
            "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
             VALUES (?1, 'feed.updated', '{}', 'feed-001', 'node-a', 'sig-a', ?2, ?3)",
            params![format!("evt-{i}"), i, now],
        )
        .unwrap();
    }

    let mut stmt = conn
        .prepare("SELECT seq FROM events ORDER BY seq ASC")
        .unwrap();
    let seqs: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

// ---------------------------------------------------------------------------
// 16. Event insert idempotent
// ---------------------------------------------------------------------------

#[test]
fn event_insert_idempotent() {
    let conn = common::test_db();
    let now = common::now();

    conn.execute(
        "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-dup', 'feed.updated', '{}', 'feed-001', 'node-a', 'sig-a', 1, ?1)",
        params![now],
    )
    .unwrap();

    // Second insert with same event_id should fail (PK constraint).
    let result = conn.execute(
        "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-dup', 'feed.updated', '{}', 'feed-001', 'node-a', 'sig-a', 2, ?1)",
        params![now],
    );
    assert!(result.is_err());

    // OR IGNORE variant: succeeds but inserts nothing.
    conn.execute(
        "INSERT OR IGNORE INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-dup', 'feed.updated', '{}', 'feed-001', 'node-a', 'sig-a', 2, ?1)",
        params![now],
    )
    .unwrap();

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);

    // seq should still be 1 (original).
    let seq: i64 = conn
        .query_row(
            "SELECT seq FROM events WHERE event_id = 'evt-dup'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(seq, 1);
}

// ---------------------------------------------------------------------------
// 17. Events pagination (get_events_since)
// ---------------------------------------------------------------------------

#[test]
fn events_pagination() {
    let conn = common::test_db();
    let now = common::now();

    for i in 1..=20 {
        conn.execute(
            "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
             VALUES (?1, 'track.created', '{}', 'track-001', 'node-a', 'sig', ?2, ?3)",
            params![format!("evt-page-{i}"), i, now],
        )
        .unwrap();
    }

    // Page 1: after_seq = 0, limit = 5  -> seq 1..5
    let mut stmt = conn
        .prepare("SELECT seq FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2")
        .unwrap();
    let page1: Vec<i64> = stmt
        .query_map(params![0, 5], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(page1, vec![1, 2, 3, 4, 5]);

    // Page 2: after_seq = 5, limit = 5  -> seq 6..10
    let page2: Vec<i64> = stmt
        .query_map(params![5, 5], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(page2, vec![6, 7, 8, 9, 10]);

    // Page past end: after_seq = 20, limit = 5  -> empty
    let page_end: Vec<i64> = stmt
        .query_map(params![20, 5], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(page_end.is_empty());
}

// ---------------------------------------------------------------------------
// 18. Feed crawl cache upsert
// ---------------------------------------------------------------------------

#[test]
fn feed_crawl_cache_upsert() {
    let conn = common::test_db();
    let now = common::now();
    let url = "https://example.com/feed.xml";

    // Insert.
    conn.execute(
        "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) VALUES (?1, ?2, ?3)",
        params![url, "hash-v1", now],
    )
    .unwrap();

    let hash: String = conn
        .query_row(
            "SELECT content_hash FROM feed_crawl_cache WHERE feed_url = ?1",
            params![url],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hash, "hash-v1");

    // Update (upsert via INSERT OR REPLACE, since feed_url is PK).
    conn.execute(
        "INSERT OR REPLACE INTO feed_crawl_cache (feed_url, content_hash, crawled_at) VALUES (?1, ?2, ?3)",
        params![url, "hash-v2", now + 60],
    )
    .unwrap();

    let updated_hash: String = conn
        .query_row(
            "SELECT content_hash FROM feed_crawl_cache WHERE feed_url = ?1",
            params![url],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(updated_hash, "hash-v2");

    // Only one row should exist.
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM feed_crawl_cache", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

// ---------------------------------------------------------------------------
// 19. Peer node CRUD — upsert, failure tracking, eviction
// ---------------------------------------------------------------------------

#[test]
fn peer_node_upsert() {
    let conn = common::test_db();
    let now = common::now();

    conn.execute(
        "INSERT INTO peer_nodes (node_pubkey, node_url, discovered_at) VALUES (?1, ?2, ?3)",
        params!["pk-1", "https://peer1.example.com", now],
    )
    .unwrap();

    let url: String = conn
        .query_row(
            "SELECT node_url FROM peer_nodes WHERE node_pubkey = ?1",
            params!["pk-1"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(url, "https://peer1.example.com");

    // Update URL (upsert pattern).
    conn.execute(
        "INSERT OR REPLACE INTO peer_nodes (node_pubkey, node_url, discovered_at) VALUES (?1, ?2, ?3)",
        params!["pk-1", "https://peer1-new.example.com", now],
    )
    .unwrap();

    let updated_url: String = conn
        .query_row(
            "SELECT node_url FROM peer_nodes WHERE node_pubkey = ?1",
            params!["pk-1"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(updated_url, "https://peer1-new.example.com");
}

#[test]
fn peer_node_failure_tracking() {
    let conn = common::test_db();
    let now = common::now();

    conn.execute(
        "INSERT INTO peer_nodes (node_pubkey, node_url, discovered_at) VALUES (?1, ?2, ?3)",
        params!["pk-fail", "https://failing.example.com", now],
    )
    .unwrap();

    // Increment consecutive_failures.
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = consecutive_failures + 1 WHERE node_pubkey = ?1",
        params!["pk-fail"],
    )
    .unwrap();
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = consecutive_failures + 1 WHERE node_pubkey = ?1",
        params!["pk-fail"],
    )
    .unwrap();

    let failures: i64 = conn
        .query_row(
            "SELECT consecutive_failures FROM peer_nodes WHERE node_pubkey = ?1",
            params!["pk-fail"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(failures, 2);

    // Reset on success.
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = 0, last_push_at = ?1 WHERE node_pubkey = ?2",
        params![now, "pk-fail"],
    )
    .unwrap();

    let after_reset: i64 = conn
        .query_row(
            "SELECT consecutive_failures FROM peer_nodes WHERE node_pubkey = ?1",
            params!["pk-fail"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(after_reset, 0);
}

#[test]
fn peer_node_eviction() {
    let conn = common::test_db();
    let now = common::now();

    // Insert several peers, some with high failure counts.
    for i in 0..5 {
        conn.execute(
            "INSERT INTO peer_nodes (node_pubkey, node_url, discovered_at, consecutive_failures)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                format!("pk-evict-{i}"),
                format!("https://peer{i}.example.com"),
                now,
                i * 5 // 0, 5, 10, 15, 20
            ],
        )
        .unwrap();
    }

    // Evict peers with >= 10 consecutive failures.
    conn.execute(
        "DELETE FROM peer_nodes WHERE consecutive_failures >= 10",
        [],
    )
    .unwrap();

    let remaining: i64 = conn
        .query_row("SELECT COUNT(*) FROM peer_nodes", [], |r| r.get(0))
        .unwrap();
    // Peers with 0, 5 failures survive -> 2 remaining.
    assert_eq!(remaining, 2);
}

// ---------------------------------------------------------------------------
// 20. Ingest transaction atomicity
// ---------------------------------------------------------------------------

#[test]
fn ingest_transaction_atomicity() {
    let conn = common::test_db();
    let now = common::now();

    // Simulate a full atomic ingest: artist + credit + feed + track + routes + event.
    conn.execute_batch("BEGIN").unwrap();

    // Artist.
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, created_at, updated_at)
         VALUES ('art-txn', 'Txn Artist', 'txn artist', 'Txn Artist', 1, ?1, ?1)",
        params![now],
    )
    .unwrap();

    // Alias.
    conn.execute(
        "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at)
         VALUES ('txn artist', 'art-txn', ?1)",
        params![now],
    )
    .unwrap();

    // Credit.
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES ('Txn Artist', ?1)",
        params![now],
    )
    .unwrap();
    let cid = conn.last_insert_rowid();

    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name)
         VALUES (?1, 'art-txn', 0, 'Txn Artist')",
        params![cid],
    )
    .unwrap();

    // Feed.
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, created_at, updated_at)
         VALUES ('feed-txn', 'https://example.com/txn', 'Txn Album', 'txn album', ?1, ?2, ?2)",
        params![cid, now],
    )
    .unwrap();

    // Track.
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, duration_secs, created_at, updated_at)
         VALUES ('track-txn', 'feed-txn', ?1, 'Txn Song', 'txn song', ?2, 240, ?2, ?2)",
        params![cid, now],
    )
    .unwrap();

    // Payment routes.
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split)
         VALUES ('track-txn', 'feed-txn', 'Txn Artist', 'keysend', 'node-txn', 95)",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES ('feed-txn', 'Txn Artist', 'keysend', 'node-txn-feed', 100)",
        [],
    )
    .unwrap();

    // Event.
    conn.execute(
        "INSERT INTO events (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at)
         VALUES ('evt-txn', 'feed.created', '{}', 'feed-txn', 'node-a', 'sig-txn', 1, ?1)",
        params![now],
    )
    .unwrap();

    conn.execute_batch("COMMIT").unwrap();

    // Verify everything landed.
    let artist_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM artists WHERE artist_id = 'art-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(artist_exists);

    let feed_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM feeds WHERE feed_guid = 'feed-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(feed_exists);

    let track_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM tracks WHERE track_guid = 'track-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(track_exists);

    let route_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE track_guid = 'track-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(route_count, 1);

    let feed_route_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = 'feed-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(feed_route_count, 1);

    let event_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM events WHERE event_id = 'evt-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(event_exists);

    let alias_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM artist_aliases WHERE alias_lower = 'txn artist' AND artist_id = 'art-txn'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(alias_exists);
}

// ---------------------------------------------------------------------------
// Bonus: Transaction rollback leaves DB clean
// ---------------------------------------------------------------------------

#[test]
fn ingest_transaction_rollback() {
    let conn = common::test_db();
    let now = common::now();

    conn.execute_batch("BEGIN").unwrap();

    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, created_at, updated_at)
         VALUES ('art-rb', 'Rollback Artist', 'rollback artist', 'Rollback Artist', 1, ?1, ?1)",
        params![now],
    )
    .unwrap();

    conn.execute_batch("ROLLBACK").unwrap();

    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artists WHERE artist_id = 'art-rb'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "rollback should have removed the artist");
}
