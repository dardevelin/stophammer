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

/// Insert a feed with full control over guid, url, title, and credit_id.
fn insert_feed_full(
    conn: &rusqlite::Connection,
    guid: &str,
    url: &str,
    title: &str,
    credit_id: i64,
) -> String {
    let now = common::now();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![guid, url, title, title.to_lowercase(), credit_id, now],
    )
    .unwrap();
    guid.to_string()
}

/// Insert a minimal feed and return its feed_guid.
fn insert_feed(conn: &rusqlite::Connection, guid: &str, credit_id: i64) -> String {
    insert_feed_full(
        conn,
        guid,
        &format!("https://example.com/{guid}"),
        "Test Feed",
        credit_id,
    )
}

/// Insert a track with full control over title.
fn insert_track_full(
    conn: &rusqlite::Connection,
    track_guid: &str,
    feed_guid: &str,
    credit_id: i64,
    title: &str,
) -> String {
    let now = common::now();
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![track_guid, feed_guid, credit_id, title, title.to_lowercase(), now],
    )
    .unwrap();
    track_guid.to_string()
}

/// Insert a minimal track and return its track_guid.
fn insert_track(
    conn: &rusqlite::Connection,
    track_guid: &str,
    feed_guid: &str,
    credit_id: i64,
) -> String {
    insert_track_full(conn, track_guid, feed_guid, credit_id, "Test Track")
}

// ---------------------------------------------------------------------------
// 1. Full ingest-to-query pipeline
// ---------------------------------------------------------------------------

#[test]
fn ingest_to_query_pipeline() {
    let conn = common::test_db();
    let now = common::now();

    // Step 1: Create an artist.
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, area, created_at, updated_at)
         VALUES ('art-e2e', 'E2E Artist', 'e2e artist', 'E2E Artist', 1, 'Brazil', ?1, ?1)",
        params![now],
    )
    .unwrap();

    // Step 2: Create an artist_credit + artist_credit_name.
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES ('E2E Artist', ?1)",
        params![now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();

    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name)
         VALUES (?1, 'art-e2e', 0, 'E2E Artist')",
        params![credit_id],
    )
    .unwrap();

    // Step 3: Create a feed linked to the artist_credit.
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, language, created_at, updated_at)
         VALUES ('feed-e2e', 'https://example.com/e2e', 'E2E Album', 'e2e album', ?1, 'A test album', 'https://img.example.com/e2e.jpg', 'en', ?2, ?2)",
        params![credit_id, now],
    )
    .unwrap();

    // Step 4: Create 3 tracks with payment routes and VTS.
    for i in 1..=3 {
        let tg = format!("track-e2e-{i}");
        conn.execute(
            "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, duration_secs, track_number, created_at, updated_at)
             VALUES (?1, 'feed-e2e', ?2, ?3, ?4, ?5, ?6, ?7, ?5, ?5)",
            params![
                &tg,
                credit_id,
                format!("Song {i}"),
                format!("song {i}"),
                now + i * 60,
                180 + i * 30,
                i,
            ],
        )
        .unwrap();

        // Payment route per track.
        conn.execute(
            "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split)
             VALUES (?1, 'feed-e2e', 'E2E Artist', 'keysend', ?2, 95)",
            params![&tg, format!("node-e2e-{i}")],
        )
        .unwrap();

        // VTS entry per track.
        conn.execute(
            "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at)
             VALUES (?1, 0, 60, 'remote-feed-e2e', ?2, 50, ?3)",
            params![&tg, format!("remote-item-{i}"), now],
        )
        .unwrap();
    }

    // Step 5: Add feed payment routes.
    conn.execute(
        "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, split)
         VALUES ('feed-e2e', 'E2E Artist', 'keysend', 'node-feed-e2e', 100)",
        [],
    )
    .unwrap();

    // Step 6: Add tags to the feed.
    conn.execute(
        "INSERT INTO tags (name, created_at) VALUES ('electronic', ?1)",
        params![now],
    )
    .unwrap();
    let tag_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO feed_tag (feed_guid, tag_id, created_at) VALUES ('feed-e2e', ?1, ?2)",
        params![tag_id, now],
    )
    .unwrap();

    // Step 7: Query each entity to verify.

    // Verify artist.
    let (artist_name, artist_area): (String, String) = conn
        .query_row(
            "SELECT name, area FROM artists WHERE artist_id = 'art-e2e'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(artist_name, "E2E Artist");
    assert_eq!(artist_area, "Brazil");

    // Verify credit.
    let credit_display: String = conn
        .query_row(
            "SELECT display_name FROM artist_credit WHERE id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(credit_display, "E2E Artist");

    let credit_name_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit_name WHERE artist_credit_id = ?1",
            params![credit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(credit_name_count, 1);

    // Verify feed.
    let (feed_title, feed_desc, feed_lang): (String, String, String) = conn
        .query_row(
            "SELECT title, description, language FROM feeds WHERE feed_guid = 'feed-e2e'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(feed_title, "E2E Album");
    assert_eq!(feed_desc, "A test album");
    assert_eq!(feed_lang, "en");

    // Verify 3 tracks linked to feed.
    let track_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tracks WHERE feed_guid = 'feed-e2e'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(track_count, 3);

    // Verify payment routes (3 track-level + 1 feed-level).
    let track_route_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM payment_routes WHERE feed_guid = 'feed-e2e'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(track_route_count, 3);

    let feed_route_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM feed_payment_routes WHERE feed_guid = 'feed-e2e'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(feed_route_count, 1);

    // Verify VTS entries.
    let vts_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM value_time_splits WHERE source_track_guid LIKE 'track-e2e-%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(vts_count, 3);

    // Verify tag linked to feed.
    let tag_name: String = conn
        .query_row(
            "SELECT t.name FROM tags t
             JOIN feed_tag ft ON ft.tag_id = t.id
             WHERE ft.feed_guid = 'feed-e2e'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tag_name, "electronic");
}

// ---------------------------------------------------------------------------
// 2. FTS5 search index population
//
// NOTE: The search_index uses content='' (contentless FTS5), which means
// column values are not stored and return NULL when selected. We use rowid
// and COUNT(*) to verify matches, and maintain a separate lookup for
// entity_type/entity_id mapping.
// ---------------------------------------------------------------------------

#[test]
fn search_index_population() {
    let conn = common::test_db();

    // Insert prerequisites.
    insert_artist(&conn, "art-fts", "FTS Artist");
    let cid = insert_single_credit(&conn, "art-fts", "FTS Artist");
    insert_feed(&conn, "feed-fts", cid);
    insert_track(&conn, "track-fts", "feed-fts", cid);

    // Populate search index for the feed (rowid=1).
    conn.execute(
        "INSERT INTO search_index (rowid, entity_type, entity_id, name, title, description, tags)
         VALUES (1, 'feed', 'feed-fts', 'FTS Artist', 'Test Feed', 'A searchable feed', 'electronic')",
        [],
    )
    .unwrap();

    // Populate search index for the track (rowid=2).
    conn.execute(
        "INSERT INTO search_index (rowid, entity_type, entity_id, name, title, description, tags)
         VALUES (2, 'track', 'track-fts', 'FTS Artist', 'Test Track', 'A searchable track', 'rock')",
        [],
    )
    .unwrap();

    // Query with MATCH on title for "searchable" -- should find nothing (it's in description).
    let title_match_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM search_index WHERE search_index MATCH 'title:searchable'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title_match_count, 0);

    // Query with MATCH on description -- both rows have "searchable" in description.
    let desc_match_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM search_index WHERE search_index MATCH 'description:searchable'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(desc_match_count, 2);

    // Query with MATCH on name -- both rows have "FTS Artist".
    let name_match_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM search_index WHERE search_index MATCH 'name:\"FTS Artist\"'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name_match_count, 2);

    // Verify rowids returned by MATCH correspond to the correct entities.
    let mut stmt = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH 'tags:electronic'")
        .unwrap();
    let rowids: Vec<i64> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rowids, vec![1], "only the feed row (rowid=1) has 'electronic' tag");

    let mut stmt2 = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH 'tags:rock'")
        .unwrap();
    let rowids2: Vec<i64> = stmt2
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rowids2, vec![2], "only the track row (rowid=2) has 'rock' tag");
}

// ---------------------------------------------------------------------------
// 3. Quality score integration
// ---------------------------------------------------------------------------

#[test]
fn quality_score_integration() {
    let conn = common::test_db();
    let now = common::now();

    // Create two fully-populated feeds.
    insert_artist(&conn, "art-q1", "Quality Artist 1");
    let cid1 = insert_single_credit(&conn, "art-q1", "Quality Artist 1");
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, language, created_at, updated_at)
         VALUES ('feed-q1', 'https://example.com/q1', 'High Quality Album', 'high quality album', ?1, 'Excellent', 'https://img.example.com/q1.jpg', 'en', ?2, ?2)",
        params![cid1, now],
    )
    .unwrap();

    insert_artist(&conn, "art-q2", "Quality Artist 2");
    let cid2 = insert_single_credit(&conn, "art-q2", "Quality Artist 2");
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, language, created_at, updated_at)
         VALUES ('feed-q2', 'https://example.com/q2', 'Low Quality Album', 'low quality album', ?1, 'Sparse', 'https://img.example.com/q2.jpg', 'en', ?2, ?2)",
        params![cid2, now],
    )
    .unwrap();

    // Insert quality scores.
    conn.execute(
        "INSERT INTO entity_quality (entity_type, entity_id, score, computed_at)
         VALUES ('feed', 'feed-q1', 95, ?1)",
        params![now],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO entity_quality (entity_type, entity_id, score, computed_at)
         VALUES ('feed', 'feed-q2', 30, ?1)",
        params![now],
    )
    .unwrap();

    // Verify scores are retrievable.
    let score_q1: i64 = conn
        .query_row(
            "SELECT score FROM entity_quality WHERE entity_type = 'feed' AND entity_id = 'feed-q1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(score_q1, 95);

    let score_q2: i64 = conn
        .query_row(
            "SELECT score FROM entity_quality WHERE entity_type = 'feed' AND entity_id = 'feed-q2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(score_q2, 30);

    // Test search ranking with quality scores using the feeds table directly
    // (contentless FTS5 does not store column values, so we rank via feeds + quality).
    let mut stmt = conn
        .prepare(
            "SELECT f.feed_guid, eq.score
             FROM feeds f
             JOIN entity_quality eq ON eq.entity_type = 'feed' AND eq.entity_id = f.feed_guid
             WHERE f.feed_guid IN ('feed-q1', 'feed-q2')
             ORDER BY eq.score DESC",
        )
        .unwrap();
    let ranked: Vec<(String, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].0, "feed-q1");
    assert_eq!(ranked[0].1, 95);
    assert_eq!(ranked[1].0, "feed-q2");
    assert_eq!(ranked[1].1, 30);
}

// ---------------------------------------------------------------------------
// 4. Cursor-based pagination
// ---------------------------------------------------------------------------

#[test]
fn pagination_with_cursors() {
    let conn = common::test_db();

    // Create shared artist/credit.
    insert_artist(&conn, "art-page", "Page Artist");
    let cid = insert_single_credit(&conn, "art-page", "Page Artist");

    // Insert 5 feeds with deterministic guids for ordering.
    for i in 1..=5 {
        let guid = format!("feed-page-{i:02}");
        insert_feed_full(
            &conn,
            &guid,
            &format!("https://example.com/page/{i}"),
            &format!("Album {i}"),
            cid,
        );
    }

    // Page 1: first 2 feeds ordered by feed_guid.
    let mut stmt = conn
        .prepare("SELECT feed_guid FROM feeds WHERE feed_guid LIKE 'feed-page-%' ORDER BY feed_guid ASC LIMIT ?1")
        .unwrap();
    let page1: Vec<String> = stmt
        .query_map(params![2], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(page1, vec!["feed-page-01", "feed-page-02"]);

    // Cursor: use last guid from page 1 as cursor.
    let cursor = &page1[page1.len() - 1];
    let mut stmt2 = conn
        .prepare("SELECT feed_guid FROM feeds WHERE feed_guid LIKE 'feed-page-%' AND feed_guid > ?1 ORDER BY feed_guid ASC LIMIT ?2")
        .unwrap();
    let page2: Vec<String> = stmt2
        .query_map(params![cursor, 2], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(page2, vec!["feed-page-03", "feed-page-04"]);

    // Non-overlapping: page1 and page2 share no elements.
    for id in &page2 {
        assert!(!page1.contains(id), "page2 should not overlap page1");
    }

    // Page 3: remainder.
    let cursor2 = &page2[page2.len() - 1];
    let page3: Vec<String> = stmt2
        .query_map(params![cursor2, 2], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(page3, vec!["feed-page-05"]);

    // has_more logic: page3 has only 1 item (less than limit 2), so no more pages.
    let has_more = page3.len() as i64 == 2;
    assert!(!has_more, "should be no more pages after page3");

    // Page 4: past end.
    let cursor3 = &page3[page3.len() - 1];
    let page4: Vec<String> = stmt2
        .query_map(params![cursor3, 2], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(page4.is_empty(), "page past end should be empty");
}

// ---------------------------------------------------------------------------
// 5. Unicode search
// ---------------------------------------------------------------------------

#[test]
fn unicode_search() {
    let conn = common::test_db();

    insert_artist(&conn, "art-uni", "Unicode Artist");
    let cid = insert_single_credit(&conn, "art-uni", "Unicode Artist");

    let unicode_title = "Musica Electronica \u{65E5}\u{672C}\u{8A9E}";
    insert_feed_full(
        &conn,
        "feed-uni",
        "https://example.com/unicode",
        unicode_title,
        cid,
    );

    // Insert into FTS5 search_index (contentless -- use rowid for lookups).
    conn.execute(
        "INSERT INTO search_index (rowid, entity_type, entity_id, name, title, description, tags)
         VALUES (1, 'feed', 'feed-uni', 'Unicode Artist', ?1, '', 'world')",
        params![unicode_title],
    )
    .unwrap();

    // Query with unicode term (japanese characters).
    let jp_match_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM search_index WHERE search_index MATCH ?1",
            params!["\u{65E5}\u{672C}\u{8A9E}"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(jp_match_count, 1);

    // Verify the matching rowid.
    let rowid: i64 = conn
        .query_row(
            "SELECT rowid FROM search_index WHERE search_index MATCH ?1",
            params!["\u{65E5}\u{672C}\u{8A9E}"],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rowid, 1);

    // Query with latin portion.
    let latin_match_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM search_index WHERE search_index MATCH 'Electronica'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(latin_match_count, 1);

    // Verify the title round-trips correctly.
    let stored_title: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'feed-uni'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_title, unicode_title);
}

// ---------------------------------------------------------------------------
// 6. Empty database queries
// ---------------------------------------------------------------------------

#[test]
fn empty_db_queries() {
    let conn = common::test_db();

    // Query non-existent artist.
    let artist_result = conn.query_row(
        "SELECT artist_id FROM artists WHERE artist_id = 'nonexistent'",
        [],
        |r| r.get::<_, String>(0),
    );
    assert!(artist_result.is_err());

    // Query non-existent feed.
    let feed_result = conn.query_row(
        "SELECT feed_guid FROM feeds WHERE feed_guid = 'nonexistent'",
        [],
        |r| r.get::<_, String>(0),
    );
    assert!(feed_result.is_err());

    // Query non-existent track.
    let track_result = conn.query_row(
        "SELECT track_guid FROM tracks WHERE track_guid = 'nonexistent'",
        [],
        |r| r.get::<_, String>(0),
    );
    assert!(track_result.is_err());

    // Count queries return 0, not errors.
    let artist_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM artists", [], |r| r.get(0))
        .unwrap();
    assert_eq!(artist_count, 0);

    let feed_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM feeds", [], |r| r.get(0))
        .unwrap();
    assert_eq!(feed_count, 0);

    let track_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM tracks", [], |r| r.get(0))
        .unwrap();
    assert_eq!(track_count, 0);

    // FTS5 on empty index returns no results.
    let mut stmt = conn
        .prepare("SELECT entity_id FROM search_index WHERE search_index MATCH 'anything'")
        .unwrap();
    let fts_results: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(fts_results.is_empty());
}

// ---------------------------------------------------------------------------
// 7. SQL injection prevention (parameterized queries)
// ---------------------------------------------------------------------------

#[test]
fn sql_injection_prevention() {
    let conn = common::test_db();
    let now = common::now();

    let injection_name = "'; DROP TABLE feeds; --";
    let injection_id = "art-inject";

    // Insert an artist with an injection string as the name.
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5)",
        params![injection_id, injection_name, injection_name.to_lowercase(), injection_name, now],
    )
    .unwrap();

    // Verify feeds table still exists.
    let feed_table_exists: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='feeds'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(feed_table_exists, "feeds table should still exist");

    // Retrieve the artist and verify the name was stored literally.
    let stored_name: String = conn
        .query_row(
            "SELECT name FROM artists WHERE artist_id = ?1",
            params![injection_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_name, injection_name);

    // Try querying with an injection string as a parameter.
    let query_injection = "' OR '1'='1";
    let result = conn.query_row(
        "SELECT artist_id FROM artists WHERE name = ?1",
        params![query_injection],
        |r| r.get::<_, String>(0),
    );
    // Should find nothing (the injection string is not a valid name).
    assert!(result.is_err());

    // Create a credit with injection string as display_name.
    let cid = insert_single_credit(&conn, injection_id, injection_name);
    let stored_display: String = conn
        .query_row(
            "SELECT display_name FROM artist_credit WHERE id = ?1",
            params![cid],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_display, injection_name);

    // Insert a feed with injection string as title.
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, created_at, updated_at)
         VALUES ('feed-inject', 'https://example.com/inject', ?1, ?2, ?3, ?4, ?4)",
        params![injection_name, injection_name.to_lowercase(), cid, now],
    )
    .unwrap();

    let stored_title: String = conn
        .query_row(
            "SELECT title FROM feeds WHERE feed_guid = 'feed-inject'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_title, injection_name);

    // Final sanity: all core tables still exist.
    let table_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(table_count > 20, "all tables should still exist");
}
