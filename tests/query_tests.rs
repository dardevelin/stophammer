mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert a test artist and return its ID.
fn insert_test_artist(conn: &rusqlite::Connection, name: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = common::now();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, name, name.to_lowercase(), now, now],
    )
    .unwrap();
    id
}

/// Insert an artist credit for a single artist and return the credit_id.
fn insert_credit(conn: &rusqlite::Connection, artist_id: &str, name: &str) -> i64 {
    let now = common::now();
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params![name, now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, 0, ?3, '')",
        params![credit_id, artist_id, name],
    )
    .unwrap();
    credit_id
}

/// Insert a test feed.
fn insert_feed(
    conn: &rusqlite::Connection,
    guid: &str,
    url: &str,
    title: &str,
    credit_id: i64,
) {
    let now = common::now();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, \
         explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7)",
        params![guid, url, title, title.to_lowercase(), credit_id, now, now],
    )
    .unwrap();
}

/// Insert a test track.
fn insert_track(
    conn: &rusqlite::Connection,
    guid: &str,
    feed_guid: &str,
    credit_id: i64,
    title: &str,
    pub_date: Option<i64>,
) {
    let now = common::now();
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, \
         pub_date, explicit, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8)",
        params![guid, feed_guid, credit_id, title, title.to_lowercase(), pub_date, now, now],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 1. get_artist_by_id — insert artist, query by ID, verify all fields returned
// ---------------------------------------------------------------------------

#[test]
fn get_artist_by_id() {
    let conn = common::test_db();
    let now = common::now();

    let id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, area, \
         img_url, url, begin_year, end_year, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            id,
            "Thrice",
            "thrice",
            "Thrice",
            "United States",
            "https://example.com/img.jpg",
            "https://thrice.com",
            2000_i64,
            9999_i64,
            now,
            now,
        ],
    )
    .unwrap();

    let row: (String, String, String, Option<String>, Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT artist_id, name, name_lower, area, begin_year, end_year \
             FROM artists WHERE artist_id = ?1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .unwrap();

    assert_eq!(row.0, id);
    assert_eq!(row.1, "Thrice");
    assert_eq!(row.2, "thrice");
    assert_eq!(row.3.as_deref(), Some("United States"));
    assert_eq!(row.4, Some(2000));
    assert_eq!(row.5, Some(9999));
}

// ---------------------------------------------------------------------------
// 2. get_artist_not_found — query nonexistent ID, verify empty result
// ---------------------------------------------------------------------------

#[test]
fn get_artist_not_found() {
    let conn = common::test_db();

    let result = conn.query_row(
        "SELECT artist_id FROM artists WHERE artist_id = ?1",
        params!["nonexistent-id-that-is-not-in-db"],
        |r| r.get::<_, String>(0),
    );

    assert!(
        result.is_err(),
        "expected no rows for a nonexistent artist_id"
    );
}

// ---------------------------------------------------------------------------
// 3. get_artist_with_redirect — insert artist + redirect, query OLD id via
//    LEFT JOIN on artist_id_redirect → resolve to NEW artist
// ---------------------------------------------------------------------------

#[test]
fn get_artist_with_redirect() {
    let conn = common::test_db();
    let now = common::now();

    let old_id = uuid::Uuid::new_v4().to_string();
    let new_id = uuid::Uuid::new_v4().to_string();

    // Only the new artist exists in the artists table.
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![new_id, "Merged Artist", "merged artist", now, now],
    )
    .unwrap();

    // Record the redirect from old → new.
    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![old_id, new_id, now],
    )
    .unwrap();

    // Query via old ID using LEFT JOIN to follow the redirect.
    let resolved_name: String = conn
        .query_row(
            "SELECT a.name \
             FROM artist_id_redirect r \
             JOIN artists a ON a.artist_id = r.new_artist_id \
             WHERE r.old_artist_id = ?1",
            params![old_id],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(resolved_name, "Merged Artist");
}

// ---------------------------------------------------------------------------
// 4. get_artist_aliases — insert artist with 3 aliases, verify all returned
// ---------------------------------------------------------------------------

#[test]
fn get_artist_aliases() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_test_artist(&conn, "Radiohead");

    let aliases = ["radiohead", "radio head", "r.h."];
    for alias in &aliases {
        conn.execute(
            "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
             VALUES (?1, ?2, ?3)",
            params![alias, artist_id, now],
        )
        .unwrap();
    }

    let mut stmt = conn
        .prepare("SELECT alias_lower FROM artist_aliases WHERE artist_id = ?1 ORDER BY alias_lower")
        .unwrap();
    let returned: Vec<String> = stmt
        .query_map(params![artist_id], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(returned.len(), 3);

    let mut expected: Vec<&str> = aliases.to_vec();
    expected.sort_unstable();
    for (got, want) in returned.iter().zip(expected.iter()) {
        assert_eq!(got.as_str(), *want);
    }
}

// ---------------------------------------------------------------------------
// 5. get_feeds_for_artist — insert artist + credit + 3 feeds, query via JOIN
// ---------------------------------------------------------------------------

#[test]
fn get_feeds_for_artist() {
    let conn = common::test_db();

    let artist_id = insert_test_artist(&conn, "Björk");
    let credit_id = insert_credit(&conn, &artist_id, "Björk");

    // Insert 3 feeds for this artist.
    let feed_data = [
        ("feed-guid-a", "https://example.com/a.xml", "Album A"),
        ("feed-guid-b", "https://example.com/b.xml", "Album B"),
        ("feed-guid-c", "https://example.com/c.xml", "Album C"),
    ];
    for (guid, url, title) in &feed_data {
        insert_feed(&conn, guid, url, title, credit_id);
    }

    // Query feeds for the artist via credit JOIN.
    let mut stmt = conn
        .prepare(
            "SELECT f.feed_guid, f.title \
             FROM feeds f \
             JOIN artist_credit_name acn ON acn.artist_credit_id = f.artist_credit_id \
             WHERE acn.artist_id = ?1 \
             ORDER BY f.title ASC",
        )
        .unwrap();

    let rows: Vec<(String, String)> = stmt
        .query_map(params![artist_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3, "should return all 3 feeds for the artist");
    assert_eq!(rows[0].1, "Album A");
    assert_eq!(rows[1].1, "Album B");
    assert_eq!(rows[2].1, "Album C");
}

// ---------------------------------------------------------------------------
// 6. get_feeds_pagination — insert 5 feeds, paginate with limit=2 via cursor
// ---------------------------------------------------------------------------

#[test]
fn get_feeds_pagination() {
    let conn = common::test_db();

    let artist_id = insert_test_artist(&conn, "Paginator");
    let credit_id = insert_credit(&conn, &artist_id, "Paginator");

    // Insert 5 feeds with deterministic, sortable titles.
    for i in 1..=5_u32 {
        insert_feed(
            &conn,
            &format!("page-feed-{i:02}"),
            &format!("https://example.com/page{i}.xml"),
            &format!("Page Feed {:02}", i),
            credit_id,
        );
    }

    // Page 1: no cursor (title_lower > '' simulates start).
    let page1: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT feed_guid FROM feeds WHERE title_lower > ?1 \
                 ORDER BY title_lower ASC LIMIT ?2",
            )
            .unwrap();
        stmt.query_map(params!["", 2], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0], "page-feed-01");
    assert_eq!(page1[1], "page-feed-02");

    // Find cursor for page 2: title_lower of the last item on page 1.
    let cursor: String = conn
        .query_row(
            "SELECT title_lower FROM feeds WHERE feed_guid = ?1",
            params![page1.last().unwrap()],
            |r| r.get(0),
        )
        .unwrap();

    // Page 2.
    let page2: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT feed_guid FROM feeds WHERE title_lower > ?1 \
                 ORDER BY title_lower ASC LIMIT ?2",
            )
            .unwrap();
        stmt.query_map(params![cursor, 2], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0], "page-feed-03");
    assert_eq!(page2[1], "page-feed-04");

    // Page 3: only one item remaining.
    let cursor2: String = conn
        .query_row(
            "SELECT title_lower FROM feeds WHERE feed_guid = ?1",
            params![page2.last().unwrap()],
            |r| r.get(0),
        )
        .unwrap();

    let page3: Vec<String> = {
        let mut stmt = conn
            .prepare(
                "SELECT feed_guid FROM feeds WHERE title_lower > ?1 \
                 ORDER BY title_lower ASC LIMIT ?2",
            )
            .unwrap();
        stmt.query_map(params![cursor2, 2], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    assert_eq!(page3.len(), 1);
    assert_eq!(page3[0], "page-feed-05");
}

// ---------------------------------------------------------------------------
// 7. get_feed_by_guid — insert feed, query by guid, verify fields
// ---------------------------------------------------------------------------

#[test]
fn get_feed_by_guid() {
    let conn = common::test_db();

    let artist_id = insert_test_artist(&conn, "Portishead");
    let credit_id = insert_credit(&conn, &artist_id, "Portishead");

    let guid = "portishead-dummy";
    let url = "https://example.com/portishead.xml";
    let title = "Dummy";

    insert_feed(&conn, guid, url, title, credit_id);

    let (returned_guid, returned_url, returned_title, returned_credit): (
        String,
        String,
        String,
        i64,
    ) = conn
        .query_row(
            "SELECT feed_guid, feed_url, title, artist_credit_id \
             FROM feeds WHERE feed_guid = ?1",
            params![guid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();

    assert_eq!(returned_guid, guid);
    assert_eq!(returned_url, url);
    assert_eq!(returned_title, title);
    assert_eq!(returned_credit, credit_id);
}

// ---------------------------------------------------------------------------
// 8. get_feed_with_tracks — insert feed + 3 tracks, query tracks, verify count
// ---------------------------------------------------------------------------

#[test]
fn get_feed_with_tracks() {
    let conn = common::test_db();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_test_artist(&conn, "Massive Attack");
    let credit_id = insert_credit(&conn, &artist_id, "Massive Attack");

    let feed_guid = "mezzanine-feed";
    insert_feed(
        &conn,
        feed_guid,
        "https://example.com/mezzanine.xml",
        "Mezzanine",
        credit_id,
    );

    // Insert 3 tracks with distinct pub_dates for ordering.
    let tracks = [
        ("track-angel", "Angel", base_date + 300),
        ("track-risingson", "Risingson", base_date + 200),
        ("track-teardrop", "Teardrop", base_date + 100),
    ];
    for (guid, title, pub_date) in &tracks {
        insert_track(&conn, guid, feed_guid, credit_id, title, Some(*pub_date));
    }

    // Query all tracks for the feed, ordered by pub_date DESC.
    let mut stmt = conn
        .prepare(
            "SELECT track_guid, title, pub_date \
             FROM tracks WHERE feed_guid = ?1 ORDER BY pub_date DESC",
        )
        .unwrap();

    let rows: Vec<(String, String, Option<i64>)> = stmt
        .query_map(params![feed_guid], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3, "should return all 3 tracks for the feed");

    // Ordered by pub_date DESC: Angel (300) → Risingson (200) → Teardrop (100).
    assert_eq!(rows[0].0, "track-angel");
    assert_eq!(rows[0].1, "Angel");
    assert_eq!(rows[1].0, "track-risingson");
    assert_eq!(rows[1].1, "Risingson");
    assert_eq!(rows[2].0, "track-teardrop");
    assert_eq!(rows[2].1, "Teardrop");
}
