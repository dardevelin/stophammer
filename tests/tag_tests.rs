mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn insert_artist(conn: &rusqlite::Connection, name: &str) -> String {
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

fn insert_feed(conn: &rusqlite::Connection, guid: &str, url: &str, title: &str, credit_id: i64) {
    let now = common::now();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, \
         explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7)",
        params![guid, url, title, title.to_lowercase(), credit_id, now, now],
    )
    .unwrap();
}

fn insert_tag(conn: &rusqlite::Connection, name: &str) -> i64 {
    let now = common::now();
    conn.execute(
        "INSERT INTO tags (name, created_at) VALUES (?1, ?2)",
        params![name.to_lowercase(), now],
    )
    .unwrap();
    conn.last_insert_rowid()
}

// ---------------------------------------------------------------------------
// 1. tag_create_and_lookup
// ---------------------------------------------------------------------------

/// Insert a tag, query it back, verify name is stored lowercased.
/// Insert same name again with INSERT OR IGNORE, verify no duplicate.
#[test]
fn tag_create_and_lookup() {
    let conn = common::test_db();
    let now = common::now();

    // Insert a tag with mixed case — stored lowercased.
    conn.execute(
        "INSERT INTO tags (name, created_at) VALUES (?1, ?2)",
        params!["Electronic".to_lowercase(), now],
    )
    .unwrap();

    let (id, name): (i64, String) = conn
        .query_row(
            "SELECT id, name FROM tags WHERE name = ?1",
            params!["electronic"],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();

    assert!(id > 0);
    assert_eq!(name, "electronic");

    // Insert same name again with INSERT OR IGNORE — should be a no-op.
    let rows_changed = conn
        .execute(
            "INSERT OR IGNORE INTO tags (name, created_at) VALUES (?1, ?2)",
            params!["electronic", now],
        )
        .unwrap();
    assert_eq!(rows_changed, 0, "duplicate tag insert must be a no-op");

    // Verify only one row exists.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tags WHERE name = 'electronic'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

// ---------------------------------------------------------------------------
// 2. artist_tag_applied
// ---------------------------------------------------------------------------

/// Create artist + tag, INSERT into artist_tag, verify the join returns the tag.
#[test]
fn artist_tag_applied() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Burial");
    let tag_id = insert_tag(&conn, "Dubstep");

    conn.execute(
        "INSERT INTO artist_tag (artist_id, tag_id, created_at) VALUES (?1, ?2, ?3)",
        params![artist_id, tag_id, now],
    )
    .unwrap();

    let tag_name: String = conn
        .query_row(
            "SELECT t.name FROM tags t \
             JOIN artist_tag at ON at.tag_id = t.id \
             WHERE at.artist_id = ?1",
            params![artist_id],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(tag_name, "dubstep");
}

// ---------------------------------------------------------------------------
// 3. feed_tag_applied
// ---------------------------------------------------------------------------

/// Create feed (needs artist + credit), tag it, verify join returns tag.
#[test]
fn feed_tag_applied() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Four Tet");
    let credit_id = insert_credit(&conn, &artist_id, "Four Tet");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/rounds.xml",
        "Rounds",
        credit_id,
    );

    let tag_id = insert_tag(&conn, "IDM");

    conn.execute(
        "INSERT INTO feed_tag (feed_guid, tag_id, created_at) VALUES (?1, ?2, ?3)",
        params![feed_guid, tag_id, now],
    )
    .unwrap();

    let tag_name: String = conn
        .query_row(
            "SELECT t.name FROM tags t \
             JOIN feed_tag ft ON ft.tag_id = t.id \
             WHERE ft.feed_guid = ?1",
            params![feed_guid],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(tag_name, "idm");
}

// ---------------------------------------------------------------------------
// 4. tag_removal
// ---------------------------------------------------------------------------

/// Apply a tag to an artist, verify it exists, DELETE from artist_tag, verify gone.
#[test]
fn tag_removal() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Aphex Twin");
    let tag_id = insert_tag(&conn, "Ambient");

    conn.execute(
        "INSERT INTO artist_tag (artist_id, tag_id, created_at) VALUES (?1, ?2, ?3)",
        params![artist_id, tag_id, now],
    )
    .unwrap();

    // Verify it exists.
    let count_before: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_tag WHERE artist_id = ?1 AND tag_id = ?2",
            params![artist_id, tag_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_before, 1);

    // Remove the tag.
    conn.execute(
        "DELETE FROM artist_tag WHERE artist_id = ?1 AND tag_id = ?2",
        params![artist_id, tag_id],
    )
    .unwrap();

    // Verify it is gone.
    let count_after: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_tag WHERE artist_id = ?1 AND tag_id = ?2",
            params![artist_id, tag_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_after, 0);
}

// ---------------------------------------------------------------------------
// 5. tag_deduplication
// ---------------------------------------------------------------------------

/// Insert same tag name twice (case-insensitive), verify only one row.
/// Apply same tag to same artist twice, verify only one junction row.
#[test]
fn tag_deduplication() {
    let conn = common::test_db();
    let now = common::now();

    // Insert "Jazz" lowercased.
    conn.execute(
        "INSERT INTO tags (name, created_at) VALUES (?1, ?2)",
        params!["Jazz".to_lowercase(), now],
    )
    .unwrap();

    // Attempt to insert "JAZZ" lowercased — same value, should conflict.
    let rows_changed = conn
        .execute(
            "INSERT OR IGNORE INTO tags (name, created_at) VALUES (?1, ?2)",
            params!["JAZZ".to_lowercase(), now],
        )
        .unwrap();
    assert_eq!(rows_changed, 0, "case-insensitive duplicate must be ignored");

    let tag_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM tags WHERE name = 'jazz'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tag_count, 1, "only one tag row must exist for 'jazz'");

    // Get the tag id.
    let tag_id: i64 = conn
        .query_row("SELECT id FROM tags WHERE name = 'jazz'", [], |r| r.get(0))
        .unwrap();

    // Apply tag to artist.
    let artist_id = insert_artist(&conn, "Miles Davis");
    conn.execute(
        "INSERT INTO artist_tag (artist_id, tag_id, created_at) VALUES (?1, ?2, ?3)",
        params![artist_id, tag_id, now],
    )
    .unwrap();

    // Apply same tag to same artist again — should conflict on PK.
    let junction_rows = conn
        .execute(
            "INSERT OR IGNORE INTO artist_tag (artist_id, tag_id, created_at) VALUES (?1, ?2, ?3)",
            params![artist_id, tag_id, now],
        )
        .unwrap();
    assert_eq!(junction_rows, 0, "duplicate junction row must be ignored");

    let junction_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_tag WHERE artist_id = ?1 AND tag_id = ?2",
            params![artist_id, tag_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(junction_count, 1, "only one artist_tag row must exist");
}
