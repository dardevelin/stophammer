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

fn insert_track(
    conn: &rusqlite::Connection,
    guid: &str,
    feed_guid: &str,
    credit_id: i64,
    title: &str,
) {
    let now = common::now();
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, \
         explicit, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)",
        params![guid, feed_guid, credit_id, title, title.to_lowercase(), now, now],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 1. artist_artist_rel_created
// ---------------------------------------------------------------------------

/// Create 2 artists, insert into artist_artist_rel with rel_type_id=20
/// (member_of), verify the row is stored with correct fields.
#[test]
fn artist_artist_rel_created() {
    let conn = common::test_db();
    let now = common::now();

    let artist_a = insert_artist(&conn, "Jack White");
    let artist_b = insert_artist(&conn, "The White Stripes");

    conn.execute(
        "INSERT INTO artist_artist_rel (artist_id_a, artist_id_b, rel_type_id, begin_year, end_year, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![artist_a, artist_b, 20, 1997, 2011, now],
    )
    .unwrap();

    let (stored_a, stored_b, stored_rel, stored_begin, stored_end): (
        String,
        String,
        i64,
        Option<i64>,
        Option<i64>,
    ) = conn
        .query_row(
            "SELECT artist_id_a, artist_id_b, rel_type_id, begin_year, end_year \
             FROM artist_artist_rel WHERE artist_id_a = ?1 AND artist_id_b = ?2",
            params![artist_a, artist_b],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();

    assert_eq!(stored_a, artist_a);
    assert_eq!(stored_b, artist_b);
    assert_eq!(stored_rel, 20);
    assert_eq!(stored_begin, Some(1997));
    assert_eq!(stored_end, Some(2011));
}

// ---------------------------------------------------------------------------
// 2. rel_type_lookup
// ---------------------------------------------------------------------------

/// Query rel_type table for id=1 (performer), verify name and entity_pair.
#[test]
fn rel_type_lookup() {
    let conn = common::test_db();

    let (name, entity_pair): (String, String) = conn
        .query_row(
            "SELECT name, entity_pair FROM rel_type WHERE id = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();

    assert_eq!(name, "performer");
    assert_eq!(entity_pair, "artist-track");
}

// ---------------------------------------------------------------------------
// 3. artist_rels_bidirectional
// ---------------------------------------------------------------------------

/// Create 2 artists, add rel from A->B, query rels for artist B using
/// WHERE artist_id_a = ?1 OR artist_id_b = ?1, verify B can find the relationship.
#[test]
fn artist_rels_bidirectional() {
    let conn = common::test_db();
    let now = common::now();

    let artist_a = insert_artist(&conn, "John Lennon");
    let artist_b = insert_artist(&conn, "The Beatles");

    conn.execute(
        "INSERT INTO artist_artist_rel (artist_id_a, artist_id_b, rel_type_id, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![artist_a, artist_b, 20, now],
    )
    .unwrap();

    // Query from artist B's perspective — should find the relationship.
    let mut stmt = conn
        .prepare(
            "SELECT artist_id_a, artist_id_b, rel_type_id \
             FROM artist_artist_rel \
             WHERE artist_id_a = ?1 OR artist_id_b = ?1",
        )
        .unwrap();

    let rows: Vec<(String, String, i64)> = stmt
        .query_map(params![artist_b], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 1, "artist B should find exactly one relationship");
    assert_eq!(rows[0].0, artist_a);
    assert_eq!(rows[0].1, artist_b);
    assert_eq!(rows[0].2, 20);
}

// ---------------------------------------------------------------------------
// 4. track_rel_created
// ---------------------------------------------------------------------------

/// Create 2 tracks (needs feeds), insert into track_rel with rel_type_id=12
/// (remixer), verify stored correctly.
#[test]
fn track_rel_created() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Radiohead");
    let credit_id = insert_credit(&conn, &artist_id, "Radiohead");

    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/okcomputer.xml",
        "OK Computer",
        credit_id,
    );

    let track_a = uuid::Uuid::new_v4().to_string();
    let track_b = uuid::Uuid::new_v4().to_string();
    insert_track(&conn, &track_a, &feed_guid, credit_id, "Everything In Its Right Place");
    insert_track(&conn, &track_b, &feed_guid, credit_id, "Everything In Its Right Place (Remix)");

    conn.execute(
        "INSERT INTO track_rel (track_guid_a, track_guid_b, rel_type_id, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![track_a, track_b, 12, now],
    )
    .unwrap();

    let (stored_a, stored_b, stored_rel): (String, String, i64) = conn
        .query_row(
            "SELECT track_guid_a, track_guid_b, rel_type_id \
             FROM track_rel WHERE track_guid_a = ?1 AND track_guid_b = ?2",
            params![track_a, track_b],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();

    assert_eq!(stored_a, track_a);
    assert_eq!(stored_b, track_b);
    assert_eq!(stored_rel, 12);

    // Verify rel_type_id=12 is 'remixer'.
    let rel_name: String = conn
        .query_row(
            "SELECT name FROM rel_type WHERE id = 12",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rel_name, "remixer");
}
