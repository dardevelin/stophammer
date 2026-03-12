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

// ---------------------------------------------------------------------------
// 1. redirect_resolves_old_id
// ---------------------------------------------------------------------------

/// Create artist A and artist B, insert redirect old->new, query
/// artist_id_redirect for old, verify new_artist_id = B's id.
#[test]
fn redirect_resolves_old_id() {
    let conn = common::test_db();
    let now = common::now();

    let old_id = insert_artist(&conn, "Old Artist Name");
    let new_id = insert_artist(&conn, "Canonical Artist Name");

    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![old_id, new_id, now],
    )
    .unwrap();

    let resolved_id: String = conn
        .query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![old_id],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(resolved_id, new_id);
}

// ---------------------------------------------------------------------------
// 2. redirect_chain
// ---------------------------------------------------------------------------

/// Insert redirect A->B and B->C, query for A to get B, then query for B to
/// get C. Verify two-hop resolution works.
#[test]
fn redirect_chain() {
    let conn = common::test_db();
    let now = common::now();

    let id_a = insert_artist(&conn, "Artist A (oldest)");
    let id_b = insert_artist(&conn, "Artist B (intermediate)");
    let id_c = insert_artist(&conn, "Artist C (canonical)");

    // A -> B
    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![id_a, id_b, now],
    )
    .unwrap();

    // B -> C
    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![id_b, id_c, now],
    )
    .unwrap();

    // First hop: A -> B.
    let hop1: String = conn
        .query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![id_a],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hop1, id_b);

    // Second hop: B -> C.
    let hop2: String = conn
        .query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![hop1],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(hop2, id_c);

    // Verify C is not redirected (no further hops).
    let final_hop = conn.query_row(
        "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
        params![id_c],
        |r| r.get::<_, String>(0),
    );
    assert!(
        final_hop.is_err(),
        "canonical artist should have no redirect"
    );
}

// ---------------------------------------------------------------------------
// 3. redirect_with_query
// ---------------------------------------------------------------------------

/// Create artist, feed with credit, insert redirect from old_artist_id, verify
/// the join query returns the correct new artist name.
#[test]
fn redirect_with_query() {
    let conn = common::test_db();
    let now = common::now();

    let old_id = uuid::Uuid::new_v4().to_string();
    let new_artist_id = insert_artist(&conn, "Canonical Band");
    let credit_id = insert_credit(&conn, &new_artist_id, "Canonical Band");

    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/canonical.xml",
        "Canonical Album",
        credit_id,
    );

    // The old_id is not a real artist row, but it is used as a redirect source.
    // We need it as a real artist for the FK to work.
    let ts = common::now();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![old_id, "Old Band Name", "old band name", ts, ts],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![old_id, new_artist_id, now],
    )
    .unwrap();

    // The join query: given the old_artist_id, find the new artist's name.
    let resolved_name: String = conn
        .query_row(
            "SELECT a.name FROM artist_id_redirect r \
             JOIN artists a ON a.artist_id = r.new_artist_id \
             WHERE r.old_artist_id = ?1",
            params![old_id],
            |r| r.get(0),
        )
        .unwrap();

    assert_eq!(resolved_name, "Canonical Band");
}
