mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// Shared helpers
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

/// Insert an artist_credit row with an explicit integer id and return that id.
fn insert_credit(conn: &rusqlite::Connection, id: i64, display_name: &str) {
    let now = common::now();
    conn.execute(
        "INSERT INTO artist_credit (id, display_name, created_at) VALUES (?1, ?2, ?3)",
        params![id, display_name, now],
    )
    .unwrap();
}

/// Insert an artist_credit_name row.
fn insert_credit_name(
    conn: &rusqlite::Connection,
    artist_credit_id: i64,
    artist_id: &str,
    position: i64,
    name: &str,
    join_phrase: &str,
) {
    conn.execute(
        "INSERT INTO artist_credit_name \
         (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![artist_credit_id, artist_id, position, name, join_phrase],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 1. artist_credit_single_creation
// ---------------------------------------------------------------------------

/// Creating a single-artist credit produces one artist_credit row and one
/// artist_credit_name row with the correct display_name and artist_id.
#[test]
fn artist_credit_single_creation() {
    let conn = common::test_db();
    let artist_id = insert_artist(&conn, "Zola Jesus");
    insert_credit(&conn, 1, "Zola Jesus");
    insert_credit_name(&conn, 1, &artist_id, 0, "Zola Jesus", "");

    // artist_credit row exists with correct display_name.
    let display: String = conn
        .query_row(
            "SELECT display_name FROM artist_credit WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(display, "Zola Jesus");

    // Exactly one artist_credit_name row linked to this credit.
    let name_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit_name WHERE artist_credit_id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(name_count, 1);

    // The name row points to the right artist.
    let linked_artist: String = conn
        .query_row(
            "SELECT artist_id FROM artist_credit_name WHERE artist_credit_id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(linked_artist, artist_id);
}

// ---------------------------------------------------------------------------
// 2. artist_credit_multi_creation
// ---------------------------------------------------------------------------

/// A two-artist credit ("Alice & Bob") stores both artist_credit_name rows
/// with the expected positions and join phrases.
#[test]
fn artist_credit_multi_creation() {
    let conn = common::test_db();
    let alice_id = insert_artist(&conn, "Alice");
    let bob_id = insert_artist(&conn, "Bob");

    insert_credit(&conn, 10, "Alice & Bob");
    insert_credit_name(&conn, 10, &alice_id, 0, "Alice", " & ");
    insert_credit_name(&conn, 10, &bob_id, 1, "Bob", "");

    // Two name rows under this credit.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit_name WHERE artist_credit_id = 10",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);

    // Alice is at position 0 with join phrase " & ".
    let (alice_name, alice_join): (String, String) = conn
        .query_row(
            "SELECT name, join_phrase FROM artist_credit_name \
             WHERE artist_credit_id = 10 AND position = 0",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(alice_name, "Alice");
    assert_eq!(alice_join, " & ");

    // Bob is at position 1 with empty join phrase.
    let (bob_name, bob_join): (String, String) = conn
        .query_row(
            "SELECT name, join_phrase FROM artist_credit_name \
             WHERE artist_credit_id = 10 AND position = 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(bob_name, "Bob");
    assert_eq!(bob_join, "");
}

// ---------------------------------------------------------------------------
// 3. artist_credit_appears_on_feed
// ---------------------------------------------------------------------------

/// A feed created with a given artist_credit_id stores that id and can be
/// joined back to the artist_credit table.
#[test]
fn artist_credit_appears_on_feed() {
    let conn = common::test_db();
    let now = common::now();
    let artist_id = insert_artist(&conn, "Nils Frahm");
    insert_credit(&conn, 20, "Nils Frahm");
    insert_credit_name(&conn, 20, &artist_id, 0, "Nils Frahm", "");

    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, \
         artist_credit_id, explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?6)",
        params![
            "feed-nils",
            "https://example.com/nils.xml",
            "Felt",
            "felt",
            20_i64,
            now,
        ],
    )
    .unwrap();

    let stored_credit_id: i64 = conn
        .query_row(
            "SELECT artist_credit_id FROM feeds WHERE feed_guid = 'feed-nils'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_credit_id, 20);

    // Verify the join to artist_credit returns the correct display_name.
    let display: String = conn
        .query_row(
            "SELECT ac.display_name \
             FROM feeds f \
             JOIN artist_credit ac ON ac.id = f.artist_credit_id \
             WHERE f.feed_guid = 'feed-nils'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(display, "Nils Frahm");
}

// ---------------------------------------------------------------------------
// 4. artist_credit_appears_on_track
// ---------------------------------------------------------------------------

/// A track created with a given artist_credit_id stores that id and the join
/// to artist_credit returns the expected display_name.
#[test]
fn artist_credit_appears_on_track() {
    let conn = common::test_db();
    let now = common::now();
    let artist_id = insert_artist(&conn, "Grouper");
    insert_credit(&conn, 30, "Grouper");
    insert_credit_name(&conn, 30, &artist_id, 0, "Grouper", "");

    // Feed (required FK for track).
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, \
         artist_credit_id, explicit, episode_count, created_at, updated_at) \
         VALUES ('feed-grouper', 'https://example.com/grouper.xml', \
         'Dragging a Dead Deer', 'dragging a dead deer', 30, 0, 0, ?1, ?1)",
        params![now],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, \
         title, title_lower, explicit, created_at, updated_at) \
         VALUES ('track-grouper-1', 'feed-grouper', 30, \
         'Heavy Water', 'heavy water', 0, ?1, ?1)",
        params![now],
    )
    .unwrap();

    let stored_credit_id: i64 = conn
        .query_row(
            "SELECT artist_credit_id FROM tracks WHERE track_guid = 'track-grouper-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_credit_id, 30);

    // Join back to artist_credit.
    let display: String = conn
        .query_row(
            "SELECT ac.display_name \
             FROM tracks t \
             JOIN artist_credit ac ON ac.id = t.artist_credit_id \
             WHERE t.track_guid = 'track-grouper-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(display, "Grouper");
}

// ---------------------------------------------------------------------------
// 5. artist_credit_idempotent_by_display_name
// ---------------------------------------------------------------------------

/// INSERT OR IGNORE on the primary key means a second attempt to insert a
/// credit with the same explicit id is a no-op — the COUNT stays at 1.
#[test]
fn artist_credit_idempotent_by_display_name() {
    let conn = common::test_db();
    let now = common::now();

    // First insert.
    conn.execute(
        "INSERT OR IGNORE INTO artist_credit (id, display_name, created_at) \
         VALUES (99, 'Test', ?1)",
        params![now],
    )
    .unwrap();

    // Second insert with the same id — must be a no-op due to PK constraint.
    let rows_changed = conn
        .execute(
            "INSERT OR IGNORE INTO artist_credit (id, display_name, created_at) \
             VALUES (99, 'Test', ?1)",
            params![now],
        )
        .unwrap();
    assert_eq!(rows_changed, 0, "second INSERT OR IGNORE must not insert a new row");

    // Exactly one row should exist.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM artist_credit WHERE id = 99",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "only one artist_credit row with id=99 must exist");
}

// ---------------------------------------------------------------------------
// 6. artist_credit_names_ordered_by_position
// ---------------------------------------------------------------------------

/// Inserting three artist_credit_name rows at positions 2, 0, 1 (out of order)
/// and querying them back ORDER BY position returns the names in the correct
/// positional order.
#[test]
fn artist_credit_names_ordered_by_position() {
    let conn = common::test_db();
    let id_a = insert_artist(&conn, "Artist A");
    let id_b = insert_artist(&conn, "Artist B");
    let id_c = insert_artist(&conn, "Artist C");

    insert_credit(&conn, 50, "Artist A, Artist B & Artist C");

    // Insert deliberately out of order.
    insert_credit_name(&conn, 50, &id_c, 2, "Artist C", "");
    insert_credit_name(&conn, 50, &id_a, 0, "Artist A", ", ");
    insert_credit_name(&conn, 50, &id_b, 1, "Artist B", " & ");

    let mut stmt = conn
        .prepare(
            "SELECT name FROM artist_credit_name \
             WHERE artist_credit_id = 50 \
             ORDER BY position ASC",
        )
        .unwrap();

    let names: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(names, vec!["Artist A", "Artist B", "Artist C"]);

    // Also verify the join phrases round-trip correctly.
    let mut phrase_stmt = conn
        .prepare(
            "SELECT join_phrase FROM artist_credit_name \
             WHERE artist_credit_id = 50 \
             ORDER BY position ASC",
        )
        .unwrap();

    let phrases: Vec<String> = phrase_stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(phrases, vec![", ", " & ", ""]);
}
