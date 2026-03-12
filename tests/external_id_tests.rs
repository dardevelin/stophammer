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

// ---------------------------------------------------------------------------
// 1. external_id_linked
// ---------------------------------------------------------------------------

/// Insert an external_id for an artist (scheme="musicbrainz", value="some-mbid"),
/// query back, verify fields.
#[test]
fn external_id_linked() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Bjork");
    let mbid = uuid::Uuid::new_v4().to_string();

    conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params!["artist", artist_id, "musicbrainz", mbid, now],
    )
    .unwrap();

    let (entity_type, entity_id, scheme, value): (String, String, String, String) = conn
        .query_row(
            "SELECT entity_type, entity_id, scheme, value \
             FROM external_ids WHERE entity_id = ?1 AND scheme = ?2",
            params![artist_id, "musicbrainz"],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();

    assert_eq!(entity_type, "artist");
    assert_eq!(entity_id, artist_id);
    assert_eq!(scheme, "musicbrainz");
    assert_eq!(value, mbid);
}

// ---------------------------------------------------------------------------
// 2. external_id_uniqueness
// ---------------------------------------------------------------------------

/// INSERT two external IDs with same (entity_type, entity_id, scheme), second
/// should conflict (UNIQUE constraint). Use INSERT OR REPLACE to update, verify
/// only one row.
#[test]
fn external_id_uniqueness() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Sigur Ros");

    // First insert.
    conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params!["artist", artist_id, "musicbrainz", "old-mbid-1234", now],
    )
    .unwrap();

    // Second insert with same (entity_type, entity_id, scheme) but different value.
    // Use INSERT OR REPLACE to update.
    conn.execute(
        "INSERT OR REPLACE INTO external_ids (entity_type, entity_id, scheme, value, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params!["artist", artist_id, "musicbrainz", "new-mbid-5678", now],
    )
    .unwrap();

    // Verify only one row exists for this entity+scheme.
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM external_ids \
             WHERE entity_type = 'artist' AND entity_id = ?1 AND scheme = 'musicbrainz'",
            params![artist_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "only one external_id row must exist after replace");

    // Verify the value was updated.
    let stored_value: String = conn
        .query_row(
            "SELECT value FROM external_ids \
             WHERE entity_type = 'artist' AND entity_id = ?1 AND scheme = 'musicbrainz'",
            params![artist_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_value, "new-mbid-5678");
}

// ---------------------------------------------------------------------------
// 3. external_id_reverse_lookup
// ---------------------------------------------------------------------------

/// Insert external_id, query by scheme+value to find the entity.
#[test]
fn external_id_reverse_lookup() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Massive Attack");
    let mbid = "mb-massive-attack-9999";

    conn.execute(
        "INSERT INTO external_ids (entity_type, entity_id, scheme, value, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params!["artist", artist_id, "musicbrainz", mbid, now],
    )
    .unwrap();

    let (entity_type, entity_id): (String, String) = conn
        .query_row(
            "SELECT entity_type, entity_id FROM external_ids \
             WHERE scheme = ?1 AND value = ?2",
            params!["musicbrainz", mbid],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();

    assert_eq!(entity_type, "artist");
    assert_eq!(entity_id, artist_id);
}

// ---------------------------------------------------------------------------
// 4. entity_source_recorded
// ---------------------------------------------------------------------------

/// Insert into entity_source table, query back, verify source_type, trust_level,
/// and source_url.
#[test]
fn entity_source_recorded() {
    let conn = common::test_db();
    let now = common::now();

    let artist_id = insert_artist(&conn, "Portishead");

    conn.execute(
        "INSERT INTO entity_source (entity_type, entity_id, source_type, source_url, trust_level, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params!["artist", artist_id, "musicbrainz_import", "https://musicbrainz.org/artist/xyz", 3, now],
    )
    .unwrap();

    let (source_type, source_url, trust_level): (String, Option<String>, i64) = conn
        .query_row(
            "SELECT source_type, source_url, trust_level \
             FROM entity_source WHERE entity_type = ?1 AND entity_id = ?2",
            params!["artist", artist_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();

    assert_eq!(source_type, "musicbrainz_import");
    assert_eq!(
        source_url.as_deref(),
        Some("https://musicbrainz.org/artist/xyz")
    );
    assert_eq!(trust_level, 3);
}
