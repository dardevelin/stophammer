mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// FTS5 contentless mode notes:
//
// With `content=''` the FTS5 table stores tokens for MATCH but column values
// read back as NULL.  We therefore verify search results by checking that the
// expected rowid(s) are returned by a MATCH query.  Production code stores
// entity_type + entity_id in the `entity_quality` table and JOINs on rowid.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 1. search_by_name — insert artist row into search_index, MATCH by name,
//    verify the expected rowid is returned
// ---------------------------------------------------------------------------

#[test]
fn search_by_name() {
    let conn = common::test_db();

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (1, 'artist', 'a1', 'Radiohead', '', '', '')",
        [],
    )
    .unwrap();

    let mut stmt = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let rows: Vec<i64> = stmt
        .query_map(params!["radiohead"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], 1, "should return the rowid of the Radiohead entry");
}

// ---------------------------------------------------------------------------
// 2. search_by_title — insert feed into search_index, MATCH by title word
// ---------------------------------------------------------------------------

#[test]
fn search_by_title() {
    let conn = common::test_db();

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (1, 'feed', 'f1', '', 'OK Computer', '', '')",
        [],
    )
    .unwrap();

    let mut stmt = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let rows: Vec<i64> = stmt
        .query_map(params!["computer"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], 1, "should find the feed by a word in its title");
}

// ---------------------------------------------------------------------------
// 3. search_type_filter — insert artist + feed, use column filter in MATCH
//    to restrict to entity_type column containing 'artist'
// ---------------------------------------------------------------------------

#[test]
fn search_type_filter() {
    let conn = common::test_db();

    // Insert an artist with "sonic" in the name column
    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (1, 'artist', 'a1', 'Sonic Youth', '', '', '')",
        [],
    )
    .unwrap();

    // Insert a feed with "sonic" in the title column
    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (2, 'feed', 'f1', '', 'Sonic Highways', '', '')",
        [],
    )
    .unwrap();

    // Without filter: both rows match "sonic"
    let mut stmt_all = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let all_rows: Vec<i64> = stmt_all
        .query_map(params!["sonic"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(all_rows.len(), 2, "both rows contain 'sonic'");

    // With column filter: only entity_type='artist' matches
    let mut stmt_filtered = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let filtered_rows: Vec<i64> = stmt_filtered
        .query_map(params!["entity_type:artist"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(filtered_rows.len(), 1, "only the artist row should match");
    assert_eq!(filtered_rows[0], 1);
}

// ---------------------------------------------------------------------------
// 4. search_no_results — query for a term that doesn't exist, verify empty
// ---------------------------------------------------------------------------

#[test]
fn search_no_results() {
    let conn = common::test_db();

    let mut stmt = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let rows: Vec<i64> = stmt
        .query_map(params!["nonexistenttermxyz"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 0, "should return no results for a nonexistent term");
}

// ---------------------------------------------------------------------------
// 5. search_multiple_results — insert 3 artist rows with "rock" in tags,
//    verify all 3 rowids returned
// ---------------------------------------------------------------------------

#[test]
fn search_multiple_results() {
    let conn = common::test_db();

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (1, 'artist', 'a1', 'Band One', '', '', 'rock alternative')",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (2, 'artist', 'a2', 'Band Two', '', '', 'rock punk')",
        [],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES (3, 'artist', 'a3', 'Band Three', '', '', 'rock metal')",
        [],
    )
    .unwrap();

    let mut stmt = conn
        .prepare("SELECT rowid FROM search_index WHERE search_index MATCH ?1")
        .unwrap();

    let rows: Vec<i64> = stmt
        .query_map(params!["rock"], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3, "all 3 artists with 'rock' in tags should be returned");

    let mut sorted = rows.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2, 3]);
}

// ---------------------------------------------------------------------------
// 6. quality_score_stored — insert into entity_quality, read back, verify
//    score and computed_at
// ---------------------------------------------------------------------------

#[test]
fn quality_score_stored() {
    let conn = common::test_db();
    let now = common::now();

    conn.execute(
        "INSERT INTO entity_quality (entity_type, entity_id, score, computed_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params!["artist", "a1", 85, now],
    )
    .unwrap();

    let (score, computed_at): (i64, i64) = conn
        .query_row(
            "SELECT score, computed_at FROM entity_quality \
             WHERE entity_type = ?1 AND entity_id = ?2",
            params!["artist", "a1"],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();

    assert_eq!(score, 85);
    assert_eq!(computed_at, now);
}
