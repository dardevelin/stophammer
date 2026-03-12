//! Full-text search over the contentless FTS5 `search_index` table.
//!
//! The FTS5 table uses `content=''`, which means it is a contentless index:
//! data is stored for MATCH queries but cannot be read back with SELECT.
//! We manage rowids ourselves using a deterministic hash of
//! `entity_type + entity_id`.

#![allow(dead_code)]

use std::hash::{DefaultHasher, Hash, Hasher};

use rusqlite::{Connection, params};

use crate::db::DbError;

/// A single search result returned by [`search`].
pub struct SearchResult {
    pub entity_type:   String,
    pub entity_id:     String,
    pub rank:          f64,
    pub quality_score: i64,
}

/// Computes a deterministic positive `i64` rowid from entity type and id.
///
/// FTS5 with `content=''` requires us to manage rowids ourselves so that
/// updates and deletes can target the correct row.
fn rowid_for(entity_type: &str, entity_id: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    entity_type.hash(&mut hasher);
    entity_id.hash(&mut hasher);
    // Mask to 63 bits so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF).cast_signed()
}

/// Inserts or replaces a row in the `search_index` FTS5 table.
///
/// Because the table is contentless we first delete any existing row for this
/// entity (by rowid) and then insert a fresh one. This avoids duplicate
/// entries when the same entity is re-indexed.
pub fn populate_search_index(
    conn: &Connection,
    entity_type: &str,
    entity_id: &str,
    name: &str,
    title: &str,
    description: &str,
    tags: &str,
) -> Result<(), DbError> {
    let rowid = rowid_for(entity_type, entity_id);

    // Delete-then-insert pattern for contentless FTS5 tables.
    conn.execute(
        "INSERT INTO search_index(search_index, rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES('delete', ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![rowid, entity_type, entity_id, name, title, description, tags],
    ).ok(); // Ignore error when row does not yet exist.

    conn.execute(
        "INSERT INTO search_index(rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![rowid, entity_type, entity_id, name, title, description, tags],
    )?;

    Ok(())
}

/// Removes the search index entry for the given entity.
pub fn delete_from_search_index(
    conn: &Connection,
    entity_type: &str,
    entity_id: &str,
    name: &str,
    title: &str,
    description: &str,
    tags: &str,
) -> Result<(), DbError> {
    let rowid = rowid_for(entity_type, entity_id);

    conn.execute(
        "INSERT INTO search_index(search_index, rowid, entity_type, entity_id, name, title, description, tags) \
         VALUES('delete', ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![rowid, entity_type, entity_id, name, title, description, tags],
    )?;

    Ok(())
}

/// Searches the FTS5 index using a `MATCH` query, ordered by BM25 rank
/// weighted by an optional quality score from `entity_quality`.
///
/// `entity_type_filter` — if `Some`, restricts results to that entity type.
/// `limit` and `offset` provide pagination.
pub fn search(
    conn: &Connection,
    query: &str,
    entity_type_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<SearchResult>, DbError> {
    let sql = if entity_type_filter.is_some() {
        "SELECT s.entity_type, s.entity_id, s.rank, \
                COALESCE(q.score, 0) AS quality_score \
         FROM search_index s \
         LEFT JOIN entity_quality q \
           ON q.entity_type = s.entity_type AND q.entity_id = s.entity_id \
         WHERE search_index MATCH ?1 \
           AND s.entity_type = ?2 \
         ORDER BY (s.rank * (1.0 + CAST(COALESCE(q.score, 0) AS REAL) / 100.0)) \
         LIMIT ?3 OFFSET ?4"
    } else {
        "SELECT s.entity_type, s.entity_id, s.rank, \
                COALESCE(q.score, 0) AS quality_score \
         FROM search_index s \
         LEFT JOIN entity_quality q \
           ON q.entity_type = s.entity_type AND q.entity_id = s.entity_id \
         WHERE search_index MATCH ?1 \
         ORDER BY (s.rank * (1.0 + CAST(COALESCE(q.score, 0) AS REAL) / 100.0)) \
         LIMIT ?3 OFFSET ?4"
    };

    let filter = entity_type_filter.unwrap_or("");
    let mut stmt = conn.prepare(sql)?;

    let rows = stmt.query_map(params![query, filter, limit, offset], |row| {
        Ok(SearchResult {
            entity_type:   row.get(0)?,
            entity_id:     row.get(1)?,
            rank:          row.get(2)?,
            quality_score: row.get(3)?,
        })
    })?;

    let mut results = Vec::new();
    for row in rows {
        results.push(row?);
    }

    Ok(results)
}
