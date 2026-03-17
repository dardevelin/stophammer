//! Database access layer for stophammer.
//!
//! All SQL operations are collected here: schema initialisation, per-entity
//! upserts, event insertion, crawl-cache management, and the single
//! `ingest_transaction` that writes an entire feed ingest atomically.
//!
//! Errors are surfaced as [`DbError`], which wraps rusqlite and `serde_json`
//! failures. `api.rs` pattern-matches on the variants to produce appropriate
//! HTTP status codes, so the typed error is intentional.

use std::fmt;
use std::sync::{Arc, Mutex};
use rusqlite::{Connection, OptionalExtension, params};
use crate::model::{Artist, ArtistCredit, ArtistCreditName, Feed, FeedPaymentRoute, PaymentRoute, RouteType, Track, ValueTimeSplit};
use crate::event::{Event, EventPayload, EventType};
use crate::signing::NodeSigner; // Issue-SEQ-INTEGRITY — 2026-03-14

pub type Db = Arc<Mutex<Connection>>;

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors returned by all database operations in this module.
// Mutex safety compliant — 2026-03-12
pub enum DbError {
    /// A rusqlite operation failed (query, execute, or schema application).
    Rusqlite(rusqlite::Error),
    /// A JSON serialisation or deserialisation step failed.
    Json(serde_json::Error),
    /// The database mutex was poisoned (a thread panicked while holding the lock).
    Poisoned,
    /// A non-SQLite, non-JSON error (e.g. connection pool failure).
    // Issue-WAL-POOL — 2026-03-14
    Other(String),
}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Rusqlite(e)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rusqlite(e) => write!(f, "SQLite error: {e}"),
            Self::Json(e)     => write!(f, "JSON error: {e}"),
            Self::Poisoned    => write!(f, "database mutex poisoned"),
            Self::Other(msg)  => write!(f, "{msg}"),
        }
    }
}

impl fmt::Debug for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rusqlite(e) => Some(e),
            Self::Json(e)     => Some(e),
            Self::Poisoned    => None,
            Self::Other(_)    => None,
        }
    }
}

// ── EventRow ─────────────────────────────────────────────────────────────────

/// A pre-assembled event ready to be written to the `events` table.
///
/// `signed_by` and `signature` are no longer stored here because the
/// signature covers the DB-assigned `seq` (Issue-SEQ-INTEGRITY). The
/// `NodeSigner` passed to [`ingest_transaction`] signs each event after
/// insertion and updates the row with the real signature.
// CRIT-03 Debug derive — 2026-03-13
// Issue-SEQ-INTEGRITY — 2026-03-14
#[derive(Debug)]
pub struct EventRow {
    /// Globally unique identifier for this event (UUID v4).
    pub event_id:     String,
    /// Discriminant describing the kind of state change this event records.
    pub event_type:   EventType,
    /// Canonical JSON representation of the event-specific payload.
    pub payload_json: String,
    /// GUID of the primary entity this event concerns (feed, track, etc.).
    pub subject_guid: String,
    /// Unix timestamp (seconds) at which the event was created.
    pub created_at:   i64,
    /// Human-readable warnings produced by the verifier chain, if any.
    pub warnings:     Vec<String>,
}

// ── ExternalIdRow ──────────────────────────────────────────────────────────────

/// A row from the `external_ids` table linking an entity to an external system.
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct ExternalIdRow {
    pub id:     i64,
    pub scheme: String,
    pub value:  String,
}

// ── EntitySourceRow ────────────────────────────────────────────────────────────

/// A row from the `entity_source` table recording where an entity came from.
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct EntitySourceRow {
    pub id:          i64,
    pub source_type: String,
    pub source_url:  Option<String>,
    pub trust_level: i64,
    pub created_at:  i64,
}

// ── Migrations ───────────────────────────────────────────────────────────────
// Issue-MIGRATIONS — 2026-03-14

/// Ordered list of schema migrations.  Each entry is a SQL script that is
/// applied exactly once, inside its own transaction.  The `schema_migrations`
/// table tracks which versions have already been applied, so restarts never
/// re-execute earlier migrations and data is never silently dropped.
const MIGRATIONS: &[&str] = &[
    // Migration 1: baseline schema (formerly src/schema.sql, all DROPs removed)
    include_str!("../migrations/0001_baseline.sql"),
    // Migration 2: scope artist credits to feed_guid (Issue-ARTIST-IDENTITY — 2026-03-14)
    include_str!("../migrations/0002_artist_credit_feed_scope.sql"),
    // Migration 3: unique constraint on search_entities (Issue-HASH-COLLISION — 2026-03-14)
    include_str!("../migrations/0003_search_entities_unique.sql"),
    // Migration 4: add proof_level to proof_tokens (Issue-PROOF-LEVEL — 2026-03-14)
    include_str!("../migrations/0004_proof_level.sql"),
];

/// Applies any pending schema migrations to `conn`.
///
/// On the very first run the `schema_migrations` table is created.  Each
/// migration runs inside a transaction so that a failure rolls back cleanly
/// without leaving the database in a half-migrated state.
///
/// # Errors
///
/// Returns [`DbError`] if any migration SQL fails or if the bookkeeping
/// queries fail.
fn run_migrations(conn: &mut Connection) -> Result<(), DbError> {
    // Ensure the tracker table exists (idempotent).
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL
        );"
    )?;

    let current: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
        [],
        |r| r.get(0),
    )?;

    for (idx, sql) in MIGRATIONS.iter().enumerate() {
        // Migration versions are 1-indexed; the array will never have enough
        // entries for the index to overflow i64.
        let version = i64::try_from(idx).expect("migration count overflowed i64") + 1;
        if version > current {
            // Issue-CHECKED-TX — 2026-03-16: conn is freshly opened in open_db, no nesting.
            let tx = conn.transaction()?;
            tx.execute_batch(sql)?;
            tx.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                params![version, unix_now()],
            )?;
            tx.commit()?;
        }
    }

    Ok(())
}

// ── open_db ──────────────────────────────────────────────────────────────────

/// Opens the `SQLite` database at `path` and runs pending schema migrations.
///
/// PRAGMAs are applied before migrations so that WAL mode, foreign keys, and
/// synchronous settings are active for all subsequent operations.
///
/// # Panics
///
/// Panics if the file cannot be opened (e.g. permission denied) or if a
/// migration fails to apply. Both are unrecoverable startup failures.
#[must_use]
// SP-01 stable FTS5 hash — 2026-03-13
// Note: The FTS5 table uses content='' (contentless), so the 'rebuild' command
// is not available. Hash stability is enforced by using SipHash-2-4 with fixed
// keys in search::rowid_for. If the hash ever changes, the index must be
// dropped and re-populated from the source tables.
// HIGH-02 impl AsRef<Path> param — 2026-03-13
pub fn open_db(path: impl AsRef<std::path::Path>) -> Connection {
    let mut conn = Connection::open(path.as_ref()).expect("failed to open database");
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\n\
         PRAGMA foreign_keys = ON;\n\
         PRAGMA synchronous = NORMAL;"
    ).expect("failed to set PRAGMAs");
    run_migrations(&mut conn).expect("failed to apply migrations");
    conn
}

// ── Helper: serialize EventType to snake_case string (no quotes) ─────────────

fn event_type_str(et: &EventType) -> Result<String, DbError> {
    let s = serde_json::to_string(et)?;
    Ok(s.trim_matches('"').to_string())
}

// ── Helper: current unix timestamp ──────────────────────────────────────────

// SP-05 epoch guard — 2026-03-12
/// Returns the current Unix timestamp in seconds.
///
/// # Panics
///
/// Panics if the system clock is before the Unix epoch (1970-01-01T00:00:00Z).
/// This indicates a misconfigured system clock that would corrupt all
/// time-based operations.
#[must_use]
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch — check system time configuration")
        .as_secs()
        .cast_signed()
}

// ── resolve_artist ────────────────────────────────────────────────────────────
// Issue-ARTIST-IDENTITY — 2026-03-14

/// Returns an existing artist matched by alias or lowercased `name`, scoped to
/// a specific feed when `feed_guid` is provided, or inserts a new one and
/// auto-registers its canonical name as an alias.
///
/// When `feed_guid` is `Some`, alias and name lookups are scoped to artists
/// already linked to that feed (via `artist_aliases.feed_guid`). This prevents
/// cross-feed name collisions where two unrelated podcasts with the same
/// `owner_name` would otherwise share an artist record.
///
/// Resolution order:
/// 1. `artist_aliases.alias_lower = name.to_lowercase()` scoped by
///    `feed_guid` — alias lookup.
/// 2. Insert new artist + insert a feed-scoped canonical alias row.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
pub fn resolve_artist(
    conn: &Connection,
    name: &str,
    feed_guid: Option<&str>,
) -> Result<Artist, DbError> {
    let name_lower = name.to_lowercase();
    let now = unix_now();

    // 1. Check alias table, scoped by feed_guid.
    let via_alias: Option<Artist> = if let Some(fg) = feed_guid {
        conn.query_row(
            "SELECT a.artist_id, a.name, a.name_lower, a.sort_name, a.type_id, a.area, \
             a.img_url, a.url, a.begin_year, a.end_year, a.created_at, a.updated_at \
             FROM artist_aliases aa \
             JOIN artists a ON a.artist_id = aa.artist_id \
             WHERE aa.alias_lower = ?1 AND aa.feed_guid = ?2 \
             LIMIT 1",
            params![name_lower, fg],
            |row| {
                Ok(Artist {
                    artist_id:  row.get(0)?,
                    name:       row.get(1)?,
                    name_lower: row.get(2)?,
                    sort_name:  row.get(3)?,
                    type_id:    row.get(4)?,
                    area:       row.get(5)?,
                    img_url:    row.get(6)?,
                    url:        row.get(7)?,
                    begin_year: row.get(8)?,
                    end_year:   row.get(9)?,
                    created_at: row.get(10)?,
                    updated_at: row.get(11)?,
                })
            },
        ).optional()?
    } else {
        conn.query_row(
            "SELECT a.artist_id, a.name, a.name_lower, a.sort_name, a.type_id, a.area, \
             a.img_url, a.url, a.begin_year, a.end_year, a.created_at, a.updated_at \
             FROM artist_aliases aa \
             JOIN artists a ON a.artist_id = aa.artist_id \
             WHERE aa.alias_lower = ?1 \
             LIMIT 1",
            params![name_lower],
            |row| {
                Ok(Artist {
                    artist_id:  row.get(0)?,
                    name:       row.get(1)?,
                    name_lower: row.get(2)?,
                    sort_name:  row.get(3)?,
                    type_id:    row.get(4)?,
                    area:       row.get(5)?,
                    img_url:    row.get(6)?,
                    url:        row.get(7)?,
                    begin_year: row.get(8)?,
                    end_year:   row.get(9)?,
                    created_at: row.get(10)?,
                    updated_at: row.get(11)?,
                })
            },
        ).optional()?
    };

    if let Some(a) = via_alias {
        return Ok(a);
    }

    // 2. New artist — insert artist row and its feed-scoped canonical alias.
    let artist_id = uuid::Uuid::new_v4().to_string();

    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![artist_id, name, name_lower, now, now],
    )?;

    conn.execute(
        "INSERT INTO artist_aliases (alias_lower, artist_id, feed_guid, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![name_lower, artist_id, feed_guid, now],
    )?;

    Ok(Artist {
        artist_id,
        name: name.to_string(),
        name_lower,
        sort_name:  None,
        type_id:    None,
        area:       None,
        img_url:    None,
        url:        None,
        begin_year: None,
        end_year:   None,
        created_at: now,
        updated_at: now,
    })
}

// ── get_artist_by_id ─────────────────────────────────────────────────────────
// Issue-12 PATCH emits events — 2026-03-13

/// Returns the artist row for `artist_id`, or `None` if absent.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_artist_by_id(conn: &Connection, artist_id: &str) -> Result<Option<Artist>, DbError> {
    let result = conn.query_row(
        "SELECT artist_id, name, name_lower, sort_name, type_id, area, \
         img_url, url, begin_year, end_year, created_at, updated_at \
         FROM artists WHERE artist_id = ?1",
        params![artist_id],
        |row| {
            Ok(Artist {
                artist_id:  row.get(0)?,
                name:       row.get(1)?,
                name_lower: row.get(2)?,
                sort_name:  row.get(3)?,
                type_id:    row.get(4)?,
                area:       row.get(5)?,
                img_url:    row.get(6)?,
                url:        row.get(7)?,
                begin_year: row.get(8)?,
                end_year:   row.get(9)?,
                created_at: row.get(10)?,
                updated_at: row.get(11)?,
            })
        },
    ).optional()?;
    Ok(result)
}

// ── artist_exists ────────────────────────────────────────────────────────────
// Issue-SSE-EXHAUSTION — 2026-03-15

/// Returns `true` if an artist with the given `artist_id` exists in the database.
///
/// Uses a lightweight `SELECT 1` query (no row parsing) so it is cheaper than
/// [`get_artist_by_id`] for pure existence checks.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn artist_exists(conn: &Connection, artist_id: &str) -> Result<bool, DbError> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM artists WHERE artist_id = ?1)",
            params![artist_id],
            |row| row.get(0),
        )?;
    Ok(exists)
}

// ── get_payment_routes_for_track ─────────────────────────────────────────────
// Issue-12 PATCH emits events — 2026-03-13

/// Returns all payment routes for `track_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_payment_routes_for_track(conn: &Connection, track_guid: &str) -> Result<Vec<PaymentRoute>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, track_guid, feed_guid, recipient_name, route_type, address, \
         custom_key, custom_value, split, fee \
         FROM payment_routes WHERE track_guid = ?1",
    )?;
    let rows = stmt.query_map(params![track_guid], |row| {
        let rt_str: String = row.get(4)?;
        let fee_i: i64 = row.get(9)?;
        Ok(PaymentRoute {
            id:             row.get(0)?,
            track_guid:     row.get(1)?,
            feed_guid:      row.get(2)?,
            recipient_name: row.get(3)?,
            route_type:     serde_json::from_str(&format!("\"{rt_str}\"")).unwrap_or(RouteType::Node),
            address:        row.get(5)?,
            custom_key:     row.get(6)?,
            custom_value:   row.get(7)?,
            split:          row.get(8)?,
            fee:            fee_i != 0,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

// ── get_value_time_splits_for_track ──────────────────────────────────────────
// Issue-12 PATCH emits events — 2026-03-13

/// Returns all value-time splits for `source_track_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_value_time_splits_for_track(conn: &Connection, track_guid: &str) -> Result<Vec<ValueTimeSplit>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, source_track_guid, start_time_secs, duration_secs, remote_feed_guid, \
         remote_item_guid, split, created_at \
         FROM value_time_splits WHERE source_track_guid = ?1",
    )?;
    let rows = stmt.query_map(params![track_guid], |row| {
        Ok(ValueTimeSplit {
            id:                row.get(0)?,
            source_track_guid: row.get(1)?,
            start_time_secs:   row.get(2)?,
            duration_secs:     row.get(3)?,
            remote_feed_guid:  row.get(4)?,
            remote_item_guid:  row.get(5)?,
            split:             row.get(6)?,
            created_at:        row.get(7)?,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

// ── add_artist_alias ──────────────────────────────────────────────────────────

/// Registers `alias` (lowercased) as an additional lookup key for `artist_id`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn add_artist_alias(conn: &Connection, artist_id: &str, alias: &str) -> Result<(), DbError> {
    let now = unix_now();
    conn.execute(
        "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
         VALUES (?1, ?2, ?3)",
        params![alias.to_lowercase(), artist_id, now],
    )?;
    Ok(())
}

// ── merge_artists ──────────────────────────────────────────────────────────────

/// Merges `source_artist_id` into `target_artist_id`.
///
/// All `artist_credit_name` entries pointing to `source` are repointed to `target`.
/// All aliases of `source` that do not already exist on `target` are transferred;
/// any that would conflict are dropped. The `source` artist row is then
/// deleted. Returns the list of alias strings that were transferred.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL statement or the transaction commit fails.
pub fn merge_artists(
    conn: &mut Connection,
    source_artist_id: &str,
    target_artist_id: &str,
) -> Result<Vec<String>, DbError> {
    let tx = conn.transaction()?;
    let transferred = merge_artists_sql(&tx, source_artist_id, target_artist_id)?;
    tx.commit()?;
    Ok(transferred)
}

/// Inner implementation of artist merge: executes all SQL operations on
/// the provided connection without managing its own transaction.  Callers
/// must ensure they are already inside a transaction or savepoint.
pub(crate) fn merge_artists_sql(
    conn: &Connection,
    source_artist_id: &str,
    target_artist_id: &str,
) -> Result<Vec<String>, DbError> {
    // Finding-1 alias transfer SQL fixed — 2026-03-13
    // Collect the aliases that will be transferred.
    let mut stmt = conn.prepare(
        "SELECT aa.alias_lower FROM artist_aliases aa \
         WHERE aa.artist_id = ?1 \
           AND NOT EXISTS ( \
               SELECT 1 FROM artist_aliases existing \
               WHERE existing.alias_lower = aa.alias_lower \
                 AND existing.artist_id = ?2 \
           )",
    )?;
    let transferred: Vec<String> = stmt
        .query_map(params![source_artist_id, target_artist_id], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    // Repoint artist_credit_name entries.
    conn.execute(
        "UPDATE artist_credit_name SET artist_id = ?1 WHERE artist_id = ?2",
        params![target_artist_id, source_artist_id],
    )?;

    // Transfer non-conflicting aliases (Finding-1 fix: use distinct table aliases).
    conn.execute(
        "UPDATE artist_aliases SET artist_id = ?1 \
         WHERE artist_id = ?2 \
           AND NOT EXISTS ( \
               SELECT 1 FROM artist_aliases existing \
               WHERE existing.alias_lower = artist_aliases.alias_lower \
                 AND existing.artist_id = ?1 \
           )",
        params![target_artist_id, source_artist_id],
    )?;

    // Drop any remaining source aliases (those that conflicted).
    conn.execute(
        "DELETE FROM artist_aliases WHERE artist_id = ?1",
        params![source_artist_id],
    )?;

    // Record redirect for old ID resolution.
    let now = unix_now();
    conn.execute(
        "INSERT OR REPLACE INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![source_artist_id, target_artist_id, now],
    )?;

    // Delete the source artist row.
    conn.execute(
        "DELETE FROM artists WHERE artist_id = ?1",
        params![source_artist_id],
    )?;

    Ok(transferred)
}

// ── upsert_artist_if_absent ───────────────────────────────────────────────────

/// Inserts the artist if no row with the same `artist_id` exists yet.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn upsert_artist_if_absent(conn: &Connection, artist: &Artist) -> Result<(), DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO artists (artist_id, name, name_lower, sort_name, type_id, area, \
         img_url, url, begin_year, end_year, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            artist.artist_id, artist.name, artist.name_lower,
            artist.sort_name, artist.type_id, artist.area,
            artist.img_url, artist.url, artist.begin_year, artist.end_year,
            artist.created_at, artist.updated_at,
        ],
    )?;
    Ok(())
}

// ── Artist credit operations ────────────────────────────────────────────────

/// Creates an artist credit with its constituent names. Returns the credit with
/// the assigned `id` populated.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL insert fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn create_artist_credit(
    conn: &Connection,
    display_name: &str,
    names: &[(String, String, String)],  // (artist_id, credited_name, join_phrase)
    feed_guid: Option<&str>,
) -> Result<ArtistCredit, DbError> {
    let now = unix_now();

    conn.execute(
        "INSERT INTO artist_credit (display_name, feed_guid, created_at) VALUES (?1, ?2, ?3)",
        params![display_name, feed_guid, now],
    )?;
    let credit_id = conn.last_insert_rowid();

    let mut credit_names = Vec::with_capacity(names.len());
    for (pos, (artist_id, name, join_phrase)) in names.iter().enumerate() {
        #[expect(clippy::cast_possible_wrap, reason = "artist credit position count never approaches i64::MAX")]
        let position = pos as i64;
        conn.execute(
            "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![credit_id, artist_id, position, name, join_phrase],
        )?;
        let acn_id = conn.last_insert_rowid();
        credit_names.push(ArtistCreditName {
            id:               acn_id,
            artist_credit_id: credit_id,
            artist_id:        artist_id.clone(),
            position,
            name:             name.clone(),
            join_phrase:      join_phrase.clone(),
        });
    }

    Ok(ArtistCredit {
        id:           credit_id,
        display_name: display_name.to_string(),
        feed_guid:    feed_guid.map(String::from),
        created_at:   now,
        names:        credit_names,
    })
}

/// Creates a simple single-artist credit and returns it.
///
/// # Errors
///
/// Returns [`DbError`] if the underlying credit creation fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn create_single_artist_credit(
    conn: &Connection,
    artist: &Artist,
    feed_guid: Option<&str>,
) -> Result<ArtistCredit, DbError> {
    create_artist_credit(
        conn,
        &artist.name,
        &[(artist.artist_id.clone(), artist.name.clone(), String::new())],
        feed_guid,
    )
}

/// Retrieves an artist credit by ID, including its constituent names.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn get_artist_credit(conn: &Connection, credit_id: i64) -> Result<Option<ArtistCredit>, DbError> {
    let credit: Option<(i64, String, Option<String>, i64)> = conn.query_row(
        "SELECT id, display_name, feed_guid, created_at FROM artist_credit WHERE id = ?1",
        params![credit_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;

    let Some((id, display_name, feed_guid, created_at)) = credit else {
        return Ok(None);
    };

    let mut stmt = conn.prepare(
        "SELECT id, artist_credit_id, artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id = ?1 ORDER BY position",
    )?;
    let names: Vec<ArtistCreditName> = stmt.query_map(params![id], |row| {
        Ok(ArtistCreditName {
            id:               row.get(0)?,
            artist_credit_id: row.get(1)?,
            artist_id:        row.get(2)?,
            position:         row.get(3)?,
            name:             row.get(4)?,
            join_phrase:      row.get(5)?,
        })
    })?.collect::<Result<_, _>>()?;

    Ok(Some(ArtistCredit { id, display_name, feed_guid, created_at, names }))
}

// Issue-6 batch credits — 2026-03-13
/// Batch-loads multiple artist credits by ID in two queries instead of 2*N.
///
/// Returns a `HashMap<credit_id, ArtistCredit>` for O(1) lookup. Credits whose
/// IDs are not found in the database are silently omitted from the map.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
pub fn load_credits_batch(
    conn: &Connection,
    ids: &[i64],
) -> Result<std::collections::HashMap<i64, ArtistCredit>, DbError> {
    use std::collections::HashMap;

    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    // Deduplicate IDs to avoid redundant rows.
    let unique_ids: Vec<i64> = {
        let mut set = std::collections::HashSet::new();
        ids.iter().copied().filter(|id| set.insert(*id)).collect()
    };

    // Build a single parameterised placeholder string: ?,?,?
    let placeholders: String = std::iter::repeat_n("?", unique_ids.len())
        .collect::<Vec<_>>()
        .join(",");

    // Query 1: artist_credit rows.
    // Issue-ARTIST-IDENTITY — 2026-03-14
    let sql_credits = format!(
        "SELECT id, display_name, feed_guid, created_at FROM artist_credit WHERE id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql_credits)?;
    let params_credits: Vec<Box<dyn rusqlite::types::ToSql>> =
        unique_ids.iter().map(|id| Box::new(*id) as Box<dyn rusqlite::types::ToSql>).collect();
    let credit_rows = stmt.query_map(params_credits.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_slice(), |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, Option<String>>(2)?, row.get::<_, i64>(3)?))
    })?;

    let mut map: HashMap<i64, ArtistCredit> = HashMap::new();
    for row in credit_rows {
        let (id, display_name, feed_guid, created_at) = row?;
        map.insert(id, ArtistCredit { id, display_name, feed_guid, created_at, names: Vec::new() });
    }

    if map.is_empty() {
        return Ok(map);
    }

    // Query 2: artist_credit_name rows for all loaded credits.
    let sql_names = format!(
        "SELECT id, artist_credit_id, artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id IN ({placeholders}) ORDER BY artist_credit_id, position"
    );
    let mut stmt_names = conn.prepare(&sql_names)?;
    let params_names: Vec<Box<dyn rusqlite::types::ToSql>> =
        unique_ids.iter().map(|id| Box::new(*id) as Box<dyn rusqlite::types::ToSql>).collect();
    let name_rows = stmt_names.query_map(params_names.iter().map(AsRef::as_ref).collect::<Vec<_>>().as_slice(), |row| {
        Ok(ArtistCreditName {
            id:               row.get(0)?,
            artist_credit_id: row.get(1)?,
            artist_id:        row.get(2)?,
            position:         row.get(3)?,
            name:             row.get(4)?,
            join_phrase:      row.get(5)?,
        })
    })?;

    for row in name_rows {
        let acn = row?;
        if let Some(credit) = map.get_mut(&acn.artist_credit_id) {
            credit.names.push(acn);
        }
    }

    Ok(map)
}

/// Looks up an artist credit by display name (case-insensitive via `LOWER()`)
/// scoped to a specific feed when `feed_guid` is provided.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn get_artist_credit_by_display_name(
    conn: &Connection,
    display_name: &str,
    feed_guid: Option<&str>,
) -> Result<Option<ArtistCredit>, DbError> {
    let lower = display_name.to_lowercase();

    let credit: Option<(i64, String, Option<String>, i64)> = if let Some(fg) = feed_guid {
        conn.query_row(
            "SELECT id, display_name, feed_guid, created_at FROM artist_credit \
             WHERE LOWER(display_name) = ?1 AND feed_guid = ?2",
            params![lower, fg],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?
    } else {
        conn.query_row(
            "SELECT id, display_name, feed_guid, created_at FROM artist_credit \
             WHERE LOWER(display_name) = ?1 AND feed_guid IS NULL",
            params![lower],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        ).optional()?
    };

    let Some((id, display_name, feed_guid_val, created_at)) = credit else {
        return Ok(None);
    };

    let mut stmt = conn.prepare(
        "SELECT id, artist_credit_id, artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id = ?1 ORDER BY position",
    )?;
    let names: Vec<ArtistCreditName> = stmt.query_map(params![id], |row| {
        Ok(ArtistCreditName {
            id:               row.get(0)?,
            artist_credit_id: row.get(1)?,
            artist_id:        row.get(2)?,
            position:         row.get(3)?,
            name:             row.get(4)?,
            join_phrase:      row.get(5)?,
        })
    })?.collect::<Result<_, _>>()?;

    Ok(Some(ArtistCredit { id, display_name, feed_guid: feed_guid_val, created_at, names }))
}

/// Idempotent artist credit retrieval, scoped by feed.
///
/// Returns an existing credit if one with a matching `display_name`
/// (case-insensitive) already exists within the same feed scope, otherwise
/// creates a new credit with the given `names`.
///
/// # Errors
///
/// Returns [`DbError`] if the lookup or creation query fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn get_or_create_artist_credit(
    conn: &Connection,
    display_name: &str,
    names: &[(String, String, String)],  // (artist_id, credited_name, join_phrase)
    feed_guid: Option<&str>,
) -> Result<ArtistCredit, DbError> {
    if let Some(existing) = get_artist_credit_by_display_name(conn, display_name, feed_guid)? {
        return Ok(existing);
    }
    create_artist_credit(conn, display_name, names, feed_guid)
}

/// Returns all artist credits in which `artist_id` participates (via
/// `artist_credit_name` JOIN).
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
// Issue-ARTIST-IDENTITY — 2026-03-14
pub fn get_artist_credits_for_artist(
    conn: &Connection,
    artist_id: &str,
) -> Result<Vec<ArtistCredit>, DbError> {
    let mut credit_stmt = conn.prepare(
        "SELECT DISTINCT ac.id, ac.display_name, ac.feed_guid, ac.created_at \
         FROM artist_credit ac \
         JOIN artist_credit_name acn ON acn.artist_credit_id = ac.id \
         WHERE acn.artist_id = ?1 \
         ORDER BY ac.id",
    )?;
    let credits: Vec<(i64, String, Option<String>, i64)> = credit_stmt.query_map(params![artist_id], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })?.collect::<Result<_, _>>()?;

    let mut name_stmt = conn.prepare(
        "SELECT id, artist_credit_id, artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id = ?1 ORDER BY position",
    )?;

    let mut result = Vec::with_capacity(credits.len());
    for (id, display_name, feed_guid, created_at) in credits {
        let names: Vec<ArtistCreditName> = name_stmt.query_map(params![id], |row| {
            Ok(ArtistCreditName {
                id:               row.get(0)?,
                artist_credit_id: row.get(1)?,
                artist_id:        row.get(2)?,
                position:         row.get(3)?,
                name:             row.get(4)?,
                join_phrase:      row.get(5)?,
            })
        })?.collect::<Result<_, _>>()?;
        result.push(ArtistCredit { id, display_name, feed_guid, created_at, names });
    }

    Ok(result)
}

// ── upsert_feed ───────────────────────────────────────────────────────────────

/// Inserts or updates a feed row keyed on `feed_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
pub fn upsert_feed(conn: &Connection, feed: &Feed) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, created_at, \
         updated_at, raw_medium) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(feed_guid) DO UPDATE SET \
           feed_url         = excluded.feed_url, \
           title            = excluded.title, \
           title_lower      = excluded.title_lower, \
           artist_credit_id = excluded.artist_credit_id, \
           description      = excluded.description, \
           image_url        = excluded.image_url, \
           language         = excluded.language, \
           explicit         = excluded.explicit, \
           itunes_type      = excluded.itunes_type, \
           episode_count    = excluded.episode_count, \
           newest_item_at   = excluded.newest_item_at, \
           oldest_item_at   = excluded.oldest_item_at, \
           updated_at       = excluded.updated_at, \
           raw_medium       = excluded.raw_medium",
        params![
            feed.feed_guid,
            feed.feed_url,
            feed.title,
            feed.title_lower,
            feed.artist_credit_id,
            feed.description,
            feed.image_url,
            feed.language,
            i64::from(feed.explicit),
            feed.itunes_type,
            feed.episode_count,
            feed.newest_item_at,
            feed.oldest_item_at,
            feed.created_at,
            feed.updated_at,
            feed.raw_medium,
        ],
    )?;
    Ok(())
}

// ── upsert_track ──────────────────────────────────────────────────────────────

/// Inserts or updates a track row keyed on `track_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
pub fn upsert_track(conn: &Connection, track: &Track) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, \
         duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, season, \
         explicit, description, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(track_guid) DO UPDATE SET \
           feed_guid        = excluded.feed_guid, \
           artist_credit_id = excluded.artist_credit_id, \
           title            = excluded.title, \
           title_lower      = excluded.title_lower, \
           pub_date         = excluded.pub_date, \
           duration_secs    = excluded.duration_secs, \
           enclosure_url    = excluded.enclosure_url, \
           enclosure_type   = excluded.enclosure_type, \
           enclosure_bytes  = excluded.enclosure_bytes, \
           track_number     = excluded.track_number, \
           season           = excluded.season, \
           explicit         = excluded.explicit, \
           description      = excluded.description, \
           updated_at       = excluded.updated_at",
        params![
            track.track_guid,
            track.feed_guid,
            track.artist_credit_id,
            track.title,
            track.title_lower,
            track.pub_date,
            track.duration_secs,
            track.enclosure_url,
            track.enclosure_type,
            track.enclosure_bytes,
            track.track_number,
            track.season,
            i64::from(track.explicit),
            track.description,
            track.created_at,
            track.updated_at,
        ],
    )?;
    Ok(())
}

// ── replace_payment_routes ────────────────────────────────────────────────────

/// Deletes all payment routes for `track_guid` and inserts the new `routes`.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL delete/insert or JSON serialisation fails.
pub fn replace_payment_routes(
    conn: &Connection,
    track_guid: &str,
    routes: &[PaymentRoute],
) -> Result<(), DbError> {
    conn.execute("DELETE FROM payment_routes WHERE track_guid = ?1", params![track_guid])?;
    for r in routes {
        let route_type = serde_json::to_string(&r.route_type)?;
        let route_type = route_type.trim_matches('"');
        conn.execute(
            "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, \
             custom_key, custom_value, split, fee) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                r.track_guid,
                r.feed_guid,
                r.recipient_name,
                route_type,
                r.address,
                r.custom_key,
                r.custom_value,
                r.split,
                i64::from(r.fee),
            ],
        )?;
    }
    Ok(())
}

// ── replace_feed_payment_routes ─────────────────────────────────────────────

/// Deletes all feed-level payment routes for `feed_guid` and inserts `routes`.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL delete/insert or JSON serialisation fails.
pub fn replace_feed_payment_routes(
    conn: &Connection,
    feed_guid: &str,
    routes: &[FeedPaymentRoute],
) -> Result<(), DbError> {
    conn.execute("DELETE FROM feed_payment_routes WHERE feed_guid = ?1", params![feed_guid])?;
    for r in routes {
        let route_type = serde_json::to_string(&r.route_type)?;
        let route_type = route_type.trim_matches('"');
        conn.execute(
            "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, \
             custom_key, custom_value, split, fee) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                r.feed_guid,
                r.recipient_name,
                route_type,
                r.address,
                r.custom_key,
                r.custom_value,
                r.split,
                i64::from(r.fee),
            ],
        )?;
    }
    Ok(())
}

// ── replace_value_time_splits ─────────────────────────────────────────────────

/// Deletes all value-time splits for `source_track_guid` and inserts `splits`.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL delete or insert fails.
pub fn replace_value_time_splits(
    conn: &Connection,
    source_track_guid: &str,
    splits: &[ValueTimeSplit],
) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM value_time_splits WHERE source_track_guid = ?1",
        params![source_track_guid],
    )?;
    for s in splits {
        conn.execute(
            "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, \
             remote_feed_guid, remote_item_guid, split, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                s.source_track_guid,
                s.start_time_secs,
                s.duration_secs,
                s.remote_feed_guid,
                s.remote_item_guid,
                s.split,
                s.created_at,
            ],
        )?;
    }
    Ok(())
}

// ── delete_track ────────────────────────────────────────────────────────────

/// Cascade-deletes a track and all child rows, respecting FK constraints.
///
/// Deletes in order: `track_tag`, `value_time_splits`, `payment_routes`,
/// `entity_quality`, `entity_field_status`, then the `tracks` row itself.
///
/// Idempotent: calling with a non-existent `track_guid` is a no-op.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL delete fails.
pub fn delete_track(conn: &mut Connection, track_guid: &str) -> Result<(), DbError> {
    let sp = conn.savepoint()?;
    delete_track_sql(&sp, track_guid)?;
    sp.commit()?;
    Ok(())
}

/// Inner implementation of track cascade-delete: executes all SQL deletes on
/// the provided connection without managing its own transaction.  Callers
/// must ensure they are already inside a transaction or savepoint.
pub(crate) fn delete_track_sql(conn: &Connection, track_guid: &str) -> Result<(), DbError> {
    conn.execute(
        "DELETE FROM track_tag WHERE track_guid = ?1",
        params![track_guid],
    )?;
    conn.execute(
        "DELETE FROM value_time_splits WHERE source_track_guid = ?1",
        params![track_guid],
    )?;
    conn.execute(
        "DELETE FROM payment_routes WHERE track_guid = ?1",
        params![track_guid],
    )?;
    conn.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'track' AND entity_id = ?1",
        params![track_guid],
    )?;
    conn.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'track' AND entity_id = ?1",
        params![track_guid],
    )?;
    conn.execute(
        "DELETE FROM tracks WHERE track_guid = ?1",
        params![track_guid],
    )?;
    Ok(())
}

// ── delete_feed ─────────────────────────────────────────────────────────────

/// Cascade-deletes a feed and all child rows, respecting FK constraints.
///
/// Uses correlated subqueries (`WHERE col IN (SELECT track_guid FROM tracks
/// WHERE feed_guid = ?1)`) so that child-row deletion is O(1) SQL operations
/// regardless of the number of tracks. Deletes in dependency order: track-level
/// children, feed-level children, tracks, then the feed itself.
///
/// Idempotent: calling with a non-existent `feed_guid` is a no-op.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query or delete fails.
// DB performance compliant (subqueries) — 2026-03-12
pub fn delete_feed(conn: &mut Connection, feed_guid: &str) -> Result<(), DbError> {
    let sp = conn.savepoint()?;
    delete_feed_sql(&sp, feed_guid)?;
    sp.commit()?;
    Ok(())
}

/// Inner implementation of feed cascade-delete: executes all SQL deletes on
/// the provided connection without managing its own transaction.  Callers
/// must ensure they are already inside a transaction or savepoint.
// DB performance compliant (subqueries) — 2026-03-12
pub(crate) fn delete_feed_sql(conn: &Connection, feed_guid: &str) -> Result<(), DbError> {
    // 1. track_tag for all tracks in the feed (subquery)
    conn.execute(
        "DELETE FROM track_tag WHERE track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;

    // 2. feed_tag
    conn.execute(
        "DELETE FROM feed_tag WHERE feed_guid = ?1",
        params![feed_guid],
    )?;

    // 3. value_time_splits for all tracks (subquery)
    conn.execute(
        "DELETE FROM value_time_splits WHERE source_track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;

    // 4. payment_routes for all tracks (subquery)
    conn.execute(
        "DELETE FROM payment_routes WHERE track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;

    // 5. feed_payment_routes
    conn.execute(
        "DELETE FROM feed_payment_routes WHERE feed_guid = ?1",
        params![feed_guid],
    )?;

    // 6. entity_quality for all tracks (subquery) and the feed
    conn.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'track' AND entity_id IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    conn.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'feed' AND entity_id = ?1",
        params![feed_guid],
    )?;

    // 7. entity_field_status for all tracks (subquery) and the feed
    conn.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'track' AND entity_id IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    conn.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'feed' AND entity_id = ?1",
        params![feed_guid],
    )?;

    // 8. proof_tokens & proof_challenges (SG-07)
    conn.execute(
        "DELETE FROM proof_tokens WHERE subject_feed_guid = ?1",
        params![feed_guid],
    )?;
    conn.execute(
        "DELETE FROM proof_challenges WHERE feed_guid = ?1",
        params![feed_guid],
    )?;

    // 9. tracks
    conn.execute(
        "DELETE FROM tracks WHERE feed_guid = ?1",
        params![feed_guid],
    )?;

    // 10. feeds
    conn.execute(
        "DELETE FROM feeds WHERE feed_guid = ?1",
        params![feed_guid],
    )?;

    Ok(())
}

// ── delete_feed_with_event ───────────────────────────────────────────────────

/// Cascade-deletes a feed and records a `FeedRetired` event in a single atomic
/// transaction, returning the assigned event `seq`.
///
/// Uses correlated subqueries for track-level child deletion, matching the
/// strategy in [`delete_feed`].
///
/// # Errors
///
/// Returns [`DbError`] if any SQL statement, JSON serialisation, or the
/// transaction commit fails.
// DB performance compliant (subqueries) — 2026-03-12
// Issue-SEQ-INTEGRITY — 2026-03-14
#[expect(clippy::too_many_arguments, reason = "all event fields are required for a complete atomic delete+event")]
pub fn delete_feed_with_event(
    conn:         &mut Connection,
    feed_guid:    &str,
    event_id:     &str,
    payload_json: &str,
    subject_guid: &str,
    signer:       &NodeSigner,
    created_at:   i64,
    warnings:     &[String],
) -> Result<(i64, String, String), DbError> {
    let tx = conn.transaction()?;

    tx.execute(
        "DELETE FROM track_tag WHERE track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    tx.execute("DELETE FROM feed_tag WHERE feed_guid = ?1", params![feed_guid])?;
    tx.execute(
        "DELETE FROM value_time_splits WHERE source_track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    tx.execute(
        "DELETE FROM payment_routes WHERE track_guid IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    tx.execute("DELETE FROM feed_payment_routes WHERE feed_guid = ?1", params![feed_guid])?;
    tx.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'track' AND entity_id IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    tx.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'feed' AND entity_id = ?1",
        params![feed_guid],
    )?;
    tx.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'track' AND entity_id IN \
         (SELECT track_guid FROM tracks WHERE feed_guid = ?1)",
        params![feed_guid],
    )?;
    tx.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'feed' AND entity_id = ?1",
        params![feed_guid],
    )?;
    // proof_tokens & proof_challenges (SG-07)
    tx.execute("DELETE FROM proof_tokens WHERE subject_feed_guid = ?1", params![feed_guid])?;
    tx.execute("DELETE FROM proof_challenges WHERE feed_guid = ?1", params![feed_guid])?;

    tx.execute("DELETE FROM tracks WHERE feed_guid = ?1", params![feed_guid])?;
    tx.execute("DELETE FROM feeds WHERE feed_guid = ?1", params![feed_guid])?;

    let et_str = event_type_str(&crate::event::EventType::FeedRetired)?;
    let warnings_json = serde_json::to_string(warnings)?;
    // Issue-SEQ-INTEGRITY — 2026-03-14: insert with placeholder, sign with seq, update.
    let seq = tx.query_row(
        "INSERT INTO events \
         (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
         RETURNING seq",
        params![event_id, et_str, payload_json, subject_guid, signer.pubkey_hex(), "", created_at, warnings_json],
        |row| row.get::<_, i64>(0),
    )?;
    let (signed_by, signature) = signer.sign_event(
        event_id, &crate::event::EventType::FeedRetired, payload_json, subject_guid, created_at, seq,
    );
    update_event_signature(&tx, event_id, &signed_by, &signature)?;

    tx.commit()?;
    Ok((seq, signed_by, signature))
}

// ── delete_track_with_event ──────────────────────────────────────────────────

/// Cascade-deletes a track and records a `TrackRemoved` event in a single
/// atomic transaction, returning the assigned event `seq`.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL statement, JSON serialisation, or the
/// transaction commit fails.
// Issue-SEQ-INTEGRITY — 2026-03-14
#[expect(clippy::too_many_arguments, reason = "all event fields are required for a complete atomic delete+event")]
pub fn delete_track_with_event(
    conn:         &mut Connection,
    track_guid:   &str,
    event_id:     &str,
    payload_json: &str,
    subject_guid: &str,
    signer:       &NodeSigner,
    created_at:   i64,
    warnings:     &[String],
) -> Result<(i64, String, String), DbError> {
    let tx = conn.transaction()?;

    tx.execute("DELETE FROM track_tag WHERE track_guid = ?1", params![track_guid])?;
    tx.execute("DELETE FROM value_time_splits WHERE source_track_guid = ?1", params![track_guid])?;
    tx.execute("DELETE FROM payment_routes WHERE track_guid = ?1", params![track_guid])?;
    tx.execute(
        "DELETE FROM entity_quality WHERE entity_type = 'track' AND entity_id = ?1",
        params![track_guid],
    )?;
    tx.execute(
        "DELETE FROM entity_field_status WHERE entity_type = 'track' AND entity_id = ?1",
        params![track_guid],
    )?;
    tx.execute("DELETE FROM tracks WHERE track_guid = ?1", params![track_guid])?;

    let et_str = event_type_str(&crate::event::EventType::TrackRemoved)?;
    let warnings_json = serde_json::to_string(warnings)?;
    // Issue-SEQ-INTEGRITY — 2026-03-14: insert with placeholder, sign with seq, update.
    let seq = tx.query_row(
        "INSERT INTO events \
         (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
         RETURNING seq",
        params![event_id, et_str, payload_json, subject_guid, signer.pubkey_hex(), "", created_at, warnings_json],
        |row| row.get::<_, i64>(0),
    )?;
    let (signed_by, signature) = signer.sign_event(
        event_id, &crate::event::EventType::TrackRemoved, payload_json, subject_guid, created_at, seq,
    );
    update_event_signature(&tx, event_id, &signed_by, &signature)?;

    tx.commit()?;
    Ok((seq, signed_by, signature))
}

// ── merge_artists_with_event ──────────────────────────────────────────────────

/// Merges two artists AND records the signed event in a single atomic
/// transaction.  Returns `(transferred_aliases, seq)`.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL statement or the transaction commit fails.
// Finding-2 atomic mutation+event — 2026-03-13
// Issue-SEQ-INTEGRITY — 2026-03-14
#[expect(clippy::too_many_arguments, reason = "all event fields are required for a complete atomic merge+event")]
pub fn merge_artists_with_event(
    conn:            &mut Connection,
    source_artist_id: &str,
    target_artist_id: &str,
    event_id:        &str,
    event_type:      &EventType,
    payload_json:    &str,
    subject_guid:    &str,
    signer:          &NodeSigner,
    created_at:      i64,
    warnings:        &[String],
) -> Result<(Vec<String>, i64, String, String), DbError> {
    let tx = conn.transaction()?;

    let transferred = merge_artists_sql(&tx, source_artist_id, target_artist_id)?;

    let (seq, signed_by, signature) = insert_event(
        &tx, event_id, event_type, payload_json, subject_guid,
        signer, created_at, warnings,
    )?;

    tx.commit()?;
    Ok((transferred, seq, signed_by, signature))
}

// ── get_feed_by_guid ────────────────────────────────────────────────────────

/// Looks up the feed row by `feed_guid`, returning `None` if absent.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_feed_by_guid(
    conn:      &Connection,
    feed_guid: &str,
) -> Result<Option<Feed>, DbError> {
    let result = conn.query_row(
        "SELECT feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, \
         created_at, updated_at, raw_medium \
         FROM feeds WHERE feed_guid = ?1",
        params![feed_guid],
        |row| {
            let explicit_i: i64 = row.get(8)?;
            Ok(Feed {
                feed_guid:        row.get(0)?,
                feed_url:         row.get(1)?,
                title:            row.get(2)?,
                title_lower:      row.get(3)?,
                artist_credit_id: row.get(4)?,
                description:      row.get(5)?,
                image_url:        row.get(6)?,
                language:         row.get(7)?,
                explicit:         explicit_i != 0,
                itunes_type:      row.get(9)?,
                episode_count:    row.get(10)?,
                newest_item_at:   row.get(11)?,
                oldest_item_at:   row.get(12)?,
                created_at:       row.get(13)?,
                updated_at:       row.get(14)?,
                raw_medium:       row.get(15)?,
            })
        },
    ).optional()?;

    Ok(result)
}

// ── get_track_by_guid ───────────────────────────────────────────────────────

/// Looks up the track row by `track_guid`, returning `None` if absent.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_track_by_guid(
    conn:       &Connection,
    track_guid: &str,
) -> Result<Option<Track>, DbError> {
    let result = conn.query_row(
        "SELECT track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, \
         duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, \
         season, explicit, description, created_at, updated_at \
         FROM tracks WHERE track_guid = ?1",
        params![track_guid],
        |row| {
            let explicit_i: i64 = row.get(12)?;
            Ok(Track {
                track_guid:       row.get(0)?,
                feed_guid:        row.get(1)?,
                artist_credit_id: row.get(2)?,
                title:            row.get(3)?,
                title_lower:      row.get(4)?,
                pub_date:         row.get(5)?,
                duration_secs:    row.get(6)?,
                enclosure_url:    row.get(7)?,
                enclosure_type:   row.get(8)?,
                enclosure_bytes:  row.get(9)?,
                track_number:     row.get(10)?,
                season:           row.get(11)?,
                explicit:         explicit_i != 0,
                description:      row.get(13)?,
                created_at:       row.get(14)?,
                updated_at:       row.get(15)?,
            })
        },
    ).optional()?;

    Ok(result)
}

// ── get_tracks_for_feed ─────────────────────────────────────────────────────

/// Returns all tracks belonging to the given `feed_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_tracks_for_feed(
    conn:      &Connection,
    feed_guid: &str,
) -> Result<Vec<Track>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, \
         duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, \
         season, explicit, description, created_at, updated_at \
         FROM tracks WHERE feed_guid = ?1",
    )?;

    let rows = stmt.query_map(params![feed_guid], |row| {
        let explicit_i: i64 = row.get(12)?;
        Ok(Track {
            track_guid:       row.get(0)?,
            feed_guid:        row.get(1)?,
            artist_credit_id: row.get(2)?,
            title:            row.get(3)?,
            title_lower:      row.get(4)?,
            pub_date:         row.get(5)?,
            duration_secs:    row.get(6)?,
            enclosure_url:    row.get(7)?,
            enclosure_type:   row.get(8)?,
            enclosure_bytes:  row.get(9)?,
            track_number:     row.get(10)?,
            season:           row.get(11)?,
            explicit:         explicit_i != 0,
            description:      row.get(13)?,
            created_at:       row.get(14)?,
            updated_at:       row.get(15)?,
        })
    })?;

    let mut tracks = Vec::new();
    for row in rows {
        tracks.push(row?);
    }
    Ok(tracks)
}

// ── get_feed_payment_routes_for_feed ────────────────────────────────────────
// Issue-WRITE-AMP — 2026-03-14

/// Returns all feed-level payment routes for the given `feed_guid`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_feed_payment_routes_for_feed(
    conn:      &Connection,
    feed_guid: &str,
) -> Result<Vec<FeedPaymentRoute>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, feed_guid, recipient_name, route_type, address, \
         custom_key, custom_value, split, fee \
         FROM feed_payment_routes WHERE feed_guid = ?1",
    )?;
    let rows = stmt.query_map(params![feed_guid], |row| {
        let rt_str: String = row.get(3)?;
        let fee_i: i64 = row.get(8)?;
        Ok(FeedPaymentRoute {
            id:             row.get(0)?,
            feed_guid:      row.get(1)?,
            recipient_name: row.get(2)?,
            route_type:     serde_json::from_str(&format!("\"{rt_str}\""))
                                .unwrap_or(RouteType::Node),
            address:        row.get(4)?,
            custom_key:     row.get(5)?,
            custom_value:   row.get(6)?,
            split:          row.get(7)?,
            fee:            fee_i != 0,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

// ── diff helpers ────────────────────────────────────────────────────────────
// Issue-WRITE-AMP — 2026-03-14

/// Compares two feeds by their content fields (ignoring timestamps and
/// computed fields like `episode_count`, `newest_item_at`, `oldest_item_at`).
fn feed_fields_changed(existing: &Feed, new: &Feed) -> bool {
    existing.title       != new.title
    || existing.description != new.description
    || existing.image_url   != new.image_url
    || existing.language    != new.language
    || existing.explicit    != new.explicit
    || existing.itunes_type != new.itunes_type
    || existing.raw_medium  != new.raw_medium
    || existing.feed_url    != new.feed_url
}

/// Compares two tracks by their content fields (ignoring timestamps).
fn track_fields_changed(existing: &Track, new: &Track) -> bool {
    existing.title            != new.title
    || existing.artist_credit_id != new.artist_credit_id
    || existing.pub_date         != new.pub_date
    || existing.duration_secs    != new.duration_secs
    || existing.enclosure_url    != new.enclosure_url
    || existing.enclosure_type   != new.enclosure_type
    || existing.enclosure_bytes  != new.enclosure_bytes
    || existing.track_number     != new.track_number
    || existing.season           != new.season
    || existing.explicit         != new.explicit
    || existing.description      != new.description
}

/// Compares two artists by their content fields (ignoring timestamps).
fn artist_fields_changed(existing: &Artist, new: &Artist) -> bool {
    existing.name       != new.name
    || existing.sort_name  != new.sort_name
    || existing.type_id    != new.type_id
    || existing.area       != new.area
    || existing.img_url    != new.img_url
    || existing.url        != new.url
    || existing.begin_year != new.begin_year
    || existing.end_year   != new.end_year
}

/// Compares two sets of feed payment routes by their content fields
/// (ignoring `id` which is DB-assigned).
fn feed_routes_changed(
    existing: &[FeedPaymentRoute],
    new: &[FeedPaymentRoute],
) -> bool {
    if existing.len() != new.len() {
        return true;
    }
    // Compare route-by-route; order matters.
    existing.iter().zip(new.iter()).any(|(a, b)| {
        a.recipient_name != b.recipient_name
        || a.route_type     != b.route_type
        || a.address        != b.address
        || a.custom_key     != b.custom_key
        || a.custom_value   != b.custom_value
        || a.split          != b.split
        || a.fee            != b.fee
    })
}

// ── build_diff_events ───────────────────────────────────────────────────────
// Issue-WRITE-AMP — 2026-03-14

/// Queries existing DB state and builds event rows only for entities that
/// actually changed compared to what is stored.
///
/// On first ingest (feed not yet in DB), all events are emitted. On
/// re-ingest, only entities whose fields actually differ produce events.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query or JSON serialisation fails.
#[expect(clippy::too_many_arguments, reason = "mirrors the data needed to build events")]
pub fn build_diff_events(
    conn:           &Connection,
    artist:         &Artist,
    artist_credit:  &ArtistCredit,
    feed:           &Feed,
    feed_routes:    &[FeedPaymentRoute],
    tracks:         &[(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)],
    track_credits:  &[ArtistCredit],
    now:            i64,
    warnings:       &[String],
) -> Result<Vec<EventRow>, DbError> {
    // Use feed existence as the primary gate: if the feed is not yet in the
    // DB, this is a first ingest and all events must be emitted. Note: the
    // artist may already exist (resolve_artist creates it before this runs),
    // so we cannot rely on artist existence alone.
    let existing_feed = get_feed_by_guid(conn, &feed.feed_guid)?;

    existing_feed.map_or_else(
        || build_all_events(
            artist, artist_credit, feed, feed_routes, tracks,
            track_credits, now, warnings,
        ),
        |ef| build_changed_events(
            conn, artist, artist_credit, feed, feed_routes, tracks,
            track_credits, now, warnings, &ef,
        ),
    )
}

/// Emits all events unconditionally (first ingest of a feed).
#[expect(clippy::too_many_arguments, reason = "mirrors build_diff_events params")]
fn build_all_events(
    artist:         &Artist,
    artist_credit:  &ArtistCredit,
    feed:           &Feed,
    feed_routes:    &[FeedPaymentRoute],
    tracks:         &[(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)],
    track_credits:  &[ArtistCredit],
    now:            i64,
    warnings:       &[String],
) -> Result<Vec<EventRow>, DbError> {
    let mut event_rows: Vec<EventRow> = Vec::new();
    let warn_vec: Vec<String> = warnings.to_vec();

    event_rows.push(build_artist_upserted_event(artist, now, &warn_vec)?);
    event_rows.push(build_artist_credit_event(
        artist_credit, artist, now, &warn_vec,
    )?);
    event_rows.push(build_feed_upserted_event(
        feed, artist, artist_credit, now, &warn_vec,
    )?);

    if !feed_routes.is_empty() {
        event_rows.push(build_feed_routes_event(feed, feed_routes, now, &warn_vec)?);
    }

    for (i, (track, routes, vts)) in tracks.iter().enumerate() {
        let credit = if i < track_credits.len() {
            &track_credits[i]
        } else {
            artist_credit
        };
        event_rows.push(build_track_upserted_event(
            track, routes, vts, credit, now, &warn_vec,
        )?);
    }

    Ok(event_rows)
}

/// Emits events only for entities that differ from the stored DB state.
#[expect(clippy::too_many_arguments, reason = "mirrors build_diff_events params")]
fn build_changed_events(
    conn:           &Connection,
    artist:         &Artist,
    artist_credit:  &ArtistCredit,
    feed:           &Feed,
    feed_routes:    &[FeedPaymentRoute],
    tracks:         &[(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)],
    track_credits:  &[ArtistCredit],
    now:            i64,
    warnings:       &[String],
    existing_feed:  &Feed,
) -> Result<Vec<EventRow>, DbError> {
    let mut event_rows: Vec<EventRow> = Vec::new();
    let warn_vec: Vec<String> = warnings.to_vec();

    // --- Artist diff ---
    let artist_changed = diff_artist(conn, artist)?;
    if artist_changed {
        event_rows.push(build_artist_upserted_event(artist, now, &warn_vec)?);
        event_rows.push(build_artist_credit_event(
            artist_credit, artist, now, &warn_vec,
        )?);
    }

    // --- Feed diff ---
    if feed_fields_changed(existing_feed, feed) {
        event_rows.push(build_feed_upserted_event(
            feed, artist, artist_credit, now, &warn_vec,
        )?);
    }

    // --- Feed routes diff ---
    let existing_routes = get_feed_payment_routes_for_feed(conn, &feed.feed_guid)?;
    if !feed_routes.is_empty() && feed_routes_changed(&existing_routes, feed_routes) {
        event_rows.push(build_feed_routes_event(feed, feed_routes, now, &warn_vec)?);
    }

    // --- Track diff ---
    let existing_tracks = get_tracks_for_feed(conn, &feed.feed_guid)?;
    let existing_map: std::collections::HashMap<&str, &Track> = existing_tracks
        .iter()
        .map(|t| (t.track_guid.as_str(), t))
        .collect();

    for (i, (track, routes, vts)) in tracks.iter().enumerate() {
        let is_new_or_changed = existing_map
            .get(track.track_guid.as_str())
            .is_none_or(|existing| track_fields_changed(existing, track));

        if is_new_or_changed {
            let credit = if i < track_credits.len() {
                &track_credits[i]
            } else {
                artist_credit
            };
            event_rows.push(build_track_upserted_event(
                track, routes, vts, credit, now, &warn_vec,
            )?);
        }
    }

    Ok(event_rows)
}

// --- private event builders (keep each under 50 lines) ---

fn diff_artist(conn: &Connection, artist: &Artist) -> Result<bool, DbError> {
    let existing = get_artist_by_id(conn, &artist.artist_id)?;
    Ok(existing.is_none_or(|e| artist_fields_changed(&e, artist)))
}

fn build_artist_upserted_event(
    artist:   &Artist,
    now:      i64,
    warnings: &[String],
) -> Result<EventRow, DbError> {
    let payload = crate::event::ArtistUpsertedPayload {
        artist: artist.clone(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    Ok(EventRow {
        event_id:     uuid::Uuid::new_v4().to_string(),
        event_type:   EventType::ArtistUpserted,
        payload_json,
        subject_guid: artist.artist_id.clone(),
        created_at:   now,
        warnings:     warnings.to_vec(),
    })
}

fn build_artist_credit_event(
    credit:   &ArtistCredit,
    artist:   &Artist,
    now:      i64,
    warnings: &[String],
) -> Result<EventRow, DbError> {
    let payload = crate::event::ArtistCreditCreatedPayload {
        artist_credit: credit.clone(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    Ok(EventRow {
        event_id:     uuid::Uuid::new_v4().to_string(),
        event_type:   EventType::ArtistCreditCreated,
        payload_json,
        subject_guid: artist.artist_id.clone(),
        created_at:   now,
        warnings:     warnings.to_vec(),
    })
}

fn build_feed_upserted_event(
    feed:     &Feed,
    artist:   &Artist,
    credit:   &ArtistCredit,
    now:      i64,
    warnings: &[String],
) -> Result<EventRow, DbError> {
    let payload = crate::event::FeedUpsertedPayload {
        feed:          feed.clone(),
        artist:        artist.clone(),
        artist_credit: credit.clone(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    Ok(EventRow {
        event_id:     uuid::Uuid::new_v4().to_string(),
        event_type:   EventType::FeedUpserted,
        payload_json,
        subject_guid: feed.feed_guid.clone(),
        created_at:   now,
        warnings:     warnings.to_vec(),
    })
}

fn build_feed_routes_event(
    feed:     &Feed,
    routes:   &[FeedPaymentRoute],
    now:      i64,
    warnings: &[String],
) -> Result<EventRow, DbError> {
    let payload = crate::event::FeedRoutesReplacedPayload {
        feed_guid: feed.feed_guid.clone(),
        routes:    routes.to_vec(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    Ok(EventRow {
        event_id:     uuid::Uuid::new_v4().to_string(),
        event_type:   EventType::FeedRoutesReplaced,
        payload_json,
        subject_guid: feed.feed_guid.clone(),
        created_at:   now,
        warnings:     warnings.to_vec(),
    })
}

fn build_track_upserted_event(
    track:    &Track,
    routes:   &[PaymentRoute],
    vts:      &[ValueTimeSplit],
    credit:   &ArtistCredit,
    now:      i64,
    warnings: &[String],
) -> Result<EventRow, DbError> {
    let payload = crate::event::TrackUpsertedPayload {
        track:             track.clone(),
        routes:            routes.to_vec(),
        value_time_splits: vts.to_vec(),
        artist_credit:     credit.clone(),
    };
    let payload_json = serde_json::to_string(&payload)?;
    Ok(EventRow {
        event_id:     uuid::Uuid::new_v4().to_string(),
        event_type:   EventType::TrackUpserted,
        payload_json,
        subject_guid: track.track_guid.clone(),
        created_at:   now,
        warnings:     warnings.to_vec(),
    })
}

// ── insert_event ──────────────────────────────────────────────────────────────

/// Inserts a single event row, signs it with the DB-assigned `seq`, and
/// returns `(seq, signed_by, signature)`.
///
/// The event is inserted with a placeholder signature first so the
/// DB can assign a monotonic `seq`. The signature is then computed
/// over the full signing payload (including `seq`) and written back.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert, update, or JSON serialisation fails.
// Issue-SEQ-INTEGRITY — 2026-03-14
#[expect(clippy::too_many_arguments, reason = "all fields are required for a complete event row")]
pub fn insert_event(
    conn:         &Connection,
    event_id:     &str,
    event_type:   &EventType,
    payload_json: &str,
    subject_guid: &str,
    signer:       &NodeSigner,
    created_at:   i64,
    warnings:     &[String],
) -> Result<(i64, String, String), DbError> {
    let et_str = event_type_str(event_type)?;
    let warnings_json = serde_json::to_string(warnings)?;

    // Issue-SEQ-INTEGRITY — 2026-03-14
    // Insert with placeholder signature to get the DB-assigned seq.
    let sql = "INSERT INTO events \
        (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
        RETURNING seq";

    let seq = conn.query_row(
        sql,
        params![event_id, et_str, payload_json, subject_guid, signer.pubkey_hex(), "", created_at, warnings_json],
        |row| row.get::<_, i64>(0),
    )?;

    // Sign with the assigned seq and update the row.
    let (signed_by, signature) = signer.sign_event(
        event_id, event_type, payload_json, subject_guid, created_at, seq,
    );
    update_event_signature(conn, event_id, &signed_by, &signature)?;

    Ok((seq, signed_by, signature))
}

// ── update_event_signature ─────────────────────────────────────────────────

/// Updates the `signed_by` and `signature` columns for an existing event row.
///
/// Used by the primary after inserting an event to get the DB-assigned `seq`,
/// signing the event (including seq), and backfilling the real signature.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL update fails.
// Issue-SEQ-INTEGRITY — 2026-03-14
pub fn update_event_signature(
    conn:      &Connection,
    event_id:  &str,
    signed_by: &str,
    signature: &str,
) -> Result<(), DbError> {
    conn.execute(
        "UPDATE events SET signed_by = ?1, signature = ?2 WHERE event_id = ?3",
        params![signed_by, signature, event_id],
    )?;
    Ok(())
}

// ── upsert_feed_crawl_cache ───────────────────────────────────────────────────

/// Records the latest content hash and crawl timestamp for `feed_url`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
pub fn upsert_feed_crawl_cache(
    conn:         &Connection,
    feed_url:     &str,
    content_hash: &str,
    crawled_at:   i64,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT(feed_url) DO UPDATE SET \
           content_hash = excluded.content_hash, \
           crawled_at   = excluded.crawled_at",
        params![feed_url, content_hash, crawled_at],
    )?;
    Ok(())
}

// ── get_events_since ──────────────────────────────────────────────────────────

/// Returns up to `limit` events with `seq > after_seq`, ordered ascending.
///
/// # Errors
///
/// Returns [`DbError`] if a SQL query fails or event JSON cannot be deserialised.
pub fn get_events_since(
    conn:       &Connection,
    after_seq:  i64,
    limit:      i64,
) -> Result<Vec<Event>, DbError> {
    // Issue-NEGATIVE-LIMIT — 2026-03-15
    let safe_limit = limit.max(1);
    let mut stmt = conn.prepare(
        "SELECT event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json \
         FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
    )?;

    let rows = stmt.query_map(params![after_seq, safe_limit], |row| {
        Ok((
            row.get::<_, String>(0)?,   // event_id
            row.get::<_, String>(1)?,   // event_type string
            row.get::<_, String>(2)?,   // payload_json
            row.get::<_, String>(3)?,   // subject_guid
            row.get::<_, String>(4)?,   // signed_by
            row.get::<_, String>(5)?,   // signature
            row.get::<_, i64>(6)?,      // seq
            row.get::<_, i64>(7)?,      // created_at
            row.get::<_, String>(8)?,   // warnings_json
        ))
    })?;

    let mut events = Vec::new();
    for row in rows {
        let (event_id, et_str, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) = row?;

        let et_quoted = format!("\"{et_str}\"");
        let event_type: EventType = serde_json::from_str(&et_quoted)?;

        let tagged = format!(r#"{{"type":"{et_str}","data":{payload_json}}}"#);
        let payload: EventPayload = serde_json::from_str(&tagged)?;
        let warnings: Vec<String> = serde_json::from_str(&warnings_json)?;

        events.push(Event {
            event_id,
            event_type,
            payload,
            payload_json: payload_json.clone(),
            subject_guid,
            signed_by,
            signature,
            seq,
            created_at,
            warnings,
        });
    }

    Ok(events)
}

// ── get_event_refs_since ──────────────────────────────────────────────────────

// Finding-5 reconcile pagination — 2026-03-13
/// Returns lightweight `(event_id, seq)` references for events with `seq >= since_seq`,
/// bounded by `limit` to prevent unbounded memory usage.
///
/// Returns a tuple of `(refs, truncated)` where `truncated` is `true` when
/// more rows exist beyond the limit.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_event_refs_since(
    conn:      &Connection,
    since_seq: i64,
    limit:     i64,
) -> Result<(Vec<crate::sync::EventRef>, bool), DbError> {
    // Issue-NEGATIVE-LIMIT — 2026-03-15
    let limit = limit.max(1);
    // Fetch limit + 1 to detect truncation without a separate COUNT query.
    let fetch_limit = limit.saturating_add(1);
    let mut stmt = conn.prepare(
        "SELECT event_id, seq FROM events WHERE seq >= ?1 ORDER BY seq ASC LIMIT ?2",
    )?;

    let rows = stmt.query_map(params![since_seq, fetch_limit], |row| {
        Ok(crate::sync::EventRef {
            event_id: row.get(0)?,
            seq:      row.get(1)?,
        })
    })?;

    let mut refs = Vec::new();
    for row in rows {
        refs.push(row?);
    }

    let truncated = i64::try_from(refs.len()).unwrap_or(i64::MAX) > limit;
    if truncated {
        refs.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    }

    Ok((refs, truncated))
}

// ── upsert_node_sync_state ────────────────────────────────────────────────────

/// Records or updates the last-seen sequence number for a peer node.
///
/// The cursor is monotonic: the stored `last_seq` can only increase.
/// `MAX(last_seq, excluded.last_seq)` prevents regression when events
/// are applied out of order (e.g. seq=15 then seq=10).
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
// Issue-CURSOR-MONOTONIC — 2026-03-14
pub fn upsert_node_sync_state(
    conn:         &Connection,
    node_pubkey:  &str,
    last_seq:     i64,
    last_seen_at: i64,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO node_sync_state (node_pubkey, last_seq, last_seen_at) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT(node_pubkey) DO UPDATE SET \
           last_seq     = MAX(last_seq, excluded.last_seq), \
           last_seen_at = excluded.last_seen_at",
        params![node_pubkey, last_seq, last_seen_at],
    )?;
    Ok(())
}

// ── peer_nodes ────────────────────────────────────────────────────────────────

/// Maximum consecutive push failures before a peer is considered unhealthy.
/// Used for both startup reload and runtime eviction.
// Issue-PEER-THRESHOLD — 2026-03-16
pub const MAX_PEER_FAILURES: i64 = 10;

/// A peer node registered for push fan-out.
#[derive(Debug)]
pub struct PeerNode {
    pub node_pubkey:          String,
    pub node_url:             String,
    pub discovered_at:        i64,
    pub last_push_at:         Option<i64>,
    pub consecutive_failures: i64,
}

/// Returns all peers with `consecutive_failures < MAX_PEER_FAILURES`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
// Issue-PEER-THRESHOLD — 2026-03-16
pub fn get_push_peers(conn: &Connection) -> Result<Vec<PeerNode>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT node_pubkey, node_url, discovered_at, last_push_at, consecutive_failures \
         FROM peer_nodes WHERE consecutive_failures < ?1",
    )?;

    let rows = stmt.query_map(rusqlite::params![MAX_PEER_FAILURES], |row| {
        Ok(PeerNode {
            node_pubkey:          row.get(0)?,
            node_url:             row.get(1)?,
            discovered_at:        row.get(2)?,
            last_push_at:         row.get(3)?,
            consecutive_failures: row.get(4)?,
        })
    })?;

    let mut peers = Vec::new();
    for row in rows {
        peers.push(row?);
    }
    Ok(peers)
}

/// Upserts a peer node record.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
pub fn upsert_peer_node(
    conn:        &Connection,
    node_pubkey: &str,
    node_url:    &str,
    now:         i64,
) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO peer_nodes (node_pubkey, node_url, discovered_at) \
         VALUES (?1, ?2, ?3) \
         ON CONFLICT(node_pubkey) DO UPDATE SET node_url = excluded.node_url",
        rusqlite::params![node_pubkey, node_url, now],
    )?;
    Ok(())
}

/// Records a successful push delivery.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL update fails.
pub fn record_push_success(conn: &Connection, node_pubkey: &str, now: i64) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET last_push_at = ?1, consecutive_failures = 0 \
         WHERE node_pubkey = ?2",
        rusqlite::params![now, node_pubkey],
    )?;
    Ok(())
}

/// Increments `consecutive_failures` by 1 for the given peer.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL update fails.
pub fn increment_peer_failures(conn: &Connection, node_pubkey: &str) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = consecutive_failures + 1 \
         WHERE node_pubkey = ?1",
        rusqlite::params![node_pubkey],
    )?;
    Ok(())
}

/// Resets `consecutive_failures` to 0 for the given peer.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL update fails.
pub fn reset_peer_failures(conn: &Connection, node_pubkey: &str) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = 0 WHERE node_pubkey = ?1",
        rusqlite::params![node_pubkey],
    )?;
    Ok(())
}

/// Inserts a single event row using `INSERT OR IGNORE`.
///
/// Returns `Some(seq)` if the event was newly inserted, or `None` if a row
/// with the same `event_id` already existed (idempotent community-side apply).
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert or JSON serialisation fails.
#[expect(clippy::too_many_arguments, reason = "all fields are required for a complete event row")]
pub fn insert_event_idempotent(
    conn:         &Connection,
    event_id:     &str,
    event_type:   &crate::event::EventType,
    payload_json: &str,
    subject_guid: &str,
    signed_by:    &str,
    signature:    &str,
    created_at:   i64,
    warnings:     &[String],
) -> Result<Option<i64>, DbError> {
    // Issue-3 RETURNING seq — 2026-03-13
    let et_str = event_type_str(event_type)?;
    let warnings_json = serde_json::to_string(warnings)?;

    let sql = "INSERT OR IGNORE INTO events \
        (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
        RETURNING seq";

    let seq: Option<i64> = conn.query_row(
        sql,
        rusqlite::params![event_id, et_str, payload_json, subject_guid, signed_by, signature, created_at, warnings_json],
        |row| row.get::<_, i64>(0),
    ).optional()?;

    Ok(seq)
}

// ── get_node_sync_cursor ──────────────────────────────────────────────────────

/// Returns the `last_seq` cursor stored for `node_pubkey`, or `0` if none exists.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_node_sync_cursor(conn: &Connection, node_pubkey: &str) -> Result<i64, DbError> {
    let seq: Option<i64> = conn.query_row(
        "SELECT last_seq FROM node_sync_state WHERE node_pubkey = ?1",
        params![node_pubkey],
        |row| row.get(0),
    ).optional()?;
    Ok(seq.unwrap_or(0))
}

// ── Tags ─────────────────────────────────────────────────────────────────────

/// Returns the id of an existing tag with the given (lowercased) name, or
/// creates a new one and returns its id.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert or query fails.
pub fn get_or_create_tag(conn: &Connection, name: &str) -> Result<i64, DbError> {
    let lower = name.to_lowercase();
    let now = unix_now();

    conn.execute(
        "INSERT OR IGNORE INTO tags (name, created_at) VALUES (?1, ?2)",
        params![lower, now],
    )?;

    let id: i64 = conn.query_row(
        "SELECT id FROM tags WHERE name = ?1",
        params![lower],
        |row| row.get(0),
    )?;

    Ok(id)
}

/// Inserts a tag association into the appropriate junction table based on
/// `entity_type` ("artist", "feed", or "track").
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn apply_tag(conn: &Connection, entity_type: &str, entity_id: &str, tag_id: i64) -> Result<(), DbError> {
    let now = unix_now();
    match entity_type {
        "artist" => {
            conn.execute(
                "INSERT OR IGNORE INTO artist_tag (artist_id, tag_id, created_at) VALUES (?1, ?2, ?3)",
                params![entity_id, tag_id, now],
            )?;
        }
        "feed" => {
            conn.execute(
                "INSERT OR IGNORE INTO feed_tag (feed_guid, tag_id, created_at) VALUES (?1, ?2, ?3)",
                params![entity_id, tag_id, now],
            )?;
        }
        "track" => {
            conn.execute(
                "INSERT OR IGNORE INTO track_tag (track_guid, tag_id, created_at) VALUES (?1, ?2, ?3)",
                params![entity_id, tag_id, now],
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Removes a tag association from the appropriate junction table.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL delete fails.
pub fn remove_tag(conn: &Connection, entity_type: &str, entity_id: &str, tag_id: i64) -> Result<(), DbError> {
    match entity_type {
        "artist" => {
            conn.execute(
                "DELETE FROM artist_tag WHERE artist_id = ?1 AND tag_id = ?2",
                params![entity_id, tag_id],
            )?;
        }
        "feed" => {
            conn.execute(
                "DELETE FROM feed_tag WHERE feed_guid = ?1 AND tag_id = ?2",
                params![entity_id, tag_id],
            )?;
        }
        "track" => {
            conn.execute(
                "DELETE FROM track_tag WHERE track_guid = ?1 AND tag_id = ?2",
                params![entity_id, tag_id],
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Returns `(tag_id, name)` pairs for all tags associated with an entity.
///
/// # Errors
///
/// Returns [`DbError`] if any SQL query fails.
pub fn get_tags_for_entity(conn: &Connection, entity_type: &str, entity_id: &str) -> Result<Vec<(i64, String)>, DbError> {
    let result = match entity_type {
        "artist" => {
            let mut stmt = conn.prepare(
                "SELECT t.id, t.name FROM tags t \
                 JOIN artist_tag at ON at.tag_id = t.id \
                 WHERE at.artist_id = ?1 ORDER BY t.name"
            )?;
            stmt.query_map(params![entity_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?.collect::<Result<Vec<_>, _>>()?
        }
        "feed" => {
            let mut stmt = conn.prepare(
                "SELECT t.id, t.name FROM tags t \
                 JOIN feed_tag ft ON ft.tag_id = t.id \
                 WHERE ft.feed_guid = ?1 ORDER BY t.name"
            )?;
            stmt.query_map(params![entity_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?.collect::<Result<Vec<_>, _>>()?
        }
        "track" => {
            let mut stmt = conn.prepare(
                "SELECT t.id, t.name FROM tags t \
                 JOIN track_tag tt ON tt.tag_id = t.id \
                 WHERE tt.track_guid = ?1 ORDER BY t.name"
            )?;
            stmt.query_map(params![entity_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?.collect::<Result<Vec<_>, _>>()?
        }
        _ => Vec::new(),
    };
    Ok(result)
}

// ── Relationships ────────────────────────────────────────────────────────────

/// Row returned by [`get_artist_rels`].
#[derive(Debug)]
pub struct ArtistRelRow {
    pub id:            i64,
    pub artist_id_a:   String,
    pub artist_id_b:   String,
    pub rel_type_name: String,
    pub begin_year:    Option<i64>,
    pub end_year:      Option<i64>,
}

/// Checks whether a `rel_type_id` exists in the `rel_type` lookup table.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn validate_rel_type(conn: &Connection, rel_type_id: i64) -> Result<bool, DbError> {
    let exists: Option<i64> = conn.query_row(
        "SELECT id FROM rel_type WHERE id = ?1",
        params![rel_type_id],
        |row| row.get(0),
    ).optional()?;
    Ok(exists.is_some())
}

/// Creates an artist-to-artist relationship. Returns the new row id.
///
/// Validates `rel_type_id` before inserting.
///
/// # Errors
///
/// Returns [`DbError`] if the rel type is invalid or the SQL insert fails.
pub fn create_artist_artist_rel(
    conn:        &Connection,
    artist_id_a: &str,
    artist_id_b: &str,
    rel_type_id: i64,
    begin_year:  Option<i64>,
    end_year:    Option<i64>,
) -> Result<i64, DbError> {
    // Validate rel_type_id exists.
    let valid = validate_rel_type(conn, rel_type_id)?;
    if !valid {
        return Err(DbError::Rusqlite(rusqlite::Error::QueryReturnedNoRows));
    }

    let now = unix_now();
    conn.execute(
        "INSERT INTO artist_artist_rel (artist_id_a, artist_id_b, rel_type_id, begin_year, end_year, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![artist_id_a, artist_id_b, rel_type_id, begin_year, end_year, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Returns all artist-artist relationships where `artist_id` appears on
/// either side (as `artist_id_a` or `artist_id_b`), joined with the
/// `rel_type` name.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_artist_rels(conn: &Connection, artist_id: &str) -> Result<Vec<ArtistRelRow>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT aar.id, aar.artist_id_a, aar.artist_id_b, rt.name, aar.begin_year, aar.end_year \
         FROM artist_artist_rel aar \
         JOIN rel_type rt ON rt.id = aar.rel_type_id \
         WHERE aar.artist_id_a = ?1 OR aar.artist_id_b = ?1 \
         ORDER BY aar.id",
    )?;

    let rows: Vec<ArtistRelRow> = stmt.query_map(params![artist_id], |row| {
        Ok(ArtistRelRow {
            id:            row.get(0)?,
            artist_id_a:   row.get(1)?,
            artist_id_b:   row.get(2)?,
            rel_type_name: row.get(3)?,
            begin_year:    row.get(4)?,
            end_year:      row.get(5)?,
        })
    })?.collect::<Result<_, _>>()?;

    Ok(rows)
}

/// Creates a track-to-track relationship. Returns the new row id.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn create_track_rel(
    conn:         &Connection,
    track_guid_a: &str,
    track_guid_b: &str,
    rel_type_id:  i64,
) -> Result<i64, DbError> {
    let now = unix_now();
    conn.execute(
        "INSERT INTO track_rel (track_guid_a, track_guid_b, rel_type_id, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![track_guid_a, track_guid_b, rel_type_id, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Creates a feed-to-feed relationship. Returns the new row id.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn create_feed_rel(
    conn:        &Connection,
    feed_guid_a: &str,
    feed_guid_b: &str,
    rel_type_id: i64,
) -> Result<i64, DbError> {
    let now = unix_now();
    conn.execute(
        "INSERT INTO feed_rel (feed_guid_a, feed_guid_b, rel_type_id, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![feed_guid_a, feed_guid_b, rel_type_id, now],
    )?;
    Ok(conn.last_insert_rowid())
}

// ── get_existing_feed ─────────────────────────────────────────────────────────

/// Looks up the feed row whose `feed_url` matches, returning `None` if absent.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_existing_feed(
    conn:     &Connection,
    feed_url: &str,
) -> Result<Option<Feed>, DbError> {
    let result = conn.query_row(
        "SELECT feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, \
         created_at, updated_at, raw_medium \
         FROM feeds WHERE feed_url = ?1",
        params![feed_url],
        |row| {
            let explicit_i: i64 = row.get(8)?;
            Ok(Feed {
                feed_guid:        row.get(0)?,
                feed_url:         row.get(1)?,
                title:            row.get(2)?,
                title_lower:      row.get(3)?,
                artist_credit_id: row.get(4)?,
                description:      row.get(5)?,
                image_url:        row.get(6)?,
                language:         row.get(7)?,
                explicit:         explicit_i != 0,
                itunes_type:      row.get(9)?,
                episode_count:    row.get(10)?,
                newest_item_at:   row.get(11)?,
                oldest_item_at:   row.get(12)?,
                created_at:       row.get(13)?,
                updated_at:       row.get(14)?,
                raw_medium:       row.get(15)?,
            })
        },
    ).optional()?;

    Ok(result)
}

// ── ingest_transaction ────────────────────────────────────────────────────────

// NOTE: The feed and track upsert SQL below duplicates the standalone
// `upsert_feed` and `upsert_track` functions. This is intentional: those
// functions take `&Connection`, but inside a transaction we must use the
// `&Transaction` handle so all writes participate in the same atomic commit.
/// Writes a complete feed ingest atomically and returns the new event `seq` values.
///
/// Upserts the artist, creates the artist credit, upserts the feed (with feed
/// payment routes), all tracks (with payment routes and value-time splits),
/// and inserts the supplied event rows — all inside one `SQLite` transaction.
///
/// Tracks that existed in the DB for this feed but are absent from the new
/// crawl are removed: their search-index and quality rows are cleaned up,
/// the track row is cascade-deleted, and a signed `TrackRemoved` event is
/// emitted — all within the same transaction (Issue-STALE-TRACKS).
///
/// # Errors
///
/// Returns [`DbError`] if any SQL statement, JSON serialisation, or the
/// transaction commit fails.
// Issue-SEQ-INTEGRITY — 2026-03-14
// Issue-STALE-TRACKS — 2026-03-14
#[expect(clippy::too_many_lines, reason = "single atomic transaction — splitting would obscure the transactional boundary")]
#[expect(clippy::needless_pass_by_value, reason = "takes ownership to make the transaction boundary clear at call sites")]
#[expect(clippy::too_many_arguments, reason = "Issue-SEQ-INTEGRITY added signer param; grouping into a struct would obscure the call-site types")]
pub fn ingest_transaction(
    conn:               &mut Connection,
    artist:             Artist,
    artist_credit:      ArtistCredit,
    feed:               Feed,
    feed_routes:        Vec<FeedPaymentRoute>,
    tracks:             Vec<(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)>,
    event_rows:         Vec<EventRow>,
    signer:             &NodeSigner,
) -> Result<Vec<(i64, String, String)>, DbError> {
    let tx = conn.transaction()?;

    // 1. Resolve/insert artist (and ensure a feed-scoped canonical alias row exists)
    // Issue-ARTIST-IDENTITY — 2026-03-14
    {
        let name_lower = artist.name.to_lowercase();
        let feed_guid_ref = Some(feed.feed_guid.as_str());

        // Check if this artist already exists (by artist_id PK, not by name).
        let existing: Option<String> = tx.query_row(
            "SELECT artist_id FROM artists WHERE artist_id = ?1",
            params![artist.artist_id],
            |row| row.get(0),
        ).optional()?;

        if existing.is_none() {
            tx.execute(
                "INSERT INTO artists (artist_id, name, name_lower, sort_name, type_id, area, \
                 img_url, url, begin_year, end_year, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    artist.artist_id, artist.name, name_lower,
                    artist.sort_name, artist.type_id, artist.area,
                    artist.img_url, artist.url, artist.begin_year, artist.end_year,
                    artist.created_at, artist.updated_at,
                ],
            )?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, feed_guid, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![name_lower, artist.artist_id, feed_guid_ref, artist.created_at],
        )?;
    }

    // 2. Insert artist credit (idempotent via INSERT OR IGNORE on PK)
    // Issue-ARTIST-IDENTITY — 2026-03-14
    {
        tx.execute(
            "INSERT OR IGNORE INTO artist_credit (id, display_name, feed_guid, created_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![artist_credit.id, artist_credit.display_name, artist_credit.feed_guid, artist_credit.created_at],
        )?;
        for acn in &artist_credit.names {
            tx.execute(
                "INSERT OR IGNORE INTO artist_credit_name \
                 (artist_credit_id, artist_id, position, name, join_phrase) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![acn.artist_credit_id, acn.artist_id, acn.position, acn.name, acn.join_phrase],
            )?;
        }
    }

    // 3. Upsert feed
    tx.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, created_at, \
         updated_at, raw_medium) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(feed_guid) DO UPDATE SET \
           feed_url         = excluded.feed_url, \
           title            = excluded.title, \
           title_lower      = excluded.title_lower, \
           artist_credit_id = excluded.artist_credit_id, \
           description      = excluded.description, \
           image_url        = excluded.image_url, \
           language         = excluded.language, \
           explicit         = excluded.explicit, \
           itunes_type      = excluded.itunes_type, \
           episode_count    = excluded.episode_count, \
           newest_item_at   = excluded.newest_item_at, \
           oldest_item_at   = excluded.oldest_item_at, \
           updated_at       = excluded.updated_at, \
           raw_medium       = excluded.raw_medium",
        params![
            feed.feed_guid,
            feed.feed_url,
            feed.title,
            feed.title_lower,
            feed.artist_credit_id,
            feed.description,
            feed.image_url,
            feed.language,
            i64::from(feed.explicit),
            feed.itunes_type,
            feed.episode_count,
            feed.newest_item_at,
            feed.oldest_item_at,
            feed.created_at,
            feed.updated_at,
            feed.raw_medium,
        ],
    )?;

    // 3b. Replace feed-level payment routes
    tx.execute("DELETE FROM feed_payment_routes WHERE feed_guid = ?1", params![feed.feed_guid])?;
    for r in &feed_routes {
        let route_type = serde_json::to_string(&r.route_type)?;
        let route_type = route_type.trim_matches('"');
        tx.execute(
            "INSERT INTO feed_payment_routes (feed_guid, recipient_name, route_type, address, \
             custom_key, custom_value, split, fee) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                r.feed_guid,
                r.recipient_name,
                route_type,
                r.address,
                r.custom_key,
                r.custom_value,
                r.split,
                i64::from(r.fee),
            ],
        )?;
    }

    // 4a. Collect existing track GUIDs for this feed before upserting.
    // Issue-STALE-TRACKS — 2026-03-14
    let existing_guids: std::collections::HashSet<String> = {
        let mut stmt = tx.prepare(
            "SELECT track_guid FROM tracks WHERE feed_guid = ?1",
        )?;
        let rows = stmt.query_map(params![feed.feed_guid], |row| row.get::<_, String>(0))?;
        let mut set = std::collections::HashSet::new();
        for row in rows {
            set.insert(row?);
        }
        set
    };

    // 4. Tracks, routes, splits
    for (track, routes, splits) in &tracks {
        tx.execute(
            "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, pub_date, \
             duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, season, \
             explicit, description, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
             ON CONFLICT(track_guid) DO UPDATE SET \
               feed_guid        = excluded.feed_guid, \
               artist_credit_id = excluded.artist_credit_id, \
               title            = excluded.title, \
               title_lower      = excluded.title_lower, \
               pub_date         = excluded.pub_date, \
               duration_secs    = excluded.duration_secs, \
               enclosure_url    = excluded.enclosure_url, \
               enclosure_type   = excluded.enclosure_type, \
               enclosure_bytes  = excluded.enclosure_bytes, \
               track_number     = excluded.track_number, \
               season           = excluded.season, \
               explicit         = excluded.explicit, \
               description      = excluded.description, \
               updated_at       = excluded.updated_at",
            params![
                track.track_guid,
                track.feed_guid,
                track.artist_credit_id,
                track.title,
                track.title_lower,
                track.pub_date,
                track.duration_secs,
                track.enclosure_url,
                track.enclosure_type,
                track.enclosure_bytes,
                track.track_number,
                track.season,
                i64::from(track.explicit),
                track.description,
                track.created_at,
                track.updated_at,
            ],
        )?;

        // replace payment routes
        tx.execute("DELETE FROM payment_routes WHERE track_guid = ?1", params![track.track_guid])?;
        for r in routes {
            let route_type = serde_json::to_string(&r.route_type)?;
            let route_type = route_type.trim_matches('"');
            tx.execute(
                "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, \
                 custom_key, custom_value, split, fee) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    r.track_guid,
                    r.feed_guid,
                    r.recipient_name,
                    route_type,
                    r.address,
                    r.custom_key,
                    r.custom_value,
                    r.split,
                    i64::from(r.fee),
                ],
            )?;
        }

        // replace value time splits
        tx.execute(
            "DELETE FROM value_time_splits WHERE source_track_guid = ?1",
            params![track.track_guid],
        )?;
        for s in splits {
            tx.execute(
                "INSERT INTO value_time_splits (source_track_guid, start_time_secs, duration_secs, \
                 remote_feed_guid, remote_item_guid, split, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    s.source_track_guid,
                    s.start_time_secs,
                    s.duration_secs,
                    s.remote_feed_guid,
                    s.remote_item_guid,
                    s.split,
                    s.created_at,
                ],
            )?;
        }
    }

    // 4b. Remove stale tracks that are no longer in the new crawl.
    // Issue-STALE-TRACKS — 2026-03-14
    let new_guids: std::collections::HashSet<&str> = tracks
        .iter()
        .map(|(t, _, _)| t.track_guid.as_str())
        .collect();
    let mut removal_event_rows: Vec<EventRow> = Vec::new();
    for removed_guid in &existing_guids {
        if new_guids.contains(removed_guid.as_str()) {
            continue;
        }
        // Look up the track to get search-index fields before deleting.
        let track_opt = get_track_by_guid(&tx, removed_guid)?;
        if let Some(track) = track_opt {
            // Remove the track's search index entry (best-effort).
            let _ = crate::search::delete_from_search_index(
                &tx,
                "track",
                &track.track_guid,
                "",
                &track.title,
                track.description.as_deref().unwrap_or(""),
                "",
            );
            // Cascade-delete the track and its child rows.
            delete_track_sql(&tx, removed_guid)?;
        }
        // Build a TrackRemoved event row.
        let payload = crate::event::TrackRemovedPayload {
            track_guid: removed_guid.clone(),
            feed_guid:  feed.feed_guid.clone(),
        };
        let payload_json = serde_json::to_string(&payload)?;
        removal_event_rows.push(EventRow {
            event_id:     uuid::Uuid::new_v4().to_string(),
            event_type:   EventType::TrackRemoved,
            payload_json,
            subject_guid: removed_guid.clone(),
            created_at:   feed.updated_at,
            warnings:     vec![],
        });
    }

    // Combine original event rows with removal event rows.
    let mut all_event_rows = event_rows;
    all_event_rows.append(&mut removal_event_rows);

    // 5. Insert events, collect seqs, sign with assigned seq, update signatures
    // Issue-SEQ-INTEGRITY — 2026-03-14
    let mut seqs = Vec::new();
    for er in &all_event_rows {
        let et_str = event_type_str(&er.event_type)?;
        let warnings_json = serde_json::to_string(&er.warnings)?;
        let sql = "INSERT INTO events \
            (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
            RETURNING seq";
        let seq = tx.query_row(
            sql,
            params![
                er.event_id,
                et_str,
                er.payload_json,
                er.subject_guid,
                signer.pubkey_hex(),
                "",
                er.created_at,
                warnings_json,
            ],
            |row| row.get::<_, i64>(0),
        )?;
        // Sign with the assigned seq and update the row.
        let (signed_by, signature) = signer.sign_event(
            &er.event_id, &er.event_type, &er.payload_json, &er.subject_guid, er.created_at, seq,
        );
        update_event_signature(&tx, &er.event_id, &signed_by, &signature)?;
        seqs.push((seq, signed_by, signature));
    }

    // Issue-5 ingest atomic — 2026-03-13
    // Issue-WRITE-AMP — 2026-03-14: only recompute search/quality for
    // tracks that actually changed. Feed and artist are always recomputed
    // (one entity each — negligible cost).
    // 6. Populate search index + compute quality scores inside the same
    //    transaction so they are atomic with the entity and event writes.
    {
        // Derive the set of changed tracks from event_rows to avoid
        // redundant search/quality recomputation for unchanged tracks.
        let changed_track_guids: std::collections::HashSet<&str> = all_event_rows
            .iter()
            .filter(|e| matches!(e.event_type, EventType::TrackUpserted))
            .map(|e| e.subject_guid.as_str())
            .collect();

        // Feed search index + quality (always — single entity, negligible)
        crate::search::populate_search_index(
            &tx, "feed", &feed.feed_guid,
            "", &feed.title,
            feed.description.as_deref().unwrap_or(""),
            feed.raw_medium.as_deref().unwrap_or(""),
        )?;
        let feed_score = crate::quality::compute_feed_quality(&tx, &feed.feed_guid)?;
        crate::quality::store_quality(&tx, "feed", &feed.feed_guid, feed_score)?;

        // Artist quality + search index (always — single entity, negligible)
        let artist_score = crate::quality::compute_artist_quality(&tx, &artist.artist_id)?;
        crate::quality::store_quality(&tx, "artist", &artist.artist_id, artist_score)?;
        crate::search::populate_search_index(
            &tx, "artist", &artist.artist_id,
            &artist.name, "",
            "",
            "",
        )?;

        // Issue-WRITE-AMP — 2026-03-14: track search index + quality —
        // only for new or changed tracks. This is where the N multiplier
        // caused the write amplification bug.
        for (track, _, _) in &tracks {
            if changed_track_guids.contains(track.track_guid.as_str()) {
                crate::search::populate_search_index(
                    &tx, "track", &track.track_guid,
                    "", &track.title,
                    track.description.as_deref().unwrap_or(""),
                    "",
                )?;
                let track_score = crate::quality::compute_track_quality(&tx, &track.track_guid)?;
                crate::quality::store_quality(&tx, "track", &track.track_guid, track_score)?;
            }
        }
    }

    // 7. Commit
    tx.commit()?;

    Ok(seqs)
}

// ── External ID operations ──────────────────────────────────────────────────

/// Links an external identifier (e.g. `MusicBrainz`, ISRC, Spotify) to an entity.
///
/// Uses `INSERT OR REPLACE` so a second call with the same `(entity_type,
/// entity_id, scheme)` triple updates the stored `value`.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL upsert fails.
pub fn link_external_id(
    conn:        &Connection,
    entity_type: &str,
    entity_id:   &str,
    scheme:      &str,
    value:       &str,
) -> Result<i64, DbError> {
    let now = unix_now();
    conn.execute(
        "INSERT OR REPLACE INTO external_ids (entity_type, entity_id, scheme, value, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![entity_type, entity_id, scheme, value, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Returns all external IDs linked to the given entity.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_external_ids(
    conn:        &Connection,
    entity_type: &str,
    entity_id:   &str,
) -> Result<Vec<ExternalIdRow>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, scheme, value FROM external_ids \
         WHERE entity_type = ?1 AND entity_id = ?2 \
         ORDER BY scheme",
    )?;
    let rows = stmt.query_map(params![entity_type, entity_id], |row| {
        Ok(ExternalIdRow {
            id:     row.get(0)?,
            scheme: row.get(1)?,
            value:  row.get(2)?,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Given a `(scheme, value)` pair, returns the `(entity_type, entity_id)` that
/// owns it, or `None` if no matching row exists.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn reverse_lookup_external_id(
    conn:   &Connection,
    scheme: &str,
    value:  &str,
) -> Result<Option<(String, String)>, DbError> {
    let result = conn.query_row(
        "SELECT entity_type, entity_id FROM external_ids \
         WHERE scheme = ?1 AND value = ?2",
        params![scheme, value],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    Ok(result)
}

// ── Provenance operations ───────────────────────────────────────────────────

/// Records how an entity was discovered or imported.
///
/// `source_type` should be one of: `"rss_crawl"`, `"manifest"`, `"manual"`,
/// `"bulk_import"`. `trust_level`: 0 = unknown, 1 = rss, 2 = signed manifest,
/// 3 = verified.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL insert fails.
pub fn record_entity_source(
    conn:        &Connection,
    entity_type: &str,
    entity_id:   &str,
    source_type: &str,
    source_url:  Option<&str>,
    trust_level: i64,
) -> Result<i64, DbError> {
    let now = unix_now();
    conn.execute(
        "INSERT INTO entity_source (entity_type, entity_id, source_type, source_url, trust_level, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![entity_type, entity_id, source_type, source_url, trust_level, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Returns all provenance records for the given entity.
///
/// # Errors
///
/// Returns [`DbError`] if the SQL query fails.
pub fn get_entity_sources(
    conn:        &Connection,
    entity_type: &str,
    entity_id:   &str,
) -> Result<Vec<EntitySourceRow>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT id, source_type, source_url, trust_level, created_at \
         FROM entity_source \
         WHERE entity_type = ?1 AND entity_id = ?2 \
         ORDER BY created_at",
    )?;
    let rows = stmt.query_map(params![entity_type, entity_id], |row| {
        Ok(EntitySourceRow {
            id:          row.get(0)?,
            source_type: row.get(1)?,
            source_url:  row.get(2)?,
            trust_level: row.get(3)?,
            created_at:  row.get(4)?,
        })
    })?;
    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}
