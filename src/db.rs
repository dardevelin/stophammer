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
use crate::model::{Artist, ArtistCredit, ArtistCreditName, Feed, FeedPaymentRoute, PaymentRoute, Track, ValueTimeSplit};
use crate::event::{Event, EventPayload, EventType};

pub type Db = Arc<Mutex<Connection>>;

// ── Errors ──────────────────────────────────────────────────────────────────

/// Errors returned by all database operations in this module.
pub enum DbError {
    /// A rusqlite operation failed (query, execute, or schema application).
    Rusqlite(rusqlite::Error),
    /// A JSON serialisation or deserialisation step failed.
    Json(serde_json::Error),
}

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::Rusqlite(e)
    }
}

impl From<serde_json::Error> for DbError {
    fn from(e: serde_json::Error) -> Self {
        DbError::Json(e)
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Rusqlite(e) => write!(f, "SQLite error: {e}"),
            DbError::Json(e)     => write!(f, "JSON error: {e}"),
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
            DbError::Rusqlite(e) => Some(e),
            DbError::Json(e)     => Some(e),
        }
    }
}

// ── EventRow ─────────────────────────────────────────────────────────────────

/// A pre-assembled event ready to be written to the `events` table.
pub struct EventRow {
    /// Globally unique identifier for this event (UUID v4).
    pub event_id:     String,
    /// Discriminant describing the kind of state change this event records.
    pub event_type:   EventType,
    /// Canonical JSON representation of the event-specific payload.
    pub payload_json: String,
    /// GUID of the primary entity this event concerns (feed, track, etc.).
    pub subject_guid: String,
    /// Hex-encoded ed25519 public key of the node that signed this event.
    pub signed_by:    String,
    /// Hex-encoded ed25519 signature over the canonical signing payload.
    pub signature:    String,
    /// Unix timestamp (seconds) at which the event was created.
    pub created_at:   i64,
    /// Human-readable warnings produced by the verifier chain, if any.
    pub warnings:     Vec<String>,
}

// ── ExternalIdRow ──────────────────────────────────────────────────────────────

/// A row from the `external_ids` table linking an entity to an external system.
#[allow(dead_code)]
pub struct ExternalIdRow {
    pub id:     i64,
    pub scheme: String,
    pub value:  String,
}

// ── EntitySourceRow ────────────────────────────────────────────────────────────

/// A row from the `entity_source` table recording where an entity came from.
#[allow(dead_code)]
pub struct EntitySourceRow {
    pub id:          i64,
    pub source_type: String,
    pub source_url:  Option<String>,
    pub trust_level: i64,
    pub created_at:  i64,
}

// ── Schema constant ──────────────────────────────────────────────────────────

const SCHEMA: &str = include_str!("schema.sql");

// ── open_db ──────────────────────────────────────────────────────────────────

/// Opens the `SQLite` database at `path` and applies the bundled schema.
///
/// # Panics
///
/// Panics if the file cannot be opened (e.g. permission denied) or if the
/// schema SQL fails to execute. Both are unrecoverable startup failures.
pub fn open_db(path: &str) -> Connection {
    let conn = Connection::open(path).expect("failed to open database");
    conn.execute_batch(SCHEMA).expect("failed to apply schema");
    conn
}

// ── Helper: serialize EventType to snake_case string (no quotes) ─────────────

fn event_type_str(et: &EventType) -> Result<String, DbError> {
    let s = serde_json::to_string(et)?;
    Ok(s.trim_matches('"').to_string())
}

// ── Helper: current unix timestamp ──────────────────────────────────────────

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
}

// ── resolve_artist ────────────────────────────────────────────────────────────

/// Returns an existing artist matched by alias or lowercased `name`, or inserts
/// a new one and auto-registers its canonical name as an alias.
///
/// Resolution order:
/// 1. `artist_aliases.alias_lower = name.to_lowercase()` — alias lookup.
/// 2. `artists.name_lower = name.to_lowercase()` — direct name lookup (legacy
///    rows created before the alias table existed).
/// 3. Insert new artist + insert a canonical alias row.
pub fn resolve_artist(conn: &Connection, name: &str) -> Result<Artist, DbError> {
    let name_lower = name.to_lowercase();
    let now = unix_now();

    // 1. Check alias table first.
    let via_alias: Option<Artist> = conn.query_row(
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
    ).optional()?;

    if let Some(a) = via_alias {
        return Ok(a);
    }

    // 2. Fall back to direct name_lower match.
    let existing: Option<Artist> = conn.query_row(
        "SELECT artist_id, name, name_lower, sort_name, type_id, area, \
         img_url, url, begin_year, end_year, created_at, updated_at \
         FROM artists WHERE name_lower = ?1",
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
    ).optional()?;

    if let Some(a) = existing {
        conn.execute(
            "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
             VALUES (?1, ?2, ?3)",
            params![name_lower, a.artist_id, a.created_at],
        )?;
        return Ok(a);
    }

    // 3. New artist — insert artist row and its canonical alias.
    let artist_id = uuid::Uuid::new_v4().to_string();

    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![artist_id, name, name_lower, now, now],
    )?;

    conn.execute(
        "INSERT INTO artist_aliases (alias_lower, artist_id, created_at) VALUES (?1, ?2, ?3)",
        params![name_lower, artist_id, now],
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

// ── add_artist_alias ──────────────────────────────────────────────────────────

/// Registers `alias` (lowercased) as an additional lookup key for `artist_id`.
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
pub fn merge_artists(
    conn: &mut Connection,
    source_artist_id: &str,
    target_artist_id: &str,
) -> Result<Vec<String>, DbError> {
    let tx = conn.transaction()?;

    // Collect the aliases that will be transferred.
    let mut stmt = tx.prepare(
        "SELECT alias_lower FROM artist_aliases \
         WHERE artist_id = ?1 \
           AND NOT EXISTS ( \
               SELECT 1 FROM artist_aliases \
               WHERE alias_lower = artist_aliases.alias_lower \
                 AND artist_id = ?2 \
           )",
    )?;
    let transferred: Vec<String> = stmt
        .query_map(params![source_artist_id, target_artist_id], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    drop(stmt);

    // Repoint artist_credit_name entries.
    tx.execute(
        "UPDATE artist_credit_name SET artist_id = ?1 WHERE artist_id = ?2",
        params![target_artist_id, source_artist_id],
    )?;

    // Transfer non-conflicting aliases.
    tx.execute(
        "UPDATE artist_aliases SET artist_id = ?1 \
         WHERE artist_id = ?2 \
           AND NOT EXISTS ( \
               SELECT 1 FROM artist_aliases \
               WHERE alias_lower = artist_aliases.alias_lower \
                 AND artist_id = ?1 \
           )",
        params![target_artist_id, source_artist_id],
    )?;

    // Drop any remaining source aliases (those that conflicted).
    tx.execute(
        "DELETE FROM artist_aliases WHERE artist_id = ?1",
        params![source_artist_id],
    )?;

    // Record redirect for old ID resolution.
    let now = unix_now();
    tx.execute(
        "INSERT OR REPLACE INTO artist_id_redirect (old_artist_id, new_artist_id, merged_at) \
         VALUES (?1, ?2, ?3)",
        params![source_artist_id, target_artist_id, now],
    )?;

    // Delete the source artist row.
    tx.execute(
        "DELETE FROM artists WHERE artist_id = ?1",
        params![source_artist_id],
    )?;

    tx.commit()?;

    Ok(transferred)
}

// ── upsert_artist_if_absent ───────────────────────────────────────────────────

/// Inserts the artist if no row with the same `artist_id` exists yet.
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
pub fn create_artist_credit(
    conn: &Connection,
    display_name: &str,
    names: &[(String, String, String)],  // (artist_id, credited_name, join_phrase)
) -> Result<ArtistCredit, DbError> {
    let now = unix_now();

    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params![display_name, now],
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
        created_at:   now,
        names:        credit_names,
    })
}

/// Creates a simple single-artist credit and returns it.
#[expect(dead_code, reason = "convenience wrapper kept for future single-artist credit creation")]
pub fn create_single_artist_credit(
    conn: &Connection,
    artist: &Artist,
) -> Result<ArtistCredit, DbError> {
    create_artist_credit(
        conn,
        &artist.name,
        &[(artist.artist_id.clone(), artist.name.clone(), String::new())],
    )
}

/// Retrieves an artist credit by ID, including its constituent names.
#[expect(dead_code, reason = "used by query API tests and future enrichment")]
pub fn get_artist_credit(conn: &Connection, credit_id: i64) -> Result<Option<ArtistCredit>, DbError> {
    let credit: Option<(i64, String, i64)> = conn.query_row(
        "SELECT id, display_name, created_at FROM artist_credit WHERE id = ?1",
        params![credit_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;

    let Some((id, display_name, created_at)) = credit else {
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

    Ok(Some(ArtistCredit { id, display_name, created_at, names }))
}

/// Looks up an artist credit by display name (case-insensitive via `LOWER()`).
pub fn get_artist_credit_by_display_name(
    conn: &Connection,
    display_name: &str,
) -> Result<Option<ArtistCredit>, DbError> {
    let lower = display_name.to_lowercase();

    let credit: Option<(i64, String, i64)> = conn.query_row(
        "SELECT id, display_name, created_at FROM artist_credit WHERE LOWER(display_name) = ?1",
        params![lower],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;

    let Some((id, display_name, created_at)) = credit else {
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

    Ok(Some(ArtistCredit { id, display_name, created_at, names }))
}

/// Idempotent artist credit retrieval: returns an existing credit if one with
/// a matching `display_name` (case-insensitive) already exists, otherwise
/// creates a new credit with the given `names`.
pub fn get_or_create_artist_credit(
    conn: &Connection,
    display_name: &str,
    names: &[(String, String, String)],  // (artist_id, credited_name, join_phrase)
) -> Result<ArtistCredit, DbError> {
    if let Some(existing) = get_artist_credit_by_display_name(conn, display_name)? {
        return Ok(existing);
    }
    create_artist_credit(conn, display_name, names)
}

/// Returns all artist credits in which `artist_id` participates (via
/// `artist_credit_name` JOIN).
pub fn get_artist_credits_for_artist(
    conn: &Connection,
    artist_id: &str,
) -> Result<Vec<ArtistCredit>, DbError> {
    let mut credit_stmt = conn.prepare(
        "SELECT DISTINCT ac.id, ac.display_name, ac.created_at \
         FROM artist_credit ac \
         JOIN artist_credit_name acn ON acn.artist_credit_id = ac.id \
         WHERE acn.artist_id = ?1 \
         ORDER BY ac.id",
    )?;
    let credits: Vec<(i64, String, i64)> = credit_stmt.query_map(params![artist_id], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?.collect::<Result<_, _>>()?;

    let mut name_stmt = conn.prepare(
        "SELECT id, artist_credit_id, artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id = ?1 ORDER BY position",
    )?;

    let mut result = Vec::with_capacity(credits.len());
    for (id, display_name, created_at) in credits {
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
        result.push(ArtistCredit { id, display_name, created_at, names });
    }

    Ok(result)
}

// ── upsert_feed ───────────────────────────────────────────────────────────────

/// Inserts or updates a feed row keyed on `feed_guid`.
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

// ── insert_event ──────────────────────────────────────────────────────────────

/// Inserts a single event row and returns the assigned monotonic `seq`.
#[expect(clippy::too_many_arguments, reason = "all fields are required for a complete event row")]
pub fn insert_event(
    conn:         &Connection,
    event_id:     &str,
    event_type:   &EventType,
    payload_json: &str,
    subject_guid: &str,
    signed_by:    &str,
    signature:    &str,
    created_at:   i64,
    warnings:     &[String],
) -> Result<i64, DbError> {
    let et_str = event_type_str(event_type)?;
    let warnings_json = serde_json::to_string(warnings)?;

    let sql = "INSERT INTO events \
        (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8) \
        RETURNING seq";

    let seq = conn.query_row(
        sql,
        params![event_id, et_str, payload_json, subject_guid, signed_by, signature, created_at, warnings_json],
        |row| row.get::<_, i64>(0),
    )?;

    Ok(seq)
}

// ── upsert_feed_crawl_cache ───────────────────────────────────────────────────

/// Records the latest content hash and crawl timestamp for `feed_url`.
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
pub fn get_events_since(
    conn:       &Connection,
    after_seq:  i64,
    limit:      i64,
) -> Result<Vec<Event>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json \
         FROM events WHERE seq > ?1 ORDER BY seq ASC LIMIT ?2",
    )?;

    let rows = stmt.query_map(params![after_seq, limit], |row| {
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

/// Returns lightweight `(event_id, seq)` references for all events with `seq >= since_seq`.
pub fn get_event_refs_since(
    conn:      &Connection,
    since_seq: i64,
) -> Result<Vec<crate::sync::EventRef>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT event_id, seq FROM events WHERE seq >= ?1 ORDER BY seq ASC",
    )?;

    let rows = stmt.query_map(params![since_seq], |row| {
        Ok(crate::sync::EventRef {
            event_id: row.get(0)?,
            seq:      row.get(1)?,
        })
    })?;

    let mut refs = Vec::new();
    for row in rows {
        refs.push(row?);
    }
    Ok(refs)
}

// ── upsert_node_sync_state ────────────────────────────────────────────────────

/// Records or updates the last-seen sequence number for a peer node.
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
           last_seq     = excluded.last_seq, \
           last_seen_at = excluded.last_seen_at",
        params![node_pubkey, last_seq, last_seen_at],
    )?;
    Ok(())
}

// ── peer_nodes ────────────────────────────────────────────────────────────────

/// A peer node registered for push fan-out.
pub struct PeerNode {
    pub node_pubkey:          String,
    pub node_url:             String,
    #[expect(dead_code, reason = "returned in GET /sync/peers for future use")]
    pub discovered_at:        i64,
    pub last_push_at:         Option<i64>,
    #[expect(dead_code, reason = "used indirectly via SQL WHERE clause in get_push_peers")]
    pub consecutive_failures: i64,
}

/// Returns all peers with `consecutive_failures < 5`.
pub fn get_push_peers(conn: &Connection) -> Result<Vec<PeerNode>, DbError> {
    let mut stmt = conn.prepare(
        "SELECT node_pubkey, node_url, discovered_at, last_push_at, consecutive_failures \
         FROM peer_nodes WHERE consecutive_failures < 5",
    )?;

    let rows = stmt.query_map([], |row| {
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
pub fn record_push_success(conn: &Connection, node_pubkey: &str, now: i64) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET last_push_at = ?1, consecutive_failures = 0 \
         WHERE node_pubkey = ?2",
        rusqlite::params![now, node_pubkey],
    )?;
    Ok(())
}

/// Increments `consecutive_failures` by 1 for the given peer.
pub fn increment_peer_failures(conn: &Connection, node_pubkey: &str) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = consecutive_failures + 1 \
         WHERE node_pubkey = ?1",
        rusqlite::params![node_pubkey],
    )?;
    Ok(())
}

/// Resets `consecutive_failures` to 0 for the given peer.
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
    let et_str = event_type_str(event_type)?;
    let warnings_json = serde_json::to_string(warnings)?;

    let sql = "INSERT OR IGNORE INTO events \
        (event_id, event_type, payload_json, subject_guid, signed_by, signature, seq, created_at, warnings_json) \
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, (SELECT COALESCE(MAX(seq),0)+1 FROM events), ?7, ?8)";

    let changed = conn.execute(
        sql,
        rusqlite::params![event_id, et_str, payload_json, subject_guid, signed_by, signature, created_at, warnings_json],
    )?;

    if changed == 0 {
        return Ok(None);
    }

    let seq: i64 = conn.query_row(
        "SELECT seq FROM events WHERE event_id = ?1",
        rusqlite::params![event_id],
        |row| row.get(0),
    )?;

    Ok(Some(seq))
}

// ── get_node_sync_cursor ──────────────────────────────────────────────────────

/// Returns the `last_seq` cursor stored for `node_pubkey`, or `0` if none exists.
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
#[expect(dead_code, reason = "will be called from ingest/admin handlers")]
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
#[expect(dead_code, reason = "will be called from ingest/admin handlers")]
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
#[expect(dead_code, reason = "will be called from admin handlers")]
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
#[expect(dead_code, reason = "will be called from admin/query handlers")]
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
pub struct ArtistRelRow {
    #[allow(dead_code)]
    pub id:            i64,
    pub artist_id_a:   String,
    pub artist_id_b:   String,
    pub rel_type_name: String,
    pub begin_year:    Option<i64>,
    pub end_year:      Option<i64>,
}

/// Checks whether a `rel_type_id` exists in the `rel_type` lookup table.
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
#[expect(dead_code, reason = "will be called from admin/ingest handlers")]
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
#[expect(dead_code, reason = "will be called from admin/ingest handlers")]
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
#[expect(dead_code, reason = "will be called from admin/ingest handlers")]
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
#[expect(clippy::too_many_lines, reason = "single atomic transaction — splitting would obscure the transactional boundary")]
#[expect(clippy::needless_pass_by_value, reason = "takes ownership to make the transaction boundary clear at call sites")]
pub fn ingest_transaction(
    conn:               &mut Connection,
    artist:             Artist,
    artist_credit:      ArtistCredit,
    feed:               Feed,
    feed_routes:        Vec<FeedPaymentRoute>,
    tracks:             Vec<(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)>,
    event_rows:         Vec<EventRow>,
) -> Result<Vec<i64>, DbError> {
    let tx = conn.transaction()?;

    // 1. Resolve/insert artist (and ensure a canonical alias row exists)
    {
        let name_lower = artist.name.to_lowercase();
        let existing: Option<String> = tx.query_row(
            "SELECT artist_id FROM artists WHERE name_lower = ?1",
            params![name_lower],
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
            tx.execute(
                "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![name_lower, artist.artist_id, artist.created_at],
            )?;
        } else {
            tx.execute(
                "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![name_lower, artist.artist_id, artist.created_at],
            )?;
        }
    }

    // 2. Insert artist credit (idempotent via INSERT OR IGNORE on PK)
    {
        tx.execute(
            "INSERT OR IGNORE INTO artist_credit (id, display_name, created_at) \
             VALUES (?1, ?2, ?3)",
            params![artist_credit.id, artist_credit.display_name, artist_credit.created_at],
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

    // 5. Insert events, collect seqs
    let mut seqs = Vec::new();
    for er in &event_rows {
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
                er.signed_by,
                er.signature,
                er.created_at,
                warnings_json,
            ],
            |row| row.get::<_, i64>(0),
        )?;
        seqs.push(seq);
    }

    // 6. Commit
    tx.commit()?;

    Ok(seqs)
}

// ── External ID operations ──────────────────────────────────────────────────

/// Links an external identifier (e.g. `MusicBrainz`, ISRC, Spotify) to an entity.
///
/// Uses `INSERT OR REPLACE` so a second call with the same `(entity_type,
/// entity_id, scheme)` triple updates the stored `value`.
#[expect(dead_code, reason = "will be called from importer and enrichment pipelines")]
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
#[expect(dead_code, reason = "will be called from query API enrichment")]
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
#[expect(dead_code, reason = "will be called from reverse-lookup API endpoint")]
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
#[expect(dead_code, reason = "will be called from crawl and import pipelines")]
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
#[expect(dead_code, reason = "will be called from query API enrichment")]
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
