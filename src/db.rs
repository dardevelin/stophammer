// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

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
use crate::model::{Artist, Feed, PaymentRoute, Track, ValueTimeSplit};
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

// WHY serde_json: EventType uses #[serde(rename_all = "snake_case")], so
// serde_json::to_string is the canonical way to obtain the correct snake_case
// string (e.g. "feed_upserted") without duplicating the rename logic here.
// It produces a quoted JSON string like "\"feed_upserted\"" — we strip the
// surrounding quotes to store the bare value in the events table.
fn event_type_str(et: &EventType) -> Result<String, DbError> {
    let s = serde_json::to_string(et)?;
    // serde_json produces "\"feed_upserted\"" — strip the surrounding quotes
    Ok(s.trim_matches('"').to_string())
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
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if any SQL operation fails.
pub fn resolve_artist(conn: &Connection, name: &str) -> Result<Artist, DbError> {
    let name_lower = name.to_lowercase();

    // 1. Check alias table first.
    let via_alias: Option<Artist> = conn.query_row(
        "SELECT a.artist_id, a.name, a.name_lower, a.created_at \
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
                created_at: row.get(3)?,
            })
        },
    ).optional()?;

    if let Some(a) = via_alias {
        return Ok(a);
    }

    // 2. Fall back to direct name_lower match (handles rows predating the alias table).
    let existing: Option<Artist> = conn.query_row(
        "SELECT artist_id, name, name_lower, created_at FROM artists WHERE name_lower = ?1",
        params![name_lower],
        |row| {
            Ok(Artist {
                artist_id:  row.get(0)?,
                name:       row.get(1)?,
                name_lower: row.get(2)?,
                created_at: row.get(3)?,
            })
        },
    ).optional()?;

    if let Some(a) = existing {
        // Back-fill the alias row for this legacy artist so future lookups hit
        // the alias table instead of the slow name_lower scan.
        conn.execute(
            "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
             VALUES (?1, ?2, ?3)",
            params![name_lower, a.artist_id, a.created_at],
        )?;
        return Ok(a);
    }

    // 3. New artist — insert artist row and its canonical alias.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();
    let artist_id = uuid::Uuid::new_v4().to_string();

    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![artist_id, name, name_lower, now],
    )?;

    conn.execute(
        "INSERT INTO artist_aliases (alias_lower, artist_id, created_at) VALUES (?1, ?2, ?3)",
        params![name_lower, artist_id, now],
    )?;

    Ok(Artist { artist_id, name: name.to_string(), name_lower, created_at: now })
}

// ── add_artist_alias ──────────────────────────────────────────────────────────

/// Registers `alias` (lowercased) as an additional lookup key for `artist_id`.
///
/// Uses `INSERT OR IGNORE` — if the `(alias_lower, artist_id)` pair already
/// exists the call is a no-op.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL operation fails (e.g. `artist_id`
/// does not exist and the foreign-key constraint fires).
pub fn add_artist_alias(conn: &Connection, artist_id: &str, alias: &str) -> Result<(), DbError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();

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
/// All feeds and tracks pointing to `source` are repointed to `target`. All
/// aliases of `source` that do not already exist on `target` are transferred;
/// any that would conflict are dropped. The `source` artist row is then
/// deleted. Returns the list of alias strings that were transferred.
///
/// This operation is intentionally admin-only and not automatic: determining
/// whether two artists with the same name are truly the same entity requires
/// human judgment (see ADR 0014).
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if any SQL operation fails. The transaction is
/// rolled back automatically on error.
pub fn merge_artists(
    conn: &mut Connection,
    source_artist_id: &str,
    target_artist_id: &str,
) -> Result<Vec<String>, DbError> {
    let tx = conn.transaction()?;

    // Collect the aliases that will be transferred (those that do not already
    // exist on target) before we mutate the table.
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

    // Repoint feeds and tracks.
    tx.execute(
        "UPDATE feeds SET artist_id = ?1 WHERE artist_id = ?2",
        params![target_artist_id, source_artist_id],
    )?;
    tx.execute(
        "UPDATE tracks SET artist_id = ?1 WHERE artist_id = ?2",
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
///
/// Used by the community node to replay `ArtistUpserted` events without
/// overwriting locally-resolved `created_at` timestamps.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL `INSERT OR IGNORE` fails.
pub fn upsert_artist_if_absent(conn: &Connection, artist: &Artist) -> Result<(), DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO artists (artist_id, name, name_lower, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![artist.artist_id, artist.name, artist.name_lower, artist.created_at],
    )?;
    Ok(())
}

// ── upsert_feed ───────────────────────────────────────────────────────────────

/// Inserts or updates a feed row keyed on `feed_guid`.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL upsert fails.
pub fn upsert_feed(conn: &Connection, feed: &Feed) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, created_at, \
         updated_at, raw_medium) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(feed_guid) DO UPDATE SET \
           feed_url       = excluded.feed_url, \
           title          = excluded.title, \
           title_lower    = excluded.title_lower, \
           artist_id      = excluded.artist_id, \
           description    = excluded.description, \
           image_url      = excluded.image_url, \
           language       = excluded.language, \
           explicit       = excluded.explicit, \
           itunes_type    = excluded.itunes_type, \
           episode_count  = excluded.episode_count, \
           newest_item_at = excluded.newest_item_at, \
           oldest_item_at = excluded.oldest_item_at, \
           updated_at     = excluded.updated_at, \
           raw_medium     = excluded.raw_medium",
        params![
            feed.feed_guid,
            feed.feed_url,
            feed.title,
            feed.title_lower,
            feed.artist_id,
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
/// Returns [`DbError::Rusqlite`] if the SQL upsert fails.
pub fn upsert_track(conn: &Connection, track: &Track) -> Result<(), DbError> {
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_id, title, title_lower, pub_date, \
         duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, season, \
         explicit, description, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(track_guid) DO UPDATE SET \
           feed_guid       = excluded.feed_guid, \
           artist_id       = excluded.artist_id, \
           title           = excluded.title, \
           title_lower     = excluded.title_lower, \
           pub_date        = excluded.pub_date, \
           duration_secs   = excluded.duration_secs, \
           enclosure_url   = excluded.enclosure_url, \
           enclosure_type  = excluded.enclosure_type, \
           enclosure_bytes = excluded.enclosure_bytes, \
           track_number    = excluded.track_number, \
           season          = excluded.season, \
           explicit        = excluded.explicit, \
           description     = excluded.description, \
           updated_at      = excluded.updated_at",
        params![
            track.track_guid,
            track.feed_guid,
            track.artist_id,
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
/// Returns [`DbError::Rusqlite`] if any SQL operation fails, or
/// [`DbError::Json`] if a `route_type` variant cannot be serialised.
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

// ── replace_value_time_splits ─────────────────────────────────────────────────

/// Deletes all value-time splits for `source_track_guid` and inserts `splits`.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if any SQL operation fails.
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
///
/// # Errors
///
/// Returns [`DbError::Json`] if `event_type` or `warnings` cannot be
/// serialised, or [`DbError::Rusqlite`] if the SQL insert fails.
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
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL upsert fails.
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
/// Returns [`DbError::Rusqlite`] if the query fails, or [`DbError::Json`] if
/// any stored `event_type`, `payload_json`, or `warnings_json` cannot be
/// deserialised.
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

        // Deserialize event_type: re-add quotes so serde_json can parse the snake_case variant
        let et_quoted = format!("\"{et_str}\"");
        let event_type: EventType = serde_json::from_str(&et_quoted)?;

        // payload_json stores the inner payload struct (e.g. ArtistUpsertedPayload).
        // EventPayload uses #[serde(tag="type", content="data")] so we must wrap it
        // back into the tagged envelope before deserializing through the enum.
        let tagged = format!(r#"{{"type":"{et_str}","data":{payload_json}}}"#);
        let payload: EventPayload = serde_json::from_str(&tagged)?;
        let warnings: Vec<String> = serde_json::from_str(&warnings_json)?;

        events.push(Event {
            event_id,
            event_type,
            payload,
            // payload_json carries the original inner-struct JSON so that
            // verify_event_signature can hash the same bytes that were signed.
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
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the query or row mapping fails.
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
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL upsert fails.
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
    /// Hex-encoded ed25519 public key identifying this peer.
    pub node_pubkey:          String,
    /// Base URL where this peer's `/sync/push` endpoint is reachable.
    pub node_url:             String,
    /// Unix timestamp (seconds) when this peer was first seen.
    #[expect(dead_code, reason = "returned in GET /sync/peers for future use")]
    pub discovered_at:        i64,
    /// Unix timestamp (seconds) of the last successful push, if any.
    pub last_push_at:         Option<i64>,
    /// Number of consecutive delivery failures; peers at 5+ are skipped.
    #[expect(dead_code, reason = "used indirectly via SQL WHERE clause in get_push_peers")]
    pub consecutive_failures: i64,
}

/// Returns all peers with `consecutive_failures < 5`.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the query or row mapping fails.
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
///
/// On conflict updates `node_url` only — preserves `discovered_at` and does
/// **not** reset `consecutive_failures` (re-registration resets separately).
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL upsert fails.
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

/// Records a successful push delivery: updates `last_push_at` and resets
/// `consecutive_failures` to 0.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL update fails.
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
/// Returns [`DbError::Rusqlite`] if the SQL update fails.
pub fn increment_peer_failures(conn: &Connection, node_pubkey: &str) -> Result<(), DbError> {
    conn.execute(
        "UPDATE peer_nodes SET consecutive_failures = consecutive_failures + 1 \
         WHERE node_pubkey = ?1",
        rusqlite::params![node_pubkey],
    )?;
    Ok(())
}

/// Resets `consecutive_failures` to 0 for the given peer (called on re-registration).
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the SQL update fails.
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
/// Returns [`DbError::Json`] if `event_type` or `warnings` cannot be
/// serialised, or [`DbError::Rusqlite`] if the SQL fails.
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

    // Use INSERT OR IGNORE so a duplicate event_id is a no-op.
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

    // Retrieve the seq that was just assigned.
    let seq: i64 = conn.query_row(
        "SELECT seq FROM events WHERE event_id = ?1",
        rusqlite::params![event_id],
        |row| row.get(0),
    )?;

    Ok(Some(seq))
}

// ── get_node_sync_cursor ──────────────────────────────────────────────────────

/// Returns the `last_seq` cursor stored for `node_pubkey`, or `0` if none exists.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the query fails.
pub fn get_node_sync_cursor(conn: &Connection, node_pubkey: &str) -> Result<i64, DbError> {
    let seq: Option<i64> = conn.query_row(
        "SELECT last_seq FROM node_sync_state WHERE node_pubkey = ?1",
        params![node_pubkey],
        |row| row.get(0),
    ).optional()?;
    Ok(seq.unwrap_or(0))
}

// ── get_existing_feed ─────────────────────────────────────────────────────────

/// Looks up the feed row whose `feed_url` matches, returning `None` if absent.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if the query or row mapping fails.
pub fn get_existing_feed(
    conn:     &Connection,
    feed_url: &str,
) -> Result<Option<Feed>, DbError> {
    let result = conn.query_row(
        "SELECT feed_guid, feed_url, title, title_lower, artist_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, \
         created_at, updated_at, raw_medium \
         FROM feeds WHERE feed_url = ?1",
        params![feed_url],
        |row| {
            let explicit_i: i64 = row.get(8)?;
            Ok(Feed {
                feed_guid:      row.get(0)?,
                feed_url:       row.get(1)?,
                title:          row.get(2)?,
                title_lower:    row.get(3)?,
                artist_id:      row.get(4)?,
                description:    row.get(5)?,
                image_url:      row.get(6)?,
                language:       row.get(7)?,
                explicit:       explicit_i != 0,
                itunes_type:    row.get(9)?,
                episode_count:  row.get(10)?,
                newest_item_at: row.get(11)?,
                oldest_item_at: row.get(12)?,
                created_at:     row.get(13)?,
                updated_at:     row.get(14)?,
                raw_medium:     row.get(15)?,
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
// Extracting shared SQL into `const` strings would add complexity for no
// safety benefit; the duplication is localised here and should stay.
/// Writes a complete feed ingest atomically and returns the new event `seq` values.
///
/// Upserts the artist, feed, all tracks (with payment routes and value-time
/// splits), and inserts the supplied event rows — all inside one `SQLite`
/// transaction. Returns the monotonically assigned `seq` for each event in
/// the same order as `event_rows`.
///
/// # Errors
///
/// Returns [`DbError::Rusqlite`] if any SQL operation fails (the transaction
/// is automatically rolled back), or [`DbError::Json`] if an `event_type` or
/// `route_type` variant cannot be serialised.
#[expect(clippy::too_many_lines, reason = "single atomic transaction — splitting would obscure the transactional boundary")]
#[expect(clippy::needless_pass_by_value, reason = "takes ownership to make the transaction boundary clear at call sites")]
pub fn ingest_transaction(
    conn:        &mut Connection,
    artist:      Artist,
    feed:        Feed,
    tracks:      Vec<(Track, Vec<PaymentRoute>, Vec<ValueTimeSplit>)>,
    event_rows:  Vec<EventRow>,
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
                "INSERT INTO artists (artist_id, name, name_lower, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![artist.artist_id, artist.name, name_lower, artist.created_at],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![name_lower, artist.artist_id, artist.created_at],
            )?;
        } else {
            // Back-fill alias for pre-existing artist rows that predate the alias table.
            tx.execute(
                "INSERT OR IGNORE INTO artist_aliases (alias_lower, artist_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![name_lower, artist.artist_id, artist.created_at],
            )?;
        }
    }

    // 2. Upsert feed
    tx.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_id, description, image_url, \
         language, explicit, itunes_type, episode_count, newest_item_at, oldest_item_at, created_at, \
         updated_at, raw_medium) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
         ON CONFLICT(feed_guid) DO UPDATE SET \
           feed_url       = excluded.feed_url, \
           title          = excluded.title, \
           title_lower    = excluded.title_lower, \
           artist_id      = excluded.artist_id, \
           description    = excluded.description, \
           image_url      = excluded.image_url, \
           language       = excluded.language, \
           explicit       = excluded.explicit, \
           itunes_type    = excluded.itunes_type, \
           episode_count  = excluded.episode_count, \
           newest_item_at = excluded.newest_item_at, \
           oldest_item_at = excluded.oldest_item_at, \
           updated_at     = excluded.updated_at, \
           raw_medium     = excluded.raw_medium",
        params![
            feed.feed_guid,
            feed.feed_url,
            feed.title,
            feed.title_lower,
            feed.artist_id,
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

    // 3. Tracks, routes, splits
    for (track, routes, splits) in &tracks {
        // upsert track
        tx.execute(
            "INSERT INTO tracks (track_guid, feed_guid, artist_id, title, title_lower, pub_date, \
             duration_secs, enclosure_url, enclosure_type, enclosure_bytes, track_number, season, \
             explicit, description, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16) \
             ON CONFLICT(track_guid) DO UPDATE SET \
               feed_guid       = excluded.feed_guid, \
               artist_id       = excluded.artist_id, \
               title           = excluded.title, \
               title_lower     = excluded.title_lower, \
               pub_date        = excluded.pub_date, \
               duration_secs   = excluded.duration_secs, \
               enclosure_url   = excluded.enclosure_url, \
               enclosure_type  = excluded.enclosure_type, \
               enclosure_bytes = excluded.enclosure_bytes, \
               track_number    = excluded.track_number, \
               season          = excluded.season, \
               explicit        = excluded.explicit, \
               description     = excluded.description, \
               updated_at      = excluded.updated_at",
            params![
                track.track_guid,
                track.feed_guid,
                track.artist_id,
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

    // 4. Insert events, collect seqs
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

    // 5. Commit
    tx.commit()?;

    Ok(seqs)
}
