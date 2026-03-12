//! Entity quality scoring.
//!
//! Quality scores are integers in the range 0–100 that reflect how complete
//! and rich an entity's metadata is. Higher scores indicate more fully
//! populated records. The scores are stored in `entity_quality` and can be
//! used to weight search results via the `search` module.

#![allow(dead_code)]

use rusqlite::{Connection, OptionalExtension, params};

use crate::db::DbError;

/// Returns the current unix timestamp in seconds.
fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed()
}

/// Fields fetched from the `artists` table for quality computation.
struct ArtistFields {
    name:       Option<String>,
    sort_name:  Option<String>,
    area:       Option<String>,
    img_url:    Option<String>,
    url:        Option<String>,
    begin_year: Option<i64>,
}

/// Fields fetched from the `feeds` table for quality computation.
struct FeedFields {
    title:            Option<String>,
    description:      Option<String>,
    image_url:        Option<String>,
    language:         Option<String>,
    episode_count:    i64,
    artist_credit_id: i64,
    newest_item_at:   Option<i64>,
    explicit:         i64,
    itunes_type:      Option<String>,
}

/// Computes a quality score (0–100) for an artist based on field completeness.
///
/// Scoring breakdown:
/// - `name` present: 10
/// - `sort_name` present: 10
/// - `area` present: 10
/// - `img_url` present: 15
/// - `url` present: 10
/// - `begin_year` present: 5
/// - aliases count: min(count * 5, 15)
/// - feeds count (via `artist_credit_name` + `feeds`): min(count * 5, 25)
pub fn compute_artist_quality(conn: &Connection, artist_id: &str) -> Result<i64, DbError> {
    let mut score: i64 = 0;

    // Single query for the artist row fields.
    let row: Option<ArtistFields> = conn.query_row(
        "SELECT name, sort_name, area, img_url, url, begin_year \
         FROM artists WHERE artist_id = ?1",
        params![artist_id],
        |row| Ok(ArtistFields {
            name:       row.get(0)?,
            sort_name:  row.get(1)?,
            area:       row.get(2)?,
            img_url:    row.get(3)?,
            url:        row.get(4)?,
            begin_year: row.get(5)?,
        }),
    ).optional()?;

    if let Some(a) = row {
        if a.name.is_some() { score += 10; }
        if a.sort_name.is_some() { score += 10; }
        if a.area.is_some() { score += 10; }
        if a.img_url.is_some() { score += 15; }
        if a.url.is_some() { score += 10; }
        if a.begin_year.is_some() { score += 5; }
    }

    // Aliases count.
    let alias_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM artist_aliases WHERE artist_id = ?1",
        params![artist_id],
        |row| row.get(0),
    )?;
    score += (alias_count * 5).min(15);

    // Feeds count — number of distinct feeds this artist is credited on.
    let feed_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT f.feed_guid) \
         FROM artist_credit_name acn \
         JOIN feeds f ON f.artist_credit_id = acn.artist_credit_id \
         WHERE acn.artist_id = ?1",
        params![artist_id],
        |row| row.get(0),
    )?;
    score += (feed_count * 5).min(25);

    Ok(score)
}

/// Computes a quality score (0–100) for a feed based on field completeness.
///
/// Scoring breakdown:
/// - `title` present and non-empty: 10
/// - `description` present and non-empty: 15
/// - `image_url` present: 15
/// - `language` present: 5
/// - `episode_count` > 0: 5
/// - has tracks: 10
/// - has payment routes (feed-level or track-level): 15
/// - `artist_credit_id` present: 10
/// - `newest_item_at` present: 5
/// - `explicit` explicitly set (non-zero): 5
/// - `itunes_type` present: 5
pub fn compute_feed_quality(conn: &Connection, feed_guid: &str) -> Result<i64, DbError> {
    let mut score: i64 = 0;

    let row: Option<FeedFields> = conn.query_row(
        "SELECT title, description, image_url, language, episode_count, \
                artist_credit_id, newest_item_at, explicit, itunes_type \
         FROM feeds WHERE feed_guid = ?1",
        params![feed_guid],
        |row| Ok(FeedFields {
            title:            row.get(0)?,
            description:      row.get(1)?,
            image_url:        row.get(2)?,
            language:         row.get(3)?,
            episode_count:    row.get(4)?,
            artist_credit_id: row.get(5)?,
            newest_item_at:   row.get(6)?,
            explicit:         row.get(7)?,
            itunes_type:      row.get(8)?,
        }),
    ).optional()?;

    if let Some(f) = row {
        if f.title.as_ref().is_some_and(|t| !t.is_empty()) { score += 10; }
        if f.description.as_ref().is_some_and(|d| !d.is_empty()) { score += 15; }
        if f.image_url.is_some() { score += 15; }
        if f.language.is_some() { score += 5; }
        if f.episode_count > 0 { score += 5; }
        if f.artist_credit_id > 0 { score += 10; }
        if f.newest_item_at.is_some() { score += 5; }
        if f.explicit != 0 { score += 5; }
        if f.itunes_type.is_some() { score += 5; }
    }

    // Has tracks?
    let track_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tracks WHERE feed_guid = ?1 LIMIT 1",
        params![feed_guid],
        |row| row.get(0),
    )?;
    if track_count > 0 { score += 10; }

    // Has payment routes? (feed-level or any track-level route for this feed)
    let has_routes: bool = conn.query_row(
        "SELECT EXISTS( \
             SELECT 1 FROM feed_payment_routes WHERE feed_guid = ?1 \
             UNION ALL \
             SELECT 1 FROM payment_routes WHERE feed_guid = ?1 \
             LIMIT 1 \
         )",
        params![feed_guid],
        |row| row.get(0),
    )?;
    if has_routes { score += 15; }

    Ok(score)
}

/// Upserts a quality score into the `entity_quality` table.
pub fn store_quality(
    conn: &Connection,
    entity_type: &str,
    entity_id: &str,
    score: i64,
) -> Result<(), DbError> {
    let now = unix_now();

    conn.execute(
        "INSERT INTO entity_quality (entity_type, entity_id, score, computed_at) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(entity_type, entity_id) \
         DO UPDATE SET score = excluded.score, computed_at = excluded.computed_at",
        params![entity_type, entity_id, score, now],
    )?;

    Ok(())
}

/// Returns the quality score for an entity, defaulting to 0 if none is stored.
pub fn get_quality(
    conn: &Connection,
    entity_type: &str,
    entity_id: &str,
) -> Result<i64, DbError> {
    let score: Option<i64> = conn.query_row(
        "SELECT score FROM entity_quality \
         WHERE entity_type = ?1 AND entity_id = ?2",
        params![entity_type, entity_id],
        |row| row.get(0),
    ).optional()?;

    Ok(score.unwrap_or(0))
}
