#![expect(clippy::significant_drop_tightening, reason = "MutexGuard<Connection> must be held for the full spawn_blocking scope")]

//! Query API handlers for the `/v1/*` read-only endpoints.
//!
//! All handlers are read-only and run on both primary and community nodes.
//! Pagination uses opaque base64-encoded cursors. Nested data can be requested
//! via the `?include=` query parameter.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rusqlite::params;
use serde::{Deserialize, Serialize};

use crate::{api, db};

// ── Pagination ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct Pagination {
    cursor:   Option<String>,
    has_more: bool,
}

fn encode_cursor(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value.as_bytes())
}

fn decode_cursor(cursor: &str) -> Result<String, api::ApiError> {
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_err| api::ApiError {
        status:  StatusCode::BAD_REQUEST,
        message: "invalid cursor".into(),
        www_authenticate: None,
    })?;
    String::from_utf8(bytes).map_err(|_err| api::ApiError {
        status:  StatusCode::BAD_REQUEST,
        message: "invalid cursor encoding".into(),
        www_authenticate: None,
    })
}

// ── Response envelope ───────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct QueryResponse<T> {
    data:       T,
    pagination: Pagination,
    meta:       ResponseMeta,
}

#[derive(Debug, Serialize)]
struct ResponseMeta {
    api_version: &'static str,
    node_pubkey: String,
}

fn meta(state: &api::AppState) -> ResponseMeta {
    ResponseMeta {
        api_version: "v1",
        node_pubkey: state.node_pubkey_hex.clone(),
    }
}

// ── Query params ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    cursor:  Option<String>,
    limit:   Option<i64>,
    include: Option<String>,
}

impl ListQuery {
    fn capped_limit(&self) -> i64 {
        self.limit.unwrap_or(50).clamp(1, 200)
    }

    fn includes(&self, field: &str) -> bool {
        self.include
            .as_deref()
            .is_some_and(|s| s.split(',').any(|f| f.trim() == field))
    }
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    q:       String,
    #[serde(rename = "type")]
    kind:    Option<String>,
    limit:   Option<i64>,
    cursor:  Option<String>,
}

// ── Serializable types ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ArtistResponse {
    artist_id:  String,
    name:       String,
    sort_name:  Option<String>,
    area:       Option<String>,
    img_url:    Option<String>,
    url:        Option<String>,
    begin_year: Option<i64>,
    end_year:   Option<i64>,
    created_at: i64,
    updated_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    aliases:    Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    credits:    Option<Vec<CreditResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags:           Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    relationships:  Option<Vec<RelResponse>>,
}

#[derive(Debug, Serialize)]
struct RelResponse {
    artist_id_a: String,
    artist_id_b: String,
    role:        String,
    begin_year:  Option<i64>,
    end_year:    Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
struct CreditResponse {
    id:           i64,
    display_name: String,
    names:        Vec<CreditNameResponse>,
}

#[derive(Debug, Clone, Serialize)]
struct CreditNameResponse {
    artist_id:   String,
    position:    i64,
    name:        String,
    join_phrase: String,
}

#[derive(Debug, Serialize)]
struct FeedResponse {
    feed_guid:        String,
    feed_url:         String,
    title:            String,
    artist_credit:    CreditResponse,
    description:      Option<String>,
    image_url:        Option<String>,
    language:         Option<String>,
    explicit:         bool,
    episode_count:    i64,
    newest_item_at:   Option<i64>,
    oldest_item_at:   Option<i64>,
    created_at:       i64,
    updated_at:       i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    tracks:           Option<Vec<TrackSummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payment_routes:   Option<Vec<RouteResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags:             Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct TrackSummary {
    track_guid:    String,
    title:         String,
    pub_date:      Option<i64>,
    duration_secs: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RouteResponse {
    recipient_name: Option<String>,
    route_type:     String,
    address:        String,
    custom_key:     Option<String>,
    custom_value:   Option<String>,
    split:          i64,
    fee:            bool,
}

#[derive(Debug, Serialize)]
struct TrackResponse {
    track_guid:      String,
    feed_guid:       String,
    title:           String,
    artist_credit:   CreditResponse,
    pub_date:        Option<i64>,
    duration_secs:   Option<i64>,
    enclosure_url:   Option<String>,
    enclosure_type:  Option<String>,
    enclosure_bytes: Option<i64>,
    track_number:    Option<i64>,
    season:          Option<i64>,
    explicit:        bool,
    description:     Option<String>,
    created_at:      i64,
    updated_at:      i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    payment_routes:     Option<Vec<RouteResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_time_splits:  Option<Vec<VtsResponse>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tags:               Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct VtsResponse {
    start_time_secs:   i64,
    duration_secs:     Option<i64>,
    remote_feed_guid:  String,
    remote_item_guid:  String,
    split:             i64,
}

#[derive(Debug, Serialize)]
struct PeerResponse {
    node_pubkey:  String,
    node_url:     String,
    last_push_at: Option<i64>,
}

/// Intermediate row type for track queries to avoid complex tuple types.
struct TrackRow {
    track_guid:       String,
    feed_guid:        String,
    credit_id:        i64,
    title:            String,
    pub_date:         Option<i64>,
    duration_secs:    Option<i64>,
    enclosure_url:    Option<String>,
    enclosure_type:   Option<String>,
    enclosure_bytes:  Option<i64>,
    track_number:     Option<i64>,
    season:           Option<i64>,
    explicit_int:     i64,
    description:      Option<String>,
    created_at:       i64,
    updated_at:       i64,
}

/// Intermediate row type for feed queries to avoid complex tuple types.
struct FeedRow {
    feed_guid:      String,
    feed_url:       String,
    title:          String,
    credit_id:      i64,
    description:    Option<String>,
    image_url:      Option<String>,
    language:       Option<String>,
    explicit_int:   i64,
    episode_count:  i64,
    newest_item_at: Option<i64>,
    oldest_item_at: Option<i64>,
    created_at:     i64,
    updated_at:     i64,
}

// ── GET /v1/artists/{id} ────────────────────────────────────────────────────

async fn handle_get_artist(
    State(state): State<Arc<api::AppState>>,
    Path(artist_id): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Check for redirect first.
        let resolved_id: String = conn.query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![artist_id],
            |row| row.get(0),
        ).unwrap_or(artist_id);

        let artist = conn.query_row(
            "SELECT artist_id, name, sort_name, area, img_url, url, begin_year, end_year, \
             created_at, updated_at FROM artists WHERE artist_id = ?1",
            params![resolved_id],
            |row| Ok(ArtistResponse {
                artist_id:  row.get(0)?,
                name:       row.get(1)?,
                sort_name:  row.get(2)?,
                area:       row.get(3)?,
                img_url:    row.get(4)?,
                url:        row.get(5)?,
                begin_year: row.get(6)?,
                end_year:   row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
                aliases:        None,
                credits:        None,
                tags:           None,
                relationships:  None,
            }),
        ).map_err(|_err| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "artist not found".into(),
            www_authenticate: None,
        })?;

        let mut artist = artist;

        if params.includes("aliases") {
            let mut stmt = conn.prepare(
                "SELECT alias_lower FROM artist_aliases WHERE artist_id = ?1 ORDER BY alias_lower"
            )?;
            let aliases: Vec<String> = stmt.query_map(params![resolved_id], |row| row.get(0))?
                .collect::<Result<_, _>>()?;
            artist.aliases = Some(aliases);
        }

        if params.includes("credits") {
            let credits = db::get_artist_credits_for_artist(&conn, &resolved_id)?;
            artist.credits = Some(credits.into_iter().map(|c| CreditResponse {
                id: c.id,
                display_name: c.display_name,
                names: c.names.into_iter().map(|n| CreditNameResponse {
                    artist_id:   n.artist_id,
                    position:    n.position,
                    name:        n.name,
                    join_phrase: n.join_phrase,
                }).collect(),
            }).collect());
        }

        if params.includes("tags") {
            artist.tags = Some(load_tags(&conn, "artist", &resolved_id)?);
        }

        if params.includes("relationships") {
            let rels = db::get_artist_rels(&conn, &resolved_id)?;
            artist.relationships = Some(rels.into_iter().map(|r| RelResponse {
                artist_id_a: r.artist_id_a,
                artist_id_b: r.artist_id_b,
                role:        r.rel_type_name,
                begin_year:  r.begin_year,
                end_year:    r.end_year,
            }).collect());
        }

        Ok::<_, api::ApiError>(QueryResponse {
            data:       artist,
            pagination: Pagination { cursor: None, has_more: false },
            meta:       meta(&state2),
        })
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    Ok(Json(result))
}

// ── GET /v1/artists/{id}/feeds ──────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "single paginated feed-list flow with batch credit loading")]
async fn handle_get_artist_feeds(
    State(state): State<Arc<api::AppState>>,
    Path(artist_id): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;
        let limit = params.capped_limit();

        // Resolve redirect.
        let resolved_id: String = conn.query_row(
            "SELECT new_artist_id FROM artist_id_redirect WHERE old_artist_id = ?1",
            params![artist_id],
            |row| row.get(0),
        ).unwrap_or(artist_id);

        // Verify artist exists.
        conn.query_row(
            "SELECT 1 FROM artists WHERE artist_id = ?1",
            params![resolved_id],
            |_| Ok(()),
        ).map_err(|_err| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "artist not found".into(),
            www_authenticate: None,
        })?;

        // Issue-CURSOR-STABILITY — 2026-03-14
        // Cursor encodes (title_lower, feed_guid) for a unique tiebreaker.
        let rows: Vec<FeedRow> = if let Some(ref cursor_str) = params.cursor {
            let decoded = decode_cursor(cursor_str)?;
            let parts: Vec<&str> = decoded.splitn(2, '\0').collect();
            if parts.len() != 2 {
                return Err(api::ApiError {
                    status:  StatusCode::BAD_REQUEST,
                    message: "invalid cursor format".into(),
                    www_authenticate: None,
                });
            }
            let cursor_title = parts[0];
            let cursor_guid  = parts[1];

            let mut stmt = conn.prepare(
                "SELECT f.feed_guid, f.feed_url, f.title, f.artist_credit_id, \
                 f.description, f.image_url, f.language, f.explicit, \
                 f.episode_count, f.newest_item_at, f.oldest_item_at, \
                 f.created_at, f.updated_at \
                 FROM feeds f \
                 JOIN artist_credit_name acn ON acn.artist_credit_id = f.artist_credit_id \
                 WHERE acn.artist_id = ?1 \
                   AND (f.title_lower > ?2 OR (f.title_lower = ?2 AND f.feed_guid > ?3)) \
                 ORDER BY f.title_lower ASC, f.feed_guid ASC \
                 LIMIT ?4",
            )?;
            stmt.query_map(params![resolved_id, cursor_title, cursor_guid, limit + 1], |row| {
                Ok(FeedRow {
                    feed_guid:      row.get(0)?,
                    feed_url:       row.get(1)?,
                    title:          row.get(2)?,
                    credit_id:      row.get(3)?,
                    description:    row.get(4)?,
                    image_url:      row.get(5)?,
                    language:       row.get(6)?,
                    explicit_int:   row.get(7)?,
                    episode_count:  row.get(8)?,
                    newest_item_at: row.get(9)?,
                    oldest_item_at: row.get(10)?,
                    created_at:     row.get(11)?,
                    updated_at:     row.get(12)?,
                })
            })?.collect::<Result<_, _>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT f.feed_guid, f.feed_url, f.title, f.artist_credit_id, \
                 f.description, f.image_url, f.language, f.explicit, \
                 f.episode_count, f.newest_item_at, f.oldest_item_at, \
                 f.created_at, f.updated_at \
                 FROM feeds f \
                 JOIN artist_credit_name acn ON acn.artist_credit_id = f.artist_credit_id \
                 WHERE acn.artist_id = ?1 \
                 ORDER BY f.title_lower ASC, f.feed_guid ASC \
                 LIMIT ?2",
            )?;
            stmt.query_map(params![resolved_id, limit + 1], |row| {
                Ok(FeedRow {
                    feed_guid:      row.get(0)?,
                    feed_url:       row.get(1)?,
                    title:          row.get(2)?,
                    credit_id:      row.get(3)?,
                    description:    row.get(4)?,
                    image_url:      row.get(5)?,
                    language:       row.get(6)?,
                    explicit_int:   row.get(7)?,
                    episode_count:  row.get(8)?,
                    newest_item_at: row.get(9)?,
                    oldest_item_at: row.get(10)?,
                    created_at:     row.get(11)?,
                    updated_at:     row.get(12)?,
                })
            })?.collect::<Result<_, _>>()?
        };

        let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let items: Vec<_> = rows.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)).collect();

        let next_cursor = if has_more {
            items.last().map(|r| {
                encode_cursor(&format!("{}\0{}", r.title.to_lowercase(), r.feed_guid))
            })
        } else {
            None
        };

        // Issue-6 batch credits — 2026-03-13
        // Batch-load all credits in two queries instead of 2*N.
        let credit_map = load_credits_for_feeds(&conn, &items)?;

        let mut feeds = Vec::with_capacity(items.len());
        for r in items {
            let credit = credit_map.get(&r.credit_id)
                .cloned()
                .map_or_else(|| load_credit(&conn, r.credit_id), Ok)?;
            feeds.push(FeedResponse {
                feed_guid:        r.feed_guid,
                feed_url:         r.feed_url,
                title:            r.title,
                artist_credit:    credit,
                description:      r.description,
                image_url:        r.image_url,
                language:         r.language,
                explicit:         r.explicit_int != 0,
                episode_count:    r.episode_count,
                newest_item_at:   r.newest_item_at,
                oldest_item_at:   r.oldest_item_at,
                created_at:       r.created_at,
                updated_at:       r.updated_at,
                tracks:           None,
                payment_routes:   None,
                tags:             None,
            });
        }

        Ok::<_, api::ApiError>(QueryResponse {
            data:       feeds,
            pagination: Pagination { cursor: next_cursor, has_more },
            meta:       meta(&state2),
        })
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    Ok(Json(result))
}

// ── GET /v1/feeds/{guid} ────────────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "single paginated-detail flow with optional includes")]
async fn handle_get_feed(
    State(state): State<Arc<api::AppState>>,
    Path(feed_guid): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        let feed = conn.query_row(
            "SELECT feed_guid, feed_url, title, artist_credit_id, description, image_url, \
             language, explicit, episode_count, newest_item_at, oldest_item_at, \
             created_at, updated_at \
             FROM feeds WHERE feed_guid = ?1",
            params![feed_guid],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<i64>>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
            )),
        ).map_err(|_err| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "feed not found".into(),
            www_authenticate: None,
        })?;

        let credit = load_credit(&conn, feed.3)?;

        let mut resp = FeedResponse {
            feed_guid:      feed.0,
            feed_url:       feed.1,
            title:          feed.2,
            artist_credit:  credit,
            description:    feed.4,
            image_url:      feed.5,
            language:       feed.6,
            explicit:       feed.7 != 0,
            episode_count:  feed.8,
            newest_item_at: feed.9,
            oldest_item_at: feed.10,
            created_at:     feed.11,
            updated_at:     feed.12,
            tracks:         None,
            payment_routes: None,
            tags:           None,
        };

        if params.includes("tracks") {
            let mut stmt = conn.prepare(
                "SELECT track_guid, title, pub_date, duration_secs \
                 FROM tracks WHERE feed_guid = ?1 ORDER BY pub_date DESC",
            )?;
            let tracks: Vec<TrackSummary> = stmt.query_map(params![feed_guid], |row| {
                Ok(TrackSummary {
                    track_guid:    row.get(0)?,
                    title:         row.get(1)?,
                    pub_date:      row.get(2)?,
                    duration_secs: row.get(3)?,
                })
            })?.collect::<Result<_, _>>()?;
            resp.tracks = Some(tracks);
        }

        if params.includes("payment_routes") {
            let mut stmt = conn.prepare(
                "SELECT recipient_name, route_type, address, custom_key, custom_value, split, fee \
                 FROM feed_payment_routes WHERE feed_guid = ?1",
            )?;
            let routes: Vec<RouteResponse> = stmt.query_map(params![feed_guid], |row| {
                Ok(RouteResponse {
                    recipient_name: row.get(0)?,
                    route_type:     row.get(1)?,
                    address:        row.get(2)?,
                    custom_key:     row.get(3)?,
                    custom_value:   row.get(4)?,
                    split:          row.get(5)?,
                    fee:            row.get::<_, i64>(6)? != 0,
                })
            })?.collect::<Result<_, _>>()?;
            resp.payment_routes = Some(routes);
        }

        if params.includes("tags") {
            resp.tags = Some(load_tags(&conn, "feed", &feed_guid)?);
        }

        Ok::<_, api::ApiError>(QueryResponse {
            data:       resp,
            pagination: Pagination { cursor: None, has_more: false },
            meta:       meta(&state2),
        })
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    Ok(Json(result))
}

// ── Shared helpers ──────────────────────────────────────────────────────────

fn load_credit(conn: &rusqlite::Connection, credit_id: i64) -> Result<CreditResponse, api::ApiError> {
    let (id, display_name): (i64, String) = conn.query_row(
        "SELECT id, display_name FROM artist_credit WHERE id = ?1",
        params![credit_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_err| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: "missing artist credit".into(),
        www_authenticate: None,
    })?;

    let mut stmt = conn.prepare(
        "SELECT artist_id, position, name, join_phrase \
         FROM artist_credit_name WHERE artist_credit_id = ?1 ORDER BY position",
    ).map_err(db::DbError::from)?;
    let names: Vec<CreditNameResponse> = stmt.query_map(params![id], |row| {
        Ok(CreditNameResponse {
            artist_id:   row.get(0)?,
            position:    row.get(1)?,
            name:        row.get(2)?,
            join_phrase: row.get(3)?,
        })
    }).map_err(db::DbError::from)?
    .collect::<Result<_, _>>()
    .map_err(db::DbError::from)?;

    Ok(CreditResponse { id, display_name, names })
}

// Issue-6 batch credits — 2026-03-13
/// Converts a model `ArtistCredit` to the query-local `CreditResponse`.
fn credit_from_model(ac: &crate::model::ArtistCredit) -> CreditResponse {
    CreditResponse {
        id:           ac.id,
        display_name: ac.display_name.clone(),
        names:        ac.names.iter().map(|n| CreditNameResponse {
            artist_id:   n.artist_id.clone(),
            position:    n.position,
            name:        n.name.clone(),
            join_phrase: n.join_phrase.clone(),
        }).collect(),
    }
}

// Issue-6 batch credits — 2026-03-13
/// Batch-loads credits for a set of `FeedRow` items, returning a `HashMap`
/// of `credit_id -> CreditResponse` for O(1) lookup. Falls back to the
/// single-load path for any credit IDs missing from the batch result.
fn load_credits_for_feeds(
    conn: &rusqlite::Connection,
    items: &[FeedRow],
) -> Result<HashMap<i64, CreditResponse>, api::ApiError> {
    let credit_ids: Vec<i64> = items.iter().map(|r| r.credit_id).collect();
    let batch = db::load_credits_batch(conn, &credit_ids)?;
    Ok(batch.into_iter().map(|(id, ac)| (id, credit_from_model(&ac))).collect())
}

fn load_tags(conn: &rusqlite::Connection, entity_type: &str, entity_id: &str) -> Result<Vec<String>, api::ApiError> {
    let sql = match entity_type {
        "artist" => "SELECT t.name FROM tags t JOIN artist_tag at ON at.tag_id = t.id WHERE at.artist_id = ?1 ORDER BY t.name",
        "feed"   => "SELECT t.name FROM tags t JOIN feed_tag ft ON ft.tag_id = t.id WHERE ft.feed_guid = ?1 ORDER BY t.name",
        "track"  => "SELECT t.name FROM tags t JOIN track_tag tt ON tt.tag_id = t.id WHERE tt.track_guid = ?1 ORDER BY t.name",
        _ => return Ok(Vec::new()),
    };
    let mut stmt = conn.prepare(sql)?;
    let tags: Vec<String> = stmt.query_map(params![entity_id], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    Ok(tags)
}

// ── GET /v1/tracks/{guid} ────────────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "single paginated-detail flow with optional includes")]
async fn handle_get_track(
    State(state): State<Arc<api::AppState>>,
    Path(track_guid): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        let row = conn.query_row(
            "SELECT track_guid, feed_guid, artist_credit_id, title, pub_date, \
             duration_secs, enclosure_url, enclosure_type, enclosure_bytes, \
             track_number, season, explicit, description, created_at, updated_at \
             FROM tracks WHERE track_guid = ?1",
            params![track_guid],
            |row| Ok(TrackRow {
                track_guid:      row.get(0)?,
                feed_guid:       row.get(1)?,
                credit_id:       row.get(2)?,
                title:           row.get(3)?,
                pub_date:        row.get(4)?,
                duration_secs:   row.get(5)?,
                enclosure_url:   row.get(6)?,
                enclosure_type:  row.get(7)?,
                enclosure_bytes: row.get(8)?,
                track_number:    row.get(9)?,
                season:          row.get(10)?,
                explicit_int:    row.get(11)?,
                description:     row.get(12)?,
                created_at:      row.get(13)?,
                updated_at:      row.get(14)?,
            }),
        ).map_err(|_err| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "track not found".into(),
            www_authenticate: None,
        })?;

        let credit = load_credit(&conn, row.credit_id)?;

        let mut resp = TrackResponse {
            track_guid:      row.track_guid,
            feed_guid:       row.feed_guid,
            title:           row.title,
            artist_credit:   credit,
            pub_date:        row.pub_date,
            duration_secs:   row.duration_secs,
            enclosure_url:   row.enclosure_url,
            enclosure_type:  row.enclosure_type,
            enclosure_bytes: row.enclosure_bytes,
            track_number:    row.track_number,
            season:          row.season,
            explicit:        row.explicit_int != 0,
            description:     row.description,
            created_at:      row.created_at,
            updated_at:      row.updated_at,
            payment_routes:     None,
            value_time_splits:  None,
            tags:               None,
        };

        if params.includes("payment_routes") {
            let mut stmt = conn.prepare(
                "SELECT recipient_name, route_type, address, custom_key, custom_value, split, fee \
                 FROM payment_routes WHERE track_guid = ?1",
            )?;
            let routes: Vec<RouteResponse> = stmt.query_map(params![track_guid], |row| {
                Ok(RouteResponse {
                    recipient_name: row.get(0)?,
                    route_type:     row.get(1)?,
                    address:        row.get(2)?,
                    custom_key:     row.get(3)?,
                    custom_value:   row.get(4)?,
                    split:          row.get(5)?,
                    fee:            row.get::<_, i64>(6)? != 0,
                })
            })?.collect::<Result<_, _>>()?;
            resp.payment_routes = Some(routes);
        }

        if params.includes("value_time_splits") {
            let mut stmt = conn.prepare(
                "SELECT start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split \
                 FROM value_time_splits WHERE source_track_guid = ?1",
            )?;
            let vts: Vec<VtsResponse> = stmt.query_map(params![track_guid], |row| {
                Ok(VtsResponse {
                    start_time_secs:  row.get(0)?,
                    duration_secs:    row.get(1)?,
                    remote_feed_guid: row.get(2)?,
                    remote_item_guid: row.get(3)?,
                    split:            row.get(4)?,
                })
            })?.collect::<Result<_, _>>()?;
            resp.value_time_splits = Some(vts);
        }

        if params.includes("tags") {
            resp.tags = Some(load_tags(&conn, "track", &track_guid)?);
        }

        Ok::<_, api::ApiError>(QueryResponse {
            data:       resp,
            pagination: Pagination { cursor: None, has_more: false },
            meta:       meta(&state2),
        })
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    Ok(Json(result))
}

// ── GET /v1/recent ──────────────────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "single paginated-list flow with two SQL branches")]
async fn handle_get_recent(
    State(state): State<Arc<api::AppState>>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;
        let limit = params.capped_limit();

        let rows: Vec<FeedRow> = if let Some(ref cursor_str) = params.cursor {
            let decoded = decode_cursor(cursor_str)?;
            let parts: Vec<&str> = decoded.splitn(2, '\0').collect();
            if parts.len() != 2 {
                return Err(api::ApiError {
                    status:  StatusCode::BAD_REQUEST,
                    message: "invalid cursor format".into(),
                    www_authenticate: None,
                });
            }
            let cursor_ts: i64 = parts[0].parse().map_err(|_err| api::ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "invalid cursor timestamp".into(),
                www_authenticate: None,
            })?;
            let cursor_guid = parts[1];

            let mut stmt = conn.prepare(
                "SELECT feed_guid, feed_url, title, artist_credit_id, \
                 description, image_url, language, explicit, \
                 episode_count, newest_item_at, oldest_item_at, \
                 created_at, updated_at \
                 FROM feeds \
                 WHERE (newest_item_at, feed_guid) < (?1, ?2) \
                 ORDER BY newest_item_at DESC, feed_guid DESC \
                 LIMIT ?3",
            )?;
            stmt.query_map(params![cursor_ts, cursor_guid, limit + 1], |row| {
                Ok(FeedRow {
                    feed_guid:      row.get(0)?,
                    feed_url:       row.get(1)?,
                    title:          row.get(2)?,
                    credit_id:      row.get(3)?,
                    description:    row.get(4)?,
                    image_url:      row.get(5)?,
                    language:       row.get(6)?,
                    explicit_int:   row.get(7)?,
                    episode_count:  row.get(8)?,
                    newest_item_at: row.get(9)?,
                    oldest_item_at: row.get(10)?,
                    created_at:     row.get(11)?,
                    updated_at:     row.get(12)?,
                })
            })?.collect::<Result<_, _>>()?
        } else {
            let mut stmt = conn.prepare(
                "SELECT feed_guid, feed_url, title, artist_credit_id, \
                 description, image_url, language, explicit, \
                 episode_count, newest_item_at, oldest_item_at, \
                 created_at, updated_at \
                 FROM feeds \
                 ORDER BY newest_item_at DESC, feed_guid DESC \
                 LIMIT ?1",
            )?;
            stmt.query_map(params![limit + 1], |row| {
                Ok(FeedRow {
                    feed_guid:      row.get(0)?,
                    feed_url:       row.get(1)?,
                    title:          row.get(2)?,
                    credit_id:      row.get(3)?,
                    description:    row.get(4)?,
                    image_url:      row.get(5)?,
                    language:       row.get(6)?,
                    explicit_int:   row.get(7)?,
                    episode_count:  row.get(8)?,
                    newest_item_at: row.get(9)?,
                    oldest_item_at: row.get(10)?,
                    created_at:     row.get(11)?,
                    updated_at:     row.get(12)?,
                })
            })?.collect::<Result<_, _>>()?
        };

        let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let items: Vec<_> = rows.into_iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .collect();

        let next_cursor = if has_more {
            items.last().and_then(|r| {
                r.newest_item_at.map(|ts| encode_cursor(&format!("{ts}\0{}", r.feed_guid)))
            })
        } else {
            None
        };

        // Issue-6 batch credits — 2026-03-13
        // Batch-load all credits in two queries instead of 2*N.
        let credit_map = load_credits_for_feeds(&conn, &items)?;

        let mut feeds = Vec::with_capacity(items.len());
        for r in items {
            let credit = credit_map.get(&r.credit_id)
                .cloned()
                .map_or_else(|| load_credit(&conn, r.credit_id), Ok)?;
            feeds.push(FeedResponse {
                feed_guid:        r.feed_guid,
                feed_url:         r.feed_url,
                title:            r.title,
                artist_credit:    credit,
                description:      r.description,
                image_url:        r.image_url,
                language:         r.language,
                explicit:         r.explicit_int != 0,
                episode_count:    r.episode_count,
                newest_item_at:   r.newest_item_at,
                oldest_item_at:   r.oldest_item_at,
                created_at:       r.created_at,
                updated_at:       r.updated_at,
                tracks:           None,
                payment_routes:   None,
                tags:             None,
            });
        }

        Ok::<_, api::ApiError>(QueryResponse {
            data:       feeds,
            pagination: Pagination { cursor: next_cursor, has_more },
            meta:       meta(&state2),
        })
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    Ok(Json(result))
}

// ── GET /v1/search ──────────────────────────────────────────────────────────

// Issue-SEARCH-KEYSET — 2026-03-14
async fn handle_search(
    State(state): State<Arc<api::AppState>>,
    Query(params): Query<SearchQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let q = params.q.clone();
    let kind = params.kind.clone();
    let limit = params.limit.unwrap_or(20).min(100);

    // Issue-SEARCH-KEYSET — 2026-03-14
    // Parse keyset cursor: base64(f64_bits_as_decimal \0 rowid_as_decimal).
    // The f64 rank is encoded via `f64::to_bits()` for a lossless round-trip.
    let (cursor_rank, cursor_rowid) = if let Some(ref cursor_str) = params.cursor {
        let decoded = decode_cursor(cursor_str)?;
        let parts: Vec<&str> = decoded.splitn(2, '\0').collect();
        if parts.len() != 2 {
            return Err(api::ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "invalid cursor format".into(),
                www_authenticate: None,
            });
        }
        let rank_bits: u64 = parts[0].parse().map_err(|_err| api::ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "invalid cursor rank".into(),
            www_authenticate: None,
        })?;
        let rowid: i64 = parts[1].parse().map_err(|_err| api::ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "invalid cursor rowid".into(),
            www_authenticate: None,
        })?;
        let rank = f64::from_bits(rank_bits);
        if rank.is_nan() || rank.is_infinite() {
            return Err(api::ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "invalid cursor rank: non-finite value".into(),
                www_authenticate: None,
            });
        }
        (Some(rank), Some(rowid))
    } else {
        (None, None)
    };

    let db = Arc::clone(&state.db);
    // Mutex safety compliant — 2026-03-12
    let results = tokio::task::spawn_blocking(move || {
        let conn = db.lock().map_err(|_err| db::DbError::Poisoned)?;
        crate::search::search(&conn, &q, kind.as_deref(), limit + 1, cursor_rank, cursor_rowid)
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?
    // Issue-21 FTS5 sanitize — 2026-03-13
    // Catch FTS5 parse errors and return 400 instead of 500.
    .map_err(|e| {
        let msg = e.to_string();
        if msg.contains("fts5: syntax error") || msg.contains("fts5:") {
            api::ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: format!("invalid search query: {msg}"),
                www_authenticate: None,
            }
        } else {
            api::ApiError::from(e)
        }
    })?;

    let has_more = results.len() > usize::try_from(limit).unwrap_or(0);
    let data: Vec<serde_json::Value> = results.iter()
        .take(usize::try_from(limit).unwrap_or(0))
        .map(|r| serde_json::json!({
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "rank": r.rank,
            "quality_score": r.quality_score,
        }))
        .collect();

    // Issue-SEARCH-KEYSET — 2026-03-14
    // Encode keyset cursor from the last result's (effective_rank, rowid).
    let next_cursor = if has_more {
        let limit_usize = usize::try_from(limit).unwrap_or(0);
        results.get(limit_usize.saturating_sub(1)).map(|r| {
            let rank_bits = r.effective_rank.to_bits();
            encode_cursor(&format!("{}\0{}", rank_bits, r.rowid))
        })
    } else {
        None
    };

    Ok(Json(QueryResponse {
        data,
        pagination: Pagination { cursor: next_cursor, has_more },
        meta:       meta(&state),
    }))
}

// ── GET /v1/node/capabilities ───────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct CapabilitiesResponse {
    api_version:    &'static str,
    node_pubkey:    String,
    capabilities:   Vec<&'static str>,
    entity_types:   Vec<&'static str>,
    include_params: HashMap<&'static str, Vec<&'static str>>,
}

async fn handle_capabilities(
    State(state): State<Arc<api::AppState>>,
) -> impl IntoResponse {
    let mut include_params = HashMap::new();
    include_params.insert("artist", vec!["aliases", "credits", "tags", "relationships"]);
    include_params.insert("feed",   vec!["tracks", "payment_routes", "tags"]);
    include_params.insert("track",  vec!["payment_routes", "value_time_splits", "tags"]);

    Json(CapabilitiesResponse {
        api_version:  "v1",
        node_pubkey:  state.node_pubkey_hex.clone(),
        capabilities: vec!["query", "search", "sync", "push"],
        entity_types: vec!["artist", "feed", "track"],
        include_params,
    })
}

// ── GET /v1/peers ───────────────────────────────────────────────────────────

async fn handle_get_peers(
    State(state): State<Arc<api::AppState>>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_err| api::ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;
        let mut stmt = conn.prepare(
            "SELECT node_pubkey, node_url, last_push_at FROM peer_nodes ORDER BY node_pubkey",
        )?;
        let peers: Vec<PeerResponse> = stmt.query_map([], |row| {
            Ok(PeerResponse {
                node_pubkey:  row.get(0)?,
                node_url:     row.get(1)?,
                last_push_at: row.get(2)?,
            })
        })?.collect::<Result<_, _>>()?;
        Ok::<_, api::ApiError>(peers)
    })
    .await
    .map_err(|e| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;
    Ok(Json(result))
}

// ── Router builder ──────────────────────────────────────────────────────────

use axum::routing::get;

pub fn query_routes() -> axum::Router<Arc<api::AppState>> {
    axum::Router::new()
        .route("/v1/artists/{id}",       get(handle_get_artist))
        .route("/v1/artists/{id}/feeds", get(handle_get_artist_feeds))
        .route("/v1/feeds/{guid}",       get(handle_get_feed))
        .route("/v1/tracks/{guid}",      get(handle_get_track))
        .route("/v1/recent",             get(handle_get_recent))
        .route("/v1/search",             get(handle_search))
        .route("/v1/node/capabilities",  get(handle_capabilities))
        .route("/v1/peers",              get(handle_get_peers))
}
