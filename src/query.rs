//! Query API handlers for the `/v1/*` read-only endpoints.
//!
//! All handlers are read-only and run on both primary and community nodes.
//! Pagination uses opaque base64-encoded cursors. Nested data can be requested
//! via the `?include=` query parameter.

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
    let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| api::ApiError {
        status:  StatusCode::BAD_REQUEST,
        message: "invalid cursor".into(),
    })?;
    String::from_utf8(bytes).map_err(|_| api::ApiError {
        status:  StatusCode::BAD_REQUEST,
        message: "invalid cursor encoding".into(),
    })
}

// ── Response envelope ───────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct QueryResponse<T: Serialize> {
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
}

#[derive(Debug, Serialize)]
struct CreditResponse {
    id:           i64,
    display_name: String,
    names:        Vec<CreditNameResponse>,
}

#[derive(Debug, Serialize)]
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
        let conn = state2.db.lock().unwrap();

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
                aliases:    None,
                credits:    None,
            }),
        ).map_err(|_| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "artist not found".into(),
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
    })??;

    Ok(Json(result))
}

// ── GET /v1/artists/{id}/feeds ──────────────────────────────────────────────

async fn handle_get_artist_feeds(
    State(state): State<Arc<api::AppState>>,
    Path(artist_id): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let conn = state2.db.lock().unwrap();
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
        ).map_err(|_| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "artist not found".into(),
        })?;

        let cursor_value = params.cursor.as_deref()
            .map(decode_cursor)
            .transpose()?
            .unwrap_or_default();

        // Fetch limit+1 to detect has_more.
        let mut stmt = conn.prepare(
            "SELECT f.feed_guid, f.feed_url, f.title, f.artist_credit_id, \
             f.description, f.image_url, f.language, f.explicit, \
             f.episode_count, f.newest_item_at, f.oldest_item_at, \
             f.created_at, f.updated_at \
             FROM feeds f \
             JOIN artist_credit_name acn ON acn.artist_credit_id = f.artist_credit_id \
             WHERE acn.artist_id = ?1 AND f.title_lower > ?2 \
             ORDER BY f.title_lower ASC \
             LIMIT ?3",
        )?;

        let rows: Vec<FeedRow> =
            stmt.query_map(params![resolved_id, cursor_value, limit + 1], |row| {
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
            })?.collect::<Result<_, _>>()?;

        let has_more = rows.len() > usize::try_from(limit).unwrap_or(usize::MAX);
        let items: Vec<_> = rows.into_iter().take(usize::try_from(limit).unwrap_or(usize::MAX)).collect();

        let next_cursor = if has_more {
            items.last().map(|r| encode_cursor(&r.title.to_lowercase()))
        } else {
            None
        };

        // Build feed responses with credit info.
        let mut feeds = Vec::with_capacity(items.len());
        for r in items {
            let credit = load_credit(&conn, r.credit_id)?;
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
    })??;

    Ok(Json(result))
}

// ── GET /v1/feeds/{guid} ────────────────────────────────────────────────────

async fn handle_get_feed(
    State(state): State<Arc<api::AppState>>,
    Path(feed_guid): Path<String>,
    Query(params): Query<ListQuery>,
) -> Result<impl IntoResponse, api::ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || {
        let conn = state2.db.lock().unwrap();

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
        ).map_err(|_| api::ApiError {
            status: StatusCode::NOT_FOUND,
            message: "feed not found".into(),
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
    })??;

    Ok(Json(result))
}

// ── Shared helpers ──────────────────────────────────────────────────────────

fn load_credit(conn: &rusqlite::Connection, credit_id: i64) -> Result<CreditResponse, api::ApiError> {
    let (id, display_name): (i64, String) = conn.query_row(
        "SELECT id, display_name FROM artist_credit WHERE id = ?1",
        params![credit_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).map_err(|_| api::ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: "missing artist credit".into(),
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

// ── Router builder ──────────────────────────────────────────────────────────

use axum::routing::get;

pub fn query_routes() -> axum::Router<Arc<api::AppState>> {
    axum::Router::new()
        .route("/v1/artists/{id}",       get(handle_get_artist))
        .route("/v1/artists/{id}/feeds", get(handle_get_artist_feeds))
        .route("/v1/feeds/{guid}",       get(handle_get_feed))
}
