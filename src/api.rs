// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Axum HTTP router, handlers, and shared application state.
//!
//! Exposes three routes:
//! - `POST /ingest/feed` — crawler submission endpoint; validates via
//!   [`verify::VerifierChain`] and writes atomically via [`db::ingest_transaction`].
//! - `GET /sync/events` — paginated event log for community nodes.
//! - `POST /sync/reconcile` — negentropy-style diff for nodes rejoining after downtime.
//!
//! All blocking database operations are run in [`tokio::task::spawn_blocking`]
//! to avoid stalling the async executor. Join errors are converted to
//! [`ApiError`] with HTTP 500 rather than panicking.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;

use crate::{db, event, ingest, model, signing, sync, verify};

// ── AppState ────────────────────────────────────────────────────────────────

/// Shared application state injected into every Axum handler.
pub struct AppState {
    /// `SQLite` database handle (mutex-wrapped for blocking-task access).
    pub db:              db::Db,
    /// Ordered chain of verifiers that must all pass before an ingest is accepted.
    pub chain:           Arc<verify::VerifierChain>,
    /// Signs event payloads with this node's ed25519 key.
    pub signer:          Arc<signing::NodeSigner>,
    /// Hex-encoded ed25519 public key identifying this node in the network.
    pub node_pubkey_hex: String,
}

// ── ApiError ─────────────────────────────────────────────────────────────────

/// HTTP error response returned by all handlers; serializes to `{"error":"..."}`.
pub struct ApiError {
    /// HTTP status code sent to the client.
    pub status:  StatusCode,
    /// Human-readable error message included in the JSON body.
    pub message: String,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(ErrorBody { error: self.message });
        (self.status, body).into_response()
    }
}

impl From<db::DbError> for ApiError {
    fn from(e: db::DbError) -> Self {
        let message = match e {
            db::DbError::Rusqlite(inner) => format!("database error: {inner}"),
            db::DbError::Json(inner)     => format!("json error: {inner}"),
        };
        ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

/// Builds the full read-write router used by the primary node.
///
/// Routes exposed:
/// - `POST /ingest/feed` — crawler submission; validates via [`verify::VerifierChain`].
/// - `GET  /sync/events` — paginated event log for community nodes.
/// - `POST /sync/reconcile` — negentropy-style diff for nodes rejoining after downtime.
/// - `GET  /health` — liveness probe.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ingest/feed",    post(handle_ingest_feed))
        .route("/sync/events",    get(handle_sync_events))
        .route("/sync/reconcile", post(handle_sync_reconcile))
        .route("/health",         get(|| async { "ok" }))
        .with_state(state)
}

/// Read-only router for community nodes.
///
/// Exposes only `GET /sync/events` and `GET /health`; ingest and reconcile
/// write-paths are intentionally absent so a community node can never be used
/// as an ingestion endpoint.
pub fn build_readonly_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/sync/events", get(handle_sync_events))
        .route("/health",      get(|| async { "ok" }))
        .with_state(state)
}

// ── POST /ingest/feed ─────────────────────────────────────────────────────────

// Flow: verify crawl_token → run VerifierChain → resolve artist → build event rows
// → spawn_blocking DB transaction → return IngestResponse.
#[expect(clippy::too_many_lines, reason = "single ingest flow — splitting would obscure the sequential validation steps")]
async fn handle_ingest_feed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ingest::IngestFeedRequest>,
) -> Result<Json<ingest::IngestResponse>, ApiError> {
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || -> Result<ingest::IngestResponse, ApiError> {
        let mut conn = state2.db.lock().unwrap();

        // 1. Get existing feed
        let existing = db::get_existing_feed(&conn, &req.canonical_url)?;

        // 2. Build verify context and run chain
        let ctx = verify::IngestContext {
            request:  &req,
            db:       &conn,
            existing: existing.as_ref(),
        };

        // Sentinel pattern: ContentHashVerifier returns Err(NO_CHANGE_SENTINEL)
        // to signal "identical content, skip ingest" without it being a true
        // error. We special-case it here before treating other Err values as
        // real rejections. The sentinel is a public const so both sides of this
        // contract can refer to the same value without magic strings.
        let warnings = match state2.chain.run(&ctx) {
            Err(reason) if reason == verify::NO_CHANGE_SENTINEL => {
                return Ok(ingest::IngestResponse {
                    accepted:       true,
                    no_change:      true,
                    reason:         None,
                    events_emitted: vec![],
                    warnings:       vec![],
                });
            }
            Err(reason) => {
                return Ok(ingest::IngestResponse {
                    accepted:       false,
                    no_change:      false,
                    reason:         Some(reason),
                    events_emitted: vec![],
                    warnings:       vec![],
                });
            }
            Ok(w) => w,
        };

        // 3. Unwrap feed_data — must exist after passing verifiers
        let feed_data = req.feed_data.as_ref().ok_or_else(|| ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "feed_data is required for successful ingest".into(),
        })?;

        // 4. Resolve artist
        let artist_name = feed_data
            .owner_name
            .as_deref()
            .or(feed_data.author_name.as_deref())
            .unwrap_or(feed_data.title.as_str())
            .to_string();

        let feed_artist = db::resolve_artist(&conn, &artist_name)?;

        // 5. Get current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .cast_signed();

        // 6. Compute newest_item_at and oldest_item_at from track pub_dates
        let pub_dates: Vec<i64> = feed_data
            .tracks
            .iter()
            .filter_map(|t| t.pub_date)
            .collect();

        let newest_item_at = pub_dates.iter().copied().max();
        let oldest_item_at = pub_dates.iter().copied().min();

        // 7. Build Feed struct
        let feed = model::Feed {
            feed_guid:      feed_data.feed_guid.clone(),
            feed_url:       req.canonical_url.clone(),
            title:          feed_data.title.clone(),
            title_lower:    feed_data.title.to_lowercase(),
            artist_id:      feed_artist.artist_id.clone(),
            description:    feed_data.description.clone(),
            image_url:      feed_data.image_url.clone(),
            language:       feed_data.language.clone(),
            explicit:       feed_data.explicit,
            itunes_type:    feed_data.itunes_type.clone(),
            #[expect(clippy::cast_possible_wrap, reason = "episode counts never approach i64::MAX")]
            episode_count:  feed_data.tracks.len() as i64,
            newest_item_at,
            oldest_item_at,
            created_at:     now,
            updated_at:     now,
            raw_medium:     feed_data.raw_medium.clone(),
        };

        // 8. Build track tuples
        let mut track_tuples: Vec<(
            model::Track,
            Vec<model::PaymentRoute>,
            Vec<model::ValueTimeSplit>,
        )> = Vec::with_capacity(feed_data.tracks.len());

        for track_data in &feed_data.tracks {
            // Per-track artist resolution
            let track_artist_id = if let Some(author) = &track_data.author_name {
                let track_artist = db::resolve_artist(&conn, author)?;
                track_artist.artist_id
            } else {
                feed_artist.artist_id.clone()
            };

            let track = model::Track {
                track_guid:      track_data.track_guid.clone(),
                feed_guid:       feed_data.feed_guid.clone(),
                artist_id:       track_artist_id,
                title:           track_data.title.clone(),
                title_lower:     track_data.title.to_lowercase(),
                pub_date:        track_data.pub_date,
                duration_secs:   track_data.duration_secs,
                enclosure_url:   track_data.enclosure_url.clone(),
                enclosure_type:  track_data.enclosure_type.clone(),
                enclosure_bytes: track_data.enclosure_bytes,
                track_number:    track_data.track_number,
                season:          track_data.season,
                explicit:        track_data.explicit,
                description:     track_data.description.clone(),
                created_at:      now,
                updated_at:      now,
            };

            let routes: Vec<model::PaymentRoute> = track_data
                .payment_routes
                .iter()
                .map(|r| model::PaymentRoute {
                    id:             None,
                    track_guid:     track_data.track_guid.clone(),
                    feed_guid:      feed_data.feed_guid.clone(),
                    recipient_name: r.recipient_name.clone(),
                    route_type:     r.route_type.clone(),
                    address:        r.address.clone(),
                    custom_key:     r.custom_key.clone(),
                    custom_value:   r.custom_value.clone(),
                    split:          r.split,
                    fee:            r.fee,
                })
                .collect();

            let vts: Vec<model::ValueTimeSplit> = track_data
                .value_time_splits
                .iter()
                .map(|v| model::ValueTimeSplit {
                    id:                None,
                    source_track_guid: track_data.track_guid.clone(),
                    start_time_secs:   v.start_time_secs,
                    duration_secs:     v.duration_secs,
                    remote_feed_guid:  v.remote_feed_guid.clone(),
                    remote_item_guid:  v.remote_item_guid.clone(),
                    split:             v.split,
                    created_at:        now,
                })
                .collect();

            track_tuples.push((track, routes, vts));
        }

        // 9. Build event rows
        let mut event_rows: Vec<db::EventRow> = Vec::new();

        // ArtistUpserted
        {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::ArtistUpsertedPayload {
                artist: feed_artist.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize ArtistUpserted payload: {e}"),
            })?;
            let (signed_by, signature) = state2.signer.sign_event(
                &event_id,
                &event::EventType::ArtistUpserted,
                &payload_json,
                &feed_artist.artist_id,
                now,
            );
            event_rows.push(db::EventRow {
                event_id,
                event_type:   event::EventType::ArtistUpserted,
                payload_json,
                subject_guid: feed_artist.artist_id.clone(),
                signed_by,
                signature,
                created_at:   now,
                warnings:     warnings.clone(),
            });
        }

        // FeedUpserted
        {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::FeedUpsertedPayload {
                feed:   feed.clone(),
                artist: feed_artist.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize FeedUpserted payload: {e}"),
            })?;
            let (signed_by, signature) = state2.signer.sign_event(
                &event_id,
                &event::EventType::FeedUpserted,
                &payload_json,
                &feed.feed_guid,
                now,
            );
            event_rows.push(db::EventRow {
                event_id,
                event_type:   event::EventType::FeedUpserted,
                payload_json,
                subject_guid: feed.feed_guid.clone(),
                signed_by,
                signature,
                created_at:   now,
                warnings:     warnings.clone(),
            });
        }

        // TrackUpserted — one per track
        for (track, routes, vts) in &track_tuples {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::TrackUpsertedPayload {
                track:             track.clone(),
                routes:            routes.clone(),
                value_time_splits: vts.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize TrackUpserted payload: {e}"),
            })?;
            let (signed_by, signature) = state2.signer.sign_event(
                &event_id,
                &event::EventType::TrackUpserted,
                &payload_json,
                &track.track_guid,
                now,
            );
            event_rows.push(db::EventRow {
                event_id,
                event_type:   event::EventType::TrackUpserted,
                payload_json,
                subject_guid: track.track_guid.clone(),
                signed_by,
                signature,
                created_at:   now,
                warnings:     warnings.clone(),
            });
        }

        // Collect event_ids before moving event_rows into ingest_transaction
        let event_ids: Vec<String> = event_rows.iter().map(|r| r.event_id.clone()).collect();

        // 10. Run ingest transaction
        db::ingest_transaction(&mut conn, feed_artist, feed.clone(), track_tuples, event_rows)?;

        // 11. Update crawl cache
        db::upsert_feed_crawl_cache(&conn, &req.canonical_url, &req.content_hash, now)?;

        Ok(ingest::IngestResponse {
            accepted:       true,
            no_change:      false,
            reason:         None,
            events_emitted: event_ids,
            warnings,
        })
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
    })?;

    result.map(Json)
}

// ── GET /sync/events ──────────────────────────────────────────────────────────

// Query parameters for GET /sync/events; `after_seq` defaults to 0 (fetch from start).
#[derive(serde::Deserialize)]
struct SyncEventsQuery {
    #[serde(default)]
    after_seq: i64,
    limit:     Option<i64>,
}

// Flow: parse query params → cap limit at 1000 → spawn_blocking DB read
// → return SyncEventsResponse with pagination cursor.
async fn handle_sync_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SyncEventsQuery>,
) -> Result<Json<sync::SyncEventsResponse>, ApiError> {
    let after_seq = params.after_seq;
    let capped_limit = params.limit.unwrap_or(500).min(1000);

    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || -> Result<sync::SyncEventsResponse, ApiError> {
        let conn = state2.db.lock().unwrap();
        let events = db::get_events_since(&conn, after_seq, capped_limit)?;

        let has_more = events.len() == usize::try_from(capped_limit).unwrap_or(usize::MAX);
        let next_seq = events.last().map_or(after_seq, |e| e.seq);

        Ok(sync::SyncEventsResponse {
            events,
            has_more,
            next_seq,
        })
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
    })?;

    result.map(Json)
}

// ── POST /sync/reconcile ──────────────────────────────────────────────────────

// Flow: compute set-difference between node's `have` list and our refs →
// return events the node is missing; surface any IDs unknown to us as anomalies.
async fn handle_sync_reconcile(
    State(state): State<Arc<AppState>>,
    Json(req): Json<sync::ReconcileRequest>,
) -> Result<Json<sync::ReconcileResponse>, ApiError> {
    let state2 = Arc::clone(&state);
    let result =
        tokio::task::spawn_blocking(move || -> Result<sync::ReconcileResponse, ApiError> {
            let conn = state2.db.lock().unwrap();

            // Get our event refs since the requested seq
            let our_refs = db::get_event_refs_since(&conn, req.since_seq)?;

            // Build ID sets
            let our_ids: HashSet<String> =
                our_refs.iter().map(|r| r.event_id.clone()).collect();
            let their_ids: HashSet<String> =
                req.have.iter().map(|r| r.event_id.clone()).collect();

            // missing_ids = what we have that they don't
            let missing_ids: HashSet<&String> = our_ids.difference(&their_ids).collect();

            // unknown = what they have that we don't
            let unknown_to_us: Vec<sync::EventRef> = req
                .have
                .into_iter()
                .filter(|r| !our_ids.contains(&r.event_id))
                .collect();

            // Fetch full events for missing_ids
            let all_events = db::get_events_since(&conn, req.since_seq, 10_000)?;
            let send_to_node: Vec<crate::event::Event> = all_events
                .into_iter()
                .filter(|e| missing_ids.contains(&e.event_id))
                .collect();

            // Record that we saw this node
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .cast_signed();

            let last_seq = our_refs.iter().map(|r| r.seq).max().unwrap_or(req.since_seq);

            db::upsert_node_sync_state(&conn, &req.node_pubkey, last_seq, now)?;

            Ok(sync::ReconcileResponse {
                send_to_node,
                unknown_to_us,
            })
        })
        .await
        .map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("internal task panic: {e}"),
        })?;

    result.map(Json)
}
