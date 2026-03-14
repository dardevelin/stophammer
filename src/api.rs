#![expect(clippy::significant_drop_tightening, reason = "MutexGuard<Connection> must be held for the full spawn_blocking scope")]

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

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
    Json, Router,
};
use tower_http::cors::{Any, CorsLayer};
use governor::{Quota, RateLimiter, clock::DefaultClock, state::keyed::DefaultKeyedStateStore};
use rusqlite::params;
use serde::{Deserialize, Serialize};

use sha2::{Sha256, Digest};
use subtle::ConstantTimeEq;

use crate::{db, event, ingest, model, proof, query, signing, sync, verify};

// ── FG-02 SSE artist follow — 2026-03-13 ─────────────────────────────────

/// Broadcast channel capacity per artist.
const SSE_CHANNEL_CAPACITY: usize = 256;

/// Maximum number of recent events kept per artist for Last-Event-ID replay.
const SSE_RING_BUFFER_SIZE: usize = 100;

/// Maximum number of unique artist entries in the SSE registry.
/// Prevents unbounded memory growth from attackers creating channels for
/// fabricated artist IDs. 10,000 is generous for any legitimate deployment.
const MAX_SSE_REGISTRY_ARTISTS: usize = 10_000;

/// Maximum number of concurrent SSE connections across the server.
/// Each SSE connection holds a long-lived tokio task polling at 100ms intervals.
/// Without a cap, an attacker can exhaust server resources with persistent connections.
const MAX_SSE_CONNECTIONS: usize = 1_000;

/// CORS preflight cache duration in seconds (1 hour).
///
/// Browsers cache the `Access-Control-Max-Age` header for this long before
/// re-issuing an OPTIONS preflight. One hour reduces preflight traffic while
/// keeping clients reasonably up-to-date with any policy changes.
const CORS_MAX_AGE_SECS: u64 = 3600;

/// Access token lifetime in seconds (1 hour) for proof-of-possession tokens.
///
/// Must match [`crate::proof::TOKEN_TTL_SECS`] so the `expires_at` returned
/// in the assertion response reflects the actual token expiry.
const PROOF_TOKEN_TTL_SECS: i64 = 3600;

/// A single SSE frame delivered to subscribers following an artist.
#[derive(Clone, Debug, Serialize)]
pub struct SseFrame {
    /// Event type, e.g. `"track_upserted"`, `"feed_upserted"`.
    pub event_type: String,
    /// The subject entity GUID (`track_guid`, `feed_guid`, etc.).
    pub subject_guid: String,
    /// Full event payload as JSON.
    pub payload: serde_json::Value,
    /// Monotonically increasing sequence number (primary key in `events` table).
    /// Used as the SSE `id:` field for unambiguous `Last-Event-ID` replay.
    pub seq: i64,
}

/// Registry managing per-artist broadcast channels and ring buffers for SSE.
// CRIT-03 Debug — 2026-03-13
pub struct SseRegistry {
    /// `artist_id` -> broadcast sender for that artist's events.
    senders: std::sync::RwLock<HashMap<String, tokio::sync::broadcast::Sender<SseFrame>>>,
    /// `artist_id` -> ring buffer of recent events for `Last-Event-ID` replay.
    ring_buffers: std::sync::RwLock<HashMap<String, std::collections::VecDeque<SseFrame>>>,
    /// Current number of active SSE connections (for connection cap enforcement).
    active_connections: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for SseRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseRegistry")
            .field("artist_count", &self.artist_count())
            .field("active_connections", &self.active_connections.load(std::sync::atomic::Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SseRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            senders: std::sync::RwLock::new(HashMap::new()),
            ring_buffers: std::sync::RwLock::new(HashMap::new()),
            active_connections: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Returns the current number of active SSE connections.
    #[must_use]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Attempts to acquire an SSE connection slot. Returns `true` if the
    /// connection is allowed, `false` if the maximum has been reached.
    // Issue #23 atomic TOCTOU fix — 2026-03-13
    pub fn try_acquire_connection(&self) -> bool {
        self.active_connections
            .fetch_update(std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire, |current| {
                (current < MAX_SSE_CONNECTIONS).then(|| current + 1)
            })
            .is_ok()
    }

    /// Releases an SSE connection slot when a client disconnects.
    pub fn release_connection(&self) {
        self.active_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the number of unique artist entries in the registry.
    #[must_use]
    pub fn artist_count(&self) -> usize {
        self.senders.read()
            .map(|g| g.len())
            .unwrap_or(0)
    }

    /// Returns a broadcast receiver for the given artist. Creates the channel
    /// lazily if it does not yet exist. Returns `None` if the registry is full
    /// and the artist is not already tracked.
    pub fn subscribe(&self, artist_id: &str) -> Option<tokio::sync::broadcast::Receiver<SseFrame>> {
        // Try read-lock first (fast path for existing channels).
        {
            if let Ok(guard) = self.senders.read()
                && let Some(tx) = guard.get(artist_id)
            {
                return Some(tx.subscribe());
            }
        }
        // Slow path: create channel under write lock.
        let mut guard = self.senders.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        // Check if another thread created it while we waited for the write lock.
        if let Some(tx) = guard.get(artist_id) {
            return Some(tx.subscribe());
        }
        // Enforce registry size limit before creating a new entry.
        if guard.len() >= MAX_SSE_REGISTRY_ARTISTS {
            return None;
        }
        let (tx, _) = tokio::sync::broadcast::channel(SSE_CHANNEL_CAPACITY);
        let rx = tx.subscribe();
        guard.insert(artist_id.to_string(), tx);
        Some(rx)
    }

    /// Publishes a frame to the broadcast channel for `artist_id` and appends
    /// it to the ring buffer.
    pub fn publish(&self, artist_id: &str, frame: SseFrame) {
        // Try read-lock first (fast path for existing channels).
        let sent = {
            if let Ok(guard) = self.senders.read()
                && let Some(tx) = guard.get(artist_id)
            {
                let _ = tx.send(frame.clone());
                true
            } else {
                false
            }
        };

        // Slow path: create channel if it did not exist and send.
        // Publish always creates channels (these are real events from ingest),
        // but is also bounded by MAX_SSE_REGISTRY_ARTISTS.
        if !sent {
            let mut guard = self.senders.write().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(tx) = guard.get(artist_id) {
                let _ = tx.send(frame.clone());
            } else if guard.len() < MAX_SSE_REGISTRY_ARTISTS {
                let (tx, _) = tokio::sync::broadcast::channel(SSE_CHANNEL_CAPACITY);
                let _ = tx.send(frame.clone());
                guard.insert(artist_id.to_string(), tx);
            } else {
                tracing::warn!(
                    artist_id,
                    "SSE registry full ({MAX_SSE_REGISTRY_ARTISTS} artists); dropping event"
                );
                return;
            }
        }

        // Append to ring buffer.
        let mut rb_guard = self.ring_buffers.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        // Also enforce the same limit on ring buffers.
        if !rb_guard.contains_key(artist_id) && rb_guard.len() >= MAX_SSE_REGISTRY_ARTISTS {
            return;
        }
        let buf = rb_guard
            .entry(artist_id.to_string())
            .or_insert_with(|| std::collections::VecDeque::with_capacity(SSE_RING_BUFFER_SIZE));
        if buf.len() >= SSE_RING_BUFFER_SIZE {
            buf.pop_front();
        }
        buf.push_back(frame);
    }

    /// Returns cloned recent events for replay (bounded by `SSE_RING_BUFFER_SIZE`).
    #[must_use]
    pub fn recent_events(&self, artist_id: &str) -> Vec<SseFrame> {
        let guard = self.ring_buffers.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .get(artist_id)
            .map(|buf| buf.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl Default for SseRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Issue-SSE-PUBLISH helpers — 2026-03-14 ────────────────────────────────

/// Extracts the artist ID(s) relevant to an event for SSE channel routing.
///
/// Returns an empty vec for event types that do not map to a specific artist
/// (e.g. `FeedRetired`, `TrackRemoved`) since those payloads only carry GUIDs
/// and the entity may already be deleted. The caller should fall back to the
/// `subject_guid` if needed, but in practice these events are less relevant
/// to live SSE followers.
fn extract_artist_ids(ev: &event::Event) -> Vec<String> {
    match &ev.payload {
        event::EventPayload::ArtistUpserted(p) => {
            vec![p.artist.artist_id.clone()]
        }
        event::EventPayload::FeedUpserted(p) => {
            vec![p.artist.artist_id.clone()]
        }
        event::EventPayload::TrackUpserted(p) => {
            p.artist_credit
                .names
                .iter()
                .map(|n| n.artist_id.clone())
                .collect()
        }
        event::EventPayload::ArtistCreditCreated(p) => {
            p.artist_credit
                .names
                .iter()
                .map(|n| n.artist_id.clone())
                .collect()
        }
        event::EventPayload::ArtistMerged(p) => {
            vec![p.target_artist_id.clone()]
        }
        // FeedRetired, TrackRemoved, RoutesReplaced, FeedRoutesReplaced:
        // These payloads do not embed artist info. We skip SSE publish for
        // these rather than doing a DB lookup that may fail (entity deleted).
        _ => vec![],
    }
}

/// Fire-and-forget SSE publish for a batch of events.
///
/// For each event, extracts the relevant artist ID(s) and publishes an
/// `SseFrame` to each artist's broadcast channel. Errors are logged but
/// never propagated — SSE is best-effort and must not fail the mutation.
// Issue-SSE-PUBLISH — 2026-03-14
pub fn publish_events_to_sse(registry: &SseRegistry, events: &[event::Event]) {
    for ev in events {
        let artist_ids = extract_artist_ids(ev);
        if artist_ids.is_empty() {
            continue;
        }
        let frame = SseFrame {
            event_type:   serde_json::to_string(&ev.event_type).unwrap_or_default().trim_matches('"').to_string(),
            subject_guid: ev.subject_guid.clone(),
            payload:      serde_json::to_value(&ev.payload).unwrap_or(serde_json::Value::Null),
            seq:          ev.seq,
        };
        for artist_id in &artist_ids {
            registry.publish(artist_id, frame.clone());
        }
    }
}

// ── SP-03 rate limiting — 2026-03-13 ─────────────────────────────────────

/// Reads `RATE_LIMIT_RPS` and `RATE_LIMIT_BURST` from the environment, falling
/// back to 50 / 100 respectively.
#[must_use]
pub fn rate_limit_config() -> (u32, u32) {
    let rps: u32 = std::env::var("RATE_LIMIT_RPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let burst: u32 = std::env::var("RATE_LIMIT_BURST")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    (rps, burst)
}

/// Per-IP token-bucket rate limiter keyed by `String` (IP address).
pub type IpRateLimiter = RateLimiter<
    String,
    DefaultKeyedStateStore<String>,
    DefaultClock,
>;

/// Builds a keyed (per-IP) governor rate limiter with the given `rps` and `burst`.
///
/// The caller owns the limiter and should apply it in the serving layer
/// (e.g. main.rs) rather than inside `build_router`, so that `tower::ServiceExt::oneshot`
/// tests are not affected.
///
/// # Panics
///
/// Panics if internal `NonZeroU32` fallback constants are zero (impossible in
/// practice — the fallbacks are hard-coded to 50 and 100).
#[must_use]
pub fn build_rate_limiter(rps: u32, burst: u32) -> IpRateLimiter {
    let quota = Quota::per_second(
        NonZeroU32::new(rps).unwrap_or(NonZeroU32::new(50).expect("50 is nonzero")),
    )
    .allow_burst(
        NonZeroU32::new(burst).unwrap_or(NonZeroU32::new(100).expect("100 is nonzero")),
    );
    RateLimiter::keyed(quota)
}

// ── Availability limits ────────────────────────────────────────────────────

/// Maximum request body size (bytes). Applied globally via `DefaultBodyLimit`.
/// 2 MiB is sufficient for the largest legitimate ingest payload (a feed with
/// ~200 tracks and payment routes). Larger payloads are rejected with 413.
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Maximum number of tracks allowed in a single `POST /ingest/feed` request.
/// Prevents a single malicious submission from creating thousands of DB rows.
const MAX_TRACKS_PER_INGEST: usize = 500;

/// Maximum number of `have` event refs in a `POST /sync/reconcile` request.
const MAX_RECONCILE_HAVE: usize = 10_000;

// Finding-5 reconcile pagination — 2026-03-13
/// Maximum number of event refs loaded by `get_event_refs_since` during reconcile.
/// Prevents unbounded memory usage when a node is far behind.
const MAX_RECONCILE_REFS: i64 = 50_000;

/// Maximum number of full events returned in a single reconcile response.
const MAX_RECONCILE_EVENTS: i64 = 10_000;

/// Maximum number of pending proof challenges allowed per `feed_guid`.
/// Prevents an attacker from flooding the `proof_challenges` table.
const MAX_PENDING_CHALLENGES_PER_FEED: i64 = 20;

/// Maximum length (bytes) for the `requester_nonce` field in proof challenge requests.
const MAX_NONCE_BYTES: usize = 256;

// ── AppState ────────────────────────────────────────────────────────────────

/// Shared application state injected into every Axum handler.
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct AppState {
    /// `SQLite` database handle (mutex-wrapped for blocking-task access).
    pub db:              db::Db,
    /// Ordered chain of verifiers that must all pass before an ingest is accepted.
    pub chain:           Arc<verify::VerifierChain>,
    /// Signs event payloads with this node's ed25519 key.
    pub signer:          Arc<signing::NodeSigner>,
    /// Hex-encoded ed25519 public key identifying this node in the network.
    pub node_pubkey_hex: String,
    /// Token required in `X-Admin-Token` for admin endpoints.
    pub admin_token:     String,
    // Finding-3 separate sync token — 2026-03-13
    /// Optional dedicated token for `POST /sync/register` (`X-Sync-Token` header).
    /// When `Some`, only this token is accepted for sync registration.
    /// When `None`, falls back to `admin_token` for backward compatibility.
    pub sync_token:      Option<String>,
    /// HTTP client used for push fan-out to peer community nodes.
    pub push_client:     reqwest::Client,
    /// In-memory cache of active push peers: pubkey → push URL.
    pub push_subscribers: Arc<RwLock<HashMap<String, String>>>,
    /// FG-02 SSE artist follow — 2026-03-13
    /// Registry for SSE per-artist broadcast channels and replay buffers.
    pub sse_registry: Arc<SseRegistry>,
    /// When true, skip SSRF validation of feed URLs during proof assertion.
    /// Only intended for test environments where mock servers use localhost.
    // CRIT-02 feature-gate — 2026-03-13
    #[cfg(feature = "test-util")]
    pub skip_ssrf_validation: bool,
}

// ── ApiError ─────────────────────────────────────────────────────────────────

/// HTTP error response returned by all handlers; serializes to `{"error":"..."}`.
// RFC 6750 compliant — 2026-03-12
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct ApiError {
    /// HTTP status code sent to the client.
    pub status:  StatusCode,
    /// Human-readable error message included in the JSON body.
    pub message: String,
    /// Optional `WWW-Authenticate` header value for 401/403 responses (RFC 6750 section 3).
    pub www_authenticate: Option<HeaderValue>,
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for ApiError {
    // RFC 6750 compliant — 2026-03-12
    fn into_response(self) -> Response {
        let body = Json(ErrorBody { error: self.message });
        if let Some(challenge) = self.www_authenticate {
            let mut headers = HeaderMap::new();
            headers.insert("WWW-Authenticate", challenge);
            (self.status, headers, body).into_response()
        } else {
            (self.status, body).into_response()
        }
    }
}

// Mutex safety compliant — 2026-03-12
impl From<db::DbError> for ApiError {
    fn from(e: db::DbError) -> Self {
        let message = match e {
            db::DbError::Rusqlite(inner) => format!("database error: {inner}"),
            db::DbError::Json(inner)     => format!("json error: {inner}"),
            db::DbError::Poisoned        => "database mutex poisoned".to_string(),
        };
        Self {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message,
            www_authenticate: None,
        }
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(e: rusqlite::Error) -> Self {
        Self::from(db::DbError::from(e))
    }
}

// ── spawn_db helpers ─────────────────────────────────────────────────────────

/// Runs a blocking closure with a shared (`&Connection`) database reference on
/// a `spawn_blocking` task. Handles mutex poisoning and join errors, converting
/// them to `ApiError` with HTTP 500.
///
/// # Errors
///
/// Returns `ApiError` (HTTP 500) if the database mutex is poisoned, the
/// blocking task panics, or the closure returns a `DbError`.
// Mutex safety compliant — 2026-03-12
pub async fn spawn_db<F, T>(db: db::Db, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&rusqlite::Connection) -> Result<T, db::DbError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let conn = db.lock().map_err(|_poison| db::DbError::Poisoned)?;
        f(&conn)
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?
    .map_err(ApiError::from)
}

/// Runs a blocking closure with an exclusive (`&mut Connection`) database
/// reference on a `spawn_blocking` task.
///
/// Required for handlers that use transactions or savepoints
/// (e.g. `delete_feed`, `delete_track`, `merge_artists`).
///
/// # Errors
///
/// Returns `ApiError` (HTTP 500) if the database mutex is poisoned, the
/// blocking task panics, or the closure returns a `DbError`.
// Mutex safety compliant — 2026-03-12
pub async fn spawn_db_mut<F, T>(db: db::Db, f: F) -> Result<T, ApiError>
where
    F: FnOnce(&mut rusqlite::Connection) -> Result<T, db::DbError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut conn = db.lock().map_err(|_poison| db::DbError::Poisoned)?;
        f(&mut conn)
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?
    .map_err(ApiError::from)
}

// ── SP-08 CORS — 2026-03-13 ──────────────────────────────────────────────────

/// Builds the CORS middleware layer used by both primary and readonly routers.
// Issue #18 configurable CORS origin — 2026-03-13
fn build_cors_layer() -> CorsLayer {
    let cors = CorsLayer::new();

    let cors = match std::env::var("CORS_ALLOW_ORIGIN") {
        Ok(origin) if !origin.is_empty() => {
            let header_value: HeaderValue = origin.parse()
                .expect("CORS_ALLOW_ORIGIN must be a valid header value");
            cors.allow_origin(header_value)
        }
        _ => cors.allow_origin(Any),
    };

    cors.allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            axum::http::HeaderName::from_static("x-admin-token"),
            // Finding-3 separate sync token — 2026-03-13
            axum::http::HeaderName::from_static("x-sync-token"),
        ])
        .max_age(Duration::from_secs(CORS_MAX_AGE_SECS))
}

// ── Router ────────────────────────────────────────────────────────────────────

/// Builds the full read-write router used by the primary node.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ingest/feed",         post(handle_ingest_feed))
        .route("/sync/events",         get(handle_sync_events))
        .route("/sync/reconcile",      post(handle_sync_reconcile))
        .route("/sync/register",       post(handle_sync_register))
        .route("/sync/peers",          get(handle_sync_peers))
        .route("/node/info",           get(handle_node_info))
        .route("/admin/artists/merge", post(handle_admin_merge_artists))
        .route("/admin/artists/alias", post(handle_admin_add_alias))
        // Route versioning compliant — 2026-03-12
        .route("/v1/feeds/{guid}",        delete(handle_retire_feed).patch(handle_patch_feed))
        .route("/v1/feeds/{guid}/tracks/{track_guid}", delete(handle_remove_track))
        .route("/v1/tracks/{guid}",       patch(handle_patch_track))
        .route("/v1/proofs/challenge",    post(handle_proofs_challenge))
        .route("/v1/proofs/assert",       post(handle_proofs_assert))
        .route("/v1/events",             get(handle_sse_events))
        .route("/health",              get(|| async { "ok" }))
        .merge(query::query_routes())
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(build_cors_layer())
        .with_state(state)
}

/// Read-only router for community nodes.
// FG-05 community peers — 2026-03-13
pub fn build_readonly_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/sync/events", get(handle_sync_events))
        .route("/sync/peers",  get(handle_sync_peers))
        .route("/node/info",   get(handle_node_info))
        .route("/v1/events",   get(handle_sse_events))
        .route("/health",      get(|| async { "ok" }))
        .merge(query::query_routes())
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(build_cors_layer())
        .with_state(state)
}

// ── POST /ingest/feed ─────────────────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "single ingest flow — splitting would obscure the sequential validation steps")]
#[expect(clippy::needless_collect, reason = "events_for_fanout snapshot is required because event_rows is consumed by ingest_transaction")]
async fn handle_ingest_feed(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ingest::IngestFeedRequest>,
) -> Result<Json<ingest::IngestResponse>, ApiError> {
    let state2 = Arc::clone(&state);
    // Mutex safety compliant — 2026-03-12
    let result = tokio::task::spawn_blocking(move || -> Result<(ingest::IngestResponse, Vec<event::Event>), ApiError> {
        let mut conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // 1. Get existing feed
        let existing = db::get_existing_feed(&conn, &req.canonical_url)?;

        // 2. Build verify context and run chain
        let ctx = verify::IngestContext {
            request:  &req,
            db:       &conn,
            existing: existing.as_ref(),
        };

        let warnings = match state2.chain.run(&ctx) {
            Err(ref e) if e.0 == crate::verifiers::content_hash::NO_CHANGE_SENTINEL => {
                return Ok((ingest::IngestResponse {
                    accepted:       true,
                    no_change:      true,
                    reason:         None,
                    events_emitted: vec![],
                    warnings:       vec![],
                }, vec![]));
            }
            Err(e) => {
                return Ok((ingest::IngestResponse {
                    accepted:       false,
                    no_change:      false,
                    reason:         Some(e.0),
                    events_emitted: vec![],
                    warnings:       vec![],
                }, vec![]));
            }
            Ok(w) => w,
        };

        // 3. Unwrap feed_data
        let feed_data = req.feed_data.as_ref().ok_or_else(|| ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "feed_data is required for successful ingest".into(),
            www_authenticate: None,
        })?;

        // 3b. Enforce track count limit to prevent DB growth attacks.
        if feed_data.tracks.len() > MAX_TRACKS_PER_INGEST {
            return Err(ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: format!(
                    "feed contains {} tracks, maximum is {MAX_TRACKS_PER_INGEST}",
                    feed_data.tracks.len()
                ),
                www_authenticate: None,
            });
        }

        // 4. Resolve artist
        let artist_name = feed_data
            .owner_name
            .as_deref()
            .or(feed_data.author_name.as_deref())
            .unwrap_or(feed_data.title.as_str())
            .to_string();

        let feed_artist = db::resolve_artist(&conn, &artist_name)?;

        // 5. Get or create artist credit for the feed artist (idempotent)
        let feed_artist_credit = db::get_or_create_artist_credit(
            &conn,
            &feed_artist.name,
            &[(feed_artist.artist_id.clone(), feed_artist.name.clone(), String::new())],
        )?;

        // 6. Get current time
        let now = db::unix_now();

        // 7. Compute newest_item_at and oldest_item_at from track pub_dates
        let pub_dates: Vec<i64> = feed_data
            .tracks
            .iter()
            .filter_map(|t| t.pub_date)
            .collect();

        let newest_item_at = pub_dates.iter().copied().max();
        let oldest_item_at = pub_dates.iter().copied().min();

        // 8. Build Feed struct
        let feed = model::Feed {
            feed_guid:        feed_data.feed_guid.clone(),
            feed_url:         req.canonical_url.clone(),
            title:            feed_data.title.clone(),
            title_lower:      feed_data.title.to_lowercase(),
            artist_credit_id: feed_artist_credit.id,
            description:      feed_data.description.clone(),
            image_url:        feed_data.image_url.clone(),
            language:         feed_data.language.clone(),
            explicit:         feed_data.explicit,
            itunes_type:      feed_data.itunes_type.clone(),
            #[expect(clippy::cast_possible_wrap, reason = "episode counts never approach i64::MAX")]
            episode_count:    feed_data.tracks.len() as i64,
            newest_item_at,
            oldest_item_at,
            created_at:       now,
            updated_at:       now,
            raw_medium:       feed_data.raw_medium.clone(),
        };

        // 8b. Build feed-level payment routes
        let feed_routes: Vec<model::FeedPaymentRoute> = feed_data
            .feed_payment_routes
            .iter()
            .map(|r| model::FeedPaymentRoute {
                id:             None,
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

        // 9. Build track tuples
        let mut track_tuples: Vec<(
            model::Track,
            Vec<model::PaymentRoute>,
            Vec<model::ValueTimeSplit>,
        )> = Vec::with_capacity(feed_data.tracks.len());

        // Track artist credits for event generation
        let mut track_credits: Vec<model::ArtistCredit> = Vec::with_capacity(feed_data.tracks.len());

        for track_data in &feed_data.tracks {
            // Per-track artist resolution
            let (track_credit_id, track_credit) = if let Some(author) = &track_data.author_name {
                let track_artist = db::resolve_artist(&conn, author)?;
                let credit = db::get_or_create_artist_credit(
                    &conn,
                    &track_artist.name,
                    &[(track_artist.artist_id.clone(), track_artist.name.clone(), String::new())],
                )?;
                (credit.id, credit)
            } else {
                (feed_artist_credit.id, feed_artist_credit.clone())
            };

            let track = model::Track {
                track_guid:       track_data.track_guid.clone(),
                feed_guid:        feed_data.feed_guid.clone(),
                artist_credit_id: track_credit_id,
                title:            track_data.title.clone(),
                title_lower:      track_data.title.to_lowercase(),
                pub_date:         track_data.pub_date,
                duration_secs:    track_data.duration_secs,
                enclosure_url:    track_data.enclosure_url.clone(),
                enclosure_type:   track_data.enclosure_type.clone(),
                enclosure_bytes:  track_data.enclosure_bytes,
                track_number:     track_data.track_number,
                season:           track_data.season,
                explicit:         track_data.explicit,
                description:      track_data.description.clone(),
                created_at:       now,
                updated_at:       now,
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
            track_credits.push(track_credit);
        }

        // 10. Build event rows
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
                www_authenticate: None,
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

        // ArtistCreditCreated
        {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::ArtistCreditCreatedPayload {
                artist_credit: feed_artist_credit.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize ArtistCreditCreated payload: {e}"),
                www_authenticate: None,
            })?;
            let (signed_by, signature) = state2.signer.sign_event(
                &event_id,
                &event::EventType::ArtistCreditCreated,
                &payload_json,
                &feed_artist.artist_id,
                now,
            );
            event_rows.push(db::EventRow {
                event_id,
                event_type:   event::EventType::ArtistCreditCreated,
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
                feed:          feed.clone(),
                artist:        feed_artist.clone(),
                artist_credit: feed_artist_credit.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize FeedUpserted payload: {e}"),
                www_authenticate: None,
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

        // FeedRoutesReplaced (if feed has payment routes)
        if !feed_routes.is_empty() {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::FeedRoutesReplacedPayload {
                feed_guid: feed.feed_guid.clone(),
                routes:    feed_routes.clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize FeedRoutesReplaced payload: {e}"),
                www_authenticate: None,
            })?;
            let (signed_by, signature) = state2.signer.sign_event(
                &event_id,
                &event::EventType::FeedRoutesReplaced,
                &payload_json,
                &feed.feed_guid,
                now,
            );
            event_rows.push(db::EventRow {
                event_id,
                event_type:   event::EventType::FeedRoutesReplaced,
                payload_json,
                subject_guid: feed.feed_guid.clone(),
                signed_by,
                signature,
                created_at:   now,
                warnings:     warnings.clone(),
            });
        }

        // TrackUpserted — one per track
        for (i, (track, routes, vts)) in track_tuples.iter().enumerate() {
            let event_id = uuid::Uuid::new_v4().to_string();
            let payload = event::TrackUpsertedPayload {
                track:             track.clone(),
                routes:            routes.clone(),
                value_time_splits: vts.clone(),
                artist_credit:     track_credits[i].clone(),
            };
            let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to serialize TrackUpserted payload: {e}"),
                www_authenticate: None,
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

        // Collect event_ids and snapshot event data before moving event_rows
        let event_ids: Vec<String> = event_rows.iter().map(|r| r.event_id.clone()).collect();

        // Snapshot events for fan-out (event_rows is consumed by ingest_transaction)
        let events_for_fanout: Vec<db::EventRow> = event_rows.iter().map(|r| db::EventRow {
            event_id:     r.event_id.clone(),
            event_type:   r.event_type.clone(),
            payload_json: r.payload_json.clone(),
            subject_guid: r.subject_guid.clone(),
            signed_by:    r.signed_by.clone(),
            signature:    r.signature.clone(),
            created_at:   r.created_at,
            warnings:     r.warnings.clone(),
        }).collect();

        // 11. Run ingest transaction
        let seqs = db::ingest_transaction(
            &mut conn,
            feed_artist,
            feed_artist_credit,
            feed,
            feed_routes,
            track_tuples,
            event_rows,
        )?;

        // 11b. Search index + quality scores are now written inside
        // ingest_transaction (Issue-5 ingest atomic — 2026-03-13).

        // 12. Update crawl cache
        db::upsert_feed_crawl_cache(&conn, &req.canonical_url, &req.content_hash, now)?;

        // 13. Reconstruct events with assigned seqs for fan-out
        let fanout_events: Vec<event::Event> = events_for_fanout.into_iter().zip(seqs.iter()).map(
            |(r, &seq)| {
                let et_str = serde_json::to_string(&r.event_type).map_err(|e| ApiError {
                    status:  StatusCode::INTERNAL_SERVER_ERROR,
                    message: format!("failed to serialize event type for fan-out: {e}"),
                    www_authenticate: None,
                })?;
                let et_str = et_str.trim_matches('"');
                let tagged = format!(r#"{{"type":"{et_str}","data":{}}}"#, r.payload_json);
                let payload = serde_json::from_str::<event::EventPayload>(&tagged)
                    .map_err(|e| ApiError {
                        status:  StatusCode::INTERNAL_SERVER_ERROR,
                        message: format!("failed to deserialize event payload for fan-out: {e}"),
                        www_authenticate: None,
                    })?;
                Ok(event::Event {
                    event_id:     r.event_id,
                    event_type:   r.event_type,
                    payload,
                    subject_guid: r.subject_guid,
                    signed_by:    r.signed_by,
                    signature:    r.signature,
                    seq,
                    created_at:   r.created_at,
                    warnings:     r.warnings,
                    payload_json: r.payload_json,
                })
            }
        ).collect::<Result<Vec<_>, ApiError>>()?;

        Ok((ingest::IngestResponse {
            accepted:       true,
            no_change:      false,
            reason:         None,
            events_emitted: event_ids,
            warnings,
        }, fanout_events))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let (response, fanout_events) = result?;

    // Fire-and-forget fan-out to push subscribers.
    if !fanout_events.is_empty() {
        // Issue-SSE-PUBLISH — 2026-03-14
        publish_events_to_sse(&state.sse_registry, &fanout_events);

        let db_fanout          = Arc::clone(&state.db);
        let client_fanout      = state.push_client.clone();
        let subscribers_fanout = Arc::clone(&state.push_subscribers);
        tokio::spawn(fan_out_push(db_fanout, client_fanout, subscribers_fanout, fanout_events));
    }

    Ok(Json(response))
}

// ── GET /sync/events ──────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct SyncEventsQuery {
    #[serde(default)]
    after_seq: i64,
    limit:     Option<i64>,
}

// Mutex safety compliant — 2026-03-12
async fn handle_sync_events(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SyncEventsQuery>,
) -> Result<Json<sync::SyncEventsResponse>, ApiError> {
    let after_seq = params.after_seq;
    let capped_limit = params.limit.unwrap_or(500).min(1000);

    let result = spawn_db(Arc::clone(&state.db), move |conn| {
        let events = db::get_events_since(conn, after_seq, capped_limit)?;

        let has_more = events.len() == usize::try_from(capped_limit).unwrap_or(usize::MAX);
        let next_seq = events.last().map_or(after_seq, |e| e.seq);

        Ok(sync::SyncEventsResponse {
            events,
            has_more,
            next_seq,
        })
    })
    .await?;

    Ok(Json(result))
}

// ── POST /sync/reconcile ──────────────────────────────────────────────────────

// Mutex safety compliant — 2026-03-12
async fn handle_sync_reconcile(
    State(state): State<Arc<AppState>>,
    Json(req): Json<sync::ReconcileRequest>,
) -> Result<Json<sync::ReconcileResponse>, ApiError> {
    // Availability: cap the size of the `have` set to prevent memory exhaustion.
    if req.have.len() > MAX_RECONCILE_HAVE {
        return Err(ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: format!("have array exceeds maximum size of {MAX_RECONCILE_HAVE}"),
            www_authenticate: None,
        });
    }

    // Finding-5 reconcile pagination — 2026-03-13
    let result = spawn_db(Arc::clone(&state.db), move |conn| {
        let (our_refs, refs_truncated) =
            db::get_event_refs_since(conn, req.since_seq, MAX_RECONCILE_REFS)?;

        let our_ids: HashSet<String> =
            our_refs.iter().map(|r| r.event_id.clone()).collect();
        let their_ids: HashSet<String> =
            req.have.iter().map(|r| r.event_id.clone()).collect();

        let missing_ids: HashSet<&String> = our_ids.difference(&their_ids).collect();

        let unknown_to_us: Vec<sync::EventRef> = req
            .have
            .into_iter()
            .filter(|r| !our_ids.contains(&r.event_id))
            .collect();

        let all_events = db::get_events_since(conn, req.since_seq, MAX_RECONCILE_EVENTS)?;
        let events_capped =
            i64::try_from(all_events.len()).unwrap_or(i64::MAX) >= MAX_RECONCILE_EVENTS;
        let send_to_node: Vec<crate::event::Event> = all_events
            .into_iter()
            .filter(|e| missing_ids.contains(&e.event_id))
            .collect();

        let now = db::unix_now();

        let has_more = refs_truncated || events_capped;
        let next_seq = our_refs.iter().map(|r| r.seq).max().unwrap_or(req.since_seq);

        db::upsert_node_sync_state(conn, &req.node_pubkey, next_seq, now)?;

        Ok(sync::ReconcileResponse {
            send_to_node,
            unknown_to_us,
            has_more,
            next_seq,
        })
    })
    .await?;

    Ok(Json(result))
}

// ── fan_out_push ──────────────────────────────────────────────────────────────

// SP-04 push retry — 2026-03-13
// Mutex safety compliant — 2026-03-12
#[expect(clippy::unused_async, reason = "must be async because tokio::spawn requires a Future")]
async fn fan_out_push(
    db:          db::Db,
    client:      reqwest::Client,
    subscribers: Arc<RwLock<HashMap<String, String>>>,
    events:      Vec<event::Event>,
) {
    fan_out_push_inner(db, client, subscribers, events);
}

/// Public entry point for integration tests that need to exercise push fan-out
/// with retry logic. Not part of the stable API — test-only.
// SP-04 push retry — 2026-03-13
#[expect(clippy::unused_async, reason = "async signature for convenience in test await context")]
#[expect(clippy::implicit_hasher, reason = "test-only API; generic hasher adds no value")]
pub async fn fan_out_push_public(
    db:          db::Db,
    client:      reqwest::Client,
    subscribers: Arc<RwLock<HashMap<String, String>>>,
    events:      Vec<event::Event>,
) {
    fan_out_push_inner(db, client, subscribers, events);
}

/// Maximum number of push attempts per peer (initial + retries).
const PUSH_MAX_ATTEMPTS: u64 = 3;

/// Number of consecutive push failures before a peer is evicted from the
/// in-memory subscriber cache.
// SP-04 push retry — 2026-03-13
const PUSH_EVICTION_THRESHOLD: i64 = 10;

// SP-04 push retry — 2026-03-13
#[expect(clippy::needless_pass_by_value, reason = "values are cloned into spawned tasks; ownership transfer is intentional")]
fn fan_out_push_inner(
    db:          db::Db,
    client:      reqwest::Client,
    subscribers: Arc<RwLock<HashMap<String, String>>>,
    events:      Vec<event::Event>,
) {
    let peers: Vec<(String, String)> = {
        let Ok(guard) = subscribers.read() else {
            tracing::error!("fanout: push_subscribers RwLock poisoned; skipping fan-out");
            return;
        };
        guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    };

    let body = sync::PushRequest { events };

    for (pubkey, push_url) in peers {
        let client2      = client.clone();
        let db2          = Arc::clone(&db);
        let subs2        = Arc::clone(&subscribers);
        let pubkey2      = pubkey.clone();
        let push_url2    = push_url.clone();
        let body2        = sync::PushRequest { events: body.events.clone() };

        tokio::spawn(async move {
            let mut success = false;

            // SP-04 push retry — 2026-03-13
            for attempt in 0..PUSH_MAX_ATTEMPTS {
                if attempt > 0 {
                    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                }
                match client2
                    .post(&push_url2)
                    .json(&body2)
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        success = true;
                        break;
                    }
                    Ok(resp) => {
                        tracing::warn!(
                            url = %push_url2, attempt, status = %resp.status(),
                            "fanout: push returned non-success HTTP status"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            url = %push_url2, attempt, error = %e,
                            "fanout: push request failed"
                        );
                    }
                }
            }

            if success {
                let now = db::unix_now();
                // Mutex safety compliant — 2026-03-12
                match db2.lock() {
                    Ok(conn) => {
                        if let Err(e) = db::record_push_success(&conn, &pubkey2, now) {
                            tracing::error!(peer = %pubkey2, error = %e, "fanout: failed to record push success");
                        }
                    }
                    Err(_) => {
                        tracing::error!(peer = %pubkey2, "fanout: db mutex poisoned; cannot record push success");
                    }
                }
            } else {
                handle_push_failure(&db2, &subs2, &pubkey2);
            }
        });
    }
}

// SP-04 push retry — 2026-03-13
// Mutex safety compliant — 2026-03-12
fn handle_push_failure(
    db:          &db::Db,
    subscribers: &Arc<RwLock<HashMap<String, String>>>,
    pubkey:      &str,
) {
    let Ok(conn) = db.lock() else {
        tracing::error!(peer = %pubkey, "fanout: db mutex poisoned; cannot track push failure");
        return;
    };
    if let Err(e) = db::increment_peer_failures(&conn, pubkey) {
        tracing::error!(peer = %pubkey, error = %e, "fanout: failed to increment failures");
        return;
    }

    let failures: i64 = conn
        .query_row(
            "SELECT consecutive_failures FROM peer_nodes WHERE node_pubkey = ?1",
            rusqlite::params![pubkey],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if failures >= PUSH_EVICTION_THRESHOLD {
        match subscribers.write() {
            Ok(mut guard) => {
                guard.remove(pubkey);
                tracing::warn!(
                    peer = %pubkey, threshold = PUSH_EVICTION_THRESHOLD,
                    "fanout: evicted peer from push cache after consecutive failures"
                );
            }
            Err(_) => {
                tracing::error!(peer = %pubkey, "fanout: push_subscribers RwLock poisoned; cannot evict");
            }
        }
    }
}

// ── POST /sync/register ───────────────────────────────────────────────────────

// CS-03 authenticated register — 2026-03-12
// Finding-3 separate sync token — 2026-03-13
// Mutex safety compliant — 2026-03-12
async fn handle_sync_register(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<sync::RegisterRequest>,
) -> Result<Json<sync::RegisterResponse>, ApiError> {
    check_sync_or_admin_token(&headers, state.sync_token.as_deref(), &state.admin_token)?;

    let now = db::unix_now();

    let pubkey = req.node_pubkey.clone();
    let url    = req.node_url.clone();

    spawn_db(Arc::clone(&state.db), move |conn| {
        db::upsert_peer_node(conn, &pubkey, &url, now)?;
        db::reset_peer_failures(conn, &pubkey)?;
        Ok(())
    })
    .await?;

    {
        let mut guard = state.push_subscribers.write().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "push_subscribers lock poisoned".into(),
            www_authenticate: None,
        })?;
        guard.insert(req.node_pubkey.clone(), req.node_url.clone());
    }

    tracing::info!(peer = %req.node_pubkey, url = %req.node_url, "registered push peer");

    Ok(Json(sync::RegisterResponse { ok: true }))
}

// ── GET /sync/peers ───────────────────────────────────────────────────────────

async fn handle_sync_peers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<sync::PeersResponse>, ApiError> {
    // Mutex safety compliant — 2026-03-12
    let result = spawn_db(Arc::clone(&state.db), move |conn| {
        let peers = db::get_push_peers(conn)?;
        let nodes = peers.into_iter().map(|p| sync::PeerEntry {
            node_pubkey:  p.node_pubkey,
            node_url:     p.node_url,
            last_push_at: p.last_push_at,
        }).collect();
        Ok(sync::PeersResponse { nodes })
    })
    .await?;

    Ok(Json(result))
}

// ── GET /node/info ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct NodeInfoResponse {
    node_pubkey: String,
}

async fn handle_node_info(
    State(state): State<Arc<AppState>>,
) -> Json<NodeInfoResponse> {
    Json(NodeInfoResponse { node_pubkey: state.node_pubkey_hex.clone() })
}

// ── FG-02 SSE artist follow — 2026-03-13 ─────────────────────────────────────

/// `GET /v1/events?artists=id1,id2,...` — Server-Sent Events for artist followers.
///
/// Subscribes the client to real-time notifications for the specified artist IDs.
/// Supports `Last-Event-ID` header for replaying missed events from the ring buffer.
///
/// Enforces two availability limits:
/// - `MAX_SSE_CONNECTIONS`: total concurrent SSE connections server-wide.
/// - `MAX_SSE_REGISTRY_ARTISTS`: total unique artist entries in the registry.
async fn handle_sse_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<axum::response::sse::Sse<impl futures_core::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>>, ApiError> {
    use tokio_stream::StreamExt as _;

    // Cap the number of artist IDs per SSE connection to prevent unbounded
    // channel creation in the registry (availability hardening).
    const MAX_SSE_ARTISTS: usize = 50;

    // Enforce concurrent SSE connection limit.
    if !state.sse_registry.try_acquire_connection() {
        return Err(ApiError {
            status:  StatusCode::SERVICE_UNAVAILABLE,
            message: format!("too many SSE connections (limit: {MAX_SSE_CONNECTIONS})"),
            www_authenticate: None,
        });
    }

    let artist_ids: Vec<String> = params
        .get("artists")
        .map(|s| {
            s.split(',')
                .map(|id| id.trim().to_string())
                .filter(|id| !id.is_empty())
                .take(MAX_SSE_ARTISTS)
                .collect()
        })
        .unwrap_or_default();

    // Issue-SSE-PUBLISH — 2026-03-14: parse Last-Event-ID as an integer seq
    // for unambiguous replay. Falls back to 0 (replay everything in ring
    // buffer) if the header is absent or not a valid integer.
    let last_seq: i64 = headers
        .get("Last-Event-ID")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    // Issue-SSE-PUBLISH — 2026-03-14: replay from ring buffer using seq-based
    // cursor instead of subject_guid matching.
    let mut replay_events: Vec<SseFrame> = Vec::new();
    if last_seq > 0 {
        for artist_id in &artist_ids {
            let recent = state.sse_registry.recent_events(artist_id);
            for frame in recent {
                if frame.seq > last_seq {
                    replay_events.push(frame);
                }
            }
        }
        // Sort by seq so replayed events arrive in order across artists.
        replay_events.sort_by_key(|f| f.seq);
    }

    // Subscribe to live broadcast channels.
    // If the registry is full for a given artist (new, not yet tracked),
    // we silently skip that artist rather than failing the whole connection.
    let mut receivers: Vec<tokio::sync::broadcast::Receiver<SseFrame>> = Vec::new();
    for artist_id in &artist_ids {
        if let Some(rx) = state.sse_registry.subscribe(artist_id) {
            receivers.push(rx);
        }
    }

    // Clone the registry Arc so the live stream can release the connection on drop.
    let registry = Arc::clone(&state.sse_registry);

    // Merge replay events as an initial stream, then live events.
    // Issue-SSE-PUBLISH — 2026-03-14: use seq as SSE id for unambiguous replay.
    let replay_stream = tokio_stream::iter(replay_events.into_iter().map(|frame| {
        let json = serde_json::to_string(&frame).unwrap_or_default();
        Ok(axum::response::sse::Event::default()
            .event(&frame.event_type)
            .id(frame.seq.to_string())
            .data(json))
    }));

    // Issue-14 SSE async stream — 2026-03-13
    // Convert each broadcast receiver into an async BroadcastStream and merge
    // them with select_all. This eliminates the 100ms busy-sleep polling loop
    // that caused 10,000 wakeups/sec at max connections.
    let live_stream = async_stream::stream! {
        // Guard: release the SSE connection slot when this stream is dropped.
        let _guard = SseConnectionGuard { registry };

        use tokio_stream::wrappers::BroadcastStream;
        use futures_util::stream::select_all;

        let streams: Vec<_> = receivers
            .into_iter()
            .map(BroadcastStream::new)
            .collect();

        let mut merged = select_all(streams);

        while let Some(item) = futures_util::StreamExt::next(&mut merged).await {
            match item {
                Ok(frame) => {
                    let json = serde_json::to_string(&frame).unwrap_or_default();
                    // Issue-SSE-PUBLISH — 2026-03-14: use seq as SSE id.
                    yield Ok(axum::response::sse::Event::default()
                        .event(&frame.event_type)
                        .id(frame.seq.to_string())
                        .data(json));
                }
                Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                    tracing::debug!(lagged = n, "SSE client lagged behind broadcast");
                }
            }
        }
    };

    let merged = replay_stream.chain(live_stream);

    Ok(axum::response::sse::Sse::new(merged)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(std::time::Duration::from_secs(30))
                .text("keepalive"),
        ))
}

/// RAII guard that releases an SSE connection slot when the stream is dropped
/// (i.e., when the client disconnects).
struct SseConnectionGuard {
    registry: Arc<SseRegistry>,
}

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.registry.release_connection();
    }
}

// ── Admin auth helper ─────────────────────────────────────────────────────────

// CS-02 constant-time — 2026-03-12
fn check_admin_token(headers: &HeaderMap, expected: &str) -> Result<(), ApiError> {
    if expected.is_empty() {
        return Err(ApiError {
            status:  StatusCode::FORBIDDEN,
            message: "admin token not configured on this node".into(),
            www_authenticate: None,
        });
    }
    let provided = headers
        .get("X-Admin-Token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let h1 = Sha256::digest(provided.as_bytes());
    let h2 = Sha256::digest(expected.as_bytes());
    if bool::from(h1.ct_eq(&h2)) {
        Ok(())
    } else {
        Err(ApiError {
            status:  StatusCode::FORBIDDEN,
            message: "invalid or missing X-Admin-Token".into(),
            www_authenticate: None,
        })
    }
}

// ── Sync registration auth helper ──────────────────────────────────────────────

// Finding-3 separate sync token — 2026-03-13
// CS-02 constant-time — 2026-03-12
/// Checks authentication for `POST /sync/register`.
///
/// When `sync_token` is `Some`, only the `X-Sync-Token` header is accepted.
/// When `sync_token` is `None` (backward compatibility), falls back to
/// `X-Admin-Token` with a deprecation warning.
/// If neither token is configured, returns 403.
fn check_sync_or_admin_token(
    headers: &HeaderMap,
    sync_token: Option<&str>,
    admin_token: &str,
) -> Result<(), ApiError> {
    if let Some(expected_sync) = sync_token {
        // SYNC_TOKEN is configured — only accept X-Sync-Token.
        if expected_sync.is_empty() {
            return Err(ApiError {
                status:  StatusCode::FORBIDDEN,
                message: "sync token not configured on this node".into(),
                www_authenticate: None,
            });
        }
        let provided = headers
            .get("X-Sync-Token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let h1 = Sha256::digest(provided.as_bytes());
        let h2 = Sha256::digest(expected_sync.as_bytes());
        if bool::from(h1.ct_eq(&h2)) {
            return Ok(());
        }
        return Err(ApiError {
            status:  StatusCode::FORBIDDEN,
            message: "invalid or missing X-Sync-Token".into(),
            www_authenticate: None,
        });
    }

    // No SYNC_TOKEN configured — fall back to ADMIN_TOKEN (backward compatibility).
    if admin_token.is_empty() {
        return Err(ApiError {
            status:  StatusCode::FORBIDDEN,
            message: "neither sync token nor admin token configured on this node".into(),
            www_authenticate: None,
        });
    }

    tracing::warn!(
        "DEPRECATED: POST /sync/register authenticated via ADMIN_TOKEN. \
         Set SYNC_TOKEN env var for least-privilege sync registration."
    );
    check_admin_token(headers, admin_token)
}

// ── POST /admin/artists/merge ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct MergeArtistsRequest {
    source_artist_id: String,
    target_artist_id: String,
}

#[derive(Serialize)]
struct MergeArtistsResponse {
    merged:         bool,
    events_emitted: Vec<String>,
}

async fn handle_admin_merge_artists(
    State(state): State<Arc<AppState>>,
    headers:      HeaderMap,
    Json(req):    Json<MergeArtistsRequest>,
) -> Result<Json<MergeArtistsResponse>, ApiError> {
    check_admin_token(&headers, &state.admin_token)?;

    let state2 = Arc::clone(&state);
    // Mutex safety compliant — 2026-03-12
    // Finding-2 atomic mutation+event — 2026-03-13
    // Issue-SSE-PUBLISH — 2026-03-14: return (response, sse_frame_info) for SSE publish.
    let result = tokio::task::spawn_blocking(move || -> Result<(MergeArtistsResponse, Option<(String, SseFrame)>), ApiError> {
        let conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        let tx = conn.unchecked_transaction().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        let transferred = db::merge_artists_sql(&tx, &req.source_artist_id, &req.target_artist_id)
            .map_err(ApiError::from)?;

        let now = db::unix_now();

        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event::ArtistMergedPayload {
            source_artist_id:    req.source_artist_id.clone(),
            target_artist_id:    req.target_artist_id.clone(),
            aliases_transferred: transferred,
        };
        let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to serialize ArtistMerged payload: {e}"),
            www_authenticate: None,
        })?;
        let (signed_by, signature) = state2.signer.sign_event(
            &event_id,
            &event::EventType::ArtistMerged,
            &payload_json,
            &req.target_artist_id,
            now,
        );

        let seq = db::insert_event(
            &tx,
            &event_id,
            &event::EventType::ArtistMerged,
            &payload_json,
            &req.target_artist_id,
            &signed_by,
            &signature,
            now,
            &[],
        )
        .map_err(ApiError::from)?;

        tx.commit().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        // Issue-SSE-PUBLISH — 2026-03-14
        let sse_info = {
            let frame = SseFrame {
                event_type:   "artist_merged".to_string(),
                subject_guid: req.target_artist_id.clone(),
                payload:      serde_json::to_value(&payload).unwrap_or(serde_json::Value::Null),
                seq,
            };
            Some((req.target_artist_id.clone(), frame))
        };

        Ok((MergeArtistsResponse {
            merged:         true,
            events_emitted: vec![event_id],
        }, sse_info))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let (response, sse_info) = result?;

    // Issue-SSE-PUBLISH — 2026-03-14
    if let Some((artist_id, frame)) = sse_info {
        state.sse_registry.publish(&artist_id, frame);
    }

    Ok(Json(response))
}

// ── POST /admin/artists/alias ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct AddAliasRequest {
    artist_id: String,
    alias:     String,
}

#[derive(Serialize)]
struct AddAliasResponse {
    ok: bool,
}

async fn handle_admin_add_alias(
    State(state): State<Arc<AppState>>,
    headers:      HeaderMap,
    Json(req):    Json<AddAliasRequest>,
) -> Result<Json<AddAliasResponse>, ApiError> {
    check_admin_token(&headers, &state.admin_token)?;

    // Mutex safety compliant — 2026-03-12
    let result = spawn_db(Arc::clone(&state.db), move |conn| {
        db::add_artist_alias(conn, &req.artist_id, &req.alias)?;
        Ok(AddAliasResponse { ok: true })
    })
    .await?;

    Ok(Json(result))
}

// ── DELETE /feeds/{guid} ───────────────────────────────────────────────────

#[expect(clippy::too_many_lines, reason = "event signing, SSE publish, and fan-out all live in one handler")]
async fn handle_retire_feed(
    State(state): State<Arc<AppState>>,
    headers:      HeaderMap,
    Path(guid):   Path<String>,
) -> Result<StatusCode, ApiError> {
    let state2 = Arc::clone(&state);
    let guid2 = guid.clone();
    // Mutex safety compliant — 2026-03-12
    // Issue-SSE-PUBLISH — 2026-03-14: return (events, artist_id) so we can
    // publish to the correct SSE channel after the entity is deleted.
    let result = tokio::task::spawn_blocking(move || -> Result<(Option<Vec<event::Event>>, Option<String>), ApiError> {
        let mut conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Auth inside lock scope: eliminates TOCTOU between auth check and DB write.
        check_admin_or_bearer_with_conn(&conn, &headers, &state2.admin_token, "feed:write", &guid2)?;

        // Look up the feed — 404 if not found.
        let feed = db::get_feed_by_guid(&conn, &guid2)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: format!("feed {guid2} not found"),
                www_authenticate: None,
            })?;

        // Issue-SSE-PUBLISH — 2026-03-14: capture artist_id before deletion.
        let sse_artist_id = db::get_artist_credit(&conn, feed.artist_credit_id)
            .ok()
            .flatten()
            .and_then(|c| c.names.first().map(|n| n.artist_id.clone()));

        // Fetch tracks to remove from search index.
        let tracks = db::get_tracks_for_feed(&conn, &guid2)?;

        // Remove search index entries (best-effort).
        for track in &tracks {
            let _ = crate::search::delete_from_search_index(
                &conn, "track", &track.track_guid,
                "", &track.title,
                track.description.as_deref().unwrap_or(""),
                "",
            );
        }
        let _ = crate::search::delete_from_search_index(
            &conn, "feed", &feed.feed_guid,
            "", &feed.title,
            feed.description.as_deref().unwrap_or(""),
            feed.raw_medium.as_deref().unwrap_or(""),
        );

        // Build and sign a FeedRetired event.
        let now = db::unix_now();

        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event::FeedRetiredPayload {
            feed_guid: guid2.clone(),
            reason:    Some("admin retired via API".to_string()),
        };
        let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to serialize FeedRetired payload: {e}"),
            www_authenticate: None,
        })?;
        let (signed_by, signature) = state2.signer.sign_event(
            &event_id,
            &event::EventType::FeedRetired,
            &payload_json,
            &guid2,
            now,
        );

        // Cascade-delete the feed and record the event atomically.
        let seq = db::delete_feed_with_event(
            &mut conn,
            &guid2,
            &event_id,
            &payload_json,
            &guid2,
            &signed_by,
            &signature,
            now,
            &[],
        )
        .map_err(ApiError::from)?;

        // Build event for fan-out.
        let tagged = format!(r#"{{"type":"feed_retired","data":{payload_json}}}"#);
        let ev_payload = serde_json::from_str::<event::EventPayload>(&tagged)
            .map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to deserialize FeedRetired event for fan-out: {e}"),
                www_authenticate: None,
            })?;

        let fanout_event = event::Event {
            event_id,
            event_type:   event::EventType::FeedRetired,
            payload:      ev_payload,
            subject_guid: guid2,
            signed_by,
            signature,
            seq,
            created_at:   now,
            warnings:     vec![],
            payload_json,
        };

        Ok((Some(vec![fanout_event]), sse_artist_id))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let (fanout_events, sse_artist_id) = result?;

    // Fire-and-forget fan-out.
    if let Some(events) = fanout_events
        && !events.is_empty()
    {
        // Issue-SSE-PUBLISH — 2026-03-14
        if let Some(ref artist_id) = sse_artist_id {
            for ev in &events {
                let frame = SseFrame {
                    event_type:   serde_json::to_string(&ev.event_type).unwrap_or_default().trim_matches('"').to_string(),
                    subject_guid: ev.subject_guid.clone(),
                    payload:      serde_json::to_value(&ev.payload).unwrap_or(serde_json::Value::Null),
                    seq:          ev.seq,
                };
                state.sse_registry.publish(artist_id, frame);
            }
        }

        let db_fanout          = Arc::clone(&state.db);
        let client_fanout      = state.push_client.clone();
        let subscribers_fanout = Arc::clone(&state.push_subscribers);
        tokio::spawn(fan_out_push(db_fanout, client_fanout, subscribers_fanout, events));
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── DELETE /feeds/{guid}/tracks/{track_guid} ────────────────────────────────

#[expect(clippy::too_many_lines, reason = "event signing, SSE publish, and fan-out all live in one handler")]
async fn handle_remove_track(
    State(state):              State<Arc<AppState>>,
    headers:                   HeaderMap,
    Path((guid, track_guid)):  Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let state2 = Arc::clone(&state);
    let guid2 = guid.clone();
    let track_guid2 = track_guid.clone();
    // Mutex safety compliant — 2026-03-12
    // Issue-SSE-PUBLISH — 2026-03-14: return (events, artist_id) so we can
    // publish to the correct SSE channel after the entity is deleted.
    let result = tokio::task::spawn_blocking(move || -> Result<(Option<Vec<event::Event>>, Option<String>), ApiError> {
        let mut conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Auth inside lock scope: eliminates TOCTOU between auth check and DB write.
        // For bearer auth the token must be scoped to the parent feed.
        check_admin_or_bearer_with_conn(&conn, &headers, &state2.admin_token, "feed:write", &guid2)?;

        // Look up the track — 404 if not found.
        let track = db::get_track_by_guid(&conn, &track_guid2)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: format!("track {track_guid2} not found"),
                www_authenticate: None,
            })?;

        // Verify the track belongs to the specified feed.
        if track.feed_guid != guid2 {
            return Err(ApiError {
                status:  StatusCode::NOT_FOUND,
                message: format!("track {track_guid2} does not belong to feed {guid2}"),
                www_authenticate: None,
            });
        }

        // Issue-SSE-PUBLISH — 2026-03-14: capture artist_id before deletion.
        let sse_artist_id = db::get_artist_credit(&conn, track.artist_credit_id)
            .ok()
            .flatten()
            .and_then(|c| c.names.first().map(|n| n.artist_id.clone()));

        // Remove search index entry (best-effort).
        let _ = crate::search::delete_from_search_index(
            &conn, "track", &track.track_guid,
            "", &track.title,
            track.description.as_deref().unwrap_or(""),
            "",
        );

        // Build and sign a TrackRemoved event.
        let now = db::unix_now();

        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event::TrackRemovedPayload {
            track_guid: track_guid2.clone(),
            feed_guid:  guid2.clone(),
        };
        let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to serialize TrackRemoved payload: {e}"),
            www_authenticate: None,
        })?;
        let (signed_by, signature) = state2.signer.sign_event(
            &event_id,
            &event::EventType::TrackRemoved,
            &payload_json,
            &track_guid2,
            now,
        );

        // Cascade-delete the track and record the event atomically.
        let seq = db::delete_track_with_event(
            &mut conn,
            &track_guid2,
            &event_id,
            &payload_json,
            &track_guid2,
            &signed_by,
            &signature,
            now,
            &[],
        )
        .map_err(ApiError::from)?;

        // Build event for fan-out.
        let tagged = format!(r#"{{"type":"track_removed","data":{payload_json}}}"#);
        let ev_payload = serde_json::from_str::<event::EventPayload>(&tagged)
            .map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to deserialize TrackRemoved event for fan-out: {e}"),
                www_authenticate: None,
            })?;

        let fanout_event = event::Event {
            event_id,
            event_type:   event::EventType::TrackRemoved,
            payload:      ev_payload,
            subject_guid: track_guid2,
            signed_by,
            signature,
            seq,
            created_at:   now,
            warnings:     vec![],
            payload_json,
        };

        Ok((Some(vec![fanout_event]), sse_artist_id))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let (fanout_events, sse_artist_id) = result?;

    // Fire-and-forget fan-out.
    if let Some(events) = fanout_events
        && !events.is_empty()
    {
        // Issue-SSE-PUBLISH — 2026-03-14
        if let Some(ref artist_id) = sse_artist_id {
            for ev in &events {
                let frame = SseFrame {
                    event_type:   serde_json::to_string(&ev.event_type).unwrap_or_default().trim_matches('"').to_string(),
                    subject_guid: ev.subject_guid.clone(),
                    payload:      serde_json::to_value(&ev.payload).unwrap_or(serde_json::Value::Null),
                    seq:          ev.seq,
                };
                state.sse_registry.publish(artist_id, frame);
            }
        }

        let db_fanout          = Arc::clone(&state.db);
        let client_fanout      = state.push_client.clone();
        let subscribers_fanout = Arc::clone(&state.push_subscribers);
        tokio::spawn(fan_out_push(db_fanout, client_fanout, subscribers_fanout, events));
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── Bearer token extraction ────────────────────────────────────────────────

/// Build a `WWW-Authenticate` header value per RFC 6750 section 3.
///
/// When `error` is `None`, emits the minimal challenge:
///   `Bearer realm="stophammer"`
///
/// When `error` is provided (e.g. `"invalid_token"`, `"insufficient_scope"`),
/// appends the error attribute:
///   `Bearer realm="stophammer", error="invalid_token"`
// RFC 6750 compliant — 2026-03-12
#[must_use]
pub fn www_authenticate_challenge(error: Option<&str>) -> HeaderValue {
    let value = error.map_or_else(
        || r#"Bearer realm="stophammer""#.to_string(),
        |e| format!(r#"Bearer realm="stophammer", error="{e}""#),
    );
    // The constructed string is always valid ASCII header characters.
    HeaderValue::from_str(&value).unwrap_or_else(|_err| HeaderValue::from_static(r#"Bearer realm="stophammer""#))
}

/// Parse `Authorization: Bearer <token>` from headers.
/// Returns `None` for missing or malformed headers (never panics).
/// Trims leading/trailing whitespace from the extracted token.
// RFC 6750 compliant — 2026-03-12
#[must_use]
pub fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("Authorization")?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?.trim();
    if token.is_empty() {
        return None;
    }
    Some(token.to_string())
}

/// Validate admin or bearer auth using an already-held connection
///
/// Accepts either `X-Admin-Token` or `Authorization: Bearer <token>`.
/// Unlike the former `check_admin_or_bearer`, this variant takes a borrowed
/// `rusqlite::Connection` so that auth validation shares the same lock scope
/// as the subsequent DB write -- eliminating the TOCTOU race where the token
/// could be invalidated between auth check and mutation.
///
/// # Errors
///
/// Returns `StatusCode::FORBIDDEN` for bad admin tokens,
/// `StatusCode::UNAUTHORIZED` with `WWW-Authenticate` for missing or invalid
/// bearer tokens (RFC 6750 section 3), and `StatusCode::FORBIDDEN` with
/// `error="insufficient_scope"` if the bearer token's subject feed does not
/// match `expected_feed_guid`.
// RFC 6750 compliant — 2026-03-12
pub fn check_admin_or_bearer_with_conn(
    conn: &rusqlite::Connection,
    headers: &HeaderMap,
    admin_token: &str,
    required_scope: &str,
    expected_feed_guid: &str,
) -> Result<(), ApiError> {
    // Prefer admin token if the header is present.
    if headers.contains_key("X-Admin-Token") {
        return check_admin_token(headers, admin_token);
    }

    // Try bearer token.  RFC 6750 compliant — 2026-03-12
    let token = extract_bearer_token(headers).ok_or_else(|| ApiError {
        status:  StatusCode::UNAUTHORIZED,
        message: "missing Authorization header".into(),
        www_authenticate: Some(www_authenticate_challenge(None)),
    })?;

    let subject = proof::validate_token(conn, &token, required_scope)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError {
            status:  StatusCode::UNAUTHORIZED,
            message: "invalid_token".into(),
            www_authenticate: Some(www_authenticate_challenge(Some("invalid_token"))),
        })?;

    if subject != expected_feed_guid {
        return Err(ApiError {
            status:  StatusCode::FORBIDDEN,
            message: "insufficient_scope".into(),
            www_authenticate: Some(www_authenticate_challenge(Some("insufficient_scope"))),
        });
    }

    Ok(())
}

// ── POST /proofs/challenge ─────────────────────────────────────────────────

#[derive(Deserialize)]
struct ProofsChallengeRequest {
    feed_guid:       String,
    scope:           String,
    requester_nonce: String,
}

#[derive(Serialize)]
struct ProofsChallengeResponse {
    challenge_id:  String,
    token_binding: String,
    state:         String,
    expires_at:    i64,
}

async fn handle_proofs_challenge(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProofsChallengeRequest>,
) -> Result<(StatusCode, Json<ProofsChallengeResponse>), ApiError> {
    // Validate scope.
    if req.scope != "feed:write" {
        return Err(ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: format!("unsupported scope: {}", req.scope),
            www_authenticate: None,
        });
    }

    if req.requester_nonce.len() < 16 {
        return Err(ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "requester_nonce must be at least 16 characters".into(),
            www_authenticate: None,
        });
    }

    // Availability: cap nonce length to prevent oversized token_binding storage.
    if req.requester_nonce.len() > MAX_NONCE_BYTES {
        return Err(ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: format!("requester_nonce exceeds maximum length of {MAX_NONCE_BYTES} bytes"),
            www_authenticate: None,
        });
    }

    // Mutex safety compliant — 2026-03-12
    let state2 = Arc::clone(&state);
    let result = tokio::task::spawn_blocking(move || -> Result<ProofsChallengeResponse, ApiError> {
        let conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Rate limit: cap pending challenges per feed_guid to prevent table exhaustion.
        let pending_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM proof_challenges WHERE feed_guid = ?1 AND state = 'pending'",
            params![req.feed_guid],
            |row| row.get(0),
        ).map_err(ApiError::from)?;

        if pending_count >= MAX_PENDING_CHALLENGES_PER_FEED {
            return Err(ApiError {
                status:  StatusCode::TOO_MANY_REQUESTS,
                message: format!(
                    "too many pending challenges for this feed (limit: {MAX_PENDING_CHALLENGES_PER_FEED})"
                ),
                www_authenticate: None,
            });
        }

        let (challenge_id, token_binding) =
            proof::create_challenge(&conn, &req.feed_guid, &req.scope, &req.requester_nonce)
                .map_err(ApiError::from)?;

        // Read back the challenge to get expires_at.
        let challenge = proof::get_challenge(&conn, &challenge_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: "challenge not found after creation".into(),
                www_authenticate: None,
            })?;

        Ok(ProofsChallengeResponse {
            challenge_id,
            token_binding,
            state:      "pending".into(),
            expires_at: challenge.expires_at,
        })
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    result.map(|r| (StatusCode::CREATED, Json(r)))
}

// ── POST /proofs/assert ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ProofsAssertRequest {
    challenge_id:    String,
    requester_nonce: String,
}

#[derive(Serialize)]
struct ProofsAssertResponse {
    access_token:      String,
    scope:             String,
    subject_feed_guid: String,
    expires_at:        i64,
}

// CS-01 pod:txt verification — 2026-03-12
#[expect(clippy::too_many_lines, reason = "three-phase spawn_blocking pattern for RSS verification requires sequential structure")]
async fn handle_proofs_assert(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProofsAssertRequest>,
) -> Result<Json<ProofsAssertResponse>, ApiError> {
    if req.requester_nonce.len() < 16 {
        return Err(ApiError {
            status:  StatusCode::BAD_REQUEST,
            message: "requester_nonce must be at least 16 characters".into(),
            www_authenticate: None,
        });
    }

    // ── Phase 1 (blocking): validate nonce, load challenge, look up feed_url ──
    let state2 = Arc::clone(&state);
    let req_challenge_id = req.challenge_id.clone();
    let req_nonce = req.requester_nonce.clone();

    let phase1 = tokio::task::spawn_blocking(move || -> Result<(String, String, String, String), ApiError> {
        // Mutex safety compliant — 2026-03-12
        let conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Load the challenge (404 if not found or expired).
        let challenge = proof::get_challenge(&conn, &req_challenge_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: "challenge not found or expired".into(),
                www_authenticate: None,
            })?;

        // Check challenge is still pending (400 if already resolved).
        if challenge.state != "pending" {
            return Err(ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: format!("challenge already resolved as '{}'", challenge.state),
                www_authenticate: None,
            });
        }

        // Recompute token_binding from stored token + requester_nonce.
        let expected = proof::recompute_binding(&challenge.token_binding, &req_nonce);
        let nonce_ok = expected.as_deref() == Some(&challenge.token_binding);

        if !nonce_ok {
            // Nonce mismatch: mark invalid and return 400.
            proof::resolve_challenge(&conn, &req_challenge_id, "invalid")
                .map_err(ApiError::from)?;
            return Err(ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "requester_nonce does not match token binding".into(),
                www_authenticate: None,
            });
        }

        // Look up feed_url from the feeds table using challenge's feed_guid.
        let feed = db::get_feed_by_guid(&conn, &challenge.feed_guid)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: "feed not found in database".into(),
                www_authenticate: None,
            })?;

        Ok((
            challenge.feed_guid,
            challenge.scope,
            challenge.token_binding,
            feed.feed_url,
        ))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })??;

    let (feed_guid, scope, token_binding, feed_url) = phase1;

    // ── Phase 2 (async): fetch RSS and verify podcast:txt ─────────────────────

    // Issue-22 async DNS — 2026-03-13
    // SSRF guard: reject feed URLs targeting private/reserved IPs before fetching.
    // validate_feed_url uses std::net::ToSocketAddrs (blocking DNS), so we run
    // it inside spawn_blocking to avoid stalling the tokio worker thread.
    // CRIT-02 feature-gate — 2026-03-13
    #[cfg(feature = "test-util")]
    let skip_ssrf = state.skip_ssrf_validation;
    #[cfg(not(feature = "test-util"))]
    let skip_ssrf = false;

    if !skip_ssrf {
        let url_clone = feed_url.clone();
        tokio::task::spawn_blocking(move || proof::validate_feed_url(&url_clone))
            .await
            .map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("SSRF validation task failed: {e}"),
                www_authenticate: None,
            })?
            .map_err(|e| ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: format!("feed URL rejected: {e}"),
                www_authenticate: None,
            })?;
    }

    let rss_verified = proof::verify_podcast_txt(&state.push_client, &feed_url, &token_binding)
        .await
        .map_err(|e| ApiError {
            status:  StatusCode::SERVICE_UNAVAILABLE,
            message: format!("RSS verification failed: {e}"),
            www_authenticate: None,
        })?;

    // ── Phase 3 (blocking): resolve challenge and issue token ─────────────────
    let state3 = Arc::clone(&state);
    let challenge_id = req.challenge_id.clone();
    let feed_guid2 = feed_guid.clone();
    let scope2 = scope.clone();
    let phase1_feed_url = feed_url;

    let result = tokio::task::spawn_blocking(move || -> Result<ProofsAssertResponse, ApiError> {
        // Mutex safety compliant — 2026-03-12
        let conn = state3.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        if !rss_verified {
            proof::resolve_challenge(&conn, &challenge_id, "invalid")
                .map_err(ApiError::from)?;
            return Err(ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "token_binding not found in RSS podcast:txt".into(),
                www_authenticate: None,
            });
        }

        // Issue-PROOF-RACE — 2026-03-14
        // Re-read the feed URL and reject if it changed since phase 1.
        // A concurrent PATCH could have changed the URL between phase 1
        // (which read it) and now, meaning the RSS verification in phase 2
        // was performed against a URL that is no longer current.
        let current_feed = db::get_feed_by_guid(&conn, &feed_guid2)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: "feed not found in database".into(),
                www_authenticate: None,
            })?;
        if current_feed.feed_url != phase1_feed_url {
            return Err(ApiError {
                status:  StatusCode::CONFLICT,
                message: "feed URL changed during verification; retry".into(),
                www_authenticate: None,
            });
        }

        // Mark the challenge as valid. If rows == 0 the challenge was already
        // resolved by a concurrent request (TOCTOU between Phase 1 and Phase 3).
        let rows = proof::resolve_challenge(&conn, &challenge_id, "valid")
            .map_err(ApiError::from)?;
        if rows == 0 {
            return Err(ApiError {
                status:  StatusCode::BAD_REQUEST,
                message: "challenge already resolved (concurrent request)".into(),
                www_authenticate: None,
            });
        }

        // Issue an access token.
        let access_token = proof::issue_token(&conn, &scope2, &feed_guid2)
            .map_err(ApiError::from)?;

        // Compute expires_at for the response.
        let now = db::unix_now();
        let expires_at = now + PROOF_TOKEN_TTL_SECS;

        Ok(ProofsAssertResponse {
            access_token,
            scope:             scope2,
            subject_feed_guid: feed_guid2,
            expires_at,
        })
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    result.map(Json)
}

// ── PATCH /feeds/{guid} ────────────────────────────────────────────────────
// REST semantics compliant (RFC 7396) — 2026-03-12
// Issue-12 PATCH emits events — 2026-03-13
// Issue-13 PATCH 404 check — 2026-03-13

#[derive(Deserialize)]
struct PatchFeedRequest {
    feed_url: Option<String>,
}

#[expect(clippy::too_many_lines, reason = "event signing and fan-out follow the handle_retire_feed pattern")]
async fn handle_patch_feed(
    State(state): State<Arc<AppState>>,
    headers:      HeaderMap,
    Path(guid):   Path<String>,
    Json(req):    Json<PatchFeedRequest>,
) -> Result<StatusCode, ApiError> {
    let state2 = Arc::clone(&state);
    let guid2 = guid.clone();
    // Mutex safety compliant — 2026-03-12
    // Finding-2 atomic mutation+event — 2026-03-13
    let result = tokio::task::spawn_blocking(move || -> Result<Option<Vec<event::Event>>, ApiError> {
        let conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Auth inside lock scope: eliminates TOCTOU between auth check and DB write.
        check_admin_or_bearer_with_conn(&conn, &headers, &state2.admin_token, "feed:write", &guid2)?;

        // Issue-13 PATCH 404 check — 2026-03-13
        // Look up the feed — 404 if not found.
        db::get_feed_by_guid(&conn, &guid2)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: format!("feed {guid2} not found"),
                www_authenticate: None,
            })?;

        let Some(new_url) = &req.feed_url else {
            return Ok(None);
        };

        // Wrap mutation + event insert in a single transaction.
        let tx = conn.unchecked_transaction().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        tx.execute(
            "UPDATE feeds SET feed_url = ?1 WHERE feed_guid = ?2",
            params![new_url, guid2],
        )
        .map_err(|e| ApiError::from(db::DbError::from(e)))?;

        // Finding-6 token revocation on URL change — 2026-03-13
        // Existing tokens were proved against the OLD feed URL's podcast:txt.
        // After a URL change, the artist must re-prove ownership.
        crate::proof::revoke_tokens_for_feed(&tx, &guid2)
            .map_err(ApiError::from)?;

        // Issue-12 PATCH emits events — 2026-03-13
        // Re-read the feed after the update to capture current state.
        let feed = db::get_feed_by_guid(&tx, &guid2)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("feed {guid2} vanished after update"),
                www_authenticate: None,
            })?;

        // Look up the artist credit and artist for the event payload.
        let artist_credit = db::get_artist_credit(&tx, feed.artist_credit_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("artist credit {} not found for feed {guid2}", feed.artist_credit_id),
                www_authenticate: None,
            })?;

        let artist_id = artist_credit.names.first().map_or("", |n| n.artist_id.as_str());
        let artist = db::get_artist_by_id(&tx, artist_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("artist {artist_id} not found for feed {guid2}"),
                www_authenticate: None,
            })?;

        // Build and sign a FeedUpserted event.
        let now = db::unix_now();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event::FeedUpsertedPayload {
            feed,
            artist,
            artist_credit,
        };
        let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to serialize FeedUpserted payload: {e}"),
            www_authenticate: None,
        })?;
        let (signed_by, signature) = state2.signer.sign_event(
            &event_id,
            &event::EventType::FeedUpserted,
            &payload_json,
            &guid2,
            now,
        );

        let seq = db::insert_event(
            &tx,
            &event_id,
            &event::EventType::FeedUpserted,
            &payload_json,
            &guid2,
            &signed_by,
            &signature,
            now,
            &[],
        )
        .map_err(ApiError::from)?;

        tx.commit().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        // Build event for fan-out AFTER commit.
        let tagged = format!(r#"{{"type":"feed_upserted","data":{payload_json}}}"#);
        let ev_payload = serde_json::from_str::<event::EventPayload>(&tagged)
            .map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to deserialize FeedUpserted event for fan-out: {e}"),
                www_authenticate: None,
            })?;

        let fanout_event = event::Event {
            event_id,
            event_type:   event::EventType::FeedUpserted,
            payload:      ev_payload,
            subject_guid: guid2,
            signed_by,
            signature,
            seq,
            created_at:   now,
            warnings:     vec![],
            payload_json,
        };

        Ok(Some(vec![fanout_event]))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let fanout_events = result?;

    // Fire-and-forget fan-out.
    if let Some(events) = fanout_events
        && !events.is_empty()
    {
        // Issue-SSE-PUBLISH — 2026-03-14
        publish_events_to_sse(&state.sse_registry, &events);

        let db_fanout          = Arc::clone(&state.db);
        let client_fanout      = state.push_client.clone();
        let subscribers_fanout = Arc::clone(&state.push_subscribers);
        tokio::spawn(fan_out_push(db_fanout, client_fanout, subscribers_fanout, events));
    }

    Ok(StatusCode::NO_CONTENT)
}

// ── PATCH /tracks/{guid} ───────────────────────────────────────────────────
// REST semantics compliant (RFC 7396) — 2026-03-12
// Issue-12 PATCH emits events — 2026-03-13
// Issue-13 PATCH 404 check — 2026-03-13

#[derive(Deserialize)]
struct PatchTrackRequest {
    enclosure_url: Option<String>,
}

#[expect(clippy::too_many_lines, reason = "event signing and fan-out follow the handle_retire_feed pattern")]
async fn handle_patch_track(
    State(state): State<Arc<AppState>>,
    headers:      HeaderMap,
    Path(guid):   Path<String>,
    Json(req):    Json<PatchTrackRequest>,
) -> Result<StatusCode, ApiError> {
    let state2 = Arc::clone(&state);
    let guid2 = guid.clone();
    // Mutex safety compliant — 2026-03-12
    // Finding-2 atomic mutation+event — 2026-03-13
    let result = tokio::task::spawn_blocking(move || -> Result<Option<Vec<event::Event>>, ApiError> {
        let conn = state2.db.lock().map_err(|_poison| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: "database mutex poisoned".into(),
            www_authenticate: None,
        })?;

        // Auth inside lock scope: eliminates TOCTOU between auth check and DB write.
        // Look up the track first to find its parent feed guid, then validate
        // the bearer token against that feed.
        // Issue-13 PATCH 404 check — 2026-03-13
        let track = db::get_track_by_guid(&conn, &guid2)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::NOT_FOUND,
                message: format!("track {guid2} not found"),
                www_authenticate: None,
            })?;

        check_admin_or_bearer_with_conn(
            &conn, &headers, &state2.admin_token, "feed:write", &track.feed_guid,
        )?;

        let Some(new_url) = &req.enclosure_url else {
            return Ok(None);
        };

        // Wrap mutation + event insert in a single transaction.
        let tx = conn.unchecked_transaction().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        tx.execute(
            "UPDATE tracks SET enclosure_url = ?1 WHERE track_guid = ?2",
            params![new_url, guid2],
        )
        .map_err(|e| ApiError::from(db::DbError::from(e)))?;

        // Issue-12 PATCH emits events — 2026-03-13
        // Re-read the track after the update to capture current state.
        let updated_track = db::get_track_by_guid(&tx, &guid2)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("track {guid2} vanished after update"),
                www_authenticate: None,
            })?;

        // Look up the artist credit for the event payload.
        let artist_credit = db::get_artist_credit(&tx, updated_track.artist_credit_id)
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("artist credit {} not found for track {guid2}", updated_track.artist_credit_id),
                www_authenticate: None,
            })?;

        // Look up payment routes and value-time splits.
        let routes = db::get_payment_routes_for_track(&tx, &guid2)
            .map_err(ApiError::from)?;
        let value_time_splits = db::get_value_time_splits_for_track(&tx, &guid2)
            .map_err(ApiError::from)?;

        // Build and sign a TrackUpserted event.
        let now = db::unix_now();
        let event_id = uuid::Uuid::new_v4().to_string();
        let payload = event::TrackUpsertedPayload {
            track:             updated_track,
            routes,
            value_time_splits,
            artist_credit,
        };
        let payload_json = serde_json::to_string(&payload).map_err(|e| ApiError {
            status:  StatusCode::INTERNAL_SERVER_ERROR,
            message: format!("failed to serialize TrackUpserted payload: {e}"),
            www_authenticate: None,
        })?;
        let (signed_by, signature) = state2.signer.sign_event(
            &event_id,
            &event::EventType::TrackUpserted,
            &payload_json,
            &guid2,
            now,
        );

        let seq = db::insert_event(
            &tx,
            &event_id,
            &event::EventType::TrackUpserted,
            &payload_json,
            &guid2,
            &signed_by,
            &signature,
            now,
            &[],
        )
        .map_err(ApiError::from)?;

        tx.commit().map_err(|e| ApiError::from(db::DbError::from(e)))?;

        // Build event for fan-out AFTER commit.
        let tagged = format!(r#"{{"type":"track_upserted","data":{payload_json}}}"#);
        let ev_payload = serde_json::from_str::<event::EventPayload>(&tagged)
            .map_err(|e| ApiError {
                status:  StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("failed to deserialize TrackUpserted event for fan-out: {e}"),
                www_authenticate: None,
            })?;

        let fanout_event = event::Event {
            event_id,
            event_type:   event::EventType::TrackUpserted,
            payload:      ev_payload,
            subject_guid: guid2,
            signed_by,
            signature,
            seq,
            created_at:   now,
            warnings:     vec![],
            payload_json,
        };

        Ok(Some(vec![fanout_event]))
    })
    .await
    .map_err(|e| ApiError {
        status:  StatusCode::INTERNAL_SERVER_ERROR,
        message: format!("internal task panic: {e}"),
        www_authenticate: None,
    })?;

    let fanout_events = result?;

    // Fire-and-forget fan-out.
    if let Some(events) = fanout_events
        && !events.is_empty()
    {
        // Issue-SSE-PUBLISH — 2026-03-14
        publish_events_to_sse(&state.sse_registry, &events);

        let db_fanout          = Arc::clone(&state.db);
        let client_fanout      = state.push_client.clone();
        let subscribers_fanout = Arc::clone(&state.push_subscribers);
        tokio::spawn(fan_out_push(db_fanout, client_fanout, subscribers_fanout, events));
    }

    Ok(StatusCode::NO_CONTENT)
}
