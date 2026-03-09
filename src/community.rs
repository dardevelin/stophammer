// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Community node mode: syncs events from a primary node and serves a
//! read-only API with a push-receive endpoint.
//!
//! On startup the community node:
//! 1. Registers itself with the Cloudflare tracker (fire-and-forget).
//! 2. Registers its push URL with the primary (`POST /sync/register`).
//! 3. Restores its `last_seq` cursor from the local DB.
//! 4. Runs a poll-loop fallback: polls the primary only when no push has
//!    been received for `push_timeout_secs` (default 90s).
//!
//! The push handler (`POST /sync/push`) is served on the same port and
//! updates `last_push_at` so the poll-loop stays quiet while pushes arrive.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, routing::post, Json, Router};
use serde::Serialize;

use crate::{apply, db, sync};

// ── CommunityConfig ──────────────────────────────────────────────────────────

/// Runtime configuration for a community (read-only replica) node.
///
/// This struct has four fields. The M-DESIGN-FOR-AI guideline recommends a
/// builder pattern for types with four or more constructor parameters, but
/// that applies to library APIs consumed by external callers. Here the struct
/// is constructed exactly once in `main.rs` from environment variables; a
/// builder would add boilerplate with no safety or usability benefit for an
/// application binary. Plain struct initialisation is the idiomatic choice.
pub struct CommunityConfig {
    /// Base URL of the primary node, e.g. `"http://primary.example.com:8008"`.
    pub primary_url: String,
    /// Base URL of the tracker, e.g. `"https://stophammer-tracker.workers.dev"`.
    pub tracker_url: String,
    /// This node's public address, e.g. `"http://mynode.example.com:8008"`.
    pub node_address: String,
    /// Seconds between poll-loop iterations. Default: 300.
    pub poll_interval_secs: u64,
    /// Seconds of silence before the fallback poll fires. Default: 90.
    pub push_timeout_secs: i64,
}

// ── CommunityState ───────────────────────────────────────────────────────────

/// Shared state for the push-receive endpoint.
pub struct CommunityState {
    /// Local database handle.
    pub db:                 db::Db,
    /// Hex-encoded ed25519 public key of the authoritative primary node.
    pub primary_pubkey_hex: String,
    /// Unix timestamp (seconds) of the last successfully received push.
    /// Stored as i64 with `Relaxed` ordering (monotonic read, no cross-thread
    /// happens-before needed — a stale read at most delays one poll cycle).
    pub last_push_at:       Arc<AtomicI64>,
}

// ── Tracker registration body ────────────────────────────────────────────────

#[derive(Serialize)]
struct RegisterBody<'a> {
    pubkey:  &'a str,
    address: &'a str,
}

// ── run_community_sync ───────────────────────────────────────────────────────

/// Spawn the background sync task. Returns immediately; the task runs until
/// the process exits.
///
/// `pubkey_hex` is the hex-encoded ed25519 pubkey of this node's key, used
/// as the cursor identity in `node_sync_state` and in tracker registration.
pub async fn run_community_sync(
    config:       CommunityConfig,
    db:           db::Db,
    pubkey_hex:   String,
    last_push_at: Arc<AtomicI64>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client");

    // 1. Fire-and-forget tracker registration.
    register_with_tracker(&client, &config.tracker_url, &pubkey_hex, &config.node_address).await;

    // 2. Register push endpoint with the primary.
    register_with_primary(
        &client,
        &config.primary_url,
        &pubkey_hex,
        &config.node_address,
    ).await;

    // 3. Load persisted cursor.
    let initial_seq = {
        let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        match db::get_node_sync_cursor(&conn, &pubkey_hex) {
            Ok(seq) => seq,
            Err(e) => {
                eprintln!("[community] failed to read sync cursor: {e}; starting from 0");
                0
            }
        }
    };

    let mut last_seq = initial_seq;
    println!("[community] sync started — primary={} cursor={last_seq}", config.primary_url);

    // 4. Poll-loop fallback.
    //
    // Yield strategy (M-YIELD-POINTS): each iteration contains at least one
    // async await that surrenders control to the runtime:
    //   - `poll_once` issues an HTTP request via reqwest (I/O yield).
    //   - `apply::apply_events` dispatches each DB write via `spawn_blocking`.
    //   - `tokio::time::sleep` at the bottom yields for the configured interval.
    loop {
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .cast_signed();

        let secs_since_push = now_secs - last_push_at.load(Ordering::Relaxed);
        if secs_since_push > config.push_timeout_secs {
            println!("[community] fallback poll (no push for {secs_since_push}s)");
            match poll_once(&client, &config.primary_url, last_seq).await {
                Err(e) => {
                    eprintln!("[community] poll error: {e}");
                }
                Ok(response) => {
                    let fetched = response.events.len();
                    if fetched > 0 {
                        let summary =
                            apply::apply_events(Arc::clone(&db), &pubkey_hex, response.events)
                                .await;
                        if summary.applied > 0 {
                            // Advance last_seq from the primary's seq values.
                            let new_seq = {
                                let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                                db::get_node_sync_cursor(&conn, &pubkey_hex).unwrap_or(last_seq)
                            };
                            if new_seq > last_seq {
                                last_seq = new_seq;
                            }
                            println!(
                                "[community] poll applied {}/{} events — cursor now {last_seq}",
                                summary.applied, fetched
                            );
                        }
                    }
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
    }
}

// ── fetch_primary_pubkey ─────────────────────────────────────────────────────

/// Fetches the primary node's pubkey from `GET {primary_url}/node/info`.
///
/// Retries up to `max_attempts` times with a 2-second delay — the primary
/// may still be starting when the community node boots. Returns `None` if
/// all attempts fail (caller falls back to the configured value).
pub async fn fetch_primary_pubkey(
    client:       &reqwest::Client,
    primary_url:  &str,
    max_attempts: u32,
) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct NodeInfo { node_pubkey: String }

    let url = format!("{primary_url}/node/info");
    for attempt in 1..=max_attempts {
        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(info) = resp.json::<NodeInfo>().await {
                    return Some(info.node_pubkey);
                }
            }
            _ => {}
        }
        if attempt < max_attempts {
            eprintln!("[community] waiting for primary node/info (attempt {attempt}/{max_attempts})…");
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    eprintln!("[community] could not fetch primary pubkey from {url}; using configured value");
    None
}

// ── register_with_tracker ────────────────────────────────────────────────────

async fn register_with_tracker(
    client:       &reqwest::Client,
    tracker_url:  &str,
    pubkey_hex:   &str,
    node_address: &str,
) {
    let url  = format!("{tracker_url}/nodes/register");
    let body = RegisterBody { pubkey: pubkey_hex, address: node_address };

    match client.post(&url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            println!("[community] registered with tracker at {tracker_url}");
        }
        Ok(resp) => {
            eprintln!(
                "[community] tracker registration returned HTTP {}: ignored",
                resp.status()
            );
        }
        Err(e) => {
            eprintln!("[community] tracker registration failed (ignoring): {e}");
        }
    }
}

// ── register_with_primary ────────────────────────────────────────────────────

/// Announces this node's push URL to the primary via `POST /sync/register`.
///
/// Errors are logged and swallowed — the poll-loop fallback handles catch-up
/// when the primary is unreachable at startup.
async fn register_with_primary(
    client:       &reqwest::Client,
    primary_url:  &str,
    pubkey_hex:   &str,
    node_address: &str,
) {
    let url  = format!("{primary_url}/sync/register");
    let body = sync::RegisterRequest {
        node_pubkey: pubkey_hex.to_string(),
        node_url:    format!("{node_address}/sync/push"),
    };

    match client.post(&url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            println!("[community] registered push endpoint with primary at {primary_url}");
        }
        Ok(resp) => {
            eprintln!(
                "[community] primary registration returned HTTP {}: ignored",
                resp.status()
            );
        }
        Err(e) => {
            eprintln!("[community] primary registration failed (ignoring): {e}");
        }
    }
}

// ── poll_once ────────────────────────────────────────────────────────────────

async fn poll_once(
    client:      &reqwest::Client,
    primary_url: &str,
    after_seq:   i64,
) -> Result<crate::sync::SyncEventsResponse, String> {
    let url = format!("{primary_url}/sync/events?after_seq={after_seq}&limit=500");

    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        return Err(format!("GET {url} returned HTTP {status}"));
    }

    resp.json::<crate::sync::SyncEventsResponse>()
        .await
        .map_err(|e| format!("failed to deserialise sync response: {e}"))
}

// ── build_community_push_router ──────────────────────────────────────────────

/// Builds the router for the community push endpoint.
///
/// Merged into the read-only router in `main.rs` so that community nodes
/// serve both `GET /sync/events` (via readonly router) and `POST /sync/push`.
pub fn build_community_push_router(state: Arc<CommunityState>) -> Router {
    Router::new()
        .route("/sync/push", post(handle_sync_push))
        .with_state(state)
}

// ── POST /sync/push ──────────────────────────────────────────────────────────

// Flow: verify each event signature → apply idempotently → update last_push_at
// → return counts.
async fn handle_sync_push(
    State(state): State<Arc<CommunityState>>,
    Json(req): Json<sync::PushRequest>,
) -> Json<sync::PushResponse> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();

    // Filter to events signed by the known primary pubkey before applying.
    // Events with unexpected signers are counted as rejected.
    let mut pre_rejected = 0usize;
    let trusted: Vec<crate::event::Event> = req.events.into_iter().filter(|ev| {
        if ev.signed_by == state.primary_pubkey_hex {
            true
        } else {
            eprintln!(
                "[push] event {} signed by unknown key {}; expected {}",
                ev.event_id, ev.signed_by, state.primary_pubkey_hex
            );
            pre_rejected += 1;
            false
        }
    }).collect();

    // Signature verification + DB apply happens inside apply_events.
    // Cursor is keyed on the primary pubkey, consistent with the poll-loop.
    let summary = apply::apply_events(
        Arc::clone(&state.db),
        &state.primary_pubkey_hex,
        trusted,
    ).await;

    if summary.applied > 0 || summary.duplicate > 0 {
        state.last_push_at.store(now, Ordering::Relaxed);
    }

    if summary.applied > 0 || summary.rejected > 0 || pre_rejected > 0 {
        println!(
            "[push] applied={} duplicate={} rejected={}",
            summary.applied,
            summary.duplicate,
            summary.rejected + pre_rejected,
        );
    }

    Json(sync::PushResponse {
        applied:   summary.applied,
        rejected:  summary.rejected + pre_rejected,
        duplicate: summary.duplicate,
    })
}

