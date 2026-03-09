#![warn(clippy::pedantic)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod api;
mod community;
mod db;
mod event;
mod ingest;
mod model;
mod signing;
mod sync;
mod verify;
mod verifiers;

#[tokio::main]
async fn main() {
    let db_path   = std::env::var("DB_PATH").unwrap_or_else(|_| "stophammer.db".into());
    let key_path  = std::env::var("KEY_PATH").unwrap_or_else(|_| "signing.key".into());
    let bind_addr = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8008".into());
    let node_mode = std::env::var("NODE_MODE").unwrap_or_else(|_| "primary".into());

    let conn   = db::open_db(&db_path);
    let db     = std::sync::Arc::new(std::sync::Mutex::new(conn));
    let signer = signing::NodeSigner::load_or_create(&key_path).expect("failed to load signing key");
    let pubkey = signer.pubkey_hex().to_string();

    match node_mode.as_str() {
        "community" => run_community(db, signer, pubkey, bind_addr).await,
        _           => run_primary(db, signer, pubkey, bind_addr).await,
    }
}

// ── Primary mode ─────────────────────────────────────────────────────────────

async fn run_primary(
    db:         db::Db,
    signer:     signing::NodeSigner,
    pubkey:     String,
    bind_addr:  String,
) {
    let crawl_token = std::env::var("CRAWL_TOKEN").expect("CRAWL_TOKEN env var required");
    // ADMIN_TOKEN is optional: if absent, all admin endpoints return 403.
    let admin_token = std::env::var("ADMIN_TOKEN").unwrap_or_default();
    let chain = verify::build_chain(&verify::ChainSpec::from_env(), crawl_token);

    let state = std::sync::Arc::new(api::AppState {
        db,
        chain:           std::sync::Arc::new(chain),
        signer:          std::sync::Arc::new(signer),
        node_pubkey_hex: pubkey,
        admin_token,
    });

    let router   = api::build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    println!("stophammer primary listening on {bind_addr}");
    axum::serve(listener, router).await.unwrap();
}

// ── Community mode ───────────────────────────────────────────────────────────

async fn run_community(
    db:        db::Db,
    signer:    signing::NodeSigner,
    pubkey:    String,
    bind_addr: String,
) {
    let primary_url = std::env::var("PRIMARY_URL")
        .expect("PRIMARY_URL env var required in community mode");
    let tracker_url = std::env::var("TRACKER_URL")
        .unwrap_or_else(|_| "https://stophammer-tracker.workers.dev".into());
    let node_address = std::env::var("NODE_ADDRESS")
        .expect("NODE_ADDRESS env var required in community mode");
    let poll_interval_secs: u64 = std::env::var("POLL_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let config = community::CommunityConfig {
        primary_url,
        tracker_url,
        node_address,
        poll_interval_secs,
    };

    // Fire-and-forget: the sync task runs for the lifetime of the process.
    // The JoinHandle is intentionally dropped — a sync failure is non-fatal
    // because the task's own loop logs errors and continues.
    let db_for_sync = std::sync::Arc::clone(&db);
    let pubkey_for_sync = pubkey.clone();
    drop(tokio::spawn(community::run_community_sync(config, db_for_sync, pubkey_for_sync)));

    // Serve the read-only API (no ingest, no reconcile write-path).
    // We still need an AppState; the signer is present but will never be called
    // because the read-only router does not expose ingest or reconcile routes.
    // Empty chain: community nodes do not run ingest, so no verifiers are needed.
    let dummy_chain = verify::VerifierChain::new(vec![]);
    let state = std::sync::Arc::new(api::AppState {
        db,
        chain:           std::sync::Arc::new(dummy_chain),
        signer:          std::sync::Arc::new(signer),
        node_pubkey_hex: pubkey,
        // Community nodes do not expose admin routes — empty token means 403 always.
        admin_token:     String::new(),
    });

    let router   = api::build_readonly_router(state);
    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    println!("stophammer community listening on {bind_addr}");
    axum::serve(listener, router).await.unwrap();
}
