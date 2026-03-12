#![warn(clippy::pedantic)]

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod api;
mod apply;
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
    let admin_token = std::env::var("ADMIN_TOKEN").unwrap_or_default();
    let chain       = verify::build_chain(&verify::ChainSpec::from_env(), crawl_token);

    // Seed push_subscribers from DB at startup.
    let push_subscribers = {
        let conn  = db.lock().unwrap();
        let peers = db::get_push_peers(&conn).unwrap_or_default();
        let map: std::collections::HashMap<String, String> =
            peers.into_iter().map(|p| (p.node_pubkey, p.node_url)).collect();
        std::sync::Arc::new(std::sync::RwLock::new(map))
    };

    let push_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build push HTTP client");

    let state = std::sync::Arc::new(api::AppState {
        db,
        chain:            std::sync::Arc::new(chain),
        signer:           std::sync::Arc::new(signer),
        node_pubkey_hex:  pubkey,
        admin_token,
        push_client,
        push_subscribers,
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
        .unwrap_or(300);
    let push_timeout_secs: i64 = std::env::var("PUSH_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(90);

    // Shared atomic timestamp updated by the push handler.
    let last_push_at = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

    // Auto-discover the primary's signing pubkey if PRIMARY_PUBKEY is not
    // explicitly configured. Fetches GET {PRIMARY_URL}/node/info with retries
    // so the community node works without any manual key distribution.
    let primary_pubkey_hex = if let Ok(pk) = std::env::var("PRIMARY_PUBKEY") {
        pk
    } else {
        let discovery_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build discovery client");
        let primary_url_for_discovery = std::env::var("PRIMARY_URL")
            .expect("PRIMARY_URL env var required in community mode");
        community::fetch_primary_pubkey(&discovery_client, &primary_url_for_discovery, 10)
            .await
            .unwrap_or_else(|| {
                eprintln!("[community] WARNING: using own pubkey as primary — push events will be rejected");
                pubkey.clone()
            })
    };

    let community_state = std::sync::Arc::new(community::CommunityState {
        db:                 std::sync::Arc::clone(&db),
        primary_pubkey_hex: primary_pubkey_hex.clone(),
        last_push_at:       std::sync::Arc::clone(&last_push_at),
    });

    let config = community::CommunityConfig {
        primary_url,
        tracker_url,
        node_address,
        poll_interval_secs,
        push_timeout_secs,
    };

    // Fire-and-forget sync task.
    let db_for_sync     = std::sync::Arc::clone(&db);
    let pubkey_for_sync = pubkey.clone();
    let lpa_for_sync    = std::sync::Arc::clone(&last_push_at);
    drop(tokio::spawn(community::run_community_sync(
        config,
        db_for_sync,
        pubkey_for_sync,
        lpa_for_sync,
    )));

    // Build merged router: readonly events API + push receiver.
    let dummy_chain = verify::VerifierChain::new(vec![]);
    let push_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("failed to build push HTTP client");
    let readonly_state = std::sync::Arc::new(api::AppState {
        db,
        chain:            std::sync::Arc::new(dummy_chain),
        signer:           std::sync::Arc::new(signer),
        node_pubkey_hex:  pubkey,
        admin_token:      String::new(),
        push_client,
        push_subscribers: std::sync::Arc::new(std::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
    });

    let router = api::build_readonly_router(readonly_state)
        .merge(community::build_community_push_router(community_state));

    let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
    println!("stophammer community listening on {bind_addr}");
    axum::serve(listener, router).await.unwrap();
}
