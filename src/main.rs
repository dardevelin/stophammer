//! Stophammer binary entry point.
//!
//! Selects primary or community mode based on the `NODE_MODE` environment
//! variable, initialises the database, signing key, and rate limiter, then
//! serves the appropriate Axum router over plain HTTP or ACME-provisioned TLS.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use stophammer::{api, community, db, proof, signing, tls, verify};

#[tokio::main]
async fn main() {
    // FG-01 structured logging — 2026-03-13
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stophammer=info".parse().expect("valid filter")),
        )
        .init();
    let db_path   = std::env::var("DB_PATH").unwrap_or_else(|_| "stophammer.db".into());
    let key_path  = std::env::var("KEY_PATH").unwrap_or_else(|_| "signing.key".into());
    let bind_addr = std::env::var("BIND").unwrap_or_else(|_| "0.0.0.0:8008".into());
    let node_mode = std::env::var("NODE_MODE").unwrap_or_else(|_| "primary".into());

    let conn   = db::open_db(&db_path);
    let db     = std::sync::Arc::new(std::sync::Mutex::new(conn));
    let signer = signing::NodeSigner::load_or_create(&key_path).expect("failed to load signing key");
    let pubkey = signer.pubkey_hex().to_string();

    // SP-02 pruner interval — 2026-03-13
    let prune_interval = proof::prune_interval_from_env();
    spawn_proof_pruner(std::sync::Arc::clone(&db), prune_interval);

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
    // Finding-3 separate sync token — 2026-03-13
    let sync_token  = std::env::var("SYNC_TOKEN").ok().filter(|s| !s.is_empty());
    let chain       = verify::build_chain(&verify::ChainSpec::from_env(), crawl_token);

    // Seed push_subscribers from DB at startup.
    // Mutex safety compliant — 2026-03-12
    let push_subscribers = {
        let conn  = db.lock().expect("db mutex poisoned at startup");
        let peers = db::get_push_peers(&conn).unwrap_or_default();
        drop(conn);
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
        // Finding-3 separate sync token — 2026-03-13
        sync_token,
        push_client,
        push_subscribers,
        sse_registry:     std::sync::Arc::new(api::SseRegistry::new()),
        #[cfg(feature = "test-util")]
        skip_ssrf_validation: false,
    });

    let router = api::build_router(state);
    serve_with_optional_tls(router, &bind_addr).await;
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
        // Issue-4 HTTPS pubkey discovery — 2026-03-13
        community::require_https_for_discovery(&primary_url)
            .expect("HTTPS required for pubkey auto-discovery");

        let discovery_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build discovery client");
        // SP-06 startup guard — 2026-03-12
        community::fetch_primary_pubkey(&discovery_client, &primary_url, 10)
            .await
            .expect(
                "FATAL: cannot determine primary node public key. Set PRIMARY_PUBKEY env var \
                 with the hex pubkey of the primary node, or ensure the primary is reachable \
                 at TRACKER_URL."
            )
    };

    // Issue-SSE-PUBLISH — 2026-03-14: share a single SSE registry between the
    // readonly router and the community push handler so pushed events are
    // visible to SSE clients.
    let shared_sse_registry = std::sync::Arc::new(api::SseRegistry::new());

    let community_state = std::sync::Arc::new(community::CommunityState {
        db:                 std::sync::Arc::clone(&db),
        primary_pubkey_hex: primary_pubkey_hex.clone(),
        last_push_at:       std::sync::Arc::clone(&last_push_at),
        sse_registry:       Some(std::sync::Arc::clone(&shared_sse_registry)),
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
    let sse_for_sync    = Some(std::sync::Arc::clone(&shared_sse_registry));
    drop(tokio::spawn(community::run_community_sync(
        config,
        db_for_sync,
        pubkey_for_sync,
        lpa_for_sync,
        sse_for_sync,
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
        // Finding-3 separate sync token — 2026-03-13
        sync_token:       None,
        push_client,
        push_subscribers: std::sync::Arc::new(std::sync::RwLock::new(
            std::collections::HashMap::new(),
        )),
        sse_registry:     shared_sse_registry,
        #[cfg(feature = "test-util")]
        skip_ssrf_validation: false,
    });

    let router = api::build_readonly_router(readonly_state)
        .merge(community::build_community_push_router(community_state));

    serve_with_optional_tls(router, &bind_addr).await;
}

// ── Proof expiry pruner ──────────────────────────────────────────────────

// SP-02 pruner interval — 2026-03-13
fn spawn_proof_pruner(db: db::Db, interval_secs: u64) {
    drop(tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            let Ok(conn) = db.lock() else {
                tracing::error!("proof-pruner: db mutex poisoned, stopping pruner");
                break;
            };
            match proof::prune_expired(&conn) {
                Ok(0) => {}
                Ok(n) => tracing::debug!(pruned = n, "proof-pruner: pruned expired proof rows"),
                Err(e) => tracing::error!(error = %e, "proof-pruner: prune error"),
            }
        }
    }));
}

// ── SP-03 rate limiting middleware — 2026-03-13 ─────────────────────────────

/// Wraps the router with per-IP token-bucket rate limiting.
///
/// Rate limiting is applied here (not inside `build_router`) so that
/// `tower::ServiceExt::oneshot` tests are not affected by the middleware.
///
/// Client IP extraction strategy:
/// - If `TRUST_PROXY=true`, use `X-Forwarded-For` (first hop) with
///   `ConnectInfo<SocketAddr>` fallback.
/// - Otherwise (default), use only `ConnectInfo<SocketAddr>`, ignoring
///   `X-Forwarded-For` to prevent spoofing by direct clients.
fn apply_rate_limit(router: axum::Router) -> axum::Router {
    use std::sync::Arc;

    let (rps, burst) = api::rate_limit_config();
    let limiter = Arc::new(api::build_rate_limiter(rps, burst));
    let trust_proxy = std::env::var("TRUST_PROXY")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    router.layer(axum::middleware::from_fn(
        move |request: axum::http::Request<axum::body::Body>,
              next: axum::middleware::Next| {
            let limiter = Arc::clone(&limiter);
            async move {
                // Skip rate limiting for /health
                if request.uri().path() == "/health" {
                    return next.run(request).await;
                }

                let ip = if trust_proxy {
                    // Behind a trusted reverse proxy: use X-Forwarded-For.
                    request
                        .headers()
                        .get("x-forwarded-for")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.split(',').next())
                        .map(|s| s.trim().to_string())
                        .or_else(|| {
                            request
                                .extensions()
                                .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                                .map(|ci| ci.0.ip().to_string())
                        })
                        .unwrap_or_else(|| "unknown".to_string())
                } else {
                    // Direct exposure: ignore X-Forwarded-For (prevents spoofing).
                    request
                        .extensions()
                        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                        .map_or_else(|| "unknown".to_string(), |ci| ci.0.ip().to_string())
                };

                match limiter.check_key(&ip) {
                    Ok(()) => next.run(request).await,
                    Err(_) => axum::http::Response::builder()
                        .status(axum::http::StatusCode::TOO_MANY_REQUESTS)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(
                            r#"{"error":"rate limit exceeded"}"#,
                        ))
                        .expect("static response is valid"),
                }
            }
        },
    ))
}

// ── TLS / plain HTTP decision ────────────────────────────────────────────────

async fn serve_with_optional_tls(router: axum::Router, bind_addr: &str) {
    // SP-03 rate limiting — 2026-03-13
    let router = apply_rate_limit(router);

    if let Ok(tls_domain) = std::env::var("TLS_DOMAIN") {
        let acme_email = std::env::var("TLS_ACME_EMAIL")
            .expect("TLS_ACME_EMAIL required when TLS_DOMAIN is set");
        let cert_path = std::env::var("TLS_CERT_PATH")
            .unwrap_or_else(|_| "./tls/cert.pem".into());
        let key_path = std::env::var("TLS_KEY_PATH")
            .unwrap_or_else(|_| "./tls/key.pem".into());
        let staging = std::env::var("TLS_ACME_STAGING")
            .map(|v| v == "true")
            .unwrap_or(false);

        let acme_account_path = std::env::var("TLS_ACME_ACCOUNT_PATH")
            .unwrap_or_else(|_| "./tls/acme-account.json".into());

        let acme_directory_url = std::env::var("TLS_ACME_DIRECTORY_URL")
            .ok()
            .filter(|s| !s.is_empty());

        let config = tls::TlsConfig {
            domain: tls_domain,
            acme_email,
            cert_path: cert_path.clone().into(),
            key_path: key_path.clone().into(),
            acme_account_path: acme_account_path.into(),
            staging,
            acme_directory_url,
        };

        if tls::cert_needs_renewal(&config.cert_path) {
            tls::provision_certificate(&config)
                .await
                .expect("ACME provisioning failed — cannot start in TLS mode");
        }
        tls::spawn_renewal_task(config);

        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path)
                .await
                .expect("failed to load TLS cert/key");

        let addr: std::net::SocketAddr = bind_addr
            .parse()
            .expect("BIND must be a valid socket address");

        // Issue-15 expect messages — 2026-03-13
        tracing::info!(bind = %bind_addr, "stophammer listening (HTTPS)");
        axum_server::bind_rustls(addr, rustls_config)
            .serve(router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .expect("failed to bind/serve HTTP — port may be in use or permission denied");
    } else {
        // Plain HTTP fallback.
        tracing::warn!("TLS_DOMAIN not set — node is serving plain HTTP. Bearer tokens and crawl tokens are transmitted unencrypted. Set TLS_DOMAIN and TLS_ACME_EMAIL for production use.");

        // Issue-15 expect messages — 2026-03-13
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("failed to bind/serve HTTP — port may be in use or permission denied");
        tracing::info!(bind = %bind_addr, "stophammer listening (plain HTTP)");
        axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .expect("failed to bind/serve HTTP — port may be in use or permission denied");
    }
}
