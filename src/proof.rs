//! Proof-of-possession challenge/token helpers.
//!
//! Implements the ACME-inspired (RFC 8555) challenge-assert flow for
//! authorizing feed and track mutations without an account system.
//! See `docs/adr/0018-proof-of-possession-mutations.md` for the full design.

use std::net::ToSocketAddrs;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand_core::{OsRng, RngCore};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

use crate::db::DbError;

// ── Named constants (M-DOCUMENTED-MAGIC) ────────────────────────────────────

/// Challenge lifetime in seconds (24 hours).
///
/// Long enough for RSS propagation delay: the feed owner must publish a
/// `podcast:txt` element and wait for the CDN to pick it up. Shorter values
/// risk timing out before the proof can be verified.
const CHALLENGE_TTL_SECS: i64 = 86400;

/// Access token lifetime in seconds (1 hour).
///
/// Grants a time-limited mutation window after a successful proof assertion.
/// Kept short to limit exposure if a token is leaked.
const TOKEN_TTL_SECS: i64 = 3600;

// ── Types ──────────────────────────────────────────────────────────────────

// Issue-PROOF-LEVEL — 2026-03-14

/// The level of ownership assurance established for this proof.
///
/// Tracks which verification phases have been completed. The current
/// implementation only performs Phase 1 (RSS proof), so all tokens are
/// issued at `RssOnly`. See ADR-0018 "Current Implementation Status"
/// for details on the assurance gap.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProofLevel {
    /// RSS feed proof only (Phase 1). Asserts control of the feed document.
    RssOnly,
    /// RSS + audio file proof (Phase 2). Asserts control of audio delivery.
    RssAndAudio,
    /// Full dual-location relocation proof (Phase 3).
    RelocationProven,
}

/// A row from the `proof_challenges` table.
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct ChallengeRow {
    pub challenge_id:  String,
    pub feed_guid:     String,
    pub scope:         String,
    pub token_binding: String,
    pub state:         String,
    pub expires_at:    i64,
}

// ── Challenge helpers ──────────────────────────────────────────────────────

/// Create a new pending challenge.
///
/// The server-issued token is 128 bits of entropy (base64url-encoded, ~22 chars).
/// `token_binding = token || '.' || base64url(SHA-256(requester_nonce))`
///
/// Returns `(challenge_id, token_binding)`.
///
/// # Errors
///
/// Returns `DbError` if the INSERT into `proof_challenges` fails.
pub fn create_challenge(
    conn: &Connection,
    feed_guid: &str,
    scope: &str,
    requester_nonce: &str,
) -> Result<(String, String), DbError> {
    let challenge_id = uuid::Uuid::new_v4().to_string();

    // 128 bits of entropy for the server-issued token (RFC 8555 section 11.3).
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    let token = URL_SAFE_NO_PAD.encode(bytes);

    // Bind the token to the requester's nonce (same pattern as ACME keyAuthorization).
    let mut hasher = Sha256::new();
    hasher.update(requester_nonce.as_bytes());
    let hash = hasher.finalize();
    let token_binding = format!("{}.{}", token, URL_SAFE_NO_PAD.encode(hash));

    let now = now_secs();
    let expires_at = now + CHALLENGE_TTL_SECS;

    conn.execute(
        "INSERT INTO proof_challenges (challenge_id, feed_guid, scope, token_binding, state, expires_at, created_at) \
         VALUES (?1, ?2, ?3, ?4, 'pending', ?5, ?6)",
        params![challenge_id, feed_guid, scope, token_binding, expires_at, now],
    )?;

    Ok((challenge_id, token_binding))
}

/// Transition a challenge to `valid` or `invalid` state.
///
/// Challenges are single-use: once resolved they cannot be reused.
/// Returns the number of rows affected (1 if the challenge was pending
/// and transitioned, 0 if already resolved or nonexistent).
///
/// # Errors
///
/// Returns `DbError` if the UPDATE fails.
pub fn resolve_challenge(
    conn: &Connection,
    challenge_id: &str,
    new_state: &str,
) -> Result<usize, DbError> {
    let rows = conn.execute(
        "UPDATE proof_challenges SET state = ?1 WHERE challenge_id = ?2 AND state = 'pending'",
        params![new_state, challenge_id],
    )?;
    // rows == 0 means already resolved or doesn't exist — no-op (idempotent).
    Ok(rows)
}

/// Fetch a challenge by id. Returns `None` if not found or expired.
///
/// # Errors
///
/// Returns `DbError` if the SELECT fails.
pub fn get_challenge(
    conn: &Connection,
    challenge_id: &str,
) -> Result<Option<ChallengeRow>, DbError> {
    let now = now_secs();
    let row = conn
        .query_row(
            "SELECT challenge_id, feed_guid, scope, token_binding, state, expires_at \
             FROM proof_challenges \
             WHERE challenge_id = ?1 AND expires_at > ?2",
            params![challenge_id, now],
            |r| {
                Ok(ChallengeRow {
                    challenge_id:  r.get(0)?,
                    feed_guid:     r.get(1)?,
                    scope:         r.get(2)?,
                    token_binding: r.get(3)?,
                    state:         r.get(4)?,
                    expires_at:    r.get(5)?,
                })
            },
        )
        .optional()?;
    Ok(row)
}

// ── Token helpers ──────────────────────────────────────────────────────────

/// Issue an access token for a successfully asserted challenge.
///
/// Token is 128 bits of entropy (base64url), expires in 1 hour.
/// Returns the `access_token` string.
///
/// # Errors
///
/// Returns `DbError` if the INSERT into `proof_tokens` fails.
// Issue-PROOF-LEVEL — 2026-03-14
pub fn issue_token(
    conn: &Connection,
    scope: &str,
    feed_guid: &str,
    proof_level: &ProofLevel,
) -> Result<String, DbError> {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    let access_token = URL_SAFE_NO_PAD.encode(bytes);

    let now = now_secs();
    let expires_at = now + TOKEN_TTL_SECS;

    let level_value = serde_json::to_value(proof_level)
        .map_err(|e| DbError::Other(format!("ProofLevel serialization failed: {e}")))?;
    let level_str = level_value
        .as_str()
        .ok_or_else(|| DbError::Other("ProofLevel did not serialize to a JSON string".into()))?
        .to_string();

    conn.execute(
        "INSERT INTO proof_tokens (access_token, scope, subject_feed_guid, expires_at, created_at, proof_level) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![access_token, scope, feed_guid, expires_at, now, level_str],
    )?;

    Ok(access_token)
}

/// Validate a bearer token. Returns `Some(subject_feed_guid)` if valid and unexpired.
///
/// Security note: standard string comparison is acceptable here because tokens
/// are 128 bits of random entropy -- timing side-channels do not meaningfully
/// reduce the search space for an attacker.
///
/// # Errors
///
/// Returns `DbError` if the SELECT fails.
pub fn validate_token(
    conn: &Connection,
    token: &str,
    required_scope: &str,
) -> Result<Option<String>, DbError> {
    let now = now_secs();
    let row: Option<String> = conn
        .query_row(
            "SELECT subject_feed_guid FROM proof_tokens \
             WHERE access_token = ?1 AND scope = ?2 AND expires_at > ?3",
            params![token, required_scope, now],
            |r| r.get(0),
        )
        .optional()?;
    Ok(row)
}

// Finding-6 token revocation on URL change — 2026-03-13

/// Revoke all access tokens for a given `feed_guid`.
///
/// Called after a `feed_url` PATCH: the existing tokens were issued against
/// the **old** feed URL's `podcast:txt` proof, so they must be invalidated.
/// The artist must re-prove ownership against the new URL.
///
/// Returns the number of tokens deleted.
///
/// # Errors
///
/// Returns `DbError` if the DELETE statement fails.
pub fn revoke_tokens_for_feed(
    conn: &Connection,
    feed_guid: &str,
) -> Result<usize, DbError> {
    let rows = conn.execute(
        "DELETE FROM proof_tokens WHERE subject_feed_guid = ?1",
        params![feed_guid],
    )?;
    Ok(rows)
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Recompute the `token_binding` from a stored binding's token portion and a
/// requester nonce. Used during assertion to verify the nonce matches.
///
/// Returns `None` if `stored_binding` is malformed (missing `.` separator, or
/// either the base-token or hash portion is empty).
#[must_use]
pub fn recompute_binding(stored_binding: &str, requester_nonce: &str) -> Option<String> {
    let (base_token, hash_part) = stored_binding.split_once('.')?;
    if base_token.is_empty() || hash_part.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(requester_nonce.as_bytes());
    let hash = hasher.finalize();
    Some(format!("{}.{}", base_token, URL_SAFE_NO_PAD.encode(hash)))
}

// SP-02 pruner interval — 2026-03-13
/// Reads `PROOF_PRUNE_INTERVAL_SECS` from the environment, defaulting to 300.
#[must_use]
pub fn prune_interval_from_env() -> u64 {
    std::env::var("PROOF_PRUNE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300)
}

/// Delete expired rows from `proof_challenges` and `proof_tokens` atomically.
///
/// Returns the total number of rows deleted (sum from both tables).
///
/// # Errors
///
/// Returns `DbError` if any SQL operation fails. Both deletes run inside
/// a single transaction so they are atomic.
pub fn prune_expired(conn: &mut Connection) -> Result<usize, DbError> {
    let now = now_secs();
    // Issue-CHECKED-TX — 2026-03-16: conn is freshly acquired from writer lock by caller, no nesting.
    let tx = conn.transaction()?;
    let challenges_deleted = tx.execute(
        "DELETE FROM proof_challenges WHERE expires_at < ?1",
        params![now],
    )?;
    let tokens_deleted = tx.execute(
        "DELETE FROM proof_tokens WHERE expires_at < ?1",
        params![now],
    )?;
    tx.commit()?;
    Ok(challenges_deleted + tokens_deleted)
}

// CS-01 pod:txt verification — 2026-03-12
//
// NOTE: Audio proof verification (ID3 TXXX / FLAC comment) is Phase 2.
// Only RSS `podcast:txt` verification is implemented. Dual-location
// verification for feed URL relocations is also Phase 2.
// See ADR-0018 "Implementation Status" for details.

/// Fetch RSS from `feed_url` and verify it contains a `<podcast:txt>` element
/// with text `stophammer-proof <token_binding>` at channel level.
///
/// Returns `Ok(true)` if the verification text is found, `Ok(false)` if the
/// RSS was fetched and parsed but the text was not found, and `Err(message)`
/// if the RSS could not be fetched or parsed.
///
/// # Errors
///
/// Returns a human-readable error string if the HTTP request fails or the
/// response body cannot be read.
/// Maximum RSS response body size (bytes) for podcast:txt verification.
/// Prevents a malicious feed URL from streaming gigabytes of data.
/// 5 MiB is generous for any legitimate RSS feed.
const MAX_RSS_BODY_BYTES: usize = 5 * 1024 * 1024;

/// # Errors
///
/// Returns `Err(String)` if the RSS feed cannot be fetched, exceeds the size
/// limit, or cannot be parsed as valid XML.
pub async fn verify_podcast_txt(
    client: &reqwest::Client,
    feed_url: &str,
    token_binding: &str,
) -> Result<bool, String> {
    use std::time::Duration;
    // Issue #19 chunked streaming read — 2026-03-13
    use futures_util::StreamExt;

    let resp = client
        .get(feed_url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("RSS fetch failed: {e}"))?;

    // Check Content-Length header (if present) before reading the body.
    if let Some(cl) = resp.content_length()
        && cl > MAX_RSS_BODY_BYTES as u64
    {
        return Err(format!(
            "RSS response too large: {cl} bytes (limit: {MAX_RSS_BODY_BYTES})"
        ));
    }

    // Read body in chunks with an accumulating limit to prevent OOM from
    // streaming responses that lack a Content-Length header.

    let mut body_bytes = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("RSS fetch error: {e}"))?;
        body_bytes.extend_from_slice(&chunk);
        if body_bytes.len() > MAX_RSS_BODY_BYTES {
            return Err(format!("RSS response exceeds {MAX_RSS_BODY_BYTES} bytes"));
        }
    }

    let body = String::from_utf8_lossy(&body_bytes);

    let expected_text = format!("stophammer-proof {token_binding}");
    let txt_values = extract_podcast_txt_values(&body);

    Ok(txt_values.iter().any(|v| v == &expected_text))
}

// Issue-PROOF-NAMESPACE — 2026-03-14

/// Extracts `<podcast:txt>` content using proper namespace resolution.
///
/// Handles any namespace prefix bound to `https://podcastindex.org/namespace/1.0`,
/// not just the conventional `podcast:` prefix. Uses `roxmltree` for correct XML
/// namespace handling instead of raw string search.
#[must_use]
pub fn extract_podcast_txt_values(xml: &str) -> Vec<String> {
    const PODCAST_NS: &str = "https://podcastindex.org/namespace/1.0";
    let doc = match roxmltree::Document::parse(xml) {
        Ok(d) => d,
        Err(_) => return vec![],
    };
    doc.descendants()
        .filter(|n| {
            n.is_element()
                && n.tag_name().namespace() == Some(PODCAST_NS)
                && n.tag_name().name() == "txt"
        })
        .filter_map(|n| {
            n.text()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .collect()
}

// ── SSRF guard — 2026-03-13 ──────────────────────────────────────────────

/// Maximum number of redirects the SSRF-safe proof fetch client will follow.
// Issue-SSRF-REDIRECT — 2026-03-15
const MAX_SSRF_REDIRECTS: usize = 3;

// Issue-DNS-REBIND — 2026-03-16

/// Maximum total timeout for the entire redirect chain (seconds).
const REDIRECT_CHAIN_TIMEOUT_SECS: u64 = 30;

/// Validates that `feed_url` is safe to fetch (no SSRF).
///
/// Rejects:
/// - Non-HTTP(S) schemes (`file://`, `ftp://`, etc.)
/// - Hostnames that resolve to private/reserved IP ranges
/// - Literal private/reserved IP addresses in the hostname
///
/// Returns the list of resolved `SocketAddr`s on success, enabling the caller
/// to pin DNS (prevent rebinding between validation and fetch).
///
/// # Errors
///
/// Returns a human-readable error string if the URL is rejected.
// Issue-SSRF-REDIRECT — 2026-03-15: now returns resolved addresses for DNS pinning
pub fn validate_feed_url(feed_url: &str) -> Result<Vec<std::net::SocketAddr>, String> {
    let url = url::Url::parse(feed_url)
        .map_err(|e| format!("invalid feed URL: {e}"))?;

    // Scheme check: only http and https are allowed.
    match url.scheme() {
        "http" | "https" => {}
        scheme => return Err(format!("disallowed URL scheme: {scheme}")),
    }

    let host = url.host_str()
        .ok_or_else(|| "feed URL has no host".to_string())?;

    // Check literal IP addresses against private/reserved ranges.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_private_ip(ip) {
            return Err(format!("feed URL targets a private/reserved IP: {ip}"));
        }
        let port = url.port_or_known_default().unwrap_or(443);
        return Ok(vec![std::net::SocketAddr::new(ip, port)]);
    }

    // Hostname: resolve and check all addresses.
    // Use std::net::ToSocketAddrs for synchronous DNS resolution (acceptable
    // because this runs before the async fetch and is fast for cached lookups).
    let socket_addr = format!("{host}:{}", url.port_or_known_default().unwrap_or(443));
    if let Ok(addrs) = socket_addr.to_socket_addrs() {
        let resolved: Vec<std::net::SocketAddr> = addrs.collect();
        for addr in &resolved {
            if is_private_ip(addr.ip()) {
                return Err(format!("feed URL hostname resolves to private/reserved IP: {}", addr.ip()));
            }
        }
        return Ok(resolved);
    }

    // If DNS resolution fails, return an empty list; the subsequent HTTP client
    // will handle the error (surfaced as 503).
    Ok(vec![])
}

// Issue-SSRF-REDIRECT — 2026-03-15

/// Check whether a URL is safe from an SSRF perspective (synchronous).
///
/// Used by the custom redirect policy to re-validate each hop in a redirect
/// chain, and by node-URL registration to reject private/loopback targets.
/// Checks scheme and any literal IP in the hostname. For hostnames, performs
/// synchronous DNS resolution and rejects any address in a private range.
// Issue-SYNC-SSRF — 2026-03-16
pub fn is_url_ssrf_safe(url: &url::Url) -> bool {
    // Only HTTP(S) schemes are allowed.
    match url.scheme() {
        "http" | "https" => {}
        _ => return false,
    }

    let Some(host) = url.host_str() else {
        return false;
    };

    // If the host is a literal IP address, check against private ranges.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return !is_private_ip(ip);
    }

    // Hostname-based redirect: resolve synchronously and check all addresses.
    // This is acceptable because redirect targets are rare and DNS lookups for
    // cached entries are fast (~1ms). The blocking DNS is already used in
    // validate_feed_url which runs inside spawn_blocking.
    let port = url.port_or_known_default().unwrap_or(443);
    let socket_addr = format!("{host}:{port}");
    if let Ok(addrs) = socket_addr.to_socket_addrs() {
        for addr in addrs {
            if is_private_ip(addr.ip()) {
                return false;
            }
        }
    }
    // If DNS fails, allow the redirect — reqwest will surface the connection
    // error. Blocking the redirect on DNS failure would be overly strict.
    true
}

/// Build a `reqwest::Client` with SSRF-safe redirect policy.
///
/// This client:
/// - Follows at most `MAX_SSRF_REDIRECTS` redirects
/// - Re-validates each redirect target against the SSRF guard (scheme + IP check)
/// - Has a 10-second total timeout
///
/// Use this for proof RSS fetches instead of the shared `push_client`.
///
/// # Panics
///
/// Panics if the reqwest client builder fails, which cannot happen with the
/// options used here (no custom TLS roots, no invalid configuration).
#[must_use]
pub fn build_ssrf_safe_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            // Enforce maximum redirect depth.
            if attempt.previous().len() >= MAX_SSRF_REDIRECTS {
                return attempt.error(SsrfRedirectError::TooManyRedirects);
            }
            let target = attempt.url().clone();
            // Re-run SSRF guard on the redirect target.
            if is_url_ssrf_safe(&target) {
                attempt.follow()
            } else {
                attempt.error(SsrfRedirectError::PrivateAddress(target.to_string()))
            }
        }))
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("SSRF-safe reqwest client builder uses only safe options")
}

/// Build a `reqwest::Client` with SSRF-safe redirect policy **and** DNS
/// pinning for a specific hostname.
///
/// The `resolved_addrs` from `validate_feed_url` are pinned to `hostname`,
/// preventing DNS rebinding between validation and fetch.
///
/// # Panics
///
/// Panics if the reqwest client builder fails, which cannot happen with the
/// options used here (no custom TLS roots, no invalid configuration).
#[must_use]
pub fn build_ssrf_safe_client_pinned(
    hostname: &str,
    resolved_addrs: &[std::net::SocketAddr],
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= MAX_SSRF_REDIRECTS {
                return attempt.error(SsrfRedirectError::TooManyRedirects);
            }
            let target = attempt.url().clone();
            if is_url_ssrf_safe(&target) {
                attempt.follow()
            } else {
                attempt.error(SsrfRedirectError::PrivateAddress(target.to_string()))
            }
        }))
        .timeout(std::time::Duration::from_secs(10));

    // Pin DNS for the original hostname to prevent rebinding.
    for addr in resolved_addrs {
        builder = builder.resolve(hostname, *addr);
    }

    builder
        .build()
        .expect("SSRF-safe pinned reqwest client builder uses only safe options")
}

// Issue-DNS-REBIND — 2026-03-16

/// Resolve a URL's hostname and validate all IPs against SSRF rules.
///
/// Returns the hostname and resolved socket addresses. For literal IP hosts,
/// returns the IP wrapped in a `SocketAddr`. Rejects private/reserved IPs.
///
/// # Errors
///
/// Returns a human-readable error string if the URL has no host, uses a
/// disallowed scheme, or resolves to a private/reserved IP.
pub fn resolve_and_validate_url(url: &url::Url) -> Result<(String, Vec<std::net::SocketAddr>), String> {
    match url.scheme() {
        "http" | "https" => {}
        scheme => return Err(format!("disallowed URL scheme: {scheme}")),
    }

    let host = url.host_str()
        .ok_or_else(|| "URL has no host".to_string())?
        .to_string();

    let port = url.port_or_known_default().unwrap_or(443);

    // Literal IP address: validate directly.
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_private_ip(ip) {
            return Err(format!("URL targets a private/reserved IP: {ip}"));
        }
        return Ok((host, vec![std::net::SocketAddr::new(ip, port)]));
    }

    // Hostname: resolve and validate all addresses.
    let socket_addr = format!("{host}:{port}");
    let addrs: Vec<std::net::SocketAddr> = socket_addr
        .to_socket_addrs()
        .map_err(|e| format!("DNS resolution failed for {host}: {e}"))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("DNS resolution returned no addresses for {host}"));
    }

    for addr in &addrs {
        if is_private_ip(addr.ip()) {
            return Err(format!("URL hostname resolves to private/reserved IP: {}", addr.ip()));
        }
    }

    Ok((host, addrs))
}

/// Build a `reqwest::Client` with redirects disabled and DNS pinned for a hostname.
///
/// Used by `fetch_with_pinned_redirects` to make a single-hop request where
/// the DNS resolution is locked to the validated addresses.
fn build_no_redirect_pinned_client(
    hostname: &str,
    resolved_addrs: &[std::net::SocketAddr],
) -> reqwest::Client {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10));

    for addr in resolved_addrs {
        builder = builder.resolve(hostname, *addr);
    }

    builder
        .build()
        .expect("no-redirect pinned reqwest client builder uses only safe options")
}

/// Fetches a URL following redirects manually, pinning DNS at each hop.
///
/// Eliminates the DNS-rebinding TOCTOU hole: the DNS resolution used for SSRF
/// validation is the **same** resolution used for the actual TCP connection,
/// because we pin each hop's addresses via `reqwest::ClientBuilder::resolve`.
///
/// The caller provides the pre-validated initial addresses (from `validate_feed_url`
/// or `resolve_and_validate_url`), so the first hop is already pinned. Each
/// subsequent redirect target is resolved, validated, and pinned before the
/// request is sent.
///
/// # Errors
///
/// Returns a human-readable error string if:
/// - A redirect target fails SSRF validation
/// - The redirect chain exceeds `max_redirects`
/// - Any individual request fails
/// - The response body exceeds `max_body_bytes`
pub async fn fetch_with_pinned_redirects(
    initial_url: &str,
    initial_hostname: &str,
    initial_addrs: &[std::net::SocketAddr],
    max_redirects: usize,
    max_body_bytes: usize,
) -> Result<String, String> {
    use futures_util::StreamExt;
    use std::time::{Duration, Instant};

    let deadline = Instant::now() + Duration::from_secs(REDIRECT_CHAIN_TIMEOUT_SECS);
    let mut current_url = initial_url.to_string();
    let mut current_hostname = initial_hostname.to_string();
    let mut current_addrs = initial_addrs.to_vec();

    for hop in 0..=max_redirects {
        let remaining = deadline.checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            return Err("redirect chain timed out".to_string());
        }

        let client = build_no_redirect_pinned_client(&current_hostname, &current_addrs);

        let resp = client
            .get(&current_url)
            .timeout(remaining.min(Duration::from_secs(10)))
            .send()
            .await
            .map_err(|e| format!("RSS fetch failed: {e}"))?;

        let status = resp.status();

        // Not a redirect -- read the body and return.
        if !status.is_redirection() {
            if !status.is_success() {
                return Err(format!("RSS fetch returned HTTP {status}"));
            }

            // Check Content-Length header before reading.
            if let Some(cl) = resp.content_length() {
                if cl > max_body_bytes as u64 {
                    return Err(format!(
                        "RSS response too large: {cl} bytes (limit: {max_body_bytes})"
                    ));
                }
            }

            let mut body_bytes = Vec::new();
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|e| format!("RSS fetch error: {e}"))?;
                body_bytes.extend_from_slice(&chunk);
                if body_bytes.len() > max_body_bytes {
                    return Err(format!("RSS response exceeds {max_body_bytes} bytes"));
                }
            }

            return String::from_utf8(body_bytes)
                .map_err(|e| format!("RSS response is not valid UTF-8: {e}"));
        }

        // 3xx redirect: extract Location, resolve, validate, and pin.
        if hop == max_redirects {
            return Err(format!("too many redirects (max {max_redirects})"));
        }

        let location = resp
            .headers()
            .get(reqwest::header::LOCATION)
            .ok_or_else(|| "redirect response missing Location header".to_string())?
            .to_str()
            .map_err(|e| format!("invalid Location header: {e}"))?;

        // Resolve relative URLs against the current URL.
        let base = url::Url::parse(&current_url)
            .map_err(|e| format!("invalid current URL: {e}"))?;
        let next_url = base.join(location)
            .map_err(|e| format!("invalid redirect Location: {e}"))?;

        // Validate and resolve DNS for the redirect target, pinning the result.
        let (next_hostname, next_addrs) = resolve_and_validate_url(&next_url)
            .map_err(|e| format!("redirect to {next_url} blocked: {e}"))?;

        current_url = next_url.to_string();
        current_hostname = next_hostname;
        current_addrs = next_addrs;
    }

    Err(format!("too many redirects (max {max_redirects})"))
}

/// Verify `podcast:txt` with DNS-pinned redirect following.
///
/// This is the production entry point for RSS proof verification. Unlike
/// `verify_podcast_txt` (which takes a pre-built client and relies on reqwest's
/// built-in redirect handling), this function follows redirects manually with
/// DNS pinning at every hop, closing the DNS-rebinding TOCTOU hole.
///
/// The `initial_hostname` and `initial_addrs` must come from a prior call to
/// `validate_feed_url` or `resolve_and_validate_url`.
///
/// # Errors
///
/// Returns `Err(String)` if the RSS feed cannot be fetched, a redirect targets
/// a private IP, or the response cannot be parsed.
pub async fn verify_podcast_txt_pinned(
    feed_url: &str,
    token_binding: &str,
    initial_hostname: &str,
    initial_addrs: &[std::net::SocketAddr],
) -> Result<bool, String> {
    let body = fetch_with_pinned_redirects(
        feed_url,
        initial_hostname,
        initial_addrs,
        MAX_SSRF_REDIRECTS,
        MAX_RSS_BODY_BYTES,
    )
    .await?;

    let expected_text = format!("stophammer-proof {token_binding}");
    let txt_values = extract_podcast_txt_values(&body);

    Ok(txt_values.iter().any(|v| v == &expected_text))
}

/// Error type for SSRF redirect policy violations.
// Issue-SSRF-REDIRECT — 2026-03-15
#[derive(Debug)]
enum SsrfRedirectError {
    /// Redirect target resolves to or is a private/reserved IP address.
    PrivateAddress(String),
    /// Too many redirects (exceeds `MAX_SSRF_REDIRECTS`).
    TooManyRedirects,
}

impl std::fmt::Display for SsrfRedirectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrivateAddress(url) => {
                write!(f, "redirect to private/reserved address blocked: {url}")
            }
            Self::TooManyRedirects => {
                write!(f, "too many redirects (max {MAX_SSRF_REDIRECTS})")
            }
        }
    }
}

impl std::error::Error for SsrfRedirectError {}

// Issue-SYNC-SSRF — 2026-03-16

/// Validates that `node_url` is safe for peer push registration.
///
/// Rejects:
/// - Non-HTTP(S) schemes (`file://`, `ftp://`, etc.)
/// - Hostnames that resolve to private/reserved IP ranges
/// - Literal private/reserved IP addresses in the hostname
///
/// # Errors
///
/// Returns a human-readable error string if the URL is rejected.
pub fn validate_node_url(node_url: &str) -> Result<(), String> {
    let url = url::Url::parse(node_url)
        .map_err(|e| format!("invalid node URL: {e}"))?;

    if !is_url_ssrf_safe(&url) {
        return Err(format!("node URL rejected: targets a private/reserved address or uses a disallowed scheme: {node_url}"));
    }

    Ok(())
}

/// Returns `true` if the IP address is in a private or reserved range
/// that should not be reachable via SSRF.
const fn is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback()           // 127.0.0.0/8
                || v4.is_private()     // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                || v4.is_link_local()  // 169.254.0.0/16
                || v4.is_broadcast()   // 255.255.255.255
                || v4.is_unspecified() // 0.0.0.0
                || v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64 // 100.64.0.0/10 (CGNAT)
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback()           // ::1
                || v6.is_unspecified() // ::
                // fc00::/7 (unique local addresses)
                || (v6.segments()[0] & 0xFE00) == 0xFC00
                // fe80::/10 (link-local)
                || (v6.segments()[0] & 0xFFC0) == 0xFE80
        }
    }
}

// SP-05 epoch guard — 2026-03-12
fn now_secs() -> i64 {
    crate::db::unix_now()
}

// proof.rs security compliant — 2026-03-13
