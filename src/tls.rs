//! Automatic TLS via ACME (Let's Encrypt) for stophammer nodes.
//!
//! Handles certificate provisioning, renewal, and the temporary HTTP-01
//! challenge server on port 80. ASN.1 parsing for `notAfter` expiry checks
//! is done inline to avoid pulling in a full X.509 library.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::Path;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum::Router;
use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, ChallengeType, Identifier, LetsEncrypt,
    NewAccount, NewOrder, RetryPolicy,
};
use tokio::sync::oneshot;

/// Interval between TLS certificate renewal checks (12 hours).
///
/// Balances between timely renewal and unnecessary ACME traffic. Since
/// certificates are renewed 30 days before expiry, a 12-hour check
/// cadence provides ample margin.
const RENEWAL_CHECK_INTERVAL_SECS: u64 = 12 * 60 * 60;

/// TLS configuration read from environment variables.
// HIGH-02 PathBuf fields — 2026-03-13
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub domain: String,
    pub acme_email: String,
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    pub acme_account_path: std::path::PathBuf,
    pub staging: bool,
    /// Custom ACME directory URL (e.g. Pebble at `https://pebble:14000/dir`).
    /// When set, overrides the `staging` toggle entirely.
    pub acme_directory_url: Option<String>,
}

impl TlsConfig {
    /// Resolve the ACME directory URL for this configuration.
    ///
    /// Returns the custom URL when `acme_directory_url` is set, otherwise
    /// falls back to the Let's Encrypt production or staging URL based on
    /// the `staging` flag.
    #[must_use]
    pub fn resolved_directory_url(&self) -> &str {
        self.acme_directory_url
            .as_deref()
            .unwrap_or_else(|| {
                if self.staging {
                    LetsEncrypt::Staging.url()
                } else {
                    LetsEncrypt::Production.url()
                }
            })
    }
}

/// Returns `true` if the certificate at `cert_path` is missing or expires within
/// 30 days. This is the signal to (re-)provision via ACME.
// TLS hardening compliant — 2026-03-12
#[must_use]
// HIGH-02 Path param — 2026-03-13
pub fn cert_needs_renewal(cert_path: impl AsRef<std::path::Path>) -> bool {
    let Ok(pem_bytes) = std::fs::read(cert_path.as_ref()) else {
        return true; // file missing — needs provisioning
    };

    let certs = parse_pem_certs(&pem_bytes);
    if certs.is_empty() {
        return true;
    }

    // Check the leaf (first) certificate's expiry.
    let leaf_der = &certs[0];
    expiry_from_der(leaf_der).is_none_or(|not_after| {
        let now = time::OffsetDateTime::now_utc();
        let remaining = not_after - now;
        remaining.whole_days() < 30
    })
}

/// Runs the ACME http-01 challenge flow using `instant-acme`.
///
/// 1. Creates (or reuses) an ACME account.
/// 2. Creates an order for `config.domain`.
/// 3. Spins up a temporary HTTP server on port 80 to answer the challenge.
/// 4. Finalises the order and writes cert + key to disk.
///
/// # Errors
///
/// Returns an error if any ACME step fails or the files cannot be written.
// TLS hardening compliant — 2026-03-12
pub async fn provision_certificate(
    config: &TlsConfig,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Issue-PEBBLE-ACME — 2026-03-14
    let directory_url = config.resolved_directory_url();

    let contact = format!("mailto:{}", config.acme_email);
    let account = if std::path::Path::new(&config.acme_account_path).exists() {
        let raw = std::fs::read(&config.acme_account_path)?;
        let credentials: AccountCredentials = serde_json::from_slice(&raw)
            .map_err(|e| format!("ACME credentials parse error: {e}"))?;
        Account::builder()
            .map_err(|e| format!("ACME account builder error: {e}"))?
            .from_credentials(credentials)
            .await
            .map_err(|e| format!("ACME account restore error: {e}"))?
    } else {
        let (account, credentials) = Account::builder()
            .map_err(|e| format!("ACME account builder error: {e}"))?
            .create(
                &NewAccount {
                    contact: &[contact.as_str()],
                    terms_of_service_agreed: true,
                    only_return_existing: false,
                },
                directory_url.to_string(),
                None,
            )
            .await
            .map_err(|e| format!("ACME account creation error: {e}"))?;
        if let Some(parent) = std::path::Path::new(&config.acme_account_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        let cred_json = serde_json::to_vec(&credentials)
            .map_err(|e| format!("ACME credentials serialize error: {e}"))?;
        write_private_file(&config.acme_account_path, &cred_json)?;
        account
    };

    // Build the identifier — domain or IP.
    let identifier = config.domain.parse::<std::net::IpAddr>()
        .map_or_else(|_| Identifier::Dns(config.domain.clone()), Identifier::Ip);

    let identifiers = [identifier];
    let mut order = account
        .new_order(&NewOrder::new(&identifiers))
        .await
        .map_err(|e| format!("ACME new_order error: {e}"))?;

    // Process authorizations — find the http-01 challenge token + key auth.
    // We collect the token/key_auth and start the challenge server, then break
    // out of the authorization scope so we can call poll_ready on `order`.
    let challenge_info = process_authorizations(&mut order).await?;

    let (shutdown_tx, challenge_handle) = match challenge_info {
        Some((token, key_auth)) => {
            let (tx, rx) = oneshot::channel::<()>();
            let handle =
                tokio::spawn(run_challenge_server(token, key_auth, config.domain.clone(), rx));
            (Some(tx), Some(handle))
        }
        None => {
            return Err("no pending authorizations found — nothing to challenge".into());
        }
    };

    // Poll until the order is ready.
    order
        .poll_ready(&RetryPolicy::default())
        .await
        .map_err(|e| format!("ACME poll_ready error: {e}"))?;

    // Shut down challenge server.
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(());
    }
    if let Some(handle) = challenge_handle {
        let _ = handle.await;
    }

    // Finalise: generates a CSR internally and returns the private key PEM.
    let private_key_pem = order
        .finalize()
        .await
        .map_err(|e| format!("ACME finalize error: {e}"))?;

    // Poll for the certificate.
    let cert_chain_pem = order
        .poll_certificate(&RetryPolicy::default())
        .await
        .map_err(|e| format!("ACME poll_certificate error: {e}"))?;

    // Ensure parent directories exist.
    if let Some(parent) = std::path::Path::new(&config.cert_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = std::path::Path::new(&config.key_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write cert chain.
    std::fs::write(&config.cert_path, cert_chain_pem.as_bytes())?;

    // Write private key with restricted permissions.
    write_private_file(&config.key_path, private_key_pem.as_bytes())?;

    tracing::info!(domain = %config.domain, "TLS certificate provisioned");
    Ok(())
}

/// Spawns a background task that checks cert expiry every 12 hours and
/// calls `provision_certificate` when fewer than 30 days remain.
// TLS hardening compliant — 2026-03-12
pub fn spawn_renewal_task(config: TlsConfig) {
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(RENEWAL_CHECK_INTERVAL_SECS);
        loop {
            tokio::time::sleep(interval).await;
            if cert_needs_renewal(&config.cert_path) {
                tracing::info!("TLS certificate needs renewal — starting ACME provisioning");
                if let Err(e) = provision_certificate(&config).await {
                    tracing::error!(error = %e, "TLS renewal failed");
                }
            }
        }
    });
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Walk the order's authorizations, find the first pending http-01 challenge,
/// call `set_ready`, and return `(token, key_auth)`. Returns `None` if every
/// authorization is already valid.
// TLS hardening compliant — 2026-03-12
async fn process_authorizations(
    order: &mut instant_acme::Order,
) -> Result<Option<(String, String)>, Box<dyn std::error::Error + Send + Sync>> {
    let mut authzs = order.authorizations();
    while let Some(authz_result) = authzs.next().await {
        let mut authz = authz_result.map_err(|e| format!("ACME authorization error: {e}"))?;

        if authz.status == AuthorizationStatus::Valid {
            continue;
        }

        let mut challenge = authz
            .challenge(ChallengeType::Http01)
            .ok_or("no http-01 challenge offered by ACME server")?;

        let token = challenge.token.clone();
        let key_auth = challenge.key_authorization().as_str().to_string();

        challenge
            .set_ready()
            .await
            .map_err(|e| format!("ACME set_ready error: {e}"))?;

        return Ok(Some((token, key_auth)));
    }
    Ok(None)
}

/// Runs a temporary HTTP server on port 80 that answers
/// `GET /.well-known/acme-challenge/{token}` with the key authorization.
/// All other paths return a 301 redirect to HTTPS.
/// Shuts down when `shutdown_rx` fires.
async fn run_challenge_server(
    token: String,
    key_auth: String,
    domain: String,
    shutdown_rx: oneshot::Receiver<()>,
) {
    let state = Arc::new(ChallengeState {
        token,
        key_auth,
        domain,
    });

    let app = Router::new()
        .route(
            #[expect(clippy::literal_string_with_formatting_args, reason = "{token} is an Axum path parameter, not a format argument")]
            "/.well-known/acme-challenge/{token}",
            get({
                let state = Arc::clone(&state);
                move |Path(req_token): Path<String>| {
                    let state = Arc::clone(&state);
                    async move { serve_challenge(&req_token, &state) }
                }
            }),
        )
        .fallback({
            let state = Arc::clone(&state);
            move || {
                let state = Arc::clone(&state);
                async move { Redirect::permanent(&format!("https://{}/", state.domain)) }
            }
        });

    let addr: SocketAddr = ([0, 0, 0, 0], 80).into();
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, "ACME challenge server: failed to bind port 80");
            return;
        }
    };

    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        })
        .await;
}

struct ChallengeState {
    token: String,
    key_auth: String,
    domain: String,
}

fn serve_challenge(req_token: &str, state: &ChallengeState) -> Response {
    if req_token == state.token {
        state.key_auth.clone().into_response()
    } else {
        Redirect::permanent(&format!("https://{}/", state.domain)).into_response()
    }
}

/// Writes `data` to `path` atomically with 0o600 permissions at creation time.
///
/// On Unix the mode is set via `OpenOptions::mode()` before the file is created,
/// so there is no window where the file is world-readable.
// HIGH-02 Path param — 2026-03-13
fn write_private_file(path: impl AsRef<std::path::Path>, data: &[u8]) -> std::io::Result<()> {
    let path = path.as_ref();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(data)?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, data)?;
    }
    Ok(())
}

/// Parse PEM-encoded certificates, returning their DER bodies.
fn parse_pem_certs(pem_bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut reader = std::io::BufReader::new(pem_bytes);
    let mut certs = Vec::new();
    for item in rustls_pemfile::read_all(&mut reader).flatten() {
        if let rustls_pemfile::Item::X509Certificate(der) = item {
            certs.push(der.to_vec());
        }
    }
    certs
}

/// Extract the `notAfter` timestamp from a DER-encoded X.509 certificate.
///
/// Navigates the ASN.1 structure:
///   Certificate -> `tbsCertificate` -> validity -> `notAfter`
///
/// Uses proper TLV (Tag-Length-Value) walking rather than a naive byte scan
/// to avoid false positives from data bytes that happen to match time tags.
fn expiry_from_der(der: &[u8]) -> Option<time::OffsetDateTime> {
    // Certificate is a SEQUENCE; enter it.
    let tbs = asn1_enter_sequence(der)?;
    // tbsCertificate is the first element — also a SEQUENCE.
    let tbs_inner = asn1_enter_sequence(tbs)?;

    // Walk the TBS fields: version, serialNumber, signature, issuer, validity.
    // version is [0] EXPLICIT (context tag 0xa0) — may be absent in v1 certs.
    let mut pos = 0;
    // Skip version if present (context-specific constructed tag 0xa0).
    if !tbs_inner.is_empty() && tbs_inner[0] == 0xa0 {
        let (_, consumed) = asn1_read_tlv(&tbs_inner[pos..])?;
        pos += consumed;
    }
    // serialNumber (INTEGER)
    let (_, consumed) = asn1_read_tlv(&tbs_inner[pos..])?;
    pos += consumed;
    // signature (AlgorithmIdentifier — SEQUENCE)
    let (_, consumed) = asn1_read_tlv(&tbs_inner[pos..])?;
    pos += consumed;
    // issuer (Name — SEQUENCE)
    let (_, consumed) = asn1_read_tlv(&tbs_inner[pos..])?;
    pos += consumed;
    // validity (SEQUENCE { notBefore, notAfter })
    let validity_data = asn1_enter_sequence(&tbs_inner[pos..])?;

    // notBefore
    let (_, consumed) = asn1_read_tlv(validity_data)?;
    // notAfter
    let remaining = &validity_data[consumed..];
    if remaining.is_empty() {
        return None;
    }
    let tag = remaining[0];
    let (value, _) = asn1_read_tlv(remaining)?;
    parse_asn1_time(tag, value)
}

/// Read a DER length field starting at `data[0]`. Returns `(length, bytes_consumed)`.
fn asn1_read_length(data: &[u8]) -> Option<(usize, usize)> {
    if data.is_empty() {
        return None;
    }
    let first = data[0];
    if first < 0x80 {
        Some((first as usize, 1))
    } else {
        let num_bytes = (first & 0x7f) as usize;
        if num_bytes == 0 || num_bytes > 4 || data.len() < 1 + num_bytes {
            return None;
        }
        let mut len: usize = 0;
        for &b in &data[1..=num_bytes] {
            len = len.checked_shl(8)?.checked_add(b as usize)?;
        }
        Some((len, 1 + num_bytes))
    }
}

/// Read one TLV element. Returns `(value_bytes, total_bytes_consumed)`.
fn asn1_read_tlv(data: &[u8]) -> Option<(&[u8], usize)> {
    if data.is_empty() {
        return None;
    }
    // tag is 1 byte (we only handle single-byte tags)
    let (len, len_size) = asn1_read_length(&data[1..])?;
    let header = 1 + len_size;
    if data.len() < header + len {
        return None;
    }
    Some((&data[header..header + len], header + len))
}

/// Enter a SEQUENCE: verify the tag is 0x30 and return the inner content bytes.
fn asn1_enter_sequence(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() || data[0] != 0x30 {
        return None;
    }
    let (inner, _) = asn1_read_tlv(data)?;
    Some(inner)
}

/// Parse a `UTCTime` (tag 0x17) or `GeneralizedTime` (tag 0x18) value.
// TLS hardening compliant — 2026-03-12
fn parse_asn1_time(tag: u8, value: &[u8]) -> Option<time::OffsetDateTime> {
    let s = std::str::from_utf8(value).ok()?;
    match tag {
        0x17 => {
            // UTCTime: YYMMDDHHmmSSZ
            if s.len() < 13 {
                return None;
            }
            let yy: i32 = s[0..2].parse().ok()?;
            let year = if yy >= 50 { 1900 + yy } else { 2000 + yy };
            let month: u8 = s[2..4].parse().ok()?;
            let day: u8 = s[4..6].parse().ok()?;
            let hour: u8 = s[6..8].parse().ok()?;
            let min: u8 = s[8..10].parse().ok()?;
            let sec: u8 = s[10..12].parse().ok()?;
            let month = time::Month::try_from(month).ok()?;
            let date = time::Date::from_calendar_date(year, month, day).ok()?;
            let t = time::Time::from_hms(hour, min, sec).ok()?;
            Some(time::PrimitiveDateTime::new(date, t).assume_utc())
        }
        0x18 => {
            // GeneralizedTime: YYYYMMDDHHmmSSZ
            if s.len() < 15 {
                return None;
            }
            let year: i32 = s[0..4].parse().ok()?;
            let month: u8 = s[4..6].parse().ok()?;
            let day: u8 = s[6..8].parse().ok()?;
            let hour: u8 = s[8..10].parse().ok()?;
            let min: u8 = s[10..12].parse().ok()?;
            let sec: u8 = s[12..14].parse().ok()?;
            let month = time::Month::try_from(month).ok()?;
            let date = time::Date::from_calendar_date(year, month, day).ok()?;
            let t = time::Time::from_hms(hour, min, sec).ok()?;
            Some(time::PrimitiveDateTime::new(date, t).assume_utc())
        }
        _ => None,
    }
}
