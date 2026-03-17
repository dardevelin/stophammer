// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Feed-ingest verification pipeline.
//!
//! # Architecture
//!
//! [`VerifierChain`] runs a sequence of [`Verifier`] implementations against
//! an [`IngestContext`]. Each verifier returns [`VerifyResult::Pass`],
//! [`VerifyResult::Warn`] (accepted, flagged for audit), or
//! [`VerifyResult::Fail`] (rejected — stops the chain).
//!
//! ## Read-only connection contract (Issue-VERIFY-READER)
//!
//! Verifiers receive a **read-only** database connection from the reader pool
//! (`PRAGMA query_only = ON`). The ingest handler runs the full verifier chain
//! against this reader *before* acquiring the writer lock. This ensures:
//!
//! - Verification never blocks the global write path.
//! - Non-trivial verifiers (external lookups, expensive queries) do not hold
//!   up other ingest requests waiting for the writer mutex.
//! - A verification failure avoids acquiring the writer lock entirely.
//!
//! # Adding a new verifier (plugin pattern)
//!
//! 1. Create `src/verifiers/<name>.rs` implementing the [`Verifier`] trait.
//! 2. Add the module to `src/verifiers/mod.rs`.
//! 3. Add a `match` arm to [`build_chain`] mapping the name string to the struct.
//! 4. Set `VERIFIER_CHAIN` to include the new name.
//!
//! No other files need to change. The chain order, and which verifiers run,
//! is controlled entirely by the `VERIFIER_CHAIN` environment variable at
//! startup — no redeployment of other nodes is required when adding verifiers
//! to a primary node.
//!
//! # Built-in verifiers
//!
//! | Name | Module | Function |
//! |---|---|---|
//! | `crawl_token` | [`verifiers::crawl_token`] | Rejects invalid crawl tokens |
//! | `content_hash` | [`verifiers::content_hash`] | Short-circuits unchanged feeds |
//! | `medium_music` | [`verifiers::medium_music`] | Rejects absent or non-music `podcast:medium` |
//! | `feed_guid` | [`verifiers::feed_guid`] | Rejects bad/malformed GUIDs |
//! | `v4v_payment` | [`verifiers::v4v_payment`] | Rejects feeds with no valid V4V payment routes |
//! | `enclosure_type` | [`verifiers::enclosure_type`] | Warns on video MIME types |
//! | `payment_route_sum` | [`verifiers::payment_route_sum`] | Optional: rejects splits ≠ 100 (not in default chain) |

use std::fmt;

use rusqlite::Connection;
use crate::ingest::IngestFeedRequest;
use crate::model::Feed;

// ── Error type ──────────────────────────────────────────────────────────────

/// Structured error returned by [`VerifierChain::run`] on the first `Fail`.
///
/// Wraps the formatted rejection reason (e.g. `"[medium_music] ..."`).
/// Implements [`std::error::Error`] so callers can use `?` and trait-object
/// error handling instead of bare `String`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifierError(pub String);

impl fmt::Display for VerifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for VerifierError {}

// ── Context ────────────────────────────────────────────────────────────────

/// Contextual data threaded through the verifier chain for a single ingest request.
///
/// The `db` field carries a **read-only** connection (`PRAGMA query_only = ON`).
/// Verification runs against the reader pool *before* the writer lock is
/// acquired, so verifiers never block the global write path.
// CRIT-03 Debug derive — 2026-03-13
// Issue-VERIFY-READER — 2026-03-16
#[derive(Debug)]
pub struct IngestContext<'a> {
    /// The ingest request being validated, including the parsed feed data.
    pub request:  &'a IngestFeedRequest,
    /// **Read-only** database connection (reader pool, `query_only = ON`).
    ///
    /// Verifiers may query prior state (e.g. crawl cache) but must NOT attempt
    /// writes — they will fail at the SQLite level.
    pub db:       &'a Connection,
    /// The feed row already stored for this URL, if one exists.
    ///
    /// Available for verifiers that need to diff against prior state; not yet
    /// consumed but retained so new verifiers can use it without API changes.
    pub existing: Option<&'a Feed>,
}

// ── Result ─────────────────────────────────────────────────────────────────

/// The outcome of a single verifier step.
#[derive(Debug)]
pub enum VerifyResult {
    /// The check passed; ingestion continues normally.
    Pass,
    /// The check raised a concern but did not block ingestion; the message is
    /// stored with the event record for later audit.
    Warn(String),
    /// The check failed; ingestion is rejected and the message is returned
    /// to the crawler as the rejection reason.
    Fail(String),
}

// ── Trait ──────────────────────────────────────────────────────────────────

/// A single step in the verifier chain.
///
/// # Connection contract
///
/// Verifiers receive a **read-only** database connection through
/// [`IngestContext::db`]. The connection has `PRAGMA query_only = ON` enforced
/// at the pool level, so any attempt to execute a write statement will return
/// a SQLite error. This is intentional:
///
/// - Verification is an *inspection* step — it decides whether a mutation is
///   allowed but must not perform mutations itself.
/// - Because the connection is read-only, the verifier chain runs **before**
///   the writer lock is acquired. This means verification never blocks the
///   global write path and non-trivial verifiers (e.g. external lookups) do
///   not hold up other ingest requests.
/// - Verifiers that need external data should cache or pre-fetch it before
///   the `verify` call rather than reaching out to external services inline.
///
/// # Implementing a verifier
///
/// ```rust,ignore
/// use stophammer::verify::{IngestContext, Verifier, VerifyResult};
///
/// pub struct MyVerifier;
///
/// impl Verifier for MyVerifier {
///     fn name(&self) -> &'static str { "my_verifier" }
///
///     fn verify(&self, ctx: &IngestContext) -> VerifyResult {
///         // inspect ctx.request or ctx.db (read-only), return Pass/Warn/Fail
///         VerifyResult::Pass
///     }
/// }
/// ```
// Issue-VERIFY-READER — 2026-03-16
pub trait Verifier: Send + Sync {
    /// Short identifier used in warning and rejection messages, e.g. `"crawl_token"`.
    fn name(&self) -> &'static str;
    /// Run this check against `ctx` and return the outcome.
    ///
    /// `ctx.db` is a **read-only** connection (`PRAGMA query_only = ON`).
    /// Attempting a write will fail at the SQLite level.
    fn verify(&self, ctx: &IngestContext) -> VerifyResult;
}

// ── Chain ──────────────────────────────────────────────────────────────────

/// An ordered sequence of [`Verifier`]s run against each ingest request.
// CRIT-03 Debug derive — 2026-03-13
pub struct VerifierChain {
    verifiers: Vec<Box<dyn Verifier>>,
}

impl std::fmt::Debug for VerifierChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.verifiers.iter().map(|v| v.name()).collect();
        f.debug_struct("VerifierChain")
            .field("verifiers", &names)
            .finish()
    }
}

impl VerifierChain {
    /// Creates a new chain that will run `verifiers` in order.
    #[must_use]
    pub fn new(verifiers: Vec<Box<dyn Verifier>>) -> Self {
        Self { verifiers }
    }

    /// Runs all verifiers in order and collects warnings.
    ///
    /// Returns `Ok(warnings)` when all verifiers pass or warn. Stops at the
    /// first [`VerifyResult::Fail`] and returns `Err(VerifierError)`.
    ///
    /// # Errors
    ///
    /// Returns [`VerifierError`] containing the formatted rejection reason on
    /// the first `Fail`. [`verifiers::content_hash::ContentHashVerifier`] uses
    /// [`verifiers::content_hash::NO_CHANGE_SENTINEL`] as its rejection string
    /// to signal a no-op — callers must check for this sentinel before treating
    /// the error as a real failure.
    // Issue #8 #[must_use] — 2026-03-13
    #[must_use = "verification warnings or rejection reason must be handled"]
    pub fn run(&self, ctx: &IngestContext) -> Result<Vec<String>, VerifierError> {
        let mut warnings = Vec::new();
        for v in &self.verifiers {
            match v.verify(ctx) {
                VerifyResult::Pass       => {}
                VerifyResult::Warn(msg)  => warnings.push(format!("[{}] {}", v.name(), msg)),
                VerifyResult::Fail(msg)  => return Err(VerifierError(format!("[{}] {}", v.name(), msg))),
            }
        }
        Ok(warnings)
    }
}

// ── ChainSpec ──────────────────────────────────────────────────────────────

/// Configuration that drives chain assembly at startup.
///
/// Read from environment variables so operators can change the verifier set
/// and order without modifying code.
///
/// # Environment variables
///
/// - `VERIFIER_CHAIN` — comma-separated list of verifier names, in run order.
///   Defaults to the full built-in set in a sensible order.
///   Example: `"crawl_token,content_hash,medium_music,feed_guid,payment_route_sum,enclosure_type"`
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub struct ChainSpec {
    /// Names of verifiers to run, in order.
    pub names: Vec<String>,
}

impl ChainSpec {
    /// Default chain: all built-ins in the recommended order.
    pub const DEFAULT: &'static str =
        "crawl_token,content_hash,medium_music,feed_guid,v4v_payment,enclosure_type";

    /// Reads `VERIFIER_CHAIN` from the environment.
    ///
    /// Falls back to [`Self::DEFAULT`] when the variable is absent or empty.
    #[must_use]
    pub fn from_env() -> Self {
        let raw = std::env::var("VERIFIER_CHAIN").unwrap_or_default();
        let names: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        // A value that parses to zero names (e.g. VERIFIER_CHAIN=",") is a
        // misconfiguration — fall back to the default rather than creating an
        // empty chain that silently accepts all ingests with no authentication.
        if names.is_empty() {
            Self {
                names: Self::DEFAULT
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
            }
        } else {
            Self { names }
        }
    }
}

// ── Registry ───────────────────────────────────────────────────────────────

/// Assembles a [`VerifierChain`] from a [`ChainSpec`].
///
/// Maps each name in `spec.names` to its built-in [`Verifier`] implementation.
/// Unknown names cause a panic — a misconfigured verifier chain is a startup
/// configuration error that makes the security pipeline untrustworthy.
///
/// `crawl_token` requires the shared secret from `CRAWL_TOKEN`; pass it via
/// the `crawl_token` argument so this function does not read env vars itself.
///
/// # Adding a new verifier
///
/// Add a `match` arm below and declare it in `src/verifiers/mod.rs`. No other
/// files need to change. See the [module-level docs](self) for the full
/// four-step procedure.
///
/// # Panics
///
/// Panics if `spec.names` contains an unrecognised verifier name. This is
/// intentional per M-PANIC-ON-BUG: a misconfigured verifier chain is a
/// programming/configuration error that should fail fast at startup rather
/// than silently running with a broken security gate.
// Finding-7 verifier fails-closed — 2026-03-13
#[must_use]
#[expect(clippy::needless_pass_by_value, reason = "takes ownership so callers can move the token into the chain")]
pub fn build_chain(spec: &ChainSpec, crawl_token: String) -> VerifierChain {
    use crate::verifiers::{
        content_hash::ContentHashVerifier,
        crawl_token::CrawlTokenVerifier,
        enclosure_type::EnclosureTypeVerifier,
        feed_guid::FeedGuidVerifier,
        medium_music::MediumMusicVerifier,
        payment_route_sum::PaymentRouteSumVerifier,
        v4v_payment::V4VPaymentVerifier,
    };

    let mut verifiers: Vec<Box<dyn Verifier>> = Vec::new();

    for name in &spec.names {
        let v: Box<dyn Verifier> = match name.as_str() {
            "crawl_token"       => Box::new(CrawlTokenVerifier { expected: crawl_token.clone() }),
            "content_hash"      => Box::new(ContentHashVerifier),
            "medium_music"      => Box::new(MediumMusicVerifier),
            "feed_guid"         => Box::new(FeedGuidVerifier),
            "v4v_payment"       => Box::new(V4VPaymentVerifier),
            "payment_route_sum" => Box::new(PaymentRouteSumVerifier),
            "enclosure_type"    => Box::new(EnclosureTypeVerifier),
            unknown => {
                panic!(
                    "FATAL: unknown verifier '{unknown}' in VERIFIER_CHAIN. \
                     Valid verifiers are: crawl_token, content_hash, medium_music, \
                     feed_guid, v4v_payment, payment_route_sum, enclosure_type. \
                     Check the VERIFIER_CHAIN env var for typos."
                );
            }
        };
        verifiers.push(v);
    }

    VerifierChain::new(verifiers)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── VerifierError trait compliance ────────────────────────────────────

    #[test]
    fn verifier_error_implements_std_error() {
        let err = VerifierError("test failure".into());
        // Must implement std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn verifier_error_display_matches_inner() {
        let msg = "[medium_music] rejected: not music";
        let err = VerifierError(msg.into());
        assert_eq!(err.to_string(), msg);
    }

    #[test]
    fn verifier_error_debug_and_clone() {
        let err = VerifierError("clone me".into());
        let cloned = err.clone();
        assert_eq!(err, cloned);
        let debug = format!("{err:?}");
        assert!(debug.contains("clone me"));
    }

    #[test]
    fn verifier_error_eq() {
        let a = VerifierError("same".into());
        let b = VerifierError("same".into());
        let c = VerifierError("different".into());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── Chain returns VerifierError ──────────────────────────────────────

    struct AlwaysFail;
    impl Verifier for AlwaysFail {
        fn name(&self) -> &'static str { "always_fail" }
        fn verify(&self, _ctx: &IngestContext) -> VerifyResult {
            VerifyResult::Fail("boom".into())
        }
    }

    struct AlwaysPass;
    impl Verifier for AlwaysPass {
        fn name(&self) -> &'static str { "always_pass" }
        fn verify(&self, _ctx: &IngestContext) -> VerifyResult {
            VerifyResult::Pass
        }
    }

    #[test]
    fn chain_run_returns_verifier_error_on_fail() {
        let chain = VerifierChain::new(vec![Box::new(AlwaysFail)]);
        let conn = rusqlite::Connection::open_in_memory().expect("open db");
        let req = crate::ingest::IngestFeedRequest {
            crawl_token:   String::new(),
            canonical_url: String::new(),
            source_url:    String::new(),
            http_status:   200,
            content_hash:  String::new(),
            feed_data:     None,
        };
        let ctx = IngestContext { request: &req, db: &conn, existing: None };
        let err = chain.run(&ctx).unwrap_err();
        assert_eq!(err.0, "[always_fail] boom");
        // Verify it implements Error trait
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn chain_run_returns_ok_on_all_pass() {
        let chain = VerifierChain::new(vec![Box::new(AlwaysPass)]);
        let conn = rusqlite::Connection::open_in_memory().expect("open db");
        let req = crate::ingest::IngestFeedRequest {
            crawl_token:   String::new(),
            canonical_url: String::new(),
            source_url:    String::new(),
            http_status:   200,
            content_hash:  String::new(),
            feed_data:     None,
        };
        let ctx = IngestContext { request: &req, db: &conn, existing: None };
        chain.run(&ctx).unwrap();
    }

    // ── ChainSpec::from_env empty-chain fallback ─────────────────────────

    #[test]
    fn chain_spec_comma_only_falls_back_to_default() {
        // VERIFIER_CHAIN="," produces zero valid names — must fall back to
        // the default chain rather than creating an empty (accept-all) chain.
        // Safety: single-threaded test, no other thread reads VERIFIER_CHAIN.
        #[expect(unsafe_code, reason = "env var manipulation in single-threaded test")]
        // SAFETY: test is single-threaded; no concurrent env reads
        unsafe { std::env::set_var("VERIFIER_CHAIN", ",") };
        let spec = ChainSpec::from_env();
        #[expect(unsafe_code, reason = "env var manipulation in single-threaded test")]
        // SAFETY: test is single-threaded; no concurrent env reads
        unsafe { std::env::remove_var("VERIFIER_CHAIN") };
        let default_names: Vec<String> = ChainSpec::DEFAULT
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(spec.names, default_names, "empty result should fall back to DEFAULT");
    }

    #[test]
    fn chain_spec_whitespace_commas_falls_back_to_default() {
        #[expect(unsafe_code, reason = "env var manipulation in single-threaded test")]
        // SAFETY: test is single-threaded; no concurrent env reads
        unsafe { std::env::set_var("VERIFIER_CHAIN", " , , ") };
        let spec = ChainSpec::from_env();
        #[expect(unsafe_code, reason = "env var manipulation in single-threaded test")]
        // SAFETY: test is single-threaded; no concurrent env reads
        unsafe { std::env::remove_var("VERIFIER_CHAIN") };
        assert!(!spec.names.is_empty(), "empty result should fall back to DEFAULT");
    }
}
