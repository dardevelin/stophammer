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

use rusqlite::Connection;
use crate::ingest::IngestFeedRequest;
use crate::model::Feed;

// ── Context ────────────────────────────────────────────────────────────────

/// Contextual data threaded through the verifier chain for a single ingest request.
pub struct IngestContext<'a> {
    /// The ingest request being validated, including the parsed feed data.
    pub request:  &'a IngestFeedRequest,
    /// Read-only view of the database, used by verifiers that need prior state.
    pub db:       &'a Connection,
    /// The feed row already stored for this URL, if one exists.
    ///
    /// Available for verifiers that need to diff against prior state; not yet
    /// consumed but retained so new verifiers can use it without API changes.
    #[expect(dead_code, reason = "used by future diff-based verifiers without API changes")]
    pub existing: Option<&'a Feed>,
}

// ── Result ─────────────────────────────────────────────────────────────────

/// The outcome of a single verifier step.
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
///         // inspect ctx.request or ctx.db, return Pass/Warn/Fail
///         VerifyResult::Pass
///     }
/// }
/// ```
pub trait Verifier: Send + Sync {
    /// Short identifier used in warning and rejection messages, e.g. `"crawl_token"`.
    fn name(&self) -> &'static str;
    /// Run this check against `ctx` and return the outcome.
    fn verify(&self, ctx: &IngestContext) -> VerifyResult;
}

// ── Chain ──────────────────────────────────────────────────────────────────

/// An ordered sequence of [`Verifier`]s run against each ingest request.
pub struct VerifierChain {
    verifiers: Vec<Box<dyn Verifier>>,
}

impl VerifierChain {
    /// Creates a new chain that will run `verifiers` in order.
    pub fn new(verifiers: Vec<Box<dyn Verifier>>) -> Self {
        Self { verifiers }
    }

    /// Runs all verifiers in order and collects warnings.
    ///
    /// Returns `Ok(warnings)` when all verifiers pass or warn. Stops at the
    /// first [`VerifyResult::Fail`] and returns `Err(reason)`.
    ///
    /// # Errors
    ///
    /// Returns the formatted rejection reason on the first `Fail`.
    /// [`verifiers::content_hash::ContentHashVerifier`] uses
    /// [`verifiers::content_hash::NO_CHANGE_SENTINEL`] as its rejection string
    /// to signal a no-op — callers must check for this sentinel before treating
    /// the error as a real failure.
    pub fn run(&self, ctx: &IngestContext) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();
        for v in &self.verifiers {
            match v.verify(ctx) {
                VerifyResult::Pass       => {}
                VerifyResult::Warn(msg)  => warnings.push(format!("[{}] {}", v.name(), msg)),
                VerifyResult::Fail(msg)  => return Err(format!("[{}] {}", v.name(), msg)),
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
    pub fn from_env() -> Self {
        let raw = std::env::var("VERIFIER_CHAIN").unwrap_or_default();
        let raw = if raw.trim().is_empty() { Self::DEFAULT.to_string() } else { raw };
        Self {
            names: raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
        }
    }
}

// ── Registry ───────────────────────────────────────────────────────────────

/// Assembles a [`VerifierChain`] from a [`ChainSpec`].
///
/// Maps each name in `spec.names` to its built-in [`Verifier`] implementation.
/// Unknown names are logged and skipped — they do not abort startup.
///
/// `crawl_token` requires the shared secret from `CRAWL_TOKEN`; pass it via
/// the `crawl_token` argument so this function does not read env vars itself.
///
/// # Adding a new verifier
///
/// Add a `match` arm below and declare it in `src/verifiers/mod.rs`. No other
/// files need to change. See the [module-level docs](self) for the full
/// four-step procedure.
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
                // TODO(logging): replace eprintln! with structured tracing once tracing crate is added
                eprintln!("[verifier_chain] unknown verifier '{unknown}' in VERIFIER_CHAIN — skipping");
                continue;
            }
        };
        verifiers.push(v);
    }

    VerifierChain::new(verifiers)
}
