// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Feed-ingest verification pipeline.
//!
//! [`VerifierChain`] runs a sequence of [`Verifier`] implementations against
//! an [`IngestContext`]. Each verifier returns [`VerifyResult::Pass`],
//! [`VerifyResult::Warn`] (accepted but flagged for audit), or
//! [`VerifyResult::Fail`] (rejected — stops the chain).
//!
//! The built-in verifiers are:
//! - [`CrawlTokenVerifier`] — gates access to known crawlers.
//! - [`MediumMusicVerifier`] — rejects non-music feeds.
//! - [`FeedGuidVerifier`] — rejects known-bad and malformed GUIDs.
//! - [`PaymentRouteSumVerifier`] — rejects tracks with splits ≠ 100.
//! - [`ContentHashVerifier`] — short-circuits unchanged feeds via [`NO_CHANGE_SENTINEL`].
//! - [`EnclosureTypeVerifier`] — warns on video enclosure MIME types.

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

pub trait Verifier: Send + Sync {
    fn name(&self) -> &'static str;
    fn verify(&self, ctx: &IngestContext) -> VerifyResult;
}

// ── Chain ──────────────────────────────────────────────────────────────────

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
    /// Returns `Ok(warnings)` when all verifiers pass or warn.  Stops at the
    /// first [`VerifyResult::Fail`] and returns `Err(reason)`.
    ///
    /// # Errors
    ///
    /// Returns the formatted rejection reason on the first `Fail`.
    /// Note: [`ContentHashVerifier`] uses [`NO_CHANGE_SENTINEL`] as the
    /// rejection string to signal a no-op rather than a true error — callers
    /// must distinguish this sentinel from a real failure.
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

// ── Built-in verifiers ─────────────────────────────────────────────────────

/// Rejects requests with an invalid crawl token.
pub struct CrawlTokenVerifier {
    pub expected: String,
}

impl Verifier for CrawlTokenVerifier {
    fn name(&self) -> &'static str { "crawl_token" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        if ctx.request.crawl_token == self.expected {
            VerifyResult::Pass
        } else {
            VerifyResult::Fail("invalid crawl token".into())
        }
    }
}

/// Rejects feeds where podcast:medium is not "music".
/// Warns (does not fail) when medium is absent — allows inference-based
/// ingestion while flagging it for review.
pub struct MediumMusicVerifier;

impl Verifier for MediumMusicVerifier {
    fn name(&self) -> &'static str { "medium_music" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        match ctx.request.feed_data.as_ref().and_then(|f| f.raw_medium.as_deref()) {
            Some("music") => VerifyResult::Pass,
            Some(other)   => VerifyResult::Fail(format!("medium is '{other}', not 'music'")),
            None          => VerifyResult::Warn("podcast:medium absent — inferred as music".into()),
        }
    }
}

/// Rejects known bad/placeholder podcast:guid values and malformed UUIDs.
pub struct FeedGuidVerifier;

/// GUIDs shared by thousands of unrelated feeds (platform defaults).
///
/// Feeds presenting one of these GUIDs are rejected. Add new entries as they
/// are discovered in the wild.
const BAD_GUIDS: &[&str] = &[
    "c9c7bad3-4712-514e-9ebd-d1e208fa1b76",
];

impl Verifier for FeedGuidVerifier {
    fn name(&self) -> &'static str { "feed_guid" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let Some(guid) = ctx.request.feed_data.as_ref().map(|f| f.feed_guid.as_str()) else {
            return VerifyResult::Pass; // fetch failed — handled elsewhere
        };
        if BAD_GUIDS.contains(&guid) {
            return VerifyResult::Fail(format!("known bad guid: {guid}"));
        }
        if uuid::Uuid::parse_str(guid).is_err() {
            return VerifyResult::Fail(format!("invalid uuid: {guid}"));
        }
        VerifyResult::Pass
    }
}

/// Rejects if payment route splits for any track do not sum to 100.
pub struct PaymentRouteSumVerifier;

impl Verifier for PaymentRouteSumVerifier {
    fn name(&self) -> &'static str { "payment_route_sum" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let Some(feed_data) = &ctx.request.feed_data else {
            return VerifyResult::Pass;
        };
        for track in &feed_data.tracks {
            if track.payment_routes.is_empty() {
                continue;
            }
            let sum: i64 = track.payment_routes.iter().map(|r| r.split).sum();
            if sum != 100 {
                return VerifyResult::Fail(format!(
                    "track '{}' payment splits sum to {sum} (expected 100)",
                    track.track_guid
                ));
            }
        }
        VerifyResult::Pass
    }
}

/// Returns a sentinel `Fail("NO_CHANGE")` if the content hash is identical
/// to the last crawl. The ingest handler treats `NO_CHANGE` as a no-op,
/// not a real error.
pub struct ContentHashVerifier;

pub const NO_CHANGE_SENTINEL: &str = "NO_CHANGE";

impl Verifier for ContentHashVerifier {
    fn name(&self) -> &'static str { "content_hash" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let last_hash: Option<String> = ctx.db
            .query_row(
                "SELECT content_hash FROM feed_crawl_cache WHERE feed_url = ?1",
                [&ctx.request.canonical_url],
                |row| row.get(0),
            )
            .ok();

        match last_hash {
            Some(h) if h == ctx.request.content_hash => {
                VerifyResult::Fail(NO_CHANGE_SENTINEL.into())
            }
            _ => VerifyResult::Pass,
        }
    }
}

/// Warns if any track enclosure MIME type looks like video rather than audio.
pub struct EnclosureTypeVerifier;

impl Verifier for EnclosureTypeVerifier {
    fn name(&self) -> &'static str { "enclosure_type" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let Some(feed_data) = &ctx.request.feed_data else {
            return VerifyResult::Pass;
        };
        for track in &feed_data.tracks {
            if let Some(mime) = &track.enclosure_type
                && mime.starts_with("video/")
            {
                return VerifyResult::Warn(format!(
                    "track '{}' has video enclosure type '{mime}'",
                    track.track_guid
                ));
            }
        }
        VerifyResult::Pass
    }
}
