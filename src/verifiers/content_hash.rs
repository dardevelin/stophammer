// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: content hash deduplication.

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Returns a sentinel `Fail("NO_CHANGE")` when the content hash matches the
/// last crawl. The ingest handler treats [`NO_CHANGE_SENTINEL`] as a no-op,
/// not a true rejection.
///
/// This verifier relies on the `feed_crawl_cache` table via the DB connection
/// in [`IngestContext`]. It is intentionally a built-in (not movable to an
/// external script) because it requires direct DB access for correctness.
pub struct ContentHashVerifier;

/// Sentinel returned by [`ContentHashVerifier`] when a feed is unchanged.
///
/// The ingest handler checks for this specific string to distinguish a
/// no-op short-circuit from a real verification failure. It is defined here
/// so callers do not need to hardcode the string.
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
