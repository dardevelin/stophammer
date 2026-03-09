// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: podcast:guid validity check.

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Rejects known bad/placeholder `podcast:guid` values and malformed UUIDs.
///
/// Some hosting platforms set the same GUID on thousands of unrelated feeds
/// as their default value. These platform-default GUIDs are collected in
/// [`BAD_GUIDS`] and rejected explicitly.
pub struct FeedGuidVerifier;

/// GUIDs shared by thousands of unrelated feeds (platform defaults).
///
/// Add new entries here as they are discovered in the wild. Each entry should
/// be the exact lowercase UUID string as it appears in the `podcast:guid` tag.
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
