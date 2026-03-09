// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: enclosure MIME type sanity check.

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Warns when any track enclosure MIME type starts with `"video/"`.
///
/// Video enclosures are unexpected in a music feed index. This verifier does
/// not fail (some music feeds embed music videos), but it flags the submission
/// for audit so operators can decide whether to add a stricter rule.
pub struct EnclosureTypeVerifier;

impl Verifier for EnclosureTypeVerifier {
    fn name(&self) -> &'static str { "enclosure_type" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let Some(feed_data) = &ctx.request.feed_data else {
            return VerifyResult::Pass;
        };
        for track in &feed_data.tracks {
            if let Some(mime) = &track.enclosure_type {
                if mime.starts_with("video/") {
                    return VerifyResult::Warn(format!(
                        "track '{}' has video enclosure type '{mime}'",
                        track.track_guid
                    ));
                }
            }
        }
        VerifyResult::Pass
    }
}
