// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: payment route splits must sum to 100.

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Rejects submissions where payment route splits for any track do not sum to 100.
///
/// A split total other than 100 indicates a malformed `podcast:value` block
/// and would result in incorrect Lightning payment routing at play time.
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
