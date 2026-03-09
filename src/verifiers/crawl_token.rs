// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: crawl token gate.

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Rejects requests with an invalid crawl token.
///
/// This verifier should always be first in the chain — it gates all other
/// checks so that unauthenticated crawlers are rejected before any DB access.
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
