// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Verifier: podcast:medium tag must be "music".

use crate::verify::{IngestContext, Verifier, VerifyResult};

/// Rejects feeds where `podcast:medium` is explicitly set to a non-music value.
///
/// When `podcast:medium` is absent the verifier warns rather than failing,
/// because adoption of the tag is near-zero in the wild. Operators that want
/// strict enforcement should pair this with a classifier verifier that
/// scores feeds without the tag.
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
