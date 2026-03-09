// Rust guideline compliant (M-MODULE-DOCS) — 2026-03-09

//! Built-in verifier implementations.
//!
//! Each module contains a single [`crate::verify::Verifier`] implementation.
//! To add a new verifier, create a new module here and add a `match` arm to
//! [`crate::verify::build_chain`].
//!
//! See the [`crate::verify`] module docs for the full four-step procedure.

pub mod content_hash;
pub mod crawl_token;
pub mod enclosure_type;
pub mod feed_guid;
pub mod medium_music;
pub mod payment_route_sum;
