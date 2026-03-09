// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Crawler-facing request and response types for `POST /ingest/feed`.
//!
//! [`IngestFeedRequest`] carries the raw crawl result including the content
//! hash used for change detection, optional parsed [`IngestFeedData`], and the
//! crawl token that gates access. [`IngestResponse`] reports whether the
//! submission was accepted, any verifier warnings, and the IDs of events emitted.

use serde::{Deserialize, Serialize};
use crate::model::RouteType;

/// Full crawler submission for `POST /ingest/feed`.
///
/// `crawl_token` is verified by `CrawlTokenVerifier` as the first step of the
/// [`verify::VerifierChain`]; requests with an invalid token are rejected before
/// any DB access occurs.  `feed_data` is `None` when the crawler could not parse
/// the feed (e.g. HTTP error); the verifier chain still runs so the error can
/// be recorded.
#[derive(Debug, Deserialize)]
pub struct IngestFeedRequest {
    pub canonical_url: String,
    pub source_url:    String,
    pub crawl_token:   String,
    pub http_status:   u16,
    pub content_hash:  String,
    pub feed_data:     Option<IngestFeedData>,
}

/// Parsed feed content supplied by the crawler when the fetch succeeded.
#[derive(Debug, Deserialize)]
pub struct IngestFeedData {
    pub feed_guid:    String,
    pub title:        String,
    pub description:  Option<String>,
    pub image_url:    Option<String>,
    pub language:     Option<String>,
    pub explicit:     bool,
    pub itunes_type:  Option<String>,
    pub raw_medium:   Option<String>,
    pub author_name:  Option<String>,
    pub owner_name:   Option<String>,
    pub pub_date:     Option<i64>,
    pub tracks:       Vec<IngestTrackData>,
}

/// Per-episode data within an [`IngestFeedData`] submission.
#[derive(Debug, Deserialize)]
pub struct IngestTrackData {
    pub track_guid:        String,
    pub title:             String,
    pub pub_date:          Option<i64>,
    pub duration_secs:     Option<i64>,
    pub enclosure_url:     Option<String>,
    pub enclosure_type:    Option<String>,
    pub enclosure_bytes:   Option<i64>,
    pub track_number:      Option<i64>,
    pub season:            Option<i64>,
    pub explicit:          bool,
    pub description:       Option<String>,
    /// Per-track author override — some feeds have different artist per track
    pub author_name:       Option<String>,
    pub payment_routes:    Vec<IngestPaymentRoute>,
    pub value_time_splits: Vec<IngestValueTimeSplit>,
}

/// Ingest-time payment route before a DB row ID is assigned.
///
/// This is the wire representation submitted by the crawler.  It maps 1-to-1
/// onto [`model::PaymentRoute`] except that `id` and the two `*_guid` foreign
/// keys are supplied by the ingest handler rather than the crawler.
#[derive(Debug, Deserialize)]
pub struct IngestPaymentRoute {
    pub recipient_name: Option<String>,
    pub route_type:     RouteType,
    pub address:        String,
    pub custom_key:     Option<String>,
    pub custom_value:   Option<String>,
    pub split:          i64,
    pub fee:            bool,
}

/// Ingest-time value-time-split entry before a DB row ID is assigned.
///
/// Maps 1-to-1 onto [`model::ValueTimeSplit`]; `id` and `source_track_guid`
/// are filled in by the ingest handler from the enclosing track context.
#[derive(Debug, Deserialize)]
pub struct IngestValueTimeSplit {
    pub start_time_secs:  i64,
    pub duration_secs:    Option<i64>,
    pub remote_feed_guid: String,
    pub remote_item_guid: String,
    pub split:            i64,
}

/// Response returned to the crawler after a `POST /ingest/feed` attempt.
#[derive(Debug, Serialize)]
pub struct IngestResponse {
    /// `true` when the submission was accepted and written to the database.
    pub accepted:       bool,
    /// Rejection reason when `accepted` is `false`; `None` on success.
    pub reason:         Option<String>,
    /// UUIDs of events emitted during this ingest, in emission order.
    pub events_emitted: Vec<String>,
    /// `true` when content hash matched the cache and no write was performed.
    pub no_change:      bool,
    /// Non-fatal verifier warnings recorded alongside the events.
    pub warnings:       Vec<String>,
}
