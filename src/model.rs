// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Core domain types for the stophammer feed index.
//!
//! Defines the persisted entities: [`Artist`], [`Feed`], [`Track`],
//! [`PaymentRoute`], and [`ValueTimeSplit`]. All types derive `Serialize` and
//! `Deserialize` so they can be embedded in event payloads and returned from
//! API endpoints without additional mapping.

use serde::{Deserialize, Serialize};

// Field names intentionally repeat the struct prefix (e.g. artist_id, feed_guid)
// because these are canonical Podcast Namespace identifiers used verbatim in
// SQLite columns, JSON payloads, and the RSS/Podcast Index spec.
#[expect(clippy::struct_field_names, reason = "field names match Podcast Namespace / DB column conventions")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artist {
    pub artist_id:  String,
    pub name:       String,
    pub name_lower: String,
    pub created_at: i64,
}

#[expect(clippy::struct_field_names, reason = "field names match Podcast Namespace / DB column conventions")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Feed {
    pub feed_guid:      String,
    pub feed_url:       String,
    pub title:          String,
    pub title_lower:    String,
    pub artist_id:      String,
    pub description:    Option<String>,
    pub image_url:      Option<String>,
    pub language:       Option<String>,
    pub explicit:       bool,
    pub itunes_type:    Option<String>,
    pub episode_count:  i64,
    pub newest_item_at: Option<i64>,
    pub oldest_item_at: Option<i64>,
    pub created_at:     i64,
    pub updated_at:     i64,
    /// Verbatim value of the `podcast:medium` tag from the RSS feed, if present.
    pub raw_medium:     Option<String>,
}

#[expect(clippy::struct_field_names, reason = "field names match Podcast Namespace / DB column conventions")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub track_guid:      String,
    pub feed_guid:       String,
    pub artist_id:       String,
    pub title:           String,
    /// Pre-lowercased copy of `title` used for case-insensitive search queries.
    pub title_lower:     String,
    pub pub_date:        Option<i64>,
    pub duration_secs:   Option<i64>,
    pub enclosure_url:   Option<String>,
    pub enclosure_type:  Option<String>,
    pub enclosure_bytes: Option<i64>,
    pub track_number:    Option<i64>,
    pub season:          Option<i64>,
    pub explicit:        bool,
    pub description:     Option<String>,
    pub created_at:      i64,
    pub updated_at:      i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RouteType {
    Node,
    Lnaddress,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaymentRoute {
    pub id:             Option<i64>,
    pub track_guid:     String,
    pub feed_guid:      String,
    pub recipient_name: Option<String>,
    pub route_type:     RouteType,
    pub address:        String,
    pub custom_key:     Option<String>,
    pub custom_value:   Option<String>,
    pub split:          i64,
    /// When `true`, this recipient is an app-fee destination, not an artist split.
    pub fee:            bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueTimeSplit {
    pub id:                Option<i64>,
    /// GUID of the track whose playback triggers this split.
    pub source_track_guid: String,
    pub start_time_secs:   i64,
    pub duration_secs:     Option<i64>,
    pub remote_feed_guid:  String,
    pub remote_item_guid:  String,
    pub split:             i64,
    pub created_at:        i64,
}
