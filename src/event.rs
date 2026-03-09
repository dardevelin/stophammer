// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Event types and signing payload for the stophammer sync protocol.
//!
//! [`Event`] is the immutable sync primitive replicated between all nodes.
//! Each event carries an [`EventPayload`] (one of several domain-specific
//! variants), an ed25519 signature over [`EventSigningPayload`], and a
//! monotonic `seq` assigned by the primary at commit time.
//!
//! `seq` is intentionally excluded from the signing payload — it is a
//! delivery-ordering field and does not affect content integrity.

use serde::{Deserialize, Serialize};
use crate::model::{Artist, Feed, PaymentRoute, Track, ValueTimeSplit};

/// Discriminant identifying which domain action produced an [`Event`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    /// A feed was created or its metadata changed.
    FeedUpserted,
    /// A feed was permanently removed from the index.
    FeedRetired,
    /// A track was created or its metadata/payment routes changed.
    TrackUpserted,
    /// A track was deleted from a feed.
    TrackRemoved,
    /// An artist record was created or its display name changed.
    ArtistUpserted,
    /// The full set of payment routes for a track was atomically replaced.
    RoutesReplaced,
}

/// Typed payload carried inside an [`Event`]; variant mirrors [`EventType`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum EventPayload {
    /// Payload for a feed create-or-update event.
    FeedUpserted(FeedUpsertedPayload),
    /// Payload for a feed removal event.
    FeedRetired(FeedRetiredPayload),
    /// Payload for a track create-or-update event.
    TrackUpserted(TrackUpsertedPayload),
    /// Payload for a track deletion event.
    TrackRemoved(TrackRemovedPayload),
    /// Payload for an artist create-or-update event.
    ArtistUpserted(ArtistUpsertedPayload),
    /// Payload for an atomic payment-route replacement event.
    RoutesReplaced(RoutesReplacedPayload),
}

/// The full signed event — the sync primitive between all nodes.
#[expect(clippy::struct_field_names, reason = "event_id and event_type are canonical field names in the protocol")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event_id:     String,
    pub event_type:   EventType,
    pub payload:      EventPayload,
    pub subject_guid: String,
    pub signed_by:    String,       // hex ed25519 pubkey
    pub signature:    String,       // hex ed25519 sig over sha256(EventSigningPayload)
    pub seq:          i64,          // monotonic, assigned by primary at commit
    pub created_at:   i64,          // unix seconds
    pub warnings:     Vec<String>,  // verifier warnings stored for audit
}

/// Canonical byte representation that is hashed and signed with ed25519.
///
/// `payload_json` is pre-serialized to avoid any re-encoding ambiguity that
/// could arise from round-tripping through a typed value.
///
/// `seq` is intentionally **excluded**: it is a delivery-ordering cursor
/// assigned by the primary at commit time and may legitimately differ between
/// replicas.  Including it would tie the signature to one replica's ordering
/// rather than to the content itself, making cross-node verification impossible.
#[derive(Debug, Serialize)]
pub struct EventSigningPayload<'a> {
    pub event_id:     &'a str,
    pub event_type:   &'a EventType,
    pub payload_json: &'a str,
    pub subject_guid: &'a str,
    pub created_at:   i64,
}

// ── Payload types ──────────────────────────────────────────────────────────

/// Emitted when a feed is created or any of its metadata fields change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedUpsertedPayload {
    pub feed:   Feed,
    pub artist: Artist,
}

/// Emitted when a feed is permanently removed from the index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedRetiredPayload {
    pub feed_guid: String,
    pub reason:    Option<String>,
}

/// Emitted when a track is created or its metadata, routes, or time-splits change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackUpsertedPayload {
    pub track:             Track,
    pub routes:            Vec<PaymentRoute>,
    pub value_time_splits: Vec<ValueTimeSplit>,
}

/// Emitted when a track is deleted from its parent feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackRemovedPayload {
    pub track_guid: String,
    pub feed_guid:  String,
}

/// Emitted when an artist record is created or its display name changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtistUpsertedPayload {
    pub artist: Artist,
}

/// Emitted when the full payment-route set for a track is atomically replaced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutesReplacedPayload {
    pub track_guid: String,
    pub routes:     Vec<PaymentRoute>,
}
