// Rust guideline compliant (M-APP-ERROR, M-MODULE-DOCS) — 2026-03-09

//! Node-to-node sync protocol types.
//!
//! [`SyncEventsResponse`] is returned by `GET /sync/events` for incremental
//! polling. [`ReconcileRequest`] / [`ReconcileResponse`] implement a
//! negentropy-style set-difference handshake used when a community node
//! rejoins after downtime: the node sends what it has, the primary returns
//! what it is missing and flags any event IDs unknown to the primary.

use serde::{Deserialize, Serialize};
use crate::event::Event;

/// Response for `GET /sync/events?after_seq={n}&limit={m}`.
/// Community nodes poll this to stay current.
#[derive(Debug, Serialize, Deserialize)]
pub struct SyncEventsResponse {
    pub events:   Vec<Event>,
    pub has_more: bool,
    pub next_seq: i64,
}

/// Body for `POST /sync/reconcile`.
///
/// Implements a negentropy-style set-difference handshake.  The community node
/// sends the event IDs it already holds (`have`) and the sequence cursor it
/// diverged from (`since_seq`); the primary returns only what the node is
/// missing and flags anything in `have` that the primary does not recognise.
#[derive(Debug, Deserialize)]
pub struct ReconcileRequest {
    pub node_pubkey: String,
    pub have:        Vec<EventRef>,
    pub since_seq:   i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EventRef {
    pub event_id: String,
    pub seq:      i64,
}

/// Response to `POST /sync/reconcile`.
#[derive(Debug, Serialize)]
pub struct ReconcileResponse {
    /// Events the requesting node is missing; the node should apply these in order.
    pub send_to_node:  Vec<Event>,
    /// Event refs the node reported that the primary does not recognise.
    ///
    /// A non-empty list is an anomaly: it means the node has events that never
    /// passed through this primary, which should not happen in normal operation
    /// and warrants investigation (e.g. data corruption or a rogue writer).
    pub unknown_to_us: Vec<EventRef>,
}
