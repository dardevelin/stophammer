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

/// Body for `POST /sync/push` sent by the primary to peer community nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct PushRequest {
    pub events: Vec<Event>,
}

/// Response from `POST /sync/push` on community nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct PushResponse {
    pub applied:   usize,
    pub rejected:  usize,
    pub duplicate: usize,
}

/// Body for `POST /sync/register` sent by community nodes to the primary.
#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterRequest {
    pub node_pubkey: String,
    pub node_url:    String,
}

/// Response from `POST /sync/register` on the primary.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub ok: bool,
}

/// Entry in the `GET /sync/peers` response.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeerEntry {
    pub node_pubkey: String,
    pub node_url:    String,
    pub last_push_at: Option<i64>,
}

/// Response from `GET /sync/peers` on the primary.
#[derive(Debug, Serialize, Deserialize)]
pub struct PeersResponse {
    pub nodes: Vec<PeerEntry>,
}
