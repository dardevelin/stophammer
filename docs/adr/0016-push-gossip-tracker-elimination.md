# ADR 0016 — Push-Based Gossip and Tracker Elimination

**Date:** 2026-03-09
**Status:** Accepted

## Context

Community nodes previously polled the primary every 30 seconds (`GET /sync/events`).
This produced continuous noise even when no new content had arrived, kept the
Cloudflare tracker as a hard dependency for peer discovery, and meant ingest-to-replica
latency was bounded by the poll interval rather than by network round-trip time.

Three problems drove this change:

1. **Polling is wasteful.** 30-second polls against a primary that ingests a few
   hundred feeds per day generate thousands of no-op requests.
2. **The Cloudflare tracker is a single point of failure.** Any new community node
   needed tracker availability to discover peers. If the tracker worker was down,
   nodes could not bootstrap.
3. **Replication latency is unnecessarily high.** A feed ingested one second after
   a poll had to wait up to 29 more seconds before replicas saw it.

## Decision

### Push fan-out from primary

After each `POST /ingest/feed` transaction commits, the primary fires-and-forgets
a `POST /sync/push` delivery to every registered peer. Deliveries are independent
goroutine-style tasks — a slow peer cannot block other peers or the ingest response.

Peers are tracked in a new `peer_nodes` table (added to `schema.sql`). An in-memory
`push_subscribers` cache (seeded at startup from the DB) avoids a DB read per ingest.
Consecutive delivery failures are counted; a peer is evicted from the cache after 5
failures (the DB row is kept; re-registration via `POST /sync/register` resets the
counter and re-populates the cache).

### Community node push receiver

Community nodes expose `POST /sync/push` on their existing port. Events are verified
(ed25519 signature against the configured `PRIMARY_PUBKEY`) and applied idempotently
via `INSERT OR IGNORE`. The handler updates `last_push_at`, which the poll-loop checks.

### Poll loop becomes a fallback

The poll loop still runs but fires only when no push has been received in
`PUSH_TIMEOUT_SECS` (default 90). The loop sleep interval becomes
`POLL_INTERVAL_SECS` (default 300). Worst-case fallback latency after a primary
restart is therefore 300 seconds — acceptable for a crash-recovery path.

Existing deployments that have not upgraded the primary continue to work unchanged:
`last_push_at` stays at 0, so every poll-loop iteration falls back to polling,
preserving the old behaviour exactly.

### Primary as tracker (`GET /sync/peers`)

The primary accumulates a peer list via `POST /sync/register` (called by community
nodes on startup). `GET /sync/peers` on the primary returns the current active peer
list (consecutive\_failures < 5). Any new community node that knows the primary URL
can call `GET /sync/peers` instead of the Cloudflare tracker.

### Cloudflare tracker demoted to optional bootstrap

Community nodes still call `POST /nodes/register` on the Cloudflare tracker at
startup (fire-and-forget, as before). The tracker remains useful only for nodes that
do not know the primary URL in advance. Once a node knows the primary URL, it can
derive the full peer list from `GET /sync/peers` and never needs the tracker again.

## Consequences

**Positive:**
- Ingest-to-replica latency drops from up to 30s to sub-second under normal operation.
- No-op poll traffic eliminated during quiet periods.
- Primary URL alone is sufficient to bootstrap a new node; Cloudflare tracker is no
  longer on the critical path.
- Crash recovery (primary restart, network partition) is handled gracefully by the
  fallback poll loop — no manual intervention required.
- `insert_event_idempotent` ensures that a push delivery of an event already applied
  by a concurrent fallback poll is a safe no-op.

**Negative / trade-offs:**
- Primary must maintain an outbound HTTP connection pool for fan-out. Under large
  peer counts this increases primary memory and connection overhead.
- Peers with 5 consecutive failures are silently evicted from the cache. They must
  re-register. A future improvement could expose a health dashboard.
- `PRIMARY_PUBKEY` must be configured correctly on community nodes; mismatched keys
  cause all pushed events to be rejected with a log line but no alert.

## Long-term

The Cloudflare tracker worker (`stophammer-tracker`) can be deprecated once all
known community nodes have upgraded and `GET /sync/peers` is deployed. The tracker
codebase should be archived rather than deleted, as it may be useful as a bootstrap
for new deployments where the primary URL is not yet publicly known.
