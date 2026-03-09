# ADR 0008: Cloudflare Workers Tracker — Implementation Decisions

## Status
Accepted (supersedes ADR-0007)

## Context
ADR-0007 proposed using Cloudflare Workers with Durable Objects as a tracker and bootstrap layer for stophammer node discovery. This ADR records the concrete implementation decisions made when that layer was built.

The tracker directory lives at `stophammer-tracker/` as a standalone TypeScript/Wrangler project, separate from the core Rust binary. It is deployed independently to Cloudflare Workers.

## Decisions

### 1. Durable Object storage, not KV or D1

Node records are stored in a single `NodeRegistry` Durable Object using its built-in `ctx.storage` (a transactional SQLite-backed key-value store local to the DO). This was chosen over Workers KV (eventually-consistent, unsuitable for a live-node list) and D1 (adds a dependency with no benefit at this scale).

Each node occupies exactly one key: `node:<pubkey>`. A `list({ prefix: "node:" })` scan on read is acceptable given the expected node count (tens to low hundreds).

### 2. Single "global" Durable Object instance

All requests are routed to a DO instance derived from the fixed name `"global"` (`idFromName("global")`). The registry is a single logical entity — sharding by pubkey prefix or geography adds complexity with no benefit at V4V network scale. If the instance migrates due to Cloudflare infrastructure changes, `idFromName` guarantees the same stable ID.

### 3. No WebSocket fanout in v1

ADR-0007 included a WebSocket fanout channel so the primary node could push new event notifications to community nodes. This has been deferred: community nodes already poll `GET /sync/events` on the Rust primary. The tracker's sole v1 responsibility is node discovery. WebSocket fanout will be addressed in a future ADR when push latency becomes a measurable problem.

### 4. No R2 artwork CDN in v1

R2 artwork hosting was proposed in ADR-0007 and is likewise deferred. Artwork URLs are served directly from feed enclosures. R2 will be added when a CDN caching layer is needed for client performance.

### 5. CORS policy: Access-Control-Allow-Origin: *

The tracker is a public discovery endpoint. Any stophammer client (browser wallet, mobile app, community node) must be able to call it. Wildcard CORS is appropriate; there is no credential or cookie involved.

### 6. 10-minute liveness window for /nodes/find

A node is considered live if its `last_seen` timestamp is within the last 600 seconds. Nodes are expected to re-register on startup and periodically (interval TBD by each node's implementation). Stale entries are not explicitly deleted — they fall out of `GET /nodes/find` results naturally. The DO storage is not expected to grow unbounded in practice, but a periodic cleanup migration can be added later if needed.

### 7. TypeScript strict mode, ES2022, ESNext modules

The project uses `"strict": true` in tsconfig. `moduleResolution: "Bundler"` is required by Wrangler v3. No runtime dependencies; `wrangler` is the only dev dependency.

## Consequences
- The tracker can be deployed with `wrangler deploy` from the `stophammer-tracker/` directory. No infrastructure provisioning is required.
- The Cloudflare free tier (100,000 requests/day, Durable Object storage included) covers the bootstrap phase.
- Nodes that already know each other are unaffected if the tracker is unavailable; only new node discovery requires the tracker.
- WebSocket fanout and R2 will each require their own ADR when implemented.
