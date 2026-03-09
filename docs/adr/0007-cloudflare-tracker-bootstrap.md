# ADR 0007: Cloudflare Workers as Tracker and Bootstrap Layer

## Status
Superseded by ADR-0008

## Context
Community nodes need a way to discover each other and bootstrap sync. In BitTorrent, this is done by a tracker or DHT. For stophammer, nodes need to:

1. Register their public key and address with a well-known endpoint
2. Clients need to find nodes to query for music data
3. The primary node needs to fanout new events to subscribed community nodes in near-realtime

A self-hosted tracker has an obvious single-point-of-failure problem. A fully decentralized DHT is complex to implement and operate.

## Decision
We will use Cloudflare Workers with Durable Objects as a lightweight tracker and bootstrap layer:

- **Node registry**: A Durable Object stores `{pubkey, address, last_seen}` for all registered nodes
- **`/nodes/register`**: Nodes POST their address and pubkey on startup
- **`/nodes/find`**: Clients GET a list of healthy nodes to query
- **Fanout WebSocket**: Community nodes maintain a WebSocket to the CF Worker; the primary pushes new event notifications
- **R2**: Artwork CDN — nodes upload cover art to R2, clients fetch from CDN URL

This mirrors the BitTorrent tracker model: the tracker is only needed for discovery, not for data transfer. The actual event sync is node-to-node.

## Consequences
- Cloudflare's free tier (100,000 requests/day on Workers, 1 GB R2 storage) is sufficient for the bootstrap phase.
- The tracker is a single logical entity but runs on Cloudflare's globally distributed infrastructure — no single point of failure in practice.
- If Cloudflare becomes unavailable, nodes that already know each other continue to sync; only new node discovery is affected.
- This layer is not yet implemented — it is planned for the next development phase.
