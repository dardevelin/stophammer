# ADR 0012: Podping Listener for Real-Time Music Feed Discovery

## Status
Accepted

## Context
Stophammer needs to discover new and updated music feeds as quickly as possible. The naive alternative — periodic polling of the Podcast Index database — introduces latency measured in hours and requires crawling feeds that have not changed. Podping is the standard real-time gossip layer for the podcast namespace ecosystem: publishers fire a podping on the Hive blockchain whenever a feed changes, and a WebSocket relay at `wss://api.livewire.io/ws/podping` re-broadcasts those pings within seconds.

A separate listener process that subscribes to Podping and forwards relevant URLs to the stophammer ingest endpoint gives near-real-time discovery without touching the core Rust binary or the database schema.

## Decisions

### 1. Watch Podping over WebSocket

The `wss://api.livewire.io/ws/podping` relay aggregates all Hive Podping operations and re-emits them as JSON blocks. Subscribing here costs no Hive RPC infrastructure and requires no blockchain node. The relay is operated by Livewire (the same team behind Podping v1.x) and has been stable in production since 2022.

Each block contains one or more operations. Each operation has:
- `iris` (v1.x) or `urls` (v0.x legacy) — the announced feed URLs
- `medium` — podcast namespace medium tag (absent on v0.x)
- `reason` — "update", "live", "newValueBlock", or absent

### 2. Filter on medium=music AND medium-absent

`medium="music"` is the unambiguous signal that a feed is a music podcast. However, adoption of the `podcast:medium` tag remains near-zero in practice: the overwhelming majority of Podping operations omit the field entirely (they originate from v0.x tooling that predates the medium tag). Dropping all medium-absent pings would eliminate most of the stream that might contain music feeds.

The decision is to accept both cases and rely on `MediumMusicVerifier` (already part of the stophammer verifier chain) to sort them out at ingest time:
- `medium="music"` — accepted, passes `MediumMusicVerifier` cleanly.
- `medium` absent — accepted, triggers a `MediumMusicVerifier` warning ("inferred as music") that is stored with the ingest event for audit. The feed is not rejected.
- `medium` present and not "music" — dropped at the listener; no crawl is queued.

`reason="newValueBlock"` is explicitly excluded. These are Value-for-Value payment streaming events that carry no feed content change; crawling them wastes resources and adds noise to the change-detection hash cache.

### 3. Bun/TypeScript as the runtime

Bun was chosen for three reasons:
1. **Native WebSocket**: `new WebSocket(...)` works out of the box in Bun; no `ws` or `undici` dependency is needed.
2. **Native `fetch`**: the crawl step uses standard `fetch` with `AbortSignal.timeout`; again, no extra dependency.
3. **Native `crypto.subtle`**: SHA-256 hashing uses the built-in Web Crypto API, not a Node.js `crypto` shim.

The result is a zero-dependency runtime image: the only `devDependencies` are TypeScript and `@types/bun` for type checking. The listener runs as `bun src/index.ts` with no build step.

This is consistent with the ADR-0006 decision that crawlers may be written in any language and deployed independently.

### 4. Reconnection with exponential backoff

The WebSocket relay is a third-party service. Network partitions and relay restarts are expected. The listener reconnects automatically using exponential backoff:
- Initial delay: 1 s
- On each successive failure: `delay = Math.min(delay * 2, 60 s)`
- On first successful message received: reset delay to 1 s

This prevents hammering a restarting relay while converging quickly once the relay recovers. The backoff resets on message receipt (not just on `open`) to guard against connections that open but immediately produce errors before delivering any data.

### 5. Bounded worker pool for crawl concurrency

Podping bursts can announce dozens of URLs simultaneously. A bounded `WorkQueue` with a configurable `CONCURRENCY` (default 3) prevents unbounded concurrent fetches that would overwhelm either the remote RSS hosts or the stophammer ingest endpoint. Items accumulate in the in-memory FIFO queue during bursts and drain at the pool rate.

The queue is in-memory and non-persistent. If the listener process restarts, queued-but-not-yet-crawled URLs are lost. This is acceptable: the next podping from the same publisher will re-announce the URL if the feed continues to update.

## Consequences

- The listener is **stateless**: it holds no database and no persistent queue. A restart loses any URLs that were enqueued but not yet crawled. This is intentional — stophammer's `ContentHashVerifier` suppresses no-change re-crawls, so a redundant crawl on restart is cheap.
- **No deduplication** at the listener level: if the same URL appears in two rapid podpings (e.g., a publisher fires twice in 30 s), both are enqueued and crawled. The `ContentHashVerifier` will mark the second as `no_change` without writing to the database.
- `MediumMusicVerifier` in the stophammer verifier chain acts as the real filter. The listener casts a wide net (medium-absent included); the verifier chain catches non-music feeds that slip through. Warnings are stored with the event record.
- The `CRAWL_TOKEN` secret must be shared with the listener via the `CRAWL_TOKEN` environment variable. This is consistent with how all other crawlers authenticate (ADR-0006).
- Feed XML parsing in the listener duplicates logic from (the future) stophammer-crawler. A `// TODO` comment marks the duplication; extracting to a `@stophammer/parser` package is deferred until the crawler exists.
