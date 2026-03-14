# Stophammer

A quality-gated V4V music index.

## Ecosystem

| Repository | Description |
|---|---|
| **[stophammer](https://github.com/dardevelin/stophammer)** | Primary / community node (this repo) |
| [stophammer-crawler](https://github.com/dardevelin/stophammer-crawler) | Unified feed crawler — one-shot crawl, PodcastIndex import, and Podping listener |
| [stophammer-parser](https://github.com/dardevelin/stophammer-parser) | Declarative RSS/Podcast XML extraction engine (Rust library) |
| [stophammer-tracker](https://github.com/dardevelin/stophammer-tracker) | Cloudflare Workers peer tracker (optional bootstrap) |

## The problem

The Lightning Network enables direct music payments — listeners send sats to artists
as they listen, DJs split payments to the tracks they play. This works beautifully
*if* you can look up a track and trust that its payment routes are real and working.

PodcastIndex is the de-facto feed directory for Podcasting 2.0. It's comprehensive,
but it doesn't enforce V4V participation — it indexes every podcast regardless of
whether it has payment routes, whether those routes are valid, or whether it's
actually music.

## What Stophammer is

Stophammer is a **verified V4V music index**. It only contains RSS feeds that:

- declare `podcast:medium=music`
- carry at least one structurally valid `podcast:value` payment route
  (non-empty address with a positive split — the verifier checks metadata
  presence, not Lightning node reachability or payment delivery)

Every entry has been crawled and passed through a verifier chain. The index is an
**append-only signed event log** — you can verify the integrity of every feed
addition, replicate the full index to your own node, and serve it locally with no
dependency on a central server.

## Who it's for

- **App developers** building V4V music players or DJ tools that need a trustworthy
  source of feed GUIDs and payment routes
- **Node operators** who want a local, independently-verifiable copy of the index
- **Contributors** running crawlers (RSS, Podping real-time, or PodcastIndex bulk
  import) to grow the index

## Architecture

```
[crawler]  →  POST /ingest/feed  →  [primary]  →  POST /sync/push  →  [community nodes]
                                        ↑
                                   verifier chain
                                   signs events
                                   GET /sync/peers   ← primary is its own tracker
```

- **Primary node** — Rust + SQLite. Crawlers POST feeds to `/ingest/feed`.
  A verifier chain gates acceptance. Accepted feeds are written atomically and
  recorded in a signed append-only event log. On each commit the primary
  immediately fans out the new events to all registered community nodes.
- **Community nodes** — receive pushed events from the primary, verify the
  ed25519 signature, apply idempotently. Fall back to polling if no push
  arrives within `PUSH_TIMEOUT_SECS` (default 90s). Register their push URL
  with the primary on startup.
- **Primary as tracker** — `GET /sync/peers` on the primary returns all known
  community nodes. A new node only needs the primary URL to bootstrap.
  The Cloudflare tracker worker is optional and only needed before the primary
  URL is publicly known.
- **Crawlers** — independent untrusted processes, authenticated by `CRAWL_TOKEN`.
  Stophammer does **not** run or schedule crawlers — that is the operator's
  responsibility (cron, systemd timer, external service). Current crawler
  implementations: RSS crawler, Podping listener, PodcastIndex snapshot importer.

## Verifier chain

Every ingest runs through an ordered chain of verifiers on the **primary only**.
Community nodes verify the ed25519 signature and trust the result — they do not
re-run verifiers. The verifier warnings from the primary are stored in each event
as an audit trail and replicated to all nodes.

Default chain:

```
crawl_token → content_hash → medium_music → feed_guid → v4v_payment → enclosure_type
```

| Verifier | Effect |
|---|---|
| `crawl_token` | Rejects invalid crawl tokens |
| `content_hash` | Short-circuits unchanged feeds (no DB write, no event) |
| `medium_music` | Rejects feeds without `podcast:medium=music` |
| `feed_guid` | Rejects malformed or known-bad `podcast:guid` values |
| `v4v_payment` | Rejects feeds with no structurally valid V4V payment routes (requires non-empty address + positive split; does not test reachability) |
| `enclosure_type` | Warns on video MIME type enclosures |

Override at runtime with `VERIFIER_CHAIN=crawl_token,content_hash,...`.
Changing the chain on a primary does not affect community nodes.

---

## Running a primary node

### Credentials

The primary generates an ed25519 signing key at `KEY_PATH` on first start.
**Back this file up.** All events in the network are signed with this key.
If you lose it and restart with a new key, community nodes will reject the new
events (signature mismatch against the stored `signed_by` pubkey). The network
does not break immediately — existing events remain valid — but new events will
be unverifiable by nodes that trusted the old key.

To recover credentials across restarts (Docker, redeployment, etc.), mount
`KEY_PATH` from a persistent volume or bind-mount, and `DB_PATH` similarly.

### Minimal setup

```bash
# Generate a signing key and start a primary
DB_PATH=./stophammer.db \
KEY_PATH=./signing.key \
CRAWL_TOKEN=change-me \
BIND=0.0.0.0:8008 \
./stophammer
```

The primary exposes:

| Endpoint | Description |
|---|---|
| `POST /ingest/feed` | Crawler submission |
| `GET /sync/events` | Paginated event log |
| `POST /sync/reconcile` | Set-diff catch-up for rejoining nodes |
| `POST /sync/register` | Community nodes announce their push URL |
| `GET /sync/peers` | Returns known active peers (primary-as-tracker) |
| `GET /node/info` | Returns this node's pubkey |
| `POST /admin/artists/merge` | Requires `X-Admin-Token` |
| `POST /admin/artists/alias` | Requires `X-Admin-Token` |
| `GET /health` | Liveness probe |

### Get the primary's pubkey

```bash
curl http://your-primary:8008/node/info
# {"node_pubkey":"0805c402..."}
```

Community nodes auto-fetch this on startup. You only need it manually if you
want to pre-configure `PRIMARY_PUBKEY` on community nodes for extra hardening.

---

## Running a community node

Community nodes are read-only replicas. They receive pushed events from the
primary, verify signatures, and serve the same read API.

### Minimal setup

```bash
NODE_MODE=community \
DB_PATH=./stophammer.db \
KEY_PATH=./signing.key \
BIND=0.0.0.0:8008 \
PRIMARY_URL=http://your-primary:8008 \
NODE_ADDRESS=http://this-node-public-url:8008 \
./stophammer
```

On startup, the community node:
1. Fetches `GET {PRIMARY_URL}/node/info` to auto-discover the primary's pubkey
   (retries up to 10 times with 2s delay — handles primary still booting)
2. Registers its push URL with the primary: `POST {PRIMARY_URL}/sync/register`
3. Does an initial fallback poll to catch up from the current cursor
4. Enters the push-receive + fallback-poll loop

### Credentials

Same rule as primary: mount `KEY_PATH` and `DB_PATH` from persistent storage.
The community node's signing key identifies it in `GET /sync/peers`. If the key
changes, the old peer row in the primary's `peer_nodes` table becomes orphaned
(push still works — the node re-registers with its new pubkey and a new row is
created). The old row stays dormant until it accumulates 5 failures and is evicted.

### Fallback poll

If the primary is down or slow, the community node falls back to polling after
`PUSH_TIMEOUT_SECS` (default 90) of silence. The poll interval is `POLL_INTERVAL_SECS`
(default 300). Worst-case catch-up latency after a primary restart: 300 seconds.

### Optional: pin the primary pubkey

If you want community nodes to reject events signed by any key other than the
known primary (stronger trust model):

```bash
PRIMARY_PUBKEY=0805c402f021e6e0dfbb6b2f5d34628f7b166b075a0170e6e5e293c50b3b55e2
```

Without this, the pubkey is auto-discovered from `/node/info` at startup.

---

## Running the full network locally (Docker)

```bash
cd hey-v4v

# Bring up primary + 3 community nodes
CRAWL_TOKEN=secret ADMIN_TOKEN=admintoken docker compose up -d

# Run the crawler against some feeds (one-shot)
CRAWL_TOKEN=secret ADMIN_TOKEN=admintoken \
  FEED_URLS="https://feeds.rssblue.com/stereon-music
https://feeds.rssblue.com/ainsley-costello-love-letter" \
  docker compose run --rm crawler

# Watch push fan-out in real time
docker compose logs -f primary community1 community2 community3

# Check which peers are registered
curl http://localhost:8008/sync/peers

# Check event counts across all nodes
for port in 8008 8009 8010 8011; do
  echo -n "port $port: "
  curl -s "http://localhost:$port/sync/events?after_seq=0&limit=1000" \
    | bun -e "const d=await (await fetch('http://localhost:$port/sync/events?after_seq=0&limit=1000')).json(); console.log('seq='+d.next_seq)"
done
```

The compose file runs no crawlers by default. Crawlers are one-shot containers
you invoke manually or via a scheduler. Stophammer does not schedule them.

### Override the verifier chain (dev/test)

```bash
# Skip medium_music for feeds that don't set podcast:medium yet
CRAWL_TOKEN=secret ADMIN_TOKEN=admintoken \
  VERIFIER_CHAIN=crawl_token,content_hash,v4v_payment,enclosure_type \
  docker compose up -d primary
```

### Persistent credentials across compose restarts

By default, `docker compose up` preserves named volumes (`primary-data`,
`community1-data`, etc.) across restarts — signing keys and databases survive
`docker compose down` and `up` cycles.

To fully reset (wipe all state):
```bash
docker compose down -v   # removes volumes too
```

To back up a signing key from a running container:
```bash
docker compose cp primary:/data/signing.key ./primary-signing.key.bak
```

---

## Running crawlers

Stophammer does not run or schedule crawlers. Crawlers are separate processes
that authenticate with `CRAWL_TOKEN` and POST to `/ingest/feed`.

### RSS crawler

All crawling modes live in [stophammer-crawler](https://github.com/dardevelin/stophammer-crawler):

```bash
# One-shot: crawl specific feed URLs
CRAWL_TOKEN=secret stophammer-crawler crawl https://feeds.rssblue.com/stereon-music

# From a file
CRAWL_TOKEN=secret stophammer-crawler crawl feeds.txt
```

Schedule with cron:
```
0 */6 * * *  CRAWL_TOKEN=secret INGEST_URL=http://primary:8008/ingest/feed \
             stophammer-crawler crawl /etc/stophammer/feeds.txt
```

### Podping listener

Listens to the Podping WebSocket stream and submits music feeds in real time:

```bash
CRAWL_TOKEN=secret stophammer-crawler podping --concurrency 3
```

### PodcastIndex snapshot importer

Bulk-imports from a PodcastIndex database snapshot:

```bash
CRAWL_TOKEN=secret stophammer-crawler import --db /path/to/podcastindex_feeds.db
```

---

## What community nodes do NOT do

- **Do not re-run verifiers.** The verifier chain runs on the primary only.
  Community nodes verify the ed25519 signature — if it's valid and signed by
  the known primary key, the event is accepted. The `warnings` field in each
  event carries the primary's verifier output as an audit trail.
- **Do not ingest feeds.** The `POST /ingest/feed` endpoint is primary-only.
- **Do not sign events.** Community nodes have a signing key for identity
  (peer registration) but never sign events. All events in the log are signed
  by the primary.
