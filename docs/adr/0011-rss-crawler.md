# ADR 0011: RSS Crawler Implementation

## Status
Accepted

## Context
ADR 0006 established that crawlers are separate HTTP clients that submit to
`POST /ingest/feed`. This ADR records the concrete implementation choices for
the primary RSS crawler.

The crawler must fetch Podcast 2.0 / RSS 2.0 feeds, extract structured data
matching `IngestFeedRequest`, compute a content hash for change detection, and
POST the payload to the stophammer core. It runs as a short-lived process
invoked by an external scheduler (cron, CI, or a podping listener).

## Decision

### Runtime: Bun / TypeScript

Bun was chosen over Python, Go, or a second Rust crate for three reasons:

1. **Ecosystem fit** — the `fast-xml-parser` library provides namespace-aware
   XML parsing that handles `podcast:` and `itunes:` prefixes correctly without
   manual namespace stripping. No equivalent single-package solution exists in
   the Bun stdlib alone.
2. **Fast startup** — Bun starts in ~10 ms, suitable for on-demand invocation
   by a podping watcher or cron job. A compiled Rust binary would be faster but
   the marginal gain does not justify the added build complexity.
3. **Tooling consistency** — the existing `podping_watch.mjs` tool in this
   repository is already JavaScript. Keeping the crawler in the same ecosystem
   reduces the number of runtimes an operator needs to install.

### Why separate process (reference ADR 0006)
ADR 0006 already decided that crawlers run outside the core node. This crawler
follows that decision: it communicates with the core exclusively via
`POST /ingest/feed` and shares no code or process with the Rust binary.

### XML parsing: `fast-xml-parser`

Standard XML libraries in JavaScript either strip namespace prefixes (making
`podcast:guid` inaccessible) or require complex SAX-style event handling.
`fast-xml-parser` preserves prefixed tag names by default when
`removeNSPrefix: false` is set, giving direct access to `podcast:guid`,
`podcast:value`, `podcast:valueRecipient`, and `podcast:valueTimeSplit` without
a separate namespace resolver step.

### Content hash strategy

The SHA-256 is computed over the **raw response bytes** before any decoding or
parsing occurs. This has two benefits:

- The hash is stable regardless of how the XML parser normalises whitespace or
  attribute order.
- A fetch that returns HTTP 4xx/5xx still produces a valid hash (of the error
  body), allowing the core's `ContentHashVerifier` to detect repeated failures
  without re-processing.

### `podcast:guid` absent → empty string

When `podcast:guid` is absent from a feed the crawler sets `feed_guid` to `""`
rather than skipping the feed. The `FeedGuidVerifier` on the core will reject
the submission with a clear error (`invalid uuid: ""`). This surfaces missing
GUIDs in the ingest logs rather than silently dropping the feed.

### `podcast:valueTimeSplit` with `remotePercentage`

Value time splits that carry `remotePercentage` instead of `split` are skipped
by the crawler. The stophammer data model uses split-based routing exclusively;
percentage-based splits would require per-play revenue calculation that is out
of scope for the ingest layer.

## Consequences

- **Stateless** — the crawler holds no database, cache, or queue. Change
  detection is delegated to the core's `ContentHashVerifier` (which queries
  `feed_crawl_cache`). The crawler can be restarted freely without data loss.
- **Scheduling is external** — the crawler is a one-shot process. Periodic
  crawling must be arranged by an external mechanism (cron, systemd timer, or a
  podping watcher that triggers the crawler on new `<podcast:guid>` events).
- **No redirect resolution** — `canonical_url` and `source_url` are both set to
  the input URL. If a feed permanently redirects, the operator must update the
  feed list; the crawler does not follow and record redirect chains.
- **Concurrency** — the `CONCURRENCY` env var (default 5) controls the number
  of in-flight fetches. High values may trigger rate limiting on feed hosts;
  low values increase total wall-clock time for large feed lists.
