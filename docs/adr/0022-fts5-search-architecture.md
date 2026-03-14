# ADR 0022: FTS5 Contentless Search with Companion Table

## Status
Accepted

## Context

Stophammer provides full-text search over artists, feeds, and tracks via the
`GET /v1/search` endpoint. The search index must support:

- Fast BM25-ranked MATCH queries across multiple text fields (name, title,
  description, tags).
- Inserts and updates as entities are ingested from RSS crawls and event
  replication.
- Deletes when feeds are retired or tracks are removed (ADR 0018, ADR 0009).
- Low storage overhead — the source-of-truth data already lives in the core
  entity tables (`artists`, `feeds`, `tracks`). Duplicating all text in the
  FTS5 index doubles storage for no read-path benefit.

### Why contentless FTS5

SQLite FTS5 supports a `content=''` mode where the inverted index stores term
positions and frequencies for MATCH queries but does not store the original
column values. This cuts index size roughly in half compared to a content-
bearing FTS5 table. The tradeoff: `SELECT` on an FTS5 column returns an empty
string, not the original value.

This tradeoff is acceptable because the search endpoint never needs to read
text back from the FTS5 table — it only needs entity identifiers to look up
the full entity from the core tables.

### The bug this ADR documents

The original implementation stored `entity_type` and `entity_id` as FTS5
columns and read them back in the search query via `SELECT entity_type,
entity_id FROM search_index WHERE search_index MATCH ?`. Because the table is
contentless, those columns silently returned empty strings. Search appeared to
work (rows were returned) but every result had blank `entity_type` and
`entity_id` — making results unusable.

The root cause is an architectural gap: using a contentless FTS5 table requires
an explicit companion table to map FTS5 rowids back to entity identifiers. No
ADR documented this requirement, and the original code did not implement it.

### Rowid management

FTS5 with `content=''` requires the caller to manage rowids explicitly. Without
deterministic rowids, updates and deletes cannot target the correct row.
The rowid must be:

- **Deterministic**: the same `(entity_type, entity_id)` pair must always
  produce the same rowid, so that an update can delete-then-reinsert at the
  same rowid.
- **Stable across Rust toolchain versions**: Rust's `Hash` trait does not
  guarantee cross-version stability. Using it directly would cause index
  corruption after a toolchain upgrade.
- **Collision-resistant**: two distinct entities should not share a rowid.

## Decision

### Companion table `search_entities`

A regular `STRICT` table maps each FTS5 rowid to its `(entity_type,
entity_id)` pair:

```sql
CREATE TABLE IF NOT EXISTS search_entities (
    rowid       INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_search_entities_entity
    ON search_entities(entity_type, entity_id);
```

The index on `(entity_type, entity_id)` supports efficient existence checks
during delete operations.

### Rowid computation: SipHash-2-4 with fixed keys

Rowids are computed by `search::rowid_for(entity_type, entity_id)` using
`SipHash-2-4` from the `siphasher` crate with fixed zero keys. The raw bytes
of `"{entity_type}\0{entity_id}"` are hashed (not via Rust's `Hash` trait), and
the result is masked to 63 bits to guarantee a positive `i64`.

SipHash-2-4 was chosen because:
- It is a stable, published algorithm — the output is defined by the
  specification, not by a compiler or standard library version.
- The `siphasher` crate hashes raw byte input directly, bypassing Rust's
  `Hash` trait entirely.
- 63-bit output space provides sufficient collision resistance for the
  expected entity count (hundreds of thousands of entities).
- The NUL separator between entity_type and entity_id prevents prefix
  collisions (e.g., `("ab", "c")` vs `("a", "bc")`).

### Insert/update: delete-then-insert with existence guard

`populate_search_index` performs:

1. Check `search_entities` for an existing row at the computed rowid.
2. If it exists, issue an FTS5 `'delete'` command with the old column values.
3. Insert the new row into FTS5 with explicit rowid.
4. `INSERT OR REPLACE` into `search_entities` to keep the companion table in
   sync.

The existence guard (step 1) is critical: issuing an FTS5 contentless delete
for a non-existent row corrupts the internal term-frequency statistics, causing
`bm25()` to return NULL for subsequent queries. This was observed during
testing and is a documented SQLite FTS5 behavior.

### Delete: guarded removal from both tables

`delete_from_search_index` checks `search_entities` before issuing the FTS5
delete command (same guard as insert/update), then deletes the companion row.
This is called during `FeedRetired` and `TrackRemoved` event processing
(ADR 0009, ADR 0018).

### Search query: subquery + JOIN

The `search()` function uses a two-level query:

1. Inner subquery: `SELECT rowid, bm25(search_index) AS fts_rank FROM
   search_index WHERE search_index MATCH ?` — this runs inside the FTS5
   context where `bm25()` is valid.
2. Outer query: `JOIN search_entities e ON e.rowid = m.rowid` — resolves
   the entity identifiers from the companion table.

This structure is required because `bm25()` is an FTS5 auxiliary function that
is only valid when the FTS5 table is the primary table in a MATCH query. Moving
the JOIN into the inner query would prevent `bm25()` from functioning.

### Keyset pagination

Search results are paginated using keyset cursors rather than OFFSET. The
cursor encodes the last result's `(effective_rank, rowid)` pair as a
base64-encoded string following the same `base64(value1 \0 value2)` format
used by the other paginated endpoints (`/v1/artists/{id}/feeds`, `/v1/recent`).

The `effective_rank` is a quality-adjusted BM25 score:
`bm25(search_index) * (1.0 + quality_score / 100.0)`. Lower (more negative)
values indicate better matches. The `rowid` (SipHash-2-4 of entity type + id)
serves as a deterministic tiebreaker for results with identical effective rank.

Because `effective_rank` is a floating-point value, the cursor encodes it
losslessly via `f64::to_bits()` / `f64::from_bits()` as a `u64` decimal
string. The handler rejects cursors containing `NaN` or infinity. The keyset
WHERE clause filters to `(effective_rank > cursor_rank) OR
(effective_rank = cursor_rank AND rowid > cursor_rowid)`, matching the
`ORDER BY effective_rank ASC, rowid ASC` sort.

The response uses the same `pagination: { cursor, has_more }` envelope as
all other paginated query endpoints.

## Consequences

- Search results now correctly return `entity_type` and `entity_id` from the
  companion table instead of empty strings from the contentless FTS5 columns.
- The companion table adds minimal storage overhead — one `INTEGER` and two
  `TEXT` columns per indexed entity, far less than duplicating all searchable
  text.
- The `search_entities` table serves double duty as an existence check during
  insert and delete operations, preventing the FTS5 term-frequency corruption
  bug.
- Any future code that adds or removes entities from the search index must
  maintain both `search_index` and `search_entities` in lockstep. The
  `populate_search_index` and `delete_from_search_index` functions in
  `search.rs` are the sole entry points for this — direct SQL against the
  FTS5 table without updating the companion table will cause divergence.
- The deterministic rowid hash is a one-way function. If the hash algorithm
  or key changes, the entire search index must be rebuilt. This is acceptable
  because the index is derived data that can be reconstructed from the core
  entity tables at any time.
