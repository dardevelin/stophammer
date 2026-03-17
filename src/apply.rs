//! Shared event-application logic used by both the poll-loop fallback and the
//! push-receiver handler in community mode.
//!
//! `apply_single_event` writes one verified event to the local DB idempotently.
//! `apply_events` verifies signatures and drives the batched apply loop,
//! returning counts of applied/duplicate/rejected events.

use std::sync::Arc;

use crate::{api, db, db_pool, event, signing};

/// Fixed cursor key for primary-sync progress tracking.
///
/// Both the push handler and the poll-loop fallback use this single well-known
/// key (instead of per-pubkey keys) so that push-advanced cursors are visible
/// to poll, and vice versa.
// Issue-CURSOR-IDENTITY — 2026-03-14
pub const SYNC_CURSOR_KEY: &str = "primary_sync_cursor";

// ── ApplyOutcome ─────────────────────────────────────────────────────────────

/// Per-event outcome returned by [`apply_single_event`].
// CRIT-03 Debug derive — 2026-03-13
#[derive(Debug)]
pub enum ApplyOutcome {
    /// Event was new and written to the DB; contains the assigned `seq`.
    Applied(i64),
    /// Event already existed in the DB (`INSERT OR IGNORE` was a no-op).
    Duplicate,
}

// ── ApplySummary ─────────────────────────────────────────────────────────────

/// Aggregate counts returned by [`apply_events`].
// CRIT-03 Debug derive — 2026-03-13
// Issue-BATCH-APPLY — 2026-03-16: promoted to pub for integration testing
#[derive(Debug)]
pub struct ApplySummary {
    pub applied:   usize,
    pub duplicate: usize,
    pub rejected:  usize,
    /// Highest seq applied during this batch (0 if nothing was applied).
    pub max_seq:   i64,
}

// ── apply_single_event_inner ────────────────────────────────────────────────

/// Inner implementation that applies a single **pre-verified** event within an
/// existing connection/transaction scope. Does NOT acquire the writer lock or
/// open its own transaction — the caller is responsible for both.
///
/// Returns `(ApplyOutcome, max_event_seq)` where `max_event_seq` is the
/// primary-side seq from the event (used for cursor updates by the batch
/// wrapper).
///
/// # Errors
///
/// Returns `DbError` if any database operation fails.
// Issue-BATCH-APPLY — 2026-03-16
#[expect(clippy::too_many_lines, reason = "single event-application match covering all EventPayload variants")]
fn apply_single_event_inner(
    conn: &rusqlite::Connection,
    ev: &event::Event,
) -> Result<ApplyOutcome, db::DbError> {
    // Issue-DEDUP-ORDER — 2026-03-14
    // Insert the event row FIRST (dedup guard). If the event_id already
    // exists, `INSERT OR IGNORE` returns no rows and we skip all mutations.
    let seq_opt = db::insert_event_idempotent(
        conn,
        &ev.event_id,
        &ev.event_type,
        &ev.payload_json,
        &ev.subject_guid,
        &ev.signed_by,
        &ev.signature,
        ev.created_at,
        &ev.warnings,
    )?;

    let Some(seq) = seq_opt else {
        // Duplicate event — already applied; return early.
        return Ok(ApplyOutcome::Duplicate);
    };

    // Issue-PAYLOAD-INTEGRITY — 2026-03-14
    // Re-derive the payload from `payload_json` (the signed bytes) instead of
    // trusting `ev.payload`.  This closes a MITM vector where an attacker
    // could alter the deserialized struct without breaking the signature,
    // which only covers `payload_json`.
    let et_str = serde_json::to_string(&ev.event_type)?;
    let et_str = et_str.trim_matches('"');
    let tagged = format!(r#"{{"type":"{et_str}","data":{}}}"#, ev.payload_json);
    let verified_payload: event::EventPayload = serde_json::from_str(&tagged)?;

    match &verified_payload {
        event::EventPayload::ArtistUpserted(p) => {
            db::upsert_artist_if_absent(conn, &p.artist)?;
            // Recompute artist quality + search index
            let score = crate::quality::compute_artist_quality(conn, &p.artist.artist_id)?;
            crate::quality::store_quality(conn, "artist", &p.artist.artist_id, score)?;
            crate::search::populate_search_index(
                conn, "artist", &p.artist.artist_id,
                &p.artist.name, "",
                "",
                "",
            )?;
        }
        event::EventPayload::FeedUpserted(p) => {
            db::upsert_artist_if_absent(conn, &p.artist)?;
            // Ensure artist credit exists before upserting feed
            upsert_artist_credit_if_absent(conn, &p.artist_credit)?;
            db::upsert_feed(conn, &p.feed)?;
            // Recompute feed quality + search index
            let score = crate::quality::compute_feed_quality(conn, &p.feed.feed_guid)?;
            crate::quality::store_quality(conn, "feed", &p.feed.feed_guid, score)?;
            crate::search::populate_search_index(
                conn, "feed", &p.feed.feed_guid,
                "", &p.feed.title,
                p.feed.description.as_deref().unwrap_or(""),
                p.feed.raw_medium.as_deref().unwrap_or(""),
            )?;
        }
        event::EventPayload::TrackUpserted(p) => {
            // Ensure artist credit exists before upserting track
            upsert_artist_credit_if_absent(conn, &p.artist_credit)?;
            db::upsert_track(conn, &p.track)?;
            db::replace_payment_routes(conn, &p.track.track_guid, &p.routes)?;
            db::replace_value_time_splits(conn, &p.track.track_guid, &p.value_time_splits)?;
            // Recompute track quality + search index
            let score = crate::quality::compute_track_quality(conn, &p.track.track_guid)?;
            crate::quality::store_quality(conn, "track", &p.track.track_guid, score)?;
            crate::search::populate_search_index(
                conn, "track", &p.track.track_guid,
                "", &p.track.title,
                p.track.description.as_deref().unwrap_or(""),
                "",
            )?;
        }
        event::EventPayload::RoutesReplaced(p) => {
            db::replace_payment_routes(conn, &p.track_guid, &p.routes)?;
            // Recompute track quality (routes affect score)
            let score = crate::quality::compute_track_quality(conn, &p.track_guid)?;
            crate::quality::store_quality(conn, "track", &p.track_guid, score)?;
        }
        event::EventPayload::ArtistCreditCreated(p) => {
            upsert_artist_credit_if_absent(conn, &p.artist_credit)?;
        }
        event::EventPayload::FeedRoutesReplaced(p) => {
            db::replace_feed_payment_routes(conn, &p.feed_guid, &p.routes)?;
            // Recompute feed quality (routes affect score)
            let score = crate::quality::compute_feed_quality(conn, &p.feed_guid)?;
            crate::quality::store_quality(conn, "feed", &p.feed_guid, score)?;
        }
        event::EventPayload::FeedRetired(p) => {
            // Look up the feed to get search-index fields. If already gone, no-op.
            let feed_opt = db::get_feed_by_guid(conn, &p.feed_guid)?;
            if let Some(feed) = feed_opt {
                // Fetch all tracks to remove their search index entries.
                let tracks = db::get_tracks_for_feed(conn, &p.feed_guid)?;
                for track in &tracks {
                    let _ = crate::search::delete_from_search_index(
                        conn,
                        "track",
                        &track.track_guid,
                        "",
                        &track.title,
                        track.description.as_deref().unwrap_or(""),
                        "",
                    ); // best-effort: index entry may not exist
                }
                // Remove the feed's search index entry.
                let _ = crate::search::delete_from_search_index(
                    conn,
                    "feed",
                    &feed.feed_guid,
                    "",
                    &feed.title,
                    feed.description.as_deref().unwrap_or(""),
                    feed.raw_medium.as_deref().unwrap_or(""),
                );
                // Cascade-delete the feed and all child rows (using inner
                // _sql variant that works on &Connection within our tx).
                db::delete_feed_sql(conn, &p.feed_guid)?;
            }
        }
        event::EventPayload::TrackRemoved(p) => {
            // Look up the track to get search-index fields. If already gone, no-op.
            let track_opt = db::get_track_by_guid(conn, &p.track_guid)?;
            if let Some(track) = track_opt {
                // Remove the track's search index entry.
                let _ = crate::search::delete_from_search_index(
                    conn,
                    "track",
                    &track.track_guid,
                    "",
                    &track.title,
                    track.description.as_deref().unwrap_or(""),
                    "",
                ); // best-effort
                // Cascade-delete the track and its child rows (using inner
                // _sql variant that works on &Connection within our tx).
                db::delete_track_sql(conn, &p.track_guid)?;
            }
        }
        event::EventPayload::ArtistMerged(p) => {
            // Use inner _sql variant that works on &Connection within our tx.
            db::merge_artists_sql(
                conn,
                &p.source_artist_id,
                &p.target_artist_id,
            )?;
        }
    }

    Ok(ApplyOutcome::Applied(seq))
}

// ── apply_single_event ───────────────────────────────────────────────────────

/// Applies a single **pre-verified** event to the local DB idempotently.
///
/// All mutations go through `INSERT OR IGNORE` / upsert variants so that
/// re-delivering the same event is safe. The caller must have already verified
/// the event signature before calling this function.
///
/// This is the public wrapper for single-event application (e.g., from the push
/// handler). It acquires the writer lock, opens a transaction, delegates to the
/// inner function, updates the sync cursor, and commits.
///
/// # Errors
///
/// Returns `DbError` if any database operation fails.
// Issue-17 apply atomic transaction — 2026-03-13
// Issue-CURSOR-IDENTITY — 2026-03-14
// Issue-WAL-POOL — 2026-03-14: accepts DbPool, uses writer for mutations
// Issue-BATCH-APPLY — 2026-03-16
#[expect(clippy::significant_drop_tightening, reason = "conn is used across the entire event-application scope")]
pub fn apply_single_event(
    db: &db_pool::DbPool,
    ev: &event::Event,
) -> Result<ApplyOutcome, db::DbError> {
    // Timestamp ordering compliant — 2026-03-12
    let now = db::unix_now();

    // Issue-WAL-POOL — 2026-03-14: use writer for event application
    let mut conn = db.writer().lock().map_err(|_poison| db::DbError::Poisoned)?;

    // Wrap the ENTIRE body in a single transaction so all writes (entity
    // upsert + quality computation + search index + event record) are
    // atomic. On error the transaction is rolled back automatically.
    // Issue-CHECKED-TX — 2026-03-16: conn is freshly acquired from writer lock, no nesting.
    let tx = conn.transaction()?;

    let outcome = apply_single_event_inner(&tx, ev)?;

    if let ApplyOutcome::Applied(_) = &outcome {
        // Update the sync cursor only when the event was actually applied.
        // Issue-CURSOR-IDENTITY — 2026-03-14
        db::upsert_node_sync_state(&tx, SYNC_CURSOR_KEY, ev.seq, now)?;
    }

    tx.commit()?;
    Ok(outcome)
}

/// Helper: insert an artist credit and its names if they don't already exist.
// Issue-ARTIST-IDENTITY — 2026-03-14
fn upsert_artist_credit_if_absent(
    conn: &rusqlite::Connection,
    credit: &crate::model::ArtistCredit,
) -> Result<(), db::DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO artist_credit (id, display_name, feed_guid, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![credit.id, credit.display_name, credit.feed_guid, credit.created_at],
    )?;
    for acn in &credit.names {
        conn.execute(
            "INSERT OR IGNORE INTO artist_credit_name \
             (artist_credit_id, artist_id, position, name, join_phrase) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![acn.artist_credit_id, acn.artist_id, acn.position, acn.name, acn.join_phrase],
        )?;
    }
    Ok(())
}

// ── Batched outcome (internal) ──────────────────────────────────────────────

/// Per-event result tracked during batched apply so SSE can be published after
/// the transaction commits.
// Issue-BATCH-APPLY — 2026-03-16
struct BatchedApplyResult {
    /// The original event (owned), needed for SSE publishing.
    event: event::Event,
    /// The locally-assigned seq from `insert_event_idempotent`.
    local_seq: i64,
}

// ── apply_events ─────────────────────────────────────────────────────────────

/// Verify and apply a batch of events to the local DB.
///
/// All verified events are applied inside a single `spawn_blocking` call with
/// one wrapping transaction. The sync cursor is updated once at the end with
/// the maximum seq from the batch. SSE events are published after the
/// transaction commits, not during.
///
/// When `sse_registry` is `Some`, each successfully applied event is published
/// to the SSE broadcast channels for the relevant artist(s). This enables
/// community-node SSE clients to receive live events.
// Issue-SSE-PUBLISH — 2026-03-14
// Issue-CURSOR-IDENTITY — 2026-03-14
// Issue-WAL-POOL — 2026-03-14: accepts DbPool instead of db::Db
// Issue-BATCH-APPLY — 2026-03-16
pub async fn apply_events(
    db:           db_pool::DbPool,
    events:       Vec<event::Event>,
    sse_registry: Option<&Arc<api::SseRegistry>>,
) -> ApplySummary {
    let mut summary = ApplySummary { applied: 0, duplicate: 0, rejected: 0, max_seq: 0 };

    // Phase 1: verify signatures on the async side (CPU-bound but no DB I/O).
    // Partition into verified events and rejected events.
    // Issue-BATCH-APPLY — 2026-03-16
    let mut verified: Vec<event::Event> = Vec::with_capacity(events.len());
    for ev in events {
        if let Err(e) = signing::verify_event_signature(&ev) {
            tracing::warn!(
                event_id = %ev.event_id, seq = ev.seq, error = %e,
                "apply: invalid signature, skipping"
            );
            summary.rejected += 1;
        } else {
            verified.push(ev);
        }
    }

    if verified.is_empty() {
        return summary;
    }

    // Save count before moving `verified` into the blocking closure.
    // Issue-BATCH-APPLY — 2026-03-16
    let verified_count = verified.len();

    // Phase 2: apply all verified events in a single spawn_blocking call with
    // one wrapping transaction. This eliminates per-event scheduler overhead
    // and per-event transaction commit latency.
    // Issue-BATCH-APPLY — 2026-03-16
    let db2 = db.clone();
    let batch_result = tokio::task::spawn_blocking(move || -> Result<(ApplySummary, Vec<BatchedApplyResult>), db::DbError> {
        let now = db::unix_now();
        let mut conn = db2.writer().lock().map_err(|_poison| db::DbError::Poisoned)?;
        // Issue-CHECKED-TX — 2026-03-16: conn is freshly acquired from writer lock, no nesting.
        let tx = conn.transaction()?;

        let mut applied: Vec<BatchedApplyResult> = Vec::new();
        let mut batch_summary = ApplySummary { applied: 0, duplicate: 0, rejected: 0, max_seq: 0 };
        let mut max_primary_seq: i64 = 0;

        for ev in &verified {
            match apply_single_event_inner(&tx, ev) {
                Ok(ApplyOutcome::Applied(local_seq)) => {
                    batch_summary.applied += 1;
                    if local_seq > batch_summary.max_seq {
                        batch_summary.max_seq = local_seq;
                    }
                    if ev.seq > batch_summary.max_seq {
                        batch_summary.max_seq = ev.seq;
                    }
                    if ev.seq > max_primary_seq {
                        max_primary_seq = ev.seq;
                    }
                    applied.push(BatchedApplyResult {
                        event: ev.clone(),
                        local_seq,
                    });
                }
                Ok(ApplyOutcome::Duplicate) => {
                    batch_summary.duplicate += 1;
                    // Track max primary seq even for duplicates so the cursor
                    // advances past already-seen events.
                    if ev.seq > max_primary_seq {
                        max_primary_seq = ev.seq;
                    }
                }
                Err(db_err) => {
                    tracing::error!(
                        event_id = %ev.event_id, error = %db_err,
                        "apply: DB error in batch"
                    );
                    batch_summary.rejected += 1;
                }
            }
        }

        // Update the sync cursor once for the entire batch, using the max
        // primary-side seq seen (including duplicates).
        // Issue-BATCH-APPLY — 2026-03-16
        if max_primary_seq > 0 {
            db::upsert_node_sync_state(&tx, SYNC_CURSOR_KEY, max_primary_seq, now)?;
        }

        tx.commit()?;
        Ok((batch_summary, applied))
    })
    .await;

    // Phase 3: merge batch results and publish SSE events after commit.
    // Issue-BATCH-APPLY — 2026-03-16
    match batch_result {
        Err(panic_err) => {
            tracing::error!(error = %panic_err, "apply: batch task panicked");
            // All verified events are considered rejected on panic.
            summary.rejected += verified_count;
        }
        Ok(Err(db_err)) => {
            tracing::error!(error = %db_err, "apply: batch transaction failed");
            // All verified events are considered rejected on DB error.
            summary.rejected += verified_count;
        }
        Ok(Ok((batch_summary, applied_events))) => {
            summary.applied += batch_summary.applied;
            summary.duplicate += batch_summary.duplicate;
            summary.rejected += batch_summary.rejected;
            if batch_summary.max_seq > summary.max_seq {
                summary.max_seq = batch_summary.max_seq;
            }

            // Publish SSE events AFTER the transaction has committed.
            // Issue-BATCH-APPLY — 2026-03-16
            if let Some(registry) = sse_registry {
                for result in &applied_events {
                    // Use the locally assigned seq for the SSE frame, not
                    // the primary's seq, since SSE replay is local-DB-based.
                    let mut local_ev = result.event.clone();
                    local_ev.seq = result.local_seq;
                    api::publish_events_to_sse(registry, &[local_ev]);
                }
            }
        }
    }

    summary
}

// Issue-CURSOR-IDENTITY — 2026-03-14
#[cfg(test)]
#[expect(clippy::significant_drop_tightening, reason = "MutexGuard<Connection> must be held for the full scope in test assertions")]
mod tests {
    use super::*;

    use crate::event::{ArtistUpsertedPayload, EventPayload, EventType};
    use crate::model::Artist;
    use crate::signing::NodeSigner;

    /// Build a temporary file-based DB pool for tests.
    // Issue-WAL-POOL — 2026-03-14
    fn test_db() -> (db_pool::DbPool, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let db_path = dir.path().join("test.db");
        let pool = db_pool::DbPool::open(&db_path).expect("failed to open test db pool");
        (pool, dir) // dir must be kept alive for the DB file to persist
    }

    /// Build a properly signed `ArtistUpserted` event.
    fn make_signed_event(signer: &NodeSigner, event_id: &str, seq: i64) -> event::Event {
        let artist = Artist {
            artist_id:  "artist-1".into(),
            name:       "Test Artist".into(),
            name_lower: "test artist".into(),
            sort_name:  None,
            type_id:    None,
            area:       None,
            img_url:    None,
            url:        None,
            begin_year: None,
            end_year:   None,
            created_at: 1_000_000,
            updated_at: 1_000_000,
        };

        let inner = ArtistUpsertedPayload { artist };
        let payload_json = serde_json::to_string(&inner).unwrap();

        let (signed_by, signature) = signer.sign_event(
            event_id,
            &EventType::ArtistUpserted,
            &payload_json,
            "artist-1",
            1_000_000,
            seq,
        );

        event::Event {
            event_id:     event_id.into(),
            event_type:   EventType::ArtistUpserted,
            payload:      EventPayload::ArtistUpserted(inner),
            payload_json,
            subject_guid: "artist-1".into(),
            signed_by,
            signature,
            seq,
            created_at:   1_000_000,
            warnings:     vec![],
        }
    }

    /// After push-path applies events (via `apply_events`), the poll-loop
    /// must see the advanced cursor by reading with `SYNC_CURSOR_KEY`.
    /// Before the fix, push wrote under the primary pubkey and poll read
    /// under the community pubkey — a different key — so the cursor was
    /// invisible to poll.
    #[tokio::test]
    async fn push_cursor_visible_to_poll_reader() {
        let (pool, _dir) = test_db();
        let signer = NodeSigner::load_or_create("/tmp/cursor-identity-test-1.key").unwrap();

        let ev = make_signed_event(&signer, "evt-push-1", 42);
        let summary = apply_events(pool.clone(), vec![ev], None).await;
        assert_eq!(summary.applied, 1, "event must be applied");

        // Simulate what the poll loop does: read cursor via SYNC_CURSOR_KEY.
        let conn = pool.reader().unwrap();
        let cursor = db::get_node_sync_cursor(&conn, SYNC_CURSOR_KEY).unwrap();
        assert_eq!(cursor, 42, "poll must see the push-advanced cursor");
    }

    /// Both push and poll advance the same cursor.  After poll applies an
    /// event at seq 10 and push applies an event at seq 20, reading the
    /// cursor must return 20 (the maximum).
    #[tokio::test]
    async fn push_and_poll_share_cursor() {
        let (pool, _dir) = test_db();
        let signer = NodeSigner::load_or_create("/tmp/cursor-identity-test-2.key").unwrap();

        // Simulate poll applying seq 10.
        let ev1 = make_signed_event(&signer, "evt-poll-1", 10);
        let s1 = apply_events(pool.clone(), vec![ev1], None).await;
        assert_eq!(s1.applied, 1);

        {
            let conn = pool.reader().unwrap();
            let cursor = db::get_node_sync_cursor(&conn, SYNC_CURSOR_KEY).unwrap();
            assert_eq!(cursor, 10);
        }

        // Simulate push applying seq 20.
        let ev2 = make_signed_event(&signer, "evt-push-2", 20);
        let s2 = apply_events(pool.clone(), vec![ev2], None).await;
        assert_eq!(s2.applied, 1);

        let conn = pool.reader().unwrap();
        let cursor = db::get_node_sync_cursor(&conn, SYNC_CURSOR_KEY).unwrap();
        assert_eq!(cursor, 20, "cursor must be the max of both paths");
    }

    /// The cursor must not regress: applying a lower seq after a higher seq
    /// must keep the higher value (monotonic advancement).
    #[tokio::test]
    async fn cursor_is_monotonic() {
        let (pool, _dir) = test_db();
        let signer = NodeSigner::load_or_create("/tmp/cursor-identity-test-3.key").unwrap();

        let ev_high = make_signed_event(&signer, "evt-high", 100);
        apply_events(pool.clone(), vec![ev_high], None).await;

        let ev_low = make_signed_event(&signer, "evt-low", 50);
        apply_events(pool.clone(), vec![ev_low], None).await;

        let conn = pool.reader().unwrap();
        let cursor = db::get_node_sync_cursor(&conn, SYNC_CURSOR_KEY).unwrap();
        assert_eq!(cursor, 100, "cursor must not regress");
    }
}
