//! Shared event-application logic used by both the poll-loop fallback and the
//! push-receiver handler in community mode.
//!
//! `apply_single_event` writes one verified event to the local DB idempotently.
//! `apply_events` verifies signatures and drives the per-event apply loop,
//! returning counts of applied/duplicate/rejected events.

use std::sync::Arc;

use crate::{db, event, signing};

// ── ApplyOutcome ─────────────────────────────────────────────────────────────

/// Per-event outcome returned by [`apply_single_event`].
pub(crate) enum ApplyOutcome {
    /// Event was new and written to the DB; contains the assigned `seq`.
    Applied(i64),
    /// Event already existed in the DB (`INSERT OR IGNORE` was a no-op).
    Duplicate,
}

// ── ApplySummary ─────────────────────────────────────────────────────────────

/// Aggregate counts returned by [`apply_events`].
pub(crate) struct ApplySummary {
    pub applied:   usize,
    pub duplicate: usize,
    pub rejected:  usize,
    /// Highest seq applied during this batch (0 if nothing was applied).
    pub max_seq:   i64,
}

// ── apply_single_event ───────────────────────────────────────────────────────

/// Applies a single **pre-verified** event to the local DB idempotently.
///
/// All mutations go through `INSERT OR IGNORE` / upsert variants so that
/// re-delivering the same event is safe. The caller must have already verified
/// the event signature before calling this function.
pub(crate) fn apply_single_event(
    db:          &db::Db,
    node_pubkey: &str,
    ev:          &event::Event,
) -> Result<ApplyOutcome, db::DbError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .cast_signed();

    let mut conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);

    match &ev.payload {
        event::EventPayload::ArtistUpserted(p) => {
            db::upsert_artist_if_absent(&conn, &p.artist)?;
        }
        event::EventPayload::FeedUpserted(p) => {
            db::upsert_artist_if_absent(&conn, &p.artist)?;
            // Ensure artist credit exists before upserting feed
            upsert_artist_credit_if_absent(&conn, &p.artist_credit)?;
            db::upsert_feed(&conn, &p.feed)?;
        }
        event::EventPayload::TrackUpserted(p) => {
            // Ensure artist credit exists before upserting track
            upsert_artist_credit_if_absent(&conn, &p.artist_credit)?;
            db::upsert_track(&conn, &p.track)?;
            db::replace_payment_routes(&conn, &p.track.track_guid, &p.routes)?;
            db::replace_value_time_splits(&conn, &p.track.track_guid, &p.value_time_splits)?;
        }
        event::EventPayload::RoutesReplaced(p) => {
            db::replace_payment_routes(&conn, &p.track_guid, &p.routes)?;
        }
        event::EventPayload::ArtistCreditCreated(p) => {
            upsert_artist_credit_if_absent(&conn, &p.artist_credit)?;
        }
        event::EventPayload::FeedRoutesReplaced(p) => {
            db::replace_feed_payment_routes(&conn, &p.feed_guid, &p.routes)?;
        }
        event::EventPayload::FeedRetired(p) => {
            eprintln!(
                "[apply] FeedRetired for {} not yet implemented; skipping",
                p.feed_guid
            );
        }
        event::EventPayload::TrackRemoved(p) => {
            eprintln!(
                "[apply] TrackRemoved for {} not yet implemented; skipping",
                p.track_guid
            );
        }
        event::EventPayload::ArtistMerged(p) => {
            db::merge_artists(
                &mut conn,
                &p.source_artist_id,
                &p.target_artist_id,
            )?;
        }
    }

    // Insert the event row idempotently.
    let seq_opt = db::insert_event_idempotent(
        &conn,
        &ev.event_id,
        &ev.event_type,
        &ev.payload_json,
        &ev.subject_guid,
        &ev.signed_by,
        &ev.signature,
        ev.created_at,
        &ev.warnings,
    )?;

    if let Some(seq) = seq_opt {
        db::upsert_node_sync_state(&conn, node_pubkey, ev.seq, now)?;
        return Ok(ApplyOutcome::Applied(seq));
    }

    Ok(ApplyOutcome::Duplicate)
}

/// Helper: insert an artist credit and its names if they don't already exist.
fn upsert_artist_credit_if_absent(
    conn: &rusqlite::Connection,
    credit: &crate::model::ArtistCredit,
) -> Result<(), db::DbError> {
    conn.execute(
        "INSERT OR IGNORE INTO artist_credit (id, display_name, created_at) \
         VALUES (?1, ?2, ?3)",
        rusqlite::params![credit.id, credit.display_name, credit.created_at],
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

// ── apply_events ─────────────────────────────────────────────────────────────

/// Verify and apply a batch of events to the local DB.
pub(crate) async fn apply_events(
    db:          db::Db,
    node_pubkey: &str,
    events:      Vec<event::Event>,
) -> ApplySummary {
    let mut summary = ApplySummary { applied: 0, duplicate: 0, rejected: 0, max_seq: 0 };
    let node_pubkey = node_pubkey.to_string();

    for ev in events {
        let seq      = ev.seq;
        let event_id = ev.event_id.clone();

        if let Err(e) = signing::verify_event_signature(&ev) {
            eprintln!("[apply] invalid signature on event {event_id} seq={seq}: {e}; skipping");
            summary.rejected += 1;
            continue;
        }

        let db2    = Arc::clone(&db);
        let pk     = node_pubkey.clone();
        let result = tokio::task::spawn_blocking(move || apply_single_event(&db2, &pk, &ev))
            .await;

        match result {
            Err(panic_err) => {
                eprintln!("[apply] apply task panicked for event {event_id}: {panic_err}");
                summary.rejected += 1;
            }
            Ok(Err(db_err)) => {
                eprintln!("[apply] DB error applying event {event_id}: {db_err}");
                summary.rejected += 1;
            }
            Ok(Ok(ApplyOutcome::Applied(s))) => {
                summary.applied += 1;
                if s > summary.max_seq {
                    summary.max_seq = s;
                }
                if seq > summary.max_seq {
                    summary.max_seq = seq;
                }
            }
            Ok(Ok(ApplyOutcome::Duplicate)) => {
                summary.duplicate += 1;
            }
        }
    }

    summary
}
