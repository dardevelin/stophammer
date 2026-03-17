// Issue-VERIFY-READER — 2026-03-16
//
// Tests verifying that the verifier chain operates correctly on read-only
// connections and that the reader/writer separation is enforced.

mod common;

use rusqlite::params;

use stophammer::ingest::{IngestFeedRequest, IngestFeedData, IngestTrackData, IngestPaymentRoute};
use stophammer::model::RouteType;
use stophammer::verify::{IngestContext, Verifier, VerifierChain, VerifyResult};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn sample_request() -> IngestFeedRequest {
    IngestFeedRequest {
        crawl_token:   String::new(),
        canonical_url: "https://example.com/feed.xml".into(),
        source_url:    "https://example.com/feed.xml".into(),
        http_status:   200,
        content_hash:  "hash-abc".into(),
        feed_data:     Some(IngestFeedData {
            feed_guid:          "917393e3-1b1e-5f2c-a927-9e29e2d26b32".into(),
            title:              "Test Feed".into(),
            description:        Some("A test".into()),
            author_name:        Some("Tester".into()),
            owner_name:         None,
            image_url:          None,
            language:           Some("en".into()),
            explicit:           false,
            itunes_type:        None,
            raw_medium:         Some("music".into()),
            pub_date:           None,
            feed_payment_routes: vec![IngestPaymentRoute {
                recipient_name: Some("host".into()),
                route_type:     RouteType::Keysend,
                address:        "03e7156ae33b0a208d0744199163177e909e80176e55d97a2f221ede0f934dd9ad".into(),
                custom_key:     None,
                custom_value:   None,
                split:          100,
                fee:            false,
            }],
            tracks: vec![IngestTrackData {
                track_guid:     "t-guid-1".into(),
                title:          "Track One".into(),
                author_name:    None,
                pub_date:       Some(1_700_000_000),
                duration_secs:  Some(180),
                enclosure_url:  Some("https://example.com/t1.mp3".into()),
                enclosure_type: Some("audio/mpeg".into()),
                enclosure_bytes: Some(5_000_000),
                track_number:   Some(1),
                season:         None,
                explicit:       false,
                description:    None,
                payment_routes: vec![],
                value_time_splits: vec![],
            }],
        }),
    }
}

// ── Test verifier implementations ────────────────────────────────────────────

/// A verifier that reads from the DB (read-only operation).
struct ReadingVerifier;

impl Verifier for ReadingVerifier {
    fn name(&self) -> &'static str { "reading_verifier" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        // Perform a harmless read query against the connection.
        let count: i64 = ctx.db
            .query_row("SELECT count(*) FROM feeds", [], |row| row.get(0))
            .unwrap_or(-1);
        if count >= 0 {
            VerifyResult::Pass
        } else {
            VerifyResult::Fail("unexpected query failure".into())
        }
    }
}

/// A verifier that attempts a write — should fail on a read-only connection.
struct WritingVerifier;

impl Verifier for WritingVerifier {
    fn name(&self) -> &'static str { "writing_verifier" }

    fn verify(&self, ctx: &IngestContext) -> VerifyResult {
        let result = ctx.db.execute(
            "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
             VALUES ('bad', 'bad', 0)",
            [],
        );
        match result {
            Ok(_)  => VerifyResult::Pass,
            Err(e) => VerifyResult::Fail(format!("write blocked: {e}")),
        }
    }
}

/// A verifier that always fails, used to confirm the writer lock is not taken.
struct AlwaysFailVerifier;

impl Verifier for AlwaysFailVerifier {
    fn name(&self) -> &'static str { "always_fail" }

    fn verify(&self, _ctx: &IngestContext) -> VerifyResult {
        VerifyResult::Fail("intentional rejection".into())
    }
}

// ── 1. Chain runs successfully on a reader connection ────────────────────────

#[test]
fn verifier_chain_runs_on_reader_connection() {
    // Issue-VERIFY-READER — 2026-03-16
    let (pool, _dir) = common::test_db_pool();

    let reader = pool.reader().expect("should get reader");
    let req = sample_request();
    let ctx = IngestContext {
        request:  &req,
        db:       &*reader,
        existing: None,
    };

    let chain = VerifierChain::new(vec![Box::new(ReadingVerifier)]);
    let result = chain.run(&ctx);
    assert!(result.is_ok(), "chain should pass on reader: {result:?}");
}

// ── 2. Writer attempt fails on reader (query_only enforced) ──────────────────

#[test]
fn verifier_write_attempt_blocked_on_reader() {
    // Issue-VERIFY-READER — 2026-03-16
    let (pool, _dir) = common::test_db_pool();

    let reader = pool.reader().expect("should get reader");
    let req = sample_request();
    let ctx = IngestContext {
        request:  &req,
        db:       &*reader,
        existing: None,
    };

    let chain = VerifierChain::new(vec![Box::new(WritingVerifier)]);
    let result = chain.run(&ctx);
    assert!(result.is_err(), "chain should fail when verifier attempts a write");
    let err = result.unwrap_err();
    assert!(
        err.0.contains("writing_verifier"),
        "error should identify the offending verifier: {err}"
    );
}

// ── 3. Verification failure skips writer acquisition ─────────────────────────

#[test]
fn verification_failure_does_not_acquire_writer() {
    // Issue-VERIFY-READER — 2026-03-16
    //
    // Strategy: hold the writer lock on the current thread, then run the
    // verifier chain with a failing verifier on the reader. If the chain
    // tried to acquire the writer it would deadlock. Since verification
    // only uses the reader, it completes and returns the failure immediately.
    let (pool, _dir) = common::test_db_pool();

    // Hold the writer lock for the duration of this test.
    let writer_guard = pool.writer().lock().expect("should lock writer");

    let reader = pool.reader().expect("should get reader");
    let req = sample_request();
    let ctx = IngestContext {
        request:  &req,
        db:       &*reader,
        existing: None,
    };

    let chain = VerifierChain::new(vec![Box::new(AlwaysFailVerifier)]);
    let result = chain.run(&ctx);

    assert!(result.is_err(), "chain should return failure");
    assert_eq!(result.unwrap_err().0, "[always_fail] intentional rejection");

    // Writer was held the entire time — verification never touched it.
    drop(writer_guard);
}

// ── 4. Mixed chain: reader query + pass proceeds normally ────────────────────

#[test]
fn mixed_chain_reader_queries_succeed() {
    // Issue-VERIFY-READER — 2026-03-16
    let (pool, _dir) = common::test_db_pool();

    // Seed some data so the reading verifier has something to query.
    {
        let writer = pool.writer().lock().expect("lock writer");
        writer.execute(
            "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
             VALUES (?1, ?2, ?3)",
            params!["https://example.com/feed.xml", "old-hash", 1_700_000_000i64],
        ).expect("seed data");
    }

    let reader = pool.reader().expect("should get reader");
    let req = sample_request();
    let ctx = IngestContext {
        request:  &req,
        db:       &*reader,
        existing: None,
    };

    // Chain with two read-based verifiers.
    let chain = VerifierChain::new(vec![
        Box::new(ReadingVerifier),
        Box::new(ReadingVerifier),
    ]);
    let result = chain.run(&ctx);
    assert!(result.is_ok(), "all-read chain should pass: {result:?}");
}

// ── 5. Content-hash verifier works on reader ─────────────────────────────────

#[test]
fn content_hash_verifier_works_on_reader() {
    // Issue-VERIFY-READER — 2026-03-16
    // The content_hash verifier is the one built-in that actually queries
    // ctx.db — confirm it works against a real reader pool connection.
    use stophammer::verifiers::content_hash::{ContentHashVerifier, NO_CHANGE_SENTINEL};

    let (pool, _dir) = common::test_db_pool();

    // Seed the crawl cache with a matching hash.
    {
        let writer = pool.writer().lock().expect("lock writer");
        writer.execute(
            "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
             VALUES (?1, ?2, ?3)",
            params!["https://example.com/feed.xml", "hash-abc", 1_700_000_000i64],
        ).expect("seed crawl cache");
    }

    let reader = pool.reader().expect("should get reader");
    let req = sample_request(); // content_hash = "hash-abc" matching the seeded row
    let ctx = IngestContext {
        request:  &req,
        db:       &*reader,
        existing: None,
    };

    let chain = VerifierChain::new(vec![Box::new(ContentHashVerifier)]);
    let result = chain.run(&ctx);
    assert!(result.is_err(), "matching hash should short-circuit as NO_CHANGE");
    assert_eq!(result.unwrap_err().0, format!("[content_hash] {NO_CHANGE_SENTINEL}"));
}
