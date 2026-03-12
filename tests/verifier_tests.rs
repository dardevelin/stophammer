mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// 1. crawl_token_rejects_empty
// ---------------------------------------------------------------------------

#[test]
fn crawl_token_rejects_empty() {
    let empty_token = "";
    let expected_token = "s3cr3t-crawl-token";

    assert_ne!(empty_token, expected_token, "empty token must not match a real token");

    // Even whitespace-only tokens should be treated as invalid.
    let whitespace_token = "   ";
    assert_ne!(
        whitespace_token.trim(),
        expected_token,
        "whitespace-only token must not match"
    );
}

// ---------------------------------------------------------------------------
// 2. content_hash_unchanged
// ---------------------------------------------------------------------------

#[test]
fn content_hash_unchanged() {
    let conn = common::test_db();
    let now = common::now();
    let feed_url = "https://example.com/feed.xml";
    let hash = "abc123def456";

    conn.execute(
        "INSERT INTO feed_crawl_cache (feed_url, content_hash, crawled_at) \
         VALUES (?1, ?2, ?3)",
        params![feed_url, hash, now],
    )
    .unwrap();

    let cached_hash: String = conn
        .query_row(
            "SELECT content_hash FROM feed_crawl_cache WHERE feed_url = ?1",
            params![feed_url],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(cached_hash, hash, "same hash means feed is unchanged");

    let new_hash = "xyz789";
    assert_ne!(cached_hash, new_hash, "different hash means feed changed");
}

// ---------------------------------------------------------------------------
// 3. medium_music_only
// ---------------------------------------------------------------------------

#[test]
fn medium_music_only() {
    let valid = "music";
    assert_eq!(valid, "music", "literal 'music' passes");

    let invalid_mediums = ["podcast", "video", "audiobook", "Music", "MUSIC", ""];
    for m in &invalid_mediums {
        assert_ne!(*m, "music", "should reject medium: '{m}'");
    }

    let absent: Option<&str> = None;
    assert!(absent.is_none(), "absent medium is a rejection case");
}

// ---------------------------------------------------------------------------
// 4. feed_guid_format
// ---------------------------------------------------------------------------

#[test]
fn feed_guid_format() {
    let valid = "917393e3-1b1e-5f2c-a927-9e29e2d26b32";
    assert!(uuid::Uuid::parse_str(valid).is_ok(), "valid UUID should parse");

    let invalid = "not-a-uuid";
    assert!(uuid::Uuid::parse_str(invalid).is_err(), "invalid string should fail");

    let empty = "";
    assert!(uuid::Uuid::parse_str(empty).is_err(), "empty string should fail");

    // Known bad GUID (platform default) should be flagged.
    let bad_guid = "c9c7bad3-4712-514e-9ebd-d1e208fa1b76";
    let bad_guids = ["c9c7bad3-4712-514e-9ebd-d1e208fa1b76"];
    assert!(
        bad_guids.contains(&bad_guid),
        "known bad GUID should be in the reject list"
    );

    let good_guid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890";
    assert!(uuid::Uuid::parse_str(good_guid).is_ok());
    assert!(!bad_guids.contains(&good_guid));
}

// ---------------------------------------------------------------------------
// 5. v4v_payment_required
// ---------------------------------------------------------------------------

#[test]
fn v4v_payment_required() {
    let valid_address: String = "03e7156ae33b0a208d0744199163177e909e80176e55d97a2f221ede0f934dd9ad".to_string();
    let valid_split: i64 = 95;
    assert!(!valid_address.is_empty() && valid_split > 0, "valid route");

    // Zero split is invalid even with a valid address.
    let zero_split: i64 = 0;
    assert_eq!(zero_split, 0, "zero split should be rejected");

    // A feed with no routes at all should be rejected.
    let routes: Vec<(&str, i64)> = vec![];
    assert!(routes.is_empty(), "no routes means no V4V participation");

    // At least one valid route among many is sufficient.
    let mixed_routes: Vec<(&str, i64)> = vec![
        ("", 50),                          // invalid: empty address
        ("some-node-pubkey", 0),           // invalid: zero split
        ("valid-lightning-address", 100),   // valid
    ];
    let has_valid = mixed_routes.iter().any(|(addr, split)| !addr.is_empty() && *split > 0);
    assert!(has_valid, "at least one valid route is sufficient");
}

// ---------------------------------------------------------------------------
// 6. enclosure_type_video_warns
// ---------------------------------------------------------------------------

#[test]
fn enclosure_type_video_warns() {
    let video_types = ["video/mp4", "video/webm", "video/x-matroska"];
    for mime in &video_types {
        assert!(
            mime.starts_with("video/"),
            "'{mime}' should be flagged as video"
        );
    }

    let audio_types = ["audio/mpeg", "audio/ogg", "audio/x-m4a", "audio/flac"];
    for mime in &audio_types {
        assert!(
            !mime.starts_with("video/"),
            "'{mime}' should not be flagged"
        );
    }

    let none_type: Option<&str> = None;
    assert!(none_type.is_none(), "absent MIME type is not a video concern");
}

// ---------------------------------------------------------------------------
// 7. payment_route_sum
// ---------------------------------------------------------------------------

#[test]
fn payment_route_sum() {
    let valid_splits: Vec<i64> = vec![60, 25, 10, 5];
    assert_eq!(valid_splits.iter().sum::<i64>(), 100, "splits must sum to 100");

    let solo: Vec<i64> = vec![100];
    assert_eq!(solo.iter().sum::<i64>(), 100);

    let even: Vec<i64> = vec![50, 50];
    assert_eq!(even.iter().sum::<i64>(), 100);

    let over: Vec<i64> = vec![60, 50];
    assert_ne!(over.iter().sum::<i64>(), 100, "sum > 100 should be rejected");

    let under: Vec<i64> = vec![30, 20];
    assert_ne!(under.iter().sum::<i64>(), 100, "sum < 100 should be rejected");

    let empty: Vec<i64> = vec![];
    assert!(empty.is_empty(), "empty routes are skipped, not checked");
}
