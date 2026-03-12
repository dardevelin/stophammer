mod common;

use rusqlite::params;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert a test artist and return its artist_id.
fn insert_artist(conn: &rusqlite::Connection, name: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let now = common::now();
    conn.execute(
        "INSERT INTO artists (artist_id, name, name_lower, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, name, name.to_lowercase(), now, now],
    )
    .unwrap();
    id
}

/// Insert an artist credit for a single artist and return the credit_id.
fn insert_credit(conn: &rusqlite::Connection, artist_id: &str, name: &str) -> i64 {
    let now = common::now();
    conn.execute(
        "INSERT INTO artist_credit (display_name, created_at) VALUES (?1, ?2)",
        params![name, now],
    )
    .unwrap();
    let credit_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO artist_credit_name (artist_credit_id, artist_id, position, name, join_phrase) \
         VALUES (?1, ?2, 0, ?3, '')",
        params![credit_id, artist_id, name],
    )
    .unwrap();
    credit_id
}

/// Insert a test feed.
fn insert_feed(
    conn: &rusqlite::Connection,
    guid: &str,
    url: &str,
    title: &str,
    credit_id: i64,
) {
    let now = common::now();
    conn.execute(
        "INSERT INTO feeds (feed_guid, feed_url, title, title_lower, artist_credit_id, \
         explicit, episode_count, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 0, 0, ?6, ?7)",
        params![guid, url, title, title.to_lowercase(), credit_id, now, now],
    )
    .unwrap();
}

/// Insert a test track.
fn insert_track(
    conn: &rusqlite::Connection,
    guid: &str,
    feed_guid: &str,
    credit_id: i64,
    title: &str,
    pub_date: Option<i64>,
) {
    let now = common::now();
    conn.execute(
        "INSERT INTO tracks (track_guid, feed_guid, artist_credit_id, title, title_lower, \
         pub_date, explicit, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8)",
        params![guid, feed_guid, credit_id, title, title.to_lowercase(), pub_date, now, now],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// 1. track_by_guid — insert a track, query by guid, verify all fields
// ---------------------------------------------------------------------------

#[test]
fn track_by_guid() {
    let conn = common::test_db();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_artist(&conn, "Thom Yorke");
    let credit_id = insert_credit(&conn, &artist_id, "Thom Yorke");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/anima.xml",
        "Anima",
        credit_id,
    );

    let track_guid = uuid::Uuid::new_v4().to_string();
    insert_track(
        &conn,
        &track_guid,
        &feed_guid,
        credit_id,
        "Traffic",
        Some(base_date),
    );

    let (returned_guid, returned_feed, returned_credit, returned_title, returned_pub_date): (
        String,
        String,
        i64,
        String,
        Option<i64>,
    ) = conn
        .query_row(
            "SELECT track_guid, feed_guid, artist_credit_id, title, pub_date \
             FROM tracks WHERE track_guid = ?1",
            params![track_guid],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();

    assert_eq!(returned_guid, track_guid);
    assert_eq!(returned_feed, feed_guid);
    assert_eq!(returned_credit, credit_id);
    assert_eq!(returned_title, "Traffic");
    assert_eq!(returned_pub_date, Some(base_date));
}

// ---------------------------------------------------------------------------
// 2. track_payment_routes — insert a track + 2 payment routes, verify both
// ---------------------------------------------------------------------------

#[test]
fn track_payment_routes() {
    let conn = common::test_db();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_artist(&conn, "Noname");
    let credit_id = insert_credit(&conn, &artist_id, "Noname");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/room25.xml",
        "Room 25",
        credit_id,
    );

    let track_guid = uuid::Uuid::new_v4().to_string();
    insert_track(
        &conn,
        &track_guid,
        &feed_guid,
        credit_id,
        "Blaxploitation",
        Some(base_date),
    );

    // Insert 2 payment routes
    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![track_guid, feed_guid, "Artist Wallet", "keysend", "abc123def456", 90, 0],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO payment_routes (track_guid, feed_guid, recipient_name, route_type, address, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![track_guid, feed_guid, "App Wallet", "keysend", "xyz789ghi012", 10, 0],
    )
    .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT recipient_name, route_type, address, split, fee \
             FROM payment_routes WHERE track_guid = ?1 ORDER BY split DESC",
        )
        .unwrap();

    let rows: Vec<(Option<String>, String, String, i64, i64)> = stmt
        .query_map(params![track_guid], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2, "should return both payment routes");
    assert_eq!(rows[0].0.as_deref(), Some("Artist Wallet"));
    assert_eq!(rows[0].1, "keysend");
    assert_eq!(rows[0].2, "abc123def456");
    assert_eq!(rows[0].3, 90);
    assert_eq!(rows[0].4, 0);
    assert_eq!(rows[1].0.as_deref(), Some("App Wallet"));
    assert_eq!(rows[1].3, 10);
}

// ---------------------------------------------------------------------------
// 3. track_value_time_splits — insert a track + 2 value_time_splits, verify
// ---------------------------------------------------------------------------

#[test]
fn track_value_time_splits() {
    let conn = common::test_db();
    let now = common::now();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_artist(&conn, "Snarky Puppy");
    let credit_id = insert_credit(&conn, &artist_id, "Snarky Puppy");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/ground-up.xml",
        "Ground Up",
        credit_id,
    );

    let track_guid = uuid::Uuid::new_v4().to_string();
    insert_track(
        &conn,
        &track_guid,
        &feed_guid,
        credit_id,
        "Lingus",
        Some(base_date),
    );

    // Insert 2 value time splits
    conn.execute(
        "INSERT INTO value_time_splits \
         (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![track_guid, 0, 120, "remote-feed-1", "remote-item-1", 50, now],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO value_time_splits \
         (source_track_guid, start_time_secs, duration_secs, remote_feed_guid, remote_item_guid, split, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![track_guid, 120, 180, "remote-feed-2", "remote-item-2", 50, now],
    )
    .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT source_track_guid, start_time_secs, duration_secs, \
             remote_feed_guid, remote_item_guid, split \
             FROM value_time_splits WHERE source_track_guid = ?1 \
             ORDER BY start_time_secs ASC",
        )
        .unwrap();

    let rows: Vec<(String, i64, Option<i64>, String, String, i64)> = stmt
        .query_map(params![track_guid], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2, "should return both value time splits");

    assert_eq!(rows[0].0, track_guid);
    assert_eq!(rows[0].1, 0);
    assert_eq!(rows[0].2, Some(120));
    assert_eq!(rows[0].3, "remote-feed-1");
    assert_eq!(rows[0].4, "remote-item-1");
    assert_eq!(rows[0].5, 50);

    assert_eq!(rows[1].1, 120);
    assert_eq!(rows[1].2, Some(180));
    assert_eq!(rows[1].3, "remote-feed-2");
    assert_eq!(rows[1].4, "remote-item-2");
}

// ---------------------------------------------------------------------------
// 4. tracks_ordered_by_pub_date — insert 3 tracks, verify DESC ordering
// ---------------------------------------------------------------------------

#[test]
fn tracks_ordered_by_pub_date() {
    let conn = common::test_db();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_artist(&conn, "Hiatus Kaiyote");
    let credit_id = insert_credit(&conn, &artist_id, "Hiatus Kaiyote");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/mood-valiant.xml",
        "Mood Valiant",
        credit_id,
    );

    let track1 = uuid::Uuid::new_v4().to_string();
    let track2 = uuid::Uuid::new_v4().to_string();
    let track3 = uuid::Uuid::new_v4().to_string();

    insert_track(&conn, &track1, &feed_guid, credit_id, "Chivalry Is Not Dead", Some(base_date + 100));
    insert_track(&conn, &track2, &feed_guid, credit_id, "Get Sun", Some(base_date + 300));
    insert_track(&conn, &track3, &feed_guid, credit_id, "Red Room", Some(base_date + 200));

    let mut stmt = conn
        .prepare(
            "SELECT track_guid, title, pub_date FROM tracks \
             WHERE feed_guid = ?1 ORDER BY pub_date DESC",
        )
        .unwrap();

    let rows: Vec<(String, String, Option<i64>)> = stmt
        .query_map(params![feed_guid], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1, "Get Sun");
    assert_eq!(rows[0].2, Some(base_date + 300));
    assert_eq!(rows[1].1, "Red Room");
    assert_eq!(rows[1].2, Some(base_date + 200));
    assert_eq!(rows[2].1, "Chivalry Is Not Dead");
    assert_eq!(rows[2].2, Some(base_date + 100));
}

// ---------------------------------------------------------------------------
// 5. recent_feeds_ordering — insert 3 feeds with different newest_item_at,
//    verify ORDER BY newest_item_at DESC matches idx_feeds_newest index
// ---------------------------------------------------------------------------

#[test]
fn recent_feeds_ordering() {
    let conn = common::test_db();
    let base_date: i64 = 1_700_000_000;

    let artist_id = insert_artist(&conn, "Various Artists");
    let credit_id = insert_credit(&conn, &artist_id, "Various Artists");

    let feed1 = uuid::Uuid::new_v4().to_string();
    let feed2 = uuid::Uuid::new_v4().to_string();
    let feed3 = uuid::Uuid::new_v4().to_string();

    // Insert feeds with no newest_item_at first, then update them
    insert_feed(&conn, &feed1, "https://example.com/f1.xml", "Feed Alpha", credit_id);
    insert_feed(&conn, &feed2, "https://example.com/f2.xml", "Feed Beta", credit_id);
    insert_feed(&conn, &feed3, "https://example.com/f3.xml", "Feed Gamma", credit_id);

    // Set different newest_item_at values
    conn.execute(
        "UPDATE feeds SET newest_item_at = ?1 WHERE feed_guid = ?2",
        params![base_date + 100, feed1],
    )
    .unwrap();
    conn.execute(
        "UPDATE feeds SET newest_item_at = ?1 WHERE feed_guid = ?2",
        params![base_date + 300, feed2],
    )
    .unwrap();
    conn.execute(
        "UPDATE feeds SET newest_item_at = ?1 WHERE feed_guid = ?2",
        params![base_date + 200, feed3],
    )
    .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT feed_guid, title, newest_item_at FROM feeds \
             WHERE newest_item_at IS NOT NULL \
             ORDER BY newest_item_at DESC",
        )
        .unwrap();

    let rows: Vec<(String, String, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, feed2, "Feed Beta (newest_item_at=+300) should be first");
    assert_eq!(rows[0].2, base_date + 300);
    assert_eq!(rows[1].0, feed3, "Feed Gamma (newest_item_at=+200) should be second");
    assert_eq!(rows[1].2, base_date + 200);
    assert_eq!(rows[2].0, feed1, "Feed Alpha (newest_item_at=+100) should be third");
    assert_eq!(rows[2].2, base_date + 100);
}

// ---------------------------------------------------------------------------
// 6. feed_payment_routes_stored — insert a feed + 2 feed_payment_routes, verify
// ---------------------------------------------------------------------------

#[test]
fn feed_payment_routes_stored() {
    let conn = common::test_db();

    let artist_id = insert_artist(&conn, "Vulfpeck");
    let credit_id = insert_credit(&conn, &artist_id, "Vulfpeck");
    let feed_guid = uuid::Uuid::new_v4().to_string();
    insert_feed(
        &conn,
        &feed_guid,
        "https://example.com/vulfpeck.xml",
        "The Beautiful Game",
        credit_id,
    );

    conn.execute(
        "INSERT INTO feed_payment_routes \
         (feed_guid, recipient_name, route_type, address, custom_key, custom_value, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![feed_guid, "Band Fund", "keysend", "wallet-aaa-111", "7629169", "podcast_id_1234", 80, 0],
    )
    .unwrap();

    conn.execute(
        "INSERT INTO feed_payment_routes \
         (feed_guid, recipient_name, route_type, address, custom_key, custom_value, split, fee) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![feed_guid, "Hosting Service", "keysend", "wallet-bbb-222", rusqlite::types::Null, rusqlite::types::Null, 20, 1],
    )
    .unwrap();

    let mut stmt = conn
        .prepare(
            "SELECT feed_guid, recipient_name, route_type, address, \
             custom_key, custom_value, split, fee \
             FROM feed_payment_routes WHERE feed_guid = ?1 ORDER BY split DESC",
        )
        .unwrap();

    let rows: Vec<Vec<rusqlite::types::Value>> = stmt
        .query_map(params![feed_guid], |r| {
            Ok(vec![
                r.get::<_, rusqlite::types::Value>(0)?,
                r.get::<_, rusqlite::types::Value>(1)?,
                r.get::<_, rusqlite::types::Value>(2)?,
                r.get::<_, rusqlite::types::Value>(3)?,
                r.get::<_, rusqlite::types::Value>(4)?,
                r.get::<_, rusqlite::types::Value>(5)?,
                r.get::<_, rusqlite::types::Value>(6)?,
                r.get::<_, rusqlite::types::Value>(7)?,
            ])
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(rows.len(), 2, "should return both feed payment routes");

    // Helper to extract string from Value
    fn val_str(v: &rusqlite::types::Value) -> Option<&str> {
        match v {
            rusqlite::types::Value::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
    fn val_i64(v: &rusqlite::types::Value) -> i64 {
        match v {
            rusqlite::types::Value::Integer(n) => *n,
            _ => panic!("expected integer"),
        }
    }

    // First route (split=80)
    assert_eq!(val_str(&rows[0][0]).unwrap(), feed_guid);
    assert_eq!(val_str(&rows[0][1]), Some("Band Fund"));
    assert_eq!(val_str(&rows[0][2]), Some("keysend"));
    assert_eq!(val_str(&rows[0][3]), Some("wallet-aaa-111"));
    assert_eq!(val_str(&rows[0][4]), Some("7629169"));
    assert_eq!(val_str(&rows[0][5]), Some("podcast_id_1234"));
    assert_eq!(val_i64(&rows[0][6]), 80);
    assert_eq!(val_i64(&rows[0][7]), 0);

    // Second route (split=20)
    assert_eq!(val_str(&rows[1][1]), Some("Hosting Service"));
    assert_eq!(val_str(&rows[1][3]), Some("wallet-bbb-222"));
    assert!(matches!(rows[1][4], rusqlite::types::Value::Null));
    assert!(matches!(rows[1][5], rusqlite::types::Value::Null));
    assert_eq!(val_i64(&rows[1][6]), 20);
    assert_eq!(val_i64(&rows[1][7]), 1);
}
