use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Opens an in-memory SQLite database with the stophammer schema applied.
/// Uses the same schema.sql that the production code uses (via include_str!).
pub fn test_db() -> Connection {
    let conn = Connection::open_in_memory().expect("failed to open in-memory database");
    // Apply the schema exactly as production does
    const SCHEMA: &str = include_str!("../../src/schema.sql");
    conn.execute_batch(SCHEMA).expect("failed to apply schema");
    conn
}

/// Returns the DB as an Arc<Mutex<Connection>> matching the `db::Db` type.
#[allow(dead_code)]
pub fn test_db_arc() -> Arc<Mutex<Connection>> {
    Arc::new(Mutex::new(test_db()))
}

/// Returns current unix timestamp as i64.
pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
