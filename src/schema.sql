PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS artists (
    artist_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    name_lower  TEXT NOT NULL,
    created_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_artists_name_lower ON artists(name_lower);

CREATE TABLE IF NOT EXISTS feeds (
    feed_guid       TEXT PRIMARY KEY,
    feed_url        TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    title_lower     TEXT NOT NULL,
    artist_id       TEXT NOT NULL REFERENCES artists(artist_id),
    description     TEXT,
    image_url       TEXT,
    language        TEXT,
    explicit        INTEGER NOT NULL DEFAULT 0,
    itunes_type     TEXT,
    episode_count   INTEGER NOT NULL DEFAULT 0,
    newest_item_at  INTEGER,
    oldest_item_at  INTEGER,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    raw_medium      TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_feeds_artist  ON feeds(artist_id);
CREATE INDEX IF NOT EXISTS idx_feeds_newest  ON feeds(newest_item_at DESC);
CREATE INDEX IF NOT EXISTS idx_feeds_title   ON feeds(title_lower);

CREATE TABLE IF NOT EXISTS tracks (
    track_guid      TEXT PRIMARY KEY,
    feed_guid       TEXT NOT NULL REFERENCES feeds(feed_guid),
    artist_id       TEXT NOT NULL REFERENCES artists(artist_id),
    title           TEXT NOT NULL,
    title_lower     TEXT NOT NULL,
    pub_date        INTEGER,
    duration_secs   INTEGER,
    enclosure_url   TEXT,
    enclosure_type  TEXT,
    enclosure_bytes INTEGER,
    track_number    INTEGER,
    season          INTEGER,
    explicit        INTEGER NOT NULL DEFAULT 0,
    description     TEXT,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_tracks_feed     ON tracks(feed_guid);
CREATE INDEX IF NOT EXISTS idx_tracks_artist   ON tracks(artist_id);
CREATE INDEX IF NOT EXISTS idx_tracks_pub_date ON tracks(pub_date DESC);
CREATE INDEX IF NOT EXISTS idx_tracks_title    ON tracks(title_lower);

CREATE TABLE IF NOT EXISTS payment_routes (
    id              INTEGER PRIMARY KEY,
    track_guid      TEXT NOT NULL REFERENCES tracks(track_guid),
    feed_guid       TEXT NOT NULL,
    recipient_name  TEXT,
    route_type      TEXT NOT NULL,
    address         TEXT NOT NULL,
    custom_key      TEXT,
    custom_value    TEXT,
    split           INTEGER NOT NULL,
    fee             INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE INDEX IF NOT EXISTS idx_routes_track ON payment_routes(track_guid);
CREATE INDEX IF NOT EXISTS idx_routes_feed  ON payment_routes(feed_guid);

CREATE TABLE IF NOT EXISTS value_time_splits (
    id                  INTEGER PRIMARY KEY,
    source_track_guid   TEXT NOT NULL REFERENCES tracks(track_guid),
    start_time_secs     INTEGER NOT NULL,
    duration_secs       INTEGER,
    remote_feed_guid    TEXT NOT NULL,
    remote_item_guid    TEXT NOT NULL,
    split               INTEGER NOT NULL,
    created_at          INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_vts_source ON value_time_splits(source_track_guid);

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    subject_guid    TEXT NOT NULL,
    signed_by       TEXT NOT NULL,
    signature       TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    created_at      INTEGER NOT NULL,
    warnings_json   TEXT NOT NULL DEFAULT '[]'
) STRICT;

CREATE INDEX IF NOT EXISTS idx_events_seq     ON events(seq);
CREATE INDEX IF NOT EXISTS idx_events_subject ON events(subject_guid);
CREATE INDEX IF NOT EXISTS idx_events_type    ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);

-- Crawl cache: used by ContentHashVerifier to detect no-change submissions
CREATE TABLE IF NOT EXISTS feed_crawl_cache (
    feed_url     TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    crawled_at   INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS node_sync_state (
    node_pubkey  TEXT PRIMARY KEY,
    last_seq     INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL
) STRICT;

-- Artist aliases: multiple name variants can map to the same canonical artist.
-- Every new artist auto-registers its canonical name here so merges need no
-- special-case for the primary name.
CREATE TABLE IF NOT EXISTS artist_aliases (
    alias_lower  TEXT NOT NULL,
    artist_id    TEXT NOT NULL REFERENCES artists(artist_id),
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (alias_lower, artist_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_aliases_lower ON artist_aliases(alias_lower);

CREATE TABLE IF NOT EXISTS peer_nodes (
    node_pubkey          TEXT NOT NULL PRIMARY KEY,
    node_url             TEXT NOT NULL,
    discovered_at        INTEGER NOT NULL,
    last_push_at         INTEGER,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
) STRICT;
