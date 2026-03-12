PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

-- ---------------------------------------------------------------------------
-- LOOKUP TABLES
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS artist_type (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS feed_type (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
) STRICT;

CREATE TABLE IF NOT EXISTS rel_type (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    entity_pair TEXT NOT NULL,
    description TEXT
) STRICT;

-- ---------------------------------------------------------------------------
-- CORE ENTITY TABLES
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS artists (
    artist_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    name_lower  TEXT NOT NULL,
    sort_name   TEXT,
    type_id     INTEGER REFERENCES artist_type(id),
    area        TEXT,
    img_url     TEXT,
    url         TEXT,
    begin_year  INTEGER,
    end_year    INTEGER,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_artists_name_lower ON artists(name_lower);

CREATE TABLE IF NOT EXISTS artist_aliases (
    alias_lower  TEXT NOT NULL,
    artist_id    TEXT NOT NULL REFERENCES artists(artist_id),
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (alias_lower, artist_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_aliases_lower ON artist_aliases(alias_lower);

-- MusicBrainz-style artist credits
CREATE TABLE IF NOT EXISTS artist_credit (
    id           INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at   INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS artist_credit_name (
    id               INTEGER PRIMARY KEY,
    artist_credit_id INTEGER NOT NULL REFERENCES artist_credit(id),
    artist_id        TEXT NOT NULL REFERENCES artists(artist_id),
    position         INTEGER NOT NULL,
    name             TEXT NOT NULL,
    join_phrase      TEXT NOT NULL DEFAULT '',
    UNIQUE(artist_credit_id, position)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_acn_credit ON artist_credit_name(artist_credit_id);
CREATE INDEX IF NOT EXISTS idx_acn_artist ON artist_credit_name(artist_id);

CREATE TABLE IF NOT EXISTS feeds (
    feed_guid        TEXT PRIMARY KEY,
    feed_url         TEXT NOT NULL UNIQUE,
    title            TEXT NOT NULL,
    title_lower      TEXT NOT NULL,
    artist_credit_id INTEGER NOT NULL REFERENCES artist_credit(id),
    description      TEXT,
    image_url        TEXT,
    language         TEXT,
    explicit         INTEGER NOT NULL DEFAULT 0,
    itunes_type      TEXT,
    episode_count    INTEGER NOT NULL DEFAULT 0,
    newest_item_at   INTEGER,
    oldest_item_at   INTEGER,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL,
    raw_medium       TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_feeds_credit ON feeds(artist_credit_id);
CREATE INDEX IF NOT EXISTS idx_feeds_newest ON feeds(newest_item_at DESC);
CREATE INDEX IF NOT EXISTS idx_feeds_title  ON feeds(title_lower);

CREATE TABLE IF NOT EXISTS tracks (
    track_guid       TEXT PRIMARY KEY,
    feed_guid        TEXT NOT NULL REFERENCES feeds(feed_guid),
    artist_credit_id INTEGER NOT NULL REFERENCES artist_credit(id),
    title            TEXT NOT NULL,
    title_lower      TEXT NOT NULL,
    pub_date         INTEGER,
    duration_secs    INTEGER,
    enclosure_url    TEXT,
    enclosure_type   TEXT,
    enclosure_bytes  INTEGER,
    track_number     INTEGER,
    season           INTEGER,
    explicit         INTEGER NOT NULL DEFAULT 0,
    description      TEXT,
    created_at       INTEGER NOT NULL,
    updated_at       INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_tracks_feed     ON tracks(feed_guid);
CREATE INDEX IF NOT EXISTS idx_tracks_credit   ON tracks(artist_credit_id);
CREATE INDEX IF NOT EXISTS idx_tracks_pub_date ON tracks(pub_date DESC);
CREATE INDEX IF NOT EXISTS idx_tracks_title    ON tracks(title_lower);

-- Track-level payment routes
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

-- Feed-level payment routes
CREATE TABLE IF NOT EXISTS feed_payment_routes (
    id              INTEGER PRIMARY KEY,
    feed_guid       TEXT NOT NULL REFERENCES feeds(feed_guid),
    recipient_name  TEXT,
    route_type      TEXT NOT NULL,
    address         TEXT NOT NULL,
    custom_key      TEXT,
    custom_value    TEXT,
    split           INTEGER NOT NULL,
    fee             INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE INDEX IF NOT EXISTS idx_feed_routes_guid ON feed_payment_routes(feed_guid);

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

-- ---------------------------------------------------------------------------
-- EVENTS & SYNC
-- ---------------------------------------------------------------------------

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

CREATE TABLE IF NOT EXISTS peer_nodes (
    node_pubkey          TEXT NOT NULL PRIMARY KEY,
    node_url             TEXT NOT NULL,
    discovered_at        INTEGER NOT NULL,
    last_push_at         INTEGER,
    consecutive_failures INTEGER NOT NULL DEFAULT 0
) STRICT;

-- ---------------------------------------------------------------------------
-- METADATA TABLES
-- ---------------------------------------------------------------------------

-- Artist location (latitude/longitude for map views)
CREATE TABLE IF NOT EXISTS artist_location (
    artist_id  TEXT PRIMARY KEY REFERENCES artists(artist_id),
    latitude   REAL NOT NULL,
    longitude  REAL NOT NULL,
    city       TEXT,
    region     TEXT,
    country    TEXT
) STRICT;

-- Artist-to-artist relationships (member_of, collaboration, etc.)
CREATE TABLE IF NOT EXISTS artist_artist_rel (
    id          INTEGER PRIMARY KEY,
    artist_id_a TEXT NOT NULL REFERENCES artists(artist_id),
    artist_id_b TEXT NOT NULL REFERENCES artists(artist_id),
    rel_type_id INTEGER NOT NULL REFERENCES rel_type(id),
    begin_year  INTEGER,
    end_year    INTEGER,
    created_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_aar_a ON artist_artist_rel(artist_id_a);
CREATE INDEX IF NOT EXISTS idx_aar_b ON artist_artist_rel(artist_id_b);

-- Artist ID redirect (when artists are merged, old ID -> new ID)
CREATE TABLE IF NOT EXISTS artist_id_redirect (
    old_artist_id TEXT PRIMARY KEY,
    new_artist_id TEXT NOT NULL REFERENCES artists(artist_id),
    merged_at     INTEGER NOT NULL
) STRICT;

-- Track relationships (featuring, remix_of, cover_of)
CREATE TABLE IF NOT EXISTS track_rel (
    id           INTEGER PRIMARY KEY,
    track_guid_a TEXT NOT NULL REFERENCES tracks(track_guid),
    track_guid_b TEXT NOT NULL REFERENCES tracks(track_guid),
    rel_type_id  INTEGER NOT NULL REFERENCES rel_type(id),
    created_at   INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_trel_a ON track_rel(track_guid_a);
CREATE INDEX IF NOT EXISTS idx_trel_b ON track_rel(track_guid_b);

-- Feed relationships
CREATE TABLE IF NOT EXISTS feed_rel (
    id           INTEGER PRIMARY KEY,
    feed_guid_a  TEXT NOT NULL REFERENCES feeds(feed_guid),
    feed_guid_b  TEXT NOT NULL REFERENCES feeds(feed_guid),
    rel_type_id  INTEGER NOT NULL REFERENCES rel_type(id),
    created_at   INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_frel_a ON feed_rel(feed_guid_a);
CREATE INDEX IF NOT EXISTS idx_frel_b ON feed_rel(feed_guid_b);

-- ---------------------------------------------------------------------------
-- TAGS
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tags (
    id         INTEGER PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS artist_tag (
    artist_id  TEXT NOT NULL REFERENCES artists(artist_id),
    tag_id     INTEGER NOT NULL REFERENCES tags(id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (artist_id, tag_id)
) STRICT;

CREATE TABLE IF NOT EXISTS feed_tag (
    feed_guid  TEXT NOT NULL REFERENCES feeds(feed_guid),
    tag_id     INTEGER NOT NULL REFERENCES tags(id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (feed_guid, tag_id)
) STRICT;

CREATE TABLE IF NOT EXISTS track_tag (
    track_guid TEXT NOT NULL REFERENCES tracks(track_guid),
    tag_id     INTEGER NOT NULL REFERENCES tags(id),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (track_guid, tag_id)
) STRICT;

-- ---------------------------------------------------------------------------
-- EXTERNAL IDS & PROVENANCE
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS external_ids (
    id          INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    scheme      TEXT NOT NULL,
    value       TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    UNIQUE(entity_type, entity_id, scheme)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_extid_entity ON external_ids(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_extid_scheme ON external_ids(scheme, value);

CREATE TABLE IF NOT EXISTS entity_source (
    id          INTEGER PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_url  TEXT,
    trust_level INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_esrc_entity ON entity_source(entity_type, entity_id);

CREATE TABLE IF NOT EXISTS manifest_source (
    id           INTEGER PRIMARY KEY,
    feed_guid    TEXT NOT NULL REFERENCES feeds(feed_guid),
    manifest_url TEXT NOT NULL UNIQUE,
    signed_by    TEXT,
    verified_at  INTEGER,
    created_at   INTEGER NOT NULL
) STRICT;

-- ---------------------------------------------------------------------------
-- QUALITY
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS entity_quality (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    score       INTEGER NOT NULL DEFAULT 0,
    computed_at INTEGER NOT NULL,
    PRIMARY KEY (entity_type, entity_id)
) STRICT;

CREATE TABLE IF NOT EXISTS entity_field_status (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    field_name  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'present',
    PRIMARY KEY (entity_type, entity_id, field_name)
) STRICT;

-- ---------------------------------------------------------------------------
-- FTS5 SEARCH (virtual table, no STRICT mode)
-- ---------------------------------------------------------------------------

CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    entity_type,
    entity_id,
    name,
    title,
    description,
    tags,
    content='',
    tokenize='unicode61'
);

-- ---------------------------------------------------------------------------
-- SEED DATA
-- ---------------------------------------------------------------------------

-- Seed artist_type
INSERT OR IGNORE INTO artist_type (id, name) VALUES (1, 'person');
INSERT OR IGNORE INTO artist_type (id, name) VALUES (2, 'group');
INSERT OR IGNORE INTO artist_type (id, name) VALUES (3, 'orchestra');
INSERT OR IGNORE INTO artist_type (id, name) VALUES (4, 'choir');
INSERT OR IGNORE INTO artist_type (id, name) VALUES (5, 'character');
INSERT OR IGNORE INTO artist_type (id, name) VALUES (6, 'other');

-- Seed feed_type
INSERT OR IGNORE INTO feed_type (id, name) VALUES (1, 'album');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (2, 'ep');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (3, 'single');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (4, 'compilation');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (5, 'soundtrack');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (6, 'live');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (7, 'remix');
INSERT OR IGNORE INTO feed_type (id, name) VALUES (8, 'other');

-- Seed rel_type
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (1, 'performer', 'artist-track', 'Primary performing artist');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (2, 'songwriter', 'artist-track', 'Writer of the song');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (3, 'producer', 'artist-track', 'Music producer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (4, 'engineer', 'artist-track', 'Sound/recording engineer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (5, 'mixer', 'artist-track', 'Mixing engineer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (6, 'mastering', 'artist-track', 'Mastering engineer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (7, 'composer', 'artist-track', 'Composer of the music');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (8, 'lyricist', 'artist-track', 'Writer of the lyrics');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (9, 'arranger', 'artist-track', 'Musical arranger');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (10, 'conductor', 'artist-track', 'Orchestra/ensemble conductor');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (11, 'dj', 'artist-track', 'DJ / selector');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (12, 'remixer', 'artist-track', 'Created a remix');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (13, 'featuring', 'artist-track', 'Featured artist');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (14, 'vocal', 'artist-track', 'Vocal performance');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (15, 'instrument', 'artist-track', 'Instrument performance');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (16, 'programming', 'artist-track', 'Electronic programming');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (17, 'recording', 'artist-track', 'Recording engineer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (18, 'mixing', 'artist-track', 'Mixing');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (19, 'live_sound', 'artist-track', 'Live sound engineer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (20, 'member_of', 'artist-artist', 'Member of a group');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (21, 'collaboration', 'artist-artist', 'Collaborative project');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (22, 'cover_art', 'artist-feed', 'Cover art creator');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (23, 'photographer', 'artist-feed', 'Photographer');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (24, 'liner_notes', 'artist-feed', 'Liner notes author');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (25, 'translator', 'artist-feed', 'Translator');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (26, 'promoter', 'artist-feed', 'Promoter');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (27, 'booking', 'artist-artist', 'Booking agent');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (28, 'management', 'artist-artist', 'Artist management');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (29, 'label', 'artist-feed', 'Record label');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (30, 'publisher', 'artist-feed', 'Music publisher');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (31, 'distributor', 'artist-feed', 'Distributor');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (32, 'legal', 'artist-feed', 'Legal representation');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (33, 'marketing', 'artist-feed', 'Marketing');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (34, 'a_and_r', 'artist-feed', 'A&R representative');
INSERT OR IGNORE INTO rel_type (id, name, entity_pair, description) VALUES (35, 'other', 'artist-track', 'Other role');
