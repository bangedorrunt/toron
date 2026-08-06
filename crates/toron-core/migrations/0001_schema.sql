-- governed-by: ADR-0003
-- bd-led.4.1: AM-Rust-compatible SQLite index schema.
-- Timestamps are i64-microseconds (UNIX epoch, µs); local_seq breaks same-µs ties (ADR G7).

CREATE TABLE IF NOT EXISTS agents (
    id                INTEGER PRIMARY KEY,
    project_slug      TEXT    NOT NULL,
    name              TEXT    NOT NULL,
    pubkey            TEXT    NOT NULL,
    program           TEXT    NOT NULL DEFAULT '',
    model             TEXT    NOT NULL DEFAULT '',
    task_description  TEXT    NOT NULL DEFAULT '',
    contact_policy    TEXT    NOT NULL DEFAULT 'auto',
    created_ts_us     INTEGER NOT NULL,
    last_active_ts_us INTEGER,
    UNIQUE (project_slug, name)
);

CREATE TABLE IF NOT EXISTS messages (
    id            INTEGER PRIMARY KEY,
    event_id      TEXT    NOT NULL UNIQUE,
    project_slug  TEXT    NOT NULL,
    sender        TEXT    NOT NULL,
    sender_pubkey TEXT    NOT NULL,
    subject       TEXT    NOT NULL DEFAULT '',
    body          TEXT    NOT NULL DEFAULT '',
    importance    TEXT    NOT NULL DEFAULT 'normal',
    ack_required  INTEGER NOT NULL DEFAULT 0,
    ack_ts_us     INTEGER,
    read_ts_us    INTEGER,
    thread_id     TEXT,
    reply_to      TEXT,
    to_list       TEXT    NOT NULL DEFAULT '[]',
    cc_list       TEXT    NOT NULL DEFAULT '[]',
    created_ts_us INTEGER NOT NULL,
    local_seq     INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_project_ts
    ON messages (project_slug, created_ts_us DESC);

CREATE INDEX IF NOT EXISTS idx_messages_thread
    ON messages (thread_id, created_ts_us);

CREATE TABLE IF NOT EXISTS threads (
    id           INTEGER PRIMARY KEY,
    thread_id    TEXT    NOT NULL UNIQUE,
    project_slug TEXT    NOT NULL,
    subject      TEXT    NOT NULL DEFAULT '',
    msg_count    INTEGER NOT NULL DEFAULT 0,
    last_ts_us   INTEGER
);

CREATE TABLE IF NOT EXISTS contacts (
    id          INTEGER PRIMARY KEY,
    project_slug TEXT   NOT NULL,
    from_name   TEXT    NOT NULL,
    from_pubkey TEXT    NOT NULL,
    to_name     TEXT    NOT NULL,
    to_pubkey   TEXT    NOT NULL,
    to_project  TEXT,
    status      TEXT    NOT NULL DEFAULT 'pending',
    created_ts_us INTEGER NOT NULL,
    UNIQUE (from_pubkey, to_pubkey)
);

CREATE TABLE IF NOT EXISTS file_reservations (
    id           INTEGER PRIMARY KEY,
    project_slug TEXT    NOT NULL,
    agent_name   TEXT    NOT NULL,
    agent_pubkey TEXT    NOT NULL,
    path_pattern TEXT    NOT NULL,
    exclusive    INTEGER NOT NULL DEFAULT 1,
    reason       TEXT    NOT NULL DEFAULT '',
    status       TEXT    NOT NULL DEFAULT 'active',
    event_id     TEXT,
    expires_ts_us INTEGER,
    created_ts_us INTEGER NOT NULL,
    UNIQUE (project_slug, path_pattern, agent_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_reservations_project
    ON file_reservations (project_slug, status);

CREATE TABLE IF NOT EXISTS build_slots (
    id           INTEGER PRIMARY KEY,
    project_slug TEXT    NOT NULL,
    slot         TEXT    NOT NULL,
    agent_name   TEXT    NOT NULL DEFAULT '',
    agent_pubkey TEXT    NOT NULL DEFAULT '',
    exclusive    INTEGER NOT NULL DEFAULT 0,
    status       TEXT    NOT NULL DEFAULT 'active',
    event_id     TEXT,
    expires_ts_us INTEGER,
    created_ts_us INTEGER NOT NULL,
    UNIQUE (project_slug, slot)
);

CREATE TABLE IF NOT EXISTS products (
    id           INTEGER PRIMARY KEY,
    product_key  TEXT    NOT NULL UNIQUE,
    name         TEXT    NOT NULL DEFAULT '',
    created_ts_us INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS product_projects (
    product_id   INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    project_slug TEXT    NOT NULL,
    PRIMARY KEY (product_id, project_slug)
);

CREATE TABLE IF NOT EXISTS proof_nonces (
    id           INTEGER PRIMARY KEY,
    nonce        TEXT    NOT NULL UNIQUE,
    pubkey       TEXT    NOT NULL,
    scope        TEXT    NOT NULL DEFAULT '',
    used         INTEGER NOT NULL DEFAULT 0,
    created_ts_us INTEGER NOT NULL,
    used_ts_us   INTEGER
);

-- FTS5: AM-Rust tokenizer contract (schema.rs):
--   porter unicode61 remove_diacritics 2, prefix='2 3'
-- External-content table keeps FTS in sync with `messages` via triggers.
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5 (
    subject,
    body,
    content = 'messages',
    content_rowid = 'id',
    tokenize = 'porter unicode61 remove_diacritics 2',
    prefix = '2 3'
);

CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts (rowid, subject, body)
    VALUES (new.id, new.subject, new.body);
END;

CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, subject, body)
    VALUES ('delete', old.id, old.subject, old.body);
END;

CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE ON messages BEGIN
    INSERT INTO messages_fts (messages_fts, rowid, subject, body)
    VALUES ('delete', old.id, old.subject, old.body);
    INSERT INTO messages_fts (rowid, subject, body)
    VALUES (new.id, new.subject, new.body);
END;
