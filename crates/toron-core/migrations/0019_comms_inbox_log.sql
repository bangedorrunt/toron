-- ADR-0041 (stack-comms Phase 1): the per-recipient log, its producer
-- fence, one cursor notion, and per-(message, recipient) delivery state.
-- Additive: nothing reads these unless `[comms] inbox_log = true` /
-- TORON_COMMS_INBOX=1. Redesign §2.2 names them inbox_log / producer_state /
-- cursor / delivery; they carry an `inbox_` prefix here so they cannot be
-- confused with the legacy `inbox_cursors` (0007) and `delivery_cursors`
-- (0016), which they will replace once the flag defaults on.

-- One table backs every toron DurableLog stream ('inbox' for envelopes,
-- 'journal' for a sender loop's durable intents). Offsets are minted by
-- AUTOINCREMENT, so they are monotone and never reused. The L16 triple is
-- unique across the whole log: the same triple appends once.
CREATE TABLE IF NOT EXISTS inbox_log (
    log_offset     INTEGER PRIMARY KEY AUTOINCREMENT,
    stream         TEXT    NOT NULL,
    recipient      TEXT    NOT NULL DEFAULT '',
    producer       TEXT    NOT NULL,
    epoch          INTEGER NOT NULL,
    seq            INTEGER NOT NULL,
    msg_key        TEXT    NOT NULL,
    payload        TEXT    NOT NULL,
    appended_ts_us INTEGER NOT NULL,
    UNIQUE (producer, epoch, seq)
);
CREATE INDEX IF NOT EXISTS inbox_log_by_recipient ON inbox_log (stream, recipient, log_offset);

-- Current epoch and last accepted seq per producer. Never expires (L16).
CREATE TABLE IF NOT EXISTS inbox_producer_state (
    producer TEXT    PRIMARY KEY,
    epoch    INTEGER NOT NULL,
    last_seq INTEGER NOT NULL
);

-- (reader, stream) -> committed offset. Monotone; only the reader's own
-- stream can be committed (enforced in toron_core::inbox, refused for any
-- other reader).
CREATE TABLE IF NOT EXISTS inbox_cursor (
    reader_npub   TEXT    NOT NULL,
    stream        TEXT    NOT NULL,
    log_offset    INTEGER NOT NULL,
    updated_ts_us INTEGER NOT NULL,
    PRIMARY KEY (reader_npub, stream)
);

-- Delivery state per (message, recipient): DeliveryState as JSON plus the
-- columns queries need. Moved only through DeliveryFsm::step.
CREATE TABLE IF NOT EXISTS inbox_delivery (
    msg_key       TEXT    NOT NULL,
    recipient     TEXT    NOT NULL,
    phase         TEXT    NOT NULL,
    gen           INTEGER NOT NULL,
    task_id       TEXT,
    state_json    TEXT    NOT NULL,
    evidence_json TEXT,
    updated_ts_us INTEGER NOT NULL,
    PRIMARY KEY (msg_key, recipient)
);
CREATE INDEX IF NOT EXISTS inbox_delivery_by_task ON inbox_delivery (task_id);
