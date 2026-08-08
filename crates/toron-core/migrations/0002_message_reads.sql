-- governed-by: ADR-0003
-- bd-5icg: per-recipient read state (read receipts in the projection).
--
-- Read state is PROJECTION state, never event state (bd-5icg decision):
-- -- written ONLY by mark_message_read / acknowledge_message;
-- -- fetched by fetch_inbox(unread_only: true);
-- -- bus/replay and Nostr events carry no read info (events are immutable).
-- Consequence: an index rebuild (drop SQLite -> rebuild from Git/bus) loses
-- read receipts. Acceptable and documented; the archive does not store them.
--
-- Why not the `messages.read_ts_us` column (dead since 0001): the `messages`
-- row is UNIQUE(event_id) with NO recipient column, so a shared read_ts there
-- would leak one recipient's read state to every co-recipient of the same
-- rumor (to/cc fan-out). Read state is per-(message, recipient) by design.

CREATE TABLE IF NOT EXISTS message_reads (
    id           INTEGER PRIMARY KEY,
    project_slug TEXT    NOT NULL,
    message_id   TEXT    NOT NULL,
    recipient    TEXT    NOT NULL,
    read_ts_us   INTEGER NOT NULL,
    UNIQUE (message_id, recipient)
);

CREATE INDEX IF NOT EXISTS idx_message_reads_recipient
    ON message_reads (recipient, read_ts_us);
