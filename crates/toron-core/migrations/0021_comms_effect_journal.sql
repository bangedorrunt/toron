-- ADR-0041 (stack-comms Phase 2): the effect journal behind
-- toron_core::journal::ToronJournal, a kernel::JournalStore over SQLite.
-- Additive: read only by code that opens a ToronJournal.
--
-- One row per (scope, key): the intent recorded before the provider runs.
-- Claims bump `generation`; settles carry it and a stale one changes
-- nothing. Every committed change appends one effect_journal_log entry in
-- the SAME transaction, so the row's `log_offset` and its state never
-- disagree. Offsets are AUTOINCREMENT: monotone, never reused.
CREATE TABLE IF NOT EXISTS effect_journal (
    scope          TEXT    NOT NULL,
    key            TEXT    NOT NULL,
    state          TEXT    NOT NULL,
    owner          TEXT,
    lease_deadline INTEGER,
    generation     INTEGER NOT NULL,
    log_offset     INTEGER NOT NULL,
    disposition    TEXT,
    attempts       INTEGER NOT NULL,
    max_attempts   INTEGER NOT NULL,
    PRIMARY KEY (scope, key)
);

-- The change log: one entry per commit, carrying the row as committed.
-- Retention trims the oldest entries per scope; reading below the first
-- retained offset answers Gone, never a silent skip.
CREATE TABLE IF NOT EXISTS effect_journal_log (
    log_offset INTEGER PRIMARY KEY AUTOINCREMENT,
    scope      TEXT    NOT NULL,
    key        TEXT    NOT NULL,
    row_json   TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS effect_journal_log_by_scope ON effect_journal_log (scope, log_offset);
