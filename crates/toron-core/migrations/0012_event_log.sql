-- governed-by: ADR-0035
-- Wave-4 storage truth (bd-lnit): one event log, many projections.
-- `events` + the generation manifest is the source of truth; the index,
-- receipts, workflow effects, and exports are projections rebuilt from it.
-- Flush appends events and advances the manifest; exports stay commutative
-- by event_id dedup. Times are int-microseconds (ADR G7).

CREATE TABLE IF NOT EXISTS events (
    generation INTEGER NOT NULL,
    event_id   TEXT PRIMARY KEY,
    kind       TEXT NOT NULL,
    payload    TEXT NOT NULL,
    ts_us      INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS events_generation ON events (generation);

CREATE INDEX IF NOT EXISTS events_kind ON events (kind);

CREATE TABLE IF NOT EXISTS generation_manifest (
    singleton      INTEGER PRIMARY KEY CHECK (singleton = 1),
    generation     INTEGER NOT NULL,
    updated_ts_us  INTEGER NOT NULL
);

INSERT INTO generation_manifest (singleton, generation, updated_ts_us)
    SELECT 1, 0, 0
    WHERE NOT EXISTS (SELECT 1 FROM generation_manifest WHERE singleton = 1);
