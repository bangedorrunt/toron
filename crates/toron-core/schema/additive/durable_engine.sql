-- governed-by: ADR-0005
-- Additive durable-engine state (elm-effect-rs OurEngine): run metadata and
-- active parks. No older binary reads these tables, so their absence is
-- correct; no versioned migration and no store-skew bump.

CREATE TABLE IF NOT EXISTS durable_engine_runs (
    run_key TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    generation INTEGER NOT NULL,
    until_ms INTEGER,
    attempt INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS durable_engine_parks (
    run_key TEXT NOT NULL,
    until_ms INTEGER NOT NULL,
    park_generation INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (run_key, until_ms)
);

CREATE INDEX IF NOT EXISTS idx_durable_engine_parks_until
    ON durable_engine_parks (until_ms);
