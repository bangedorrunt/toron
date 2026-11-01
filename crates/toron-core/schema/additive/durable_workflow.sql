-- governed-by: ADR-0005
-- Additive state for the pure linear durable workflow model. No older binary
-- reads it, so its absence is correct; no versioned migration and no
-- store-skew bump.

CREATE TABLE IF NOT EXISTS durable_workflow_runs (
    run_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
