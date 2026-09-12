-- governed-by: ADR-0032
-- Workflow-effect journal storage (Spec 5.1, bd-v139.1): run ledger,
-- logical-effect current state, append-only attempt evidence, parked
-- pending work, and terminal-publication evidence in the existing SQLite
-- persistence boundary. No new database. The mailbox `receipts` table is
-- untouched and unreferenced: message lifecycle never substitutes for
-- workflow-effect state.
-- States are TEXT (typed enums in index::workflow_effects); times are
-- int-microseconds (ADR G7); bounded evidence projections are TEXT.

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    project TEXT,
    workflow TEXT NOT NULL,
    workflow_digest TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    suspended_at INTEGER,
    finished_at INTEGER,
    current_node TEXT,
    failed_node TEXT,
    terminal_digest TEXT,
    artifact_paths TEXT,
    failure_projection TEXT,
    terminal_publication_state TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workflow_effects (
    effect_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    project TEXT,
    workflow_digest TEXT NOT NULL,
    node_path TEXT NOT NULL,
    iteration_key TEXT,
    action_type TEXT NOT NULL,
    action_digest TEXT NOT NULL,
    redacted_action TEXT,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    attempt_id TEXT,
    claim_token TEXT,
    lease_expires_at INTEGER,
    transport_corr TEXT,
    external_id TEXT,
    outcome_json TEXT,
    failure_json TEXT,
    reconcile_after INTEGER,
    retention_until INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_effects_run
    ON workflow_effects (run_id);

CREATE TABLE IF NOT EXISTS workflow_effect_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_key TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    attempt_id TEXT NOT NULL,
    claim_token TEXT,
    state TEXT NOT NULL,
    evidence_json TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_effect_attempts_key
    ON workflow_effect_attempts (effect_key);

CREATE TABLE IF NOT EXISTS workflow_pending (
    run_id TEXT NOT NULL,
    promise_id TEXT NOT NULL,
    park_generation INTEGER NOT NULL,
    project TEXT,
    workflow TEXT NOT NULL,
    node_path TEXT NOT NULL,
    effect_key TEXT,
    kind TEXT NOT NULL,
    pending_payload TEXT,
    deadline INTEGER,
    publication_state TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (run_id, promise_id, park_generation)
);

CREATE INDEX IF NOT EXISTS idx_workflow_pending_run
    ON workflow_pending (run_id);

CREATE TABLE IF NOT EXISTS workflow_terminal_events (
    terminal_digest TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    node_path TEXT,
    evidence_json TEXT,
    publication_state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_terminal_events_run
    ON workflow_terminal_events (run_id);
