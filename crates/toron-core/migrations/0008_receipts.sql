-- governed-by: ADR-0027
-- Receipts durable store (E2): assignment receipts keyed by corr, sqlite truth
-- cached in WorkspaceModel.assignment. Next to message_reads / inbox_cursors.
-- State: owed|acked|resulted; wake: standing|tasked; due_at: µs; result_path.

CREATE TABLE IF NOT EXISTS receipts (
    corr TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    wake TEXT,
    due_at INTEGER,
    result_path TEXT,
    updated_at INTEGER NOT NULL
);
