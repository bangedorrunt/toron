-- governed-by: ADR-0033
-- Judgment-effect journal (bd-ncpt): one row per effect_key (idempotent —
-- re-asks replay the stored row). Answers + full effect as JSON; times are
-- int-microseconds (ADR G7). Same SQLite boundary as workflow_effects; the
-- mailbox `receipts` table stays message-lifecycle only.

CREATE TABLE IF NOT EXISTS judgment_effects (
    effect_key TEXT PRIMARY KEY,
    project TEXT NOT NULL,
    pack_id TEXT NOT NULL,
    pack_version INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    confidence REAL NOT NULL,
    escalated INTEGER NOT NULL,
    fallback_flag INTEGER NOT NULL,
    answers_json TEXT NOT NULL,
    effect_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
