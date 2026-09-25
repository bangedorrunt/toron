-- governed-by: ADR-0035
-- Run-state checkpoint storage (flywheel B2/B3, spec
-- docs/notes/2026-09-26-run-state-checkpoint-design.md).
--
-- WHY THIS EXISTS. A long-lived pure fold (`Msg -> tick(Model, Msg) ->
-- (Model, Vec<Cmd>)`) is trivially replayable in memory and completely
-- unreplayable once the process is gone, because nothing durable records the
-- state it passed through. Recovering "what was the state at step N" today
-- means grepping journals. This table is the durable projection that makes
-- replay and fork possible.
--
-- THE AUTHORITY RULE (spec S3, and the whole reason this is not a forbidden
-- "second store"). ADR-0035 lists second stores as a pattern to avoid. This
-- table is a store beside the model, so it is only legitimate under one rule:
-- the log is authoritative, the model is a pure function of the log, and no
-- code path may seed the model from any other source. A field whose ownership
-- is ambiguous between this table and a projection seeder is a defect, not a
-- detail — resolve it or refuse the design.
--
-- KEY SHAPE. The store orders by `seq`, the fold's own monotonic counter, NOT
-- by an id: `seq` does not depend on the wall clock, so a backwards clock step
-- can never make "latest" return an older boundary than one already handed out.
-- The `(thread_id, seq)` primary key serves every ordered read directly, so no
-- secondary ordering index is needed.
--
-- `checkpoint_id` is the durable evidence identity that stays unique across
-- threads and processes. It is FIXED-WIDTH lowercase hex, so it remains
-- lexicographically sortable for a caller that wants to sort across threads,
-- and `idx_run_state_checkpoint_id` makes it an O(1) equality lookup rather
-- than a per-lookup scan. It is built from (created_at_us, seq) rather than a
-- ULID crate to avoid a new dependency for an id we already fully control.
--
-- Times are int-microseconds (ADR G7); bounded evidence projections are TEXT.

CREATE TABLE IF NOT EXISTS run_state_checkpoints (
    thread_id     TEXT    NOT NULL,
    seq           INTEGER NOT NULL,
    checkpoint_id TEXT    NOT NULL,
    parent_checkpoint_id TEXT,
    model_json    TEXT    NOT NULL,
    msg_kind      TEXT,
    effect_keys   TEXT,
    created_at    INTEGER NOT NULL,
    PRIMARY KEY (thread_id, seq)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_run_state_checkpoint_id
    ON run_state_checkpoints (checkpoint_id);
