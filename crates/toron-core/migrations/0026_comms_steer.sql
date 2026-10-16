-- ADR-0041 (stack-comms Phase 4a.1): a steer is a log fact.
--
-- A `steer@1` Control envelope is appended like any other. This table is its
-- projection, written in the same transaction, so a later `done` can see that
-- a redirect fenced the generation it names without folding the log. The log
-- row stays the record.
--
-- One message, one row (`msg_key`). A new steer for the same (context, task)
-- marks the pending row `superseded` and inserts itself as `pending`. `state`
-- is `pending`, `acked`, or `superseded`. `gen` is the task generation the
-- steer was issued at (the envelope's `gen`), not a second counter.
--
-- Additive: nothing reads this unless a steer is appended.
CREATE TABLE IF NOT EXISTS inbox_steer (
    msg_key    TEXT    NOT NULL PRIMARY KEY,
    context    TEXT    NOT NULL,
    task       TEXT    NOT NULL,
    gen        INTEGER NOT NULL,
    op         TEXT    NOT NULL,
    priority   TEXT    NOT NULL,
    sup_epoch  INTEGER NOT NULL,
    state      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS inbox_steer_by_task
    ON inbox_steer (context, task, state);
