-- ADR-0041 (stack-comms Phase 4.2): topology is a log fact.
--
-- A context's topology is a `context@1` Control envelope on the inbox log,
-- appended like any other. This table is its projection, one row per context:
-- the CURRENT topology, written in the same transaction as the append, so a
-- reader projects allowed verbs by key instead of folding the log. The log
-- row stays the record; this is derivable from it (highest `gen` per context).
--
-- `gen` is the fence: a fact is accepted only if its gen is above the row's.
-- A context with no row is ungoverned by a topology: `allowed_now` stays the
-- phase table. `supervisor` is stored like the others; the projection refuses
-- it (`NotYet`) until Phase 4a, and a send in that context is refused rather
-- than run as a swarm.
--
-- Additive: nothing reads this unless a context fact is appended or read.
CREATE TABLE IF NOT EXISTS inbox_context (
    context    TEXT    NOT NULL PRIMARY KEY,
    topology   TEXT    NOT NULL,
    gen        INTEGER NOT NULL,
    msg_key    TEXT    NOT NULL
);
