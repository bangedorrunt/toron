-- ADR-0041 (stack-comms Phase 4.1): roles and authority at append.
--
-- A role is a log fact: a `role@1` Control envelope on the inbox log
-- (`inbox_log`), appended like any other. This table is its projection, one
-- row per (context, npub): the CURRENT role, written in the same transaction
-- as the append that carried the fact, so the authority check at append reads
-- it by key instead of folding the log. The log row stays the record; this is
-- derivable from it (`role@1` envelopes in `inbox_log`, highest `gen` per
-- (context, npub)).
--
-- `gen` is the fence: a role fact is accepted only if its gen is above the
-- row's, so a stale issuer cannot overwrite a newer grant.
--
-- A context with no row here is ungoverned: no authority check applies, which
-- is today's behaviour. The first row makes it governed.
--
-- Additive: nothing reads this unless the comms flag is on and a role fact is
-- appended or read.
CREATE TABLE IF NOT EXISTS inbox_role (
    context    TEXT    NOT NULL,
    npub       TEXT    NOT NULL,
    role       TEXT    NOT NULL,
    gen        INTEGER NOT NULL,
    msg_key    TEXT    NOT NULL,
    issuer     TEXT    NOT NULL,
    set_ts_us  INTEGER NOT NULL,
    PRIMARY KEY (context, npub)
);
