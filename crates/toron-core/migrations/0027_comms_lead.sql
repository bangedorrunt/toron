-- ADR-0041 (stack-comms Phase 4a.3): the lead lease is a log fact.
--
-- A `lead@1` Control envelope names the holder and the `sup_epoch` steers
-- must speak for. This table is its projection, one row per context, written
-- in the same transaction as the append. The log row stays the record.
--
-- A fact is stored only when its `sup_epoch` is strictly above the row's.
-- No row: a steer is not fenced by an epoch, because there is none to invent.
--
-- Additive: nothing reads this unless a lead fact is appended or a steer is
-- checked against it.
CREATE TABLE IF NOT EXISTS inbox_lead (
    context    TEXT    NOT NULL PRIMARY KEY,
    holder     TEXT    NOT NULL,
    sup_epoch  INTEGER NOT NULL,
    msg_key    TEXT    NOT NULL
);
