-- bd-epoch-claim-z5m5 (ADR-0041, Phase 2): the atomic epoch claim.
--
-- A producer's epoch used to be chosen by its caller (flywheel leased one from
-- a local file lock). Two processes on two hosts, or one host with a lost
-- lock file, could pick the same epoch and then share a (producer, epoch, seq)
-- triple. The claim moves that decision into the log: one row per producer
-- holding the highest epoch ever handed out, advanced only inside a
-- BEGIN IMMEDIATE transaction. Appends are fenced against it exactly like the
-- producer state's epoch, so a claimed epoch fences every older incarnation
-- before the new one has appended anything.
--
-- Additive: nothing reads this unless the claim API is called.
CREATE TABLE IF NOT EXISTS inbox_epoch_claim (
    producer      TEXT    PRIMARY KEY,
    epoch         INTEGER NOT NULL,
    holder        TEXT    NOT NULL,
    claimed_ts_us INTEGER NOT NULL
);
