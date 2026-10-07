-- bd-ejp4: at most one ack-sla-expired wake per (post, recipient set) per SLA
-- window.
--
-- `check_ack_sla_expired` re-emits a wake for every overdue ack-required post
-- it cannot prove handled, and it had no record that it had already done so.
-- Measured on a live store: 98,539 kind-9 wake events, of which only 129 were
-- DISTINCT posts — each re-woken about 771 times over 13 hours. The bus hit its
-- 100,000-row cap and cap prune deleted the kind-1059 gift wraps (DURABLE_KINDS
-- exempts 39000/39002/30010/30013/30031, not mail), leaving 3 wraps against 544
-- indexed flywheel mail rows. The TUI inbox was empty as a downstream effect.
--
-- An in-process guard would not have held: the flood survived a daemon restart.
-- The record has to be durable, and it has to be keyed on the AUDIENCE, because
-- the scan's own contract is per-recipient — one reader's ack must not silence
-- the wake for a co-recipient who has not read it. Keying on (post, audience
-- hash) keeps that: a genuinely new co-recipient produces a different key and
-- still gets woken.
--
-- `emitted_us` is the last announcement time, and the re-wake is deliberately
-- NOT a permanent mute: a post whose readers still have not acted is genuinely
-- overdue, so it is woken again once the SLA window lapses. A permanent mute
-- would have traded the flood for a silent drop, which is the same class of
-- defect in the other direction. The write refreshes `emitted_us` on conflict
-- so a re-woken post does not immediately re-wake on the next scan.
CREATE TABLE IF NOT EXISTS ack_sla_wakes (
    post_id    TEXT NOT NULL,
    audience   TEXT NOT NULL,
    emitted_us INTEGER NOT NULL,
    PRIMARY KEY (post_id, audience)
);

-- The scan's read is "has this pair been emitted", the write is the insert.
CREATE INDEX IF NOT EXISTS idx_ack_sla_wakes_emitted
    ON ack_sla_wakes (post_id, emitted_us);
