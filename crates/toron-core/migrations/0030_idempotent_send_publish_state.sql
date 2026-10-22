-- bd-ff23: the idempotent-send claim row IS the durable command.
--
-- rumor_created_at: the rumor's created_at (unix seconds). It is part of the
-- event id, so a retry that re-publishes an unfinished claim rebuilds the
-- SAME event id -- the key IS the event id.
--
-- published: 1 once the publish completed. 0 means the claim exists but the
-- mail never left: the next retry re-publishes it instead of suppressing it
-- (the old behaviour lost the mail for the whole TTL).
--
-- Rows written before this migration keep rumor_created_at NULL: the retry
-- cannot rebuild their event, so they keep the old suppress behaviour.
ALTER TABLE idempotent_sends ADD COLUMN rumor_created_at INTEGER;
ALTER TABLE idempotent_sends ADD COLUMN published INTEGER NOT NULL DEFAULT 0;
