-- bd-n24w (ADR-0041, Phase 2): lookup of every envelope of a task by task id.
--
-- `inbox_delivery` already carries `task_id` (indexed, 0019). The lookup joins
-- it to the log by `msg_key`, so the log needs that key indexed too, or the
-- join scans the whole log once per delivery row. Additive and idempotent.
CREATE INDEX IF NOT EXISTS inbox_log_by_msg_key ON inbox_log (msg_key);
