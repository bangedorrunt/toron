-- ADR-0041: the pending-urgent point read (`InboxStore::pending_urgent`).
--
-- A partial index over the inbox rows that are urgent steers, keyed like
-- `inbox_log_by_recipient`, so "the recipient's oldest unread urgent row" is
-- one seek however many normal rows are ahead of it. The WHERE clause is the
-- urgency rule (`inbox::urgency_of`: `steer@1` with `priority = "urgent"`)
-- over the stored envelope, written exactly as `inbox::URGENT_WHERE` so the
-- planner matches it; a test holds the two texts equal.
--
-- Additive: an index only. No row is rewritten; building it reads the log
-- once. Nothing reads it except that query.
CREATE INDEX IF NOT EXISTS inbox_log_urgent ON inbox_log (stream, recipient, log_offset)
    WHERE json_extract(payload, '$.contract.name') = 'steer' AND json_extract(payload, '$.contract.major') = 1 AND json_extract(payload, '$.body.priority') = 'urgent';
