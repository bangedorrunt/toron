-- A read is not an ack. The SLA scan counted any message_reads row as an
-- acknowledgement, so reading a high-importance post silenced its reminder.
-- Reads stay in this table; the ack fact gets its own monotonic column.
ALTER TABLE message_reads ADD COLUMN acked INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_message_reads_acked
    ON message_reads (message_id, acked);
