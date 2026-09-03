-- governed-by: ADR-0028
-- bd-adr-0028-implementation-zfnx.12: corr-keyed idempotent send.
-- Same (project, sender, corr) within TTL converges on one rumor: the
-- send claims (project, sender, corr) -> event_id BEFORE the bus publish
-- (INSERT OR IGNORE), so a crash between publish and receipt upsert — or
-- an executor-level retry of Publish — returns the existing event id
-- instead of emitting a second rumor. TTL-bounded (24h) + pruned
-- best-effort on the send path; an index rebuild resets the table
-- (one-time re-send risk on rebuild, not data loss).

CREATE TABLE IF NOT EXISTS idempotent_sends (
    send_key TEXT NOT NULL PRIMARY KEY,
    event_id TEXT NOT NULL,
    project_slug TEXT NOT NULL,
    sender_name TEXT NOT NULL,
    corr TEXT NOT NULL,
    recipients_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotent_sends_created
    ON idempotent_sends (created_at);
