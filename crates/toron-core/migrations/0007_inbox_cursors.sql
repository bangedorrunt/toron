-- governed-by: ADR-0003
-- bd-since-cursor-70cf: daemon-side inbox high-water cursor, one row per
-- (project, agent, plane). Plane-P pull discipline previously stored
-- last_seen in model memory only — a crashed/re-spawned agent lost the
-- cursor and had to re-read history or guess. `toron mail inbox --since
-- last` reads this cursor then atomically bumps it (UPSERT with MAX), so
-- the cursor survives crashes and never rewinds or skips.
--
-- The cursor is PROJECTION state (same class as message_reads, bd-5icg):
-- never carried on events; an index rebuild resets it to 0 (a one-time
-- re-read of history, not data loss).

CREATE TABLE IF NOT EXISTS inbox_cursors (
    project_slug TEXT    NOT NULL,
    agent_name   TEXT    NOT NULL,
    plane        TEXT    NOT NULL,
    last_seen_us INTEGER NOT NULL,
    PRIMARY KEY (project_slug, agent_name, plane)
);
