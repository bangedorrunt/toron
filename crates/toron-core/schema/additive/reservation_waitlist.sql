-- bd-ll7bs: FIFO waiters for a held path. One row per (project, path, agent);
-- the auto-increment seq is the arrival order a handoff reads back.
CREATE TABLE IF NOT EXISTS reservation_waitlist (
  seq           INTEGER PRIMARY KEY AUTOINCREMENT,
  project_slug  TEXT NOT NULL,
  path_pattern  TEXT NOT NULL,
  agent_name    TEXT NOT NULL,
  reason        TEXT,
  created_us    INTEGER NOT NULL,
  UNIQUE (project_slug, path_pattern, agent_name)
);

CREATE INDEX IF NOT EXISTS reservation_waitlist_order
  ON reservation_waitlist (project_slug, path_pattern, seq);
