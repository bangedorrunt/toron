-- bd-aqvg: durable session delivery cursors for toron-workflow
-- DeliveryCursorStore snapshots. One row per session; delivered ids ride as
-- JSON (BTreeSet order = stable bytes, diffable). updated_us drives the idle
-- prune: a pruned session re-delivers on next sighting (a bounded duplicate,
-- chosen over unbounded table growth). in_flight never persists — reservations
-- unconfirmed at crash are retryable, and restoring them would park ids that
-- nobody owns.
CREATE TABLE IF NOT EXISTS delivery_cursors (
    session TEXT PRIMARY KEY,
    delivered_json TEXT NOT NULL DEFAULT '[]',
    cursor INTEGER NOT NULL DEFAULT 0,
    updated_us INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_delivery_cursors_updated
    ON delivery_cursors (updated_us);
