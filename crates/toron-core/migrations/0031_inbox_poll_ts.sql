-- governed-by: ADR-0003
-- bd-oracle-dead-source-72az: liveness poll timestamp, separate from the
-- delivery high-water. `last_seen_us` is the `--since last` delivery cursor
-- (rumor-time; bumping it to wall-clock would skip late-arriving messages
-- whose rumor timestamps predate the poll). `last_poll_us` records that a
-- read happened (wall-clock at the read edge) — the lease-liveness oracle's
-- activity signal. A poll proves the reader is alive even with an empty
-- inbox, which is exactly when `last_seen_us` never advances.

ALTER TABLE inbox_cursors ADD COLUMN last_poll_us INTEGER NOT NULL DEFAULT 0;
