-- bd-reserve-yield-taqf: durable stand-down markers. A pane the loop resumed
-- onto a bead whose lane a live peer holds used to answer with a comment
-- commit EVERY pass (dozens of commits for one fact). `toron reserve yield`
-- records that stand-down once, per (project, bead, holder); the loop reads
-- `suppressed_resume_beads` and skips the bead.
--
-- Deliberately NOT a bus projection: a yield is an agent assertion about who
-- owns a bead, not a lease transition, so there is no kind-30031 to replay
-- from. The table is the truth (same contract as `receipts`).
--
-- No delete-on-release: suppression is computed against the LIVE lease board,
-- so a released holder stops suppressing on its own, and the marker cannot
-- outlive its meaning by a stale row. A stale row is inert (it names a holder
-- nobody can resolve), which is why nothing prunes it.
CREATE TABLE IF NOT EXISTS reservation_yields (
    id           INTEGER PRIMARY KEY,
    project_slug TEXT    NOT NULL,
    bead_id      TEXT    NOT NULL,
    holder       TEXT    NOT NULL,
    yielder      TEXT    NOT NULL DEFAULT '',
    note         TEXT    NOT NULL DEFAULT '',
    created_ts_us INTEGER NOT NULL,
    UNIQUE (project_slug, bead_id, holder)
);
CREATE INDEX IF NOT EXISTS idx_reservation_yields_project
    ON reservation_yields (project_slug, bead_id);
