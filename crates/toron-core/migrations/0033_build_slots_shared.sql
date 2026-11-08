-- Shared build slots are multi-holder by definition. The original UNIQUE
-- (project_slug, slot) forced a second shared acquirer to overwrite the first
-- holder's row. Rebuild the table with holder-scoped identity; exclusive
-- acquisition is still serialized by the NOT EXISTS check in try_acquire.
CREATE TABLE build_slots_new (
    id            INTEGER PRIMARY KEY,
    project_slug  TEXT    NOT NULL,
    slot          TEXT    NOT NULL,
    agent_name    TEXT    NOT NULL DEFAULT '',
    agent_pubkey  TEXT    NOT NULL DEFAULT '',
    exclusive     INTEGER NOT NULL DEFAULT 0,
    status        TEXT    NOT NULL DEFAULT 'active',
    event_id      TEXT,
    expires_ts_us INTEGER,
    created_ts_us INTEGER NOT NULL
);

INSERT INTO build_slots_new
    (id, project_slug, slot, agent_name, agent_pubkey, exclusive, status,
     event_id, expires_ts_us, created_ts_us)
SELECT id, project_slug, slot, agent_name, agent_pubkey, exclusive, status,
       event_id, expires_ts_us, created_ts_us
FROM build_slots;

DROP TABLE build_slots;
ALTER TABLE build_slots_new RENAME TO build_slots;

CREATE UNIQUE INDEX IF NOT EXISTS ux_build_slots_holder
    ON build_slots (project_slug, slot, agent_pubkey);
CREATE INDEX IF NOT EXISTS idx_build_slots_project_status
    ON build_slots (project_slug, status);
