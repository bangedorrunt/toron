-- governed-by: ADR-0003
-- Shared pin → pane bindings. Flywheel's spawn/recover/rebind paths are the
-- writers; toron-core's whois/list_agents reads are the consumers. Keeping
-- this in the shared index removes toron-core's reverse dependency on
-- ~/.local/state/flywheel and gives concurrent spawns one uniqueness rule:
-- one pin per project, and at most one pin per non-empty pane.
CREATE TABLE IF NOT EXISTS agent_bindings (
    project_slug     TEXT    NOT NULL,
    pin              TEXT    NOT NULL,
    pane_id          TEXT    NOT NULL DEFAULT '',
    name             TEXT    NOT NULL DEFAULT '',
    profile          TEXT,
    harness          TEXT,
    model            TEXT,
    model_pin        TEXT,
    skills_digest    TEXT,
    binding_location TEXT    NOT NULL DEFAULT 'pane',
    harness_session  TEXT,
    incarnation      INTEGER NOT NULL DEFAULT 0,
    skills_injected  INTEGER NOT NULL DEFAULT 0,
    updated_ts_us    INTEGER NOT NULL,
    PRIMARY KEY (project_slug, pin)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_agent_bindings_pane
    ON agent_bindings (project_slug, pane_id)
    WHERE pane_id <> '';

CREATE INDEX IF NOT EXISTS idx_agent_bindings_project
    ON agent_bindings (project_slug, pin);
