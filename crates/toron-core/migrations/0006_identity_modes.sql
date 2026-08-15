-- governed-by: ADR-0018
-- Agent identity naming grammar and routing rules (ADR-0018 D3/D4).
--
-- project_identity_modes: the per-project routing policy. `swarm` enforces
-- canonical-name-or-npub routing on whois/send/register; `open` (default —
-- absent row) keeps legacy any-name routing (AM-Rust parity). Written by
-- ensure_project_with_mode / set_identity_mode; read by whois/send-path.
--
-- agent_name_aliases: one-time legacy reconciliation (ADR-0018 D4). A row
-- maps a legacy name (lane-1-bootstrap, DustySparrow-era, ...) to its
-- canonical identity when the pane binding is discoverable. whois/send
-- resolve the legacy name THROUGH the alias to the canonical npub; the
-- canonical name remains the routable identity. Append-only: rows are never
-- deleted, so the relation is always reversible.

CREATE TABLE IF NOT EXISTS project_identity_modes (
    project_slug    TEXT PRIMARY KEY,
    identity_mode   TEXT NOT NULL,
    updated_ts_us   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_name_aliases (
    project_slug    TEXT NOT NULL,
    alias           TEXT NOT NULL,
    canonical       TEXT NOT NULL,
    created_ts_us   INTEGER NOT NULL,
    PRIMARY KEY (project_slug, alias)
);
