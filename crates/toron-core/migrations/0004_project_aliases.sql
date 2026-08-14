-- governed-by: ADR-0017
-- Project-root canonicalization: alias table mapping any path-derived project
-- key to its canonical short slug (ADR-0017 D1/D3).
--
-- Written by mailbox::aliases — canonicalize() on path-form keys, and
-- reconcile_aliases() on daemon open (fold pre-existing path-slug roots).
-- Read by alias_read_set() for alias-expanded lookups. Append-only: rows are
-- never deleted, so legacy path-slug state stays addressable read-only and
-- the relation can be reversed by an operator (ADR-0017 D3 recovery path).

CREATE TABLE IF NOT EXISTS project_aliases (
    alias         TEXT PRIMARY KEY,
    canonical     TEXT NOT NULL,
    created_ts_us INTEGER NOT NULL
);
