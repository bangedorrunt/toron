# ADR-0017: Project Root Canonicalization and Identity Resolution

**Status:** Accepted (2026-08-15)
**Related:** bd-cmgu (closed 2026-08-12 — FALSE CLOSURE), bd-f5tt (guard
slug-alias walk), ADR-0007 R2 (durable XDG root), ADR-0003 (project `#t`
scoping), toron.dev ADR-0002 (catalog contract), flywheel ADR-0015
(boundary guarantees)
**Spec:** `.flywheel/FIX-SPECS-2026-08-15.md` S2 (keystone lane)

## Context

`bd-cmgu` closed 2026-08-12 with a key-basename fallback: MCP clients pass
the short project key (`toron`) while the flywheel CLI passes the absolute
path slug (`/Users/bangedorrunt/workspace/toron`). The fix made key LOOKUPS
tolerate both spellings (`agent_key_path` trim + `alt_agent_key` walk +
`project_slug_aliases`), but the two spellings remained **two project roots**:

- separate agent key files (a second registration minted a second keypair),
- separate `agents` rows (`UNIQUE (project_slug, name)`),
- separate profile-event scoping tags (`#t = <slug-as-written>`),
- separate inbox/read/reservation attribution.

The symptoms reproduced **2026-08-14** — one day after `bd-cmgu` closed:
agents live in two roots with separate keys, registrations, contacts, and
inboxes; MCP-vs-CLI urgent divergence; inbox projection holes; whois misses;
impossible acks ("message not found"); lost done-mail routing. The
2026-08-14 repro evidence is the split-root state itself: the same logical
project registered under both spellings with distinct npubs per spelling.

The key-based fallback could not repair it: it tolerates alternate keys but
does not prevent their creation, and it cannot reconcile registration state,
profile scoping, or message attribution.

## Decision

### D1 — Canonical project key = the short slug; an alias table in the archive DB

The canonical project key is the **short slug** (no path separators:
`toron`). Any path-derived key is an alias of it. The mapping lives in a
persisted **alias table** in the archive DB:

```sql
CREATE TABLE project_aliases (
    alias         TEXT PRIMARY KEY,   -- e.g. "/Users/.../workspace/toron"
    canonical     TEXT NOT NULL,      -- e.g. "toron"
    created_ts_us INTEGER NOT NULL
);
```

The table is append-only: rows are never deleted, so the relation is always
reversible (a recovery path, see D3). `mailbox::aliases` is the module;
`Mailbox::canonical_project` / `Mailbox::project_read_set` /
`Mailbox::reconcile_project_aliases` are the entry helpers.

Derivation rule: a path-form key resolves to its **basename** (`/a/b/toron`
→ `toron`). First writer wins the mapping (insert-on-conflict), so two
path forms sharing a basename collapse deterministically onto one canonical
root. Without an index pool (demo/test roots) the pure basename derivation
stands in — identical result, just not durable.

### D2 — Normalize at EVERY entry point. One root per project.

Every operation that takes a project key resolves it through the canonical
root first:

- `ensure_project` — directory, kind-30010 manifest `d`-tag, scoping tags all
  use the canonical slug.
- `register_agent` / `create_agent_identity` / `persist_and_publish` — key
  file, profile event tags, agents-index row all canonical.
- `agent_key_path` — always the canonical (basename) path; every spelling
  reads and writes the ONE key file.
- `send_message` / `reply_message` / `fetch_inbox` / `acknowledge_message` /
  `mark_message_read` / `summarize_thread` — canonical root.
- `search_messages` — project filter expanded over the alias set.
- reservations / contacts / build-slots — canonical at entry.
- `MailSurface` calls — normalized transitively through the `Mailbox` choke
  point (the surface has no independent project state).
- MCP daemon resolution (`build_mailbox`) — `reconcile_aliases` on open.
- CLI — all robot commands and `toron serve` flow through `build_mailbox`
  and the same mailbox methods.

Result: a registration via any spelling upserts the **one** canonical row
(the `UNIQUE (project_slug, name)` row) and the **one** key file — no
duplicate agent rows can be created, so the split-root failure mode is
structurally impossible for new state.

### D3 — Migration: aliased read-only. No row movement. Recovery path.

Pre-ADR-0017 path-slug rows (agents, messages, read state, reservations)
are **not moved**: on open, `reconcile_aliases` walks the data dir and folds
every existing path-slug root (both spellings — relative and
leading-separator — mirroring `project_slug_aliases`) into the alias table.
All read paths then query the **alias set** (canonical ∪ every stored
alias ∪ the caller's own spelling), so legacy rows stay visible from every
spelling.

**Why no merge:** folding rows would require resolving same-name/different-
npub conflicts between the two roots — an unsafe merge with no honest winner.
Aliased read-only makes merge-safety a non-question: nothing moves, nothing
is destroyed, and legacy state degrades gracefully to read-only.

Key adoption: `register_agent` falls back to the alternate-root key walk
(`alt_agent_key`, now reading verbatim legacy paths) when the canonical key
file is absent — re-registering under the canonical slug **adopts** a legacy
identity (same npub) instead of minting a second one.

**Recovery path** (operator): the alias relation is append-only and
reversible — promote any alias to canonical by re-registering the identity
under the canonical slug (key adoption makes it seamless), or clear the
alias row to return a spelling to its own root. Legacy rows remain in the
archive and the Git archive forever; nothing is deleted by this ADR.

### D4 — Ack authorization: recipient, pane-bound identity, or thread participant

`acknowledge_message` / `mark_message_read` may be performed by:

- **(a)** the addressed recipient identity — the acker's own keys unwrap the
  wrap (the historical happy path);
- **(b)** any identity bound to the same herdr pane as the recipient
  (`resolve_pane_identity`; best-effort — no herdr backend wired → the check
  degrades to (c) only);
- **(c)** any thread participant — the message's sender, or any identity able
  to unwrap a wrap in the same thread (the thread root counts: a head message
  is its own thread, so holding the root is membership).

**Identity-drift repair:** when the acker's keys cannot unwrap the message
(its historical "message not found" failure), the actual recipient is
discovered across the project's agents (alias-set expanded) before the
authorization gate runs. The ack then proceeds with the acker's own keys to
the original sender. Authorization failures are REFUSED with a clear error —
the gate is never silently skipped.

## Consequences

- **The split-root failure mode is structurally impossible for new state**:
  one canonical root per project, one key file per identity, one registration
  row per name. The 2026-08-14 repro cannot re-form.
- **Legacy split-root state degrades to read-only visibility** through the
  alias relation; adoption on re-registration heals it without data movement.
- `project_slug_aliases` / `alt_agent_key` remain as the filesystem-level
  read fallbacks for legacy roots; the DB alias table is the primary relation.
- Ack authorization widens from recipient-only to pane/thread participants —
  a deliberate trust widening for swarm identity drift, gated on herdr
  binding or demonstrable thread membership.

## Verification

```bash
cargo test -p toron-core --lib          # aliases unit tests + ack authz tests
cargo test -p toron-e2e --test sc25_canonical_roots   # acceptance: two roots, one project
bash scripts/verify-gates.sh
cargo test --workspace
```

Acceptance (SC-25): a message sent addressing an agent via one root is
visible + ackable via the other; registration state spans both (same npub);
no duplicate agent rows; `ensure_project` via either spelling returns the
same canonical slug; the legacy-path migration test folds a hand-built
pre-ADR-0017 root and heals it read-only.
