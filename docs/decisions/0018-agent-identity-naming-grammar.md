# ADR-0018: Agent Identity Naming Grammar and Routing Rules

**Status:** Accepted (2026-08-15)
**Related:** ADR-0017 (project-root canonicalization), ADR-0003 (identity
cluster G1/G2/G9), flywheel S8.1/S8.3 (wake identity + spawn-side canonical
registration), flywheel `agent_name_n` (the name builder this ADR mirrors),
toron.dev ADR-0002 (catalog contract)
**Spec:** `.flywheel/FIX-SPECS-2026-08-15.md` S8 + lane-2 ADR bead
`bd-adr-naming-grammar-h8nj`

## Context

The naming triangle has two of three corners:

1. **ADR-0017** landed project-root canonicalization: one canonical root per
   project, path-form keys alias into the canonical short slug.
2. **flywheel S8.3** landed spawn-side canonical registration: `spawn` binds a
   mail identity per pane as the **canonical herdr name** — `flywheel-<project>-
   <role><N>` — writes it to `.flywheel/agent-names.json`, and registers that
   exact name idempotently before instructing fetch.

The third corner — the **naming grammar and routing rules on the toron side** —
was never formalized. Consequences of the gap:

- `whois` and the send-path (`send_message`/`reply_message` recipient
  resolution) accept ANY registered name: `lane-1-bootstrap`,
  `lane-4-catalog`, `assistant`, DustySparrow-era adjective+noun identities
  (`DustySparrow`, `RoseDune`, `YellowCastle`, `BeigeFern`, `CalmPeak`, ...),
  bare `pi`/`oc`, and the current canonical swarm names all resolve equally.
  Routing has no notion of what an identity IS.
- The pane→identity authority is implicit: flywheel pins `pane_id → name →
  mail_identity` in `.flywheel/agent-names.json` (project-local mirror) and
  the spawn registry (`~/.local/state/flywheel/agents/<project>.json`), but
  toron never formalizes that file as the authority or consumes it.
- The 2026-08-14 swarm state (see ADR-0017) proves what ungoverned naming
  produces: overlapping identities, stale bindings, impossible acks, and
  senders that cannot be addressed.

This ADR completes the triangle: **the canonical name is the routable
identity; everything else is metadata.**

## Decision

### D1 — Grammar: `flywheel-<project-slug>-<role><seq>` is THE single routable identity per pane

The canonical agent name is:

```
flywheel-<project-slug>-<role><seq>
```

- **`flywheel-`** — fixed prefix.
- **`<project-slug>`** — the canonical short slug of the project (ADR-0017
  D1: `toron`, never a path form).
- **`<role>`** — the lowercase role short (`oc`, `pi`, `gk`, `coder`, ...).
- **`<seq>`** — 1-based ordinal per (project, role). **Seq 1 is suffix-less**
  (`flywheel-toron-oc`); seq ≥ 2 appends digits (`flywheel-toron-oc2`,
  `-oc3`). This mirrors flywheel's `agent_name_n` exactly, so both sides of
  the boundary build and parse the SAME names.

One pane has exactly ONE canonical identity: the name its agent-names.json
entry pins (S8.3 self-bound). The grammar is project-scoped: a name embeds
its project slug, so `flywheel-toron-oc2` is canonical only in project
`toron`.

Pure functions (new `mailbox/naming.rs` module, fully unit-tested):

- `parse_canonical_name(name, project_slug) -> Option<(role, seq)>`
- `canonical_name(project_slug, role, seq) -> String`
- `is_canonical_name(name, project_slug) -> bool`
- `is_npub(s) -> bool` (64-hex routing address)

### D1a — Amendment (2026-08-15, captain): the routable identity is a registered adjective+noun pin

The captain prefers **adjective+noun identities** (`DustySparrow`,
`RoseDune` — the legacy spawn binding style) as the human-facing, routable
mail identity. This amends D1/D3:

1. **The pinned adjective+noun name is the canonical routable identity** per
   pane: `agent-names.json` `mail_identity` = the adj+noun pin, **registered
   idempotently at spawn** (the S8.1 pattern — registration, not randomness,
   was what fixed routing) and unique per project (spawn registry rejects a
   collision with a fresh draw).
2. **The `flywheel-<slug>-<role><seq>` name is the technical pane handle** —
   the herdr name, still resolved by `settle`/`send`/whois as an alias
   through the `agent-names.json` binding, but NOT the primary mail identity.
3. **D3 display-name demotion is reversed for pins**: registered adj+noun
   pins ARE routing targets. Unregistered free strings remain rejected.
4. `register_spawned_agent` (S8.3) is reversed to register the adj+noun pin;
   the slug-name registration it introduced is retired. Existing
   slug-registered identities stay as aliases (history), not re-registered.
5. `flywheel principles` / briefs address crews by their pin; humans steer
   by pin. `whois <pin>` resolves.

Rationale (captain, 2026-08-15): slug names read like machinery; adj+noun
names read like people, and the workspace/label convention already uses
them. The routing fixes stand — they came from REGISTRATION and PINNING, not
from the slug grammar.

### D2 — `agent-names.json` is the pane-to-identity authority

`.flywheel/agent-names.json` (project-local, survives state-dir loss) is the
authoritative pane→identity mapping, mirroring the spawn registry in the
flywheel state dir. Each entry is `{pane_id, name, mail_identity}` where
`name` is the canonical herdr name and `mail_identity` is the registered
toron identity (S8.3: the canonical name itself).

- Flywheel WRITES the authority at spawn; toron CONSUMES it.
- toron's live pane view is the herdr backend (ADR-0003 G9
  `resolve_pane_identity`); the JSON file is the durable authority that
  survives herdr session restarts (flywheel S8.1 wake pin).
- The one-time reconciliation (D4) reads the authority's bindings and folds
  legacy identities into canonical names **where discoverable**.

### D3 — Routing by canonical name or npub ONLY, per-project `identity_mode`

Routing — whois and send-path recipient resolution — accepts exactly:

1. a canonical name for the project (D1 grammar), or
2. an npub (64-hex).

**Display names are demoted to non-routable profile metadata.** Legacy
registered names (`lane-1-bootstrap`, `lane-4-catalog`, `assistant`,
DustySparrow-era adjective+noun) remain registered (history, `list_agents`,
archive attribution preserved) but are NOT routing targets.

Enforcement is **per-project** through a persisted `identity_mode`:

| Mode | whois / send-path recipients | register / create identity |
| :--- | :--- | :--- |
| `swarm` | canonical name or npub ONLY; non-canonical → actionable error naming the grammar | non-canonical names REJECTED with the same error |
| `open` (default) | any registered name (legacy AM-Rust parity) | any name (legacy parity) |

- Persisted in the archive DB (`project_identity_modes` table, migration
  0006) — written by `ensure_project` (MCP arg `identity_mode` already exists
  on the frozen surface; it was accepted and ignored, now wired) and read by
  whois/send/register. No index pool → degrade to `open` (same posture as
  `message_reads` / `project_aliases`).
- The live `toron` project is set to `swarm` by the one-time reconciliation
  (D4). The `demo`/fixture projects and the AM-Rust parity surface stay
  `open` (documented divergence row in `docs/parity/known-divergences.md`
  updated).
- **Sender resolution is unchanged**: the sender is the acting identity whose
  key file is loaded (key possession = authority, ADR-0003 G1). Hardening
  targets ADDRESSING (recipients + whois), never key-holder reads/sends — a
  legacy key holder can still fetch and send, but cannot be addressed by name.
- whois additionally accepts an npub (reverse lookup to the registered name),
  the read-side twin of npub routing.

### D4 — Legacy reconciliation policy (one-time)

Existing non-canonical identities fold or demote:

- **Fold where discoverable**: an `agent_name_aliases` row (project_slug,
  alias, canonical) maps a legacy name to the canonical identity when the
  pane binding is discoverable (agent-names.json entry whose `mail_identity`
  is the legacy name and whose `name` is canonical). whois/send resolve the
  legacy name THROUGH the alias to the canonical npub (read-path preserved;
  the canonical name remains the routable identity).
- **Demote where not discoverable**: legacy names with no discoverable pane
  binding stay registered (history, display) but are non-routable in `swarm`
  mode — whois/send fail with an actionable error naming the grammar.
- Nothing is deleted; the archive, keys, and message attribution are
  untouched (same posture as ADR-0017 D3).

The one-time reconciliation for the current toron swarm:
- The live authority (`toron.json` spawn registry + project-local
  `agent-names.json`) binds `w7W:p1..p4 → flywheel-toron-oc..oc4`, all
  self-bound (mail_identity == name). S8.3 already made the live swarm
  canonical — no folds are discoverable.
- `lane-1-bootstrap`, `lane-4-catalog`, `assistant`, and the DustySparrow-era
  identities have no pane binding in the current authority → **demote**.
- The project is set to `identity_mode = swarm`; routing hardens immediately.

## Consequences

- The naming triangle is closed: ADR-0017 (one root), S8.3 (canonical
  registration at spawn), ADR-0018 (canonical routing + authority).
- New non-canonical identities cannot be created in swarm projects
  (registration rejects); legacy identities cannot be addressed (whois/send
  reject) while their history stays readable.
- `open` projects keep the pre-ADR behavior — demo fixtures and the AM-Rust
  parity surface are untouched.
- The `flywheel` project's own swarm (CopperSparrow/EmeraldOtter pins in its
  agent-names.json) remains `open` until its own reconciliation run; the
  mechanism is identical.

## Implementation Plan

- **Affected paths**:
  - `crates/toron-core/migrations/0006_identity_modes.sql` (new —
    `project_identity_modes` + `agent_name_aliases`).
  - `crates/toron-core/src/mailbox/naming.rs` (new — pure grammar + npub).
  - `crates/toron-core/src/mailbox/identity.rs` — whois hardening + npub
    lookup + alias read-through; register/create rejection in swarm mode.
  - `crates/toron-core/src/mailbox/messaging.rs` — `resolve_recipients`
    hardening (canonical/npub/alias only in swarm mode).
  - `crates/toron-core/src/mailbox/infra.rs` — `ensure_project` persists
    `identity_mode` (new `ensure_project_with_mode`; the MCP tool wires the
    existing arg through).
  - `crates/toron-mcp/src/tools/system.rs` — pass `identity_mode` to
    `ensure_project_with_mode`.
  - `crates/toron-e2e/tests/sc26_canonical_naming.rs` (new, un-gated).
  - `docs/parity/known-divergences.md` — naming-policy row updated.
- **Pattern**: pure functional core (`naming.rs`) + Mailbox methods; mirror
  the `project_aliases` module's DB helpers for the two new tables.
- **Tests**: naming.rs unit tests (grammar parse/build/npub); identity.rs
  whois swarm/open mode + npub + alias fold; messaging.rs recipient
  hardening; infra.rs mode persistence; e2e sc26 acceptance.
- **TDD**: tests first (RED), then implementation (GREEN), per repo policy.

## Verification

```bash
cargo test -p toron-core --lib            # naming + whois + send-path + mode tests
cargo test -p toron-e2e --test sc26_canonical_naming   # acceptance
bash scripts/verify-gates.sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Acceptance (sc26):
- swarm project: whois + send by canonical name and npub work; non-canonical
  names fail with errors naming the grammar; registration of a non-canonical
  name is rejected; a folded alias resolves through to the canonical npub.
- open project: legacy names still whois/send (parity preserved).
- live reconciliation: `toron` project reports `identity_mode = swarm`;
  `whois(lane-1-bootstrap)` fails with the grammar error while
  `whois(flywheel-toron-oc2)` resolves; `list_agents` still shows the legacy
  identities.
