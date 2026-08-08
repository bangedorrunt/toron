---
status: accepted
date: 2026-08-08
accepted: 2026-08-08
revision: 2
decision-makers: bangedorrunt (captain)
consulted: architect agent (multi-pass review of ADR-0003/0004/0005 implementation)
informed: ""
supersedes: []
extends: [ADR-0003, ADR-0004, ADR-0005]
related: [flywheel, herdr, superlogical, mcp_agent_mail_rust, nostr, bd-review-hardening-mail-backend-c3b]
---

# ADR-0007: Post-Implementation Hardening, Bug Ledger, and Backend/API Redesign for Seamless Flywheel Integration

> **Self-contained analysis ADR.** Captains ship ADR-0003 (Nostr mailbox fabric),
> ADR-0004 (operational hardening), and ADR-0005 (orchestration tier) as
> **spec-complete** (beads closed, phase tables green). This ADR records a
> **de novo, full-scope, adversarial review** of the *actual tree* (toron +
> flywheel) against those contracts: material bugs, incomplete production
> wiring, impedance mismatches that block "Nostr as superset of agent-mail,"
> and a concrete redesign of the backend/API seam so herdr (default
> multiplexer) + Nostr mail (default) integrate cleanly with flywheel/workflow,
> with agent-mail as fallback and superlogical as stub.
>
> **Status: ACCEPTED (Revision 2, 2026-08-08).** Captain locked D1–D8 answers
> (see §Captain Decisions). Implementation proceeds via R0–R5 beads in **toron**
> and adapted children under flywheel epic
> `bd-review-hardening-mail-backend-c3b` (+ new ADR-0007 children).
>
> **Scope of audit (2026-08-08):**
> - `toron` workspace: `toron-core`, `toron-workflow`, `toron-mcp`, `toron-cli`, `toron-e2e`
> - `flywheel` (sibling): `src/backend/*`, `mail_shim.rs`, `toron_embed.rs`,
>   `workflow_catalog.rs`, `Cargo.toml` features
> - ADRs 0003–0005 + parity catalogs + `scripts/verify-gates.sh`
> - Methods: multi-pass bug hunting, adversarial soundness grading, Rust
>   async/API design review, ADR verification cross-check (not grepping
>   "Complete" in the index)

---

## Context and Problem Statement

### What the ADRs claim is done

| ADR | Claim (README / phase tables) | What the tree actually shows |
|-----|-------------------------------|------------------------------|
| **0003** | AM-Rust 38 tools / 25 resources; Nostr bus; NIP-17; Backend trait; flywheel embeds toron | Tools present (`#[tool(` × ~39); resources templates exist but **data projection mostly stub**; NIP-17 send/unwrap real; Backend trait **unified sync** in toron-core; flywheel has real `toron-core`/`toron-mcp`/`toron-workflow` cargo edges |
| **0004** | 5 ships: coalesce, loop-prevention, doctor reconstruct, guard bound, overseer | Library primitives + tests exist; **production stamp of `toron:workflow-emitted` never applied on send path**; coalescer flush in host **does not emit jobs**; overseer path in unwrap is real |
| **0005** | P0–P6 complete; one engine; failure path; profiles; smart cron; DAG | Engine large + many unit tests; **`call_workflow` is a structural stub**; **`foreach` never injects `{{item}}`**; MessagePosted trigger **cannot fire on NIP-17 mail**; MailBackend still flywheel-local AM-shaped (`u64` ids) |

### The captain's integration requirements (this review's north star)

1. **Multiplexer backend:** herdr default; superlogical future (stub only now); tmux hold.
2. **Mail backend:** Nostr/toron default; agent-mail **fallback** (must remain compiled).
3. **Nostr is a superset of agent-mail:** every AM behavior agents rely on must
   work; Nostr adds crypto identity, E2E, federation, real-time push.
4. **Implementation must follow Nostr protocol/policy** (NIP-01/10/14/17/40/44/59,
   frozen kind registry, projection rules) — not "AM over Nostr cosplay."
5. **Flywheel/workflow seamless integration** — one vocabulary, one engine,
   zero-IPC integrated mode, durable across leaf subprocesses where the loop
   demands it.

### Problem statement

> Given ADR-0003–0005 are marked implementation-complete, what **material
> defects, incomplete seams, and API design mistakes** prevent toron+flywheel
> from meeting the captain's backend defaults (herdr + Nostr mail) as a true
> AM-Rust **superset**, and what **redesign** (if any) is required so
> flywheel/workflow integration is seamless without inventing a third mail model?

---

## Decision Drivers

1. **Nostr protocol correctness over AM shape cosplay.** Wire events, kinds,
   gift-wrap privacy, addressable replaceables, and projection rules are load-bearing.
2. **Superset, not fork.** Agent-facing ops (send/reply/inbox/ack/reserve/search)
   must work on toron with the same *intents*; response shapes may differ
   (catalogued in `docs/parity/known-divergences.md`) but **must not be unusable**.
3. **Integrated mode primary** (ADR-0003 §3A). Flywheel leaf subprocesses and
   the durable loop must not each invent a fresh empty mailbox.
4. **One-way deps:** `flywheel → toron-workflow → toron-core`. No reverse.
5. **Frozen surfaces:** kind registry; 38 MCP tools (additive optional args OK).
6. **Failure observability.** Stubs that return `{ok:false}` are honest; stubs
   that **silently no-op** or **claim success while doing nothing** are defects.
7. **Simplification.** Prefer fewer traits, one message-id type, one error
   taxonomy, one durable mailbox root per project — not more adapters.

---

## Implementation Inventory (what is real)

### Strengths (keep)

| Area | Evidence | Grade |
|------|----------|-------|
| Frozen kind registry | `toron-core/src/nostr/kinds.rs` + completeness tests | Solid |
| NIP-17 send / BCC variants / unwrap / G7 ordering | `mailbox/messaging.rs`, `unwrap.rs` | Solid core |
| Overseer bypass (key-scoped) | `is_overseer_bypass`, operator signing | Solid |
| Addressable force-release via revocation | ADR-0003 A1 pattern in reservations | Present |
| Backend trait unification (mux) | `toron_core::backend::Backend` sync/`&self`/dyn; flywheel re-export as `MultiplexerBackend` | Done for **mux only** |
| Herdr client single source | flywheel `herdr_shim` → toron-core | Done |
| Superlogical stub honesty | Unsupported, no silent herdr fallback | Correct stub |
| Workflow engine Resonate substrate | `WorkflowEngine`, ApprovalSink/SpawnSink TypeId fix | Real |
| Failure path P0 | `on_error: fail_run`, value-returning runner | Real |
| Loop-prevention **predicate** | `should_evaluate_trigger_event` pure + tested | Solid library |
| Gate script | `scripts/verify-gates.sh` (no asupersync/fastmcp, forbid unsafe) | Green path |
| Parity catalogs | `docs/parity/*` intentional divergences | Honest docs |
| `flywheel → toron-workflow` cargo edge | flywheel `Cargo.toml` | Exists (ADR-0005 P3 gap closed) |
| Default selection | `FLYWHEEL_MAIL=toron`, `FLYWHEEL_BACKEND=herdr` | Correct defaults |

### Incomplete / hollow (not "done")

| Claim | Reality | Severity |
|-------|---------|----------|
| "workflow-emitted stamp on action outputs" | `stamp_workflow_emitted` exists; **no production call site** outside unit tests (rg: only engine.rs + lib re-export) | HIGH |
| "coalescer flushes to one job" | Host `tick` discards flush; comment: "real job emission is a P4 subsystem" | HIGH |
| "message_posted triggers on mail" | Bus publishes **kind-1059** only; filter **rejects 1059**; `matches_trigger` wants 14/9/1 → **DM mail never triggers workflows** | CRITICAL |
| `call_workflow` | Returns `{ok:false, error: "…lands in the P5.3 wiring step"}` forever | HIGH |
| `foreach` `{{item}}` | Comment claims injection; body does `let _ = item` | HIGH (silent wrong) |
| MCP resources (25) | Templates + static stubs; data resources `"not yet wired"` | MEDIUM |
| Config layer | `toron-core/src/config/mod.rs` is a TODO stub | MEDIUM |
| MailBackend in toron-core | Explicitly deferred; flywheel keeps AM-shaped trait | HIGH for integration |
| Durable integrated mailbox | `mail_shim` uses **temp per-pid SQLite**; leaf subprocesses each get empty state | CRITICAL for loop |
| ActionRunner async bridge | `Handle::try_current().block_on(send_message)` inside sync runner | MEDIUM (deadlock hazard) |
| React / CallWebhook in host runner | Not wired | LOW (honest Null path in match arms is incomplete) |

---

## Material Findings (bug ledger)

Every finding: id, defect, evidence, remedy, confidence, minimum test, posture.

### F1 — CRITICAL: `message_posted` cannot fire on NIP-17 mail

- **recommended_change:** Introduce a **projection trigger bus** (or re-publish
  unwrapped rumors as *internal* non-wire trigger envelopes) so
  `message_posted` sees kind-14 rumor content **after** unwrap, with
  `toron:workflow-emitted` stamps on workflow-originated rumors.
- **evidence_of_defect:**
  - `messaging.rs` publishes gift wraps only (`Kind::GiftWrap`).
  - `trigger_filter.rs` **rejects** kind 1059 (correct for loop prevention).
  - `workflow_host.rs` `matches_trigger(MessagePosted)` requires kind 14/9/1.
  - Docs in `trigger_filter` admit: "unwrapped rumor kind-14 … is not a
    workflow-trigger path" — i.e. the product path is undefined.
- **evidence_of_remedy:** Channel messages (kind-9) can still trigger; DMs
  never will until a post-unwrap ingress exists.
- **confidence:** High (code-path closed by construction).
- **minimum_acceptable_fix:** e2e: agent A sends NIP-17 DM → workflow with
  `trigger: message_posted` runs exactly once; workflow's own `send_message`
  does **not** re-trigger.
- **do_not_broaden_into:** Do not put plaintext kind-14 on public relays.
- **remediation_posture:** Structural (narrow fix insufficient: filter+matcher
  contradict NIP-17 wire).

### F2 — CRITICAL: flywheel in-process mailbox is per-process ephemeral

- **recommended_change:** Durable project-scoped mailbox root
  (`$XDG_DATA_HOME/flywheel/mail/<project>/` or config path), shared by all
  leaves of one host; operator key stable; document remote-daemon mode for
  multi-host.
- **evidence_of_defect:** `mail_shim.rs` `build_mailbox`:
  `temp_dir()/flywheel-mail-{pid}.sqlite` + random operator key each process.
  Comments document the limitation; durable loop **depends** on mail across
  leaves.
- **confidence:** High.
- **minimum_acceptable_test:** leaf A `send` → leaf B (new process, same
  config) `inbox` sees message.
- **remediation_posture:** Structural (config + path + key lifecycle).

### F3 — HIGH: MailBackend message ids are `u64` (AM model); toron ids are hex event ids

- **recommended_change:** Canonical id type = **`String` (Nostr event id hex
  or content-addressed rumor id)**. Agent-mail backend accepts decimal strings
  or maps u64→string at the AM adapter boundary only.
- **evidence_of_defect:**
  - `MailBackend::ack/mark_read/reply(..., message_id: u64)`.
  - toron core: `message_id: &str`, rumor id hex.
  - flywheel shim: `message_id.to_string()` / `as i64` for remote — **cannot
    round-trip a real toron inbox id** (hex is not a decimal u64).
- **confidence:** High.
- **minimum_acceptable_test:** send → fetch_inbox → ack(inbox[0].id) succeeds
  on `FLYWHEEL_MAIL=toron`; same test on agentmail with numeric ids.
- **remediation_posture:** Structural API redesign of `MailBackend` (this ADR's
  core API decision).

### F4 — HIGH: `foreach` drops the item (false comment + weak test)

- **recommended_change:** Inject `item` into template context / step_outputs
  before `run_step`; fix test to assert body/content uses each element.
- **evidence_of_defect:** `engine.rs` `run_foreach`: `let _ = item;` while
  comment says `{{item}}`. Test `foreach_over_list_collects` only checks
  `list.len() == 3`.
- **confidence:** High.
- **minimum_acceptable_test:** over `[a,b,c]` → three actions with bodies
  `a`/`b`/`c` (or JSON values).
- **remediation_posture:** Narrow fix.

### F5 — HIGH: `call_workflow` is a permanent stub

- **recommended_change:** Nested Resonate run with deterministic sub-run id
  (parent run id + step name + inputs hash); surface outputs into
  `steps.<name>`; fail_run respects nested failure.
- **evidence_of_defect:** `run_call_workflow` returns fixed `{ok:false,...}`.
  ADR-0005 treats pipelines as `call_workflow` chains — **broken by design
  until wired**.
- **confidence:** High.
- **minimum_acceptable_test:** parent calls child; child step output visible;
  crash-resume of nested run.
- **remediation_posture:** Feature completion (spec already accepted).

### F6 — HIGH: production ActionRunner never stamps `toron:workflow-emitted`

- **recommended_change:** Stamp **rumor tags** (not wraps — wraps are
  metadata-hiding) before wrap for all workflow-originated mail; stamp public
  kinds (7/9/43001…) on the wire event. Host runner must call
  `stamp_workflow_emitted`.
- **evidence_of_defect:** `stamp_workflow_emitted` only used in unit tests;
  `mailbox_runner` SendMessage path has no stamp.
- **confidence:** High (for public kinds / channel path). For pure NIP-17
  without F1, stamp is latent.
- **minimum_acceptable_test:** workflow emits kind-9/7 → tag present →
  `should_evaluate_trigger_event` false.
- **remediation_posture:** Narrow fix + F1 for DM path.

### F7 — HIGH: mention coalescer never emits kind-43001 jobs from the host

- **recommended_change:** On flush, build/sign/publish kind-43001 via jobs
  module; stamp workflow-emitted; wire DispatchPool if needed.
- **evidence_of_defect:** `workflow_host` tick: flush logged, no publish.
  `DispatchJob` only `coalescer.submit`.
- **confidence:** High.
- **minimum_acceptable_test:** N mentions within window → one 43001 on bus.
- **remediation_posture:** Production wiring of existing library.

### F8 — MEDIUM: ActionRunner uses `block_on` on current Tokio handle

- **recommended_change:** Prefer async ActionRunner (Resonate-friendly) **or**
  dedicated blocking pool / `spawn_blocking` with a **shared** multi-thread
  runtime owned by the host — never nest `block_on` on the same runtime.
- **evidence_of_defect:** `workflow_host::mailbox_runner` /
  `Handle::try_current().block_on(...)`.
- **confidence:** Medium (depends on Resonate worker threading; hazard is real).
- **minimum_acceptable_test:** stress: N concurrent workflow sends no hang.
- **remediation_posture:** Design fix (async seam preferred).

### F9 — MEDIUM: dual error types + dual mail traits

- **recommended_change:** See Decision Outcome (MailSurface).
- **evidence_of_defect:** Mux uses `toron_core::BackendError`; mail uses
  flywheel `BackendError{Unsupported, Other}`. `From` collapses codes into
  `Other`. MailBackend still `&mut self` while Backend is `&self`.
- **confidence:** High (maintainability + call-site tax).
- **minimum_acceptable_test:** classify-on-code works across mail ops.
- **remediation_posture:** Structural.

### F10 — MEDIUM: MCP resources are URI shells

- **recommended_change:** Wire index/mailbox projections for the 25 URIs;
  keep static tooling resources.
- **evidence_of_defect:** `resources.rs` note `"data projection not yet wired"`.
- **confidence:** High.
- **minimum_acceptable_test:** `exactly_25_resources` + read
  `toron://agents/{name}/inbox` returns real data after send.
- **remediation_posture:** Feature completion (ADR-0003 Phase 1 claim).

### F11 — MEDIUM: filter fields advertised but ignored

- **recommended_change:** Implement or refuse explicitly (`Unsupported` /
  clear error) for `urgent_only`, `ack_overdue_only`, `since_ts`,
  `date_start`/`date_end` on toron path.
- **evidence_of_defect:** `MailInboxFilter` / `MailSearchFilter` dead_code
  fields; comments "not yet applied."
- **confidence:** High.
- **minimum_acceptable_test:** each filter either works or returns typed
  unsupported.
- **remediation_posture:** Narrow (implement or fail-closed).

### F12 — LOW: empty config module

- **recommended_change:** Precedence: flags > env > project `.toron.toml` >
  user config (as TODO states); wire operator key, data dir, federation, POW.
- **evidence_of_defect:** `config/mod.rs` TODO only.
- **confidence:** High.
- **remediation_posture:** Feature completion.

### F13 — LOW: Runtime::new per mail op in flywheel

- **recommended_change:** One shared `OnceLock<Runtime>` (or async main).
- **evidence_of_defect:** `mail_shim::block_on` creates a new Runtime each call.
- **confidence:** High (perf/correctness of thread explosion under load).
- **remediation_posture:** Narrow.

### F14 — LOW: reservation/build-slot parity comments still say `bd-XXX`

- **recommended_change:** Close or renumber beads; keep regression tests.
- **evidence_of_defect:** comments in identity/reservations/build_slots.
- **confidence:** Medium (hygiene).
- **remediation_posture:** Hygiene.

---

## Soundness Ledger

| claim_id | claim_or_obligation | kind | witness_required | witness_status | preservation | progress | inhabitance | evidence |
|----------|---------------------|------|------------------|----------------|--------------|----------|-------------|----------|
| S1 | NIP-17 DMs can trigger `message_posted` workflows | protocol+product | e2e SC | **MISSING** | broken | stuck | illegal empty set of triggers | F1 |
| S2 | Workflow cannot re-trigger itself | safety | stamp on every emit + filter | **PARTIAL** (filter yes, stamp no) | partial | — | unstamped kind-14/9 can loop | F6 |
| S3 | Integrated mode shares durable mail state | ADR-0003 3A | cross-process inbox | **FAIL** (per-pid temp) | broken | — | empty mailbox inhabitant | F2 |
| S4 | toron mail ids round-trip through flywheel | integration | ack after inbox | **FAIL** (u64) | broken | — | hex not inhabit u64 | F3 |
| S5 | `foreach` binds loop variable | engine | body uses item | **FAIL** | broken | — | item discarded | F4 |
| S6 | `call_workflow` composes workflows | ADR-0005 Spec | nested run | **FAIL** (stub) | n/a | stuck | always failed value | F5 |
| S7 | Mention burst → one job | ADR-0004 Ship 1 | one 43001 | **FAIL** at host | library only | stuck | flush no-ops | F7 |
| S8 | 25 resources readable | ADR-0003 | resource read | **PARTIAL** | stubs | — | "not yet wired" | F10 |
| S9 | Nostr is AM superset at flywheel trait | captain mandate | same intents, both backends | **FAIL** (AM-shaped trait) | — | — | toron forced into u64 world | F3/F9 |
| S10 | Superlogical is honest stub | ADR | Unsupported | **PASS** | preserved | — | no silent fallback | superlogical.rs |

---

## Complexity Delta (incidental vs essential)

| Surface | Essential? | Incidental today | Target |
|---------|------------|------------------|--------|
| Two backend traits (mux + mail) | Yes (domains differ) | Two **error types**, `&mut` vs `&self`, AM id types on mail | Same split, **shared errors + String ids + `&self`** |
| mail_shim + ToronMailBackend + AgentMailBackend | Yes (two transports) | Shim recreates mailbox per pid; Runtime per op | One factory, durable root, shared RT |
| ActionRunner sync + block_on | No | Async mailbox forced through sync | Async runner or blocking pool |
| stamp_workflow_emitted unused | No | Dead API surface | Call or delete |
| call_workflow stub | No | False completeness in P5 | Implement or demote feature |
| Resource templates without data | No | False 25-resource claim | Wire or document Phase gap |

---

## Architecture Analysis: why seamless integration fails today

```
                    ┌─────────────────────────────────────────┐
  flywheel loop     │  MailBackend (AM-shaped: u64 ids)       │
  / leaves          │  &mut self, flywheel BackendError       │
                    └───────────────┬─────────────────────────┘
                                    │
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
     AgentMailBackend                              ToronMailBackend
     (HTTP MCP AM)                                 mail_shim::Mail
              │                                           │
              │                              ┌────────────┴────────────┐
              │                              │ temp SQLite + random op │
              │                              │ Runtime::new per call   │
              │                              └────────────┬────────────┘
              │                                           ▼
              │                              toron_core::Mailbox (async)
              │                              NIP-17 hex event ids
              ▼                                           │
        integer message ids  ✗ cannot equal ──────────────┘
```

**Root causes (not symptoms):**

1. **Identity of a message** was never unified: AM = autoincrement int; Nostr =
   content-addressed event/rumor id. The flywheel trait picked AM.
2. **Durability of projection** was never unified: AM server owns SQLite; toron
   in-process used **temp per-pid** "demo" construction for convenience.
3. **Trigger plane vs wire plane** was never designed: NIP-17 hides kind-14;
   workflow triggers need post-unwrap visibility without violating privacy.
4. **MailBackend deferred** in ADR-0005 P1 left the orchestration trait in the
   wrong shape for the default backend (toron).

These are **API/architecture** problems. Patching `to_string()` on u64 is not
a remedy.

---

## Considered Options (redesign)

### Option A — Keep AM-shaped MailBackend; coerce at toron adapter (REJECT)

Map hex → hash to u64, or keep parallel numeric index.

* Bad: loses Nostr identity; collisions; breaks federation; not a superset —
  a **downcast**. Violates Driver 1.

### Option B — Dual traits forever; document "toron path uses strings in JSON only" (REJECT)

Leave trait as u64; only JSON Value fields carry hex.

* Bad: compile-time type lies; reply/ack signatures unusable; agents and CLI
  confuse two id worlds.

### Option C — Nostr-native MailSurface in toron-core; AM is adapter (CHOSEN)

Move a **`MailSurface`** (name TBD) trait into `toron-core` (or
`toron-mailbox` if split later):

- Sync or async: **async preferred** for mailbox; flywheel wraps with one shared runtime.
- Receiver: **`&self`** + interior mutability (match Backend).
- Message id: **`MessageId` newtype over `String`** (hex).
- Errors: **`toron_core::mailbox::MailError`** machine-coded (mirror BackendError).
- Methods: operation-shaped superset of what flywheel needs (not raw 38 tools),
  each documented as AM-intent + Nostr wire notes.
- **AgentMailBackend** implements the same trait by calling AM MCP and mapping
  `id: number → MessageId(id.to_string())`.
- **ToronMailBackend** is thin: durable `Mailbox` handle + no u64 casts.

### Option D — Kill MailBackend; flywheel only calls MCP tools by name (REJECT)

* Bad: loses typed orchestration; reintroduces schema drift; slower leaves.

### Option E — Trigger plane: post-unwrap internal events (CHOSEN with C)

Do **not** publish plaintext kind-14 to external relays.

Add `TriggerIngress` fed by `UnwrapPipeline` outcomes (and channel kind-9/1/7
from the bus). Workflow host subscribes to TriggerIngress, not raw gift wraps.
Workflow-originated projections carry `toron:workflow-emitted` **before**
ingress sees them.

---

## Decision Outcome (proposed)

### D1 — Canonical message identity

**`MessageId = String`** (hex event/rumor id on toron; decimal string on AM).
Never `u64` in shared traits. CLI/MCP already mostly stringly; align flywheel.

### D2 — MailSurface trait lives next to Mailbox (toron-core)

- Name: `MailSurface` (avoid `MailBackend` collision with AM mental model).
- Domain split retained: `Backend` = multiplexer (herdr/superlogical/tmux);
  `MailSurface` = coordination fabric (toron/agentmail).
- Fate of flywheel-local `MailBackend`: **deleted** after migration (not dual-lived).
- Errors: shared machine codes (`NotFound`, `PolicyDenied`, `Unsupported`, …).

### D3 — Durable mailbox root for integrated mode

- Default path: `$XDG_DATA_HOME/toron/projects/<slug>/` (or flywheel config
  `mail.toron.data_dir`), **not** `/tmp/...pid...`.
- Operator key: stable file (`operator.ncryptsec` / config), not random per boot.
- Document: leaf subprocesses **must** use the same data_dir; remote daemon
  remains the multi-host option.

### D4 — Trigger plane separate from NIP-17 wire plane

- Wire: gift wraps only for DMs (unchanged).
- Triggers: UnwrapPipeline → TriggerIngress → workflow host.
- Loop prevention: stamp on rumors + filter on ingress (F6+F1 together).

### D5 — Engine completeness gates (honesty)

Before any "P5 complete" claim:

| Feature | Gate |
|---------|------|
| `foreach` | item injected + test asserts values |
| `call_workflow` | nested run works **or** feature `#[cfg]` / lint forbid |
| mention coalesce | flush publishes 43001 |
| message_posted | F1 e2e green |

### D6 — ActionRunner concurrency model

Choose one (implementation plan picks after spike):

1. **Async ActionRunner** (`async fn` / boxed future) — preferred if Resonate
   dependency injection allows.
2. Else: host-owned multi-thread runtime + `spawn_blocking` for mailbox calls;
   ban nested `block_on` on worker threads.

### D7 — Defaults (reaffirm)

| Knob | Default | Fallback / stub |
|------|---------|-----------------|
| Multiplexer | `herdr` | `superlogical` stub (Unsupported); `tmux` hold |
| Mail | `toron` (Nostr) | `agentmail` compiled opt-in |
| Bus | InProcessBus | RelayBus for daemon/federation |

### D8 — Superset contract (normative)

toron MUST:

1. Support every AM **intent** flywheel uses (register, send, reply, inbox,
   ack, mark_read, reserve/release/renew/force, search, contacts, products,
   slots, pane identity helpers).
2. Return **usable** ids (string) that round-trip.
3. Preserve Nostr properties: signed events, E2E DMs, true BCC, operator
   key-scoped overseer, addressable reservations.
4. Document intentional shape diffs only in `docs/parity/known-divergences.md`.
5. Never require agents to know npubs (name resolution G2 preserved).

AM backend MUST remain a working fallback for captains mid-migration.

---

## Consequences

### Positive

- One message-id world; flywheel stops lying to the type system.
- Integrated mode becomes actually durable.
- `message_posted` workflows become real for NIP-17 without protocol violation.
- Stubs either ship or are demoted — no false "P0–P6 complete" fog.
- Superlogical remains a clean extension point (mux trait already open).

### Negative

- MailBackend migration touches ~all flywheel mail call sites (real surgery).
- TriggerIngress is a new subsystem (small, but must be designed carefully).
- Some AM-only conveniences (integer ids in JSON) need adapter mapping.

### Neutral

- 38 MCP tools stay frozen; optional args already used for program/model.
- Kind registry untouched.
- herdr Backend trait unchanged by mail redesign.

---

## Implementation Plan (after acceptance)

### Phase R0 — Honesty + quick correctness (1–3 days)

| Bead | Work | Paths |
|------|------|-------|
| R0.1 | Fix `foreach` item injection + strengthen test | `toron-workflow/src/engine.rs` |
| R0.2 | Stamp workflow-emitted on host ActionRunner outputs (public kinds + rumor tags) | `toron-mcp/src/workflow_host.rs`, messaging path for workflow sender |
| R0.3 | Shared Runtime in mail_shim; stop Runtime-per-op | `flywheel/src/mail_shim.rs` |
| R0.4 | Coalescer flush → publish kind-43001 | `workflow_host.rs`, `jobs.rs` |
| R0.5 | Document F1/F2/F3 as known blockers in README ADR table (link this ADR) | `docs/decisions/README.md` |

### Phase R1 — MessageId + MailSurface (API redesign)

| Bead | Work | Paths |
|------|------|-------|
| R1.1 | Introduce `MessageId` + `MailError` in toron-core | `mailbox/ids.rs` (or mod) |
| R1.2 | Define `MailSurface` trait (`&self`, String ids) | `toron-core` or thin facade |
| R1.3 | Implement ToronMailSurface over durable Mailbox | replace demo/temp construction |
| R1.4 | Implement AgentMailSurface adapter (u64↔String at edge) | `flywheel/src/backend/agentmail.rs` |
| R1.5 | Delete flywheel `MailBackend`; migrate call sites | `flywheel/src/backend/*`, main.rs |
| R1.6 | Parity e2e: send→inbox→ack→reply on both backends | `toron-mcp/tests`, flywheel tests |

### Phase R2 — Durable integrated mode

| Bead | Work |
|------|------|
| R2.1 | Config: data_dir, operator key path, mail backend selection (complete `config/mod.rs`) |
| R2.2 | Durable root layout + migration from temp (dev wipe OK — early product) |
| R2.3 | SC: leaf subprocess sees prior leaf's mail |
| R2.4 | sc_fw_integrated uses durable root, not only temp demo |

### Phase R3 — Trigger plane

| Bead | Work |
|------|------|
| R3.1 | `TriggerIngress` channel/bus fed by UnwrapPipeline |
| R3.2 | WorkflowHost subscribes to ingress; MessagePosted matches unwrapped mail |
| R3.3 | e2e F1 test; stamp prevents loop |
| R3.4 | Channel kind-9 path still works (regression) |

### Phase R4 — Engine completeness

| Bead | Work |
|------|------|
| R4.1 | `call_workflow` nested Resonate run |
| R4.2 | ActionRunner async or spawn_blocking policy + stress test |
| R4.3 | Wire React/CallWebhook or lint-forbid until wired |
| R4.4 | Resource projections for 25 URIs (batch by cluster) |

### Phase R5 — Simplification pass (after R1–R4 green)

| Bead | Work |
|------|------|
| R5.1 | Collapse duplicate ToJson / response shapes where safe |
| R5.2 | Reduce engine.rs size via module split (no behavior change) |
| R5.3 | Dead code / unused stamp paths / comment debt (`bd-XXX`) |
| R5.4 | Adversarial re-review (this ADR checklist) → close residual |

### Patterns to follow

- ADR-0003 layering: flywheel → toron-workflow → toron-core.
- Gift wraps never carry project tags (A3 metadata-hiding).
- Operator privilege is key-scoped (Overseer), never name-scoped.
- Unsupported-open backends (ADR-0002 §6 / ADR-0005 P1).
- TDD: RED test for each finding before fix.

### Patterns to avoid

- Casting Nostr ids to u64.
- Per-pid temp mailboxes in production paths.
- Nested `block_on` on the workflow worker runtime.
- Publishing unwrapped kind-14 to public relays for "easy triggers."
- Silent success when a feature is stubbed.
- New MCP tools or new kinds (frozen).

### Configuration (target) — XDG locked (captain Q2)

```text
$XDG_DATA_HOME/toron/                    # TORON_DATA_DIR overrides
  operator/
    operator.ncryptsec                   # stable operator key
  projects/
    <slug>/
      index.sqlite
      archive/                           # GitArchive (AM layout)
      agents/
        <name>/key.ncryptsec
```

```toml
# flywheel / toron
[backend]
multiplexer = "herdr"          # superlogical | tmux
mail = "toron"                 # agentmail

[mail.toron]
# empty url => in-process durable at TORON_DATA_DIR / XDG default
# url = "http://127.0.0.1:18080"  # remote daemon
# data_dir optional; defaults to $XDG_DATA_HOME/toron
# operator via TORON_OPERATOR_NSEC or operator/operator.ncryptsec

[mail.agentmail]
url = "http://127.0.0.1:8765"
```

Env precedence: flags > `TORON_DATA_DIR` / `FLYWHEEL_BACKEND` / `FLYWHEEL_MAIL` > config > XDG default.

---

## Verification (acceptance for this ADR)

- [ ] F1 e2e green: NIP-17 send triggers `message_posted` once; no self-loop
- [ ] F2 e2e green: two processes, same data_dir, mail visible
- [ ] F3 e2e green: inbox id → ack/reply on toron **and** agentmail
- [ ] F4 unit: foreach bodies use items
- [ ] F5 unit/e2e: call_workflow nested success + fail_run
- [ ] F6 unit: stamped public event rejected by trigger filter
- [ ] F7 e2e: N mentions → one 43001
- [ ] F8 stress: no deadlock under concurrent workflow sends
- [ ] `scripts/verify-gates.sh` green
- [ ] No `message_id: u64` in shared mail traits
- [ ] Superlogical still Unsupported-only stub
- [ ] README phase table no longer claims "FULLY COMPLETE" without residual register
- [ ] `docs/parity/known-divergences.md` updated for MessageId if needed

---

## Change Agenda (ordered low → high leverage; last = strongest next move)

1. **change:** Fix foreach item injection  
   **why:** Silent wrong control-flow  
   **posture:** narrow  

2. **change:** Shared Runtime + durable data_dir for mail_shim  
   **why:** F2/F13 destroy integrated-mode truth  
   **posture:** structural-small  

3. **change:** Stamp + coalescer publish  
   **why:** ADR-0004 ships incomplete in production  
   **posture:** wire existing libs  

4. **change:** TriggerIngress post-unwrap  
   **why:** F1 makes message workflows fiction under NIP-17  
   **posture:** structural  

5. **change:** Implement or demote call_workflow  
   **why:** Pipeline story depends on it  
   **posture:** complete or honest  

6. **change:** **MailSurface + MessageId redesign (D1–D3)**  
   **why:** Single highest-leverage move — unblocks superset claim, flywheel
   seamless use of Nostr mail, and ends AM-shaped footguns  
   **posture:** structural API redesign  
   **minimum_acceptable_test:** dual-backend send→inbox→ack→reply matrix  

---

## Fixed-Point Judgment

**Not fixed.** ADR-0003–0005 are **spec-accepted and substantially implemented**,
but **not production-complete** against their own verification claims when
judged from the live tree:

- Library quality is high (kinds, unwrap, engine units, gate scripts).
- **Integration plane is broken** (ids, durability, triggers, stamps, coalesce).
- Declaring "FULLY COMPLETE" in `docs/decisions/README.md` **overclaims**.

Fixed-point requires: Change Agenda items 1–6 green, or explicit demotion of
unfinished features out of the "complete" table with residual register.

---

## Reviewer Bottom Line

1. **Do not ship "Nostr default mail" as AM drop-in** until MessageId is
   String-native and durable roots replace per-pid temp DBs.
2. **Do not trust `message_posted` workflows for DMs** until TriggerIngress
   exists; current filter+matcher+wire triple is inconsistent by construction.
3. **Treat ADR-0005 P5 control-flow as partial:** foreach bug, call_workflow stub.
4. **Mux Backend unification succeeded;** mail trait unification was deferred
   and is now the binding constraint — this ADR proposes finishing it as
   Nostr-native MailSurface, with agent-mail as adapter (superset orientation).
5. **Next captain decision:** accept D1–D8 (or amend), then cut R0–R5 beads;
   do not open parallel feature work that deepens the u64/AM-shaped trait.

---

## Non-Material Concerns

- engine.rs size (~2.2k LOC) — split after behavior fixed.
- Comment debt (`bd-XXX`).
- Output style / caveman CLI polish — out of scope.
- Superlogical real protocol — correctly deferred.
- Blossom/ADR-0006 — orthogonal.

---

## Residual Uncertainty

- Resonate 0.6 dependency injection limits for **async** ActionRunner (needs
  spike before locking D6 option 1).
- Exact durable path convention (XDG vs project `.toron/`) — captain preference.
- Whether TriggerIngress events are in-process only or also persisted for
  audit (recommend in-process first; audit via existing Git projection).

### Unresolved questions for captain

**Resolved 2026-08-08** — see §Captain Decisions below.

---

## Captain Decisions (2026-08-08) — LOCKED

| # | Question | Captain | Locked choice |
|---|----------|---------|---------------|
| 1 | MessageId = String (breaking) | **Agree** | Canonical shared id = string. Flywheel beads must be adapted (not just toron). Trait already partially `&str` (2026-08-08 tree); **AM adapter still `parse::<u64>().unwrap_or(0)`** — silent corrupt id for hex. Full MailSurface + typed MessageId + honest AM edge remains R1. |
| 2 | Durable data_dir | **Up to architect** | **XDG data home:** `$XDG_DATA_HOME/toron/` (fallback `~/.local/share/toron/`). Layout: `operator/`, `projects/<slug>/{index.sqlite,archive/,agents/}`. Env: `TORON_DATA_DIR` overrides root. Flywheel `[mail.toron] data_dir` defaults to same root. No `/tmp`/pid-scoped production paths. |
| 3 | TriggerIngress in-process only? | **OK for now** | v1 = in-process only (UnwrapPipeline → channel/mpsc → WorkflowHost). Persist later if audit needs it. **NIP-34 / Forge git events: NOT implemented** — research note only (`docs/notes/2026-08-07-forge-nip34-research.md`); bead `bd-9o9` closed but code has **zero** NIP-34 kinds (30617/1617/1618/…). What exists is **GitArchive** (local AM-style projection store), unrelated to NIP-34. Do not conflate. Reopen/replan Forge only after ADR-0007 R0–R3. |
| 4 | call_workflow | **Architect chooses** | **Implement nested Resonate run (R4.1)** — lint-forbid alone leaves pipelines unusable. Until R4.1 lands: production `mailbox_runner` / built-in workflows must not depend on `call_workflow`; unit stub may remain only behind a test flag. Prefer full implement over permanent demotion. |
| 5 | README residual | **OK** | Already applied: ADR-0005 phase table points at this ADR residual register. |

### Flywheel bead adaptation (Q1 consequence)

Open epic **`bd-review-hardening-mail-backend-c3b`** already targets M1–M14 (shared runtime, `&self`, filter honesty, wire builder, typed results). ADR-0007 **extends** that epic — do not reinvent:

| Existing flywheel bead | ADR-0007 mapping | Adaptation required |
|------------------------|------------------|---------------------|
| `…c3b.1` shared tokio runtime | R0.3 / F13 | Keep; add durable OnceLock mailbox root (not only runtime) |
| `…c3b.2` `&self` receivers | D2 MailSurface | Keep mechanical; align with toron-core trait when moved |
| `…c3b.3` inbox filter honesty | F11 | Keep; implement or Unsupported — no silent no-op |
| `…c3b.4` MCP init classification | (orthogonal AM path) | Keep as-is |
| `…c3b.5` single wire builder | R1.4 AM adapter | Wire builder must accept **string ids**; AM converts to u64 only at MCP JSON edge with **parse error (not 0)** |
| `…c3b.6` typed result contracts | D1/D2 | **MessageId** field on inbox/search rows; ban u64 in shared types |
| `…c3b.7` pane cleanup honesty | (hygiene) | Keep |

**New flywheel beads (children of c3b or ADR-0007 epic):**

1. **mail-durable-root** — replace temp per-pid SQLite with `$XDG_DATA_HOME/toron/…`; stable operator key; leaf subprocess SC.
2. **mail-messageid-honest-am** — kill `unwrap_or(0)` on id parse; `Err` on non-decimal when backend is agentmail; hex pass-through for toron.
3. **mail-surface-align-adr0007** — when toron-core ships `MailSurface`/`MessageId`/`MailError`, re-export and delete flywheel-local duplication; migrate factory.

**Toron beads (new epic under this ADR):** R0.1 foreach, R0.2 stamp, R0.4 coalesce publish, R1 MessageId+MailSurface, R2 durable config, R3 TriggerIngress, R4.1 call_workflow implement, R4.2 ActionRunner concurrency, R4.4 resources.

### NIP-34 / git protocol status (Q3 detail)

| Layer | Status |
|-------|--------|
| Local **GitArchive** (messages/agents layout, commit coalescer, DR rebuild) | **Implemented** (`toron-core/src/archive/git.rs`) — AM-parity projection, not NIP-34 |
| **NIP-34** kinds (30617 repo, 30618 state, 30621 commit, 1617 patch, 1618 PR, 1630–1633 status) | **Not in kinds.rs, not published, not subscribed** |
| Forge channel-per-branch UX | **Research only** (note + false-complete risk on `bd-9o9`) |
| Blossom / NIP-B7 blobs | **ADR-0006 accepted** — separate from Forge |

**Conclusion:** git-as-archive is real; git-as-Nostr-forge is **not**. No work under ADR-0007 depends on NIP-34.

### call_workflow choice detail (Q4)

- **Chosen:** full nested Resonate implementation (R4.1).
- **Why not lint-only:** ADR-0005 Spec 2b absorbs pipelines as `call_workflow` chains; permanent forbid regresses the tier story.
- **Sequencing:** R0–R3 first (correctness/triggers/durability) so nested runs compose on a sound host; R4.1 immediately after.
- **Interim:** if a built-in workflow uses `call_workflow` today, rewrite to flat steps or mark ignored until R4.1 green.

---

## Counter-Argument (Round 1 — opencode adversarial, code-grounded, git-diff in-file)

> Captain directive: debate/counter, edit in same file. Counter verified
> against the **live working tree** (2026-08-08) — the tree already carries
> uncommitted R-bead work (MCP id args `i64→String`, sender-name resolution,
> free-form `thread` tags, `TORON_MCP_DATA_DIR`), so several citations in the
> audit are stale relative to what this ADR audits.

### Verdict

**AGREE-WITH-CHANGES.** F1, F4, F5, F6, F7, F8, F9, F13 **stand**. F3 stands
in spirit but its evidence is **stale**. Two **missing findings** (durable-root
↔ KDF-startup collision; frozen-surface violation). One overclaim (D8.2).
One complexity critique (TriggerIngress framing). One acceptance trap (F1 gate).

| Finding | ADR | Counter |
|---------|-----|---------|
| F1 message_posted/NIP-17 | CRITICAL | **Stands.** Only e2e in tree (`workflow_host.rs:513`) publishes **raw kind-14 on the bus**, bypassing gift-wrap — not F1 coverage |
| F2 per-pid ephemeral | CRITICAL | **Stands**, but D3/R2.2 remedy collides with the tree's own KDF fix (§7) — see C2 |
| F3 u64 ids | HIGH | **Evidence stale**: flywheel `MailBackend` trait already `&str` (`backend/mod.rs:237–252`); residual = response-side coercion — see C1 |
| F4 foreach | HIGH | Stands (`engine.rs:640` `let _ = item;`; comment still lies) |
| F5 call_workflow | HIGH | Stands (`engine.rs:705` fixed `{ok:false}` stub) |
| F6 stamp | HIGH | Stands (call sites = tests only) |
| F9 `&mut self` | MEDIUM | Stands (`backend/mod.rs:249`) |

### C1 — F3 evidence stale; response-side u64 coercion missing from ledger

- **recommended_change:** Re-point F3 at the live residual: (a)
  `mail_shim.rs:361,380` + `agentmail.rs:146,172` `message_id.parse::<u64>().unwrap_or(0)`
  on ack/mark_read responses; (b) response-side row coercion
  `mail_shim.rs:1352` `RenewedReservation { id: u64 }` + `:1370`
  `Value::as_u64().unwrap_or(0)` — hex ids silently become `0`.
- **evidence_of_defect:** `backend/mod.rs:237–252` — the trait this ADR quotes
  as `u64` no longer exists; the c3b beads already migrated it to `&str`. The
  min test (send→inbox→ack) fails today for the **response-side** reason, not
  the request-side one the ledger cites.
- **confidence:** High.
- **remediation_posture:** Documentation correction; fold response-side into R1/R1.4.

### C2 — MISSING: durable root (D3/R2.2) re-introduces the KDF startup stall

- **recommended_change:** R2 must bound Mailbox startup cost before the
  durable root: one key scan per host, or incremental KDF; gate R2.2 on a
  rebuild-cost bound with a many-keys fixture.
- **evidence_of_defect:** tree `server.rs` comment + `known-divergences §7`
  (2026-08-08): the unwrap loop runs argon2 KDF (~1s/key) over every key in
  data_dir on every rebuild; **observed 80+ keys → minutes stall, search
  never lands**. The tree's fix moved **away** from a fixed dir
  (per-invocation + `TORON_MCP_DATA_DIR`). D3's stable XDG root accumulates
  operator + agent keys across leaves — every new leaf subprocess pays the
  full KDF scan at startup (mail_shim caches per-process via `OnceLock`, but
  herdr panes are new processes). R0–R5 never touches this; `mail_shim.rs:50–58`
  caches the Mailbox, not the key scan.
- **confidence:** High (first-hand tree comment).
- **remediation_posture:** Structural-small; add bead to R2.

### C3 — MISSING: the ADR's own remedy violates its frozen tool surface (Driver 5)

- **recommended_change:** Record the arg-type break as a sanctioned,
  pre-1.0 exception with a dual-accept migration (`message_id: String | i64`
  via `#[serde(untagged)]`) for one release — or re-scope Driver 5 to "tool
  names + kinds frozen; arg types may break with a divergence note".
- **evidence_of_defect:** `tools.rs` already changes required arg types
  (`message_id`, `file_reservation_id` `i64→String`) — breaking for existing
  MCP clients. ADR-0007 Consequences/Neutral claims "38 tools stay frozen;
  additive optional args OK" — a type change is not additive, no migration
  exists, and `known-divergences §2b` documents the fix without flagging the
  frozen-surface conflict.
- **confidence:** High.
- **remediation_posture:** Spec amendment (small).

### C4 — F2 severity unweighted against its security delta

- **recommended_change:** D3 must weigh the stable-operator-key-on-disk +
  shared-root attack surface: `0600` key perms, threat note that temp-per-pid
  random keys were an accidental isolation boundary being removed.
- **evidence_of_defect:** D3 mandates stable `operator.ncryptsec` on a shared
  XDG root readable by any leaf subprocess; no security analysis in ADR-0007.
- **confidence:** Medium-High (posture, early product).
- **remediation_posture:** D3 amendment.

### C5 — D8.2 overclaims cross-backend round-trip

- **recommended_change:** Scope D8.2 per-backend: "an id returned by backend X
  is usable on backend X". Cross-backend (hex → agentmail) is impossible by
  construction — AM has no hex namespace; a hex string in AM's u64 field is
  corruption by definition.
- **evidence_of_defect:** As written, D8.2 is an unsatisfiable global
  invariant that invites adapter downcasts (Option A revival) exactly where
  Driver 1 forbids them.
- **confidence:** High (logic).
- **remediation_posture:** Wording fix.

### C6 — TriggerIngress framed as a subsystem; a narrow fix exists in-tree

- **recommended_change:** D4 / Change Agenda item 4 → "add a channel
  `ProjectionSink` implementation to the existing UnwrapPipeline" — same shape
  as `SqliteProjectionSink` at `unwrap.rs:443`. Not a new subsystem.
- **evidence_of_defect:** The ADR's own remediation doctrine (narrowest
  truthful fix first, escalate only when insufficient) is violated by its D4
  framing; the pipeline sink seam already exists.
- **confidence:** High.
- **remediation_posture:** De-scope D4 (naming + blast radius).

### C7 — F1 acceptance trap: the tree's e2e bypasses NIP-17

- **recommended_change:** F1 minimum test must send via `Mailbox::send`
  (gift-wrapped) and assert trigger; the raw-bus test must be labeled
  non-NIP-17 so it cannot count as F1 green.
- **evidence_of_defect:** `workflow_host.rs:513` publishes `kind14(...)`
  directly via `bus.publish` — the wrap/filter/ingress path (the exact
  triple F1 says is broken) is never exercised.
- **confidence:** High.
- **remediation_posture:** Test-spec amendment.

### Soundness note

| claim | counter |
|-------|---------|
| S1 | stands; C7 guards the witness (raw-publish test ≠ NIP-17 witness) |
| S3 | stands; C2 guards the remedy (durable root unusable without cost bound) |
| S9 | stands; C5 scopes the contract (round-trip is per-backend) |

### Change Agenda (changes to this ADR, low → high leverage)

1. **change:** Fix F3 evidence + add response-side coercion (C1) — posture: doc + narrow
2. **change:** F1 min test via gift-wrapped send (C7) — posture: test spec
3. **change:** Scope D8.2 per-backend (C5) — posture: wording
4. **change:** D3 security note + key perms (C4) — posture: spec amendment
5. **change:** Record frozen-surface exception + dual-accept migration (C3) — posture: spec amendment
6. **change:** De-scale D4 to a ProjectionSink (C6) — posture: de-scoping
7. **change:** **Add Mailbox startup-cost bead + many-keys gate to R2 (C2)** — posture: structural-small; strongest next move — without it, D3's durable root re-introduces the exact stall the tree just fixed.

### Bottom line

The bug ledger is materially correct and its D1–D8 direction is sound. It is
**not complete against the tree it audits** (stale F3 citation, missed
KDF/durable-root collision, missed frozen-surface break) and must not cut
R2 (durable root) before C2's startup-cost bead. Everything else in R0/R1/R3–R5
is safe to proceed after the doc-level corrections.

---

## More Information

### Audit method

- Multi-pass: surface scan (TODO/stub/rg), deep read of mailbox/messaging/
  unwrap/engine/workflow_host/backend/mail_shim, integration path flywheel↔toron,
  ADR cross-check vs code.
- Skills applied: multi-pass-bug-hunting, adversarial-reviewer, adr-skill,
  Rust async/API judgment (block_on, dyn traits, id types).

### Key file map

| Concern | Path |
|---------|------|
| Kind registry | `crates/toron-core/src/nostr/kinds.rs` |
| NIP-17 messaging | `crates/toron-core/src/mailbox/messaging.rs` |
| Unwrap + overseer | `crates/toron-core/src/mailbox/unwrap.rs` |
| Mux Backend | `crates/toron-core/src/backend/mod.rs` |
| Engine | `crates/toron-workflow/src/engine.rs` |
| Trigger filter | `crates/toron-workflow/src/trigger_filter.rs` |
| Workflow host | `crates/toron-mcp/src/workflow_host.rs` |
| MCP tools | `crates/toron-mcp/src/tools.rs` |
| Flywheel factory | `flywheel/src/backend/factory.rs` |
| Mail shim | `flywheel/src/mail_shim.rs` |
| Toron mail backend | `flywheel/src/backend/toron.rs` |
| Superlogical stub | `flywheel/src/backend/superlogical.rs` |

### Related ADRs

- [ADR-0003](0003-nostr-mailbox-fabric.md) — fabric + parity + 3A embed
- [ADR-0004](0004-operational-hardening.md) — 5 ships
- [ADR-0005](0005-orchestration-layer.md) — orchestration tier
- [ADR-0006](0006-blossom-blob-federation.md) — orthogonal blobs

// governed-by: ADR-0007 (proposed)
