# ADR-0011: Coalition Orchestration — Steer + Collab as Layers, Intent-Based Coordination

**Status:** Proposed (6-round grilling session 2026-08-09; captain review pending)
**Date:** 2026-08-09
**Decision-makers:** Captain (bangedorrunt) — grilling session
**Extends:** [ADR-0005](0005-orchestration-layer.md) (orchestration tier), [ADR-0010](0010-execution-wiring-model-registry-headless.md) (execution wiring — coalitions need the real runner + ingress).
**Amends:** none (respects the ADR-0003 kind freeze — **no new Nostr kind**).

> **Scope:** a captain dispatches a goal → a coalition self-forms → crews declare intents → overlaps
> are detected + split *before* anyone holds → disputes negotiate → crews edit non-overlapping →
> goal completes → coalition dissolves. Steer (top-down) and collab (lateral) are **always-on
> layers**, not switchable modes. Built entirely on existing primitives (30031, NIP-29 channels,
> workflow triggers, the mailbox). This ADR adds the *coalition* concept + the *intent* state +
> the *soft-conflict* mechanism on top of ADR-0005/0010.

---

## Context and Problem Statement

### The captain's question

The captain proposed two orchestration **modes**: *steer mode* (captain dispatches a fleet of
crews) and *collab mode* (crews interact — for shared issues, or permission on conflicts like file
reservations), native and blendable. The question: is this the right abstraction? Any better idea?

### What the grilling corrected (6 rounds, 2 reconsiderations)

1. **"Modes" is the wrong shape.** Steer (top-down) and collab (lateral) co-exist *constantly* —
   a steered crew that hits a file conflict doesn't "switch to collab mode"; it negotiates *while*
   steered. Modeling them as switchable modes creates a false seam + duplicates the transport (both
   use the mailbox). The "blend" the captain wants is *automatic* if they are **layers**, not modes.
   **Resolution: two always-on layers of one fabric.**

2. **"Intent-based, not lock-and-block" was the right instinct — but concurrency forces the hard
   floor back in.** Two crews declaring overlapping intents near-simultaneously both pass a
   detector-verdict "no overlap" (check-then-act race). The only thing that actually prevents a
   double-hold is the **existing atomic reservation acquire** (race-free conflict check). So the
   design is necessarily *advisory intents OVER atomic reservations* — not intents *instead of*
   reservations. **Resolution: embrace the two-layer truth; the conflict-check is NEVER removed.**
   (Removing it "because intents handle it" would be a data-corruption bug.)

3. **"Soft-default acquire + freeze-on-overlap" re-lets the race in** unless the acquire checks
   *intents* as soft-conflicts. A crew that acquires fast beats a reactive freeze. **Resolution
   (the load-bearing mechanism): extend `check_file_reservation_conflicts` to treat `status:intent`
   30031s as SOFT conflicts** (trigger freeze+propose), held reservations as HARD (block). This is
   what makes soft-default + freeze actually work.

4. **A new dedicated intent kind was rejected.** Reopens the ADR-0003 kind freeze for no
   enforceable gain. **Resolution: 30031 `status: intent→held→released` state machine** — one kind,
   zero new trigger surface (reuse `reservation_acquired` filtered by status).

### What already exists (the captain partly rediscovered)

- **Steer primitives:** `xproj send/request` (cross-project fan-out), `list --all` (multi-swarm),
  squads + `parallel over:@crew.role`, `flywheel loop` per project.
- **Collab primitives:** toron mail (DM/thread/`@mention` job 43001→43004), file reservations
  (acquire/conflict/force-release), debate + plan-review-implement patterns, NIP-29 channels,
  product bus, cascade (SC-10 `reservation_released → workflow`, specced, ingress uncertain).
- **The gaps this ADR closes:** (a) no fleet/coalition abstraction; (b) no request-release
  *negotiation* (only force-release); (c) cross-crew protocols are ad-hoc mail, not first-class;
  (d) reactive trigger ingress is uncertain (cascades/handoffs may not fire).

### The problem statement

> How do we give the captain a way to dispatch a *goal* to a coordinated set of crews that
> self-organize around shared files — declaring intents, auto-splitting overlaps before anyone
> holds, negotiating disputes — as **always-on steer+collab layers** (not modes), on the existing
> mailbox + reservation + workflow primitives, WITHOUT new Nostr kinds, and WITHOUT removing the
> atomic conflict-check that prevents data corruption?

---

## Decision Drivers

1. **ADR-0003 kind freeze is load-bearing.** No new kind. Intents reuse 30031; coalitions reuse
   NIP-29 channels + workflow runs.
2. **The atomic reservation conflict-check is the corruption-prevention floor — NEVER removed.**
   Intents are an advisory layer ON TOP, not a replacement.
3. **Peer-first (no orchestrator in the hot path).** The overlap detector is a workflow-as-reaction
   (event-driven, ephemeral), not a persistent central brain.
4. **Steer + collab are layers, always co-active.** No mode-switching; no false seam.
5. **ADR-0010 dependency.** Coalitions need the real runner (P10.1) + reactive ingress to fire the
   detector/cascades. Coalition work lands AFTER ADR-0010's ingress.
6. **Backward compatibility.** Existing reservations (status-less = held) keep working; the
   `status` field is a tolerant-reader extension.

---

## Decision Outcome

### The tier model (layers, not modes)

```
┌──────────────────────────────────────────────────────────────────────┐
│ STEER LAYER (top-down, captain → coalition)                           │
│   flywheel coalition {form, dispatch, status, disband}                │
│   formation: captain-formed OR auto-coalesce (detector)               │
├──────────────────────────────────────────────────────────────────────┤
│ COLLAB LAYER (lateral, crew ↔ crew)                                   │
│   intent protocol (declare → detect → propose → negotiate)            │
│   request-release negotiation + escalation                            │
├──────────────────────────────────────────────────────────────────────┤
│ ENFORCEMENT FLOOR (atomic, race-free — ADR-0003, UNCHANGED)          │
│   30031 reservations: acquire (conflict-checked) / release            │
│   + NEW: status:intent treated as a SOFT conflict by the check        │
├──────────────────────────────────────────────────────────────────────┤
│ TRANSPORT (ADR-0003, unchanged)                                       │
│   NIP-29 channels (coalition = intent board), mailbox, bus            │
└──────────────────────────────────────────────────────────────────────┘
```

Both layers are always active. A coalition is simultaneously steered (captain formed it / assigned
the goal) AND collabing (crews declare intents, negotiate splits). No mode transition exists.

### Spec 1 — The Coalition object (no new kind)

A **coalition** is an ephemeral, per-goal grouping of **pane crews** (interactive; headless cron
crews are a separate autonomous world — see Spec 5). It is represented as:

- A **NIP-29 channel** — the coalition's *intent board* + shared thread (membership = access gate).
- A **workflow run** — the goal driver (the goal meta-workflow, ADR-0005). Goal-complete = run
  terminal state.

No new kind. Crews join the channel; intents/proposals/negotiation are events in it; disband =
channel closes + run ends.

**Formation (two paths):**
- **Captain-formed:** `flywheel coalition form <goal> --crews <squad>` explicitly creates a
  coalition + assigns crews. Deliberate multi-crew steer.
- **Auto-coalesce:** the detector (Spec 3) merges two *unaffiliated* crews into an ad-hoc coalition
  when their intents overlap on **non-trivial source paths** (a shared source directory, NOT
  README/config/docs). Strangers become a coalition only on real source overlap.

**Disband (hybrid):** beads-closed is primary (the coalition's beads all `br close`d); auto-fallback
(workflow-run terminal, or a timeout) when beads aren't used. On disband the channel closes + run
ends; any held reservations are released (or expire by TTL).

### Spec 2 — Intents (30031 status state machine)

An **intent** is a declared plan to edit a path set, *before* acquiring. It reuses kind-30031 with
a new `status` field (tolerant-reader extension; existing status-less 30031s = held, unchanged):

```
status: intent   →  (no overlap)  →  status: held   →  status: released
                 →  (overlap)      →  freeze + propose → (agree) held | (disagree) negotiate
```

- **Intent TTL + must-convert (anti-griefing):** an intent auto-expires (default ~5min, tunable)
  UNLESS it converts to a held reservation within the window. A speculative "intent on `src/**`"
  cannot freeze peers indefinitely — it expires if the crew isn't actually acquiring. Cheap intents
  are time-bounded; only serious intents (that convert to held) hold weight.
- **Coalition-scoped:** an intent is declared IN a coalition's channel; the soft-conflict check
  (Spec 4) considers intents within the relevant scope.

### Spec 3 — The detector + proposer (workflow-as-reaction)

The **overlap detector** is a workflow triggered on `reservation_acquired` with `status=intent`
(reuses the existing trigger, filtered by status). On each intent declaration it:

1. Reads the coalition's intent board (the channel's status=intent 30031s).
2. Detects overlaps with the new intent.
3. On overlap: fires the **tiered proposer** — deterministic rules first (split by directory
   boundary / bead-dependency; sequence by dependency; merge if same bead), escalate to an **LLM
   planner** only if a crew **rejects** the rule-split.
4. On no overlap: the intent is clear to convert to held.

No persistent central brain — the detector is an ephemeral workflow-per-declaration, peer-first.
(The detector workflow itself runs on ADR-0010's real runner; the LLM-planner arm is a headless
planner crew.)

### Spec 4 — The soft-conflict check (THE load-bearing mechanism)

`check_file_reservation_conflicts` is extended to treat `status:intent` 30031s as **SOFT
conflicts**:

- An acquire against someone's **held** reservation → HARD conflict → blocked (as today).
- An acquire against someone's **declared intent** → SOFT conflict → **freeze the acquirer + fire
  the proposer** (not a hard block).

This is what makes "soft-default acquire + freeze-on-overlap" actually work — without it, a
fast-acquiring crew beats the reactive detector and the intent layer degrades to request-release.
**The conflict-check is NEVER removed; intents add a soft tier above the hard floor.**

**Proposer teeth (soft-default, freeze-on-contention):** acquire is free when there's no
soft-conflict; the freeze fires ONLY on a detected soft-conflict, blocking both crews' acquires on
the contested paths until the split is agreed. No latency in the common case; enforcement only on
real contention.

### Spec 5 — Headless crews are a separate world

Coalitions are **pane-crew-only** (interactive, persistent, shared-file work). ADR-0010's headless
cron crews are one-shot + autonomous (idempotent maintenance, no shared files, no negotiation) —
they are **outside the coalition model**. Two orchestration worlds for two workload types; no
forced fit. (A future phase may admit headless crews as *leaf workers* with a pre-split scope, no
negotiation — deferred.)

### Spec 6 — Disputes: request-release negotiation + escalation

When a proposal is rejected or a hard-conflict blocks, crews fall back to **request-release
negotiation** (the gap ADR-0011 closes alongside intents): a typed message — Crew A asks B to
release/defer; B answers/defers; if no response within a tier window, **escalate** (kind-14 DM to
captain, importance high). This replaces today's binary (voluntary-release | operator
force-release) with a structured negotiation ladder. Force-release remains the operator's last
resort.

---

## Consequences

### Positive
- **Steer + collab as layers** — the blend the captain wanted is automatic; no mode seam.
- **Intent-based coordination** reduces contention *before* work starts (propose splits early).
- **The soft-conflict check** makes the intent layer actually enforceable without a declare→verdict
  gate (no latency in the common case).
- **No new kind** — kind freeze respected; intents = 30031 status; coalition = channel + run.
- **Corruption-prevention intact** — the atomic reservation floor is unchanged and load-bearing.
- **Two workload worlds** — coalitions (interactive) vs headless cron (autonomous), cleanly
  separated.

### Negative
- **Soft-conflict is a griefing surface** — mitigated by intent TTL + must-convert (Spec 2), but a
  misbehaving crew within its TTL window can still freeze peers briefly. Acceptable; documented.
- **Detector churn** — each intent declaration fires a workflow; N crews × M intents = N×M detector
  runs (LLM only on dispute). Bounded by intent scope + TTL.
- **Coalitions depend on ADR-0010** (real runner + ingress) — cannot ship before P10.1 + ingress.
- **Auto-coalesce on "non-trivial source overlap"** needs a file-classification rule (source vs
  doc/config) — an open tuning question.

### Neutral
- **One ADR, three beads.** All in flywheel + toron-core (the soft-conflict check); coordinated via
  the bead breakdown.

---

## Verification

### Per-spec gates
- **1 (coalition):** `flywheel coalition form <goal> --crews <squad>` creates a NIP-29 channel +
  starts the goal workflow run; `flywheel coalition status <name>` lists members + active intents;
  disband closes the channel + ends the run on beads-closed.
- **2 (intents):** a crew declares `status:intent` on a path (visible in the intent board); the
  intent auto-expires after TTL if not converted to held; converting to held succeeds.
- **3 (detector):** declaring an intent that overlaps another fires the detector workflow; the
  tiered proposer emits a rule-split first; rejecting it fires the LLM planner; non-overlapping
  intents convert to held untouched.
- **4 (soft-conflict):** acquiring a path against a declared intent (status=intent, in-TTL) returns
  a SOFT-conflict → freeze + propose (NOT a hard block); acquiring against a held reservation
  hard-blocks as today; acquiring with no conflict succeeds immediately (no latency).
- **5 (headless):** a headless cron run does NOT appear in any coalition; coalitions list shows
  only pane crews.
- **6 (negotiation):** a rejected proposal / hard-block triggers a typed request-release message;
  no response within the tier window escalates (kind-14 DM to captain).

### Cross-cutting
- Existing reservations (status-less = held) acquire/release/conflict-check byte-identically.
- The atomic conflict-check is verified still-present (a regression test: two concurrent acquires
  of the same path — one held blocks the other).
- `triage.yaml`, the ADR-0004 suite, and the live `flywheel loop` pass unchanged.

---

## Implementation Blueprint

### Phasing (dependency-sequenced; all in flywheel + toron-core)

| Phase | Bead | What lands | Deps |
|---|---|---|---|
| **P11.1 — Reactive ingress** | `bd-foh8.1` | wire bus→workflow so triggers (`reservation_acquired`, `message_posted`, `reaction_added`) actually fire workflows. Closes the ADR-0007 trigger-ingress gap. **Foundational** — the detector + cascades can't run without it. | ADR-0010 P10.1 (real runner) |
| **P11.2 — Negotiation + soft-conflict + intent TTL** | `bd-foh8.2` | (a) request-release typed message + escalation tier (closes the binary-release gap); (b) extend `check_file_reservation_conflicts` to treat `status:intent` as SOFT conflict (freeze+propose); (c) 30031 `status:intent` field + TTL + must-convert. **The collab floor + the load-bearing mechanism.** Pure toron-core/mailbox — **no ingress dep** (the load-bearing mechanism unblocks immediately, in parallel with P10.1). | — (none) |
| **P11.3a — Coalition lifecycle + CLI** | `bd-foh8.3` | NIP-29 channel + workflow-run coalition lifecycle; intent-board declaration surface; captain-formed (`flywheel coalition form`); manual collab via P11.2 negotiation (no auto-proposer yet); disband (beads-closed + auto-fallback); `flywheel coalition {form,status,disband}`. Ships the steer surface + manual collab WITHOUT the detector. | P11.2 |
| **P11.3b — Detector + proposer + auto-coalesce** | `bd-foh8.4` | the detector workflow-as-reaction (tiered proposer: rules→LLM on dispute); auto-coalesce on non-trivial source overlap; wires the proposal into the P11.2 soft-conflict freeze path. The automation layer on top of P11.3a's manual baseline. | P11.3a + P11.1 |

### Deferred (explicit)
- **Headless crews as coalition leaf workers** (pre-split scope, no negotiation) — future phase.
- **Cross-project coalitions** (different file namespaces) — this ADR scopes coalitions intra-project;
  cross-project (xproj-style) is follow-on.
- **Coalition observability dashboard** (`flywheel coalition status` richness) — ships minimal in
  P11.3a, deepens later.
- **Dynamic/elastic coalition membership** (join-mid-goal) — channels handle join/leave; full
  intent-cleanup on ungraceful leave is follow-on.

### Open questions (resolve in implementation, not more design)
- Intent TTL default value (~5min proposed) — tune empirically.
- Auto-coalesce file-classification rule (source vs doc/config) — a path-suffix/ignore list.
- Intent granularity (file vs glob vs dir) — propose dir/glob-level (the proposer splits by dir).

### Architectural invariants (every phase obeys)
1. **No new Nostr kind.** Intents = 30031 status; coalition = channel + run. (ADR-0003.)
2. **The atomic conflict-check is NEVER removed.** Intents add a SOFT tier above the HARD floor.
3. **No orchestrator in the hot path.** The detector is a workflow-as-reaction, ephemeral.
4. **Steer + collab are layers, not modes.** No mode-transition logic exists.
5. **Coalitions are pane-crew-only.** Headless cron is a separate world.
6. **Backward compat.** Status-less 30031s = held, unchanged; `triage.yaml` + ADR-0004 suite + live
   loop pass at every boundary.

### Blast radius
- **Context (may change):** `toron-core/src/mailbox/reservations.rs` (status field + soft-conflict
  check + TTL); `toron-workflow/src/{trigger_filter.rs (ingress wiring), schema.rs (Trigger
  filtering)}`; `flywheel/src/{main.rs (coalition CLI + detector wiring), coalition.rs (NEW),
  intent.rs (NEW), cron.rs (unchanged — headless separate)}`. Docs: this ADR + ADR index/README.
- **Target:** coalition/intent/negotiation = flywheel; soft-conflict check + ingress = toron-core +
  toron-workflow.
- **Forbidden:** kind registry values; the atomic conflict-check's corruption-prevention role; new
  MCP tools; ADR-0003 layering.

---

## Review Log

| Round | Topic | Outcome |
|---|---|---|
| 1 | modes vs layers; conflict model; fleet persistence; first deliverable | layers (not modes); intent-based; ephemeral coalition; "all three" deliverables |
| 2 | detector home; intent kind; proposer; build order | workflow-as-reaction; (then) new kind; tiered rules→LLM; ingress→negotiation→coalition |
| 3 | kind freeze reconsidered; coalition bus object; intent lifecycle; disband | **reconsidered: 30031 status (no new kind)**; channel+run; auto-acquire; hybrid disband |
| 4 | concurrency (intents OVER reservations); proposer teeth; headless composition | embrace two-layer truth; soft-default+freeze-on-overlap; separate worlds (headless ≠ coalition) |
| 5 | soft-default race (the freeze window); coalition formation authority | **soft-conflict check (load-bearing fix)**; both: captain-formed + auto-coalesce |
| 6 | soft-conflict griefing; auto-coalesce rule | intent TTL + must-convert; coalesce on non-trivial source overlap |
| **Pending** | captain review of this ADR | — |

---

## More Information

- **Relationship to ADR-0005/0010:** extends the orchestration tier with the coalition concept +
  intent protocol; depends on ADR-0010's real runner (P10.1) + reactive ingress (P11.1).
- **Relationship to ADR-0003:** respects the kind freeze (no new kind); the atomic reservation
  floor is unchanged and load-bearing.
- **Revisit triggers:** (a) a use case forces cross-project coalitions (reopens xproj-style fleet
  scope); (b) headless crews must join coalitions (reopens leaf-worker spec); (c) the soft-conflict
  griefing proves unmanageable (reopens scoped-soft-block).

// governed-by: ADR-0003, ADR-0005, ADR-0010
