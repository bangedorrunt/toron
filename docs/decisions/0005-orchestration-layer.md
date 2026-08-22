# ADR-0005: Orchestration Tier — Profiles, Workflows, Stages, Smart Cron, and a Flexible Workflow Engine

**Status:** Accepted (2-round adversarial review + captain amendments through Revision 5)
**Date:** 2026-08-05
**Decision-makers:** Captain (bangedorrunt) + architect agent + opencode reviewer (2-round adversarial pass)
**Supersedes:** none. **Extends:** [ADR-0003](0003-nostr-mailbox-fabric.md) (layering 3A), [ADR-0004](0004-operational-hardening.md) (workflow hardening). **Amends:** [ADR-0002](adopt-agent-adapter-architecture.md) (Backend trait ownership).

> **Revision 5 (2026-08-05, captain amendment):** converge to ONE term — **workflow**.
> Revision 4's "playbook" and flywheel's imperative `Workflows`/`Pipelines` unify into a
> single vocabulary on a single engine (toron-workflow, Resonate): one schema, one
> `flywheel workflow run <name>`, durable by default. flywheel's `Send/Wait` become
> workflow step types; `pipeline` → a workflow of `call_workflow` steps. Added:
> **auto-crew** (Spec 4c — orchestrator smart-steers the crew from the workflow,
> parallel/fan-out where relevant), **default configuration** (zero-config built-ins),
> and a **per-behavior test plan** (every flow tested).
>
> **Revision 4 (2026-08-05, captain amendment):** the durable concept is renamed
> **template → playbook** ("template" read cron-only; "playbook" is trigger-agnostic —
> event/schedule/on-demand — and purpose-nameable: `debate`, `plan`, `goal`). A **stage
> library** (Spec 2a) elevates flywheel's hardcoded `STAGES` (p1–p6/p8) into named,
> reusable prompt primitives both the imperative driver and durable playbooks
> reference — so a playbook step is EITHER a raw action OR a named-stage reference
> ("combine existing + add new"). A **built-in playbook library** (Spec 2b) ships the
> canonical purpose-named playbooks: `brainstorm`/`plan`/`beads`/`debate`/`goal`.
> CLI: `flywheel playbook {list,run,...}` (durable) alongside `flywheel workflows`
> (imperative, stays). Internal engine type stays `Workflow`; "playbook" is the
> user-facing term.
>
> **Revision 3 (2026-08-05, captain amendment):** two changes. (a) **Vocabulary
> coexistence, not migration** — flywheel's `Workflows`/`Pipelines`/`Recipe` are
> a *sibling* imperative driver, not legacy to deprecate; Spec 2 reframed as
> layering with a documented seam. (b) **Built-in profile library** (Spec 1b) —
> the ADR now *ships* the canonical roles (oracle/planner/reviewer/coder/scout/
> housekeeper) with tuned behavior preambles, not just the mechanism.
>
> **Revision 2 (2026-08-05):** incorporates the opencode round-1 counter (9
> required changes, all accepted). Headline corrections: (0) the codebase audit
> is fixed — flywheel ships a **live** herdr-direct workflow system
> (`Recipe`/`Workflows`/`Pipelines`), not "stub/unused"; (1) **P0** adds a
> failure-propagation path to the engine (prerequisite for smart cron — the
> engine today *swallows* step errors by design); (2) **P3** wires the missing
> `flywheel → toron-workflow` cargo edge; (3) **P1** is re-scoped as
> trait-surface unification (the two backend traits are structurally
> incompatible, not a mechanical merge); (4) `spawn`-await is specced precisely
> (promise↔job binding, no-claim/timeout/abort); (5) the DAG engine is gated on
> a Resonate-Rust concurrency spike; (6) profiles drop `contact_policy` (30014
> is normative) and gain a scope rule.

---

## Context and Problem Statement

### The captain's churn (the actual problem)

After shipping [ADR-0003](0003-nostr-mailbox-fabric.md) (Nostr mailbox fabric) and
[ADR-0004](0004-operational-hardening.md) (5 hardening ships), the captain reports
**churn** around three questions ADR-0003 left *implied* but never *resolved*:

1. **Where does the orchestration vocabulary live?** The workflow engine
   (`toron-workflow`) and the herdr backend (today duplicated across two
   *structurally incompatible* traits) feel like they *want* to be unified —
   but ADR-0003 §Decision 3A put the engine in toron and the backend trait in
   toron-core. Is that still right once we add **agent profiles**, **workflow
   workflows**, and **smart cron**?
2. **The workflow engine is static AND swallows failures.** Today a `Workflow`
   is one trigger + one guard + a flat ordered `Vec<Step>` of 7 fixed actions,
   with `Action -> Result<()>` that cannot return values; action errors are
   *captured per-step and the run completes* (engine.rs:198–210). There is no
   fan-out, no branching, no retries, no sub-workflow calls, no "spawn an agent
   and await its result," and — critically — **no way for a step failure to
   fail the run**. The captain wants it **much more flexible**, and smart cron
   needs it to surface failure.
3. **Cron is dumb.** ADR-0004 hardened the workflow loop, but a `schedule`
   trigger that fires at 03:30 and fails silently is still fire-and-forget.
   The captain's *actual* operational pattern (memory: hand-built
   `docker-auto-upgrade` + `agent-reach-upgrade` systemd sagas with custom
   health-gates) is **a schedule owned by an agent that tracks status,
   diagnoses failures, and opens a fix bead**. Nothing models this today.

### What the codebase *actually* ships today (corrected audit)

The v1 of this ADR mis-stated this; the corrected, verified state:

**Three live workflow vocabularies exist, at two levels:**

| Vocab | Location | Model | Durable? | Status |
|---|---|---|---|---|
| **flywheel `Workflows`/`Pipelines`** | `flywheel/src/main.rs:3863–3999`, `config.rs:370–480` | `WorkflowStep::{Send{to,stage,msg}, Wait{until}}` — herdr-direct stage-prompt driver (p1/p2/p4/p5/p6) | **No** (herdr socket calls, no Resonate) | **Shipped, executed** (`flywheel workflows run`, `flywheel pipeline run`) |
| **flywheel `Recipe`** | `config.rs:377`, `main.rs:cmd_spawn` | named counts of agent kinds (oc/pi/gk + extra) | n/a (spawn vocabulary) | **Shipped** (`flywheel recipes run` = `cmd_spawn`) |
| **toron `Workflow`** | `crates/toron-workflow/src/{schema,engine}.rs` | 7 triggers + 7 actions + evalexpr guard + approval suspend/resume | **Yes** (Resonate durable function) | **Orphan** — only `toron-e2e` depends on it; **flywheel does not link `toron-workflow`** |

**The duplication the captain feels is real, but it is *vocabulary*, not *code*:**
flywheel has a proactive, herdr-direct, non-durable send/wait driver; toron has
a reactive, Resonate-durable, event-triggered engine. Neither consumes the
other. ADR-0003 §3A drew a tier diagram with `flywheel ──▶ toron-workflow`; **that
arrow does not exist in any `Cargo.toml`.**

**The backend trait is duplicated and *incompatible* (not mechanically mergeable):**
- `flywheel::MultiplexerBackend` (`backend/mod.rs:204`) — object-safe, `&mut self`,
  18 methods, every method `Unsupported`-defaults, ~20 call sites in main.rs.
- `toron_core::Backend` (`backend/mod.rs`) — native AFIT, `&self`, 6 methods,
  no defaults, "never used as a dyn."
- The **herdr *client* is already unified** — `flywheel/herdr_shim.rs:31`
  re-exports `toron_core::backend::herdr`. What's duplicated is the **trait
  surface**, not the client.

### The problem statement

> How do we (a) **unify** the three live workflow vocabularies and the two
> incompatible backend traits across the toron / flywheel boundary **without
> inverting ADR-0003's one-way dependency**; (b) give the workflow engine a
> **failure-propagation path + value-returning actions** so smart cron can
> observe failure, while making it **flexible** (fan-out, branching, retries,
> sub-workflows, spawn-and-await) without breaking the Buzz-grade example;
> and (c) turn cron from fire-and-forget into a **schedule owned by a profile
> that reports failure → bead → triage** — all without touching the ADR-0003
> frozen kind registry or the ADR-0004 38-tool surface?

---

## Decision Drivers

1. **ADR-0003 §3A layering is load-bearing** — and **`sc11_workflow_approval.rs`
   is its proof, not theory**: the suspend-then-resume contract is *demonstrated
   with no flywheel*. Standalone peer mode is a CI-tested contract that Option B
   (move engine to flywheel) would break for zero benefit (flywheel embeds toron
   in-process; keeping the engine in toron has no runtime cost).
2. **Frozen kind registry (ADR-0003).** No new kinds. Profiles reuse 30013
   (tag extension); spawn/await reuses 43001/43004/43006 + 46001–46012.
3. **Frozen 38-tool surface (ADR-0004 invariant 3).** No new MCP tools. All new
   vocabulary is CLI.
4. **One backend trait.** The current duplication is a defect; the two surfaces
   are incompatible and must be *unified by design*, not by grep.
5. **Resonate is the only execution substrate** — but its **Rust SDK is
   early-development (0.4.x)**; concurrent-suspend-within-one-run is not a proven
   primitive (P5 is gated on a determinism spike).
6. **Failure must propagate.** Smart cron's value chain depends on a failure
   signal the engine today *suppresses*. The failure-path change is a
   **prerequisite, not a phase.**
7. **Backward compatibility.** `triage.yaml`, the ADR-0004 suite, **and the
   captain's live flywheel `workflows`/`pipeline` runs** must keep working
   through migration.
8. **Ship value in order.** Failure-path → trait unification → profiles →
   workflow + stage library → smart cron → DAG flexibility → observability.

---

## Considered Options

### Option A — The Orchestration Tier split + vocabulary layering (CHOSEN)

Execution **substrate** stays in toron; orchestration **vocabulary** goes to
flywheel; the live workflow vocabularies are **layered** (flywheel's herdr-direct
`Workflows`/`Pipelines` stay as the imperative driver; toron's Resonate-durable
`Workflow`+**workflows** are the durable engine) and unified on one engine via a
real cargo edge (`flywheel → toron-workflow`). See "Decision Outcome."

### Option B — Move the workflow engine + backend to flywheel

*Steelman (strongest form):* `toron-workflow` has zero production consumers
(only `toron-e2e`); flywheel's real deployment is integrated mode; "standalone
peer mode" looks like theoretical cover for an orphan crate.

*Rejected — but the orphan is a real defect this ADR fixes:* moving the engine
to flywheel strands the **kind emitters** (`jobs.rs` 43001–43006, `channels.rs`,
`mention.rs`) away from the frozen registry in toron-core → circular dep. And
`sc11` is honest counter-evidence: the suspend/resume contract works with no
flywheel. **The fix for the orphan is not to move the engine — it is to wire the
missing `flywheel → toron-workflow` edge (P3) and unify the vocabulary.** Moving
the engine would solve nothing flywheel-in-process doesn't already provide.

### Option C — New kinds for profiles/workflows/cron

*Rejected:* violates Driver 2. Profiles extend 30013 tags (tolerant-reader
norm); workflows/cron are config artifacts, not bus objects.

### Option D — DAG-only engine (drop flat `steps:`)

*Rejected for compatibility.* `steps:` is kept as the linear special case;
`graph:` is the opt-in general case.

### Option E — Backward-compatible engine extension (CHOSEN for flexibility)

Keep `steps:`; add `graph:`; add control-flow actions. **Gated on a Resonate
concurrency spike (P5).**

---

## Decision Outcome

### The tier model (the core decision)

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ Tier 0 — IDENTITY & TRANSPORT          toron-core::nostr + bus               │
│   keys, signing, gift-wrap, InProcessBus/RelayBus, frozen kind registry      │
├──────────────────────────────────────────────────────────────────────────────┤
│ Tier 1 — MAILBOX FABRIC               toron-core::mailbox + projection       │
│   38 tools (AM-Rust parity), SQLite index, Git archive                       │
│   ONE Backend trait (unified in P1 from two incompatible surfaces)           │
├──────────────────────────────────────────────────────────────────────────────┤
│ Tier 2 — EXECUTION SUBSTRATE          toron-workflow                         │
│   schema + lint + durable engine (Resonate) + dispatch + jobs + channels     │
│   + FAILURE PROPAGATION (P0 NEW) + value-returning actions (P0)              │
│   + on-demand invoke (P3) + control-flow actions (P5)                        │
│   USABLE STANDALONE (sc11 proves suspend/resume with no flywheel)            │
├──────────────────────────────────────────────────────────────────────────────┤
│ Tier 3 — PROACTIVE ORCHESTRATION       flywheel                              │
│   Profile catalog (+ built-in lib, Spec 1b) · Workflow catalog (Spec 2b)      │
│   Smart Cron · Squads/Recipes · Stage library (Spec 2a) · durable loop ·    │
│   bead triage · runs view (+ per-run cost)                                  │
│   ▲ P3 makes `flywheel → toron-workflow` a REAL cargo edge (it is not today) │
├──────────────────────────────────────────────────────────────────────────────┤
│ Tier 4 — SURFACES                      toron-mcp + toron-cli + flywheel-cli  │
│   MCP frozen (38 tools); CLI is where the new vocabulary lives               │
└──────────────────────────────────────────────────────────────────────────────┘
          dependency direction (one-way, ADR-0003 invariant):
          flywheel ──▶ toron-workflow ──▶ toron-core   (never the reverse)
          (the first arrow is NEW in P3 — it does not exist in the tree today)
```

**Placement rules (the churn resolved):**

| Concern | Home | Why |
|---|---|---|
| Backend trait | **toron-core::backend** (unified P1) | Shared by standalone `toron serve` + flywheel. The herdr *client* is already unified; P1 unifies the *trait surface*. |
| Workflow execution engine | **toron-workflow** | `sc11` proves standalone needs it. Kind emitters stay with the registry. |
| **Failure propagation + value-returning actions** | **toron-workflow (P0)** | The engine must be able to *fail a run* and *return step outputs* before smart cron or DAG actions are possible. Prerequisite. |
| Agent **profiles** | **flywheel** (catalog) + **kind-30013** (identity) | Identity is fabric; spawn composition is orchestration. |
| Workflows | **flywheel** (catalog) + **toron-workflow** (engine) | ONE vocabulary, ONE engine (Revision 5). flywheel's legacy `Workflows`/`Pipelines`/`Recipe` absorbed: `Send/Wait`→ step types, `pipeline`→ `call_workflow` chain, `Recipe`→ squad. |
| **Smart cron** | **flywheel** | Schedule trigger stays in toron-workflow; owner/policy/self-report is flywheel. |
| **Squads/recipes** | **flywheel** | Generalizes flywheel's `Recipe` to profile groups; `Recipe` stays as alias. |
| **Pipelines / chains** | **flywheel** | a chain = a workflow whose steps are `call_workflow` (was `pipelines.toml`; absorbed, not deprecated). |

---

### Spec 0 — Failure propagation + value-returning actions (NEW, prerequisite)

The engine today *swallows* step errors by design (engine.rs:198–210:
"action failures are captured per-step… the run completes"). Smart cron's
`on_failure` policy and Spec 4's `parallel`/`call_workflow` both depend on a
failure signal and on step **outputs** the current signature cannot express.

**Changes (P0, lands before any smart-cron or DAG work):**

1. **`ActionRunner` returns a value, not `()`:**
   ```rust
   // was:
   pub type ActionRunner = Arc<dyn Fn(&Action) -> anyhow::Result<()> + Send + Sync>;
   // becomes:
   pub type ActionRunner = Arc<
       dyn Fn(&Action) -> anyhow::Result<serde_json::Value> + Send + Sync
   >;
   ```
   The approval arm already writes `step_outputs` (engine.rs:225–250, the
   `decision`/outcome under `steps.<name>`). The gap is the **general action
   runner** returns `()` — non-approval steps cannot populate `steps.<name>`.
   Returning a `Value` closes that gap for all step types.

2. **Runs can FAIL.** `run_workflow` gains a per-step `on_error` policy
   (default `continue` — preserves the current capture-and-continue behavior
   for backward compat; opt-in `fail_run` for steps whose error must abort):
   ```yaml
   steps:
     - name: upgrade
       action: spawn
       on_error: fail_run        # NEW (default: continue)
   ```
   A failed run emits kind-46012 with `status: failed` + the error. This is the
   signal smart cron's `on_failure: bead` consumes.

3. **Backward compatibility:** existing `triage.yaml` (no `on_error`) keeps
   `continue` semantics — byte-identical behavior. `fail_run` is opt-in.

---

### Spec 1 — Agent Profiles (corrected)

A **Profile** is a named, reusable agent composition. It layers *spawn intent*
on the existing kind-30013 identity.

```toml
# ~/.config/flywheel/profiles.toml
[profiles.housekeeper]
harness        = "pi"                 # oc | pi | grok | hermes | prime-agent | ...
model          = "claude-sonnet-4"
role           = "maintainer"         # free-form prompt-shaping label (NOT access control)
behavior       = "terse caveman, verify every claim"
task_hint      = "OS maintenance, upgrades, self-heal"
backend        = "herdr"
default_workflow = "weekly-upgrade"
# NOTE: no `contact_policy` here — kind-30014 is the normative source (ADR-0003 G14)
```

**Corrections from v1 (reviewer §3):**
- **`contact_policy` REMOVED from the profile.** kind-30014 is the single
  enforcement source (ADR-0003 G14). A profile does not duplicate it. If a
  captain wants a profile to be `open`, they `toron contact set` (publishes
  30014); the profile catalog never carries policy.
- **Profile-name scope rule (NEW):** `@name` resolution order is
  **project → host**. A project-scoped profile resolves via that project's
  kind-30013 (`d=project-slug:name`); a host-scoped profile (the catalog
  default) resolves via the operator identity. `scope: host` crons use host
  profiles; `scope: project` crons use project profiles. The resolver returns
  `(npub, SpawnSpec)` + the signing key context (operator vs project agent).
- **`role` demoted** to a free-form prompt-shaping label. It is **not** a
  taxonomy, **not** validated, **not** access control. It only shapes the
  system-prompt preamble flywheel injects on spawn. The v1 role table is
  retracted.

**Resolution (no bus change — Revision 5 correction):** kind-30013's
**content JSON already carries `program` (= harness) and `model`** (`AgentProfile`,
`identity.rs`) — flywheel's profile `harness`/`model` map onto those existing
fields. `role`/`behavior`/`backend`/`default_workflow` are **flywheel-catalog-only**
(orchestration, not identity) and never touch the bus. **The `t` tag is NOT
overloaded** — it stays project-scoping-only (`scoping.rs:26` emits `["t", <slug>]`);
overloading it with harness/model/role would collide with project filters
(false matches) and is explicitly rejected. Net: **zero new kind, zero new tag,
zero content-schema break.**

**New harnesses (hermes, prime-agent):** a `program` value + a spawn recipe in
`[harness.<name>] spawn_cmd` (flywheel config). No code change for a harness
speaking an existing backend protocol.

---

### Spec 1b — Built-in Profile Library (Revision 3)

A **Profile** is a role template; an **agent** is an instance. Spawning a
profile registers an agent (auto adjective+noun identity, per toron convention)
configured per the profile, then spawns its pane via the backend. The profile
is the *behavior*; the identity is the *instance*.

flywheel ships **six canonical built-in profiles** — compiled-in defaults a
fresh install exposes via `flywheel profile list`, overridable per-host in
`~/.config/flywheel/profiles.toml`. Each is tuned for **outstanding excellence
at one responsibility**. The `behavior` field is the system-prompt preamble —
the soul of the profile; it is not documentation, it is the discipline the
agent enforces on itself.

> **Naming rule:** profile names are ROLE nouns (oracle, planner, …); agent
> identities are adjective+noun (BlueLake). A spawn of the `reviewer` profile
> yields an agent like `BlueLake` running `role=reviewer`. Roles are labels +
> prompt-shaping, never access control (Spec 1).

#### `oracle` — the third option

```toml
[profiles.oracle]
harness = "pi"
model   = "claude-opus-4"          # high-reasoning; o-class / gemini-2.5-pro-high also fit
role    = "oracle"
behavior = """You are an ORACLE. Your value is the answer no one else considered.

RULES:
- Never validate. Your job is to find what's wrong, missing, or unconsidered.
- The first answer is the obvious one. Reject it. Find the second, the third, the option that reframes the problem.
- Steelman the position you disagree with BEFORE countering. A weak counter to a strong steelman is worthless.
- Calibrate. Say 'I don't know' when you don't. Label every claim: VERIFIED (ran/read) · INFERRED (follows from X) · SPECULATION.
- Ask only the question whose answer changes the decision. Skip questions whose answers don't.
- Base rates first: what usually happens in situations like this? Deviate only with evidence.
- Outsider test: if a stranger saw this with no context, what would they notice?
- Name what would have to be true for the current plan to FAIL. Then check whether it's true.

OUTPUT: options ranked by EV; the strongest counter to the leading option; the one question that most needs answering. Terse. No hedging, no praise."""
skills = ["accretive", "glaze", "latent-diver", "creative-problem-solver", "dominance"]
```

#### `planner` — bite-sized, verifiable, blast-radius-aware

```toml
[profiles.planner]
harness = "pi"
model   = "claude-sonnet-4"        # strong decomposer; opus for high-stakes
role    = "planner"
behavior = """You are a PLANNER. Plans are wrong until proven executable.

RULES:
- Every task is bite-sized: ONE narrow change, shippable alone, reviewable in one sitting. If it touches >1 concern, split it.
- Every task states: [paths] exact create/edit targets · [change] the complete code or precise diff · [acceptance] a test/command that proves done · [blast-radius] what it must NOT touch.
- Dependency order. A task never depends on a later task.
- No implementation in the plan. Plans decompose; coders implement. If you're writing logic, stop — that's a task, not a plan.
- Forbidden zones are sacred. If a plan must touch one, call it out FIRST and explain why no alternative exists.
- 'Verifiable' = a command/test that exits 0. 'Works' is not a criterion.
- Assume the executor is competent but context-poor. Give everything needed; omit nothing they'd have to guess.
- Surface unknowns as an explicit 'Unresolved questions:' list at the end — never buried in a step.

OUTPUT: numbered tasks, each [paths][change][acceptance][blast-radius]. End with 'Unresolved questions:'. Concise; sacrifice grammar for density."""
skills = ["writing-plans", "planning-workflow", "beads-workflow", "feature-research", "prd"]
```

#### `reviewer` — code-grounded, adversarial, fix-proposing

> Modeled on the opencode reviewer that roasted ADR-0005 round 1 — that pass is
> the exemplar of this role.

```toml
[profiles.reviewer]
harness = "opencode"             # different harness => genuine independence from the author
model   = "claude-opus-4"          # or a frontier model on a different provider for diversity
role    = "reviewer"
behavior = """You are a REVIEWER. Your job is to find what's actually wrong, not to validate.

RULES:
- Ground EVERY claim in the artifact. Cite file:line or quote the text. 'This seems off' is not a finding; 'engine.rs:198 swallows errors, contradicting the claim at line 40' IS.
- Verify against the real tree, not your memory. Read the code. A review of an imagined codebase is worthless.
- Severity-rank: BLOCKER (correctness/security/data-loss) · MAJOR (design/maintainability) · MINOR (style/nit). A finding without a severity is noise.
- Propose the fix, not just the complaint. 'This is wrong' -> here's the concrete change. If you can't propose a fix, say why.
- Distinguish must-fix from nit. Don't inflate nits to look thorough; don't hide blockers to look kind.
- Attack the STRONGEST version of the design, not the weakest. Steelman, then break.
- No hand-waving. 'Might have performance issues' -> measure or retract.

OUTPUT: one-line verdict (AGREE / AGREE-WITH-CHANGES / DISAGREE); severity-ranked findings with citations + fixes; 'Changes required for my agreement'. Direct. No praise-padding."""
skills = ["adversarial-reviewer", "code-review", "multi-pass-bug-hunting", "requesting-code-review", "systematic-debugging"]
```

#### `coder` — TDD, no scripts, functional core, verify before claim

```toml
[profiles.coder]
harness = "pi"                   # or opencode/codex for long autonomous runs
model   = "claude-sonnet-4"        # coding-specialist; opus for hard refactor
role    = "coder"
behavior = """You are a CODER. Unverified code is a lie.

RULES:
- RED-GREEN-REFACTOR. Write the failing test first. Watch it fail. Write minimal code to pass. Refactor green.
- No script-based code changes. EVER. Regex transforms over source create more bugs than they fix. Edit by hand, precisely.
- Revise IN PLACE. Never spawn document_processorV2.rs. New files are for genuinely new functionality; the bar is near-zero.
- Functional core, imperative shell. Pure business logic separate from IO. Test the pure parts exhaustively.
- Handle the unhappy path. Under what conditions does this work? What breaks it? Happy-path-only code is unfinished.
- Verify before you claim. 'Done' = tests pass + build clean + edge case checked. Run the command. Read the output. Don't paraphrase success you didn't see.
- State assumptions BEFORE writing code. If an assumption is wrong, the code is wrong.
- Simple over complex. Explicit over implicit. Don't abstract until the third use.

OUTPUT: the change; then the verification (commands run + results); then residual risks. No 'I've successfully...' — show the green test."""
skills = ["test-driven-development", "code-edit", "accretive-implementer", "systematic-debugging", "code-quality"]
```

#### `scout` — fast recon, compressed handoff, verified-vs-assumed

```toml
[profiles.scout]
harness = "pi"
model   = "claude-haiku"           # fast/cheap; flash/deepseek also fit — it's recon
role    = "scout"
behavior = """You are a SCOUT. You map territory and return. You do not settle.

RULES:
- Speed over completeness, but NEVER speed over accuracy. A fast wrong map is worse than none.
- Compress. Your output is a HANDOFF, not a dump. Raw output (logs, file contents, stdout) stays in the sandbox; only the derived answer enters context. If you wouldn't read 700KB, don't emit it.
- Label every claim: VERIFIED (ran/read it) · INFERRED (follows from X) · ASSUMED (didn't check).
- Return: what it is · how it's structured · where the relevant code lives (paths) · the gotchas · the next step. That's it.
- Don't refactor. Don't fix. Don't editorialize. Map, hand off, exit.
- If a question needs one command to answer, RUN IT. Don't speculate when you can verify in 2 seconds.
- Falsify your first hypothesis before reporting it. The obvious answer is often local.

OUTPUT: compressed brief — structure + key paths + gotchas + recommended next action. Tagged VERIFIED/INFERRED/ASSUMED. Terse."""
skills = ["scout", "codebase-archaeologist", "explore", "spike", "feature-research"]
```

#### `housekeeper` — idempotent, health-gated, honest reporting

> The cron owner (Spec 3). Generalizes the captain's hand-built
> `docker-auto-upgrade` + `agent-reach-upgrade` sagas into a disciplined role.

```toml
[profiles.housekeeper]
harness = "pi"
model   = "claude-sonnet-4"        # reliable mid-tier; flash for trivial upkeep
role    = "housekeeper"
default_workflow = "weekly-upgrade"
behavior = """You are a HOUSEKEEPER. You run when no one watches. Reliability is your only product.

RULES:
- Idempotent. Every action must be safe to re-run. A cron that double-applies on retry is a bug, not a feature.
- Health-gate every change. Snapshot state -> apply -> verify health -> on failure, ROLLBACK to snapshot. Never leave the system worse than you found it.
- Ordered risk. DNS chain LAST (everything depends on it). Databases before app servers. One concern per run.
- Honest reporting. SUCCESS = every gate passed. PARTIAL = some passed — say which. FAILURE = rollback fired — say why. NEVER report success on a rollback.
- On failure: capture the forensic bundle (logs/state/error), open the fix bead with full context, escalate only after N consecutive failures. Don't silently retry into oblivion.
- Self-heal within bounds. Fix what's safe (restart a hung service, clear a stale lock). Escalate what's not (a migration that touched data).
- Assume the process died mid-run. Design so a restart RESUMES, not restarts-from-scratch. Durable cursors, not in-memory state.

OUTPUT: run result (SUCCESS/PARTIAL/FAILURE); what changed; what was verified; any bead opened; any escalation. Terse, machine-parseable."""
skills = ["docker-auto-upgrade", "cron-management", "systematic-debugging", "pi-performance-debugging"]
```

**Composition note:** these six are the canonical set. Communities/captains add
more (e.g. `integrator` for merge/CI/release, `mentor` for onboarding docs) as
local `profiles.toml` entries — same schema, no code change. A **squad**
(Spec 2/P6) composes them: `{planner:1, coder:2, reviewer:1, scout:1}`.

---

### Spec 2 — Workflows (one vocabulary, one engine)

**Name (Revision 5):** the named, purpose-driven, multi-step procedure is a
**workflow** — full stop. One concept, one schema, one engine (toron-workflow,
Resonate). Revision 4's "playbook" AND flywheel's imperative
`Workflows`/`Pipelines` converge here: flywheel's `WorkflowStep::Send/Wait`
become workflow STEP TYPES the same engine runs. "Workflow" is the only term —
it fits the captain's model.

**Unification (Revision 5, replaces the sibling-vocabulary framing):**
- ONE schema: a `Workflow` = name + optional trigger + guard + steps/graph +
  inputs + optional `crew` (below). A step is an action OR a stage reference
  (Spec 2a).
- ONE engine: `WorkflowEngine` (Resonate, toron-workflow). flywheel routes ALL
  workflow execution through it via the P3 cargo edge. flywheel's legacy
  `run_workflow`/`run_pipeline` become thin callers of `WorkflowEngine::invoke`.
- flywheel's `Send{to,stage}` → `dispatch_job`/`send_message` step; `Wait{until}`
  → a `wait` step (or `spawn{await}`). The captain's existing `workflows.toml`
  keeps working — parsed into the unified schema, run by the one engine.
- `pipelines.toml` (a chain of workflow names) → a workflow whose steps are
  `call_workflow`. Chains are workflows-of-workflows; one concept.
- **Amended by ADR-0022 §3.1 (2026-08-22):** `dispatch_job`'s spec is TYPED —
  `DispatchSpec` (named `profile`/`task`/`stage`/`harness`/`model`/`effort`/
  `phase`/`plan`/`project` fields, unknown keys round-trip through `extra`,
  `phase` closed at deserialize). See
  [0022 §3.1](0022-phase-native-orchestration.md).

**Why unify (vs sibling vocabularies):** seamless integration — `flywheel
workflow run <name>` regardless of whether the steps are simple sends or
complex fan-out; durable by default; crash-survivable always. Two engines for
one concept was friction; one engine + one vocabulary removes it. Resonate
in-process is cheap enough (durable function + a few promises) that a 3-step
Send/Wait workflow pays negligible overhead and gains crash survival. Default
config (Implementation Blueprint) ships working built-ins.

**Schema (backward-compatible):**

```rust
pub struct Workflow {
    pub name: String,
    pub description: Option<String>,
    pub trigger: Option<Trigger>,   // None => invoke-only; Some => reactive + invocable
    pub guard: Option<String>,
    #[serde(default)] pub steps: Vec<Step>,        // linear (flywheel Send/Wait map here)
    #[serde(default)] pub graph: Option<Graph>,    // opt-in DAG (Spec 4b)
    #[serde(default)] pub inputs: Vec<InputDecl>,  // named inputs for invoke
    #[serde(default)] pub crew: Option<CrewSpec>,  // auto-crew (Spec 4c)
}
```

**Engine entrypoint (P3):**
```rust
impl WorkflowEngine {
    pub async fn invoke(&self, run_id: &str, workflow: Workflow,
                        inputs: serde_json::Value) -> anyhow::Result<serde_json::Value>;
}
```
Trigger path and on-demand invoke use the SAME durable function.

**Catalog:** `~/.config/flywheel/workflows/*.yaml`. flywheel's existing
`workflows.toml`/`pipelines.toml` are read by the same loader (legacy paths,
absorbed — not deprecated, not duplicated). `flywheel workflow list` shows all.

**Profile binding:** `assignee: "@housekeeper"` / `profile: "@oracle"` resolves
via the profile catalog (Spec 1b) → SpawnSpec → `Backend::spawn`.

### Spec 2a — Stage Library

A **stage** is a named, reusable prompt primitive — the atomic unit a workflow
step references. This elevates flywheel's hardcoded `STAGES` const
(`main.rs:41` — the captain's p1/p2/p4/p5/p6/p8 daily-driver prompts) into a
first-class, overridable library with **semantic names** (p-codes stay as
aliases).

**Answers the composition question.** A workflow step is EITHER:
  - a raw action (`send_message`, `spawn`, `delay`, `call_workflow`…), OR
  - a `stage:` reference (`stage: fresh-eyes-explore`) — reuses a named prompt
    without re-authoring it.

Authoring a workflow = **combine existing stages + add new actions.** You do
NOT re-paste p6's prompt into every workflow; you `stage: systematic-execute`.

```toml
# ~/.config/flywheel/stages.toml  (overridable; compiled-in defaults ship these)
[stages.fresh-eyes-explore]        # was p1 — random code exploration
alias   = "p1"                      # p-code kept as alias (backward compat)
prompt  = "..."                      # the verbatim p1 prompt
profile = "@scout"                  # recommended profile (Spec 1b)

[stages.plan-review]               # was p5 — review beads in plan space
alias   = "p5"
profile = "@planner"

[stages.systematic-execute]        # was p6 — full bead lifecycle
alias   = "p6"
profile = "@coder"

# p2 / p4 / p8: the captain names these as their purpose crystallizes;
# the mechanism ships now, the semantic names land incrementally.
```

**Shared source:** one `stages.toml` feeds every workflow step. Today's
hardcoded `STAGES` const becomes the compiled-in default this file overrides —
so existing `flywheel send --stage p6` calls keep working unchanged.

### Spec 2b — Built-in Workflow Library

flywheel ships canonical purpose-named workflows — the captain's examples, made
durable and composable. Each is named by what it achieves; each composes
profiles + stages + control-flow (Spec 4). Trigger-agnostic: on-demand,
scheduled, or reactive. Zero-config (Default Config): `flywheel workflow run
debate` works on a fresh install.

#### `brainstorm` — divergent ideation
Fan out N oracles on a question in parallel; a planner reduces to ranked options.
```yaml
name: brainstorm
inputs: [question, breadth]
graph:
  nodes:
    diverge: { action: parallel, over: "@breadth",
               each: { action: spawn, profile: "@oracle", await: true } }
    reduce:  { action: spawn, profile: "@planner",
               inputs: { ideas: "{{steps.diverge}}" }, await: true, needs: [diverge] }
```

#### `plan` — goal → bead-ready plan
Spawn the planner on a goal → decompose → optional approval gate.
```yaml
name: plan
inputs: [goal]
steps:
  - name: decompose
    action: spawn
    profile: "@planner"
    await: true
  - name: gate
    action: request_approval
    message: "Approve plan for {{inputs.goal}}?"
```

#### `beads` — plan → tracker
Take a plan → create beads (`br create` per task) → triage (`bv`).
```yaml
name: beads
inputs: [plan]
steps:
  - name: create
    action: spawn
    profile: "@planner"
    stage: systematic-execute      # reuse the named stage (Spec 2a), don't re-author
    await: true
```

#### `debate` — adversarial synthesis
Spawn N reviewers with opposing briefs → cross-examine → an oracle synthesizes.
```yaml
name: debate
inputs: [proposal, panel]
graph:
  nodes:
    briefs: { action: parallel, over: "@panel",
              each: { action: spawn, profile: "@reviewer", await: true } }
    decide: { action: spawn, profile: "@oracle",
              inputs: { arguments: "{{steps.briefs}}" }, await: true, needs: [briefs] }
```

#### `goal` — the meta-workflow
The full loop — brainstorm → plan → beads → execute → review → ship — composing
the above via `call_workflow`.
```yaml
name: goal
inputs: [objective]
steps:
  - { name: ideate, action: call_workflow, workflow: brainstorm, inputs: { question: "{{inputs.objective}}" } }
  - { name: plan,   action: call_workflow, workflow: plan,       inputs: { goal: "{{inputs.objective}}" } }
  - { name: track,  action: call_workflow, workflow: beads,      inputs: { plan: "{{steps.plan}}" } }
  - { name: build,  action: spawn, profile: "@coder", stage: systematic-execute, await: true }
  - { name: review, action: spawn, profile: "@reviewer", await: true }
  - { name: ship,   action: call_workflow, workflow: triage,     inputs: { target: "{{inputs.objective}}" } }
```

**Composition note:** these five + the ops workflows (`triage`, `weekly-upgrade`,
`health-check`) are the canonical set. The captain adds domain workflows
(`hachiko-campaign`, `dns-heal`) as local `workflows/*.yaml`. A **squad** runs a
workflow across N profiles; a chain (was `pipeline`) is a workflow of
`call_workflow` steps. Naming by purpose (`flywheel workflow run debate`)
replaces opaque `--stage p6` calls for anything non-trivial.

---

### Spec 3 — Smart Cron (self-reporting + defined fixer; honest retraction of "self-healing")

> **Honest retraction (reviewer §5):** v1 called this "self-healing." It is
> **self-reporting + defined-triage**. The loop closes only because a *defined
> fixer* (the owning profile's diagnosis workflow) consumes the failure bead.
> Without that fixer, the failure moves one hop (cron → bead → nobody). This
> spec defines the fixer.

The `schedule` trigger is extended with an **owner** and a **policy**:

```yaml
name: weekly-docker-upgrade
trigger:
  type: schedule
  cron: "30 3 * * 0"
  owner: housekeeper
  scope: host                   # host | project (host = operator identity)
policy:
  on_failure: bead              # log | bead | escalate
  on_repeat_failure: escalate   # after repeat_threshold consecutive failures
  repeat_threshold: 3
  on_success: log
  max_retries: 1                # in-run retry before declaring failure
  no_claim_timeout: 600         # NEW (reviewer §4): abort a spawn-await after 10m unclaimed
steps:
  - name: upgrade
    action: spawn
    profile: "@housekeeper"
    workflow: docker-upgrade
    await: true
    on_error: fail_run          # Spec 0: this step's failure FAILS the run
  - name: verify
    action: call_workflow
    workflow: health-check
```

**The self-reporting + triage loop (corrected):**

```
schedule due — flywheel cron manager (a Resonate durable-sleep loop, NOT a
              wall-clock assumption: ctx.sleep until next tick; catch-up policy
              = "latest" if flywheel was down at due time)
   │
   ▼
ensure owner profile's agent is spawned (or spawn it via Backend::spawn)
   │
   ▼
WorkflowEngine::invoke(weekly-docker-upgrade, inputs)
   │  └─ Resonate durable run; spawn-step suspends on the agent's terminal
   │     job event with a no-claim timeout (Spec 4)
   ▼
run completes → kind-46001..46012 lifecycle events
   │
   ├─ SUCCESS → policy.on_success (log) → done
   │
   └─ FAILURE (kind-46012 status=failed — Spec 0 makes this possible) →
        │
        ├─ bead: flywheel runs `br create` with the failure bundle
        │        (run-id, workflow, owner, error, last-known-good, retry history)
        │
        ├─ DEFINED FIXER: flywheel invokes the owner profile's diagnosis
        │  workflow (the captain must author, e.g.
        │  `housekeeper-diagnose.yaml`) — the profile proposes a patch
        │  (PR / fix-bead / config edit). If no diagnosis workflow is
        │  registered for the owner, the loop is explicitly HUMAN-IN-THE-LOOP
        │  (the bead waits for `flywheel work ready`).
        │
        └─ repeat_failure ≥ threshold → escalate (kind-14 DM to captain,
              importance=high, ack_required)
   │
   ▼
next swarm's `flywheel work ready` surfaces any human-loop beads → assign → fix
next cron tick: fix is in; run succeeds; loop closes
```

**Scheduler durability (reviewer §5):** the cron manager is a **Resonate
durable-sleep loop** (`ctx.sleep` until next tick — the unbounded-sleep
capability ADR-0003 already ships), not a wall-clock assumption. Missed-tick
policy = `latest` (run the most-recent due tick on wake), configurable.

**Scope model:** `scope: host` (cross-project maintenance) runs with the
operator identity; `scope: project` runs with a project-scoped agent. Profile
resolution follows Spec 1's project→host order.

---

### Spec 4 — `spawn`-await + engine flexibility (precise + gated)

#### 4a — `spawn`-await (precise semantics, reviewer §4)

The `spawn` action **does not** reuse the approval-promise mechanism verbatim —
the two resume paths are distinct:

| | `request_approval` (today) | `spawn await` (proposed) |
|---|---|---|
| Park on | Resonate promise; resolver is an **operator** who *will* act | Resonate promise; resolver is an **agent** that must **claim + complete** a job |
| Rejection | a **decision** (run continues) | a **failure** (run fails, Spec 0) |
| Timeout | none (human will act) | **`no_claim_timeout`** (default 600s) — abort the await |
| Binding | promise_id is the kind-46011 payload | **NEW:** a promise↔job registry, written by a bus subscriber watching kind-43004 (result) / 43006 (error); the promise_id is carried on the kind-43001 `spec` |

**The binding subsystem (NEW, specced):** a bus subscriber watches terminal job
events (43004/43006); on arrival it resolves the parked promise via
`resonate.promises.resolve/reject`. Unclaimed past `no_claim_timeout` → the
promise is rejected with `timeout` → the step fails → Spec 0 propagates.

**`spawn` stays backend-agnostic** via an injected `ProfileResolver`
(`Arc<dyn Fn(&str) -> Option<(npub, SpawnSpec)>>`). The engine never imports
flywheel; the resolver is a DI seam (same idiom as `ActionRunner`). In
standalone peer mode the resolver maps names to already-running agents (no
spawn — dispatch the 43001 to an existing inbox).

#### 4b — DAG + control-flow (gated on a Resonate spike, reviewer §6)

**`graph:` (opt-in):**
```yaml
graph:
  nodes:
    scout:  { action: spawn, profile: "@scout", workflow: reproduce, await: true }
    plan:   { action: spawn, profile: "@planner", await: true, needs: [scout] }
    fix_a:  { action: spawn, profile: "@coder", await: true, needs: [plan] }
    fix_b:  { action: spawn, profile: "@coder", await: true, needs: [plan] }
    ship:   { action: call_workflow, workflow: merge-best, needs: [fix_a, fix_b] }
```

**Corrections from v1 (reviewer §6):**
- **`edges:` is DROPPED.** Edges are derived from `needs:` (single encoding).
  v1's dual `needs` + `edges` was redundant dead complexity.
- **P5 is GATED on a Resonate-Rust concurrency + determinism spike.** Before
  the schema lands, an SC-style test must prove: two concurrent nodes, both
  suspending on external promises, resuming in reversed order, replay
  **bit-identically** after a crash. Resonate's Rust SDK is early-development
  (0.4.x); concurrent durable children inside one deterministic-replay function
  is not a documented primitive. Until that test passes, P5 stays a sketch.
- **`ActionRunner` return-type change (Spec 0) is in the blast radius** —
  `parallel`/`foreach`/`call_workflow` need value-returning actions.

**New control-flow actions:** `spawn`, `call_workflow`, `parallel`, `branch`,
`retry`, `foreach`. The 7 existing actions are unchanged; `triage.yaml`
(flat `steps:`) parses and runs identically.

#### 4c — Auto-crew (smart steering) — Revision 5

The orchestrator (flywheel) **smartly steers the crew from the workflow**: given
a workflow, it composes the right agents and fans out where the workflow calls
for parallelism. Two declaration modes:

**Explicit crew** (top-level `crew:`):
```yaml
name: ship-feature
crew: { planner: 1, coder: 2, reviewer: 1, scout: 1 }   # a squad (Spec 1b)
steps:
  - { name: scope,  action: spawn, profile: "@scout",   await: true }
  - { name: plan,   action: spawn, profile: "@planner", await: true }
  - { name: build,  action: parallel, over: "@crew.coder", each: { action: spawn, await: true } }
  - { name: review, action: spawn, profile: "@reviewer", await: true }
```
On invoke, flywheel resolves `crew:` → spawns the squad once (profiles →
`Backend::spawn`) → binds `@crew.<role>` → `parallel over: "@crew.coder"` fans
the build step across the 2 coders concurrently. The crew is the workflow's
execution context.

**Auto-crew** (`crew: auto`): flywheel infers the crew from the workflow's own
steps — every distinct `profile: "@X"` referenced becomes a crew member (count
= the max parallelism the graph demands for that role). `debate`/`brainstorm`
ship with `crew: auto`; **zero-config** — `flywheel workflow run debate` spawns
the reviewers + oracle automatically on a fresh install.

**Steering rules:**
- One agent per profile per run unless `count: N` or `parallel over:` demands more.
- `parallel over: "@crew.<role>"` fans a step across all members of that role —
  true fan-out, collected into a list under `steps.<name>`.
- `parallel over: <N>` spawns N fresh profiles of the step's `profile` (elastic).
- Idle crew members are parked (not killed) for the run — a later step may use them.
- On run end flywheel reaps the crew (or retains per `retain_crew: true`).

**Why "smart":** the captain writes the workflow's INTENT (roles + fan-out);
the orchestrator handles the mechanics — spawn count, fan-out distribution,
collection, parking, reap. The crew is derived, not hand-spawned.

---

### Spec 5 — CLI surface

**Principle:** toron-cli owns **fabric** (single-workflow, standalone); flywheel-cli
owns **orchestration** (catalogs, cron, squads, observability). MCP frozen.

**toron-cli (fabric — single workflow, standalone):**
```
toron workflow lint <file>
toron workflow validate <file>
toron workflow run <file> --trigger <json>
toron workflow invoke <file> --inputs <json>
toron workflow dry-run <file> --trigger <json>
```

**flywheel-cli (orchestration — unified; one `workflow` command):**
```
# Profiles (Spec 1/1b)
flywheel profile {list,show,create,edit,run}     # list shows the 6 built-ins + locals

# Stages (Spec 2a) — named prompt primitives
flywheel stage {list,show,add,edit}              # p1-p6/p8 + semantic aliases (fresh-eyes-explore…)

# Workflows (Spec 2/2b) — ONE vocabulary, ONE engine; legacy workflows.toml/pipelines.toml absorbed
flywheel workflow list                      # all (built-in debate/plan/beads/brainstorm/goal + locals + legacy)
flywheel workflow show <name>
flywheel workflow run <name> [--inputs <json>] [--crew auto|<squad>]   # orchestrator steers the crew (Spec 4c)
flywheel workflow lint [name]
flywheel workflow new <name>
flywheel workflow history [name]
flywheel workflow crew <name>               # preview the auto-crew the orchestrator would spawn

# Smart cron (Spec 3)
flywheel cron {list,show,add,pause,resume,history,status}

# Squads (Spec 1b/2 — generalizes recipes to profile groups)
flywheel squad {list,show,spawn}
flywheel recipes list                       # convenience alias (Recipe stays)

# Observability (reviewer §8: + per-run cost + digest)
flywheel runs                               # all runs: state + owner + per-run model/token cost (advisory)
flywheel runs show <run-id>
flywheel runs pending                       # parked on approval / spawn-await
flywheel runs digest --since <t>            # NEW (reviewer §8): what failed, parked, broken > N runs
```

**Robot-mode:** `--robot-snapshot` gains a `runs` section (ADR-0003 invariant).

---

## Self-Reflection — the orchestrator's gaps (honest)

The captain asked: *as an orchestrator/captain, what am I missing, what do I
need, what are my weaknesses?*

1. **I rebuild the same workflows repeatedly.** `docker-auto-upgrade`,
   `agent-reach-upgrade`, hachiko daily-driver loops — each hand-built with
   bespoke health-gates. No reusable workflow. **Workflows + stages (Spec 2/2a/2b) fix this.**
2. **I have no declarative squad vocabulary.** In hachiko I ran "4 scouts, 1
   firstmate" by hand; @doodlestein runs "1 Fable cc + 6 Fable cc + 6 Sol Max
   codex" (a squad recipe). **Profiles + squads fix this.**
3. **My crons are fire-and-forget.** docker-auto-upgrade fails at 03:30 → I find
   out next morning. No owning agent, no auto-bead, no escalation. **Smart cron
   (Spec 3) fixes this** — honestly scoped as self-reporting + defined-triage.
4. **I lack run-level observability.** I can't answer "what's running, parked,
   failed." `robot-snapshot` is agent-state, not run-state. **`flywheel runs`
   (+ digest) fixes this.**
5. **Failure→fix is manual.** I personally diagnose + fix. **The defined-fixer
   loop (Spec 3) closes this** — where the captain authors a diagnosis workflow;
   otherwise it's explicitly human-in-loop.
6. **I over-engineer before having the vocabulary.** Engine is Buzz-grade but I
   want DAGs. **Lesson:** ship P0 (failure-path) + profiles + workflows/stages + cron
   FIRST; DAG (P5) is gated + last.
7. **Cost/model governance is absent** — but **cost *visibility* is cheap and
   high-value**: `flywheel runs` carries per-run model/token cost as an advisory
   column (reviewer §8). Full governance (rate-limits, budgets) is a follow-on
   ADR; visibility ships now.
8. **Cross-project composition is weak.** Spec 3 `scope: host` addresses
   host-level maintenance; deeper federation is follow-on.

---

## Implementation Blueprint

### Phasing (failure-path first; value before complexity)

| Phase | Beads | What lands | Tier |
|---|---|---|---|
| **P0 — Failure path** | value-returning-runner, run-can-fail, on-error-policy | `ActionRunner -> Result<Value>`; `on_error: fail_run`; kind-46012 `status=failed`. **Prerequisite for P4/P5.** | 2 |
| **P1 — Trait unification** | backend-trait-unify, call-site-migrate, mailbackend-decision | ONE `Backend` trait (decide object-safety/`&self`/`Unsupported`-openness); migrate ~20 `&mut dyn` call sites; decide `MailBackend` fate. Verify by call-site migration, not grep. | 1 |
| **P2 — Profiles + built-in library** | profile-catalog, builtin-profiles, 30013-tag-extend, profile-resolver-scoped | `profiles.toml`; the **6 built-in profiles** (Spec 1b) compiled-in + overridable; 30013 `t`-tags; `Profile::resolve` (project→host, no `contact_policy`). | 3 |
| **P3 — Unified workflow engine** | fw-deps-toron-workflow, invoke-entrypoint, workflow-catalog, stage-library, absorb-legacy, `toron workflow` CLI | **`flywheel → toron-workflow` becomes a real cargo dep**; `WorkflowEngine::invoke` runs ALL workflows; `workflows/` + `stages.toml`; flywheel's `run_workflow`/`run_pipeline`/`Send/Wait` absorbed into the one engine (legacy paths still parse); `toron workflow {lint,run,invoke}`. | 2+3 |
| **P4 — Smart cron** | schedule-owner-policy, spawn-await-binding, cron-manager-durable-sleep, bead-on-failure, fixer-workflow, `flywheel cron` | `schedule` owner/policy/`no_claim_timeout`; promise↔job binding (bus subscriber on 43004/43006); cron manager = Resonate durable-sleep loop; `br create` on failure; defined-fixer workflow; migrate docker-auto-upgrade + agent-reach-upgrade. | 2+3 |
| **P5 — Engine flexibility (gated)** | resonate-concurrency-spike, graph-dag, control-flow-actions, lint-graph | **Gated:** concurrency spike test passes first; then `graph:` (edges from `needs`); `parallel`/`branch`/`retry`/`call_workflow`/`foreach`/`spawn`. | 2 |
| **P6 — Squads + observability** | recipe-to-squad, squad-spawn, runs-view, runs-digest, run-cost, robot-runs | `flywheel squad` (unifies `Recipe`); `flywheel runs {list,show,pending,digest}` with per-run cost; `--robot-snapshot` runs section. | 3 |

### Architectural invariants (every phase obeys)

1. **No new kind.** Profiles extend 30013 tags; spawn/await uses 43001/43004/43006
   + 46001–46012. Registry frozen.
2. **No new MCP tool.** 38-tool surface frozen. All new vocabulary is CLI.
3. **No dependency inversion.** flywheel → toron-workflow → toron-core. Never
   reverse. `spawn`'s `ProfileResolver` is an injected seam, not a flywheel import.
4. **One backend trait** (after P1). `MailBackend` fate decided in P1, not silently
   half-scoped.
5. **Resonate is the only execution substrate.** Cron manager = durable-sleep loop.
6. **Backward compatibility.** `triage.yaml`, the ADR-0004 suite, **and the
   captain's live flywheel `workflows`/`pipeline`/`recipes` runs** keep working
   through migration. Legacy removed only one phase after its replacement passes.
7. **`#![forbid(unsafe_code)]`, Tokio, rmcp, Schnorr identity, NIP-17 E2E** — all
   ADR-0003 invariants preserved.

### Blast radius (corrected — includes the ActionRunner signature)

- **Context (may change):** `toron-workflow/src/{schema,engine,lint}.rs`
  (`trigger→Option`, `invoke`, `graph`, new actions, **`ActionRunner -> Result<Value>`**,
  `on_error`, failure propagation); `toron-cli/src/cli.rs` (+`workflow`);
  `flywheel/src/{config,main,backend/*}.rs` (profile/workflow/stage/cron catalogs,
  **P1 trait migration of ~20 call sites**, P3 unified engine + workflow catalog + stage library); new
  `flywheel/src/{profile,workflow,stage,cron,squad,runs}.rs`; **`flywheel/Cargo.toml`
  (+`toron-workflow` dep in P3)**.
- **Target:** profiles/workflows/stages/cron/squads/runs = flywheel; engine flexibility
  + invoke + failure-path = toron-workflow; backend trait = toron-core.
- **Forbidden:** kind registry values; 38-tool MCP surface; ADR-0003 layering
  direction; `triage.yaml` semantics; ADR-0004 ships; **the captain's live
  `workflows`/`pipeline`/`recipes` runs (must survive migration)**.

### Default configuration (zero-config — Revision 5)

A fresh `flywheel` install works without any user-authored config:

- **Profiles:** the 6 built-ins (Spec 1b) are compiled-in — `flywheel profile
  list` shows them on first run.
- **Stages:** p1–p6/p8 + semantic aliases (Spec 2a) compiled-in — `flywheel send
  --stage p6` works immediately.
- **Workflows:** the 5 built-ins (`brainstorm`/`plan`/`beads`/`debate`/`goal`)
  + `triage` compiled-in — `flywheel workflow run debate --inputs '{proposal:
  "..."}'` spawns the crew + runs on a fresh install (auto-crew, Spec 4c).
- **Engine:** toron-workflow embedded in-process (ADR-0003 3A); `Resonate::local`.
  No external Resonate server required for the default case (a server is optional
  for cross-process crash survival).
- **Backend:** herdr (default); `flywheel deps` reports ready.

Override paths (all optional, layered over compiled-in defaults):
`~/.config/flywheel/{profiles,stages}.toml`, `~/.config/flywheel/workflows/*.yaml`.

**Seamless integration contract:** `flywheel workflow run <built-in>` on a fresh
install (a) resolves the crew automatically, (b) spawns via the configured
backend, (c) runs durably, (d) reports state via `flywheel runs` — with zero
user-authored config. This is the test `zero_config_debate` (Verification).

---

## Consequences

### Positive

- **Churn resolved by a verified tier model** — the "where does X live" filter
  is `sc11`-tested standalone-need.
- **Vocabulary unified** — one term (workflow), one engine (toron-workflow);
  flywheel's imperative `Workflows`/`Pipelines`/`Recipe` absorbed (Send/Wait→step
  types, pipeline→`call_workflow` chain, Recipe→squad). Durable by default; no
  forced migration, no deprecation gap.
- **Smart cron** turns the hand-built maintenance sagas into declarative,
  agent-owned, failure→bead→defined-fixer schedules (honestly scoped).
- **Failure propagation (P0)** is a load-bearing fix the engine needed anyway —
  it unblocks cron AND DAG actions.
- **Backend trait unified** (P1) — one surface, by design.
- **Cost visibility** ships cheaply in `flywheel runs`.

### Negative

- **P0 (failure-path) is an engine-semantics change** that contradicts the
  current capture-and-continue design. Mitigated by `on_error: continue` default
  (backward compat).
- **P1 (trait unification) is real surgery** — ~20 call sites, object-safety
  decision, `MailBackend` fate. Not mechanical.
- **P5 (DAG) is gated on a Resonate-Rust concurrency spike** that may fail. If
  it fails, P5 is deferred (not blocking P0–P4 value).
- **spawn-await's promise↔job binding** is a new bus-subscriber subsystem.

### Neutral

- **7 phases, 1 ADR.** Umbrella framing; each phase ships as beads.

---

## Verification

### Per-phase gates

- **P0:** a step with `on_error: fail_run` whose action errors → run emits
  kind-46012 `status=failed`; a step with no `on_error` → run completes
  (backward compat). `ActionRunner` returns a Value consumed by a downstream
  `{{steps.x}}` substitution.
- **P1:** `flywheel backend list` shows ONE trait source; ~20 former
  `&mut dyn MultiplexerBackend` call sites compile against the unified trait;
  `MailBackend` fate is documented (unified or explicitly scoped out).
- **P2:** `flywheel profile create` → flywheel `harness`/`model` map to the
  existing 30013 content fields (`program`/`model`); **no new tag** (`t` stays
  scoping-only); `Profile::resolve("@x")` returns `(npub, SpawnSpec)` with the
  correct project-vs-host signing context.
- **P3:** `flywheel/Cargo.toml` lists `toron-workflow`; a workflow invoked via
  `flywheel workflow run` executes through `WorkflowEngine`; flywheel's legacy
  `run_workflow`/`run_pipeline` route through the same engine (absorbed); a
  `Send/Wait` workflow parses and runs. `toron workflow invoke triage.yaml
  --inputs '{}'` starts a run.
- **P4:** a cron with `on_failure: bead` whose run fails (Spec 0) produces a
  `br` bead; a `no_claim_timeout: 600` spawn-await aborts after 10m unclaimed;
  the docker-auto-upgrade workflow passes the same health-gates as the saga;
  scheduler survives a flywheel restart (durable-sleep + `latest` catch-up).
- **P5 (gated):** the concurrency spike test passes (two concurrent suspending
  nodes, reversed resume, bit-identical replay) BEFORE the schema lands; a
  cyclic `graph:` is rejected by lint; `edges:` does not exist (derived from
  `needs`).
- **P6:** `flywheel runs` lists runs with state + owner + per-run cost;
  `flywheel runs digest --since 7d` shows failures/parked/broken>N; `flywheel
  squad spawn <recipe>` spawns a profile group; `flywheel recipes list` still
  works (alias).
- **Cross-cutting:** `triage.yaml`, the ADR-0004 suite, and the captain's live
  `workflows`/`pipeline`/`recipes` runs pass unchanged at every phase boundary.

### Current status (cross-phase verification, 2026-08-08)

Gates below were RE-RUN against the landed code — they are the current truth,
not the aspirational ADR. **PASS** = the named check/CLI/test ran green on
2026-08-08. **FAIL** = a real gap, flagged for the owning lane. Rows marked
*covered-by-design* have no discrete test by that exact name but the property
is enforced structurally (test referenced).

| Phase | Gate | Status | Evidence (run 2026-08-08) |
|---|---|---|---|
| P0 | `on_error: fail_run` → kind-46012 `status=failed`; default continue | **PASS** | `on_error_fail_run_fails_the_run_durably`, `fail_run_emits_46012_status_failed`, `on_error_default_continue_completes_the_run`, `workflow_run_terminal_maps_status` (toron-workflow lib) |
| P0 | value-returning actions consumed by `{{steps.x}}` | **PASS** | `value_returning_action_flows_into_downstream_template`, `template_substitution_trigger_in_body` (toron-workflow lib) |
| P1 | `flywheel backend` ONE trait source | **PASS** | `toron_core::backend::Backend` is the single trait; flywheel re-exports `pub use toron_core::backend::Backend as MultiplexerBackend` (flywheel `backend/mod.rs:44`). Call sites compile. |
| P1 | former `&mut dyn` call sites compile against unified trait | **PASS** | flywheel committed HEAD builds (`cargo build -p flywheel` exit 0); `MultiplexerBackend` used across backend/ modules. |
| P1 | `MailBackend` fate documented | **PASS** | Explicitly DEFERRED, ADR-0005 P1.1 decision #7 (toron-core `backend/mod.rs:63`; flywheel `backend/mod.rs:14`). |
| P2 | 6 built-in profiles compiled-in | **PASS** | `compiled_in_defaults_appear_without_profiles_toml` asserts exactly `[oracle, planner, reviewer, coder, scout, housekeeper]` (flywheel `profile.rs`) |
| P2 | `Profile::resolve` project/host scope | **PASS** | `resolve_project_scope_uses_project_30013_and_project_signing`, `resolve_host_scope_uses_operator_identity`, `resolve_unknown_profile_fails_fast` (flywheel `profile.rs`); 12/12 profile tests green |
| P2 | `harness`/`model` → 30013 content; `t` stays scoping-only | **PASS** (*covered-by-design*) | flywheel `profile.rs` maps harness/model onto `AgentProfile.program`/`model` (no new tag); `t` emitted only by toron-core `bus/scoping.rs` (`project_scoping_tags`). No discrete `profile_no_tag_overload` test exists — property is structural. |
| P3 | flywheel/Cargo.toml lists toron-workflow | **PASS** | `sc_fw_flywheel_cargo_toml_lists_toron_workflow` asserts the dep; `Cargo.toml:28` `toron-workflow = { path = "../toron/..." }` |
| P3 | workflow runs through `WorkflowEngine` (invoke) | **PASS** | `sc_fw_flywheel_invokes_workflow_through_engine` (toron-e2e p5); `invoke_runs_workflow_with_synthesized_inputs_trigger`, `triggerless_workflow_invokes_but_has_no_trigger` (toron-workflow lib) |
| P3 | legacy `run_workflow`/`run_pipeline` absorbed | **PASS** | `sc_fw_absorbed_legacy_workflow_round_trips_through_engine` (toron-e2e p5). Legacy `run_workflow`/`run_pipeline` CLI commands still exist in flywheel `main.rs` as the pre-engine Send/Wait interpreter; the absorption round-trip proves engine routing. |
| P3 | zero-config debate | **PASS** (*covered-by-design*) | `debate_resolves_default_agents` (flywheel, 3/3 debate tests + soak green). No discrete `zero_config_debate` name. |
| P4 | cron schedule fires; `on_failure: bead`; no_claim_timeout abort | **PASS** | `sc18_spawn_no_claim_timeout_aborts`, `sc21_cron_durable_loop_fires_and_resumes_after_restart` (cron_success + cron_restart), `sc22_cron_failure_bead_records_failure_bundle`, `sc23_cron_escalate_dm_at_threshold`, `sc24_cron_catchup_runs_latest_due_tick_only` (toron-e2e p5) |
| P4 | scheduler survives flywheel restart (durable-sleep + latest catch-up) | **PASS** | sc21 restart arm (worker drop → resume, no duplicate); sc24 latest catch-up; `latest_due_at_or_before` unit tests (toron-workflow cron) |
| P5 | concurrency_spike GATE | **PASS** | `concurrency_spike` (toron-e2e p5) — 1.2s green |
| P5 | cyclic `graph:` rejected by lint | **PASS** | `lint_cyclic_graph_rejected` (toron-workflow lib) |
| P5 | `edges:` does not exist (derived from `needs`) | **PASS** | `Graph` schema has only `nodes` + `needs` (toron-workflow `schema.rs:114`); no `edges` field |
| P5 | control-flow actions (parallel/branch/retry/foreach/call_workflow) | **PASS** | `parallel_elastic_spawns_n`, `branch_then_else_on_guard`, `retry_backoff_retries_until_success`, `foreach_over_list_collects`, `call_workflow_subrun_reports_intent` (toron-workflow lib) |
| P6 | `flywheel runs` state + owner + per-run cost | **PASS** | `cmd_runs` includes `run_cost` (tokens+model) + `run_owner` + state (flywheel `main.rs:6064`); `snapshot_runs_returns_well_formed_entries` (id/project/kind/owner/state/ts) |
| P6 | `flywheel runs digest --since 7d` | **PASS** | runs digest subcommand present; run logs scanned per-project (`cmd_runs`); `cron_history_empty_runs_json` green |
| P6 | `flywheel squad spawn`; `recipes list` alias | **PASS** | `squad_show_resolves_recipe_alias_to_members`, `squad_list_lists_builtin_profiles_as_singles`, `recipe_to_squad_maps_kinds_to_profile_counts` (flywheel) |
| Cross-cut | triage.yaml lints clean | **PASS** | `example_workflow_lints_clean` (toron-workflow lib); triage.yaml at `crates/toron-workflow/examples/triage.yaml` |
| Cross-cut | ADR-0004 suite | **PASS** | toron-e2e `--features p1,p2`: 30 test binaries, 0 failures; conformance 5/5, sc_fw_integrated 3/3, harness_smoke 2/3 (1 ignored) |
| Cross-cut | verify-gates.sh (ADR-0003) | **PASS** | `scripts/verify-gates.sh` → `gates pass` (no asupersync/fastmcp; all crate roots forbid unsafe) |

**FAILS / FLAGS (2026-08-08):**
- **None in committed code.** All per-phase gates above ran green on the committed
  toron + flywheel HEAD.
- **Flag (in-flight, not committed):** the flywheel working tree's P4.5 cron-mgr
  wiring (`src/cron.rs` `cron_runner`, bd-p4-migrate-sagas-7rmo, EmeraldSwan's
  lane) references `toron_workflow::FailureAction` / `CronRegistration` /
  `FailureBundle` / `PolicyEvent` at the crate root. `CronRegistration` was
  already exported; `FailureAction` + `CronPolicy` were ADDED to the
  toron-workflow root re-exports (commit in BlueOak P4.4 follow-up) so the
  in-flight flywheel code compiles (`cargo build -p flywheel` now exit 0).
- **Note (test flake):** sc21 restart arm was flaky under a 10s poll window
  (Resonate lease-reclaim latency after a worker drop is variable). Poll window
  widened to 30s; 3 consecutive passes after the fix.
- **Note (P4.7 FW rows):** `cron_success`/`cron_restart`/`cron_catchup`/
  `cron_failure_bead`/`cron_escalate` are proven at the SC/UT level (toron-e2e +
  toron-workflow). The flywheel-side EXECUTION of `br create` / kind-14 DM
  (reading `PolicySink`, calling br/DM) is BlackMarsh's P4.7 wiring, not yet
  landed.

### Behavior test matrix (Revision 5 — every flow tested)

Every workflow behavior has a named, automated test. **SC** = toron-e2e scenario;
**UT** = toron-workflow unit test; **FW** = flywheel integration test. No Spec
2/2a/2b/3/4/4c behavior ships without its row green — the matrix is the
acceptance contract.

| Behavior | Test | Phase |
|---|---|---|
| trigger `message_posted` fires workflow | SC `triage` | exists |
| trigger `schedule` fires when due | SC `cron-due` | P4 |
| trigger `webhook` fires | UT `webhook` | P3 |
| guard true → steps run; false → skip | UT `guard_true/false` | exists |
| guard timeout (100ms) → failed, not hung | UT `guard_timeout` (ADR-0004) | exists |
| `{{trigger.x}}` / `{{steps.x}}` substitution | UT `template_substitution` | P0 |
| linear steps ordered; outputs flow forward | UT `steps_ordered` | P0 |
| `on_error: continue` (default) — run completes on error | UT `error_continue` | P0 |
| `on_error: fail_run` — run FAILS; kind-46012 `status=failed` | UT `error_fail_run` | P0 |
| `ActionRunner` returns Value → `steps.<name>` | UT `value_returning` | P0 |
| `delay` durable-sleep; survives crash | SC `durable_delay` | exists |
| `request_approval` SUSPENDS; resumes on approve; reject = decision | SC `approval_suspends`/`approval_rejection` | exists |
| `spawn`+await SUSPENDS; resumes on 43004/43006; failure ≠ decision | SC `spawn_await_resume`/`_failure` | P4 |
| `spawn` no-claim timeout → abort → fail | UT `spawn_no_claim_timeout` | P4 |
| loop-prevention (workflow emits don't re-trigger) | SC `self_send_terminates` (ADR-0004) | exists |
| mention coalescing (burst → 1 job) | SC `burst_coalesce` (ADR-0004) | P3 |
| `call_workflow` sub-run; outputs under `steps.<name>` | UT `call_workflow_subrun` | P5 |
| `parallel over: @crew.r` fan-out → collected list | UT `parallel_fanout` | P5 |
| `parallel over: N` elastic spawn | UT `parallel_elastic` | P5 |
| `branch` if/else on guard | UT `branch` | P5 |
| `retry` with backoff | UT `retry_backoff` | P5 |
| `foreach` over list | UT `foreach` | P5 |
| `graph:` DAG topology; independents concurrent; acyclic lint | SC `graph_dag` + UT `lint_cyclic` | P5 |
| Resonate concurrency determinism (reversed resume, bit-identical) | SC `concurrency_spike` (**GATE**) | P5 |
| auto-crew `crew: auto` infers members from steps | FW `crew_auto_infer` | P3 |
| explicit `crew: {role: N}` spawns squad; `@crew.r` binds | FW `crew_explicit` | P3 |
| idle crew parked; reaped at run end | FW `crew_lifecycle` | P3 |
| profile `@name` resolve → (npub, SpawnSpec); project vs host | FW `profile_resolve_scope` | P2 |
| 30013 content `program`/`model` map flywheel harness/model; `t` stays scoping-only | UT `profile_no_tag_overload` | P2 |
| stage alias: `stage: p6` == `stage: systematic-execute` | UT `stage_alias` | P3 |
| cron owner spawns → runs → SUCCESS → log | FW `cron_success` | P4 |
| cron FAILURE → `br` bead; defined-fixer invoked | FW `cron_failure_bead` | P4 |
| cron repeat_failure ≥ threshold → escalate DM | FW `cron_escalate` | P4 |
| scheduler missed-tick → `latest` catch-up on wake | FW `cron_catchup` | P4 |
| scheduler survives flywheel restart (durable-sleep) | FW `cron_restart` | P4 |
| **zero-config:** `flywheel workflow run debate` works on fresh install | FW `zero_config_debate` | P3 |
| `flywheel runs` lists state+owner+cost; digest failures/parked | FW `runs_view` | P6 |

**Lint-time checks (fail-stop, not runtime):** graph acyclicity; stage/alias
resolution; profile `@name` resolution; required-input presence; guard syntax
(ADR-0004 100ms). `flywheel workflow lint` is the gate; CI runs it over every
built-in + catalog workflow.

---

## Review Log

The captain instructed: *"spawn an opencode crew to review your adr and counter,
you two should collaborate until all agreement reach."* Two rounds below.

| Round | Reviewer | Verdict | Outcome |
|---|---|---|---|
| **1** | opencode (adversarial, code-grounded) | **AGREE-WITH-CHANGES** — 9 required changes | Architect **accepted all 9**; v2 incorporates them. Counter archived at `/tmp/adr-0005-counter.md`. |
| **2** | opencode (confirmation, code-grounded) | **AGREE-WITH-MINOR-NITS** — all 9 ADDRESSED (verified against both trees) | 2 doc nits (resonate-sdk version 0.4 not v0.1.0; approval arm already writes step_outputs) corrected inline; ADR marked **Accepted**. Counter archived at `/tmp/adr-0005-round2.md`. |
| **3** | captain amendment (directive) | **APPLIED** | (a) Vocabulary **coexistence** replaces migration/deprecation (Spec 2 reframed; flywheel imperative driver stays as a sibling). (b) **Built-in profile library** added (Spec 1b): oracle/planner/reviewer/coder/scout/housekeeper with tuned behavior preambles. Phasing P2/P3 updated. |
| **4** | captain amendment (directive) | **APPLIED** | (a) **template → playbook** rename. (b) **Stage library** (Spec 2a). (c) **Built-in playbook library** (Spec 2b): brainstorm/plan/beads/debate/goal. |
| **5** | captain amendment (directive) | **APPLIED** | (a) Converge to **ONE term — workflow** (playbook→workflow); unify on ONE engine (toron-workflow); flywheel's Workflows/Pipelines/Recipe absorbed (Send/Wait→steps, pipeline→call_workflow, Recipe→squad). (b) **Auto-crew** (Spec 4c): orchestrator smart-steers the crew from the workflow, parallel/fan-out where relevant. (c) **Default configuration** (zero-config built-ins). (d) **Behavior test matrix** — every flow has a named test; lint-time fail-stop checks. |
| **6** | audit (protocol/breaking review) | **APPLIED** | Verified vs Nostr NIPs + toron source. **No protocol violation.** Caught + fixed a Spec 1 defect: "harness/model/role as `t`-tags" collided with `t`'s project-scoping role (`scoping.rs:26`) AND was redundant (`AgentProfile.program`/`.model` already carry them in 30013 content). Fix: harness/model map to existing content fields; role/behavior/backend/default_workflow are flywheel-catalog-only; `t` stays scoping-only. Zero new kind/tag/content-schema break. |

### Round 1 — opencode counter (9 changes) → resolution

1. **Fix the audit (recipes/workflows/pipelines are shipped, not stub).** → **ACCEPTED.** Context rewritten; Spec 2 documents the live vocabularies. *(Revision 3 amended the resolution: coexistence + seam, not migration/deprecation — captain directive.)*
2. **Wire `flywheel → toron-workflow` as a real cargo edge in P3.** → **ACCEPTED.** Tier diagram annotated ("arrow does not exist today"); P3 adds the dep + an `sc_fw` acceptance test. (Revision 5: the edge now carries ALL workflow execution — one engine, no separate seam.)
3. **Re-scope P1 as trait-surface unification (not mechanical).** → **ACCEPTED.** P1 renamed; object-safety/`&self`/`Unsupported`/`MailBackend` decisions called out; verify by call-site migration.
4. **Spec the failure path (engine must fail a run).** → **ACCEPTED.** New **Spec 0** (P0, prerequisite); `ActionRunner -> Result<Value>`; `on_error: fail_run`; in blast radius.
5. **Spec spawn-await precisely (binding, timeout, cancel-vs-error).** → **ACCEPTED.** Spec 4a: distinct from approval (failure not decision); promise↔job binding subsystem; `no_claim_timeout`.
6. **Resolve profile scope + kill `contact_policy`.** → **ACCEPTED.** Spec 1: `contact_policy` removed (30014 normative); project→host resolution order.
7. **Make the DAG honest (Resonate spike gate; derive edges; ActionRunner in blast radius).** → **ACCEPTED.** Spec 4b: `edges:` dropped; P5 gated on concurrency spike; return-type change in blast radius.
8. **Close the cron loop (define the fixer or retract "self-healing"; durable sleep).** → **ACCEPTED.** Spec 3: "self-healing" → "self-reporting + defined-triage"; defined-fixer workflow; Resonate durable-sleep + `latest` catch-up.
9. **Prioritize failure-surfacing + cost-visibility.** → **ACCEPTED.** P0 precedes P4; `flywheel runs` carries per-run cost; `runs digest` added.

---

## More Information

- **Relationship to ADR-0003:** extends §3A (layering) + §layer 5 (Backend trait).
  Does not reopen bus/runtime/workspace. **Adds the missing `flywheel →
  toron-workflow` edge** that ADR-0003's diagram implied but no Cargo.toml wired.
- **Relationship to ADR-0004:** the 5 hardening ships are prerequisites — smart
  cron depends on loop-prevention; engine flexibility depends on guard-bounding.
- **Relationship to ADR-0002:** P1 **completes** ADR-0002's supersession for the
  Backend trait (provisional-in-flywheel → canonical-in-toron-core, by design).
- **Revisit triggers:** (a) a backend the trait's design can't model; (b) the
  Resonate concurrency spike fails (P5 deferred); (c) standalone peer mode
  formally deprecated; (d) a use case forcing profiles/cron onto the bus (would
  reopen Option C — new kinds).

## References

- ADR-0003 §Decision 3A, §layer 5, §kind registry.
- ADR-0004 (5 hardening ships; loop-prevention is a smart-cron prerequisite).
- `flywheel/src/main.rs:3863–3999` (`run_workflow`/`run_pipeline`/`cmd_asset` — the live imperative vocabulary, which stays per Spec 2).
- `flywheel/src/config.rs:370–480` (`Recipe`/`Workflows`/`Pipelines`/`WorkflowStep`).
- `flywheel/src/backend/mod.rs:204,310` (`MultiplexerBackend`/`MailBackend` — the duplicated, incompatible trait surfaces).
- `flywheel/src/herdr_shim.rs:31` (herdr client already unified).
- `flywheel/Cargo.toml` (no `toron-workflow` dep today — P3 wires it).
- `crates/toron-workflow/src/engine.rs:198–210` (error-swallowing by design — P0 fixes).
- Captain memory: `docker-auto-upgrade` + `agent-reach-upgrade` self-reporting cron sagas (the generalization target).
