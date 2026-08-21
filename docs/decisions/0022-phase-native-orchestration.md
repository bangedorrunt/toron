<!-- governed-by: ADR-0005, ADR-0010, ADR-0015, ADR-0019, ADR-0020 -->
---
status: accepted
date: 2026-08-21
decision-makers: Captain (bangedorrunt)
consulted: fresh-eyes flywheel × toron seam review (2026-08-21); bd-8xca compose live acceptance; bd-4spn unwired-surface precedent
informed: flywheel maintainers; skill owners (toron, flywheel, runtime-adaptation)
---

# ADR-0022: Phase-Native Orchestration — typed JobSpec, harness phase mechanics, and one durable execution rail

**Status:** Accepted (2026-08-21; umbrella + wave beads filed, 3-opencode crew steered via the durable loop)
**Date:** 2026-08-21
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0005](0005-orchestration-layer.md) (orchestration tier — this ADR gives `dispatch_job` a typed spec contract), [ADR-0010](0010-execution-wiring-model-registry-headless.md) (model registry + headless — the phase registry extends `model_args`), [ADR-0012](0012-standalone-cron-type.md) (cron stays a distinct type; this ADR adds a `--live` amendment note)
**Amends:** ADR-0005 Spec 2 (the `dispatch_job` action's `spec` gains a typed contract; YAML surface unchanged for existing workflows)
**Does not amend:** the frozen MCP tool surface (39 tools, 25 resources — JobSpec is engine-internal schema, not a new MCP tool), [ADR-0015](0015-flywheel-toron-integration-contract.md) (one-way dep flywheel → toron-workflow → toron-core; the phase registry lives flywheel-side, the schema lives toron-side), [ADR-0019](0019-bead-verified-execution-and-principle-playbooks.md) (beads as the tracker, no worktrees, no PRs)
**Companion:** [ADR-0020](0020-nostr-two-plane-cli-and-crew-steering.md) (crew-steering planes — phase built-ins use plane-P mail and beads, never new tools)
**Tracker:** beads. Never pull requests.

> **Scope:** make workflow *phases* (resolve / plan / implement / review / verify) a mechanic the engine types, the harness enforces, and the durable rail checkpoints — instead of prose in profile behavior text. Make compose-in-flight and steer run through the same Resonate-durable engine path as catalog workflows and cron. One execution rail. Gates on artifacts, not agent claims.

---

## 1. TL;DR

The workflow engine's unit of work and the harness's unit of work are mismatched. `Action::DispatchJob` carries an untyped `serde_json::Value` spec; everything behavioral ("you are a PLANNER, plans are wrong until executable") is prompt prose in flywheel `profile.rs`; the only mechanics that exist are model and effort. Consequence: the orchestrator cannot tell a plan step from a build step, cannot gate a build on plan artifacts, cannot measure phases separately, and composed workflows execute outside the durable engine entirely (`WorkflowEngine::invoke` has zero flywheel callers — a crash mid-composed-run is a lost run).

**Decision — five layers, four waves:**

```
L1  typed JobSpec + Phase enum     toron-workflow/schema.rs     validation, not enforcement
L2  phase registry per harness     flywheel headless.rs/pane.rs phase flags reach agent_start
L3  phase-native built-ins         flywheel builtin_workflows  built-ins ride the compose path
L4  one durable rail               engine.invoke for compose    + steer becomes a composed run
L5  phase gates + metrics          playbook_lint + runs board   warn-first, enforce later
```

Each layer ships independently and is independently useful; the closure is what delivers "captain composes, engine guarantees."

---

## 2. Context and Problem Statement

### 2.1 Measured drift (2026-08-21 review; evidence, not vibes)

| Aspect | Claim / intent | Code today | Consequence |
| :--- | :--- | :--- | :--- |
| Dispatch spec | ADR-0005 Spec 2 `dispatch_job` carries the job | `schema.rs:354-360` — `spec: serde_json::Value`, zero engine-side contract; flywheel headless interprets `spec.harness` ad hoc (bd-apd2) | spec fields are undocumented and unvalidated; drift between YAML authors and the runner is invisible |
| Phase semantics | planner/coder/reviewer roles | `profile.rs` behavior **text** only ("You are a PLANNER…"); no mechanical phase exists anywhere | orchestrator cannot distinguish, gate, or measure phases; role fidelity depends on prompt adherence |
| Mechanics that DO exist | model + effort | `profile_launch_args` (bd-eign.9) + `headless.rs model_args` incl. omp `--thinking` (bd-42fs) | proves the spawn path can carry per-invocation mechanics — phase belongs on the same rail |
| Compose-in-flight | bd-8xca "captain composes when the catalog doesn't fit" — shipped live | `crew.rs compose_workflow` + `WorkflowCmd::Compose --run` dispatches via **pane prompt**; `engine/mod.rs:425 pub async fn invoke` has **zero flywheel callers** | composed runs are not Resonate-durable: crash = lost, `on_failure: bead` silently does nothing, two execution paths for the same `Workflow` struct invite parity drift (filed `bd-compose-not-durable-oxjy`) — **RESOLVED 2026-08-21**: Compose arm routes through `engine.invoke`, PolicySink drained (`flywheel ac1c79d`/`a1498f1`; gate test green) |
| Steer | `flywheel steer` = goal→brainstorm→plan→beads→build→review→ship | `cmd_steer` (main.rs:17262) drives hardcoded stage logic, not an engine run | the captain's one-call steering gets none of the engine's checkpoint/resume/policy guarantees |
| On-demand cron | fire a schedule NOW | `cron run` is a structural **dry-run** (recording runner, bd-8wvz close) | no way to force-fire a cron with real execution (filed `bd-cron-run-no-live-u9tg`) |
| Decoration risk | mechanisms must have consumers | bd-4spn: `check_principles` unit-tested, zero runtime callers for weeks | a `phase:` field bolted on without the consumers of L2–L5 would recreate exactly this failure mode |

### 2.2 The incident that motivates mechanics-over-prose

2026-08-21: all 14 flywheel slash commands (`/swarm`, `/loop`, `/workflow-*`, `/beads-*`) were **dead for every opencode agent** — `prompts_dir` wrote to `~/.config/opencode/command/` (singular) while opencode reads `commands/` (plural). Flywheel commit `63cedc1` fixed it. Lesson for this ADR: anything that exists only as convention (a directory name, a prompt paragraph, an implicit spec shape) silently breaks. Mechanics — typed fields, validated registries, checked flags — fail loudly. Phases are currently pure convention. This ADR makes them mechanics.

### 2.3 What "fully agent automation" needs that prose cannot give

The captain × crews/peers vision (ADR-0019/0020) requires the loop to make decisions on *evidence*: dispatch the plan, wait for **beads with acceptance criteria to exist**, then dispatch builds against those beads, wait for **commits citing the bead + green gates**, then reviews. Today the loop can at best pattern-match mail text. With a typed phase, each transition has a machine-checkable completion contract (§4.5) and the loop's `plan_actions` can key on it.

---

## 3. Decision

### 3.1 Layer 1 — typed JobSpec in toron-workflow (validation first, enforcement never silently)

New public surface in `crates/toron-workflow/src/schema.rs`:

```rust
/// Phase of an agent job. The ORCHESTRATOR's vocabulary; harnesses map
/// it to their own mechanics via the flywheel phase registry (ADR-0022 L2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase { Resolve, Plan, Implement, Review, Verify }

/// Typed view of a dispatch_job spec. LENIENT: unknown fields pass
/// through; every field except `prompt` is optional. Parsing never
/// rejects a job — it annotates it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobSpec {
    pub prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<Phase>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,   // validated against the model registry kinds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,    // harness effort vocabulary (e.g. omp thinking levels)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs: Option<serde_json::Map<String, Value>>,
    /// Unknown fields preserved verbatim (forward compat; lint warns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Map<String, Value>>,
}
```

Wire compatibility contract:

- `Action::DispatchJob.spec` **stays** `serde_json::Value` on the wire in Wave 1. The engine gains `impl JobSpec { pub fn from_value(v: &Value) -> JobSpec }` (lenient — see struct doc) and stashes the typed copy into run state for observability and downstream consumers.
- Existing YAML, templates, `compose_workflow` parts, and the flywheel headless runner keep working byte-identically. Wave 1 changes no behavior anywhere.
- `harness` values validate against the existing harness vocabulary (pi | omp | opencode | grok — the bd-apd2 override set). Unknown harness = lint error at lint time, warning at runtime (the run proceeds with the profile default).
- `extra` capture is the forward-compat valve: new spec fields graduate from `extra` to typed fields only via a schema-rs change, never by silently reinterpreting.

### 3.2 Layer 2 — the phase registry (flywheel; one match arm per harness)

New pure function beside `model_args` in `flywheel/src/headless.rs`, mirrored into the pane path through `profile_launch_args`:

```rust
/// Per-harness launch args for an orchestration phase (ADR-0022 L2).
/// Returns (args, degraded): degraded=true means the harness has no
/// native mechanic for this phase — the runner WARNS (never silent) and
/// proceeds; the run log records `phase_degraded`.
pub fn phase_launch_args(harness: &str, phase: Phase) -> (Vec<String>, bool)
```

Mechanics known to exist today (wire in Wave 2; verify each flag against the installed harness before relying on it):

| Harness | Phase mechanic (verified at wire time) | Status |
| :--- | :--- | :--- |
| omp | `--thinking=<level>` (bd-42fs, shipped) maps effort; plan-phase maps to the harness's plan/thinking flag per its CLI help | flag verified at wire time |
| opencode | command-file frontmatter `agent:`/`model:` (proven — ADR-0005 table; the working `commands/` dir) + agent modes in `~/.config/opencode/agents/`; plan-phase selects the plan-mode agent | mode name verified at wire time |
| pi / grok | none known — degrade loudly per the contract above | documented degradation |

Contract rules:

1. **The registry is the only place a harness's phase vocabulary lives.** One match arm per harness in `phase_launch_args`. No scattering flags through profiles or stage text.
2. **Loud degradation.** No native mechanic → `degraded=true` → `eprintln!` warning naming harness+phase, run log tagged `phase_degraded`, prompt-mode phase proceeds. Harness flag drift (a harness renames a flag) must never produce a silent no-op.
3. **Preflight integration check.** `flywheel deps` (the H3 integration surface, main.rs:15658) grows a phase-registry probe per registered harness: does the flag/mode the registry names still exist in the installed version? Fail → warn with the exact remedy. This is the anti-slash-command-incident defense (§2.2).
4. **Effort and phase stay orthogonal.** Effort = reasoning depth (existing). Phase = workflow role. Neither implies the other; both ride the same args vector.

### 3.3 Layer 3 — phase-native built-ins; built-ins are data that exercises compose

New built-in workflows in `flywheel/src/builtin_workflows.rs` (catalog-resolvable, invocable by engine, CLI, and the existing `/workflow-*` command surface — no new commands):

| Built-in | Phase of its steps | Completion contract (machine-checkable) |
| :--- | :--- | :--- |
| `resolve` | `resolve` | goal statement posted to the project thread (toron mail, plane P) carrying `target`, `non_goals`, `blast_radius` fields |
| `plan` | `plan` | ≥1 bead created with acceptance criteria + dependencies; `br dep cycles` empty for the created set |
| `implement` | `implement` | ≥1 commit whose message cites the bead id; the bead's named gate command exit 0 |
| `review` | `review` | findings posted to thread; verdict recorded (AGREE / AGREE-WITH-CHANGES / REJECT) — harness default opencode per bd-42fs reviewer arm |
| `ship` | chain | `call_workflow` chain resolve→plan→implement→review with the §4.5 gates between links; `crew: auto` |

Stage library entries (`stages.rs` `StageLibrary`) gain an optional `phase` field; `crew.rs parse_compose_part` extends from `role:stage` to also resolve `phase:stage`. Target YAML (the UX this whole ADR serves):

```yaml
steps:
  - name: plan
    action: dispatch_job
    assignee: "@crew.planner"
    spec: { phase: plan, prompt: "{{inputs.goal}}" }
  - name: build
    action: dispatch_job
    assignee: "@crew.coder"
    spec:
      phase: implement
      harness: opencode      # per-step override — today's bd-apd2, unchanged
      prompt: "Implement {{steps.plan.output}}"
```

Design rule: **built-ins ride the same compose machinery the captain uses interactively.** `ship` is literally `compose_workflow` output baked into the catalog. This validates compose-in-flight in CI on every run instead of leaving it a side door, and makes the built-ins data, not special code.

### 3.4 Layer 4 — one durable execution rail

Closes `bd-compose-not-durable-oxjy` on the same rail as L1/L3:

1. **Compose → engine.** `WorkflowCmd::Compose --run` stops dispatching via pane prompt. It builds the `Workflow` struct (unchanged, `compose_workflow`), then calls `WorkflowEngine::invoke` (engine/mod.rs:425) — the SAME durable function the trigger path uses. Pane agents still execute the individual `dispatch_job` steps (the engine's spawn sink lands on herdr panes exactly as today); what changes is **ownership of the run**: crash → resume from the last Resonate checkpoint; `on_failure: bead` drains the PolicySink into `br create` for composed runs too. **Shipped 2026-08-21 (`bd-compose-not-durable-oxjy`):** flywheel `ac1c79d`+`a1498f1` route the Compose arm through `engine.invoke` with pane agents as executor surface only and drain the PolicySink (`on_failure: bead` wired); gate green — `cargo test -p toron-workflow` 173 passed incl. `invoke_redelivery_replays_recorded_run`.
2. **Steer → engine.** Wave 4: `cmd_steer` builds its chain via `compose_workflow` + `invoke`. The hardcoded stage sequence in main.rs:17262 shrinks to prompt construction; the chain semantics become a resumable run.
3. **Cron on-demand live fire (adjacent, rides the rail).** `flywheel cron run <name> --live` reuses the durable spec_runner path with an immediate tick injection (skip the sleep), bounded output. Dry-run stays the default (bd-8wvz semantics preserved). Closes `bd-cron-run-no-live-u9tg`.
4. **Transition visibility.** During migration the old pane-prompt compose path remains as an explicit fallback behind `--pane-dispatch` (deprecated, run log tags which path executed) so drift between paths is visible in `flywheel runs`, not inferred from behavior. Removed one phase after the crash-resume equivalence gate (§6 W3) is green on live swarms.

### 3.5 Layer 5 — gates and measurement (warn-first, enforce-later)

**Lint-time (playbook_lint.rs, extends the existing ADR-0019 rule family):**

- `R-P1` every `dispatch_job` spec must parse as `JobSpec`; unknown `extra` fields → warning naming the field.
- `R-P2` `phase: implement` steps must carry an acceptance command (extends the existing bug-fix `repro` rule).
- `R-P3` `phase: review` fan-outs must satisfy the existing n≥2 + mode rules (extends the existing arena rule); review steps with `n < 2` → lint error.
- `R-P4` `phase: plan` steps must declare `inputs.data_shape` (extends the existing feature rule) and dispatch to a planner-role profile.

**Runtime (opt-in, warn-first):** a build step whose spec lacks a completed plan artifact (the §3.3 `plan` contract) warns before dispatch. Enforcement flips to blocking ONLY after one live swarm completes a full `ship` chain with zero phase-gate false positives. Rationale: a hard artifact-gate that fires before the artifact convention is universal would stall dispatch — the exact bd-bvwf.5 failure mode. The flip is a config default change, one line, tracked as a W5 bead.

**Observability:** run logs record `phase` (and `phase_degraded`) per dispatch; `flywheel runs` (ADR-0005 P6.3) gains a phase column → per-phase cycle time and cost. This is the honest baseline the crew-model rework (bd-42fs) needs for deciding which phases deserve expensive models.

### 3.6 Compose versatility ladder (extends L3/L4; ships with W3 unless noted)

Compose today exposes two part forms (`workflow:name`, `role:stage`) over an
engine DSL that is far richer (`retry`/`branch`/`foreach`/`parallel`/`delay`/
`request_approval`, step `guard`, `{{steps.*}}` dataflow). The ladder closes
that gap so "the catalog doesn't fit" degrades gracefully instead of dead-ending:

| Tier | Capability | Status |
| :--- | :--- | :--- |
| T0 | Recombine: parts from catalog workflows + stage library | **shipped** (bd-8xca) |
| T1 | Full-grammar emission: parts cover the entire `Action` vocabulary — `each:`, `when:`, `retry(n)`, `parallel:` | W3 |
| T2 | Author from scratch: `workflow compose --from-yaml -` (full `Workflow` from stdin/mail); durable via W3's `engine.invoke` routing | W3 |
| T3 | Learn + persist: `--keep <name>` graduates a green composed run into the project catalog with provenance (`origin: composed`, `from: <parts>`, `first-run`, `author`) — the hillclimb keep-or-discard pattern lifted to workflow shapes; detect-then-find-existing starts finding previously improvised workflows | W6 |
| T4 | Delegated composition: any agent mails a compose-request job; the loop composes when `decide` reports no catalog coverage (archetype classification: scout-fanout / verify-fence / debate / hillclimb / review) | W7 |

**Safety envelope (all tiers, rides existing machinery):**
1. Lint on every compose — fail-stop, extends R-P1..P4; unknown part forms are
   parse errors, never silently dropped.
2. Structural dry-run first-class (`compose --dry-run`): resolved graph,
   fanout counts, guards against sample inputs, phase tags — the hillclimb
   dry-run rendering generalized.
3. Budget caps per composed run: max total dispatches, wall-clock, nesting
   depth; declared blast-radius as reservation globs (audit trail today,
   enforced the moment bd-reservations-not-projected-5ohc lands).
4. Durability: T1/T2 compose through `engine.invoke` (W3) — improvisation is
   a checkpointed run; `on_failure: bead` files the failure.
5. Provenance: every composed run records base, parts, author, dry-run hash.

**Explicitly rejected: mutating a live run.** Mid-run step editing breaks
Resonate replay determinism (a checkpoint must resume to the same future).
Adaptation is expressed AS composition (`retry` + observe→`branch`), and a
genuinely-wrong run is aborted → compose v2 seeded from v1's final state
(base-seed mechanism) → run v2, with run lineage linking them.

### 3.7 Learning and delegation (post-acceptance waves)

W6 (persist/lifecycle): `--keep` writes `.flywheel/workflows/<name>.yaml`
with provenance; graduation requires one completed green live run (never a
dry-run) + full lint; the catalog resolver picks it up like any entry.
W7 (delegation): compose-request as a job spec shape; loop-side composition
gated on decide's coverage report; initially behind a contact/policy
allowlist so a buggy crewmate cannot improvise unbounded dispatch.

### 3.8 Stage registry — agent-writable permanence (captain's counter-proposal, adopted 2026-08-21)

Principle: **everything an agent composes WITH is data; "built-in" is a tier
agents walk into with evidence, not a code boundary.** Rust seeds are
bootstrap only. This replaces the earlier stance that agents never author
built-ins — agents author DATA at every tier; the shared tier costs
evidence, not human gating.

Current state (verified): stages are compiled-seed p1–p8 +
`~/.config/flywheel/stages.toml` field-level overrides + semantic aliases
(ADR-0005 Spec 2a, stages.rs); the CLI surface is `stage list` ONLY; compose
parts are role-bound; there is no project tier and no promotion path.

**Layered stage registry:**

| Tier | Location | Writable by | Scope |
| :--- | :--- | :--- | :--- |
| seed | compiled (p1–p8 + aliases) | nobody | fallback, always present |
| user | `~/.config/flywheel/stages.toml` | any agent | this machine |
| project | `.flywheel/stages.toml` | any agent | ships with the repo (git) |
| repo (built-in) | flywheel source seed data | promotion | every flywheel install |

Stage records gain metadata: default role affinity, `phase` (W3), declared
principles, provenance (author, origin run).

**CLI surface (the ergonomics the captain specified — list, add, select):**

```bash
flywheel stage list [--all]     # every tier, tier-tagged, phase + role affinity
flywheel stage show <name>     # body + metadata + tier
flywheel stage new <name>      # scaffold with role/phase/principles frontmatter
flywheel stage lint            # all tiers: shape, affinity validity, size cap
```

**Stage-first compose:** a bare stage name becomes a valid part —
`--parts "fresh-eyes-explore,systematic-execute"` dispatches each stage to
its own default role; `role:stage` remains the explicit override; `phase:stage`
per W3. "Pick multiple stages, get a workflow" is one command.

**Permanence ladder (stages AND workflows, uniform):**

```
ephemeral compose → project catalog (--keep, W6)
                  → user catalog (stage promote / workflow promote)
                  → repo built-in tier (promote with evidence:
                      green across ≥2 projects or ≥2 distinct objectives
                      + a review bead; lands as DATA in the flywheel repo)
```

Why this is safe: stages are prompts (bounded blast radius); stage lint
validates shape + affinity names + size; the repo tier's evidence bar keeps
the registry from rotting (the crew-conduct rule — done is a claim, the
consumer validates against the run log — applies to promotion). Over time
the compiled seed set can be regenerated from promoted data; Rust seeds
remain the zero-config bootstrap.

---

## 4. Phase semantics — the contract (normative)

| Phase | Intent | Agent produces | Completion evidence (machine-checkable) |
| :--- | :--- | :--- | :--- |
| `resolve` | reduce ambiguity | goal statement: target, non-goals, blast radius, open questions | statement posted to the project thread with the four fields present |
| `plan` | decompose into verifiable work | beads with acceptance + dependency order | beads exist; acceptance non-empty; `br dep cycles` empty on the set |
| `implement` | build against a bead | code + tests + commit | commit citing bead id; bead's gate command exit 0 (ADR-0019 evidence-based done) |
| `review` | adversarial falsification | findings + verdict | findings posted; verdict ∈ {AGREE, AGREE-WITH-CHANGES, REJECT} |
| `verify` | prove the contract | gate run output | named gate command exit 0, output recorded |

Rules:

- **Evidence over claims.** No phase transition keys on an agent saying "done." This generalizes ADR-0019's ledger-gated close from beads to the whole chain.
- **Phases compose; they do not gate creativity.** A `--direct` dispatch (no plan artifact) remains legal — it warns (W5 warn-first), it does not block, until the enforcement flip.
- **The vocabulary is closed** (5 values). New phases require an ADR amendment, not a string.

---

## 5. Alternatives considered and rejected

| Alternative | Core tradeoff | Why rejected |
| :--- | :--- | :--- |
| **Status quo** (phases as profile prose) | zero work | cannot gate/measure/resume on phase; role fidelity depends on prompt adherence; the §2.2 incident class recurs |
| **Narrow: add `phase` string to spec only** | one-day change | recreates bd-4spn — typed, tested, unconsumed. The value is in the closure (registry consumers, gates, durable rail) |
| **Harness-side only** (opencode agents/modes, omp flags; no engine knowledge) | no cross-repo schema change | the orchestrator stays blind: no completion contracts, no phase metrics, compose stays non-durable; per-harness conventions drift silently (slash-command incident class) |
| **Everything through typed `Action::DispatchJob { spec: JobSpec }`** (strict from day one) | cleanest type | breaks every existing YAML/template/compose part and the headless runner in one commit; lenient-parse-then-stricten delivers the same end state without a flag day |

---

## 6. Implementation Blueprint (waves, files, gates)

**Zone rules (ADR-0008 blast-radius vocabulary):** Context (may read) = both repos' engine/runner surfaces. Target (may modify) = files named below. Forbidden (must not touch) = MCP tool surface (39 names), kind registry (ADR-0014), Nostr wire structs, `MailSurface`, the atomic reservation conflict-check, herdr client (`HDR_PROTOCOL`), ADR-0001 leftovers.

### Wave 1 — typed spec (toron-workflow only; no behavior change)
- **Target:** `crates/toron-workflow/src/schema.rs` (Phase, JobSpec, `from_value`), `crates/toron-workflow/src/playbook_lint.rs` (R-P1), engine run-state stash of the typed spec.
- **Tests:** `jobspec_parses_canonical_shape`, `jobspec_lenient_unknown_fields_pass_through`, `jobspec_missing_prompt_is_error_only_at_use`, lint fixture with an unknown field warns.
- **Gate:** `cargo test -p toron-workflow` green; full `flywheel` test suite green against the new schema crate (proves byte-compat). governed-by markers on touched files.

### Wave 2 — phase registry (flywheel; two harnesses)
- **Target:** `flywheel/src/headless.rs` (`phase_launch_args` beside `model_args`), `flywheel/src/pane.rs` (extend `profile_launch_args`), `flywheel/src/main.rs` deps section (preflight probe).
- **Tests:** registry table test per harness; `degraded` contract test; preflight probe test with a fake harness binary.
- **Gate:** a live 2-agent swarm where a `phase: plan` dispatch reaches the harness with the registry args observable in the pane (herdr `agent read` evidence); `flywheel deps` prints the phase-registry line.

### Wave 3 — compose → engine + phase built-ins + ladder T1/T2 (closes bd-compose-not-durable-oxjy, bd-dispatch-spec-untyped-0iud consumers)
- **Target:** `flywheel/src/crew.rs` (compose --run routes to `engine.invoke`; `parse_compose_part` phase + Tier-1 grammar resolution: `each:`/`when:`/`retry(n)`/`parallel:` + stage-first bare-name parts (§3.8)), `flywheel/src/builtin_workflows.rs` (resolve/plan/implement/review/ship), `flywheel/src/stages.rs` (stage `phase` field), `flywheel/src/cron.rs` (`cron run --live`), compose `--from-yaml -` + `--dry-run` first-class (Tier 2 + §3.6 envelope).
- **Tests:** composed run executes as engine run (run id exists in runs board); `on_failure: bead` creates a bead on a forced-failure composed run; crash-resume equivalence (kill -9 mid composed run → restart → completes; mirror `scripts/e2e-hardened-loop.sh` scenario 2); `ship` chain dry-run renders the expected 5-link graph; Tier-1 grammar parse table (every part form → expected Action); `--from-yaml` round-trip (author → lint → dry-run → run); dry-run renders fanout counts + phase tags; budget caps enforced (a composed workflow exceeding max dispatches fails closed at lint).
- **Gate:** live: compose a 2-step workflow, run via engine, kill flywheel mid-run, restart, run completes; catalog + composed + cron all appear in `flywheel runs`; one Tier-1 `each:` compose executed live.

### Wave 4 — steer migration + enforcement flip groundwork
- **Target:** `flywheel/src/main.rs` `cmd_steer` (build via compose, invoke), run-log `phase`/`phase_degraded` fields, `flywheel runs` phase column.
- **Tests:** steer produces an engine run id; steer crash-resume equivalence.
- **Gate:** live `flywheel steer "<small objective>" --direct` completes as an engine run with phase-tagged steps in the runs board.

### Wave 5 — enforcement flip (separate bead, only after W3+W4 live evidence)
- Blocking phase gates per §3.5. One-line default change + gate tests. Requires: one live swarm completing a full `ship` chain with zero phase-gate false positives.

### Wave 6 — persist/lifecycle + promotion ladder (§3.7 T3, §3.8)
- **Target:** `flywheel/src/crew.rs` + catalog resolver (`--keep <name>` → `.flywheel/workflows/<name>.yaml` with provenance), `workflow new` path unification, stage registry surface (`stage show/new/lint`, project tier `.flywheel/stages.toml`, stage metadata fields), `stage promote` / `workflow promote` (project → user → repo tiers with the §3.8 evidence bar).
- **Tests:** keep-after-green writes catalog entry the resolver finds; keep-without-green-run refuses; provenance fields present; promote path (`workflow new`) and `--keep` produce identical lint outcomes; stage tiers resolve seed<user<project precedence; stage lint rejects bad affinity/phase names; promote-to-repo without evidence refuses.
- **Gate:** live: compose → run green → keep → `flywheel workflow list` shows it → a later `decide` find-existing arm selects it; one stage added at project tier and used stage-first in a compose.

### Wave 7 — delegated composition (§3.7 T4)
- **Target:** compose-request job spec shape (toron-workflow), loop-side coverage report in `decide`, contact/policy allowlist gate.
- **Tests:** delegated compose honors the allowlist (deny case never dispatches); decide coverage miss → compose candidate rendered for approval (not auto-run in W7).
- **Gate:** live: one agent-composed workflow via mail request, allowlisted, linted, run through the engine.

**Post-acceptance:** each wave becomes a bead spawned from this ADR (existing: W1↔`bd-dispatch-spec-untyped-0iud`, W3↔`bd-compose-not-durable-oxjy`, cron↔`bd-cron-run-no-live-u9tg`). governed-by: ADR-0022 markers required on every Target file per `docs/decisions/_scripts/verify-adr-links.sh`.

---

## 7. Consequences

**Positive**
- Phase becomes gateable, measurable, checkpointable; the loop decides on artifacts, not prose.
- One execution rail: catalog, composed, cron, steer all durable (crash-resume + `on_failure: bead` everywhere).
- Built-ins validate compose-in-flight in CI; compose stops being a side door.
- Profile behavior text shrinks — mechanics replace prose; role drift becomes observable (`phase_degraded` tags).
- Per-phase cost/cycle-time data grounds future model assignment (bd-42fs).

**Negative / risks & mitigations**
- *Harness flag drift* (a harness renames a plan flag) → preflight probe + loud degradation (§3.2 R3); never a silent no-op.
- *Over-gating crews* on artifacts they cannot yet produce → warn-first everywhere; enforcement flip only after live-swarm evidence (W5).
- *Two compose paths during migration* → deprecated `--pane-dispatch` tagged in run logs; removed one phase after the equivalence gate.
- *Schema churn risk* → `extra` field is the forward-compat valve; unknown fields warn, never reject; strictness only via future ADR-amended graduation.
- *Scope creep into the MCP surface* → explicitly forbidden (zone rules); JobSpec is engine-internal.

---

## 8. Verification (acceptance for this ADR)

- [ ] W1: `cargo test -p toron-workflow` green incl. 4 new JobSpec tests; flywheel suite green against new schema (byte-compat proof).
- [ ] W1: lint fixture with unknown spec field produces R-P1 warning naming the field.
- [ ] W2: `phase_launch_args` table tests + degraded contract green; `flywheel deps` prints per-harness phase-registry status.
- [ ] W2 live: `phase: plan` dispatch observable in the target pane with registry args (herdr `agent read` capture).
- [ ] W3: composed `--run` produces an engine run id; kill -9 mid-run → restart → completes (crash-resume equivalence).
- [ ] W3: forced-failure composed run with `on_failure: bead` creates the bead.
- [ ] W3: `ship` dry-run renders the 5-link chain; `cron run --live` executes for real once.
- [ ] W3 ladder: Tier-1 grammar parse table green; `--from-yaml` round-trip green; dry-run renders fanouts + phase tags; budget caps fail closed.
- [ ] W4: `steer --direct` completes as an engine run with phase-tagged steps in `flywheel runs`.
- [ ] W6: keep-after-green lands in catalog and a later decide find-existing selects it; keep-without-green-run refuses.
- [ ] W6 stages: project-tier stage resolvable and usable stage-first in a compose; promote-to-repo without evidence refuses.
- [ ] W7: one agent-composed workflow via mail request, allowlisted, runs through the engine.
- [ ] All Target files carry `governed-by: ADR-0022`; `bash docs/decisions/_scripts/verify-adr-links.sh` green.
- [ ] `bash scripts/verify-gates.sh` green (toron) and flywheel clippy/test gates green.
- [ ] ADR-0005 Spec 2 amended with a pointer to §3.1; README index row added.

---

## 9. More Information

- Fresh-eyes review session 2026-08-21 (this ADR's trigger): seam findings + beads bd-dispatch-spec-untyped-0iud, bd-compose-not-durable-oxjy, bd-cron-run-no-live-u9tg, bd-reservations-not-projected-5ohc (orthogonal, not in scope here).
- Slash-command dead-dir incident (flywheel `63cedc1`): the §2.2 motivation for mechanics-over-convention.
- bd-4spn: the unwired-mechanism precedent this ADR's staging is designed to avoid.
