# ADR-0010: Execution Wiring, Model Registry, Role Conditioning, and Headless Mode

**Status:** Accepted (2026-08-13; implemented + live; captain audit flipped Proposed → Accepted)
**Date:** 2026-08-09
**Decision-makers:** Captain (bangedorrunt) — grilling session; implementation via beads
**Extends:** [ADR-0005](0005-orchestration-layer.md) (the orchestration tier this wires to real execution).
**Amends:** [ADR-0005](0005-orchestration-layer.md) Spec 4a (headless does not use the promise↔job binding) and the `Stage` struct (regains the dropped `profile` field).

> **Scope:** ADR-0005 built the orchestration tier (profiles, workflows, stages, smart cron,
> squads, DAG) and its verification table shows P0–P6 green. This ADR is the **wiring layer**:
> it connects that green-tested scaffold to real execution, adds a model registry + per-harness
> model injection, delivers the role-conditioning preambles that already exist but never reach
> agents, and adds a headless execution mode for autonomous/cron work. It does NOT add new
> Nostr kinds or MCP tools (ADR-0003/0004 invariants hold).

---

## Context and Problem Statement

### The 6-round grilling (the actual problem, evidence-bound)

A grilling session to "improve workflows/squads/recipes and built-in agents" surfaced,
corrected, and locked the real gaps. Three claims were corrected by evidence:

1. **"Support omp" is already done.** `herdr agent start --kind` lists `omp` as a supported
   kind with canonical executable (verified `herdr agent start --help` possible-values; omp
   runs under herdr with full lifecycle hooks — `~/.omp/agent/extensions/herdr-omp-agent-state.ts`,
   `herdr integration status` = current v8). The flywheel spawn path passes it through:
   `resolve_kind("omp") → "omp"` → `agent_start(kind="omp")`. **omp is not a gap.**

2. **The real gap is the no-op action runner.** `flywheel workflow run` injects a **no-op**
   `ActionRunner` (`main.rs:8366–8371`):
   ```rust
   // action side-effects are RECORDED, NOT DISPATCHED
   let runner = Arc::new(|a: &Action| Ok(json!({ "action": a.type_name() })));
   ```
   Every `spawn`/`dispatch_job`/`send_message` returns `{action: "spawn"}` and does nothing.
   The crew is *planned* (`crew::plan_crew`) and *logged*, then **never spawned**. The engine
   runs control-flow durably (delays/approvals/graph are real), but no agent is started, no
   prompt is sent, no mail is dispatched. ADR-0005's P3/P4 gates are engine-level
   (`toron-e2e` + `toron-workflow` unit tests); the **flywheel-side execution never landed**
   (ADR-0005 line 1131 admits P4.7 wiring "not yet landed"). Real work today flows only
   through the legacy imperative path: `flywheel loop` → herdr-direct `Send/Wait`;
   `flywheel send`/`spawn` → direct herdr ops.

3. **Role-conditioning preambles are dead text.** All six built-in `Profile.behavior`
   preambles ("You are a PLANNER… RULES…", 200+ words each) are authored, stored, and
   asserted in tests — but **never prepended to any dispatched prompt**. `stage_prompt()`
   returns the stage text only; the dispatch path never looks up the profile's behavior.
   The `Stage` struct dropped its Spec 2a `profile` field in implementation. Agents never
   receive their role definition.

### Evidence-bound feasibility (verified, not assumed)

- **Model injection is feasible for all four harnesses** (`--help`): omp `--model=<value>`
  (+ role models `--smol`/`--slow`/`--plan`), opencode `-m`/`--model`, pi `--model`, grok
  `-m`/`--model`. herdr `agent.start [-- AGENT_ARG]` passes args to the agent.
- **Headless/non-interactive mode is feasible for all four**: omp `-p`/`--print` +
  `--mode=json` (+ `acp` server), opencode `run`/`serve`/`acp` + export JSON, pi `-p` +
  `--mode=json`, grok `--output-format json`/`streaming-json`. Each emits structured output.
- **The loop already solved the sync/async boundary via subprocess**: `durable.rs:9` —
  "Leaves shell out to the flywheel binary itself (`__send`) so the blocking herdr socket
  client never runs [in the loop process]." Mail uses a shared `OnceLock<Runtime>`
  (`mail_shim.rs:55`). An in-process runner calling herdr/toron directly from inside the
  engine's fresh `Runtime::new()` (`main.rs:8372`) risks nested-`block_on` deadlock.

### The problem statement

> How do we (a) replace the no-op `ActionRunner` with real execution WITHOUT breaking the
> live `flywheel loop` or the no-op default; (b) inject per-profile models into the spawn
> so model-per-role is real, not advisory; (c) deliver the authored role preambles to agents
> at dispatch time; (d) add a headless execution mode for autonomous/cron work with
> structured run logging; and (e) close the CLI gaps (no `remove` anywhere; no
> `--execute`/`--mode`/`--model`) — all WITHOUT new Nostr kinds or MCP tools, and with the
> ADR-0003 one-way dependency (`flywheel → toron-workflow → toron-core`) intact?

---

## Decision Drivers

1. **ADR-0005's tier model is load-bearing** — execution substrate in `toron-workflow`,
   orchestration vocabulary in `flywheel`, one-way dependency. This ADR adds a wiring layer
   in `flywheel` (the real runner) + a model registry in `flywheel`; it touches `toron-workflow`
   only if an action needs a new primitive (none anticipated).
2. **Frozen kind registry (ADR-0003) + frozen 38-tool MCP surface (ADR-0004).** No new kinds,
   no new tools. All new vocabulary is CLI/config.
3. **The live `flywheel loop` must not break.** The real runner ships behind a flag; the loop
   stays on its proven herdr-direct path until the wired engine is proven, then migrates.
4. **Subprocess-reuse dissolves the sync/async deadlock** (Driver: evidence). The real runner
   shells out to proven leaves, never holding an in-process herdr/toron client.
5. **Headless is a distinct execution model**, not a degraded pane. Its semantics (synchronous,
   local, non-durable, logged) are coherent for autonomous work and MUST NOT fake the pane
   promise↔job lifecycle on the bus.
6. **Backward compatibility.** `triage.yaml`, the ADR-0004 suite, and the live loop pass
   unchanged. The no-op runner stays the default.

---

## Considered Options

### For dispatch mechanism (Spec A)
- **Subprocess-reuse (CHOSEN):** runner shells to `flywheel __send`/`__spawn`/`__run-headless`.
  Reuses the loop's proven sync-safe pattern; dissolves the nested-block_on deadlock; maximal
  code reuse.
- *Direct in-process (rejected):* runner holds herdr Client + toron Mail; risks nested-runtime
  deadlock + reimplements `__send`.
- *Library-extract (deferred):* extract `__send` into a shared lib; code-reuse without
  subprocess overhead, but a refactor of proven loop code + still faces the in-process boundary.

### For headless await semantics (Spec E)
- **Block vs fire-and-forget (CHOSEN):** `await:true`=block on subprocess + capture; `await:false`
  = detached + log-when-done. Matches one-shot semantics.
- *Always-block (rejected):* loses fire-and-forget headless.
- *Synthetic job events (rejected):* fakes the 43001/43004 lifecycle; muddies the bus audit trail.

---

## Decision Outcome

### Spec A — Real ActionRunner (subprocess-reuse, `--execute` flag)

A `RealRunner` replaces the no-op behind `flywheel workflow run --execute` (no-op stays
default). The runner is a thin **sync orchestrator** that shells out to proven leaves:

| Action | Leaf | Effect |
|---|---|---|
| `spawn` / `dispatch_job` (pane mode) | `flywheel __send <project> <target> <prompt> --confirm` | herdr `agent.prompt` (reuse the loop's confirmed-send path) |
| `spawn` (pre-spawn crew) | `flywheel spawn <project> --recipe <crew-recipe>` (or a `__spawn` leaf) | `cmd_spawn` → herdr `agent_start` |
| `send_message` | `flywheel mail send <project> ...` | toron mail (the existing CLI parity surface) |
| `spawn` / `dispatch_job` (headless mode) | `flywheel __run-headless <project> <profile> <prompt> --model X` | harness `-p --mode=json` subprocess; output → run log |

The runner captures `Arc<RealRunnerCtx { project, workspace, crew_registry, catalog, models }>`
— **no in-process herdr/toron client, no nested block_on.** Sync runner calling sync CLI
leaves; each leaf owns its own blocking client + runtime (exactly the loop's architecture).

`WorkflowCmd::Run` gains `--execute` (bool, default false). Without it: today's no-op
(control-flow only, backward compat). With it: real dispatch. The durable loop migration onto
this runner is a **separate, later phase** (the loop is live load-bearing).

### Spec B — Crew pre-spawn + lifecycle

Before invoke, the `CrewPlan` becomes running agents: `CrewPlan → profile::resolve →
flywheel spawn` (reuses `cmd_spawn`) → `agent_start(kind=harness, AGENT_ARG=[--model, X])`.
The engine then **dispatches to the existing crew** (ADR-0005 Spec 4a "dispatch to existing
inbox" — the `ProfileResolver` maps `@role → running npub → pane`).

- **Lifecycle (captain choice: both):** ephemeral default (workspace + crew reaped at run end)
  + `--reuse <workspace>` (dispatch into a standing crew) + `--retain` (keep the ephemeral crew
  after the run). Uniform across cron + on-demand.
- **Durable-resume contract (captain choice: accept herdr-persistence):** ephemeral crew panes
  survive a flywheel crash because **herdr owns the panes** (separate server, own session
  registry). On resume, re-bind via the spawn registry; orphans swept by the existing
  `--retire-orphans` pattern / housekeeper.
- **Crew registry (captain choice: extend existing):** `save_spawn_registry` (`state.rs`)
  gains a **profile column** → resolve `@planner → agent_name → pane_id → npub`. No new state
  artifact.
- **Recorded risk:** frequent cron + ephemeral panes = workspace churn; mitigation = reliable
  reap-at-run-end + periodic orphan sweep (housekeeper). **Largely dissolved by Spec E**
  (cron defaults to headless, which creates no workspace).

### Spec C — Role conditioning (the soul)

At dispatch, **prepend `Profile.behavior` to the step prompt.** The six authored preambles
finally reach agents. The dispatch leaf (`__send` / `__run-headless`) resolves the step's
profile, reads its `behavior`, and constructs `"<behavior preamble>\n\n<step prompt>"`.

- `Stage` **regains its Spec 2a `profile` field** (dropped in implementation). A step may be
  `{ stage: systematic-execute }` → resolves to the stage prompt + the stage's `@coder` profile
  → coder behavior prepended.
- Role conditioning is **mode-agnostic** (applies to both pane and headless dispatch).

### Spec D — Model registry + injection

- **Registry:** `~/.config/flywheel/models.toml` — `[models.<id>]` with `provider`,
  `cost_in_per_mtok`, `cost_out_per_mtok`, `caps`, `fallback`. `Profile.model` resolves against
  it; unknown id → warning + pass-through (never block spawn on a missing registry entry).
- **Injection:** spawn resolves the model from the registry, maps it to the harness's flag via
  a per-harness adapter, passes via herdr `agent.start [-- AGENT_ARG]`:
  - omp: `-- --model=<id>`
  - opencode: `-- -m <provider/model>`
  - pi: `-- --model <pattern>`
  - grok: `-- -m <id>`
- **Single model field this phase.** omp role-models (`--smol`/`--slow`/`--plan`) → phase 2
  (would extend `Profile` with optional `smol`/`slow`/`plan` + per-harness gate).
- **Display + resolution this phase; full routing (cost/capability-aware) deferred** to a
  follow-on that requires a metering pipeline (ADR-0005 ships advisory cost only).

### Spec E — Headless execution mode + run logging

A **general, opt-in** execution mode. Cron implicitly uses it (autonomous); any step can opt in
via `profile.exec = "headless"` or step `mode: headless`.

- **One-shot stateless:** `flywheel __run-headless <project> <profile> <prompt> --model X` runs
  `<harness> -p --mode=json --model=X "<behavior preamble + prompt>"` (adapter per harness),
  captures JSON stdout, writes a run log, exits.
- **await semantics:** `await:true` (default) = block on subprocess + capture JSON →
  `{{steps.<name>}}`; `await:false` = spawn detached, return immediately, log the result to
  `runs/` when it finishes.
- **Non-durable (captain choice: accept):** a crash mid-headless-step loses that step; recovery
  = the next cron tick (idempotent re-run) or manual re-invoke. Headless steps MUST be
  idempotent or short. (Long/federated work uses pane + the durable promise binding.)
- **Local-only (captain choice):** headless emits NO bus events (no 43001/43002/43004). Results
  go to `~/.local/state/flywheel/runs/<run_id>.jsonl` → queryable via `flywheel runs`. Not
  federated/peer-visible. Headless + cross-machine peer work is out of scope.
- **Headless does NOT use the promise↔job binding** (amends ADR-0005 Spec 4a): there is no live
  agent to claim a 43002. The subprocess output IS the result, synchronously. Pane mode keeps
  the durable binding for interactive/federated/long work.

**Run-log schema** (`runs/<run_id>.jsonl`, one JSON object per run):
```json
{ "run_id": "...", "ts": "...", "workflow": "...", "cron": "...?", "profile": "...",
  "harness": "...", "model": "...", "mode": "headless|pane",
  "prompt_hash": "...", "response": "...", "status": "success|fail|timeout",
  "tokens_in": N, "tokens_out": N, "duration_ms": N, "exit_code": N, "error": "...?" }
```
Full prompt/response stored in a sidecar (`runs/<run_id>.full/`) to keep the JSONL scannable.
Feeds `flywheel runs show <id>` (full response) and `flywheel runs digest --since 7d`
(failures/parked/broken>N).

**The two-model separation (the clean outcome):**

| | Pane (Mode P) | Headless (Mode H) |
|---|---|---|
| Wait | durable promise suspend → resume on 43004 | sync subprocess block (or detached) |
| Crash | run survives; agent posts 43004 | step lost; idempotent re-run |
| Visibility | full 43001→43004 on bus (federated) | local-only `runs/` (not bus) |
| Best for | interactive, long, federated | autonomous, cron, short, idempotent |

### Spec F — CLI surface (close the gaps; agent-authorable)

**Existing (keep):** `workflow {list,show,lint,run,new,history,crew}`, `cron {list,show,add,
pause,resume,status,history,start}`, `squad {list,show,spawn}`, `profile {list,show,create,
edit,run}`, `runs {list,show,pending,digest}`, `stage {list,...}`, `recipes list`.

**New (this ADR):**
- `workflow remove <name>` / `cron remove <name>` / `profile remove <name>` / `squad remove
  <name>` — delete from the catalog/config (the named gap: **no `remove` exists today**).
- `workflow run <name> --execute [--mode headless|pane] [--reuse <ws>] [--retain]
  [--model <id>]` — the real runner, headless/pane mode, crew lifecycle, model override.
- `cron add ... --mode headless|pane` — cron owner's execution mode (default headless).
- `model {list,show,add,remove}` — the registry CRUD surface.

**Agent authoring contract:** an agent (or captain) can, via CLI alone: **create** a workflow
(`workflow new` + edit YAML, or `profile create` / `cron add` / a recipe in `recipes.toml`),
**list** (`workflow list` / `cron list` / `squad list` / `profile list`), **select/show**
(`workflow show` / `cron show`), **execute** (`workflow run --execute` / `cron start` /
`squad spawn`), and **remove** (`workflow remove` / `cron remove` / `profile remove`). See
[`docs/workflow-authoring.md`](../../flywheel/docs/workflow-authoring.md) (flywheel repo) for
the practical guide with basic + in-depth examples.

---

## Consequences

### Positive
- **The orchestration tier finally executes.** "Assign crew fleet to workflow" becomes real;
  the no-op runner was the single largest gap hiding behind green ADR-0005 gates.
- **Model-per-role is real**, not advisory — resolved model reaches every spawned harness.
- **Role preambles reach agents** — the authored discipline (200+ words/role) takes effect at
  every dispatch, pane or headless.
- **Headless gives clean cron** — no workspace churn, structured logs, trackable status
  (`flywheel runs`/`digest`).
- **Subprocess-reuse** avoids a new async-runtime footgun and maximizes code reuse.
- **No new kinds/tools** — all invariants hold.

### Negative
- **Subprocess overhead per action** (~ms) — negligible vs agent latency, but real.
- **Headless is non-durable** — acceptable for idempotent cron, but a constraint on long
  headless tasks (must be idempotent or short, or use pane mode).
- **Headless is not federated** — operator-local only; cross-machine headless is out of scope.
- **Cron + ephemeral panes churns workspaces** if a captain forces `--mode pane` on a frequent
  cron — mitigated by headless default + reap + orphan sweep, but a footgun if ignored.
- **Model registry is advisory-cost this phase** — no real metering; dynamic routing deferred.

### Neutral
- **One ADR, two repos.** Engine actions (if any) in `toron-workflow`; runner + registry +
  headless + CLI in `flywheel`. Coordinated via the bead breakdown.

---

## Verification

### Per-spec gates
- **A:** `workflow run <w> --execute` with a `spawn` action starts a real herdr pane (visible
  in `flywheel list`); without `--execute` the no-op still returns `{action:"spawn"}` (backward
  compat). A `dispatch_job` lands a confirmed `__send`; a `send_message` delivers toron mail
  (visible in `flywheel mail inbox`).
- **B:** a workflow with `crew: {planner:1, coder:1}` run `--execute` spawns 2 panes bound to
  those profiles; `--reuse <ws>` dispatches into an existing workspace; `--retain` leaves the
  crew up; a crashed run resumes and re-binds its crew (panes survive in herdr).
- **C:** a `spawn @planner` step's herdr prompt begins with the planner behavior preamble
  (verified by `agent_read` of the first lines); a `stage: systematic-execute` step carries the
  coder preamble.
- **D:** a profile with `model = "claude-sonnet-4"` spawned `--execute` results in the pane
  agent running that model (omp `--model`/opencode `-m` observed in the agent's own model
  display); `model list` shows the registry; unknown model warns but spawns.
- **E:** a headless step (`mode: headless`) produces a `runs/<run_id>.jsonl` with
  response+tokens+status; `await:false` returns before the subprocess finishes and logs later;
  `flywheel runs show <id>` shows the full response; `flywheel runs digest --since 1d` aggregates.
  No bus events emitted for a headless run (local-only verified).
- **F:** `workflow remove <name>` deletes the catalog entry; `cron remove`/`profile remove`/
  `squad remove` likewise; `model add/remove` manages the registry.

### Cross-cutting
- `triage.yaml`, the ADR-0004 suite, and the **live `flywheel loop`** pass unchanged at every
  boundary.
- `flywheel workflow run <name>` (no `--execute`) is byte-identical to today (no-op default).
- `#![forbid(unsafe_code)]` preserved.

---

## Implementation Blueprint

### Phasing (vertical slices, headless-first — revised 2026-08-09 plan-space review)

> **Revision:** the original phasing shipped infrastructure (preamble, registry) *before* the
> runner that makes them observable — a behavior preamble prepended to a no-op prompt does
> nothing; a model injected into a non-spawned agent does nothing. Revised to **vertical
> slices, headless-first**: every phase ships an observable end-to-end result; the *simplest*
> real execution (headless — subprocess + log, no herdr/crew/promise) precedes the *complex*
> pane runner (which refactors the already-working `flywheel loop`). Net-new cron value ships
> before a working-system refactor. Beads: epic **`bd-ghcj`** + children `bd-ghcj.1`–`.4`.

| Phase | Bead | What lands (observable gate) | Deps | Repo |
|---|---|---|---|---|
| **P10.1 — Headless slice** | `bd-ghcj.2` | `__run-headless` leaf + real headless `ActionRunner` (`--execute --mode headless`) + behavior-preamble prepend + run logging. **GATE:** `workflow run plan --execute --mode headless` runs + logs `{response,tokens,status}`; `runs show` displays it. | — | flywheel |
| **P10.2 — Model registry** | `bd-ghcj.3` | `models.toml` + `model {list,show,add}` + per-harness injection into `__run-headless`. **GATE:** a profile's registry model reaches the headless run (logged `model` matches; harness flag observed). | P10.1 | flywheel |
| **P10.3 — Pane runner + crew** | `bd-ghcj.1` | real pane dispatch (`__send`/`spawn`) + crew pre-spawn + `--reuse`/`--retain` + spawn-registry profile column. **GATE:** `workflow run <crew-wf> --execute` spawns a real crew in panes + dispatches with preambles. | — (independent) | flywheel |
| **P10.4 — Cron wiring** | `bd-ghcj.4` | `cron add --mode headless` (default); cron manager drives the headless runner per tick. **GATE:** a registered cron fires headlessly at schedule + logs; no workspace churn. | P10.1+P10.2 | flywheel |
| **Docs/skill/remove CLI** | **SHIPPED 2026-08-09** | authoring guide (`flywheel/docs/workflow-authoring.md`) + flywheel skill + AGENTS.md section + ADR index/README + `workflow/cron/profile/squad remove` (live, verified). | — | flywheel + toron |

> **Execution order:** P10.1 first (the only P1 ready bead that unblocks P10.2/P10.4); P10.3 may
> run in parallel (independent) but is lower priority (refactors working functionality). `bv
> --robot-next` selects P10.1.

### Deferred (explicit, with prerequisites)
- **pa (prime-agent):** herdr `agent start --kind` does not list it (verified). Defer until
  herdr adds it, OR the `spawn_cmd` escape hatch lands (config-driven shell launch).
- **Elastic/dynamic crew:** crash-safe durable — a Resonate-semantics extension (same risk class
  as the ADR-0005 P5 spike). Static pre-spawn crew serves until then.
- **Dynamic model routing:** needs a metering pipeline first (registry → metering → policy →
  routing).
- **Loop migration:** `flywheel loop` onto the wired engine — AFTER P10.3 is proven; the loop
  is live load-bearing.
- **omp role-models** (`--smol`/`--slow`/`--plan`): extends `Profile` schema; phase 2.

### Architectural invariants (every phase obeys)
1. **No new Nostr kind.** No new MCP tool. (ADR-0003/0004.)
2. **No dependency inversion.** `flywheel → toron-workflow → toron-core`, never reverse. The
   real runner lives in `flywheel`; `toron-workflow` gains no `flywheel` import.
3. **Subprocess-reuse for dispatch.** The runner holds no in-process herdr/toron client.
4. **Headless ≠ pane.** Headless never fakes the 43001/43004 lifecycle; it's synchronous +
   local-only.
5. **No-op default.** `workflow run` without `--execute` is byte-identical to today.
6. **The live `flywheel loop` is untouched** until P10.3 is proven.

### Blast radius
- **Context (may change):** `flywheel/src/{main.rs (WorkflowCmd/CronCmd/ProfileCmd/SquadCmd +
  --execute/--mode/--model/--reuse/--retain + remove handlers), profile.rs (exec field, model
  resolve), stages.rs (profile field), crew.rs, state.rs (spawn-registry profile column),
  cron.rs (mode flag)}`; new `flywheel/src/{models.rs, run_log.rs, headless.rs}`; new
  `flywheel/src/main.rs` leaves `__run-headless`. `toron-workflow` unchanged unless an action
  needs a new primitive. Docs: this ADR + flywheel `docs/workflow-authoring.md` + AGENTS.md +
  skill + ADR index/README.
- **Target:** runner/registry/headless/CLI = flywheel; role conditioning = flywheel dispatch
  leaves; engine = toron-workflow (unchanged this ADR).
- **Forbidden:** kind registry values; 38-tool MCP surface; ADR-0003 layering direction;
  `triage.yaml` semantics; the live `flywheel loop`; the no-op default.

---

## Review Log

| Round | Who | Outcome |
|---|---|---|
| **1–4** | Captain grilling (architecture forks) | interchangeable-harnesses → omp-already-done/pa-deferred; flywheel-routes-model; linchpin-first-no-dynamic; both-repos. 3 claims corrected by evidence (omp works, no-op runner, dead preambles). |
| **5** | Captain grilling (execution context) | subprocess-reuse dispatch; herdr-persistence resume; uniform-ephemeral default. |
| **6** | Captain grilling (headless + await seam) | one-shot-stateless headless; general opt-in mode; await=block-vs-fire-and-forget; accept non-durability; local-only (no bus). |
| **Pending** | Captain review of this ADR | — |

---

## More Information

- **Relationship to ADR-0005:** this is the wiring layer ADR-0005 implied but its P3/P4 gates
  (engine-level) didn't cover. ADR-0005's verification table remains honest *for the engine*;
  this ADR closes the flywheel-side execution gap.
- **Relationship to ADR-0003/0004:** no kind/tool changes; all new vocabulary is CLI/config.
- **Revisit triggers:** (a) herdr adds `prime-agent` to `agent start --kind` (un-defers pa);
  (b) a use case forces headless work onto the federated bus (reopens synthetic-job-events); (c)
  the metering pipeline lands (un-defers dynamic routing).

// governed-by: ADR-0003, ADR-0005
