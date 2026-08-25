<!-- governed-by: ADR-0003, ADR-0005, ADR-0014, ADR-0019, ADR-0020, ADR-0022 -->
---
status: proposed
date: 2026-08-25
decision-makers: Captain (bangedorrunt)
consulted: bastani-inc/atomic deep analysis (2026-08-25 — Buffy full-stack comparison: repair loops, graph-first UX, evidence-bearing approvals, verbatim compaction, worktree isolation, hashline safety, permission gates); control_flow.rs DAG scheduler audit (wave-based concurrent dispatch via `ctx.run(run_graph_node).spawn()` with deterministic replay); engine completeness-gap audit (2026-08-25 — the engine is a transport/dispatch SUBSTRATE, not an execution RUNTIME: two sequencing paths, binary `on_error`, string-tagged phases, prose stages, no cancel/timeout); flaky-test production incidents (bd-8xca compose failure, bd-cvv9 chain-gate false positives — both resolved by captain re-dispatch)
informed: flywheel maintainers; toron-workflow engine owners; skill owners (toron, flywheel, runtime-adaptation)
---

# ADR-0024: Graph-First Workflows, Bounded Repair Loops, Evidence-Bearing Approvals, and Bead-Workflow Integration

**Status:** Proposed (2026-08-25)
**Date:** 2026-08-25
**Decision-makers:** Captain (bangedorrunt)
**Extends:** [ADR-0005](0005-orchestration-layer.md) (workflow engine, DAG scheduler, control-flow actions), [ADR-0019](0019-bead-verified-execution-and-principle-playbooks.md) (ledger-gated close, VERIFY fence, 21 engineering principles), [ADR-0020](0020-nostr-two-plane-cli-and-crew-steering.md) (two-plane surface, crew steering), [ADR-0022](0022-phase-native-orchestration.md) (typed `DispatchSpec`, `Phase` enum, one durable execution rail)
**Amends:** ADR-0005 Spec 4b (`graph:` becomes the canonical internal shape; `steps:` desugars to a chain graph at parse time). ADR-0019 Spec 1 (`Verdict` gains a `RepairCycle` variant; a repair loop produces tracked gate rows). ADR-0022 §4 (phases gain a `repair` transition — on failure, route to repair → re-verify, bounded).
**Does not amend:** the frozen MCP tool surface (40 tools: 38 AM-Rust parity + 2 additive, 25 resources), the kind registry (ADR-0014), the one-way dependency (flywheel → toron-workflow → toron-core), the worktree ban (ADR-0019 Spec 3), the identity grammar (ADR-0018), the freeze on new CLI plane-surface nouns (ADR-0020), the atomic reservation conflict-check (ADR-0011).
**Tracker:** beads. Never pull requests.

> **Scope:** make the execution graph the canonical shape. Make failure routable into a bounded repair loop with a different-model verifier. Make approvals carry the run state as structured evidence, not a plain message. Make beads and workflow runs the same thing so every bead close is a workflow terminal and every workflow terminal is a bead close. Build the observability surface so captains and crews can see and steer live runs.

---

## 1. TL;DR

The DAG scheduler exists (`control_flow.rs` `run_graph` — wave-based concurrent dispatch with `needs:` edges and Resonate deterministic replay). It is opt-in; `steps:` is the primary shape. The engine can abort a run (`on_error: fail_run`) and retry a single step (`Action::Retry`), but it cannot **repair → re-verify** with a different model in a bounded cycle — the pattern every real engineering loop needs. Approval suspends the run on a plain `message` string. The operator sees "ok?" with no context. A bead and a workflow run are two parallel timelines that the loop bridges by hand.

The engine is a strong *transport and dispatch substrate* — it moves signed work around and survives crashes — but it is not yet an *execution runtime*: it has two sequencing engines, a binary failure model, string-tagged phases, prose stages, and no cancel. This ADR closes that gap. It adopts five capabilities from bastani-inc/atomic's agent runtime, adapted to our existing Rust/Resonate/Nostr spine:

1. **The graph IS the product.** Every workflow is a DAG. Linear `steps:` is sugar that desugars at parse time. The CLI renders the graph, shows node states, streams live events, and lets captains fork at any checkpoint.
2. **Failures are typed, not swallowed.** An `ErrorKind` taxonomy (transient/logic/blocked/escalate) lets the engine route retry vs repair vs escalate by itself — instead of binary `continue`/`fail_run` decided by the captain after the fact.
3. **Failure routes into bounded repair.** A failed node spawns a repairer agent (different model family, fresh context), re-runs the VERIFY command, and repeats up to `max_cycles`. Only on exhaustion does it escalate to the captain.
4. **Approvals carry evidence.** A suspended run presents the operator with the run tree (which nodes passed, which failed), produced artifacts (commit SHAs, verdicts, receipts), blast radius, and the specific decision question.
5. **Beads and workflow runs are one thing.** A bead is a workflow run. `br create --auto-workflow` generates the YAML from the VERIFY fence and principles. `br close` triggers the terminal approval gate. The kind-46012 terminal event IS the bead close.

The existing DAG scheduler is the spine — we are not building a new engine. We are making the graph the canonical path, adding repair as a first-class `on_error` mode, making approvals evidence-bearing, and uniting the two lifecycles.

---

## 2. The Execution Graph (the product this ADR delivers)

This is the target. Every workflow becomes a graph of typed, gated, repairable nodes. The captain sees this live. The crew executes it. The loop inspects it.

```
                         ┌─────────────────────────────────────┐
                         │     flywheel workflow graph ship     │
                         │     (visual, inspectable, live)      │
                         └──────────────┬──────────────────────┘
                                        │
                   ┌────────────────────┼────────────────────┐
                   │                    │                    │
          ┌────────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐
          │  resolve         │  │  plan           │  │  implement      │
          │  phase: resolve  │──▶  phase: plan    │──▶  phase: impl    │
          │  @scout          │  │  @planner        │  │  @coder         │
          │  → goal statement│  │  → beads + deps  │  │  → commit SHA   │
          └──────────────────┘  └────────┬────────┘  └───────┬─────────┘
                                         │                   │
                                         │  ┌────────────────┤
                                         │  │                │
                                ┌────────▼──▼─────┐  ┌──────▼──────────┐
                                │  review           │  │  verify          │
                                │  phase: review    │  │  phase: verify   │
                                │  @reviewer        │  │  loop runs cmd   │
                                │  → verdict (A/AWC)│  │  → receipt       │
                                │  on_error: repair │  │  on_error: repair│
                                └────────┬──────────┘  └──────┬───────────┘
                                         │                    │
                                         └──────┬─────────────┘
                                                │
                                       ┌────────▼────────┐
                                       │  gate             │
                                       │  request_approval │
                                       │  (human or auto)  │
                                       │  carries evidence │
                                       └────────┬─────────┘
                                                │
                                   ┌────────────┼────────────┐
                                   │            │            │
                          ┌────────▼───┐ ┌──────▼──────┐ ┌──▼──────────┐
                          │  repair     │ │  merge       │ │  escalate   │
                          │  fix→verify │ │  @housekeeper│ │  → captain  │
                          │  bounded N  │ │  commit+push │ │  kind-14 DM │
                          └────────────┘ └──────────────┘ └─────────────┘
```

**Every node is:**
- **Typed** with a `Phase` (resolve/plan/implement/review/verify) — ADR-0022's closed vocabulary
- **Gated** with an optional `guard` (evalexpr evaluated against prior node outputs)
- **Output-producing** — typed artifacts (`commit_sha`, `verdict`, `receipt_path`, `bead_ids`) that downstream nodes reference via `{{nodes.plan.bead_ids}}`
- **Durable** — Resonate checkpoints at each node boundary (crash → resume from last completed node)
- **Repairable** — on failure, route to repair → re-verify, bounded; escalate only on exhaustion

**The entire graph is:**
- **Inspectable** — `flywheel workflow tree <run-id>` shows node states, phases, durations
- **Resumable** — kill -9 mid-run → restart → completes from last checkpoint
- **Forkable** — `flywheel workflow fork <run-id> --at plan` creates a new run seeded from the plan node, trying an alternative implementation path
- **Composable** — child workflows (`call_workflow`) flatten into the parent graph; one inspectable tree

---

## 3. Context and Problem Statement

### 3.1 What already exists (do not rebuild)

| Piece | Location | Job |
| :--- | :--- | :--- |
| DAG scheduler | `crates/toron-workflow/src/engine/control_flow.rs` `run_graph` | Wave-based concurrent node dispatch; `needs:` edges; `ctx.run(run_graph_node).spawn()` per node; deterministic replay across crash-resume |
| `on_error: fail_run` | `control_flow.rs` `run_step` (line 226) | A step with `on_error: fail_run` whose action returns `{ok:false}` aborts the run with `{status:"failed", error, failed_step}` |
| `Action::Retry` | `control_flow.rs` `run_retry` | Retries the SAME step up to `max_attempts` with `backoff_secs` — same model, same action, same context. Useful for transient failures (network, relay); useless for logic bugs. |
| `request_approval` | `control_flow.rs` `run_step` (line 139) | Creates a Resonate promise, parks the run, publishes kind-46011 with the `promise_id`; the operator resolves/rejects → run resumes |
| VERIFY fence + `br gate` | ADR-0019 Spec 1 | Beads carry a `## VERIFY` command; `br gate report` records verdicts; the loop enforces legal-close by reopening unverified beads |
| Bead lifecycle | `flywheel/src/durable.rs` swarm_loop | Poll `br ready`, dispatch to agents, claim-watch in-progress beads, detect `br close`, verify gates, reopen unverified |
| `Phase` enum | `toron-workflow/src/schema.rs` (ADR-0022 §3.1) | Five closed-vocabulary phases: resolve/plan/implement/review/verify; typed `DispatchSpec` with lint rules R-P1..P4 |
| `flywheel runs` | `flywheel/src/runs.rs` | Run board: status, phase column, per-step cost, `runs list --json` |

### 3.2 Measured gaps (evidence, not vibes)

| Gap | Evidence | Real-world consequence |
| :--- | :--- | :--- |
| `steps:` is the primary shape | `run_workflow` dispatches `graph:` only when the field is present (line 43). Every built-in (`builtin_workflows.rs`), every example (`triage.yaml`), and every compose output (`crew.rs`) uses `steps:`. | The mental model is linear. DAG power is hidden. Captains never reach for parallelism. The `ship` built-in (ADR-0022 §3.3) is five sequential compose parts — no concurrent review+verify wave. |
| No repair-after-failure | `on_error` is binary (`Continue` / `FailRun`). `Action::Retry` retries the SAME step on the SAME model. | A failing `cargo test` with `fail_run` aborts the whole ship run. No attempt to fix the test. The captain must see the failure, compose a new run manually, and re-dispatch. This happened twice in production (bd-8xca compose failure, bd-cvv9 chain-gate false positive) — both resolved by captain re-dispatch, not self-healing. |
| Approval carries no evidence | `request_approval` sends a plain `message: String` (control_flow.rs:139). The promise payload is `{ "message": "<the string>" }` — no run state. | The operator sees "Release toron v0.4.0?" — not which nodes passed, what artifacts exist, whether verify ran. A release decision made without knowing verify passed is a \_prove-it-works\_ violation. |
| Bead and workflow are separate | `br close` writes gate rows to SQLite. `build_workflow_run_terminal` emits kind-46012 to the Nostr bus. Neither knows about the other. The loop manually bridges: poll `br list --status closed`, check `br gate report`, reopen if missing. | Two closing ceremonies. A bead can be "closed" with no workflow run at all (manual `br close`). A workflow run can "succeed" without closing its bead. The loop's bridge code is a source of drift (ADR-0007's bug ledger catalogs several bridge mismatches). |
| No live-run observation | Run output is a `serde_json::Value` returned by `run_workflow`. No CLI to inspect an in-flight run, stream node-level events, or fork at a checkpoint. | When a run is parked on approval, the captain cannot see the run tree. When a node fails and the run is still alive (repairing), the captain cannot connect to it. The only signal is the kind-46012 terminal event — which hasn't fired yet. |
| No auto-parallelization | Graph edges are manual `needs:` declarations in YAML. | Two nodes that reserve disjoint files COULD run concurrently, but the captain must know and declare that. The engine could compute it from reservation data. |
| Two sequencing engines | `run_graph` (`control_flow.rs`) owns the DAG; `run_ship_chain` (`phase_chain.rs`) owns `ship`'s `call_workflow` links with a separate `check_phase` gate between links. | One `Workflow` struct, two semantics. Phase contracts fire only on `ship`; a hand-authored graph with the same resolve→plan→implement→review→verify shape gets no contract enforcement. |
| Binary `on_error`, no error taxonomy | `StepOnError` is `Continue \| FailRun`. `ActionRunner` returns `anyhow::Result<Value>`; every failure collapses to `{ok:false, error:"<string>"}`. | The engine can't route failure. "Retry" (same step, transient) vs "repair" (different model, logic bug) vs "escalate" (human) is decided by the captain, not the engine. Self-healing is impossible without an error kind. |
| Phases are string tags | `DispatchSpec.phase` is `Option<String>`. The engine can't tell a plan node from a build node. | The engine can't gate a build on a plan artifact; only the chain runner can, and only for `ship`. |
| Stages are prose | A stage's body is a prompt template; it declares phase/role-affinity/principles but no input/output contract. | Nothing fails loudly if an agent skips the claim→reserve→close sequence inside `systematic-execute`. The stage is advice, not a contract. |
| No run lifecycle beyond checkpoint | `pause` (checkpoint + handoff) and `pickup` (resume) exist. No cancel/quit, no per-action timeout (only `spawn.no_claim_timeout`). | A run can be suspended and resumed but not cancelled or forked at an arbitrary node. A hung `call_webhook` or `dispatch_job` blocks the run forever. |

### 3.3 What Atomic gets right (and what we don't)

Atomic (bastani-inc/atomic) is a coding-agent runtime with an execution graph at its center. It was studied in depth on 2026-08-25. Key findings:

| Atomic capability | Description | Our gap | Our asset |
| :--- | :--- | :--- | :--- |
| **Bounded repair loops** | Failed stage → spawn repairer (different model) → re-verify → repeat up to N → escalate on exhaustion | No repair path. `fail_run` aborts. `retry` is same-step, same-model. | Resonate durability means a repair cycle survives a crash mid-fix. Atomic's checkpointing is coarser. |
| **Graph-first UX** | `workflow({...})` is a DAG. Linear is sugar. Runtime renders the graph, shows node states, lets you fork at checkpoints. | `steps:` is the default. `graph:` is opt-in and unused in production. | Our `run_graph` scheduler does wave-based concurrent dispatch with `needs:` edges — Atomic doesn't have durable child-spawn concurrency. |
| **Evidence-bearing approvals** | Approval gates carry the run tree: which nodes completed, which artifacts exist, what the specific question is. | `request_approval` sends a bare `message` string. | Our approval is a Resonate promise — it survives process death. Atomic's is in-process and lost on crash. |
| **Worktree-isolated parallelism** | Parallel agents each get their own git worktree. | We ban worktrees (ADR-0019 Spec 3). Use file reservations instead. | File reservations detect REAL conflicts (two agents touching the same path). Worktrees clone the world to dodge a queue — token-hungry and a lie about `.beads/` single-writer invariants. |
| **Verbatim compaction** | Compaction deletes low-value transcript lines without rewriting what survives. | Coarse `CONTEXT_LIMIT` at 80% triggers full respawn. | Deferred to a separate ADR (general improvement, not graph-specific). |

**The synthesis:** Atomic's repair-loop, graph-first UX, and evidence-bearing approvals are invariants we should adopt. Its worktree isolation is the wrong answer for our system. Our Resonate durability, file reservations, and bead dependency graphs are the right foundation — we add Atomic's three invariants on top of them without rebuilding the engine.

### 3.4 The problem statement

> How do we make the execution graph the canonical shape — the thing captains compose, crews run, and the loop inspects — add bounded repair loops so failures self-heal instead of aborting, make approvals carry the run state as structured evidence, unite beads and workflow runs into one lifecycle, and build the observability surface so live runs are inspectable, resumable, and forkable — all on the existing Resonate-durable DAG scheduler in `control_flow.rs`, without new MCP tools, without new Nostr kinds, and without worktrees?

---

## 4. Decision Drivers

1. **Build on the existing DAG scheduler.** `run_graph` already does wave-based concurrent dispatch with deterministic replay. This ADR does not rebuild it — it makes it the canonical path and extends it.
2. **No new MCP tools, no new Nostr kinds.** The frozen surfaces stay frozen. New capability is engine-internal schema (`StepOnError::Repair`, richer approval payload) + CLI observability surface. No 40th tool. No kind beyond the existing registry.
3. **One-way dependency.** Flywheel → toron-workflow → toron-core. Never reverse. The engine changes live in toron-workflow; the CLI and orchestration live in flywheel.
4. **Repair is a different model, same tree.** The repairer agent runs in the same project directory, under the same file reservations, on the same commit tree. It does not get a worktree (ADR-0019 Spec 3). The repairer's output is committed to the same branch.
5. **Beads are the tracker.** A workflow run produces a bead; a bead close is a workflow terminal. One lifecycle. The loop dispatches beads as workflow runs, not as raw prompt strings.
6. **Evidence over claims.** An approval gate must present what actually happened — node states, artifact paths, command outputs — not what an agent said happened. A repair cycle must prove the fix with a re-verify, not a self-report.
7. **TDD for new features.** Every spec below requires a failing test before the implementation lands (repo convention).
8. **Principles are cited by name.** This ADR's primary principles: `prove-it-works` (repair proves the fix, approval shows the evidence), `sequence-verifiable-units` (each node is a verifiable unit, each repair cycle is a tracked gate row), `fix-root-causes` (repair fixes the root cause, not the symptom; retry-of-same is not repair), `encode-lessons-in-structure` (the graph IS the structure — not prose about what to run next), `never-block-on-the-human` (auto-repair up to the bound before escalating to captain).
9. **Type the failure model first.** Retry/repair/escalate routing needs an `ErrorKind`. Binary `on_error` is the root cause of both abort-only behavior and the impossibility of self-healing. Type it first (Spec 6), then build repair on it (Spec 1).
10. **One sequencing engine.** The `run_graph` DAG and the `phase_chain.rs` runner must collapse into one path, or phase contracts stay un-enforceable outside `ship`. A phase contract is a node-level `on_error: repair { verify }`, not a separate gate function.

---

## 5. Considered Options

### Option A — Graph-first + error taxonomy + repair loops + evidence approvals + bead integration (CHOSEN)

Four waves on the existing DAG scheduler. Type the failure model (`ErrorKind`) so the engine can route retry/repair/escalate. Add `on_error: repair` to `StepOnError` on top of that taxonomy. Make approvals carry structured context from accumulated `step_outputs`. Unite beads and runs through a single compose path. Build the observation CLI. Collapse the two sequencing engines into one.

### Option B — Status quo (graph stays opt-in, no repair, plain approvals, separate lifecycles)

Rejected. The DAG scheduler is unused in production. `fail_run` aborts without trying to fix — every failure becomes a captain interrupt. Approvals are blind — every approval decision is made without evidence. Beads and runs diverge — every close is a dual ceremony the loop bridges by hand. The four gaps in §3.2 remain open.

### Option C — Full Atomic clone (TypeScript runtime, worktrees, session JSONL)

Rejected. Different identity model (session files vs Schnorr keys — Atomic has no cryptographic identity). Worktrees conflict with ADR-0019 Spec 3 and the `.beads/` single-writer invariant. TypeScript runtime conflicts with the Resonate/Rust spine and the one-way dependency. We take the invariants (repair loop, graph-first UX, evidence approvals), not the implementation.

### Option D — Repair as a new `Action::Repair` (not an `on_error` mode)

A new action type that a captain composes into the graph manually: "if build fails, run repair." Rejected. Repair is a **failure mode** of a node, not a standalone action a captain pre-wires. A captain shouldn't have to predict which nodes will fail and wire repair edges by hand for every workflow. The engine routes into repair when a node fails — same as `fail_run` routes into abort. The captain declares the policy; the engine executes it.

### Option E — Repair without a taxonomy (route on error-string matching)

Keep the binary `on_error` and add repair by matching `error` substrings ("flaky", "timeout", "assertion failed") to decide retry vs repair vs escalate. Rejected. String matching is fragile: it breaks when the message wording changes, can't distinguish a transient relay timeout from a logic bug in the same wording family, and pushes routing knowledge into the runner instead of a type. The `ErrorKind` taxonomy is the prerequisite — repair routing is impossible without it, and this ADR types it first.

---

## 6. Decision Outcome

Chosen option: **A** — four waves, each independently useful, each building on the existing DAG scheduler in `control_flow.rs`.

### Spec 0 — Graph-first canonical shape (Wave 1)

The `graph:` field becomes the canonical internal representation. `steps:` desugars to a chain graph at parse time. Every workflow is a DAG internally. The engine always routes through `run_graph`.

**Schema change (`toron-workflow/src/schema.rs`):**

```rust
impl Workflow {
    /// The canonical graph representation. `steps:` desugars to a chain;
    /// `graph:` is used as-is. Every workflow has a graph.
    pub fn to_graph(&self) -> Graph {
        if let Some(ref graph) = self.graph {
            return graph.clone();
        }
        // Desugar linear steps into a chain graph: each step depends on
        // the previous named step, or the root (no needs) for the first.
        // Unnamed steps get synthetic names (step_0, step_1, ...).
        let mut nodes = BTreeMap::new();
        let mut prev: Option<String> = None;
        for (i, step) in self.steps.iter().enumerate() {
            let name = step.name.clone()
                .unwrap_or_else(|| format!("step_{i}"));
            let needs: Vec<String> = prev.iter().cloned().collect();
            nodes.insert(name.clone(), GraphNode {
                action: step.action.clone(),
                needs,
                guard: step.guard.clone(),
                on_error: step.on_error,
                build_slot: step.build_slot,
            });
            prev = Some(name);
        }
        Graph { nodes }
    }
}
```

**Engine change:** `run_workflow` ALWAYS calls `run_graph(ctx, trigger, &workflow.to_graph())`. The old `steps:` path in `run_workflow` (lines 43–58) becomes dead code after the migration tests pass — deleted in the same wave per `migrate-callers-then-delete-legacy-apis`.

**Backward compatibility:** existing `steps:` YAML parses identically. The run produces identical step outputs under identical `steps.<name>` keys. A linear workflow produces a chain graph with one node per wave (exactly the same sequential execution). The only behavioral change is that a workflow with independent nodes (no `needs:`) would now run them concurrently within the same wave — but such workflows don't exist today because `steps:` is inherently sequential. The desugar preserves that property.

**Visualization:** `flywheel workflow graph <name>` renders the DAG showing nodes, edges, phases, and wave groupings. ASCII in the terminal; optional HTML artifact for review. Example output for the `ship` built-in:

```
ship (5 nodes, 3 waves)
│
├─ Wave 1 ─────────────────────────────────────────────
│  ◌ resolve          phase=resolve   @scout
│
├─ Wave 2 ─────────────────────────────────────────────
│  ◌ plan             phase=plan      @planner    needs: [resolve]
│
├─ Wave 3 ─────────────────────────────────────────────
│  ◌ implement        phase=implement @coder      needs: [plan]
│
├─ Wave 4 ─────────────────────────────────────────────
│  ◌ review           phase=review    @reviewer   needs: [implement]
│  ◌ verify           phase=verify    (loop)      needs: [implement]
│
├─ Wave 5 ─────────────────────────────────────────────
│  ◌ gate             approval        captain     needs: [review, verify]
│
└─ Wave 6 ─────────────────────────────────────────────
   ◌ merge            phase=verify    @housekeeper needs: [gate]
```

Live run rendering replaces ◌ with ✓ (completed), ✗ (failed), ◌ (in-progress), ⏸ (suspended/approval), ↻ (repairing).

---

### Spec 1 — Bounded repair loops (Wave 1)

A graph node can declare `on_error: repair`. When the node's action fails (returns `{ok: false, error: "..."}`), the engine does not abort the run. It spawns a repair step — a different model, fresh context — gives it the failure details, re-runs the VERIFY command, and repeats up to `max_cycles`. On exhaustion, it escalates to the captain.

#### 6.1.1 The repair cycle state machine

```
                    ┌──────────────────────────────┐
                    │  Node action returns          │
                    │  { ok: false, error: "..." }  │
                    └─────────────┬────────────────┘
                                  │
                    ┌─────────────▼────────────────┐
                    │  on_error == Repair?          │
                    └─────────────┬────────────────┘
                           No │         │ Yes
                    ┌─────────▼──┐  ┌───▼──────────────────────┐
                    │ Continue / │  │ Record RepairCycleStarted │
                    │ FailRun    │  │ { cycle: 1, error, bead } │
                    └────────────┘  └─────────────┬─────────────┘
                                                  │
                                    ┌─────────────▼──────────────┐
                                    │  Spawn repairer agent       │
                                    │  profile: repair_profile    │
                                    │  model: repair_model        │
                                    │  task: "Fix root cause of:  │
                                    │    {error}. Run: {verify}"  │
                                    └─────────────┬──────────────┘
                                                  │
                                    ┌─────────────▼──────────────┐
                                    │  Await repairer result      │
                                    │  (dispatch_job → 43001→06) │
                                    └─────────────┬──────────────┘
                                                  │
                                    ┌─────────────▼──────────────┐
                                    │  Run VERIFY command         │
                                    │  (headless, under build     │
                                    │   slot, same reservations)  │
                                    └─────────────┬──────────────┘
                                                  │
                              ┌───────────────────┼───────────────────┐
                              │                   │                   │
                    ┌─────────▼──┐     ┌──────────▼──────┐   ┌───────▼──────────┐
                    │ exit 0     │     │ exit ≠ 0        │   │ repairer failed  │
                    │ (PASS)     │     │ (FAIL)          │   │ (no result)      │
                    └─────┬──────┘     └──────────┬──────┘   └───────┬──────────┘
                          │                       │                   │
              ┌───────────▼──────────┐  ┌─────────▼────────────┐     │
              │ Record               │  │ Record               │     │
              │ RepairCycleSucceeded │  │ RepairCycleFailed    │     │
              │ { cycle, receipt }   │  │ { cycle, error }     │     │
              └───────────┬──────────┘  └─────────┬────────────┘     │
                          │                       │                   │
              ┌───────────▼──────────┐  ┌─────────▼────────────┐     │
              │ Node output =        │  │ cycle < max_cycles?   │     │
              │ repair result        │  └────┬────────────┬─────┘     │
              │ Continue graph ──────►      Yes │        No │          │
              └──────────────────────┘  ┌───────▼──┐ ┌───▼──────────▼─┐
                                        │ incr     │ │ Record          │
                                        │ cycle    │ │ RepairExhausted │
                                        │ go to    │ │ Escalate to     │
                                        │ spawn ───► │ captain         │
                                        └──────────┘ │ (kind-14 DM)    │
                                                     │ Node output =   │
                                                     │ { ok: false,    │
                                                     │   error: "..." }│
                                                     │ Outer on_error  │
                                                     │ policy applies  │
                                                     └─────────────────┘
```

#### 6.1.2 YAML schema

```yaml
name: ship-toron
description: "Full ship chain for toron release"
graph:
  nodes:
    build:
      action: dispatch_job
      assignee: "@crew.coder"
      spec:
        phase: implement
        task: "Implement {{inputs.spec}}"
      on_error:
        repair:
          max_cycles: 3
          repair_profile: "@coder"          # defaults to node's assignee
          repair_model: "claude-opus-4"     # optional; different model family
          verify: "cargo test -p {{inputs.crate}} -- {{inputs.test_filter}}"
```

#### 6.1.3 Rust type

```rust
/// A node's failure policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOnError {
    /// Capture the error per-step and continue the run (default).
    Continue,
    /// Fail the run: deterministic durable return, not a panic.
    FailRun,
    /// Bounded repair: on failure, spawn a repairer (different model),
    /// re-run verify, repeat up to max_cycles. Escalate on exhaustion.
    Repair {
        /// Maximum repair cycles (1..10; lint-enforced).
        max_cycles: u32,
        /// Profile for the repairer agent (defaults to the failing node's assignee).
        #[serde(default)]
        repair_profile: Option<String>,
        /// Optional model override (different model family for genuine independence).
        #[serde(default)]
        repair_model: Option<String>,
        /// The VERIFY command. Exit 0 = repair succeeded.
        verify: String,
    },
}
```

#### 6.1.4 Engine behavior (control_flow.rs)

The repair loop is a new function `run_repair_cycle` called from `run_graph_node` when the node's `on_error` is `Repair` and the action returns `{ok: false}`. Pseudocode:

```rust
async fn run_repair_cycle(
    ctx: &Context,
    node_name: &str,
    repair: &RepairConfig,
    original_error: &str,
    trigger: &Value,
) -> Result<Value> {
    for cycle in 1..=repair.max_cycles {
        // 1. Record cycle start (kind-9 post in run channel)
        emit_repair_event(ctx, RepairCycleStarted { cycle, node: node_name, error: original_error });

        // 2. Spawn repairer agent (dispatch_job via the ActionRunner)
        let repair_result = dispatch_repairer(ctx, repair, original_error, cycle).await?;

        // 3. Run VERIFY command (headless, under the build slot)
        let verify_result = run_verify(ctx, &repair.verify).await?;

        // 4. If verify passes, repair succeeded → return the repair result
        if verify_result.exit_code == 0 {
            emit_repair_event(ctx, RepairCycleSucceeded { cycle, receipt: verify_result.stdout });
            record_br_gate(ctx, "repair-cycle-succeeded", cycle);
            return Ok(repair_result);
        }

        // 5. Verify failed → record and continue to next cycle
        emit_repair_event(ctx, RepairCycleFailed { cycle, error: verify_result.stderr });
        record_br_gate(ctx, "repair-cycle-failed", cycle);
    }

    // Exhausted: all cycles failed
    emit_repair_event(ctx, RepairExhausted { max_cycles: repair.max_cycles });
    escalate_to_captain(ctx, node_name, repair.max_cycles, original_error);
    Ok(json!({ "ok": false, "error": format!("repair exhausted after {} cycles", repair.max_cycles) }))
}
```

Each repair cycle is a distinct tracked work unit:
- A kind-9 post in the run's channel (`bead:<run-bead-id>`) summarizes the attempt
- A `br gate report` row records the verify outcome (pass or fail) with cycle number
- The run tree shows repair cycles as child nodes of the failed node

#### 6.1.5 Lint rules

| Rule | Condition | Severity |
| :--- | :--- | :--- |
| R-R1 | `max_cycles` must be 1..10. 0 or >10 is a lint error. | Error |
| R-R2 | `verify` must be a non-empty command string. | Error |
| R-R3 | A `Repair` node nested inside another `Repair` node. One level of repair; if the repair fails, escalate — don't recurse. | Error |
| R-R4 | `max_cycles` exceeds `FLYWHEEL_REPAIR_MAX_CYCLES` (env, default 10). The runtime cap applies regardless of YAML value. | Warning |
| R-R5 | `repair_model` names a model that is the SAME family as the failing node's model. The value of repair is in a different perspective. | Warning |

#### 6.1.6 What repair IS and IS NOT

**Repair IS:**
- A different model (or at minimum a different agent) looking at the failure with fresh context
- Bounded (max_cycles + runtime cap prevent unbounded spend)
- Verified (each cycle re-runs the VERIFY command; no self-report)
- Tracked (each cycle is a `br gate report` row + kind-9 post)
- Escalated on exhaustion (captain gets kind-14 DM with full repair log)

**Repair IS NOT:**
- A retry of the same step on the same model (`Action::Retry` already does this for transient failures)
- Unbounded (the env cap `FLYWHEEL_REPAIR_MAX_CYCLES` is the hard stop regardless of YAML)
- A worktree (the repairer works in the same tree, under the same reservations)
- A self-report (the engine runs VERIFY; the repairer's claim of "done" is not enough)
- Silent (every cycle is a kind-9 post; exhaustion is a kind-14 DM to captain)

#### 6.1.7 Principles encoded

| Principle | How repair encodes it |
| :--- | :--- |
| `prove-it-works` | Each cycle re-runs VERIFY. A repair that doesn't re-verify is not a repair — it's a retry (which `Action::Retry` handles). |
| `fix-root-causes` | The repairer prompt includes the exact error. "Fix the root cause, not the symptom." |
| `sequence-verifiable-units` | Each cycle is a verifiable unit: fix → verify → gate row. |
| `never-block-on-the-human` | Auto-repair up to the bound. Only escalate on exhaustion. |
| `encode-lessons-in-structure` | The repair loop is a type (`StepOnError::Repair`), not a prose instruction in the task prompt. |

---

### Spec 2 — Evidence-bearing approvals (Wave 2)

`request_approval` carries structured context: the run tree state, produced artifacts, blast radius, and the specific decision question. The operator sees evidence, not a bare string.

#### 6.2.1 The approval payload

When a graph node hits `request_approval`, the engine builds a context object from the run's accumulated state and publishes it as the promise payload:

```json
{
  "run_id": "wf-2026-08-25-001",
  "workflow": "ship-toron",
  "suspended_at": {
    "node": "approve-release",
    "wave": 5
  },
  "question": "Release toron v0.4.0 to crates.io?",
  "context": {
    "nodes_completed": ["resolve", "plan", "implement", "review", "verify"],
    "nodes_failed": [],
    "nodes_pending": ["merge"],
    "repair_cycles": [
      { "node": "verify", "cycle": 1, "outcome": "failed", "error": "2 tests failed" },
      { "node": "verify", "cycle": 2, "outcome": "succeeded", "receipt": ".flywheel/receipts/wf-001-verify-cycle2.txt" }
    ],
    "artifacts": [
      { "node": "resolve", "kind": "goal_statement", "thread_id": "thread-abc" },
      { "node": "plan",    "kind": "beads", "ids": ["bd-123", "bd-124", "bd-125"], "dep_cycles_empty": true },
      { "node": "implement", "kind": "commit", "sha": "abc1234", "bead": "bd-123" },
      { "node": "review", "kind": "verdict", "outcome": "AGREE", "reviewer": "BlueLake", "findings": 3 },
      { "node": "verify", "kind": "receipt", "command": "cargo test --workspace", "exit": 0, "path": ".flywheel/receipts/wf-001-verify-cycle2.txt" }
    ],
    "blast_radius": "Publishes to crates.io. Irreversible. Affects all downstream consumers.",
    "timeout": "PT24H"
  }
}
```

The `context` is built from:
- **Graph topology**: which nodes have completed, failed, are pending (from the DAG scheduler's `done` set and `step_outputs`)
- **Accumulated outputs**: every node's output is available under `nodes.<name>`; the engine selects the fields that matter for a human decision (commit SHA, verdict outcome, receipt path)
- **Repair history**: if any node went through repair cycles, the cycle log is included
- **Blast radius**: from the workflow's `blast_radius` field or the YAML `## BLAST_RADIUS` fence
- **Timeout**: auto-reject deadline (default 24h; configurable per step via `approval_timeout_secs`)

The `question` field is the rendered template from the YAML `message` — it's the human-facing question, now surrounded by the evidence needed to answer it.

#### 6.2.2 CLI surface

| Command | Behavior |
| :--- | :--- |
| `flywheel workflow approvals pending [--project <slug>]` | List all runs currently suspended on approval. Shows run_id, workflow name, suspended node, question, elapsed time since suspension. |
| `flywheel workflow approve <run-id> [--note "..."]` | Approve the suspended run. Resolves the Resonate promise with `{ outcome: "approved", note: "..." }`. The run resumes from the suspended node. |
| `flywheel workflow reject <run-id> [--note "..."]` | Reject the suspended run. Resolves the Resonate promise with `{ outcome: "rejected", note: "..." }`. The run records the rejection and terminates with `status: "rejected"`. |

These wrap the existing `resolve_approval` / `reject_approval` engine methods. The approval CLI already works via the kind-46011 listener (operator publishes kind-46011 approval, the bus subscriber resolves the promise). This gives it a run-id-based surface with the evidence visible before the decision.

#### 6.2.3 What evidence-bearing approvals enable

Before: operator types `flywheel workflow approve wf-001` blind. The run might have failing tests, unreviewed code, or an incomplete plan. The operator trusts the agent's word.

After: operator runs `flywheel workflow approvals pending`, sees the full context, verifies that verify passed and review returned AGREE, then approves. The decision is made on evidence.

---

### Spec 3 — Bead-workflow integration (Wave 3)

A bead IS a workflow run. One lifecycle. Every bead close is a workflow terminal. Every workflow terminal is a bead close. The loop dispatches beads as workflow runs, not as raw prompt strings.

#### 6.3.1 The bridge: bead → workflow YAML

```
┌─────────────────────────────────────────────────────────────┐
│  BEAD (br SQLite)                                           │
│                                                             │
│  id: "bd-123"                                               │
│  title: "Fix reservation conflict check"                    │
│  priority: 1                                                │
│  type: bug                                                  │
│  description:                                               │
│    The conflict check in toron-core/src/reservations.rs     │
│    compares the wrong field. Held reservations from         │
│    different agents on different paths collide.             │
│                                                             │
│    ## VERIFY                                                │
│    ```bash                                                  │
│    cargo test -p toron-core reservation_conflict_detection  │
│    ```                                                      │
│                                                             │
│    ## PRINCIPLES                                            │
│    prove-it-works — ran the VERIFY fence, not a self-report │
│    fix-root-causes — the conflict check was comparing       │
│      holder_name instead of path                            │
│                                                             │
│    ## BLAST_RADIUS                                          │
│    toron-core/src/reservations.rs only                      │
│                                                             │
│  status: open                                               │
│  depends_on: []                                             │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │  br create --auto-workflow
                           │  (or: loop detects ready bead
                           │   and auto-composes)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  WORKFLOW (YAML, auto-generated)                            │
│                                                             │
│  name: "bead-bd-123"                                        │
│  description: "Fix reservation conflict check"              │
│  trigger: null                    # invoke-only             │
│  principles: [prove-it-works, fix-root-causes]              │
│  inputs:                                                    │
│    - name: bead_id                                          │
│      default: "bd-123"                                      │
│    - name: task                                              │
│      default: "Fix reservation conflict check"              │
│  graph:                                                     │
│    nodes:                                                   │
│      implement:                                             │
│        action: dispatch_job                                 │
│        assignee: "@crew.coder"                              │
│        spec:                                                │
│          phase: implement                                   │
│          task: "Implement bead bd-123: Fix reservation      │
│                 conflict check. The conflict check in        │
│                 toron-core/src/reservations.rs compares     │
│                 the wrong field."                            │
│          plan: "bd-123"                                     │
│                                                             │
│      verify:                                                │
│        needs: [implement]                                   │
│        action: dispatch_job                                 │
│        assignee: "@crew.coder"                              │
│        spec:                                                │
│          phase: verify                                      │
│          task: "Run the VERIFY command for bead bd-123"     │
│        on_error:                                            │
│          repair:                                            │
│            max_cycles: 3                                    │
│            verify: "cargo test -p toron-core                │
│                      reservation_conflict_detection"        │
│                                                             │
│      gate:                                                  │
│        needs: [verify]                                      │
│        action: request_approval                             │
│        message: "Bead bd-123 complete. VERIFY passed.       │
│                  Close bead?"                               │
└─────────────────────────────────────────────────────────────┘
```

#### 6.3.2 The lifecycle: one timeline

```
br create --auto-workflow "Fix X" -p 1 -t bug
  │
  │  (bead status: open, workflow YAML written to bead metadata)
  │
  ▼
loop detects bead ready (br ready --json)
  │
  │  (compose_workflow from bead metadata → Workflow struct)
  │
  ▼
engine.invoke("bead-bd-123", workflow, inputs)
  │
  │  (Resonate durable run starts)
  │
  ├─ implement node dispatched → @coder
  │   │
  │   ├─ coder commits with "bd-123" in message
  │   └─ node output: { commit_sha: "abc1234" }
  │
  ├─ verify node dispatched → @coder (runs VERIFY)
  │   │
  │   ├─ test fails (first attempt) → on_error: repair
  │   ├─ repair cycle 1: fix the test, re-verify → PASS
  │   └─ node output: { receipt_path: ".flywheel/receipts/...", exit: 0 }
  │
  ├─ gate node → request_approval
  │   │
  │   │  (run SUSPENDED on Resonate promise)
  │   │
  │   ├─ captain: flywheel workflow approvals pending
  │   ├─ captain reviews: implement ✓, verify ✓ (1 repair cycle)
  │   ├─ captain: flywheel workflow approve bead-bd-123
  │   │
  │   └─ (run RESUMES)
  │
  └─ terminal: kind-46012 emitted
       │
       │  { status: "succeeded", output: { implement: {...}, verify: {...}, gate: {...} } }
       │
       ├─ br gate report bd-123 --gate workflow-verified --status pass
       ├─ br close bd-123 --commit-sha abc1234 --reason "..."
       │
       └─ loop: claim-watch releases (bead closed, workflow terminal observed)
```

#### 6.3.3 The bridge rules

1. **`br create --auto-workflow`** generates a workflow YAML from the bead's `## VERIFY` fence, `## PRINCIPLES` names, `## BLAST_RADIUS` globs, type (bug/feature/task → maps to phases), and priority (P0–P1 gets mandatory reviewer node). The YAML is stored in the bead's metadata (a new `workflow_yaml` field in the beads schema). `--workflow <path>` uses an explicit YAML file instead.

2. **The loop dispatches beads as workflow runs.** When the loop sees a ready bead with `workflow_yaml` metadata, it calls `compose_workflow` + `engine.invoke` instead of the old `bead_prompt` string dispatch (already deleted per ADR-0019 Spec 1). The bead's `## PRINCIPLES` names are lint-checked against the 21-name registry. Missing VERIFY fence → refuse to dispatch (existing rule, unchanged).

3. **The terminal gate closes the bead.** The workflow's final node is a `request_approval` gate. For cheap P2+ beads with a loop-runnable VERIFY, the gate auto-approves (the loop ran VERIFY itself — ADR-0019 Spec 1 cheap band). For P0–P1 or judgment beads, the gate suspends for captain approval. On approval, the engine emits kind-46012 terminal, records `br gate report --gate workflow-verified`, and calls `br close`.

4. **The loop's claim-watch listens for kind-46012.** Instead of polling `br list --status closed` and grep-ing for gate rows, the loop subscribes to kind-46012 events for its owned runs. A terminal event with `status: "succeeded"` releases the claim-watch entry. A terminal with `status: "failed"` reopens the bead and mails the captain.

5. **Manual beads still work.** A bead created without `--auto-workflow` (quick `br q` or legacy) has no `workflow_yaml` metadata. The old `br gate report` path applies. Both paths coexist; the workflow path is the preferred one for loop-dispatched work. Over time, the loop should auto-compose workflows for any ready bead that lacks one (Wave 3 stretch goal).

6. **`flywheel workflow run bead-<id>`** resumes a stranded bead's workflow run from the last Resonate checkpoint. The command to run after a crash. Thin wrapper: looks up the bead's `workflow_yaml`, calls `engine.invoke` with the same `run_id`, Resonate deduplicates and replays from the last checkpoint.

---

### Spec 4 — Workflow observability surface (Wave 4)

A CLI surface for inspecting, steering, resuming, and forking live runs. Builds on the existing `flywheel runs` board and the Resonate promise store.

| Command | Behavior | JSON support |
| :--- | :--- | :--- |
| `flywheel workflow tree <run-id>` | ASCII tree of the DAG: ✓ completed, ✗ failed, ◌ in-progress, · pending, ⏸ suspended/approval, ↻ repairing. Each node shows phase, assignee, duration, and repair cycle count if applicable. | `--json` emits the tree as a nested object |
| `flywheel workflow status <run-id>` | One-line summary: state, completed/total nodes, suspended node (if any), elapsed time, current repair cycle (if any). | `--json` for machine consumption |
| `flywheel workflow connect <run-id>` | Stream node-level events to stdout as JSON lines: `NodeStarted`, `NodeCompleted`, `NodeFailed`, `RepairCycleStarted`, `RepairCycleSucceeded`, `RepairCycleFailed`, `RepairExhausted`, `ApprovalSuspended`, `ApprovalResolved`. Like `tail -f` for a workflow run. `--timeout-ms` required. | Always JSON lines |
| `flywheel workflow resume <run-id>` | Resume a crashed/killed run from the last Resonate checkpoint. Thin wrapper over Resonate's built-in resume. | `--json` prints final outcome |
| `flywheel workflow fork <run-id> --at <node>` | Fork a run at a completed node: create a new run seeded from the parent run's state up to that node, with a new tail YAML. `--kill-original` terminates the parent. Forked runs carry `parent_run_id` in kind-46012. | `--json` prints the new run_id |
| `flywheel workflow approvals pending` | List all runs currently suspended on approval, across all projects. Shows run_id, workflow, node, question, elapsed. `--project <slug>` to filter. | `--json` default |
| `flywheel workflow approve <run-id> [--note]` | Approve the suspended run. | `--json` prints outcome |
| `flywheel workflow reject <run-id> [--note]` | Reject the suspended run. | `--json` prints outcome |

**`fork` mechanics:** the engine serializes the `RunInput` up to the fork point (all node outputs accumulated before and including the named node). A new `run_workflow` is spawned with a new `run_id`, the cloned state as initial `step_outputs`, and the new tail YAML provided by the captain. The original run is untouched unless `--kill-original` is passed. Deterministic replay is per-run — fork does not affect the parent's replay.

---

### Spec 5 — Auto-parallelization from reservation data (Wave 4)

The graph analyzer can compute edges from file reservations. Two nodes that reserve disjoint file sets have no data dependency and can run concurrently.

**Mechanism:** `flywheel workflow lint` inspects each node's `blast_radius` (from the YAML `## BLAST_RADIUS` fence or the `DispatchSpec.plan` field's bead metadata). Nodes with disjoint file globs that have no explicit `needs:` edge are flagged as `independent`:

```
Nodes implement-auth and implement-rate-limit are independent
  (disjoint files: src/auth/ vs src/rate_limit/)
  → consider removing explicit serial edge or confirming it's intentional

Nodes implement-auth and fix-auth-bug conflict on src/auth/handler.rs
  → serial edge required (implicit from reservation overlap)
```

The engine does NOT automatically add or remove edges — the lint ADVISES. The captain confirms or overrides. This is `separate-before-serializing` applied at the graph level: the lint finds false sharing, the captain decides whether to split or serialize.

---

### Spec 6 — Error Taxonomy and Execution-Model Closure (foundational; feeds Spec 1 and Spec 4)

*Foundational, discovered during the 2026-08-25 completeness audit. Spec 6 is
listed after Spec 5 because the error taxonomy was the last gap identified,
but it is the routing substrate that Spec 1 (repair) and Spec 4 (observability)
consume. Repair routing is impossible without it: you cannot route a failure
to "repair" vs "retry" vs "escalate" when every failure is an undifferentiated
`anyhow::Error` collapsed into `{ok:false, error:"<string>"}`.*

#### 6.6.1 The error taxonomy

`ActionRunner` returns `anyhow::Result<Value>`; every failure collapses to a
string. The engine cannot distinguish a transient relay timeout from a logic
bug from a blocked reservation. That is the root cause of binary `on_error` —
with no error kind, the only choices are "abort" or "swallow."

```rust
/// Why an action failed. The engine routes on this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorKind {
    /// Transient: retry the SAME step on the SAME model (network, relay,
    /// API rate limit, lock contention). `Action::Retry` is the consumer.
    Transient,
    /// Logic: the step's model got it wrong (wrong test assertion, missed
    /// edge case, bad assumption). Route to repair (different model).
    Logic,
    /// Blocked: the environment won't allow progress (reservation held,
    /// build slot busy, dependency missing). Route to escalate — a repair
    /// cannot unblock a held reservation.
    Blocked,
    /// Escalate: an explicit call for human judgment, or a bound exhausted.
    /// Route straight to the captain. Never auto-repair.
    Escalate,
}

/// The ActionRunner return type. Replaces `anyhow::Result<Value>`.
pub type ActionResult = Result<Value, ActionError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionError {
    pub kind: ErrorKind,
    pub message: String,
    /// Optional structured detail (which reservation, which relay, which test).
    #[serde(default)]
    pub detail: Option<Value>,
}
```

Routing table (the engine, not the captain, decides):

| `ErrorKind` | `on_error` default | Mechanism |
| :--- | :--- | :--- |
| `Transient` | retry (bounded backoff) | `Action::Retry` (same step, same model) |
| `Logic` | repair (bounded, different model) | `StepOnError::Repair` (Spec 1) |
| `Blocked` | escalate | kind-14 DM to captain naming the blocker |
| `Escalate` | escalate | kind-14 DM to captain |

`on_error` remains an override: `on_error: continue` swallows (and records) any
kind; `on_error: fail_run` aborts on any kind; `on_error: repair` overrides the
`Logic` route for a specific node. But the DEFAULT, absent `on_error`, is now
routed by kind — not silently `continue` as today. This is the behavioral flip
that makes the engine self-healing by default instead of abort-or-swallow.

**Backward compatibility:** existing YAML with no `on_error` changes meaning
from "continue on any failure" to "route by kind." That is a deliberate
behavior change with a migration gate (Wave 1 verification): every existing
built-in must carry an explicit `on_error` before the default flips, so no
workflow changes behavior implicitly. Until that gate is green, the routed
default is warn-first — a routed action logs its `ErrorKind` and the routing
it *would* have taken, but falls back to today's `continue` behavior.

**Lint rules:**
- R-E1: an action returning `ErrorKind::Blocked` or `Escalate` must not be
  wrapped in `Action::Retry` (a retry cannot unblock a held reservation).
- R-E2: `on_error: repair` on a node whose action can only fail `Transient`
  (e.g. a bare `call_webhook`) is a lint warning — repair is for `Logic`
  failures; a transient-only action should use `retry`.
- R-E3: the `ErrorKind` default routing is warn-first until the migration
  gate (§8 Wave 1) is green; flipping to routed-by-default is a separate
  bead, one line, tracked.

#### 6.6.2 One sequencing engine (retire `phase_chain.rs`)

Two engines sequence the same `Workflow` struct today:

- `run_graph` (`control_flow.rs`) — the DAG scheduler: waves, `needs:`, `on_error`.
- `run_ship_chain` (`phase_chain.rs`) — runs `ship`'s `call_workflow` links
  through `engine.invoke` with its own `check_phase` gate between links.

Consequence: phase contracts fire only on `ship`; a hand-authored graph with
the same resolve→plan→implement→review→verify shape gets no contract
enforcement. One `Workflow`, two semantics.

**Retirement:** `run_ship_chain` and its `check_phase` gate collapse into the
DAG scheduler. A phase completion contract becomes a node-level
`on_error: repair { verify }`:

| Phase contract (ADR-0022 §4.5) | Node-level encoding |
| :--- | :--- |
| `resolve` — 4 fields present | `on_error: repair { verify: "<goal statement has target, non_goals, blast_radius, open_questions>" }` |
| `plan` — beads + dep_cycles=0 | `on_error: repair { verify: "br dep cycles --json | count == 0" }` |
| `implement` — commit cites bead | `on_error: repair { verify: "git log -1 --grep=<bead-id>" }` |
| `review` — verdict in {AGREE, AWC, REJECT} | `on_error: repair { verify: "<verdict parsed>" }` |
| `verify` — gate exit 0 | `on_error: repair { verify: "<the gate>" }` |

The §4.5 completion contracts are preserved — they move from
`phase_contracts.rs` (flywheel-side, one path) into the engine's `ErrorKind` +
`repair.verify` (engine-side, all paths). `FLYWHEEL_SHIP_GATE_MODE` warn →
block becomes the DAG-level escalation policy: on `Logic` exhaustion, warn
(continue) or block (`fail_run`), matching today's flip.

#### 6.6.3 Phase as engine semantics + typed stage contracts

Two related gaps:

1. **`Phase` is a string tag.** `DispatchSpec.phase` is `Option<String>`. The
   engine cannot tell a plan node from a build node, so it cannot gate a
   build on a plan artifact. This ADR does NOT re-open the closed vocabulary
   (ADR-0022 §4) — the five values stay. It makes the ENGINE consume the tag:
   `run_graph_node` reads `spec.phase` and applies the phase's default
   `on_error` routing and (Wave 4) the plan-artifact warn gate
   (`plan_artifact_missing` moves from `phase_chain.rs` into the engine).

2. **Stages are prose.** A stage declares phase/role-affinity/principles but
   no input/output contract. Nothing fails loudly if an agent skips the
   claim→reserve→close sequence inside `systematic-execute`. This ADR defers
   full typed stage contracts (inputs/outputs schemas) to a follow-up — it is
   a larger schema change that touches ADR-0022 §3.8's stage registry. It
   DOES make the stage's declared `phase` mechanical: the engine reads the
   stage's `phase` and applies the same routing a `spec.phase` gets. The
   stage stops being advice-only and becomes the node's failure contract.

#### 6.6.4 Run lifecycle: cancel and per-action timeout

Beyond checkpoint/resume, a runtime needs two missing primitives:

- **Cancel (`flywheel workflow cancel <run-id>`).** Terminate a run that is
  parked, hung, or wrong. A Resonate promise reject on the run's root
  unblocks the durable function; the terminal event records
  `status: "cancelled"`. Distinct from `pause` (checkpoint + resume) and
  `fork` (branch, keep original).

- **Per-action timeout.** `spawn` has `no_claim_timeout`, but no other action
  does. A hung `call_webhook` or `dispatch_job` blocks the run forever. Add
  `timeout_secs` to every `Action` (or a node-level default) with `on_timeout`
  routing into the same `ErrorKind::Transient` → retry path (bounded), so a
  hung action is retried a bounded number of times then escalated — never a
  permanent park.

---

## 7. Implementation Blueprint

### 7.1 Waves

| Wave | Ships | Gate |
| :--- | :--- | :--- |
| **1** (this ADR) | `Workflow::to_graph()` desugars `steps:` → chain graph. `run_workflow` always routes through `run_graph`. `ErrorKind` taxonomy + routed defaults (warn-first until the migration gate). Retire `phase_chain.rs` into the DAG (phase contracts → `on_error: repair`). `StepOnError::Repair` with bounded cycles, different-model repairer, verify gate. R-R1..R5 + R-E1..R-E3 lint rules. `flywheel workflow graph <name>` visualization. | All §8 Wave 1 tests green. No new MCP tool. No new kind. Backward compat: existing `steps:` YAML produces identical outputs. Every built-in carries explicit `on_error` before the routed default flips. |
| **2** | Evidence-bearing approvals: structured context in the approval promise payload built from `step_outputs` + graph topology. `flywheel workflow approvals pending/approve/reject`. | Existing approval tests green with the new payload shape. Approval context includes at minimum: `nodes_completed`, `artifacts`, `blast_radius`. |
| **3** | Bead-workflow integration: `br create --auto-workflow` generates YAML from VERIFY fence + PRINCIPLES + type. Loop dispatches beads as `engine.invoke` calls. `br close` triggers the terminal approval gate. `flywheel workflow run bead-<id>`. | One full bead → workflow → repair → verify → gate → close cycle green in e2e. Crash-resume works (kill -9 mid-run → restart → completes). |
| **4** | Observability surface: `tree/status/connect/resume/fork/cancel`. Per-action `timeout_secs` routing into `ErrorKind::Transient` retry. Auto-parallelization lint from reservation data. | CLI commands green. Fork produces a valid, independently resumable run with `parent_run_id` set. `cancel` produces `status: "cancelled"`. Lint correctly identifies independent and conflicting nodes from reservation globs. |

### 7.2 Affected paths

| Path | Change |
| :--- | :--- |
| `crates/toron-workflow/src/schema.rs` | `Workflow::to_graph()`. `StepOnError::Repair` variant. `GraphNode` gains `build_slot`. `Decision` gains `context` field. Approval payload builder. |
| `crates/toron-workflow/src/engine/control_flow.rs` | `run_workflow` always calls `run_graph`. Delete old `steps:` path. New `run_repair_cycle` function. Evidence bundle construction from `step_outputs`. |
| `crates/toron-workflow/src/engine/mod.rs` | `ApprovalSink` payload shape. Spawn sink carries repair model override. |
| `crates/toron-workflow/src/playbook_lint.rs` | R-R1..R5 repair lint rules. Auto-parallelization lint from reservation data. |
| `crates/toron-workflow/src/guard.rs` | Guard evaluation for repair cycle decisions (no new functions, just wiring). |
| `flywheel/src/builtin_workflows.rs` | All built-ins emit `graph:` shape instead of `steps:`. |
| `flywheel/src/crew.rs` | Compose output uses `graph:` shape. `compose_workflow` accepts bead metadata as input. |
| `flywheel/src/runs.rs` | Run board: tree view, connect stream (poll promise store), fork. |
| `flywheel/src/main.rs` | CLI: `workflow graph/tree/status/connect/resume/fork/approvals/approve/reject`. |
| `flywheel/src/durable.rs` | Loop dispatches beads via `engine.invoke` when `workflow_yaml` metadata present. Claim-watch listens for kind-46012 terminal events. |
| `docs/decisions/0014-kind-registry-specification.md` | No new kinds (verify). |
| `docs/decisions/README.md` | Row for ADR-0024. |
| Skills: toron, flywheel, br | Drift map + token budget in §7.5. No new MCP tools, so tool-count stays 40 (38 parity + 2 additive); catalog stays the 38-parity subset. |

### 7.3 Patterns to follow

- `run_graph` and `run_graph_node` are the existing DAG primitives. Repair cycles are `ctx.run(run_graph_node, ...)` children spawned from the failed node — same concurrency model, same deterministic replay guarantee.
- The approval evidence bundle is built from `step_outputs` already accumulated in the run — no new data collection, just richer serialization of existing state.
- Bead → workflow bridge reuses `compose_workflow` (ADR-0022 §3.6) with the bead as the input spec — one compose path for catalog, cron, and beads.
- `flywheel workflow connect` polls the Resonate promise store (or subscribes to kind-9 events in the run's channel) for state changes.
- Fork reuses `ctx.run(run_workflow, ...)` with a cloned `RunInput` up to the fork point — the same durable function, a new run_id.

### 7.4 Patterns to avoid

- **New MCP tools.** All new surface is CLI (`flywheel workflow *`). The frozen 40 tools are untouched.
- **New Nostr kinds.** Repair cycles use existing kinds: 9 for status posts in the run channel, 46011 for approval pending, 46012 for terminal. No new kind numbers.
- **Worktrees for repair.** The repairer runs in the same project tree, under the same file reservations, on the same branch. The repairer holds the bead's reservation (extended TTL during repair).
- **Auto-accepting repair without verify.** A repair cycle that doesn't re-run VERIFY is not a repair — it's a retry (which `Action::Retry` already handles). The engine always runs VERIFY after the repairer returns.
- **Silent repair exhaustion.** After `max_cycles`, the captain is notified via kind-14 DM with the full repair log: which cycles ran, what errors they hit, what the repairer attempted.
- **Repair-on-repair recursion.** R-R3 lint rule prevents it. One level of repair. If the repairer's fix fails verification `max_cycles` times, escalate.

### 7.5 Skill & Documentation Drift (token-friendly plan)

ADR-0024 changes **surface** (new `flywheel workflow *` CLI, `br create
--auto-workflow`) and **semantics** (graph-first, `on_error: repair`, error
taxonomy, evidence-bearing approvals, typed stage contracts). Per the Skill Sync
Contract (AGENTS.md), every one of these must land in the skill files — but the
whole point is that the drift is *small and additive*, and it lands in the
right layer so it does not bloat the always-loaded bodies.

#### 7.5.1 Skill Sync Contract applied

| Step | Contract item | ADR-0024 impact |
| :--- | :--- | :--- |
| 1 | `Command` enum (`cli.rs`) — ground truth | `flywheel/src/main.rs` gains `workflow graph/tree/status/connect/resume/fork/cancel/approvals/approve/reject`. `toron-cli` unchanged (standalone fabric). |
| 2 | MCP `#[tool(name)]` | **No change.** 40 tools frozen (38 parity + 2 additive). All new surface is CLI. |
| 3 | AGENTS.md STRUCTURE + CONVENTIONS | CONVENTIONS notes graph-first + `on_error: repair` + `ErrorKind` taxonomy + "no new tools/kinds" (already stated). STRUCTURE unchanged. |
| 4 | Skill files (toron, flywheel, br) | Drift map in §7.5.2. |
| 5 | Tool-count constant | **Unchanged** — 40 stays 40 (no MCP tool added). |
| 6 | `toron catalog --json` + `catalog_freshness` | **Unchanged** — toron CLI catalog does not grow; the new surface is flywheel CLI. Flywheel's *builtin* catalog grows in Wave 1 (compose/recipe tokens), gated by `flywheel workflow lint`, not the toron catalog gate. |

#### 7.5.2 Drift map — exact file × section × change

| Skill file | Section | Change | Layer |
| :--- | :--- | :--- | :--- |
| `toron/references/workflow-engine.md` | `### Step policies`, `### Actions`, `### Composition patterns`, `## Approval gate` | `on_error: repair` variant (`max_cycles`, `repair_model`, `verify`); `graph:` canonical + `steps:` desugar; `ErrorKind` (Transient/Logic/Blocked/Fatal/Escalate) → routed defaults; evidence-bearing approval payload fields; typed stage contracts (`inputs`/`outputs`/`phase`); `timeout_secs`. **Single source of truth for engine semantics.** | Layer 3 (on-demand) |
| `flywheel/references/workflow-authoring.md` | `## Schema`, `### Step policies`, `## Composition patterns`, `## Lint` | Same engine surface, orchestration view: compose emits `graph:`; auto-parallelization lint (disjoint vs overlapping reservation globs). Points to `workflow-engine.md` for semantics rather than duplicating. | Layer 3 |
| `flywheel/SKILL.md` | `## 5. Commands (ops)` — the `workflow {…}` token; `## 1. Classify` table | Append `graph,tree,status,connect,resume,fork,cancel,approvals,approve,reject` to the `workflow {…}` list. One line in §1 noting compose emits `graph:` shape. **~15-20 tokens total.** | Layer 2 (always loaded) |
| `flywheel/references/QUICK-REFERENCE.md` | command crib | Add the new `workflow *` subcommands verbatim. | Layer 3 |
| `toron/SKILL.md` | `## Workflow + @mention` (action list), `## CLI Surface` | One line: graph-first + `on_error: repair` + `ErrorKind` routing exist; point to `workflow-engine.md`. No toron CLI change (engine semantics only). **~10 tokens.** | Layer 2 |
| `~/.agents/skills/br/SKILL.md` | (the `br` skill) | `br create --auto-workflow` flag (Wave 3): generates `workflow_yaml` from VERIFY fence + PRINCIPLES. `br close` triggers the terminal gate. | Layer 2 |
| `toron/references/crew-steering.md` | (unchanged) | Channel/job steering is orthogonal to graph execution. No change. | — |
| `AGENTS.md` (toron) | CONVENTIONS | One clause: graph-first canonical shape, `on_error: repair`, ErrorKind routing. No structural edit. | — |

#### 7.5.3 Token budget (why this stays lean)

The two skills are already near-optimal by the skill-judge rubric: **toron 218
lines, flywheel 274 lines** — both well under the 500-line soft cap, both using
a `Load` table with explicit *read / skip* and *"Do not load every reference"*
guidance (progressive disclosure at its cleanest). The drift therefore obeys
three rules:

1. **Heavy content → Layer 3 only.** The repair state machine, `ErrorKind`
routing table, and evidence-payload field list go into the two on-demand
references (`workflow-engine.md`, `workflow-authoring.md`). They are loaded
already *only when authoring or debugging YAML*, so they absorb the new
tokens without taxing every invocation.
2. **Layer 2 delta is one line per skill.** The always-loaded `SKILL.md` bodies
(and the br skill) gain only the new command tokens — on the order of 15–20
tokens each. No new top-level sections, no restated concepts.
3. **Single source, no duplication.** Engine semantics live in
toron's `workflow-engine.md` (the engine is toron-workflow). Flywheel's
`workflow-authoring.md` adds only the orchestration delta (compose → `graph:`,
CLI commands) and links back. The ASCII DAG and repair flowchart stay in this
ADR — the skills need the *shape*, not the picture.

**Anti-drift guards (what NOT to add):**
- Do not restate "what a repair loop is" (retry → fix → verify is already in
  the model's training; only the `on_error: repair` YAML shape + field names
  are expert knowledge).
- Do not copy the full DAG diagram or repair state machine into either skill
  — that is redundant knowledge delta and pure token waste.
- Do not duplicate the `ErrorKind` routing decision table in both skills.
- Do not touch the 39-tool count or the catalog gate — they are unchanged.

#### 7.5.4 Pre-existing token debt to fix while we are in there (skill-judge finding)

Both skills' frontmatter `description` uses a folded YAML scalar (`>-`). The
skill-maker validator rejects multiline descriptions (`>`/`|`) — minimal YAML
parsers only accept a single-line value. This is a one-line, token-neutral
fix (collapse to a single line) and should land in the same Wave-1 touch. The
extra frontmatter fields (`license`, `domain`, `role`, `scope`, `triggers`,
`metadata`) are non-standard but inert — leave them unless a validator rejects
them; deleting them saves a handful of Layer-1 tokens but risks breaking
harnesses that read `triggers:`.

---

## 8. Verification (acceptance)

### Wave 1

- [ ] `Workflow::to_graph()` desugars a 3-step linear workflow into a 3-node chain graph with correct `needs:` edges: step_0→step_1→step_2.
- [ ] `run_workflow` always calls `run_graph`. The old `steps:` path in `run_workflow` is deleted (not just dead code — removed).
- [ ] Existing `steps:` YAML (`triage.yaml`, `goal.yaml`, all built-ins) produces identical `step_outputs` as before the change. Backward compat gate: a serialized run's output byte-matches the pre-change output for the same input.
- [ ] `on_error: repair` with `max_cycles: 2` and a flaky verify command (fails first attempt, succeeds second) completes on the second repair cycle. The run terminal shows `status: "succeeded"` and the node output is the repairer's result.
- [ ] `on_error: repair` on a genuinely broken step (verify always fails) exhausts cycles after `max_cycles` attempts, sends a kind-14 DM to the captain with the full repair log, and the node output is `{ok: false, error: "repair exhausted after N cycles"}`.
- [ ] The repairer agent dispatch spec carries `repair_model` when set. Test verifies the headless spawn args include the model override.
- [ ] Each repair cycle produces a `br gate report` row (`repair-cycle-succeeded` or `repair-cycle-failed`) and a kind-9 post in the run channel.
- [ ] R-R1..R5 lint rules fire on invalid repair configs.
- [ ] R-E1..R-E3 lint rules fire on invalid ErrorKind routing (retry-on-blocked, repair-on-transient-only).
- [ ] `ErrorKind` routing: `Transient`→retry, `Logic`→repair, `Blocked`→escalate, `Escalate`→escalate (unit test with a fake ActionRunner returning each kind).
- [ ] One sequencing engine: `phase_chain.rs` `run_ship_chain` is deleted; `ship` runs through `run_graph` and produces the same §4.5 contract outcomes as the old chain runner (gate test).
- [ ] Every built-in carries an explicit `on_error` before the routed default flips (migration gate). A built-in with no `on_error` triggers a lint warning until the flip.
- [ ] `flywheel workflow graph ship` renders the 5-node ship DAG with correct phases, wave groupings, and edge lines.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` on touched crates.
- [ ] `#[tool(name` count == 40 (38 parity + 2 additive). No new kinds in `kinds.rs` beyond the existing registry.
- [ ] `governed-by: ADR-0024` markers on all Target files listed in §7.2.

### Wave 2

- [ ] `request_approval` promise payload includes `run_id`, `suspended_at` (node + wave), `question`, `context.nodes_completed`, `context.nodes_failed`, `context.artifacts`, `context.repair_cycles` (if any), `context.blast_radius`, `context.timeout`.
- [ ] `flywheel workflow approvals pending` lists a suspended run with all context fields visible.
- [ ] `flywheel workflow approve <run-id> --note "looks good"` resolves the promise with `{ outcome: "approved", note: "looks good" }`. The run resumes and the terminal event shows `status: "succeeded"`.
- [ ] `flywheel workflow reject <run-id> --note "verify didn't pass"` resolves with `{ outcome: "rejected", note: "verify didn't pass" }`. The run terminal shows `status: "rejected"`.

### Wave 3

- [ ] `br create --auto-workflow "Fix reservation conflict check" -p 1 -t bug` produces a bead whose metadata includes a valid `workflow_yaml` with implement→verify(repair)→gate nodes. The VERIFY command is extracted from the `## VERIFY` fence. The PRINCIPLES names are validated against the 21-name registry.
- [ ] The loop detects the ready bead, composes the workflow from metadata, and dispatches via `engine.invoke("bead-bd-xxx", workflow, inputs)`.
- [ ] The full lifecycle completes: implement → verify (with repair if needed) → gate (captain approves) → kind-46012 terminal → `br gate report workflow-verified` → `br close`.
- [ ] The loop's claim-watch releases on the kind-46012 terminal event (not on the `br close` status change).
- [ ] `flywheel workflow run bead-bd-xxx` after a kill -9 resumes from the last Resonate checkpoint and completes (crash-resume equivalence gate).
- [ ] A bead without `--auto-workflow` still closes via the old `br gate report` path (coexistence gate).

### Wave 4

- [ ] `flywheel workflow tree <run-id>` renders a graph with correct node states (✓ ✗ ◌ · ⏸ ↻), phases, assignees, and durations.
- [ ] `flywheel workflow connect <run-id> --timeout-ms 5000` streams events (`NodeStarted`, `NodeCompleted`, `NodeFailed`, `RepairCycleStarted`, `ApprovalSuspended`) as JSON lines until timeout.
- [ ] `flywheel workflow fork <run-id> --at plan --kill-original` creates a new run with `parent_run_id` set to the original run_id. The new run completes independently with a different tail. The original run's terminal shows `status: "killed"`.
- [ ] `flywheel workflow cancel <run-id>` produces a `status: "cancelled"` terminal event and unblocks the parked durable function.
- [ ] Per-action `timeout_secs`: a hung `call_webhook` is retried (bounded) then escalated; never a permanent park.
- [ ] `flywheel workflow lint` correctly identifies independent nodes (disjoint reservation globs) and conflicting nodes (overlapping reservation globs).

---

## 9. Consequences

### Good

- **The graph is the product.** Captains compose DAGs, crews execute them, the loop inspects them. The DAG scheduler graduates from hidden engine detail to the primary mental model. Every workflow — built-in, composed, cron, bead — is a graph you can see (`workflow graph`), watch (`workflow connect`), and fork (`workflow fork`).
- **Failure self-heals.** A flaky test, a missed edge case, a wrong assumption — the repairer (different model, fresh eyes) fixes it and re-verifies. The captain only sees the ones that genuinely need a human (repair exhausted after `max_cycles`). Production incident count should drop: bd-8xca (compose failure) and bd-cvv9 (chain-gate false positive) would have self-healed in cycle 1 or 2.
- **Failures are typed and self-routing.** The engine decides retry vs repair vs escalate from the `ErrorKind`, not the captain after the fact. Transient failures retry, logic failures repair on a different model, blocked failures escalate. The binary abort-or-swallow behavior is gone.
- **Approvals carry context.** The operator decides on evidence: which nodes passed, which artifacts exist, whether verify ran and passed, what the blast radius is. No more blind "ok?" decisions. This is `prove-it-works` applied to the human-in-the-loop.
- **Beads and runs are one thing.** No parallel timelines. No dual closing ceremonies. The loop dispatches a bead as a workflow run. The workflow terminal closes the bead. One lifecycle, one audit trail.
- **Live runs are observable.** The captain can see the tree, stream events, resume after a crash, and fork at any checkpoint to try an alternative path. The run is no longer a black box between dispatch and terminal.

### Bad

- **The `steps:` → `graph:` desugar is a semantic shift.** Nodes that were implicitly sequential become explicitly dependent. Independent nodes could now run concurrently within a wave — a behavior change for workflows where step order was assumed but not declared. Mitigation: the desugar preserves exact sequential order for unnamed steps (chain graph, one node per wave). Named steps with no data dependency and no explicit `needs:` get a lint warning (Wave 4 auto-parallelization lint) but do NOT auto-parallelize without captain confirmation.
- **Repair cycles consume agent time and model tokens.** Each cycle dispatches a repairer agent + runs verify. The `max_cycles` bound (1..10, YAML) and the env cap (`FLYWHEEL_REPAIR_MAX_CYCLES`, default 10) prevent unbounded spend. A workflow with `max_cycles: 5` that genuinely cannot be fixed will consume 5 repair dispatches before escalating — that's the cost of self-healing.
- **Bead-workflow integration is a bridge between two systems.** `br` SQLite (local, file-based, git-synced) and Resonate (durable promise store, in-process or remote). Drift between them is a risk — e.g., a bead closed in SQLite but the Resonate run still suspended. Mitigation: the bridge is one code path (`compose_workflow` from bead metadata → `engine.invoke`), tested end-to-end. The loop's claim-watch listens for kind-46012 terminals, not SQLite status changes. A bead whose workflow run died silently (Resonate store lost) is detected by the loop's periodic `br gate report` check on non-workflow-owned beads.
- **The `on_error` default flip is a behavior change.** Absent `on_error`, a node that today fails silently (continue) will retry/repair/escalate under routing. Mitigation: the migration gate — every built-in carries explicit `on_error` before the default flips, and the routed default is warn-first (logs what it *would* route) until then.

### Neutral

- **The `steps:` YAML surface is unchanged.** Captains can still write linear workflows. The engine sees them as chain graphs. No migration needed.
- **Fork is not free.** A forked run is a new Resonate durable function with its own promise store entries and its own kind-46012 terminal. The cost is proportional to the run's remaining depth. Fork is a deliberate operator action, not an automatic behavior.
- **Federation is unaffected.** All graph execution, repair cycles, and approvals run on the local bus. Kind-9 status posts and kind-14 escalation DMs use the existing plane-S and plane-P surfaces.

### Risks

| Risk | Severity | Mitigation |
| :--- | :--- | :--- |
| Repairer makes things worse (introduces a new bug while fixing the first) | Medium | Each cycle re-runs VERIFY. A repairer that fails verify `max_cycles` times stops and escalates. The repairer cannot "succeed" without the verify command passing. |
| Graph desugar breaks a workflow that relied on undocumented step ordering | Low | The chain graph preserves exact sequential order. Named steps with no `needs:` get a lint warning. No auto-parallelization without captain confirmation. |
| Bead-workflow bridge diverges from bare `br close` path | Medium | The bridge is one code path. Both paths produce `br gate report` rows. The workflow path additionally emits kind-46012. The loop consumes both. Coexistence is tested (Wave 3 verification). |
| Fork breaks Resonate replay determinism | Low | Fork copies the serialized `RunInput` up to the fork point. A new run starts from that state with a new `run_id`. The original run's replay is unaffected. Deterministic replay is per-run, not cross-run. |
| `on_error: repair` on every node becomes the default, masking systemic failures | Medium | Lint rule R-R5 warns when repair_model is the same family as the failing model. The env cap `FLYWHEEL_REPAIR_MAX_CYCLES` is a global brake. The captain sees `RepairExhausted` escalations — if they're frequent, something is systemically wrong. |

---

## 10. More Information

### Intention

A year from now a captain types `flywheel workflow graph ship-toron`, sees the DAG with ✓ ✓ ✓ ✓ ⏸, sees the repair cycles that ran overnight (verify took 2 attempts, review passed first try), reads the evidence bundle, and types `flywheel workflow approve ship-toron-042`. The bead closes. The captain never reads a "done" self-report. The captain never manually re-dispatches a failed build. The graph is the product.

### Principles cited in this ADR

| Principle | Where it applies |
| :--- | :--- |
| `prove-it-works` | Repair cycles re-run VERIFY. Approvals carry evidence. The graph shows node states, not agent claims. |
| `sequence-verifiable-units` | Each graph node is a verifiable unit. Each repair cycle is a tracked gate row. The graph topology makes dependencies explicit. |
| `fix-root-causes` | Repair fixes the root cause, not the symptom. Retry-of-same is `Action::Retry` (transient failures only). |
| `encode-lessons-in-structure` | The graph IS the structure. `on_error: repair` is a type, not a prose instruction. The DAG is lint-checked. |
| `never-block-on-the-human` | Auto-repair up to `max_cycles`. Only escalate on exhaustion. |
| `build-the-lever` | `flywheel workflow tree/connect/fork` is the lever — the tool that proves the run state. |
| `type-system-discipline` | `StepOnError` is exhaustive. `Phase` is a closed enum. `DispatchSpec` is typed. |
| `laziness-protocol` | The DAG scheduler already exists. This ADR extends it — no new engine, no new kinds, no new MCP tools. Delete the old `steps:` path after migration. |
| `migrate-callers-then-delete-legacy-apis` | `run_workflow`'s old `steps:` path is deleted in Wave 1 after backward-compat tests pass. |

### Related

- `docs/notes/2026-08-25-atomic-analysis.md` — Buffy's full-stack comparison of Atomic vs our stack (what to adopt, what to keep)
- ADR-0005 Spec 4b — the existing DAG scheduler (`run_graph`) this ADR promotes to canonical
- ADR-0019 Spec 1 — VERIFY fence, `br gate`, legal-close table (repair cycles extend this)
- ADR-0022 §3.1 — `Phase` enum (graph nodes carry phase)
- ADR-0022 §3.4 — one durable execution rail (`engine.invoke` for compose + steer — beads join this rail)
- ADR-0023 — NIP-29 39000/39002 (channel create/close; run channels use these for status posts)
- bastani-inc/atomic — `loop-until-done` (bounded repair inspiration), `fan-out-and-synthesize` (parallel fan-out with evidence), `/workflow connect` (live run streaming)

### Revisit

- **Verbatim compaction** (Atomic's context management): the repairer's context window is bounded by `CONTEXT_LIMIT`. Smarter compaction — deleting low-value transcript lines without rewriting surviving text — is a general improvement, not repair-specific. Deferred to a separate ADR.
- **Permission gate hooks** (Atomic's tool-call interception, blocking dangerous shell commands before execution): our reservation system + pre-commit guards already prevent most dangerous writes. Runtime command interception is defense-in-depth. Deferred.
- **Hashline-style content verification** (Atomic's hashline edits anchored to file hashes): file reservations already prevent concurrent writes. Hash verification on commit would catch a repairer who violated the reservation. Deferred for defense-in-depth.
- **Event-driven loop promotion** (ADR-0019 wave 4): when the loop is event-driven (herdr reactor primary, no poll), kind-46012 terminal events become the native wake signal for claim-watch release. The bead-workflow bridge in this ADR is designed to consume those events whether the loop polls or subscribes.

---

## 11. Appendix: Real Scenarios — Before and After

This appendix takes five concrete workflows and stages from the current codebase
(`flywheel/src/builtin_workflows.rs`, `flywheel/src/phase_chain.rs`,
`flywheel/src/stages.rs`) and shows exactly what changes under ADR-0024.

---

### Scenario A: `goal` — the meta-workflow for overnight autonomous work

**Today** (`goal` built-in, builtin_workflows.rs:330):

```yaml
name: goal
steps:
  - name: ideate
    action: call_workflow
    workflow: brainstorm
  - name: plan
    action: call_workflow
    workflow: phase-plan
  - name: track
    action: call_workflow
    workflow: beads
  - name: build
    action: dispatch_job
    assignee: x
    spec: { profile: "@coder", stage: "systematic-execute", plan: "{{steps.track}}" }
    on_error: fail_run
  - name: review
    action: dispatch_job
    assignee: x
    spec: { profile: "@reviewer", harness: "opencode", stage: "code-review", plan: "{{steps.build}}" }
    on_error: fail_run
  - name: ship
    action: dispatch_job
    assignee: x
    spec: { profile: "@housekeeper", stage: "commit-and-push", plan: "{{steps.build}}" }
    on_error: fail_run
```

**What's wrong:** Six sequential steps. Every failure aborts the run (`fail_run`).
Review and build are sequential — they can't run concurrently. If the build
step's `cargo test` fails (flaky test, wrong assumption), the entire `goal`
run aborts. The captain wakes up to a red terminal and manually re-dispatches.
The `ship` step commits and pushes without any gate — no "did review pass?"
check, no approval.

**After ADR-0024:**

```yaml
name: goal
graph:
  nodes:
    ideate:
      action: call_workflow
      workflow: brainstorm

    plan:
      action: call_workflow
      workflow: phase-plan
      needs: [ideate]

    track:
      action: call_workflow
      workflow: beads
      needs: [plan]

    build:
      action: dispatch_job
      assignee: "@crew.coder"
      spec:
        profile: "@coder"
        phase: implement
        stage: systematic-execute
        plan: "{{nodes.track}}"
      needs: [track]
      on_error:
        repair:
          max_cycles: 3
          verify: "cargo test --workspace"

    review:
      action: dispatch_job
      assignee: "@crew.reviewer"
      spec:
        profile: "@reviewer"
        harness: opencode
        phase: review
        stage: code-review
        plan: "{{nodes.build}}"
      needs: [build]
      on_error:
        repair:
          max_cycles: 2
          # Review repair: reviewer addresses findings, re-submits verdict

    gate:
      action: request_approval
      message: "Goal complete. Approve ship?"
      needs: [review]
      on_error: fail_run

    ship:
      action: dispatch_job
      assignee: "@crew.housekeeper"
      spec:
        profile: "@housekeeper"
        phase: verify
        stage: commit-and-push
        plan: "{{nodes.build}}"
      needs: [gate]
      on_error: fail_run
```

**What changed:**

1. **`steps:` → `graph:`.** The six steps are now explicit DAG nodes with
   `needs:` edges. Identical sequential execution for this chain.

2. **Repair on build.** If the coder's `cargo test` fails, instead of aborting
   the entire run, the engine spawns a repairer (different model, fresh
   context), re-runs `cargo test --workspace`, and repeats up to 3 times.
   Only on exhaustion does it escalate to the captain.

3. **Repair on review.** If the reviewer returns REJECT instead of AGREE,
   the repairer re-reviews with fresh eyes (cycle 2) rather than aborting.
   Two cycles, then escalate.

4. **Gate before ship.** A `request_approval` node with evidence sits between
   review and ship. The captain sees: ideate ✓, plan ✓, track ✓, build ✓
   (0 repair cycles), review ✓ (AGREE from BlueLake, 3 findings). The captain
   approves — or rejects and the run records it.

5. **Concurrent review+verify (optional).** If we add a `verify` node (loop
   runs VERIFY headless) with `needs: [build]`, it runs concurrently with
   `review` in the same wave — genuine parallelism. The gate node waits for
   both (`needs: [review, verify]`).

**Captain experience:**

```bash
# Morning check
$ flywheel workflow tree goal-2026-08-26-001

goal (6 nodes, 4 waves)  elapsed: 3h12m
│
├─ Wave 1 ──────────────────────────────────────────
│  ✓ ideate           phase=resolve  @scout     12s
│
├─ Wave 2 ──────────────────────────────────────────
│  ✓ plan              phase=plan     @planner   8m
│
├─ Wave 3 ──────────────────────────────────────────
│  ✓ track             phase=plan     @planner   14s
│
├─ Wave 4 ──────────────────────────────────────────
│  ✓ build             phase=impl     @coder     2h41m
│    ↻ repair cycle 1  (cargo test flake)        4m
│  ✓ review            phase=review   @reviewer  5m
│    verdict: AGREE (BlueLake, 3 findings)
│
├─ Wave 5 ──────────────────────────────────────────
│  ⏸ gate              approval       captain    8h2m
│
└─ Wave 6 ──────────────────────────────────────────
   · ship              phase=verify    @housekeep

$ flywheel workflow approvals pending
run_id                        workflow  node  question
---------------------------   --------  ----  --------
goal-2026-08-26-001           goal      gate  Goal complete. Approve ship?
  context: build ✓ (1 repair cycle), review ✓ (AGREE)
  artifacts: commit abc1234, verdict BlueLake/AGREE

$ flywheel workflow approve goal-2026-08-26-001 --note "ship it"
run goal-2026-08-26-001: approved. resuming...
  ship ✓ (committed, pushed abc1234)
  terminal: status=succeeded
```

---

### Scenario B: `bug-fix` — repro → tdd → fix → prove, with repair

**Today** (`bug-fix` built-in, builtin_workflows.rs:665):

```yaml
name: bug-fix
graph:
  nodes:
    repro:
      action: dispatch_job
      assignee: x
      spec: { profile: "@scout", task: "Run or write the repro..." }
      on_error: fail_run
    tdd:
      action: dispatch_job
      assignee: x
      spec: { profile: "@coder", task: "Write the FAILING test first..." }
      needs: [repro]
      on_error: fail_run
    fix:
      action: dispatch_job
      assignee: x
      spec: { profile: "@coder", task: "Apply the MINIMAL fix..." }
      needs: [tdd]
      on_error: fail_run
    prove:
      action: dispatch_job
      assignee: x
      spec: { profile: "@reviewer", harness: "opencode", task: "Independently prove..." }
      needs: [fix]
      on_error: fail_run
```

**What's wrong:** `bug-fix` already uses `graph:` — one of only three built-ins
that do (alongside `brainstorm` and `debate`). Every node is `fail_run`. The
`prove` node's reviewer runs on opencode (different harness) which is good, but
if prove finds the fix didn't actually work, the entire run aborts. The captain
must manually re-dispatch from tdd or fix.

**After ADR-0024:**

```yaml
name: bug-fix
graph:
  nodes:
    repro:
      action: dispatch_job
      assignee: "@crew.scout"
      spec:
        profile: "@scout"
        phase: resolve
        task: "Run or write the repro for: {{inputs.symptom}}..."
      on_error: fail_run               # can't repair without a repro

    tdd:
      action: dispatch_job
      assignee: "@crew.coder"
      spec:
        profile: "@coder"
        phase: verify
        task: "Write the FAILING test first (RED)..."
      needs: [repro]
      on_error:
        repair:
          max_cycles: 2
          verify: "cargo test {{inputs.test_filter}} -- --test-threads=1"

    fix:
      action: dispatch_job
      assignee: "@crew.coder"
      spec:
        profile: "@coder"
        phase: implement
        task: "Apply the MINIMAL fix against the failing test..."
      needs: [tdd]
      on_error:
        repair:
          max_cycles: 3
          repair_model: "claude-opus-4"   # different model for genuine fix
          verify: "cargo test {{inputs.test_filter}} && {{inputs.repro}}"

    prove:
      action: dispatch_job
      assignee: "@crew.reviewer"
      spec:
        profile: "@reviewer"
        harness: opencode
        phase: review
        task: "Independently prove the fix..."
      needs: [fix]
      on_error:
        repair:
          max_cycles: 1                 # one re-review, then escalate
          verify: "cargo test {{inputs.test_filter}}"

    gate:
      action: request_approval
      message: "Bug-fix complete. Repo OK?"
      needs: [prove]
```

**What changed:**

1. **Phases on every node.** `repro=resolve`, `tdd=verify`, `fix=implement`,
   `prove=review`. The phase column in `flywheel runs` now has real data for
   cycle-time analysis.

2. **Repair on fix.** If the coder's minimal fix doesn't make the test pass,
   a different-model repairer (`claude-opus-4`, not the same model that wrote
   the bad fix) gets the error and repairs it, up to 3 cycles.

3. **Repair on prove.** If the reviewer finds the fix incomplete (returns
   REJECT), one repair cycle re-reviews before escalating.

4. **Gate at the end.** The bug-fix run suspends for captain approval with
   full evidence: repro ✓ (baseline captured), tdd ✓ (test fails),
   fix ✓ (0 repair cycles), prove ✓ (AGREE). The captain approves or rejects.

**Captain experience:**

```bash
$ flywheel workflow tree bug-fix-bd-456

bug-fix (5 nodes, 2 waves)  elapsed: 17m
│
├─ Wave 1 ──────────────────────────────────────────
│  ✓ repro            phase=resolve @scout     3m
│
├─ Wave 2 ──────────────────────────────────────────
│  ✓ tdd              phase=verify  @coder     4m
│    ↻ repair cycle 1 (test assertion wrong)   2m
│
├─ Wave 3 ──────────────────────────────────────────
│  ✓ fix              phase=impl    @coder     8m
│
├─ Wave 4 ──────────────────────────────────────────
│  ✓ prove            phase=review  @reviewer  2m
│    verdict: AGREE-WITH-CHANGES (1 minor finding)
│
└─ Wave 5 ──────────────────────────────────────────
   ⏸ gate             approval      captain
```

The captain sees the tdd node took 2 repair cycles (first assertion was wrong),
the fix passed on the first try (0 cycles), and the reviewer returned
AGREE-WITH-CHANGES (minor finding, not blocking). They approve.

---

### Scenario C: `ship` — the gated phase chain, now with concurrent review+verify

**Today** (`ship` + `phase_chain.rs`):

The `ship` built-in runs five `call_workflow` links sequentially through a
gated chain runner (`run_ship_chain`). Between each link, `check_phase`
verifies the §4.5 completion contract. The gates are warn-first
(`FLYWHEEL_SHIP_GATE_MODE=block` is the W5 flip). The review and verify links
are sequential — `implement → review → verify`.

```yaml
name: ship
steps:
  - name: resolve
    action: call_workflow
    workflow: phase-resolve
    inputs: { objective: "{{inputs.objective}}" }
  - name: plan
    action: call_workflow
    workflow: phase-plan
    inputs: { goal: "{{inputs.objective}}", data_shape: "{{inputs.data_shape}}" }
  - name: implement
    action: call_workflow
    workflow: phase-implement
    inputs: { goal: "{{inputs.objective}}", plan: "{{steps.plan}}" }
  - name: review
    action: call_workflow
    workflow: phase-review
    inputs: { target: "{{inputs.objective}}", artifact: "{{steps.implement}}" }
  - name: verify
    action: call_workflow
    workflow: phase-verify
    inputs: { goal: "{{inputs.objective}}", gate: "{{inputs.gate}}" }
```

**What's wrong:** Review and verify are sequential — they could run in parallel.
Both depend on implement, neither depends on the other. Running them
sequentially adds ~5 minutes to every ship run. The chain runner
(`phase_chain.rs`) is a separate execution path from the DAG scheduler — it
can't express concurrency at all.

**After ADR-0024:**

```yaml
name: ship
graph:
  nodes:
    resolve:
      action: call_workflow
      workflow: phase-resolve
      inputs: { objective: "{{inputs.objective}}" }

    plan:
      action: call_workflow
      workflow: phase-plan
      inputs: { goal: "{{inputs.objective}}", data_shape: "{{inputs.data_shape}}" }
      needs: [resolve]

    implement:
      action: call_workflow
      workflow: phase-implement
      inputs: { goal: "{{inputs.objective}}", plan: "{{nodes.plan}}" }
      needs: [plan]
      on_error:
        repair:
          max_cycles: 3
          verify: "{{inputs.gate}}"

    review:                          # Wave 4: concurrent with verify
      action: call_workflow
      workflow: phase-review
      inputs: { target: "{{inputs.objective}}", artifact: "{{nodes.implement}}" }
      needs: [implement]
      on_error:
        repair:
          max_cycles: 2

    verify:                          # Wave 4: concurrent with review
      action: call_workflow
      workflow: phase-verify
      inputs: { goal: "{{inputs.objective}}", gate: "{{inputs.gate}}" }
      needs: [implement]
      on_error:
        repair:
          max_cycles: 3
          verify: "{{inputs.gate}}"

    gate:
      action: request_approval
      message: "Ship {{inputs.objective}}?"
      needs: [review, verify]        # waits for both concurrent nodes
```

**What changed:**

1. **`steps:` → `graph:`.** The chain is now a DAG. The chain runner
   (`phase_chain.rs`) can be retired — the DAG scheduler handles the gates
   natively with `needs:` edges and `on_error: repair`.

2. **Wave 4: concurrent review + verify.** Both depend on `implement`, neither
   depends on the other. They run in the same wave. The `gate` node waits for
   both (`needs: [review, verify]`). This saves ~5 minutes per ship run.

3. **Repair on implement.** If the build fails (flaky test, missed edge case),
   the engine spawns a repairer instead of aborting. The gated chain
   (`run_ship_chain`) would have aborted at the failed contract check.

4. **Repair on verify.** If the verify command fails, up to 3 repair cycles
   fix and re-verify before escalating.

5. **Evidence-bearing gate.** The approval carries: resolve ✓, plan ✓,
   implement ✓ (0 cycles), review ✓ (AGREE), verify ✓ (passed, 0 cycles).

```
ship (6 nodes, 4 waves)
│
├─ Wave 1 ──────────────────────────────────────────
│  ✓ resolve          phase=resolve  @scout
│
├─ Wave 2 ──────────────────────────────────────────
│  ✓ plan             phase=plan     @planner   needs: [resolve]
│
├─ Wave 3 ──────────────────────────────────────────
│  ✓ implement        phase=impl     @coder     needs: [plan]
│    ↻ repair cycle 1 (test assertion wrong)
│
├─ Wave 4 ──── CONCURRENT ──────────────────────────
│  ✓ review           phase=review   @reviewer  needs: [implement]
│  ✓ verify           phase=verify   (loop)     needs: [implement]
│
└─ Wave 5 ──────────────────────────────────────────
   ⏸ gate             approval       captain    needs: [review, verify]
```

**The chain runner retirement path:** `phase_chain.rs`'s `run_ship_chain` is
replaced by the DAG scheduler. The §4.5 completion contracts (`check_phase`)
become node-level `on_error: repair` with `verify` commands. The warn-first
→ blocking flip (`FLYWHEEL_SHIP_GATE_MODE`) becomes a DAG-level config: nodes
with `on_error: repair` on `max_cycles` exhaustion route to escalate (the
current warn behavior) or to `fail_run` (the block behavior). One execution
path instead of two.

---

### Scenario D: Bead `bd-123` — from `br create` to `br close` as a single workflow run

**Today:**

```bash
# 1. Planner creates the bead
br create "Fix reservation conflict check" -p 1 -t bug \
  --description "The conflict check compares the wrong field.

## VERIFY
\`\`\`bash
cargo test -p toron-core reservation_conflict_detection
\`\`\`

## PRINCIPLES
prove-it-works — ran the VERIFY fence, not a self-report
fix-root-causes — the conflict check was comparing holder_name instead of path"

# 2. Loop detects ready bead, dispatches via bead_prompt string
flywheel send --to coder1 "Implement bead bd-123: Fix reservation..."

# 3. Coder works, commits with "bd-123" in message
# 4. Coder manually: br gate report bd-123 --gate unit-test-verified --status pass
# 5. Coder manually: br close bd-123 --commit-sha abc1234 --reason "Fixed"
# 6. Loop detects closed, runs ledger check, verifies gate row exists
# 7. Loop releases claim-watch
```

**What's wrong:** Steps 4-6 are disconnected. The coder might close without
running the gate report. The loop might reopen if the race condition between
close and gate report hits. The captain has no visibility into the work
between dispatch and close — it's a black box.

**After ADR-0024:**

```bash
# 1. Planner creates the bead WITH auto-workflow generation
br create --auto-workflow "Fix reservation conflict check" -p 1 -t bug \
  --description "The conflict check compares the wrong field.

## VERIFY
\`\`\`bash
cargo test -p toron-core reservation_conflict_detection
\`\`\`

## PRINCIPLES
prove-it-works — ran the VERIFY fence, not a self-report
fix-root-causes — comparing holder_name instead of path"

# This produces a bead whose metadata includes:
#   workflow_yaml: (auto-generated from VERIFY + PRINCIPLES + type)
#
#   name: "bead-bd-123"
#   graph:
#     nodes:
#       implement:
#         action: dispatch_job
#         assignee: "@crew.coder"
#         spec: { phase: implement, task: "Implement bead bd-123..." }
#       verify:
#         needs: [implement]
#         action: dispatch_job
#         assignee: "@crew.coder"
#         spec: { phase: verify, task: "Run VERIFY..." }
#         on_error:
#           repair:
#             max_cycles: 3
#             verify: "cargo test -p toron-core reservation_conflict_detection"
#       gate:
#         needs: [verify]
#         action: request_approval
#         message: "Bead bd-123 complete. Close?"

# 2. Loop detects ready bead WITH workflow_yaml metadata
#    → compose_workflow from metadata → engine.invoke

# 3. Engine runs the graph:
#    implement → coder commits "bd-123" in message
#    verify → coder runs VERIFY (fails first, repair cycle fixes it)
#    gate → suspends for captain approval

# 4. Captain reviews and approves
$ flywheel workflow approvals pending
bead-bd-123   implement ✓ (commit abc1234)   verify ✓ (1 repair cycle)   gate ⏸

$ flywheel workflow approve bead-bd-123
# Engine: emits kind-46012 terminal { status: "succeeded", ... }
# Engine: br gate report bd-123 --gate workflow-verified --status pass
# Engine: br close bd-123 --commit-sha abc1234 --reason "Workflow verified"

# 5. Loop: claim-watch releases on kind-46012 terminal event
#    No manual bridge. No race condition. One lifecycle.
```

**What changed:**

1. **Bead creation generates the workflow.** `br create --auto-workflow`
   extracts the VERIFY command from the fence, validates PRINCIPLES names
   against the 21-name registry, and generates a typed DAG. The bead's type
   (`bug`) and priority (`1`) determine the node set: P0–P1 gets a mandatory
   reviewer node in addition to implement+verify+gate.

2. **The loop dispatches through the engine.** Instead of a raw `send --to
   coder1 "Implement bead bd-123..."` string, the loop calls `engine.invoke`
   with the workflow struct. The dispatch is a durable run, not a one-shot
   prompt.

3. **Repair on verify.** If the coder's implementation fails the VERIFY
   command, the engine spawns a repair cycle instead of requiring the coder
   to manually fix and re-close. The repairer gets the exact failure and
   the verify command.

4. **Close is a gate, not a manual step.** The coder does not run `br gate
   report` or `br close` — the engine does both when the captain approves
   the gate. One ceremony, not two.

5. **The loop listens for kind-46012.** Instead of polling `br list --status
   closed` and grep-ing for gate rows, the loop subscribes to the terminal
   event. No race condition between close and gate report.

6. **Crash resilience.** If the process dies mid-run (after implement, before
   verify), `flywheel workflow run bead-bd-123` resumes from the last
   Resonate checkpoint. The implement node's output is preserved; verify
   picks up where it left off.

---

### Scenario E: `weekly-review` cron — self-healing the weekly scout→oracle pipeline

**Today** (`weekly-review` built-in, builtin_workflows.rs:565):

```yaml
name: weekly-review
trigger:
  type: schedule
  cron: "0 4 * * 0"
  owner: oracle
graph:
  nodes:
    scout-fanout:
      action: parallel
      over: "3"
      each:
        action: dispatch_job
        assignee: x
        spec: { profile: "@scout", task: "Scout toron+flywheel repos..." }

    oracle:
      action: dispatch_job
      assignee: x
      spec: { profile: "@oracle", task: "Synthesize..." }
      needs: [scout-fanout]
```

This is the closest built-in to the ADR-0024 vision: it already uses `graph:`
with `parallel` fan-out and a dependent synthesis node. The cron fires every
Sunday at 4am. When it fails (oracle model unavailable, scout timeout, empty
findings), the cron policy says `on_failure: bead` — it creates a bead for
manual triage.

**What's wrong:** Failure creates a bead and stops. No retry, no repair.
If the oracle model was temporarily unavailable (rate limit, API flake),
the entire weekly review is lost. The captain finds a bead on Monday morning
and manually re-runs.

**After ADR-0024:**

```yaml
name: weekly-review
graph:
  nodes:
    scout-fanout:
      action: parallel
      over: "3"
      each:
        action: dispatch_job
        assignee: x
        spec: { profile: "@scout", task: "Scout toron+flywheel repos..." }
      on_error:
        repair:
          max_cycles: 2
          # One scout failing doesn't kill the fan-out; repairer picks up

    oracle:
      action: dispatch_job
      assignee: x
      spec: { profile: "@oracle", task: "Synthesize scout findings..." }
      needs: [scout-fanout]
      on_error:
        repair:
          max_cycles: 3
          repair_model: "gemini-2.5-pro"  # different model family
          verify: "test -n '$(cat /tmp/weekly-review-result.json | jq -r .idea.title)'"
```

**What changed:**

1. **Repair on scout-fanout.** If one scout arm fails (rate limit, timeout),
   a repairer picks up that arm's task instead of failing the whole fan-out.
   2 cycles, then escalate.

2. **Repair on oracle.** If the oracle model is temporarily unavailable or
   returns empty findings, a different-model repairer (`gemini-2.5-pro`,
   not the same family as the oracle) synthesizes instead. Up to 3 cycles.
   The verify command checks that the output JSON has a non-null `idea.title`.

3. **`on_failure: bead` only on exhaustion.** The cron policy's `on_failure:
   bead` now fires only after repair exhaustion — not on the first transient
   failure. The captain only sees beads for genuinely unfixable failures.

**Captain experience:**

```bash
# Monday morning
$ flywheel runs list --workflow weekly-review --since 2026-08-23

run_id                    status     nodes   repairs  elapsed
weekly-review-2026-08-23  succeeded  2/2     1        8m
weekly-review-2026-08-16  succeeded  2/2     0        5m
weekly-review-2026-08-09  succeeded  2/2     2        14m

$ flywheel workflow tree weekly-review-2026-08-23

weekly-review (2 nodes, 2 waves)  elapsed: 8m
│
├─ Wave 1 ──────────────────────────────────────────
│  ✓ scout-fanout     phase=resolve  @scout(×3)  4m
│
├─ Wave 2 ──────────────────────────────────────────
│  ✓ oracle           phase=plan     @oracle      4m
│    ↻ repair cycle 1 (gemini-2.5-pro: empty output)
```

All three runs succeeded. The Aug 23 run needed one repair cycle
(oracle returned empty, gemini fixed it). The Aug 9 run needed two
cycles (scout timeout + oracle model unavailable). Zero beads created.
Zero captain interventions.

---

### Scenario F: How existing workflows migrate (the backward-compat contract)

Every existing workflow continues to work without modification. The engine
sees `steps:` and desugars to a chain graph internally. The captain can
opt into repair and concurrency by adding `on_error: repair` and `needs:`
edges — no migration required.

**Migration path for the 26 built-ins:**

| Workflow | Today | After Wave 1 | Opt-in gains |
| :--- | :--- | :--- | :--- |
| `goal`, `goal-direct`, `plan`, `beads`, `triage`, `plan-review`, `onboard`, `triage-deep`, `feature`, `refactoring`, `perf`, `hillclimb`, `figure-it-out`, `babysit-bead`, `ship-beads`, `autonomous-work`, `pickup`, `pause`, `wave-swarm`, `reflect`, `awareness-digest`, `phase-resolve`, `phase-plan`, `phase-implement`, `phase-review`, `phase-verify` | `steps:` | Chain graph (identical behavior) | Add `on_error: repair` to build/verify nodes. Add `needs:` to enable concurrency where file sets are disjoint. |
| `brainstorm`, `debate`, `captain-audit`, `bug-fix`, `weekly-review`, `prototype`, `arena`, `forensics`, `interrogate` | `graph:` | Already a DAG (no change) | Add `on_error: repair` to reduce/decide/synthesize nodes. Add `verify` node with `needs: [implement]` for concurrent review+verify. |
| `ship` | `steps:` (chain runner) | Chain graph, chain runner retired | Add `on_error: repair` to implement node. Add concurrent review+verify wave. |

**Stage compatibility:** All stages (`systematic-execute`, `code-review`,
`commit-and-push`, `baseline-measure`, `diagnose-only`, `judge-arms`, etc.)
continue to work unchanged. Stages are prompt templates dispatched by
`dispatch_job` actions. The graph topology and repair policy are orthogonal
to the stage content. A `dispatch_job` with `stage: systematic-execute` and
`on_error: repair` is valid — the stage runs inside a repairable node.

**Phase compatibility:** ADR-0022's `Phase` enum (resolve/plan/implement/
review/verify) is unchanged. Graph nodes carry `spec.phase` as before.
The `phase_chain.rs` completion contracts (`check_phase`) become node-level
repair policies: "implement must produce a commit citing the bead" is
now `on_error: repair { verify: "git log -1 --grep=bd-xxx" }` — the same
contract, enforced by the engine instead of the chain runner.

---

### Summary: what stays, what changes

| Aspect | Stays exactly the same | Changes |
| :--- | :--- | :--- |
| YAML surface | `steps:` still parses, `actions` still work, `dispatch_job` spec unchanged, stages unchanged, crew/inputs/principles fields unchanged | `graph:` becomes canonical internally; `steps:` desugars at parse time |
| Engine | `run_graph`, `run_graph_node`, `run_parallel`, `run_retry`, `run_branch`, `run_foreach`, `run_call_workflow`, `run_spawn` — all unchanged | `run_workflow` always routes through `run_graph`. New `run_repair_cycle`, `ErrorKind` taxonomy, richer approval payload, per-action `timeout_secs`. Old `steps:` path deleted. |
| CLI | `flywheel workflow run/list/lint/compose`, `flywheel runs`, `flywheel steer`, `flywheel loop` — all unchanged | New: `flywheel workflow graph/tree/status/connect/resume/fork/cancel/approvals/approve/reject` |
| Beads | `br create/list/close/gate`, `br ready`, `br dep` — all unchanged | New: `br create --auto-workflow`. New `workflow_yaml` metadata field. Loop dispatches beads as `engine.invoke`. |
| MCP tools | All 40 tools unchanged (38 parity + 2 additive) | None |
| Nostr kinds | All existing kinds unchanged | None |
| Worktree ban | ADR-0019 Spec 3 unchanged | Repairer runs in same tree, under same reservations |
| Chain runner | `phase_chain.rs` `run_ship_chain` retired after Wave 1 | Replaced by DAG scheduler with `needs:` + `on_error: repair` + concurrent waves |
| `on_error` default | Explicit `on_error` unchanged; `continue`/`fail_run`/`repair` semantics unchanged | Absent `on_error` now routes by `ErrorKind` (retry/repair/escalate) — warn-first until the migration gate is green |
| Run lifecycle | Checkpoint/resume (`pause`/`pickup`) unchanged | New `cancel` + per-action `timeout_secs` |