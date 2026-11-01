# Graph run-model design: durable DAG scheduling without Resonate

Bead: `bd-durable-graph-ifkn`. Step 2 of `resonate-removal-inventory.md` §7.
Status: design for captain review. **No code** — `durable.rs` and
`durable_host.rs` untouched until approval.

Governing principle (bead): `one-scheduler-per-model` — the pure model owns
graph sequencing, not the host.

## Where the scheduler lives today

`run_workflow` (`control_flow.rs:191`) always runs `run_graph` — even linear
workflows, via `Workflow::to_graph()`, which chains steps into a linked list
of `needs`. The linear durable model is therefore a special case of what is
designed here (single-chain graphs), but this slice does NOT subsume it (see
§(f)).

The Context scheduler (`run_graph`, `control_flow.rs:2482`):

1. `validate_graph_runtime`: empty / unknown-need / cycle → `{status:failed}`
   value (never partial success).
2. Waves: ready = not-done with all `needs` done, in `BTreeMap` (name) order.
3. Whole wave spawned as concurrent durable children (`run_graph_node`), then
   awaited in spawn order.
4. Per node: guard (same evaluator; false → `{skipped:true}` landed, error →
   `{ok:false}`), lenient planning render at schedule + strict re-render at
   execution, leaf dispatch **single-shot** (no inner retry) vs park/control
   actions routed through `run_step` (which HAS the inner retry loop).
5. `on_error: continue` → land value, mark done. `fail_run` + failure value →
   retryable → re-spawn same node with backoff (attempts map, `Retry`-action
   policy or default); exhausted/non-retryable → whole run
   `{status:failed, error, failed_step, kind?, disposition?, leaf_preview?,
   attempts?}`.
6. `land_node_output`: value under node name + `decision` alias for approvals
   (last-in-await-order wins — deterministic: wave order, then spawn order).
7. Stall gate: done != nodes → failed (unreachable after validation, kept).

## (a) Model shape

```rust
pub struct GraphModel {
    pub run_id: String,
    pub workflow: Workflow,          // stored: restart sees the same DAG
    pub trigger: serde_json::Value,
    pub wave: usize,                 // current wave number (1-based, like today)
    pub node_state: BTreeMap<String, NodeState>,
    pub outputs: BTreeMap<String, serde_json::Value>,
    pub attempts: BTreeMap<String, u32>,   // outer (scheduler-level) attempts
    pub phase: GraphPhase,           // Ready | Running | Parked{node,until_ms,..}
                                     // | AwaitingApproval{node,..} | Succeeded
                                     // | Failed{..} | Cancelled
}

pub enum NodeState { Pending, Settled }
```

- No `step_index`. The cursor IS the map: done = all `Settled`; ready =
  `Pending` with all `needs` `Settled`, emitted in `BTreeMap` order. Same
  deterministic order as today's `ready` vec.
- `advance(&GraphModel, &GraphMsg) -> (GraphModel, Vec<GraphEffect>)` — pure,
  total, same contract as `advance`. Every verb persists first, then the
  driver runs the engine verb (recovery rule unchanged).
- Restart: settled nodes keep outputs; resume re-emits `ExecuteNode` only
  for `Pending` nodes of the current wave. A crash mid-wave re-runs no
  settled node — the witness in §(e).
- `decision` alias: computed deterministically as the approval decision of
  the highest `(wave, node-name)` — identical to today's last-in-await-order
  winner, but independent of host completion order (concurrent fan-out must
  not make the alias restart-dependent).
- Approval `outputs` insert the `Decision` payload under the node name (as
  today) plus the alias.

## (b) Effect vocabulary (per-node, not per-wave)

```rust
pub enum GraphEffect {
    ExecuteNode { node: String, wave: usize, attempt: u32 },
    ParkUntil { node: String, until_ms: u64, reason: NodeParkReason },
    AwaitApproval { node: String, message: String },
    Complete { output: serde_json::Value },   // the outputs MAP (run_graph shape)
    Fail { node: String, error: String, kind: String,
           disposition: String, leaf_preview: serde_json::Value, attempts: u32 },
    Cancelled,
    Unsupported { reason: String },
}
pub enum GraphMsg {
    Start { now_ms: u64 },
    NodeSucceeded { node: String, output: serde_json::Value, now_ms: u64 },
    NodeFailed { node: String, error: String, value: serde_json::Value, now_ms: u64 },
    Woke { now_ms: u64 },          // delay park OR retry backoff elapsed
    Approval { node: String, decision: serde_json::Value, now_ms: u64 },
    Cancel,
}
```

Per-node, not per-wave, for two reasons: restart granularity (a mid-wave
crash resumes node-exact, not wave-exact) and fold purity (the model
sequences; the host fans a wave's `ExecuteNode`s out with tokio tasks and
feeds each result back — the fold itself never executes, sleeps, or holds a
clock).

- The host detects wave boundaries by the `wave` field: it snapshots
  `model.outputs` at the first effect of a new wave and executes every node
  of that wave against the snapshot. This is exactly today's spawn-time
  snapshot — a node never sees same-wave siblings, even if one finished
  first. (Open question 5 asks to confirm per-node over per-wave effects.)
- Retry backoff reuses the engine park, not a host sleep: `NodeFailed` +
  retryable → `attempts[node] += 1`, emit `ParkUntil` (reason `Retry`); `Woke`
  re-emits `ExecuteNode{attempt}`. Restart-safe by construction, strictly
  stronger than today's `ctx.sleep`.
- Node executor is **single-shot** (guard → strict render → one
  `dispatch_leaf`), NOT `run_linear_step`'s inner retry loop: today's leaf
  nodes get exactly one dispatch + scheduler-level retry, and reusing the
  looping variant would multiply executions up to 9×. Proposed as
  `run_graph_node_step` next to `run_linear_step` in `linear_step.rs`
  (integration point only — not written in this slice).
- `attempt` rides the invocation (effect-key stability: same run+node+attempt
  ⇒ same key across restarts).
- Delay nodes: `ParkUntil` → `Woke` lands `{ok:true, waited:true}` under the
  node (same value as the linear model).
- Terminal `Complete` carries the outputs map — the `run_graph` shape, NOT
  the linear `{status:succeeded, steps}` shape. `Fail` carries the full
  enriched bundle (`disposition_for_kind`, `leaf_preview_for_value` are pure
  and reusable verbatim).

## (c) Preserved semantics (exact)

| Scheduler behavior | Model treatment |
|---|---|
| `needs` edges, wave scheduling, BTreeMap order | Ready-set derivation in the fold; emission order identical |
| Guard false → `{skipped:true,node,reason}` landed, node done | `NodeSucceeded` with the skip value (executor mirrors `run_linear_step` guard arm) |
| Guard error → `{ok:false}` → `on_error` applies | Same, via `NodeFailed` |
| `on_error: continue` (any failure) | Land value, mark `Settled` |
| `fail_run` + non-retryable failure | `Fail` effect with the enriched bundle |
| `fail_run` + retryable + policy allows | attempt++, `ParkUntil` backoff, re-issue on `Woke` |
| `fail_run` + retryable + policy exhausted | `Fail` with `attempts` = outer count (same fields as today) |
| Strict render at execution (workflow/node/field/ref attribution) | Executor re-renders strictly; render error → `{ok:false}` → `on_error` applies (planning leniency folded in — equivalent outcomes, one fewer pass; see below) |
| Planning-render failure, fail_run | `Fail{failed_step: node}` — same shape as today's early return |
| Planning-render failure, continue | Land `{ok:false, error: render:…}`, node done |
| `decision` alias, last-wins | Canonical `(wave, name)` max (deterministic under concurrency) |
| Stall gate | `Failed{<graph>}` if no ready set with pending nodes (unreachable after refusal, kept like today) |
| `validate_workflow_lineage` failure | Host/router integration note: the fn is `pub(crate)`-private; propose exporting it for the router (open question 3) |
| `build_slot` | Dead in the scheduler (forced `false` at every call site) → the model ignores it. Parity = no serialization. If slot leases ever become real, that is a new semantic, not this slice |
| `to_graph()` chains | Single-chain graphs schedule one-node waves — behaviorally the linear path (convergence option, §(f)) |

## (d) What still refuses (whole workflow stays on `WorkflowEngine::run`)

Static (router + `open`, before any row — same rule as the linear slice):

- `graph.is_some()` graphs containing ANY node whose action is NOT a
  HostSink leaf, `Delay`, or `RequestApproval`. Concretely refused:
  `Spawn` (host spawn-await lives on the old path), `Parallel`, `Foreach`,
  `Branch`, `Retry`, `Reduce`, `Loop`, `CallWorkflow`, `Park` (all need
  Context expansion via `run_step`), and `Unsupported` partition actions
  (`React`, `CallWebhook`, …).
- Cycle / unknown-need / empty graph: today's runtime validation fails them
  as `{status:failed}` values, but refusal-before-row routes them to the old
  engine, which fails them **identically** — no observable change, and the
  durable store stays clean. (Open question 1 asks to confirm refusal over
  failed-value parity.)
- A graph whose nodes are ALL durable-sequencable is admitted; anything else
  refuses whole-graph (never partial: a `parallel` fan-out half-run on
  durable then failing is worse than clean routing). (Open question 4.)

Rationale for refusing control-flow rather than recursing: expansion
(`run_parallel` copies, `run_branch` arms, `run_retry` policies) is Context
execution, not sequencing. A model that sequences what it cannot execute
would be the silent-success lie ADR-0043 §4 forbids.

## (e) Parity witnesses

In `toron-workflow/tests/durable_host.rs` (+ `durable_model.rs` for pure-fold
units), mirroring the existing witness style:

1. `graph_diamond_join_runs_each_node_once` — SC-17 shape
   (scout→plan→{fix_a,fix_b}→ship, leaf actions): 5 `ExecuteNode`s, wave
   order scout < plan < {fix_a, fix_b} < ship, outputs under all five names,
   runner-visible dispatch order deterministic.
2. `graph_restart_between_waves_runs_no_settled_node_twice` — settle wave 1,
   drop the driver, reopen: only wave-2 nodes re-issue; settled outputs
   byte-identical; engine row shows no repeated settlement.
3. `graph_refused_shape_leaves_no_row` — `parallel`-node graph AND
   `spawn`-node graph → `Unsupported`, plus the no-row assertions (no state
   row, no engine row — the mirror of
   `unsupported_shapes_stay_on_the_old_engine`).
4. `graph_approval_alias_is_wave_ordered` — two approvals in one wave: alias
   = name-later node regardless of feed order (feeds delivered reversed).
5. `graph_fail_run_exhaustion_reports_attempts` — retryable failure,
   policy exhausted → `Fail` bundle fields match today's shape.
6. Host-level (toron-mcp, `bd-host-cut-pqkg` pattern): SC-17 ported onto the
   durable driver, and the existing `graph_dag_runs_and_collects_outputs`
   + `graph_parallel_node_expands_copies` engine tests keep passing
   untouched (the latter stays old-path by refusal — its passing pins the
   routing).

## (f) Convergence note (not proposed, for the captain)

`to_graph()` makes every linear workflow a single-chain graph, so the graph
model *could* absorb the linear driver. Do not flip yet: keep the witnessed
linear driver until the graph model lands with §(e) green; then either (i)
route chains to the graph model and retire the linear fold, or (ii) keep both
(the linear fold is then dead weight — laziness says (i)). The flip is a
separate bead with a diff-of-behavior witness (chain-graph output shape vs
linear `Complete` shape must be reconciled first: they differ today).

## Cannot-preserve list (explicit, not papered over)

1. **Concurrent timing.** Old waves ran concurrently inside one replay
   function; the durable host fans out with tokio tasks. Completion order
   differs run to run — but outputs land by node and the alias is
   order-canonical, so nothing observable depends on it except wall time.
   Preserved observationally, not mechanically.
2. **`Retry`-action policy nodes.** A node whose action is `retry:` stays
   old-path; all durable nodes use the default coordinator. No silent policy
   change because the whole graph refuses with them.
3. **In-flight Compat.** None needed: no graph promise exists on the durable
   store; old-path in-flight runs stay on Resonate until drained.
4. **Backoff substrate.** `ctx.sleep` → engine `ParkUntil`. Stronger
   (restart-safe), not identical — named, not hidden.
5. **Run lifecycle rows.** The driver reports through its own journal/store
   (existing pattern), not `RunJournal` deps — same posture as the linear
  slice.

## Open questions (for review)

1. Cycle/unknown-need/empty: static refusal (recommended — old engine still
   fails them identically) or failed-value parity in the model?
2. Linear convergence via `to_graph`: flip now, later, or never?
3. `validate_workflow_lineage` is `pub(crate)`-private: export it for the
   host router, or skip the check on the durable path?
4. Refuse-whole-graph on one control-flow node (recommended) vs admit-graph
   / fail-the-node?
5. Per-node effects (recommended: restart granularity + pure fold) vs
   per-wave effects?
6. Single-shot node executor placement: `linear_step.rs` next to
   `run_linear_step`?

## Captain review — 2026-11-02

Approved to implement with the answers and two required changes below.

**Open questions**

1. Cycle / unknown-need / empty: **static refusal** before any row. The old
   engine still fails them with the same `{status:failed}` value, and a clean
   store beats a durable row that only records a doomed run.
2. Linear convergence via `to_graph`: **later, separate bead**. Keep the
   witnessed linear driver; its `Complete` shape differs from the graph
   output map. Do not flip in this slice.
3. `validate_workflow_lineage`: **export it and run it in the router /
   preflight** for the durable path too. A validation the old path performs
   must not silently disappear because the workflow took the new path.
4. Refuse-whole-graph on any control-flow node: **yes**. A half-run graph that
   fails on the second node is worse than routing the whole graph to the old
   engine.
5. Per-node effects: **yes**. Restart granularity is the point; a wave
   effect re-runs settled siblings on a mid-wave crash.
6. Single-shot executor: **`linear_step.rs`, next to `run_linear_step`**.
   One file owns "run one node of this kind"; both call
   `classify_step_outcome` and `invocation_for_leaf`.

**Required change 1 — keep the planning render in the model.**

The design's "compile the planning render into the strict execution render"
is not parity. The old scheduler renders leniently **before spawning the
wave** and, on failure:

- `fail_run` → immediate `{status:failed, error, failed_step}` and the rest of
  that wave is never spawned;
- `continue` → lands `{ok:false, error}` and settles the node.

Do the same render in the pure fold when it schedules a wave. It is
`serde_json`-only and already pure. The strict execution render stays in the
node executor, exactly as today. This keeps the failure shape and the
no-later-node behavior, and removes the "same shape as today" claim that the
current text cannot meet.

**Required change 2 — carry the graph approval evidence.**

The old graph approval publishes `build_approval_payload`: run, workflow,
`suspended_at {node, wave}`, nodes completed/failed/pending, artifacts,
ledger path, blast radius, timeout, evidence hash. `GraphEffect::AwaitApproval
{ node, message }` loses all of it. Put the payload in the effect (the fold
has workflow, wave, outputs, graph, so it can build it purely), and have the
host publish that payload instead of a bare message. The linear slice's
`PendingEntry` is not enough for a graph park.

**Still to name in the implementation, not hide**

- The alias winner stays `(wave, node-name)` max; that matches today's
  spawn/await order. A test feeds two approvals reversed and asserts it.
- `Attempt` on the effect key must be the scheduler attempt, not the leaf
  attempt, so a retry after restart reuses the same effect identity.
- Refusal must leave no state row and no engine row; assert both.

## Captain addendum — 2026-11-02 (failure carrier)

The linear cut has a parity hole the graph model must not inherit:
`run_linear_step` returns the full `classify_step_outcome` value, but the
host calls `step_failed(error)`, and the fold stores `{ok:false,error}` for
`continue` and emits `Fail{step,error}` for `fail_run`. That drops `kind`,
`disposition`, `leaf_preview`, `retryable`, and `attempts`.

Required while adding graphs:

- One carrier for a finished node's value (for example `StepDone { value }`);
  the **fold** applies `is_failure_value` + `on_error`. `continue` lands the
  full value; `fail_run` emits `RunEffect::Fail { step, output: Value }`
  carrying the enriched bundle. `step_failed(error)` is retired rather than
  extended.
- The host settles the terminal from that output, not from a bare error.
- Witnesses: a `continue` failure keeps `kind` + `leaf_preview` in outputs; a
  `fail_run` retry exhaustion keeps `attempts`; a restart after a `continue`
  failure resumes with the bundle intact.

Also: `RunEffect::Unsupported` at drive time must **not** start the legacy
run with the same id. The router refused unsupported shapes before any row;
if this branch is ever reached, the durable model already exists and the
sweep would drive it again — a double run. Log it as an invariant violation
and stop; do not spawn the old engine.
