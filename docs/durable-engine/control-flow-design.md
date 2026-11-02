# Durable control-flow run models: branch / retry / parallel / foreach / loop / call_workflow / park

Bead: `bd-control-flow-design-0jmd`. Design for captain review. **No code.**
Step after the linear slice, the graph slice (`graph-model-design.md`,
approved), and the spawn park (`spawn-park-design.md`, approved + transport
addendum). Reuses their patterns rather than inventing a second scheduler.

Governing principles (beads): `lower-before-you-fork` — a shape that is a
graph or a sequence reuses that model; `one-scheduler-per-model` — the pure
model owns sequencing, not the host.

## Where the schedulers live today

The vendor engine owns expansion: `run_parallel` (copies),
`run_branch` (arms), `run_retry` (policy), `run_foreach` (fan-out),
`run_loop` (iteration), `run_call_workflow` (child run), `Park`
(Context suspension). The durable linear + graph models own leaves, delay
(`ParkUntil`/`Woke`), approval (`AwaitingApproval` + decision alias +
evidence payload), and spawn (spawn park + launcher seam + durable claims).

Shipped usage to cover (measured, not assumed): flywheel builtins/crew
composition use `parallel` ~18x, `call_workflow` ~22x, `retry` ~6x,
`branch` ~4x, `foreach` ~2x, `loop` ~2x. Toron workflows
`housekeeper-diagnose` and `agent-reach-upgrade` use `branch` with a pure
guard (`{{steps.triage.patch}} != null`, `{{steps.upgrade.verified}} ==
true`) around leaf steps — but note their arms also contain
`dispatch_job` / `send_dm` / `call_webhook`, and `call_webhook` is an
`Unsupported` partition action, so those two workflows refuse whole-graph
until those actions port, independent of the branch lowering below.

## Uniform rules (all shapes)

**Refusal.** Static, before any row: a workflow using an unimplemented
shape stays on `WorkflowEngine::run` in full — never partial. No state row,
no engine row (assert both, mirroring `graph_refused_shape_leaves_no_row`).
Drive-time `Unsupported` is an invariant violation: log it and stop, never
start the legacy run with the same id (double run). This is the graph
slice's rule (captain review Q1/Q4) extended to every shape here.

**Schedule-time reads.** Guards, iterators, policies, and child references
are read in the pure fold from `workflow` + `trigger` + `outputs` only.
No host, env, clock, or bus reads inside the fold. Anything the fold cannot
decide from those three inputs refuses (see each shape).

**Restart.** Settled work keeps outputs; resume re-emits effects only for
unsettled work. Child keys are deterministic functions of
`(run_id, node, attempt[, index])` and attempt rides the effect key, so a
retry after restart reuses the same effect identity (graph slice, captain
review: scheduler attempt, not leaf attempt).

**Exactly-once.** Model-first: persist the intent before the host acts.
Single-shot node executors. Idempotent open/settle by key. No double
dispatch on restart — resume never re-emits a settled node, copy, item, or
child run.

**Failure carrier.** The full `classify_step_outcome` bundle (`kind`,
`disposition`, `leaf_preview`, `retryable`, `attempts`) plus `on_error`:
`continue` lands the full value; `fail_run` emits `Fail` carrying the
enriched bundle, with `attempts` on exhaustion. `step_failed(error)` stays
retired (failure-carrier addendum). The host settles the terminal from that
output, not from a bare error.

## parallel — lower onto the graph (no new model)

**(a) Lowering.** `parallel` copies are graph nodes `{node}:{i}` in a single
wave; the join is a downstream node with `needs` = all copies. One fan-out
mechanism, reused by `foreach`. Load-bearing (18x), zero new model surface.

**(b) Schedule-time reads.** The copy list is the static step list — read
from the workflow definition in the fold. No host read.

**(c) Restart / join.** Settled copies keep outputs; resume re-emits
`ExecuteNode` only for pending copies. Join key = downstream node name;
the join runs when all copies are settled (partial waves resume, never
re-run).

**(d) Exactly-once.** Each copy dispatches once: single-shot executor +
effect key `(run_id, node, index, attempt)`.

**(e) Failure.** Per-copy `on_error`: `continue` lands the copy's full
failure value; `fail_run` fails the run with the bundle (which copy failed
is in `failed_step`). The join sees landed values only.

**(f) Refusal.** While unimplemented: whole workflow refuses before any row.
After: a copy containing an unsequencable action refuses the whole workflow
(a half-fanned run is worse than clean routing).

**Witnesses** (`durable_model.rs` pure + `durable_host.rs` driver):
`parallel_fanout_runs_each_copy_once`, `parallel_restart_reruns_only_pending_copies`,
`parallel_join_collects_all_copy_outputs`,
`parallel_copy_failure_continue_lands_bundle`.

## foreach — same fan-out path as parallel (no new model)

**(a) Lowering.** `foreach` over a list is a fan-out with N named child
nodes `{node}:{i}`, identical machinery to `parallel`; the only new work is
expanding the iterator. Implement on the shared path; if the audit shows
both shipped uses are static lists, rewriting them as `parallel` is the
acceptable fallback. Either way there is one fan-out mechanism, not two.

**(b) Schedule-time reads.** The list is read in the fold from trigger /
inputs, or from upstream `outputs` (the fold has the map — pure, no host
I/O). A list that needs a host read to materialize refuses.

**(c–e) Restart / join / exactly-once / failure.** Same as `parallel`:
settled items keep outputs, item-keyed effect keys, per-item `on_error`,
downstream join on all-settled.

**(f) Refusal.** Unimplemented → whole workflow refuses before any row.
Post-landing refusal only for unmaterializable iterators or unsequencable
item bodies.

**Witnesses:** `foreach_expands_list_to_named_nodes`,
`foreach_restart_keeps_settled_items`, `foreach_item_failure_applies_on_error`,
`foreach_unmaterializable_iterator_refuses_without_row`.

## branch — lower onto graph node guards (no new model)

**(a) Lowering.** No branch model. Each arm's steps become nodes guarded by
the branch condition (`then`: `if`; `else`: its negation); guard-false
lands `{skipped:true}` exactly like a graph guard. The two toron workflows
already fit this shape (pure guards over leaf steps).

**(b) Schedule-time reads.** The guard is evaluated by the existing guard
evaluator from trigger / inputs / upstream outputs in the fold. Guards
needing host I/O refuse.

**(c) Restart.** Guard decisions re-derive deterministically from the same
inputs — no stored choice, no extra state. A restart cannot flip an arm.

**(d) Exactly-once.** Only the chosen arm dispatches; the skipped arm never
emits effects.

**(e) Failure.** Guard error → failure value + `on_error` (same as a graph
guard error). Arm-step failures apply the arm node's `on_error`.

**(f) Refusal.** Non-pure guards refuse whole-workflow before any row.
Arms containing unsequencable actions refuse whole-workflow (this is why
`housekeeper-diagnose` / `agent-reach-upgrade` stay old-path until
`dispatch_job` / `send_dm` / `call_webhook` port — the lowering is correct,
the arms are not yet sequencable).

**Witnesses:** `branch_true_arm_runs_false_skips`,
`branch_restart_never_flips_arm`, `branch_guard_error_applies_on_error`,
`branch_impure_guard_refuses_without_row`.

## retry — policy on graph nodes (small new surface, justified)

**(a) Model.** `retry` is not a shape, it is a per-node policy overriding
the default coordinator: `{ attempts, backoff }` read at schedule time.
Mechanics reuse the graph slice exactly (outer attempts map + `ParkUntil`
backoff with reason `Retry` + `Woke` re-issue). Load-bearing (6x, flaky
leaves) with near-zero new surface — this is the one place a small policy
addition beats rewriting all six call sites.

**(b) Schedule-time reads.** Policy comes from the workflow definition
(`retry:` wrapper). No host read.

**(c) Restart.** Attempt counts persist in the model; backoff parks are
restart-safe (`Woke` re-emits `ExecuteNode{attempt}`).

**(d) Exactly-once.** Scheduler-attempt effect keys: same run + node +
attempt ⇒ same key across restarts (captain review).

**(e) Failure.** Exhaustion emits `Fail` with `attempts` = outer count and
the full bundle (graph witness
`graph_fail_run_exhaustion_reports_attempts` covers the shape).

**(f) Refusal.** `Retry`-action policy nodes stay old-path until this lands
(already the graph rule); after landing, unparseable policies refuse before
any row rather than degrading to the default.

**Witnesses:** `retry_policy_exhaustion_reports_attempts`,
`retry_backoff_park_is_restart_safe`,
`retry_unparseable_policy_refuses_without_row`.

## call_workflow — child run + join (new surface, justified by 22x)

**(a) Model.** A child run with its own run id, joined by the parent node.
Unlike the fan-out shapes this is genuinely new surface — boundaries,
evidence, and failure attribution must survive the call — so it is not
lowered away:

```rust
// parent model
phase: AwaitingChild { node, child_run_id, .. }
// effects
ChildRun { node, child_run_id, workflow, trigger }
ChildJoin { node, child_run_id }   // emitted on child terminal
// messages
ChildSucceeded { node, output, .. }
ChildFailed { node, output /* full bundle */, .. }
```

Inline expansion (splicing the callee's steps with prefixed names) was
considered and rejected as the primary path: it erases the callee's
identity, blast radius, and evidence trail, and it cannot serve recursive
or dynamically-dispatched callees. It remains an allowed optimization only
if the audit proves all 22 shipped uses acyclic — the design does not
depend on it.

**(b) Schedule-time reads.** Child reference (workflow name/ref) + trigger
are read in the fold from the parent definition and trigger/inputs/outputs.
Unresolvable references refuse before any row.

**(c) Restart / join.** Child run id = `{parent_run_id}:{node}:{attempt}`,
opened idempotently (create-if-absent); the parent persists
`AwaitingChild` before the child opens (model-first). Join key = parent
node: the child terminal (success or failure value) feeds exactly one
`ChildSucceeded` / `ChildFailed`. A completed child is reused, never
re-opened; partial waves do not apply (one child per node per attempt).

**(d) Exactly-once.** Idempotent child open by key + single parent join.
A restarted parent re-attaches to the live or completed child instead of
opening a second one.

**(e) Failure.** The child terminal carries the full bundle; the parent
node's `on_error` applies to it. Recursion is refused initially (a
recursive callee fails admission before any row); relax only with an
explicit depth bound in a later slice.

**(f) Refusal.** Unimplemented → whole workflow refuses before any row. A
callee containing unimplemented shapes transitively refuses the parent
before any row (audit the callee at admission; no partial parent + failed
child).

**Witnesses:** `call_workflow_child_runs_once_and_joins`,
`call_workflow_restart_reuses_completed_child`,
`call_workflow_child_failure_carries_bundle_to_parent_on_error`,
`call_workflow_recursive_callee_refuses_without_row`,
`call_workflow_unresolvable_ref_refuses_without_row`.

## loop — rewrite, do not model (deferred sketch only)

`loop` needs iteration state (counter + condition + back-edge) and a
termination argument — the only shape here that is neither a graph nor a
policy. With ~2 shipped uses, a new loop model is not justified: rewrite
the two call sites as bounded `foreach` over a range or an unrolled
sequence, and refuse `loop` while unimplemented (stays on the vendor
engine). If a future loop cannot be rewritten, the sketch is: counter in
the model, condition read purely from outputs at each schedule tick,
explicit max-iteration bound (unbounded loops never admit), restart resumes
at the counter with iteration-keyed effect keys, max-iterations trips
`Fail` with the bundle. That sketch is not approved surface in this slice.

**Witnesses (only if the sketch is ever built):**
`loop_bounded_terminates`, `loop_restart_resumes_at_counter`,
`loop_max_iterations_fails_with_bundle`, `loop_unbounded_refuses_without_row`.

## park — stays refused (no model)

Bare `Park` (suspend until an external event) stays on the vendor engine.
The durable models already own what `park` was used for internally: `delay`
is `ParkUntil`/`Woke`, retry backoff is a `Retry` park, spawn-await and
approval are phased parks with their own settling messages. A workflow step
with action `park` refuses whole-workflow before any row. No new surface.

## Already owned (references, not new design)

- **Delay:** `ParkUntil { until_ms }` → `Woke` lands `{ok:true, waited:true}`
  (linear + graph slices).
- **Approval:** `AwaitingApproval` + `decision` alias (wave/name max,
  order-canonical) + evidence payload in the effect (captain review,
  required change 2).
- **Spawn:** spawn park model + launcher seam + durable `spawn_key` claims
  with adopt-on-restart + deadline parks (spawn-park design + transport
  addendum).

## Recommended implementation order

Dependency order, not usage order — each step reuses proven machinery:

1. **Branch guards** (4 flywheel + 2 toron uses, ~0 new surface): pure-guard
   lowering onto graph nodes. Unblocks the toron workflows' guard shape
   (their arms still wait on action ports). Proves schedule-time reads.
2. **Parallel expansion** (18x): fan-out onto graph waves + join node.
   Proves the fan-out/join discipline restarts depend on.
3. **Foreach on the same path** (2x): iterator expansion only. Shared
   mechanism with 2; rewrite-as-parallel is the fallback.
4. **Retry policy** (6x): per-node policy on the graph coordinator.
5. **call_workflow child-run + join** (22x, biggest): needs the join
   discipline of 2–3 proven first. Audit the 22 for recursion before
   building (recursive → refused; acyclic does not change the design).
6. **Loop: rewrite, no model.** Identify the 2 shipped uses and convert
   them; `loop` stays refused. Park stays refused permanently.

Usage order would put `call_workflow` first (22x). Not recommended: its
join is the least-proven piece, and the vendor path keeps serving those
workflows meanwhile — nothing is blocked, only the removal is sequenced.

## Witness index (where they live)

Pure-fold units in `toron-workflow/tests/durable_model.rs`, driver
restart/join tests in `toron-workflow/tests/durable_host.rs`, mirroring the
graph slice's witness style. Refusal tests always assert both absences (no
state row, no engine row). Exactly-once tests always kill the driver
mid-flight and assert single execution on resume.

## Open questions

1. Do any of the 22 shipped `call_workflow` uses recurse (directly or
   transitively)? If yes, the refusal stands and the audit names them; if
   all are acyclic, is inline expansion worth a follow-up bead, or does the
   evidence-trail argument keep child runs regardless?
2. `foreach` list source: trigger/inputs only, or also upstream `outputs`?
   Outputs-read is still pure (the fold owns the map) — confirm the
   boundary stays "no host I/O", not "trigger-only".
3. Which two workflows use `loop`, and what is each one's rewrite (unroll
   vs `foreach`-over-range)? Name them in the implementation bead.
4. Branch guard purity: which expression forms qualify as pure (today:
   template comparisons over trigger/inputs/outputs)? Name the checker.
5. `retry` surface: new per-node policy field on the workflow schema, or
   reuse the `retry:` action wrapper with the policy read at schedule time?
6. Partial-wave join: a join whose copies settle across a restart with
   mixed settled/pending — covered by resume-only-pending, but assert the
   join waits rather than firing early.
7. `call_webhook` / `dispatch_job` / `send_dm` inside branch arms: those
   actions need their own port beads before the two toron workflows can
   move; who owns that order?

## Captain review — 2026-11-02

Approved with one architecture correction and the open questions answered.

**Correction — the lowering target is not uniform**

The draft lowers branch/parallel/foreach onto the graph. That breaks the two
toron workflows, which spawn BEFORE they branch: a graph cannot admit a spawn
node today, so lowering them to a graph would refuse the workflow for the
wrong reason and leave `housekeeper-diagnose` on the vendor path.

Approved split:

- **branch** lowers into the **linear** model. A branch is a choice, not a
  fan-out: expand the arms into the linear step plan at open, evaluate the
  pure guard when the branch marker is reached, and run only the chosen
  group. Spawn-before-branch keeps working because nothing leaves the linear
  model. The chosen arm's steps are ordinary linear steps (no second
  scheduler).
- **parallel / foreach / retry / call_workflow** lower onto the **graph**
  model, whose wave scheduler already is the fan-out/join engine.
- A workflow that mixes a graph-lowered shape with `spawn` or with a linear
  branch is refused WHOLE, with a named follow-up bead if a shipped workflow
  ever needs the mix. The audit says none do: the crew compositions use
  parallel + call_workflow (no spawn), the toron workflows use spawn + branch
  (no parallel).
- Do not add graph spawn/child-run nodes in this epic. If a real workflow
  later needs them, that is a separate bead with its own witnesses; the
  refusal above is the honest interim.

**Answers**

1. **call_workflow recursion:** audit the 22 before implementation; recursive
   callees refuse (as drafted). Child runs stay the model even if all 22 are
   acyclic — inline expansion would erase the callee's identity, evidence, and
   blast radius, and the 22 include dynamically named children. No inline
   optimization in this epic.
2. **foreach list source:** trigger, inputs, AND upstream `outputs` are all
   pure (the fold owns the map). The boundary is "no host I/O", not
   "trigger-only". A list that needs a host read refuses.
3. **loop:** the two shipped uses are flywheel builtins `climb`
   (`max: {{inputs.max_iters}}`) and `cycle` (`max: {{inputs.max_iterations}}`),
   both `until:` loops over `dispatch_job`. Rewrite each as a bounded guarded
   SEQUENCE of that many repetitions (the max is a small integer default,
   10 and 3), with each repetition guarded by the negated `until`. `loop`
   stays refused permanently; the rewrites land in the loop bead.
4. **guard purity:** the accepted grammar is exactly the template comparisons
   the guard evaluator already resolves from trigger/inputs/outputs
   (`{{ref}} <op> <literal>`, boolean combinations). Anything that reaches the
   host — shell, env, clock, files — refuses before any row. Name the checker
   in the branch bead and reuse it, never a second parser.
5. **retry surface:** reuse the existing `Action::Retry { step, max_attempts,
   backoff_secs }` fields read at schedule time. No schema migration and no
   new policy field; a retry wrapper around a graph-lowered shape is itself a
   refusal until that shape lands.
6. **partial-wave join:** the join MUST NOT fire on a partial wave. Witness:
   one copy settled, one pending, restart — the join effect is absent until
   the pending copy settles, and no settled copy is re-dispatched.
7. **actions inside arms:** `dispatch_job` and `send_dm` are HostSink leaves
   and already sequencable, so `housekeeper-diagnose` is unblocked by branch
   alone. `agent-reach-upgrade`'s fail-closed arm uses `call_webhook`, which
   is Unsupported by contract — rewrite that arm to `send_dm` +
   `on_error: fail_run`, do not port `call_webhook`. The branch bead owns that
   workflow rewrite.

**Implementation order (confirmed)**

1. Branch (linear lowering) + the `agent-reach-upgrade` rewrite; housekeeper's
   guard shape becomes durable.
2. Parallel (graph fan-out + join), with the partial-wave join witness.
3. Foreach on the same path.
4. Retry policy on graph nodes.
5. call_workflow child-run + join, after the recursion audit.
6. Loop rewrites (`climb`, `cycle`); loop stays refused.

Each bead inherits the uniform rules above: refusal before any row,
model-first persistence, full failure carrier, restart-only-pending, and the
whole-workflow rule. No shape ships with a partial-wave or double-dispatch
hole.
