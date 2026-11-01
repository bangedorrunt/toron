# Host cut design: daemon run path over to the durable host

Bead: `bd-host-cut-pqkg`. Step 5 of `resonate-removal-inventory.md` §7.
Status: design accepted as the build order below; implementation lands in
reviewable slices, smallest first.

## Rule

`unsupported_reason(&workflow) == None` → durable host. Anything else →
`WorkflowEngine::run` (the Resonate path), unchanged. The refusal happens
**before any row exists**: `DurableRun::open` checks the shape before
`engine.start` and before the state save, so a refused workflow leaves no
engine row and no model row to clean up. The host router (`durable_route`)
checks first so the refusal is a routing decision, not an error; a
`HostError::Unsupported` escaping the open is defense-in-depth and falls
back to the old path with a warn, never a failure.

No-store rule: with no `WorkflowEffectStore` attached (journal-less
`over_bus` embeddings) the durable arm cannot build its three handles, so
the router returns `Err("no durable store")` and everything stays on the
old path. Fail-closed, never a silent downgrade.

## (a) Every function in `workflow_host.rs` that changes, and how

- `WorkflowHost` struct: two new fields.
  - `seams: LeafSeams` — the host's context-free leaf policy
    (`toron_workflow::engine::leaf::{LeafSeams, dispatch_leaf}` is the ONE
    implementation; `dispatch_leaf_or_compat` is the Context adapter, so the
    two substrates cannot drift).
  - `durable_parks: Arc<std::sync::Mutex<HashMap<String, DurablePark>>>` —
    `run_id → { node, generation, workflow, trigger }` for approvals parked
    on the durable path. Separate from `PendingDrain`: the drain validates
    Resonate promise ids, which do not exist here.
- `new()`: clone the `async_effect_runner` Arc **before** it moves into the
  engine constructors; after the engine is built, set
  `seams = { async_runner: Some(clone), sync_runner: None,
  verify_sink: engine.verify_sink().clone(), journal_mode: Enforced }`.
- `over_bus()`: `seams.async_runner = effect.clone()`; `journal_mode` is
  `Enforced` when a runner is present, `Recording` when absent (mirrors the
  engine arm built on the next line).
- `over_bus_with_engine()`: `seams = LeafSeams::default()` — the router
  never selects the durable arm without a store, so the value is inert.
- `durable_route(workflow, has_store) -> Result<(), String>` (new, pure):
  `Err` with the `unsupported_reason` text, or `"no durable store"`.
  Unit-tested, no I/O.
- `spawn_run_and_drain()`: after the lifecycle admit + durable track, branch
  on `durable_route`. Durable arm spawns `run_durable_loop(run_id, workflow,
  input.trigger)`; old arm is the current `engine.run` spawn + follower,
  byte-identical.
- `run_durable_loop()` (new): build the three handles from
  `pending.store().pool()` — `ToronEngineStore::spawn`,
  `ToronJournal::spawn(JournalDb::new(pool, "host", 64))`,
  `SyncWorkflowState::spawn` — then `open_host_run` inside
  `spawn_blocking` (every `DurableRun` verb is sync-over-channels and must
  never run on an async worker), then `drive_durable_effects`.
- `drive_durable_effects()` (new): the effect loop, now with a
  `workflow_digest: String` parameter.
  - `ExecuteStep { index, step }` → `run_linear_step(LinearStep { run_id,
    workflow: &workflow.name, workflow_digest: &digest, step, index,
    trigger, outputs: &mut outputs }, &seams).await`, then `step_ok` /
    `step_failed` back through `spawn_blocking`. `digest` is computed once
    per run with `toron_workflow::identity::workflow_digest` — the same
    helper the old path uses (`control_flow.rs` feeds it into every
    invocation) — and rides in `DurablePark` for continuations, so step
    effect keys are stable across restarts. (The `""` stored by
    `track_run_durable`'s `workflow_runs` row is untouched: that row's
    digest column is the old path's concern, not this slice's.)
  - `ParkUntil { until_ms, .. }` (delay) → sleep until the deadline, then
    `woke()` and continue. Holds one task per delayed run; acceptable for
    this slice (see restart note).
  - `AwaitApproval { step, .. }` → record `durable_parks`, publish the
    pending (side-channel 1), `set_run_state_durable(Suspended)`, return.
  - `Complete` / `Fail` → `set_run_state_durable` terminal +
    `publish_terminal` (kind-46012, unchanged shape), return.
  - `Unsupported` → unreachable via the router; falls back to the old path
    with a warn.
- `resolve_parked()`: before touching `PendingDrain`, check `durable_parks`.
  On a hit, validate `res.promise_id == run_id` and generation equality, then
  `DurableRun::open` (resume path — the model wins, workflow/trigger args
  ignored) + `approve(decision)` in `spawn_blocking`, then continue driving
  with `drive_tracked`. Miss → current promise-id path untouched.
- `sweep_durable_runs()` + `spawn_durable_sweep(interval)` +
  `drive_swept_run()` + `drive_tracked()`: the restart resumer (see §(c)).
- `WorkflowHost.durable_active: Arc<Mutex<HashSet<String>>>`: runs
  currently driving in this process; the sweep skips them.
- `DurablePark.workflow_digest`: the continuation's effect identity.
- `publish_approval_pending` / `publish_terminal`: reused as-is. The durable
  `PendingEntry` sets `promise_id = run_id` (the correlation key change).
- Tests (in-file `#[cfg(test)]`, multi_thread): `durable_route` admits a
  linear `SendMessage` workflow, refuses `graph:`, refuses `Spawn`, refuses
  everything without a store; kind-46011 pending body for a durable entry
  carries `run_id` and no `verdict` (so `parse_approval_resolution` ignores
  it); resolution validation rejects a wrong-generation durable resolve.

## (b) The three side-channels

1. **Publishing an approval.** The old engine owned this (promise creation
   surfaced in the approval sink; the drain published it). The model does not:
   on `RunEffect::AwaitApproval` the host builds the `PendingEntry` itself
   and publishes kind-46011 with the **run id as the correlation key** (it
   replaces the Resonate promise id). Pending publications carry no `verdict`,
   so they never parse as resolutions (existing wire-contract test pins this).
   An operator resolving an older event (promise-id era) against a durable run
   finds nothing — documented, and the resolve path ignores unknown ids loudly
   rather than erroring.
2. **Resolving an approval.** `resolve_approval` / `reject_approval` become
   `DurableRun::approve` through `SyncWorkflowState`, not
   `Resonate::promises.resolve`. A resolution that arrives **before** the park
   is durable (no `durable_parks` entry yet) is ignored + debug-logged — the
   same posture as the old path's unknown-promise ignore; the operator
   re-resolves after the pending appears. No queue, no silent hold.
3. **The park drain.** `PendingDrain::drain` reads the engine's approval sink;
   the durable path never touches the sink — the `AwaitApproval` effect IS the
   drain event. `drain_all` / the per-run follower keep serving old-path runs.
   Spawn parks are untouched: the model refuses `spawn`, so `SpawnSink` stays
   on the old path with the workflow (no second spawn-await implementation).

## (c) Restart resume (production, not test-only)

The durable arm holds `ParkUntil` as a task in the daemon, so a restart
would drop the timer and nothing would re-open the parked model. The old
engine got resume from the server; the durable path gets it from the
sweep — without this the cut is a regression.

- `spawn_durable_sweep(interval)`: one pass now, then the same path on the
  interval. The daemon wires this at startup (same place it spawns the
  trigger and resolution loops).
- `sweep_durable_runs()`: builds the three handles from the attached
  store's pool, lists models via `RunState::list()` (newest first), and
  per row: unparsable → warn, leave the row (never destroy evidence);
  `is_terminal()` → `delete` the row (terminal cleanup); live here
  (`durable_active`) or approval-parked (`durable_parks`) → skip; else
  spawn `drive_swept_run`.
- `drive_swept_run(run_id, …)`: re-reads the model (a row lost between list
  and open skips loudly), `DurableRun::open` (the persisted model wins —
  resume, not a fresh start), then `drive_tracked`, which derives
  workflow, trigger, and digest from the reopened model.
- `drive_tracked()` holds the run in `durable_active` for the whole drive.
  A second driver feeding `step_ok` for the same step would hit
  `WrongPhase` and fail the run, so the sweep never double-drives.
- Gap, named: two sweep tasks for the same run id started in the same
  pass both pass the skip-check (neither has inserted yet) and both open.
  The model's phase gate makes the loser fail loudly (`WrongPhase` → run
  marked failed), which is the wrong outcome for a race the host created.
  The witness that closes it: a test driving two concurrent sweeps over
  one parked run, asserting exactly one drives and the run still settles.
  Until then the interval stays wide relative to drive time.

Upstream (already green, not re-proved here):
`toron-workflow/tests/durable_host.rs::restart_resumes_at_the_unsettled_step`,
`::delay_park_survives_restart_and_rewakes`,
`::approval_parks_with_a_watchdog_and_lands_the_decision`.

This slice's host-level witness:
`durable_sweep_resumes_a_parked_run_after_restart` — host1 parks an
approval and dies; host2's sweep re-opens the persisted model, re-drives
it to a fresh park, and a store resolution settles it `Succeeded`.

## (d) What stays on the old path and where the refusal happens

Stays: every workflow with `graph.is_some()` (SC-17 unchanged), and any step
whose action is not `Delay` / `RequestApproval` / HostSink — `spawn`,
`branch`, `foreach`, `parallel`, `reduce`, `loop`, `call_workflow`, `retry`,
`park`. The refusal point is `durable::unsupported_reason`, called by the
router before any row exists and re-checked by `DurableRun::open` before
`engine.start`. `control_flow.rs`, `cron.rs`, and the Resonate constructors
are not touched by this slice.

## Fences

- `toron_core::kernel` types only (`OurEngine`, `end_run` via the driver).
  No file outside `kernel.rs` names an `elm-effect` crate (fence test).
- `crates/toron-workflow/src/engine/*` is DustySparrow's (run_linear_step
  now on disk at `engine/linear_step.rs` — design against that signature;
  this slice calls it, stubs nothing).
- `crates/toron-core/*` untouched.
