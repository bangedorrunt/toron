<!-- governed-by: ADR-0003, ADR-0005, ADR-0012, ADR-0032, ADR-0040 -->
---
status: accepted
date: 2026-11-01
decision-makers: bangedorrunt
consulted: elm-effect-rs ADR-0005 (durable-engine port and OurEngine); elm-effect-rs ADR-0002 (carrier law); docs/durable-engine/resonate-removal-inventory.md
informed: flywheel, beads, chiebukuro maintainers
---

# ADR-0043: Replace Resonate with the in-house durable engine

**Status:** Accepted — captain word 2026-11-01 ("accept the adr, keep cranking
away"), relayed to the captain session as the acceptance of this record. The
in-house engine becomes the substrate as each cutover step lands with its
witness; Resonate is removed in the recorded order, never ahead of its
replacement.

**Decision in one sentence:** toron's workflow execution substrate is the in-house
durable engine — `OurEngine` plus the pure run model and its `DurableRun` host
driver, reached through the one kernel port `toron_core::kernel` — and Resonate
becomes a design reference that is removed in the recorded order, one witnessed
step at a time.

**Supersedes (in part):** [ADR-0005](0005-orchestration-layer.md)
invariant 5 ("Resonate is the only execution substrate", §971) and its §134
assumption; [ADR-0032](0032-workflow-engine-honesty-jido-contracts.md) sentence
"Keep Resonate" (§18). Everything else in both records stands: the closed
`Action` enum, the DAG, the normalized action boundary, the effect journal, the
run lifecycle, the kind registry, and the frozen tool surface.

**Supersedes (in part, continued):** [ADR-0012](0012-standalone-cron-type.md)
Spec 5 ("Resonate-native" cron). Cron timing moves to `CronSchedule` in the
kernel; registration, pause/resume, catch-up policy, and the tick body stay
host-side.

**Amends:** [ADR-0040](0040-adopt-elm-effect-for-the-comms-kernel.md) by scope
only — the kernel port now also carries the durable-engine types
(`DurableEngine`, `OurEngine`, `EngineStorage`, `JournalStore`, `RunEnd`,
`CronSchedule`). No second runtime, no second store.

## Context

elm-effect-rs ADR-0005 ships the engine: a synchronous kernel with fallible
`EngineStorage` and `JournalStore`, park/wake planning, terminal settlement, and
pure cron timing. toron already carries the port (`toron_core::kernel`), the
SQLite bridges (`ToronJournal`, `ToronEngineStore`, `SyncWorkflowState`), and
the replacement slices:

- `engine/durable.rs` — pure `RunModel` / `RunMsg` / `RunEffect` / `advance`.
- `engine/durable_host.rs` — `DurableRun`: one model, one engine row, one
  `RunState` seam; persists the model first, then drives the engine verb.
- `cron_elm.rs` — host-driven cron over `CronSchedule` + `OurEngine`.

What is missing is not the engine. It is the cutover: `WorkflowEngine::run`
still calls `Resonate::run`, and `control_flow.rs` still suspends through
`Error::Suspended`. The blast radius of that swap is recorded in
[docs/durable-engine/resonate-removal-inventory.md](../../docs/durable-engine/resonate-removal-inventory.md).

## Decision

1. **The in-house engine is the substrate.** A workflow run is a `RunModel`
   driven by `DurableRun`; its park is an `OurEngine` park; its completion is a
   journal settle through `kernel::end_run`. No Resonate process participates in
   a new run.
2. **The model is explicit, not a replay shim.** `advance` is pure and total
   over the linear shape. It executes nothing: every host effect leaves as
   `RunEffect::ExecuteStep` and comes back as a `RunMsg`. There is no second
   runtime and no replay VM.
3. **One source of truth per fact.** The model owns progress and outputs; the
   engine owns the park and the fence; the effect journal owns external-effect
   settlement. The driver persists the model before the engine verb, because a
   lost park is recoverable from a parked model and a lost model is not.
4. **Shapes are admitted, not guessed.** A shape the linear model cannot
   sequence (`graph:`, `retry`, `branch`, `foreach`, `parallel`, `reduce`,
   `loop`, `spawn`, `park`) is refused before any engine or state row exists, so
   the host keeps it on the old path until that shape's model lands. Silent
   success is never an option.
5. **Approval and delay keep their visible behavior.** `delay` parks until
   `now + secs`; `request_approval` parks under a one-day watchdog so a
   forgotten decision resurfaces in the wake drain instead of disappearing.
   Both survive a restart by re-parking from the model.
6. **Resonate is removed in the recorded order**, each step with its witness,
   and only after this record is accepted. The order and witnesses are the
   inventory's §7: cron, graph, host run path, server selection and doctor,
   testkit harness, dependencies, decisions and prose, corpses.

## Witnesses

| Property | Witness |
|---|---|
| Restart resumes at the unsettled step, not the first | `toron-workflow/tests/durable_host.rs::restart_resumes_at_the_unsettled_step` |
| A delay parks across a restart and the wake drain finds it | `durable_host.rs::delay_park_survives_restart_and_rewakes` |
| An approval lands a decision and settles the run | `durable_host.rs::approval_parks_with_a_watchdog_and_lands_the_decision` |
| Non-linear shapes are refused before any row exists | `durable_host.rs::unsupported_shapes_stay_on_the_old_engine` |
| Phase guards are typed, not silent | `durable_host.rs::out_of_phase_transitions_are_typed_errors` |
| The model is pure and total over the linear shape | `tests/durable_model.rs` (6) |
| The state seam survives a real process boundary | `toron-core/tests/durable_workflow_store.rs` (2) |
| The engine store survives start/park/restart/wake | `toron-core/tests/durable_engine_store.rs` |
| The framework stays behind one port | `toron-core/tests/kernel_fence.rs` |

## Consequences

- **Durability moves from a server-side promise store to local SQLite.** The
  claim changes from "survives the server" to "survives the host", and the
  witness for it is the restart test above, not a running server.
- **In-flight runs need a bridge.** Runs parked today carry Resonate promise
  ids published in kind-46011/43001. The cutover either resolves those ids
  through the old server once, or drains them before the flip; a run that
  suspends forever with no error is the failure this record must not ship.
- **flywheel is an out-of-repo caller.** `WorkflowEngine::remote_*`,
  `resolve_approval`, and `resolve_spawn` are called from the flywheel build.
  Removing them is a compile break there, coordinated in the same release.
- **Graph workflows are refused until the graph model lands.** SC-17 keeps its
  behavior on the old path, and the refusal is the witness that it did not
  silently move.
- **Rollback** is: keep the old server running, redeploy the previous build,
  and replay any run id created after the flip. Nothing in the new path deletes
  a Resonate row.

## Decision log

- **2026-11-01 — accepted.** Captain word; the linear driver, cron host loop,
  and the removal inventory were already green before acceptance. The default
  run path flips on the host-loop cut (inventory step 5), not on this record.

## Alternatives considered

- **Keep Resonate and wrap it.** Rejected: the wrapper still needs the replay
  VM the in-house engine deliberately does not have, and it keeps a server and
  a remote URL in the failure path.
- **Port the async `Context` surface.** Rejected: `ctx.run`/`ctx.sleep`/
  `ctx.promise` assume deterministic replay. A synchronous kernel plus an
  explicit model is smaller and testable without a server.
- **Delete Resonate in one change.** Rejected by the inventory: the run path,
  the testkit harness, 24 live-server guards, and an out-of-repo caller would
  move at once with no witness between them.

## Completion addendum — 2026-11-02

The replacement is complete. Every shape toron ships is on the durable
models: linear leaves, delay, approval, graphs, branch (linear plan),
parallel/foreach/retry/call_workflow (graph fan-out and child runs), spawn
(launcher seam + durable claims), and the bounded loop (persisted counter).
`park` and the deprecated `reduce`/`react`/`call_webhook` refuse whole before
any row by contract. The loop, cron, and status paths in flywheel read local
rows and the engine store; no vendor process is started or probed.

The public `WorkflowEngine` remains as the embedding door, now implemented by
`engine/drive.rs` as a thin driver over `DurableRun`/`DurableGraphRun`: the
constructors keep the injected seams, `run`/`invoke` drive to terminal, and
approvals/spawns register one-shot waiters resolved by the existing drainers
using the run id / spawn key. The vendor schedulers, `CronManager`, the remote
engine selection, the testkit server harness, and the `resonate-sdk`
dependency are deleted. A `{status:"failed",…}` terminal is still a VALUE,
not an `Err`.

Accepted witnesses: toron-workflow lib/durable suites; toron-mcp durable and
the ported host lifecycle; the seven e2e scenarios with no server; flywheel
cron/loop/spawn/crew filters; clippy `-D warnings`; and the live stack check
recorded in the removal inventory.
