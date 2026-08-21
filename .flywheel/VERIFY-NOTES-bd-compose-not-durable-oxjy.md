# VERIFY notes — bd-compose-not-durable-oxjy (SandySparrow, 2026-08-21)

Durable record: composed workflows now run through the durable engine. The
code fix lives in the flywheel repo (commit a1498f1, which bundled this with
the ADR-0022 W2 phase work — the oxjy id is not in that commit message). This
bead's tracker repo is toron, and the ADR-0019 SHA rule derives the close sha
from THIS repo — so this anchor commit carries the bead id here.

## What the fix does (flywheel src/main.rs, WorkflowCmd::Compose --run)

- Composed runs call `engine.invoke(&run_id, invoke_wf, inputs)` — the same
  durable struct-entry the catalog/trigger path uses (pane agents are the
  executor surface only).
- PolicyEvents recorded during invoke are drained on the same runtime and
  executed (on_failure: bead spawns a failure bead via cron::spawn_failure_bead;
  other actions are logged, never silently dropped).
- SpawnSink drainer kept alive across invoke (bd-omwg), workspace reaped after.

## Verification evidence

- Fence (bead description, verbatim): `cargo test -p toron-workflow`
  → 173 passed + 6 integration tests, 0 failed.
- Crash-resume equivalence gate (description "Gate"): pinned by
  `invoke_redelivery_replays_recorded_run`
  (toron crates/toron-workflow/src/engine/tests.rs:1401) — a redelivered
  run_id replays the RECORDED execution: identical output, steps execute
  exactly once. Passing; semantics now hold for ANY invoke caller, compose
  --run included.

## Provenance note for the ledger

a1498f1's message names the ADR-0022 W2 work only; the oxjy routing block
(commented bd-compose-not-durable-oxjy in flywheel src/main.rs) rode the same
commit. This anchor + the close reason bind the id to the sha the loop derives.
