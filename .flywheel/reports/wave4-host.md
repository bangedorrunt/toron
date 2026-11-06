# wave4-host report — bd-wave4-host-move-hvc9

status: done
terminal: done

## Moved surface (toron-mcp → toron-workflow)
- New `toron-workflow::engine::host` (~4.3k lines): `WorkflowHost` + full
  impl (start/dispatch/reconcile/sweep/park/wake/cleanup, trigger loop),
  `HostDurableRun`/`HostDurableGraphRun` aliases, `host_handles`, seam
  inventory (`mailbox_async_effect_runner[_with_delivery]`,
  `dispatch_host_sink`, `armored_send_request`, `build_coalesced_job_event`,
  `publish_coalesced_job[_with_cursor]`, `host_acquire_for_assignee`,
  `mention_outcome`, `apply_run_status`, `async_run_journal`,
  `verify_outcome`/`classify_send_error`/`mail_outcome`/`fenced_outcome`/
  `scoped_*`/`sender_for`/`now_ms`), `ParkCtx`/`park_candidates`/`RunState`/
  `PendingDrain`, `find_durable_park`/`model_head`/`await_spawn_launch`/
  `durable_now_ms`, 14 host tests.
- New `toron-workflow::engine::armor` (moved `send_armor.rs` whole, incl.
  its tower-stack tests): hermetic (std+tower only), re-exported path for
  the runner seam.
- Visibility bumps for kept-mcp-test callers (now `pub` + re-exported):
  `mention_outcome`, `now_ms`, `find_durable_park`, `DurablePark`,
  `persist_hosts_cursors`, plus `classify_outcome` (wave1, kept `pub` for
  the same reason).

## Import plan
- toron-workflow gains `tower 0.5` (timeout/limit/util, mirrored),
  `tracing 0.1`, `sqlx 0.8` (same features as toron-core) — no toron-mcp
  edge (`cargo tree -p toron-workflow` shows no `toron-mcp`).
- `toron-mcp/src/workflow_host.rs` keeps: module docs (links fixed to full
  paths), thin re-exports (dispatcher surface incl. `DispatchResult` for
  flywheel, full host surface), `test_spec`, and the kept trigger/mention/
  dispatch test suites (test-only imports moved into `mod tests`).
- `toron-mcp/src/lib.rs`: `pub mod send_armor` removed (no remaining users).
- `WorkflowHost::new(mailbox, workflows)` signature UNCHANGED — server.rs
  untouched; flywheel untouched and compiling.

## Tests run (all green at the commit sha)
- `check --workspace --all-targets`: clean, zero warnings.
- VERIFY greps: `pub struct WorkflowHost` in toron-workflow only (×1);
  absent from mcp; `cargo tree` no-cycle confirmed.
- `test -p toron-workflow host` → 21 passed (14 moved host tests among them).
- `test -p toron-mcp` → full suite green (kept suites exercise the
  re-exports; no test loss — moved tests run in their new home).
- Downstream: `flywheel check --workspace --all-targets` clean (read-only).
- `fmt --all -- --check` clean.

## Commit
sha=4da63cc4bee4444ce31ddc1632b675843c4caca3 (1 commit, bead id in message)

## Residual
- None. Next wave drops flywheel's `toron-mcp` workflow imports (surface is
  already re-exported and compiling; the cutover itself is out of scope).

## PRINCIPLES cited
- fix-root-causes — the durable host belongs in the engine crate (moved the
  host + its armor seam with it, instead of leaving a split-brain host).
- prove-it-works — workspace check + moved host tests + kept mcp suite +
  downstream flywheel check, all at the commit sha; ledger LEGAL to follow.
