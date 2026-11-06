# wave1-dispatcher report — bd-wave1-dispatcher-move-9hpl

status: done
terminal: done

## Moved items (toron-mcp → toron-workflow `engine::dispatch`, new file
`crates/toron-workflow/src/engine/dispatch.rs`, `pub mod dispatch` in
`engine/mod.rs`)
- `DispatchRequest`, `DispatchResult`, `EffectDispatcher` (+ private
  `ClaimedAttempt`/`ClaimOutcome` machinery), `DISPATCH_LEASE_US`,
  `durable_route`, and the dispatch algorithm (`dispatch`, `dispatch_async`,
  `prepare_and_claim`, `settle_and_record`, `run_cleanup`, `unknown_report`,
  `result_from_row`).
- `classify_outcome` moved too and kept `pub`: the kept mcp-side behavior
  tests call it directly, and a cross-crate `pub(crate)` is impossible.
  It rides the same re-export (6th item; documented deviation, not scope
  creep — the alternative was moving test fns out of the mcp suite).

## Import plan
- `dispatch.rs` imports: `super::{CleanupKind, CleanupRequest, DispatchPlan,
  FORCED_CLEANUP_CAPABILITY, ProviderCapability, ProviderOutcome,
  UnknownReport, capability_for_action_type, cleanup_effect_key,
  corr_for_effect, describe_unknown, effect_view, plan_attempt}`,
  `super::durable::unsupported_reason`,
  `super::spawn::spawn_launcher_refusal`, `crate::schema::Workflow`,
  `toron-core workflow_effects::{ClaimAttempt, EffectState, NewAttempt,
  NewEffect, WorkflowEffect, WorkflowEffectStore, now_us}`.
- `toron-workflow` gains no new dependency edge (`cargo tree -p
  toron-workflow` shows no `toron-mcp`); `tracing`/`Mailbox`/`SendArmor` were
  never referenced by the moved code.
- `workflow_host.rs` keeps a trimmed `workflow_effects` import for its
  remaining code, drops the moved names from its `engine::{...}` import,
  and re-exports the surface:
  `pub use toron_workflow::engine::dispatch::{DISPATCH_LEASE_US,
  DispatchRequest, DispatchResult, EffectDispatcher, classify_outcome,
  durable_route};`
- Test-only names the kept mcp tests still need (`DispatchPlan`,
  `ProviderCapability`, `effect_view`, `plan_attempt`) moved into the mcp
  `mod tests` import; `capability_for_action_type` stays top-level (lib code
  uses it at the call site).

## Tests run (all green)
- Bead VERIFY: `check --workspace --all-targets` clean, zero warnings;
  `rg` confirms the five items defined in toron-workflow and not in
  `workflow_host.rs`; `test -p toron-workflow dispatch` → 22 passed;
  `cargo tree` no-cycle confirmed.
- Kept-suite proof (no test loss): full `test -p toron-mcp` green
  (142 + 5 + 9 + 2 + 2 + 1 + 1 + 3 + 1 passed across targets — includes the
  dispatcher reconciliation/cleanup/classify tests via re-export).
- Downstream proof: `flywheel check --workspace --all-targets` clean
  (read-only; flywheel untouched — next wave drops its toron-mcp dep).
- `fmt --all -- --check` clean (formatted touched files only; tree was green).

## Commit
sha=ea4ecfb9d6431c4e59efaeeab13c5bcb663cd2d5 (3 paths, bead id in message)

## Residual
- None from this lane. Next wave (not this bead): drop flywheel's
  `toron-mcp` dependency now that the surface lives in toron-workflow.

## PRINCIPLES cited
- fix-root-causes — the shared effect surface belongs in the engine crate,
  not the MCP server; moved the code instead of wrapping it.
- prove-it-works — check + moved dispatcher tests + kept mcp suite +
  downstream flywheel check, all at the commit sha; ledger LEGAL to follow.
