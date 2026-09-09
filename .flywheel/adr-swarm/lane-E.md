# Lane E — engine (toron repo)
> CAPTAIN NOTE 2026-08-29: You are StormyDesert (opencode, w8E:p1). Your pin is registered on BOTH projects
> (flywheel + toron). The jcode crew was disbanded; you replace it. Everything else in this brief stands.

You are Lane E of the ADR-0024/0025/0026 implementation swarm. Captain: TealDune.
Your repo: `/Users/bangedorrunt/workspace/toron` (crate `toron-workflow`; later one CLI noun in `toron-cli`).

## Specs (read first)
- `/Users/bangedorrunt/workspace/toron/docs/decisions/0024-graph-first-repair-loops-bead-workflow.md` — §6 Spec 0/1/2, §6 Spec 6 file table, §8 Wave 1 acceptance.
- The bead: `bd-adr24-w1-graph-canonical-f0z9` (p1, "THE GATE" — ADR-0025 Waves 2–5 are gated on it).

## Scope (Ship order from ADR §6 — do not skip steps)
1. `Workflow::to_graph()` compile-correct (copies only fields `GraphNode` has today; add `GraphNode.build_slot` in the same cut only if playbook lint needs it).
2. `run_workflow` always calls `run_graph`. Same-cut park fix: route `RequestApproval`/`Delay`/`Spawn` through the park path (`run_step`) — without this, desugared SIGNOFF silently records `{action:"request_approval"}` (WF-08 regression). Delete the linear path only after the park gate test is green.
3. Same-cut `fail_run` predicate on BOTH paths: `ok == false` → apply on_error. Presence of an `error` key without `ok:false` is NOT a failure.
4. Failure object fields documented on the Value shape: `{ok:false, error, kind?, retryable?, resumable?, disposition?, retry_after_ms?}`. Kinds: timeout|rate_limit|auth|blocked|cancelled|unknown (billing/provider come later via ADR-0026 — do not add them here). `kind` optional; default on_error stays Continue. Kind-aware fail_run: retryable kinds (rate_limit/timeout) do NOT abort `fail_run` — retry same node with backoff (honor retry_after_ms, else 1s×2^(attempt-1) cap 60s; bound = enclosing Retry.max_attempts else 3; then escalate).
5. `DispatchSpec.context: Option<ContextMode>` (inherit default, fresh = new session). Add the field AND honor it in the same cut: honoring lives in `/Users/bangedorrunt/workspace/flywheel/src/pane.rs` (~:755–771 respawn split → map `fresh` onto a new spawn pane). You have a scoped exception to edit ONLY `flywheel/src/pane.rs` for this — no other flywheel files (main.rs is Lane R's; durable.rs is Lane S's).
6. `Action::Verify { command }` (serde tag `verify`) + `VerifySink` newtype in `engine/sinks.rs` (unique TypeId, same pattern as ApprovalSink); register in `engine/mod.rs`. Missing sink → `{ok:false, error:"verify sink missing"}`. NEVER `std::process::Command` in toron-workflow. NEVER call `br` from the engine. Never write `command-verified`.
7. Lint (in `lint.rs`, NOT only playbook_lint.rs): R-E1 retry-on-blocked/auth is an error; R-E2 missing kind = warning; R-E3 spawn+fail_run without enclosing Retry = warning; desugar-or-reject `on_error: repair` (reject — the recipe is the authoring path); unnamed steps desugar as `step_N` (document).
8. Recipe support: engine must run the unrolled `repair-N`/`evaluate-N`/`verify-N` named-node subgraph exactly as plain graph nodes (no engine magic, no `StepOnError::Repair` variant). Ledger JSON + evaluator schema live flywheel-side (Lane S, later bead) — engine only needs to execute the nodes.
9. Tests: §8 Wave 1 checklist is the contract. Name tests so `cargo test -p toron-workflow graph_desugar` and `cargo test -p toron-workflow park` pass (bead VERIFY).

## Hard constraints
- `// governed-by: ADR-0024` at top of every touched toron-workflow file (verify-adr-links.sh).
- No new MCP tools, no new Nostr kinds, no new toron CLI nouns (bd-auqj later is ADR-sanctioned), no worktrees, no engine `br`/shell.
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check` clean on touched crates. `#[tool(name` count stays 40.
- TDD: failing test first per unit.
- Build slot: acquire the `cargo` slot before running cargo tests (`flywheel mail slot acquire --slot cargo` or ask captain), release after.
- Reserve before edit: `flywheel mail locks reserve --project flywheel --reason bd-adr24-w1-graph-canonical-f0z9 src/pane.rs` for the pane.rs exception; toron repo paths `crates/toron-workflow/**` via `--project toron`.

## Process
- Claim: `br update bd-adr24-w1-graph-canonical-f0z9 --status in_progress --assignee <your herdr name>` (run in /Users/bangedorrunt/workspace/toron).
- Mail captain on start, on completion (`[<bead-id>] done` with commit sha + test tail), and immediately when blocked >15min (then STOP — do not thrash).
- Commit: conventional message including the bead id, explicit `git add` paths only, never `-A`; include `.beads/` after `br sync --flush-only`.
- After this bead closes (captain verifies), your queue: `bd-47fw` (ADR-0024 W2a evidence-bearing approvals, Spec 3), then `bd-auqj` (ADR-0025 Spec 4 toron half: `toron contact captain --clear <project>` + toron Skill Sync Contract). Await captain's go per bead.
