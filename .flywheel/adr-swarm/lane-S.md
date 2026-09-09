# Lane S — supervision / reclaim (flywheel repo)

You are Lane S of the ADR-0024/0025/0026 implementation swarm. Captain: TealDune.
Your repo: `/Users/bangedorrunt/workspace/flywheel`. Baseline HEAD: 186a13e (checkpoint), `cargo check` green.

## Specs (read first)
- `/Users/bangedorrunt/workspace/toron/docs/decisions/0025-adopt-graph-dispatch-artifact-reduce-pin-supervision.md` — Spec 1 (lease reclaim), §8 Wave 1, §9 Wave 1 verification checkboxes.
- The bead: `bd-adr25-w1-lease-reclaim-ozqn` (UNGATED — ships now; "cargo test reclaim").

## Scope (ADR-0025 Wave 1)
1. `src/durable.rs`: new `LoopAction::Reclaim { bead_id, pin, herdr_name, reason: "pane_exited" }`. On herdr `pane.exited` for an `in_progress` assignee: Diagnose immediately, Respawn next tick (budgeted) — **Reclaim fires ONLY after the respawn budget is exhausted or herdr no longer lists the pane** (Spec 1 step 0; reclaiming under a live heal races the respawn). Both orderings tested: exit→respawn-attempt AND respawn-attempt→exit.
2. `plan_reclaim(now, beads, panes, reservations) -> Vec<LoopAction>` — pure, standalone (Wave 3 will fold it into `tick`; do NOT build LoopModel now). Stale reclaim thresholds: swarm assignee + `updated_at` > 7200s + no active reservation + no `working` status → Reclaim; human/unclear assignee → 86400s. Live reservation, recent comment, or dirty files named in last comment → NOT abandoned.
3. Execution leaf beside Diagnose/Respawn: (a) release the dead pin's reservation by reason (bead id) — `release_file_reservations_by_reason`; (b) `br comments add` audit line ("reclaim: previous in_progress claim appears abandoned; evidence: ..."); (c) `br update <id> --status open` (clear assignee). NEVER `br close`. NEVER forge VERIFY. NEVER reclaim while a respawn attempt is live. NEVER reclaim when a DIFFERENT pin holds the reservation (HARD).
4. Config constants `FLYWHEEL_RECLAIM_SWARM_SECS` (7200) / `FLYWHEEL_RECLAIM_HUMAN_SECS` (86400), overridable.
5. Tests (§9 Wave 1): pane.exited emits Reclaim (unit, no herdr); suppression while respawn budget remains + both orderings; leaf comments then sets open (fixture bead reappears in mocked ready set); different-pin reservation → skip; 7201s→Reclaim / 7199s→no; human 86400s not 7200s; `rg "br close" flywheel/src/durable.rs` has no match in the Reclaim leaf. One live smoke: dead-holder release via toron.
6. Migration: none — in-flight dead claims reclaim on next tick.

## Hard constraints
- `// governed-by: ADR-0025` on touched enforcing files.
- **Do NOT touch `src/main.rs`** (Lane R owns it in Wave 0). Your surface: `src/durable.rs` + tests. If you find you need main.rs, mail the captain instead.
- No new CLI nouns/kinds/MCP tools. Reclaim never blocks on a human (pane death is observable — `never-block-on-the-human`).
- TDD; `cargo clippy --all-targets -- -D warnings` + `cargo fmt --check` clean; bead VERIFY `cargo test reclaim` passes.
- Build slot: acquire `cargo` slot before test runs, release after.
- Reserve before edit: `flywheel mail locks reserve --project flywheel --reason bd-adr25-w1-lease-reclaim-ozqn src/durable.rs` (+ renew).

## Process
- Claim: `br update bd-adr25-w1-lease-reclaim-ozqn --status in_progress --assignee <your herdr name>` (in /Users/bangedorrunt/workspace/flywheel).
- Mail captain on start / done (`[<bead-id>] done` + sha + test tail) / blocked >15min (then STOP).
- Commits: conventional + bead id, explicit paths only, `.beads/` included after `br sync --flush-only`.
- Queue after this bead (gated — captain sends go): `bd-zbhq` (ADR-0024 W1b: flywheel VerifySink impl reusing `__loop-run-verify` under the cargo mail slot, workflow receipt only, + `repair-until-verified` recipe; needs Lane E's engine sink first), then `bd-kvfo` (W2b: drain ApprovalSink), then `bd-adr25-w2` (artifacts + stitcher). Await captain's go per bead.
