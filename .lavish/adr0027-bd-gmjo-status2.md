# lane3 bd-gmjo status — 2026-09-01

**Bead:** bd-gmjo Slice 1 S1/S2 + source-scan guards
**Commit:** flywheel 631b14e (tests/adr0027_slice1.rs) — DRAFT RED
**Claim:** CreamFern in_progress (force, blocked by wave-1)
**Reservations:** flywheel/tests/** (flywheel), flywheel/tests/** (toron), crates/toron-cli/tests/** (toron)

## Draft state
- 13 tests in `flywheel/tests/adr0027_slice1.rs`, filtered `cargo test adr0027_s1`
- Result at 631b14e: **11 failed / 2 passed → RED as intended** (wave-1 gates). After bd-2u27/bd-gokp closed, now **9 failed / 4 passed** on re-run (toron-side turning GREEN).

## What is testable now (REAL, not mocks) vs gated

**Testable now — toron-side contracts LANDED (keep GREEN, prove-it-works):**
- `bd-2u27` CLOSED — `toron mail send --corr --wake --priority --audience --result-path` rides `SendMessage.extra_tags` (rumor 14 / job 43001, never 1059 envelope). Vocabulary typed (`standing|tasked`, `queue|interrupt`, `human|agents`), `audience=human` fan-out >1 hard error, `interrupt` requires overseer + never onto human.
  - Tests now: CLI help contains flags, `toron mail send --corr FM-GS-012 --wake tasked ...` succeeds, rumor carries corr/wake/priority/audience/result_path, envelope clean.
- `bd-gokp` CLOSED — receipts sqlite `receipts(corr TEXT PRIMARY KEY, state TEXT NOT NULL, wake TEXT, due_at INTEGER, result_path TEXT, updated_at INTEGER NOT NULL)` next to message_reads/inbox_cursors, `ReceiptStore` upsert/get/list (idempotent), `toron receipt get --corr` / `receipt list [--state]`.
  - Tests now: `cargo test --workspace receipts` + `toron receipt get` roundtrip survives walker death (new process rebuilds Model assignment from table, not journal).

**Still gated — flywheel-side folds PENDING (keep RED-honest, gate wave-1 close):**
- `bd-7fat` CLOSED — `src/workspace.rs` with `WorkspaceModel { doorbell, swarm, assignment, roles, presence, open_human, seq }` + `Msg` + `Cmd` + `tick(&WorkspaceModel,&Msg)->(WorkspaceModel,Vec<Cmd>)` — basic shape now PASS, but E3 assignment semantics still pending.
- `bd-6b1y` IN_PROGRESS + `bd-gs8b` OPEN — Tick Law leftovers: double `bell_model = Default` (7747/7819), `statuses.insert(to)` before HerdrStatus kills drain (8006/8444), `WatchDied` discards model' (8055), `--once` still uses `apply_watch_line` (1240/1267), `watch_sends` after tick (6996), `Diagnose pane_exited` filter (6956-6962), `heal_states` not on `ActiveLoop`, `why --corr/--seq` still grep not fold.
- `bd-dhm1` OPEN — assignment/standing/tasked/audience-human in tick (E2/E3): `BTreeMap<Corr,Receipt>` updates from tagged kind-9, standing empty silent vs tasked empty result+result_path, default queue, interrupt never human.
  - These 9 guards stay RED until `cargo test adr0027_s1` is GREEN; they gate `bd-gmjo` close.

## Next
- Firm up `crates/toron-cli/tests/adr0027_slice1.rs` against REAL flags/store (bd-2u27/bd-gokp) while keeping flywheel guards RED.
- Watch `br list --json` for `bd-6b1y`/`bd-gs8b`/`bd-dhm1` close; do not spin.
- Close protocol when `cargo test adr0027_s1` GREEN: `br gate report bd-gmjo --gate unit-test-verified --provider CreamFern --status pass --to closed; br close bd-gmjo --commit-sha <sha> --reason "..."; mail "[bd-gmjo] done" to captain thread bd-gmjo`.

## Principles
- prove-it-works — scenarios run real binaries, not unit proxies
