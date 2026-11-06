# wave14-withlease-nongit report — bd-wave14-withlease-nongit-2s0o

status: done
terminal: done

## Rule (filesystem is the source of truth, git only an optimization)
`repo_relative_files_at` no longer returns empty outside a git worktree —
it runs the same bounded walk everywhere. The old gate skipped the walk
for non-repo roots (the 9 GB workspace park); the budget (200ms) + cap
(100k entries) already bound it, so the gate only hid the file list.
- `WALK_SKIP_DIRS`: `.git`, `target`, `node_modules`, `.venv` (+ `venv`,
  `__pycache__`, `dist`, `build`, `.tox`, `.next`, `.nuxt`,
  `.mypy_cache`, `.pytest_cache`, `.ruff_cache`, `.hypothesis`,
  `.parcel-cache`, `.svelte-kit`, `.idea`, `.vscode`). Hidden dirs NOT on
  the list (`.github`, `.config`) are still walked — no false refusals.
- Never follows symlinks (unchanged); output sorted (deterministic);
  budget/cap hits eprintln the root + limit (never silent truncation).
- Deleted the now-unused `inside_git_worktree` gate (+ its tests).

## Clean-export repro (proven both directions)
- Pre-fix: the new non-git integration test FAILS (stash-checked) —
  temp dir without `.git` lists nothing, the zero-match check is skipped,
  the command runs, exit 0. Exactly the reported clean-export shape.
- Post-fix: same zero-match glob refuses with exit 3 and the child never
  runs; a matching glob in the same dir still acquires and runs (the
  list is real, not a vacuous always-refuse).

## Tests
- Unit (`reserve_warn_scan_tests`, 3 pass): non-repo root lists files;
  sorted/deterministic/hidden-cache skips; bounded walk + symlink loop
  (pre-existing rows kept green).
- Integration (`cli_robot`, new `reserve_with_lease_zero_match_refuses_without_git`):
  temp non-git cwd + temp store, zero-match → exit 3 + no run + no lease;
  matching glob → exit 0 + ran.
- Full VERIFY green: workspace check clean; daemon_discovery 6 pass;
  `cli_robot` full suite 49 pass (incl. the exact lease test);
  operator `reserve list` (no env) exit 0.
- Pre-existing, untouched: `robot.rs:56` clippy lint (empty doc line;
  VERIFY does not run clippy).

## Commit
sha=e6ad5f5282d6fa41e3cd83114d51c5e83e6f517a (1 commit, bead id in message)

## Residual
- None.

## PRINCIPLES cited
- fix-root-causes — reproduced the empty-list skip first (stash-proven),
  then removed the gate instead of special-casing export layouts.
- prove-it-works — the exact failing shape as a regression test plus the
  full VERIFY at the commit sha; gate+ledger close below.
