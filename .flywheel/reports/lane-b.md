# lane-b report — bd-lane-b-toron-correctness-10jz

status: done with CEILING residuals (scopes 1 + 4 shipped; scopes 2 + 3 CEILING, code left untouched)
terminal: done

## Files changed
- `crates/toron-core/src/mailbox/reservations.rs`
  - New `revocation_target()` helper: blank/missing/unparseable lease id is a
    typed `anyhow` error, never a fabricated id (fix-root-causes: one shared
    helper, not three wrapped copies).
  - Three revocation sites (`force_release_file_reservation`,
    `reap_ghost_leases`, `release_by_reason`) use it; literal-tag
    `Tag::parse(...).expect(...)` replaced with infallible constructors
    (`Tag::identifier`, `Tag::custom`, `Tag::event`). Wire format unchanged
    (`["d",..]`, `["status","revoked"]`, `["e",..]`, `["a",..]`, `["note",..]`
    serialize identically).
  - New regression test `revocation_target_rejects_missing_or_bad_lease_id`.
- `crates/toron-core/src/mailbox/matrix.rs`
  - `run_proof_matrix` is test-only (sole caller: in-module conformance test):
    now `#[cfg(test)]` private fn; its four imports gated `#[cfg(test)]`.
    `coverage_cases()` stays `pub` (import-free grep pointer, still used).
- `.flywheel/reports/lane-b.md` (this file).

## What I ran (targeted VERIFY per bead rev 4; full suite deferred to captain)
- `mbx +nightly-aarch64-apple-darwin clippy --workspace --all-targets --all-features -- -D warnings` → clean.
- `mbx +nightly-aarch64-apple-darwin test -p toron-core --lib -- mailbox::reservations::tests::revocation_target_rejects_missing_or_bad_lease_id` → 1 passed.
- `mbx +nightly-aarch64-apple-darwin test -p toron-core --lib -- mailbox::matrix` → 2 passed.
- Extra (not required, for the record): full `mailbox::reservations` suite 62/62
  pass; `toron-workflow` + `toron-mcp` suites all pass; workspace clippy clean.

## Test summary
New test proves the old code would fail it (old code never returned `Err` —
`unwrap_or_else` always minted the fallback — so `expect_err` could not pass).
All 62 reservations tests green, so revocation wire behavior is unchanged.

## Deviations
- Scope-4 shape differs from the brief's first guess (`pub(crate)` → `#[cfg(test)]`
  private): `pub(crate)` trips `-D dead-code` on the lib target because the only
  caller is a test. The cfg-gated private fn is the honest visibility.
- Did not unify the three revocation tag-blocks into one helper (simplify pass
  considered and declined): note-tag semantics differ per site; merging risks
  wire drift for zero gain.
- Full `test --workspace` NOT run per captain amendment (timebox); captain runs
  it at final verification.

## Baselines / pre-existing red (proven at HEAD, not mine — do not chase)
- `fmt --all -- --check` is RED at HEAD (~25 files, rustfmt-version drift, incl.
  one long line in reservations.rs outside my hunks). Captain-confirmed baseline.
  My added lines are fmt-clean.
- `toron-cli cli_framework::config_show_emits_effective_defaults` fails at HEAD
  (expects backend default `hc`, binary reports `auto`) — proven via `git stash`
  of my files. Another lane's change.
- One `toron-core` lib test failed once across repeated full-crate runs, then
  passed on re-run with no code change (flaky; name not captured before the
  amendment halted full-suite runs). Flagged for the captain's final verification.

## Residual inventory (CEILING — needs no new machinery, next pass finishes as-is)
- `crates/toron-core/src/mailbox/guard.rs` GitFacts writer — LEFT IN PLACE.
  Live consumers: `toron-cli` `GuardCommand::Record` (`robot.rs run_guard_record`,
  the post-commit hook path, default journal via `git_fact_journal_path`) and the
  sandbox test in `config/mod.rs`. Enabler: none (policy decision, not code —
  deleting needs a captain call on the `guard record` CLI contract).
- `crates/toron-core/src/bus/wake.rs` + `crates/toron-cli/src/wake_sock.rs` —
  LEFT IN PLACE. Live callers: `toron-mcp server.rs start_wake_sidecar`,
  `plane.rs` watch paths (1581/1704/1804), `WAKE_KINDS` use, e2e
  `sc32_watch_once`, plus `wake_fd_budget`/`bus_wake_stats`/`comms_*` tests.
  Enabler: none (an explicit decommission decision with a migration for
  `watch --live`/MCP sidecar, not a silent delete).
- Full-suite green + `watch --once` sc32 re-proof — enabler: machinery (captain's
  final verification run).

recommendation: close with amendment + residuals as filed beads

## PRINCIPLES cited
- prove-it-works — close rests on clippy + targeted tests at the commit sha,
  plus stash-proven baselines; rg alone was never the proof.
- fix-root-causes — fabricated id deleted via one shared typed-error helper,
  not wrapped or special-cased per site.
- sequence-verifiable-units — each edit ended in a check (clippy → targeted
  tests → 62-test reservations suite) before the next began; tree never left red.
