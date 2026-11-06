# bd-wave9-doctor-freshness-exmk — doctor judges the live daemon

status: done
commit: 236422fa42ad08dda17687c41910afc9af9d48f5
verdict: worker-receipt
terminal: done

## Daemon metadata source

Boot record `serve.build.json` (pid, canonical binary path, built_at, ino/size)
+ serving pid via `lsof` on the daemon address + on-disk artifact re-stat +
source time (HEAD `%ct` in a checkout, else bounded newest-mtime walk under a
`Cargo.toml` root skipping target/.git/node_modules, cap 20k). No new
dependency, no probe beyond the existing local endpoint, nothing revived.

## Comparison rule

Pure `serve_build::daemon_freshness_verdict` (no clock/FS/env): pid mismatch
→ NotRunning (pass, existing behavior); disk artifact ≠ boot artifact →
Stale; source strictly newer than the running build → Stale; else Fresh.
Strict `>` so same-second rebuilds pass rather than flap. The pre-existing
`freshness` leg is relabeled `local_binary` (target on disk); the live leg is
`daemon_freshness` — "target current, daemon stale" is now visible instead of
all-green.

## Remedy

One line: reinstall toron from this source, then `toron daemon restart`.
`--check` exits 3 on it (verified live).

## Tests

- 4 unit tests on the pure verdict (stale/current/equal/replaced/not-running).
- `doctor_freshness.rs`: no-boot-record keeps the not-running pass; the two
  legs report different subjects. Live-port faking would need the daemon
  address, so the stale path is proven by unit + a live read-only run.
- Live proof (this checkout's real daemon, read-only): `daemon_build`
  ok=true (the old trap) while `daemon_freshness` ok=false names the 11-03
  build vs 11-06 source with the remedy; `doctor --check` exits 3.
- Regression: config_strict 5/5, cli_framework 7/7, serve_build 11/11,
  workspace check clean, fmt + gates green.

## Deviations

- `config_strict.rs` updated for the `freshness` → `local_binary` rename
  (my own test from wave7-strict).
- Pre-existing flake left untouched: core
  `test_default_lease_ttl...` fails under parallel load (180±5s window vs
  KDF contention, saw 186s), passes solo; file owned by another lane, zero
  intersection with this diff.

## Residual

None from this bead.

PRINCIPLES: prove-it-works — stale fails with the remedy on live data, exit
3 observed, not asserted; fix-root-causes — judge the running binary via the
boot record, never the local target; sequence-verifiable-units — check, both
test legs, live proof, then report.
