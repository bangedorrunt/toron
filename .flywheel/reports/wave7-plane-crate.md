# CEILING REPORT — bd-wave7-plane-crate-tx3w

status: CEILING
done:
  - toron split complete (receipt toron/.flywheel/receipts/bd-wave7-plane-crate-tx3w-9342e7a52276.verify.txt, commit 9342e7a522765137852cff1666b23f93a0eb0e29)
  - flywheel allowed-files retarget complete (commit 4b054cafe49c6f7b76d0272a3bff3c2e5e187778)

## Toron legs (all green, evidenced)

- `check --workspace --all-targets`: clean, zero warnings shown.
- `cargo tree -p toron-core`: no `toron-plane` (no core→plane edge).
- `test -p toron-plane`: green incl. moved backend suites (77 passed wave6
  baseline carried over) + new live-port compile test.
- `test -p toron-core --lib`: 914 passed (regression sweep for the
  default-None injection change); toron-mcp 120, toron-cli bins 145,
  toron-workflow 271, whois integration 1 — all green.
- `verify-gates.sh`: pass (new crate root forbids unsafe, ADR markers resolve).
- `rustfmt --edition 2024`: touched files only, clean.
- Crate layout: `toron-plane/{backend/*,osc7501.rs,lib.rs}` owns adapters +
  fold + `LivePanePlane`; core keeps `pane_types.rs` (vocabulary + error
  codes) + `plane_port.rs` (trait + mock); `toron_plane::backend::*` mirrors
  old paths so retargets are prefix swaps.

## Flywheel legs (blocked, with proof)

- Allowed files (comms_send, pane_write, hc, inbox_ports, backend/*, mail,
  coalition + Cargo.toml/lock): retargeted, committed; zero errors
  attributable to them (proven by scoped error inventory).
- `! rg toron_core::backend flywheel/src`: 30 hits remain, ALL in forbidden
  files (main.rs 5, cmd/captain.rs 1, cmd/internal.rs 2, cmd/send.rs 3,
  cmd/spawn.rs 19). Removing `toron_core::backend` breaks them; a shim is
  explicitly forbidden. Retargeting them is another lane's file ownership.
- `check -p flywheel`: red on the same forbidden files (their WIP churn:
  dup imports, unresolved eviction refs) — my files contribute zero errors.

## Residual inventory (one row per residual)

- flywheel/src/main.rs — 5 `toron_core::backend::` rows — retarget to
  `toron_plane::backend::` — enabler: none (mechanical prefix swap; needs a
  lane with main.rs clearance)
- flywheel/src/cmd/spawn.rs — 19 rows — same retarget — enabler: none (same)
- flywheel/src/cmd/send.rs — 3 rows — same retarget — enabler: none (same)
- flywheel/src/cmd/internal.rs — 2 rows — same retarget — enabler: none (same)
- flywheel/src/cmd/captain.rs — 1 row — same retarget — enabler: none (same)
- bd-wave7-plane-crate-tx3w — green-tree re-run of the four flywheel VERIFY
  legs + mechanical close — enabler: none (same commands as-is once the
  forbidden files compile)

recommendation: close with amendment + residual
