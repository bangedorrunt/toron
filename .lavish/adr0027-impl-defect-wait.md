# Impl defect — `toron wait` missing dispatch

**Bead:** likely bd-rkom / wait --corr (E7/E9) — `crates/toron-cli/src/cli.rs:147` adds `Command::Wait(WaitArgs)` but `crates/toron-cli/src/main.rs:60` match is non-exhaustive.

**Failing scenario:**
```
cargo test -p toron-cli adr0027_s2 -- --nocapture
```
or
```
cargo test --workspace --no-fail-fast
```
Both abort with:
```
error[E0004]: non-exhaustive patterns: `&cli::Command::Wait(_)` not covered
  --> crates/toron-cli/src/main.rs:60:22
   let code = match command {
                     ^^^^^^^ pattern `&cli::Command::Wait(_)` not covered
```

**Evidence:**
- `cargo fmt --check` now passes (2f30c2d), `catalog_freshness` passes, but `cargo test -p toron-cli` fails to compile the `toron` bin.
- `cargo test --workspace --no-fail-fast` therefore never reaches most bins (aborts at toron-cli bin).
- `toron wait --help` would also be unavailable until the dispatch is wired.

**Expected:** `main.rs` should handle `Command::Wait` (likely `Command::Wait(args) => wait::run(args)` or similar) and route to the `wait --corr` implementation (subscribe-until-receipt, same live watch as doorbell per E7/E9). No new MCP tool, just CLI verb.

**Impact:** Blocks `bd-twcq` S3 RED-draft verification (toron-cli tests need the `toron` bin to compile) and the honest `cargo test --workspace --no-fail-fast` gate.

**Not patching around** — posting as impl defect per wave-gate protocol. Test defect (fmt) already fixed in 2f30c2d; this is impl.

**Principles:** type-system-discipline — exhaustive match must cover new verb.

**Reporter:** CreamFern lane3
