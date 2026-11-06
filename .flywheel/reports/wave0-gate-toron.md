# wave0-gate report — bd-wave0-toron-gate-4jf0

status: done
terminal: done

## What
Nightly-rustfmt drift across toron, formatted with the repo toolchain
(`mbx +nightly-aarch64-apple-darwin fmt --all`). 21 `.rs` files, all
rustfmt normalizations (block collapse, match-arm dedent, long-line reflow).
No logic, no renames, no suppressions.

## Files count
21 `.rs` files: toron-cli (1), toron-core (6), toron-mcp (7), toron-workflow
(6 + 1 test). `.beads/issues.jsonl` left untouched per FORBIDDEN.

## What ran
- `mbx +nightly-aarch64-apple-darwin fmt --all -- --check` → exit 0.
- `mbx +nightly-aarch64-apple-darwin check --workspace --all-targets` → clean.
- `mbx ... test -p toron-core --lib -- mailbox::reservations` → 62 passed.
- `mbx ... test -p toron-cli --bins` → 143 passed (no lib target; closest filter).

## Commit
sha=d5172e6a7bf0b167a8e10e96215624d783554286 (single commit, bead id in message)

## Deviations
- Brief's "`git diff -w` is empty" sanity check is imprecise: rustfmt block
  collapsing (`_ => {\n}` → `_ => {}`, match-arm dedent) always shows under
  `-w`. Verified instead that every hunk is brace/line reflow with no
  identifier, literal, or type change, and that the tree compiles + spot
  suites pass. Formatting-only stands.
- toron-cli spot filter is `--bins` (package has no lib target).

## PRINCIPLES cited
- prove-it-works — gate green at the commit sha plus compile + spot-suite proof,
  not reformatting alone.
