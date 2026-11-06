# wave3-parity report — bd-wave3-parity-declaim-0hax

status: done
terminal: done

## Claims changed (parity = provenance, catalog = live spec)
- `crates/toron-cli/src/cli.rs`: root about no longer claims the "full
  38-tool AM-Rust parity surface"; names `toron catalog --json` (38 parity +
  3 additive). `catalog` subcommand help: frozen 38 parity tools + live-3
  note. `bench` help: dropped "AM-Rust parity", describes live latency
  percentiles.
- `crates/toron-cli/src/main.rs`: bench doc keeps purpose, drops parity
  label; catalog doc names frozen parity catalog + live +3.
- `crates/toron-cli/src/catalog.rs`: freshness message says "parity catalog"
  (was "38-tool catalog"; message-only, gate-required since the VERIFY greps
  `toron-cli/src` for zero `38-tool`).
- `README.md`: Phase-1 header reframed as lineage + live-spec pointer;
  toron-mcp tree/roadmap counts now read "38 parity (+3 additive)".
- `docs/parity/am-rust-tool-catalog.md`: retitled Historical Record + banner
  (live spec pointer, links to sibling + flywheel known-divergences).
- `docs/parity/known-divergences.md`: "frozen 38-tool contract" → "frozen
  historical 38 tool parity record".
- `conformance.rs`: 38-count pins the frozen fixture (kept, real); messages
  now say frozen parity subset, not canonical surface.
- `am_parity_e2e.rs`: one doc parenthetical clarified to "38 parity tools".
  `acceptance.rs` already 41-aware — untouched.

## Bench decision
KEPT with purpose: `toron bench` measures live mailbox latency (20
iterations, percentile report over startup/inbox/send/search/threads) — a
real benchmark, not a parity relic. Help fixed to say so.

## Test / catalog notes
- Generator: `toron catalog --json --output docs/catalog/toron-mcp.json`
  (freshness gate rebuilds with committed `generated_at`; byte-identical).
- Regen WAS required: my two help-text edits drifted `cli_commands`
  (tools/resources identical). Regenerated with the freshly built binary;
  `catalog_freshness` green after.
- catalog_shapes (41-tool gate), acceptance count tests (41 = 38+3),
  conformance golden-38 all green. No tool/wire change.

## Commit
sha=fcafd1137e0aeb9673cf2108a2d45c4498b78f4b (1 commit, bead id in message)

## Residual
- None.

## PRINCIPLES cited
- prove-it-works — the live catalog is the contract, not a frozen count
  (help/README/parity all point at `catalog --json`; freshness + shapes +
  acceptance gates green at the commit sha).
- laziness-protocol — de-claim the record; do not delete history
  (frozen catalog, fixtures, and parity tests kept; only the contract
  framing changed).
