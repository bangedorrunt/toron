# Project fence — crews (AUTHORED; `standing-orders regen` preserves this file).

## COMPOUND ORDERS (ADR-0009)

- **Terminal states** in every report: `terminal: done | clean_skip | blocked | failed`.
  clean_skip is a SUCCESS outcome (the gate judged the work N/A); a missing report is
  unknown — never assumed failed.
- **`settled:` decisions** (captain briefing go/no-go, crew shape, model picks) are not
  yours to improve. A real defect inside a settled approach still reports at full severity.
- **Open-mode compound**: after verified non-trivial work, run the learning gate once —
  `flywheel workflow run ce-compound --execute --inputs '{"source":"<what>"}'`. The
  three-part gate (non-obvious ∧ durable ∧ material) decides; most work clean-skips.
- **Independence**: two lenses reasoned in one pane are two perspectives, never
  "independently confirmed" — claim corroboration only across separately-dispatched contexts.
- **Ledger gate**: a close is illegal unless `flywheel ledger check <bead>` prints LEGAL.
  A warning at close is not permission — reopen, record the sha-bound gate row, re-close.
