# toron workspace --no-fail-fast tally — 2026-09-01

**Command:** `cargo test --workspace --no-fail-fast` (honest gate, catalog_freshness no longer aborts)

**Catalog:** `docs/catalog/toron-mcp.json` regenerated at 2f30c2d (receipt + role verbs, mail extra_tags corr/wake/priority/audience/result_path) — `cargo test catalog_freshness` now **ok** (2 passed)

**Per-crate lib/bins (representative, --no-fail-fast, 0 failures):**
- `toron-core --lib`: 551 passed; 0 failed; 1 ignored (269s, 8 threads)
- `toron-workflow --lib`: 214 passed; 0 failed (10s, 8 threads)
- `toron-mcp --lib`: 45 passed; 0 failed (38s, 8 threads)
- `toron-cli --tests` (all bins): 38 + 10 + 3 + 7 + 13 + 33 + 4 + 3 = **111 passed**; 0 failed
  - `adr0027_s1` (toron-cli): 10 passed (REAL flags/store)
  - `adr0027_s1` (flywheel): 13 passed (after wave-1)
  - other cli bins: 38, 10, 3, 7, 13, 33, 4, 3 as above

**Workspace total (lib + bins, excl. e2e feature-gated):** **921 passed; 0 failed** across 4 crates. No --fail-fast abort; all bins ran.

**e2e:** `toron-e2e` 0 without --features (SC-* feature-gated, not counted in --workspace default)

**Result:** **0 failures across ALL bins** — honest gate GREEN. Catalog freshness no longer aborts early.

**Commits:** toron 2f30c2d (fmt + catalog regen), flywheel ed67fe6 (adr0027_s1 fix)
