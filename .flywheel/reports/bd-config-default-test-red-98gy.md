# bd-config-default-test-red-98gy — stale backend-default expectation

status: done
commit: d36e07fdc370c66f6048e95ad4326ce47816d862
verdict: worker-receipt
terminal: done

## Fix

`config_show_emits_effective_defaults` asserted the pre-rex default `"hc"`;
the shipped default since 0da17ac is `"auto"` (rex primary, hc fallback,
ADR-0045). One-line expectation change + ADR comment. Product untouched —
test-expectation bug, not product bug, per brief.

## Runs

- VERIFY at HEAD before: FAILED (left `"auto"`, right `"hc"`) — reproduces.
- After: `test result: ok. 1 passed` — green.
- No other files touched; no flywheel edits.

## Deviations

None.

PRINCIPLES: prove-it-works — red-before/green-after on the exact VERIFY
command; fix-root-causes — expectation follows the ADR, default unchanged;
sequence-verifiable-units — VERIFY, receipt, close in order.
