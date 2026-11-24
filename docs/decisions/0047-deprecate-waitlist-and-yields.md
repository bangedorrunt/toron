# ADR-0047: Deprecate Reservation Waitlists and Yields (`D3/R6`)

- **Status:** Accepted (`D3/R6`)
- **Date:** 2026-11-24
- **Context:** Under the shared working tree model (`D3/R6`), runtime file-lease negotiation layers (`reservation_waitlist`, `yields`, and flywheel-side lease/waitlist/p2p statecharts and `deadlock`) add ~2.6k LOC of complexity where direct peer mail (`flywheel peer request-release` / `toron mail send`) is simpler and faster.

## Decision

1. Keep the shared working tree (no worktree slots in Stage B) and keep `reservations.rs` and `guard.rs` intact.
2. Deprecate `reservation_waitlist` and `yields` for **one release**, surfacing `"removed: mail the holder"` (`yields::DEPRECATED_MAIL_HOLDER_NOTICE`), before hard-deleting the dead modules.
3. Hard-deletion inventory (~2.6k LOC, queued for after the 1-release deprecation window with operator approval):
   - `toron`: `crates/toron-core/src/mailbox/reservation_waitlist.rs`, `crates/toron-core/src/mailbox/yields.rs`, and waitlist/yield CLI/MCP arms.
   - `flywheel`: `src/deadlock.rs`, `src/cmd/deadlock.rs`, `src/elm/deadlock_watch_statechart.rs`, `src/elm/p2p_negotiation.rs`, `src/elm/pre_edit_lease_statechart.rs`, and their mutation specs.
