// governed-by: ADR-0005 (P4.3/P4.7 — scheduler durability, Behavior Test
// Matrix row `cron_catchup`)
#![forbid(unsafe_code)]

//! SC-24 — `cron_catchup` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! **Scheduler durability**: if flywheel is DOWN at a cron's due time, on
//! wake the loop runs the MOST-RECENT due tick (`latest` policy) — never a
//! burst of every missed tick, and never a silent skip.
//!
//! Scenario (impl blocked on P4.3 cron-mgr catch-up wiring):
//! 1. Register a cron (`* * * * *`). Start the manager, let one tick fire.
//! 2. KILL the worker (drop the manager — the `resonate serve` child stays
//!    up; durable state survives). Simulate "downtime" past 2+ due ticks
//!    (e.g. restart the loop after a >2-min gap, or drive the durable loop
//!    input's `last_fired` back in time).
//! 3. Restart the manager on the SAME server.
//! 4. Assert the loop fires the LATEST due tick at-or-before now — exactly
//!    ONE run — and the earlier missed ticks are skipped (no burst).
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc24_cron_catchup -- --ignored`

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::*;

#[test]
#[ignore = "blocked on P4.3 cron-mgr catch-up wiring (bd-p4-cron-mgr-ur33): latest-policy wake not observable via the manager yet"]
fn sc24_cron_catchup_runs_latest_due_tick_only() {
    // IMPL(P4.3): with the manager down across 2+ due ticks, on wake assert
    // exactly ONE run fires (the latest due tick), no burst of missed ticks.
    // Timing via `next_due`/`next_tick` fixture helpers.
    unimplemented!("cron_catchup: wire once bd-p4-cron-mgr catch-up lands")
}
