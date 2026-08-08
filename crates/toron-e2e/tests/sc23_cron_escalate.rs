// governed-by: ADR-0005 (P4.4/P4.7 — repeat-failure escalate, Behavior Test
// Matrix row `cron_escalate`)
#![forbid(unsafe_code)]

//! SC-23 — `cron_escalate` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! When `repeat_failure` reaches `repeat_threshold` consecutive failures,
//! the cron ESCALATES: a kind-14 DM to the captain with
//! `importance=high, ack_required` — the human is pulled in, not silently
//! retried forever.
//!
//! Scenario (impl blocked on P4.4 `bd-p4-bead-on-fail-0uaf`):
//! 1. Register a cron with `policy { on_repeat_failure: escalate,
//!    repeat_threshold: N }` whose workflow FAILS every run.
//! 2. Let the manager run N consecutive failing ticks.
//! 3. Assert a kind-14 DM was published to the captain with the escalate
//!    payload (importance=high, ack_required=true, run/error context).
//! 4. Assert escalation happens at exactly the threshold (N-1 failures do
//!    NOT escalate; the N-th does).
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc23_cron_escalate -- --ignored`

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::*;

#[test]
#[ignore = "blocked on P4.4 repeat-failure escalate (bd-p4-bead-on-fail-0uaf): kind-14 escalate DM not landed"]
fn sc23_cron_escalate_dm_at_threshold() {
    // IMPL(P4.4): register failing cron with repeat_threshold=N; after N
    // consecutive failures assert a kind-14 DM (importance=high,
    // ack_required=true) to the captain carrying the run/error context;
    // assert N-1 failures do NOT escalate (threshold is exact).
    unimplemented!("cron_escalate: wire once bd-p4-bead-on-fail lands")
}
