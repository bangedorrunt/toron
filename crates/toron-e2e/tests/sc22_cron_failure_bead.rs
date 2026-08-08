// governed-by: ADR-0005 (P4.4/P4.7 — bead-on-failure, Behavior Test Matrix row
// `cron_failure_bead`)
#![forbid(unsafe_code)]

//! SC-22 — `cron_failure_bead` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! A cron run that FAILS (kind-46012 `status=failed`, P0) must produce a
//! **triageable bead** via `br create` with the full failure bundle (run-id,
//! workflow, owner, error, last-known-good, retry history) — the defined-
//! fixer loop (P4.4) closes the failure → bead → triage path.
//!
//! Scenario (impl blocked on P4.4 `bd-p4-bead-on-fail-0uaf`):
//! 1. Start a cron whose workflow FAILS (a step with `on_error: fail_run` +
//!    a runner that errors — the P0 failure path).
//! 2. Wait for the run's kind-46012 `status=failed` terminal.
//! 3. Assert a `br` bead was created carrying the failure bundle fields
//!    (run-id, workflow name, owner, error, retry history).
//! 4. Assert no diagnosis workflow registered → the bead waits
//!    (documented human-in-loop, NOT silent).
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc22_cron_failure_bead -- --ignored`

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::*;

#[test]
#[ignore = "blocked on P4.4 bead-on-fail (bd-p4-bead-on-fail-0uaf): br-create of the failure bundle not landed"]
fn sc22_cron_failure_bead_creates_triageable_bundle() {
    // IMPL(P4.4): boot server + failing cron workflow; after the run fails
    // (kind-46012 status=failed), assert a br bead exists with:
    //   - run-id
    //   - workflow name
    //   - owner (the cron's owning profile)
    //   - error (the step failure)
    //   - last-known-good + retry history (if the runner records them)
    // Assert the loop is HUMAN-IN-LOOP when no diagnosis workflow is
    // registered for the owner (bead waits, not silent).
    unimplemented!("cron_failure_bead: wire once bd-p4-bead-on-fail lands")
}
