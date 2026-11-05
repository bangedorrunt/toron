// governed-by: ADR-0005 (P4.3/P4.7 — scheduler durability, Behavior Test
#![forbid(unsafe_code)]

//! SC-24 — `cron_catchup` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! **Scheduler durability — `latest` catch-up**: if flywheel is DOWN at a
//! cron's due times, on wake the durable loop runs the MOST-RECENT due tick
//! at-or-before `now` — exactly ONE run — never a burst of every missed tick.
//!
//! Pure selection (`latest_due_at_or_before`): given `last_fired` in the
//! past and `now` covering several due ticks, the catch-up selects the
//! LATEST one, and it is the same tick `next_due` would produce from the
//! previous tick (a single deterministic target — no burst). The
//! manager-level wake path (old assertion 2, vendor durable-sleep loop)
//! is retired with the vendor loop; its durable replacement is owned by
//! `cron_elm` tick tests (`Latest` policy) and the flywheel cron host.
//!
//! Run: `cargo test -p toron-e2e --test sc24_cron_catchup`

use toron_workflow::cron::{latest_due_at_or_before, next_due};

/// A `* * * * *` (every minute) cron has due ticks each minute.
const EVERY_MINUTE: &str = "0 * * * * *";

/// Pure selection: `latest` catch-up picks the newest due tick at-or-before
/// `now` — a single tick, not every missed one.
#[test]
fn sc24_catchup_selects_latest_due_tick_only() {
    let last_fired = 1_767_225_600i64;
    let now = last_fired + 3 * 60;

    let catch_up = latest_due_at_or_before(EVERY_MINUTE, last_fired, now)
        .expect("a catch-up tick exists after downtime");
    let latest_missed = next_due(EVERY_MINUTE, last_fired + 2 * 60)
        .expect("the tick after the previous missed one");

    assert_eq!(
        catch_up, latest_missed,
        "catch-up fires the LATEST missed tick (not every missed one): catch_up={catch_up} latest_missed={latest_missed}"
    );
    assert!(
        catch_up <= now,
        "catch-up tick is at-or-before now: {catch_up} <= {now}"
    );
    assert!(
        catch_up > last_fired,
        "catch-up tick is strictly after last_fired (no re-fire of an already-run tick): {catch_up} > {last_fired}"
    );

    let next_after = next_due(EVERY_MINUTE, catch_up).expect("next tick after catch-up");
    assert!(
        next_after > now,
        "after the catch-up, the loop resumes normal scheduling (next tick in the future): {next_after} > {now}"
    );
}
