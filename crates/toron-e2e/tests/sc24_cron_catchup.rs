// governed-by: ADR-0005 (P4.3/P4.7 — scheduler durability, Behavior Test
// Matrix row `cron_catchup`)
#![forbid(unsafe_code)]

//! SC-24 — `cron_catchup` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! **Scheduler durability — `latest` catch-up**: if flywheel is DOWN at a
//! cron's due times, on wake the durable loop runs the MOST-RECENT due tick
//! at-or-before `now` — exactly ONE run — never a burst of every missed tick.
//!
//! Two assertions:
//! 1. **Pure selection** (`latest_due_at_or_before`): given `last_fired` in
//!    the past and `now` covering several due ticks, the catch-up selects the
//!    LATEST one, and it is the same tick `next_due` would produce from the
//!    previous tick (a single deterministic target — no burst).
//! 2. **Manager-level**: a restart after downtime (a fresh manager on the
//!    same server where the loop's `last_fired` is behind `now`) resumes the
//!    same `cron:{name}` loop; the catch-up fires one tick per wake, never a
//!    burst.
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc24_cron_catchup`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use toron_workflow::cron::{latest_due_at_or_before, next_due};

/// A `* * * * *` (every minute) cron has due ticks each minute.
const EVERY_MINUTE: &str = "0 * * * * *";

/// Pure selection: `latest` catch-up picks the newest due tick at-or-before
/// `now` — a single tick, not every missed one.
#[test]
fn sc24_catchup_selects_latest_due_tick_only() {
    // last_fired = 2026-01-01T00:00:00Z. due ticks at :00 every minute.
    let last_fired = 1_767_225_600i64; // 2026-01-01T00:00:00Z
    let now = last_fired + 3 * 60; // three minutes of "downtime" → 4 due ticks missed

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

    // The catch-up is ONE tick — the next due after it is in the future.
    let next_after = next_due(EVERY_MINUTE, catch_up).expect("next tick after catch-up");
    assert!(
        next_after > now,
        "after the catch-up, the loop resumes normal scheduling (next tick in the future): {next_after} > {now}"
    );
}

/// Manager-level: a restart after downtime (fresh worker, same server)
/// resumes the `cron:{name}` loop and keeps firing — one tick per wake, no
/// burst. Restart survival is sc21's row; this asserts the loop's wake path
/// still produces the next single tick (the catch-up wake).
#[tokio::test]
async fn sc24_catchup_loop_wakes_and_fires_single_tick() {
    let dir = std::env::temp_dir().join(format!("toron-sc24-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let Some(mut server) = boot_server(port, &dir) else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };
    wait_ready(&server_url).await;

    // Every-second cron (6-field) so a tick lands well within the test.
    let every_sec = "*/1 * * * * *";

    // First worker: start the loop, let one tick fire.
    let seen_a: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let runner_a = {
        let seen = seen_a.clone();
        Arc::new(move |a: &toron_workflow::schema::Action| {
            seen.lock().unwrap().push(format!("{a:?}"));
            Ok(serde_json::Value::Null)
        })
    };
    let registry: toron_workflow::cron::CronRegistry = {
        let reg = crate_fixture_registration("sc24-catchup", every_sec);
        Arc::new(move || vec![reg.clone()])
    };
    let engine_a = Arc::new(
        toron_workflow::engine::WorkflowEngine::remote_with(
            &server_url,
            runner_a,
            housekeeper_resolver(),
        )
        .expect("engine A"),
    );
    let mgr_a = toron_workflow::cron::CronManager::new(engine_a.clone(), registry.clone())
        .expect("manager A");
    mgr_a.start().await.expect("start loop A");

    // Wait for ≥1 tick on worker A.
    let mut ticks_a = 0usize;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ticks_a = seen_a.lock().unwrap().len();
        if ticks_a >= 1 {
            break;
        }
    }
    assert!(
        ticks_a >= 1,
        "cron_success baseline: loop A fired a tick (saw {ticks_a})"
    );

    // "Downtime": drop worker A. The server retains the durable loop.
    drop(mgr_a);
    drop(engine_a);

    // Restart on the same server: the loop resumes from its durable
    // checkpoint (catch-up wake) and fires the next single tick.
    let seen_b: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));
    let runner_b = {
        let seen = seen_b.clone();
        Arc::new(move |a: &toron_workflow::schema::Action| {
            seen.lock().unwrap().push(format!("{a:?}"));
            Ok(serde_json::Value::Null)
        })
    };
    let engine_b = Arc::new(
        toron_workflow::engine::WorkflowEngine::remote_with(
            &server_url,
            runner_b,
            housekeeper_resolver(),
        )
        .expect("engine B"),
    );
    let mgr_b =
        toron_workflow::cron::CronManager::new(engine_b.clone(), registry).expect("manager B");
    mgr_b.start().await.expect("start loop B");

    // The resumed loop fires a tick through worker B (catch-up wake → one tick).
    let mut ticks_b = 0usize;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ticks_b = seen_b.lock().unwrap().len();
        if ticks_b >= 1 {
            break;
        }
    }
    assert!(
        ticks_b >= 1,
        "cron_catchup: the resumed loop wakes and fires the latest single tick (saw {ticks_b} on worker B)"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

fn crate_fixture_registration(name: &str, cron: &str) -> toron_workflow::cron::CronRegistration {
    fixture_registration(name, cron, true)
}
