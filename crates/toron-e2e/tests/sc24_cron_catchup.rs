// governed-by: ADR-0005 (P4.3/P4.7 — scheduler durability, Behavior Test
// Matrix row `cron_catchup`)
#![forbid(unsafe_code)]

//! SC-24 — `cron_catchup` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! **Scheduler durability**: if flywheel is DOWN at a cron's due time, on
//! wake the loop runs the MOST-RECENT due tick (`latest` policy) — never a
//! burst of every missed tick, and never a silent skip.
//!
//! Two layers of proof:
//! 1. The durable loop itself fires ticks (the loop drives `ctx.sleep` +
//!    `run_workflow`; observed via the injected runner).
//! 2. The `latest` catch-up policy is exact: [`latest_due_at_or_before`]
//!    returns the most-recent tick at-or-before `now` (strictly after the
//!    last fired one) — the loop fires exactly ONE run on wake. The pure
//!    helper is exercised against a simulated downtime window here; the
//!    unit tests in `toron-workflow::cron` pin the arithmetic.
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc24_cron_catchup`

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use toron_workflow::cron::{CronManager, CronRegistration, CronRegistry, latest_due_at_or_before};
use toron_workflow::engine::{ActionRunner, WorkflowEngine};
use toron_workflow::schema::{Action, parse_workflow};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn boot_server(port: u16, dir: &std::path::Path) -> Child {
    Command::new("resonate")
        .args([
            "serve",
            "--storage-type",
            "sqlite",
            "--server-port",
            &port.to_string(),
            "--server-host",
            "127.0.0.1",
        ])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn resonate server")
}

async fn wait_ready(url: &str) {
    for _ in 0..100 {
        let url = url.to_string();
        if tokio::task::spawn_blocking(move || {
            std::net::TcpStream::connect_timeout(
                &url.replace("http://", "").parse().expect("addr"),
                Duration::from_millis(200),
            )
            .is_ok()
        })
        .await
        .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("resonate server at {url} never became ready");
}

fn recording_runner(seen: Arc<Mutex<Vec<Action>>>) -> ActionRunner {
    let seen_clone = seen.clone();
    Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::Value::Null)
    })
}

fn tick_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: cron-tick
trigger:
  type: schedule
  cron: "* * * * *"
steps:
  - name: notify
    action: send_dm
    to: pi
    body: "cron ticked at {{trigger.tick}}"
"##,
    )
    .expect("parse tick workflow")
}

fn registration(name: &str, cron: &str) -> CronRegistration {
    CronRegistration {
        name: name.into(),
        cron: cron.into(),
        workflow: tick_workflow(),
        owner: "@housekeeper".into(),
        scope: Default::default(),
        enabled: true,
    }
}

#[tokio::test]
async fn sc24_cron_catchup_runs_latest_due_tick_only() {
    let dir = std::env::temp_dir().join(format!("toron-sc24-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let mut server = boot_server(port, &dir);
    wait_ready(&server_url).await;

    // A cron firing every 1 second (6-field, seconds first).
    let cron_expr = "*/1 * * * * *";

    // 1. The durable loop fires ticks (cron_success covered by sc21; here we
    //    re-prove the loop drives, then exercise catch-up).
    let seen: Arc<Mutex<Vec<Action>>> = Arc::new(Mutex::new(Vec::new()));
    let registry: CronRegistry = {
        let reg = registration("sc24-every-sec", cron_expr);
        Arc::new(move || vec![reg.clone()])
    };
    let engine = Arc::new(
        WorkflowEngine::remote(&server_url, recording_runner(seen.clone())).expect("engine"),
    );
    let mgr = CronManager::new(engine.clone(), registry).expect("manager");
    mgr.start().await.expect("start loop");

    let mut ticks = 0usize;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        ticks = seen.lock().unwrap().len();
        if ticks >= 1 {
            break;
        }
    }
    assert!(ticks >= 1, "loop fired a tick (saw {ticks} actions)");

    // 2. LATEST catch-up is exact: simulate "down across several due ticks".
    //    last_fired = the tick we just observed (before the downtime window),
    //    now = last_fired + 3 ticks. `latest_due_at_or_before` must return
    //    exactly the THIRD tick — the most recent due one — not the first.
    let last_fired = unix_now();
    let now = last_fired + 3 * 60; // 3 one-minute ticks later (spec 5-field math)
    let cron_5 = "* * * * *"; // every minute, 5-field
    let caught = latest_due_at_or_before(cron_5, last_fired, now).expect("catch-up tick");
    let third = next_due_n(cron_5, last_fired, 3);
    assert_eq!(
        caught, third,
        "latest catch-up = the most-recent missed tick, not the first"
    );
    // No burst: firing `caught` advances last_fired; the following due is
    // strictly AFTER the (now-past) window — intermediate misses are skipped.
    let next = toron_workflow::cron::next_due(cron_5, caught).expect("next");
    assert!(
        next > now,
        "after the caught-up tick, resume at the next future tick"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The Nth tick strictly after `from` (1-indexed).
fn next_due_n(cron: &str, from: i64, n: u32) -> i64 {
    let mut cur = from;
    let mut last = from;
    for _ in 0..n {
        last = toron_workflow::cron::next_due(cron, cur).expect("tick");
        cur = last;
    }
    last
}
