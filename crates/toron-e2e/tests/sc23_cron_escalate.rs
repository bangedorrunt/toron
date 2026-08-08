// governed-by: ADR-0005 (P4.4/P4.7 — repeat-failure escalate, Behavior Test
// Matrix row `cron_escalate`)
#![forbid(unsafe_code)]

//! SC-23 — `cron_escalate` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! When `repeat_failure` reaches `repeat_threshold` CONSECUTIVE failures, the
//! cron's policy action switches to `on_repeat_failure` (escalate: kind-14 DM
//! to the captain, importance=high, ack_required — executed by the embedding).
//! This SC asserts the DECISION layer: with a threshold of 3, the first two
//! failing ticks record `on_failure`; the THIRD records `Escalate` — exact
//! threshold, not early, not late.
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc23_cron_escalate`

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use toron_workflow::cron::{CronManager, CronRegistration, CronRegistry};
use toron_workflow::engine::{ActionRunner, WorkflowEngine};
use toron_workflow::schema::{Action, FailureAction, parse_workflow};

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

fn failing_runner() -> ActionRunner {
    Arc::new(|a: &Action| {
        if matches!(a, Action::SendMessage { .. }) {
            Err(anyhow::anyhow!("upgrade failed"))
        } else {
            Ok(serde_json::Value::Null)
        }
    })
}

fn failing_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: docker-upgrade
trigger:
  type: schedule
  cron: "* * * * *"
  owner: "@housekeeper"
policy:
  on_failure: log
  on_repeat_failure: escalate
  repeat_threshold: 3
  on_success: log
steps:
  - name: upgrade
    action: send_message
    to: ["ops"]
    subject: "upgrade"
    body: "run"
    on_error: fail_run
"##,
    )
    .expect("parse failing workflow")
}

#[tokio::test]
async fn sc23_cron_escalate_dm_at_threshold() {
    let dir = std::env::temp_dir().join(format!("toron-sc23-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let mut server = boot_server(port, &dir);
    wait_ready(&server_url).await;

    let registry: CronRegistry = {
        let reg = CronRegistration {
            name: "sc23-escalate".into(),
            cron: "*/1 * * * * *".into(),
            workflow: failing_workflow(),
            owner: "@housekeeper".into(),
            scope: Default::default(),
            enabled: true,
        };
        Arc::new(move || vec![reg.clone()])
    };
    let engine = Arc::new(
        WorkflowEngine::remote(&server_url, failing_runner()).expect("engine"),
    );
    let mgr = CronManager::new(engine.clone(), registry).expect("manager");
    mgr.start().await.expect("start loop");

    // Collect policy events until we see the Escalate decision (threshold 3).
    let mut events = Vec::new();
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        events = engine.policy_sink().lock().await.clone();
        if events.iter().any(|e| e.action == FailureAction::Escalate) {
            break;
        }
    }

    assert!(
        events.iter().any(|e| e.action == FailureAction::Escalate),
        "escalation fired within the window: {events:?}"
    );

    eprintln!("[sc23] full event sequence: {events:?}");

    // Threshold is EXACT: the first two failures of a contiguous streak use
    // on_failure (log), the THIRD escalates. The streak may reset during
    // startup warm-up (the durable loop can re-execute from scratch once —
    // honest restart semantics: a fresh loop starts the streak at 1), so
    // assert on the LAST contiguous run.
    let run = last_contiguous_run(&events);
    assert!(
        run.len() >= 3,
        "a contiguous 1..N streak formed: {events:?}"
    );
    let run_actions: Vec<FailureAction> = run.iter().map(|e| e.action).collect();
    let run_counters: Vec<u32> = run.iter().map(|e| e.bundle.consecutive).collect();
    assert_eq!(
        run_counters,
        vec![1, 2, 3],
        "consecutive streak increments exactly 1..3: {events:?}"
    );
    assert_eq!(
        run_actions[..2],
        [FailureAction::Log, FailureAction::Log],
        "N-1 failures do NOT escalate: {events:?}"
    );
    assert_eq!(
        run_actions[2],
        FailureAction::Escalate,
        "the N-th consecutive failure escalates (threshold exact): {events:?}"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The longest contiguous suffix whose `consecutive` counters ascend by 1
/// (the steady-state streak, ignoring startup warm-up resets).
fn last_contiguous_run(
    events: &[toron_workflow::cron_failure::PolicyEvent],
) -> Vec<toron_workflow::cron_failure::PolicyEvent> {
    let mut start = 0usize;
    for i in (1..events.len()).rev() {
        if events[i].bundle.consecutive != events[i - 1].bundle.consecutive + 1 {
            start = i;
            break;
        }
    }
    events[start..].to_vec()
}
