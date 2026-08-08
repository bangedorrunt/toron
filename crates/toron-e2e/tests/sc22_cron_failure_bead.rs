// governed-by: ADR-0005 (P4.4/P4.7 — bead-on-failure, Behavior Test Matrix row
// `cron_failure_bead`)
#![forbid(unsafe_code)]

//! SC-22 — `cron_failure_bead` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! A cron run that FAILS (kind-46012 `status=failed`, P0) records a
//! **`PolicyEvent`** on the engine's `PolicySink` with the full failure
//! bundle (run-id, workflow, owner, error, failed step, consecutive count) and
//! the policy action (Spec 3). The embedding (flywheel) executes the action —
//! `br create` a triageable bead, or escalate. This SC asserts the DECISION
//! layer: a failing cron with `on_failure: bead` pushes a `Bead` event whose
//! bundle carries the failure fields.
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc22_cron_failure_bead`

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

/// A runner that FAILS `send_message` actions (the failing step) and succeeds
/// on everything else.
fn failing_runner() -> ActionRunner {
    Arc::new(|a: &Action| {
        if matches!(a, Action::SendMessage { .. }) {
            Err(anyhow::anyhow!("docker upgrade failed: health-gate timeout"))
        } else {
            Ok(serde_json::Value::Null)
        }
    })
}

/// A workflow whose `upgrade` step fails via `on_error: fail_run` → the run
/// returns `{status: failed, error, failed_step}`.
fn failing_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: docker-upgrade
trigger:
  type: schedule
  cron: "* * * * *"
  owner: "@housekeeper"
policy:
  on_failure: bead
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
async fn sc22_cron_failure_bead_records_failure_bundle() {
    let dir = std::env::temp_dir().join(format!("toron-sc22-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let mut server = boot_server(port, &dir);
    wait_ready(&server_url).await;

    // A cron firing every 1 second whose workflow ALWAYS fails.
    let registry: CronRegistry = {
        let reg = CronRegistration {
            name: "sc22-failing".into(),
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

    // Wait for a failure policy event: the first failing tick records
    // `on_failure: bead` with the full bundle.
    let mut event = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let sink = engine.policy_sink().lock().await;
        if let Some(e) = sink.last() {
            event = Some(e.clone());
            break;
        }
    }
    let event = event.expect("failed tick recorded a policy event");

    assert_eq!(
        event.action,
        FailureAction::Bead,
        "on_failure: bead on the first failure"
    );
    assert_eq!(event.cron, "sc22-failing");
    assert_eq!(event.bundle.workflow, "docker-upgrade");
    assert_eq!(event.bundle.owner, "@housekeeper");
    assert!(
        event.bundle.error.contains("health-gate timeout"),
        "error surfaced: {}",
        event.bundle.error
    );
    assert_eq!(event.bundle.failed_step.as_deref(), Some("upgrade"));
    assert_eq!(event.bundle.consecutive, 1, "first failure streak = 1");
    assert!(
        event.bundle.run_id.starts_with("cron:sc22-failing:"),
        "run id is the tick run: {}",
        event.bundle.run_id
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
