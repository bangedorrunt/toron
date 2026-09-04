// governed-by: ADR-0005 (P4.4/P4.7 — bead-on-failure, Behavior Test Matrix row
// `cron_failure_bead`)
#![forbid(unsafe_code)]

//! SC-22 — `cron_failure_bead` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! A cron run that FAILS (P0: kind-46012 `status=failed`) must produce a
//! triageable bead. The P4.4 failure policy records a [`PolicyEvent`] with
//! `action=Bead` on the [`PolicySink`] — the bundle is the `br create` payload
//! (run-id, workflow, owner, error, failed_step, consecutive). When no
//! diagnosis workflow is registered for the owner the loop is HUMAN-IN-LOOP:
//! the bead waits — it is created, not silently dropped.
//!
//! Scenario:
//! 1. Start a cron whose workflow FAILS (`on_error: fail_run` + an action the
//!    runner errors on) with `policy { on_failure: bead, repeat_threshold: N }`.
//! 2. Wait for the PolicySink to record a Bead event.
//! 3. Assert the bundle is complete (run_id, workflow, owner, error,
//!    failed_step) and the action is Bead.
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc22_cron_failure_bead`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use toron_workflow::cron::{CronManager, CronRegistration, CronRegistry};
use toron_workflow::cron_failure::PolicyEvent;
use toron_workflow::engine::{ActionRunner, WorkflowEngine};
use toron_workflow::schema::{Action, FailureAction, parse_workflow};

/// A runner that FAILS the `upgrade` action (returns `{ok:false,error}`) —
/// with `on_error: fail_run` that flips the run to `status=failed` (P0).
fn failing_runner() -> ActionRunner {
    Arc::new(|a: &Action| {
        if let Action::SendDm { body, .. } = a
            && body == "upgrade"
        {
            return Ok(serde_json::json!({
                "ok": false,
                "error": "health-gate timeout: container unhealthy",
            }));
        }
        Ok(serde_json::Value::Null)
    })
}

/// The failing cron workflow: `on_error: fail_run` + a step the runner errors
/// on, plus the Spec 3 failure policy (`on_failure: bead`).
fn failing_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: docker-upgrade
trigger:
  type: schedule
  cron: "* * * * *"
  owner: housekeeper
policy:
  on_failure: bead
  on_repeat_failure: escalate
  repeat_threshold: 100
  on_success: log
steps:
  - name: upgrade
    on_error: fail_run
    action: send_dm
    to: pi
    body: upgrade
"##,
    )
    .expect("parse failing workflow")
}

fn registration(name: &str) -> CronRegistration {
    let wf = failing_workflow();
    CronRegistration {
        name: name.into(),
        cron: "*/1 * * * * *".into(),
        workflow: wf,
        owner: "@housekeeper".into(),
        scope: Default::default(),
        enabled: true,
    }
}

/// Poll the PolicySink for an event matching the cron name + action.
async fn wait_for_policy(
    engine: &WorkflowEngine,
    cron: &str,
    action: FailureAction,
) -> Option<PolicyEvent> {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let sink = engine.policy_sink().lock().await;
        if let Some(ev) = sink.iter().find(|e| e.cron == cron && e.action == action) {
            return Some(ev.clone());
        }
    }
    None
}

#[tokio::test]
async fn sc22_cron_failure_bead_records_triageable_bundle() {
    let dir = std::env::temp_dir().join(format!("toron-sc22-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let Some(mut server) = boot_server(port, &dir) else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };
    wait_ready(&server_url).await;

    let engine = Arc::new(
        WorkflowEngine::remote_with(&server_url, failing_runner(), housekeeper_resolver())
            .expect("engine"),
    );
    let registry: CronRegistry = {
        let reg = registration("sc22-failing");
        Arc::new(move || vec![reg.clone()])
    };
    let mgr = CronManager::new(engine.clone(), registry).expect("manager");
    mgr.start().await.expect("start cron loop");

    // The failing tick must record a Bead policy event with the full bundle.
    let ev = wait_for_policy(&engine, "sc22-failing", FailureAction::Bead)
        .await
        .expect("cron_failure_bead: a failed run records a Bead policy event");

    // The bundle is the `br create` payload — every field the triage loop needs.
    assert_eq!(
        ev.bundle.workflow, "docker-upgrade",
        "bundle carries the failed workflow name: {ev:?}"
    );
    assert_eq!(
        ev.bundle.owner, "@housekeeper",
        "bundle carries the owning profile (the fixer's identity): {ev:?}"
    );
    assert!(
        ev.bundle.run_id.starts_with("cron:sc22-failing:"),
        "bundle carries the run id: {ev:?}"
    );
    assert!(
        ev.bundle.error.contains("health-gate timeout"),
        "bundle carries the step error: {ev:?}"
    );
    assert_eq!(
        ev.bundle.failed_step.as_deref(),
        Some("upgrade"),
        "bundle carries the failed step: {ev:?}"
    );
    assert_eq!(
        ev.bundle.consecutive, 1,
        "first failure is consecutive=1 (bead at on_failure, not repeat): {ev:?}"
    );

    // Human-in-loop: the bead is created (event recorded) even though no
    // diagnosis workflow is registered — the loop is NOT silent. The bead
    // waits for `flywheel work ready` (the embedding's job).
    assert_eq!(
        ev.action,
        FailureAction::Bead,
        "on_failure: bead is the recorded action"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
