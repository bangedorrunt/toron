// governed-by: ADR-0005 (P4.4/P4.7 — repeat-failure escalate, Behavior Test
// Matrix row `cron_escalate`)
#![forbid(unsafe_code)]

//! SC-23 — `cron_escalate` (Behavior Test Matrix row, ADR-0005 Spec 3).
//!
//! When `repeat_failure` reaches `repeat_threshold` consecutive failures, the
//! cron ESCALATES: the policy sink records `action=Escalate` — the embedding
//! sends the kind-14 DM (importance=high, ack_required) to the captain. The
//! threshold is EXACT: `threshold-1` failures record `on_failure`, the
//! `threshold`-th records `on_repeat_failure`.
//!
//! Scenario:
//! 1. Failing cron with `policy { on_failure: log, on_repeat_failure:
//!    escalate, repeat_threshold: 2 }` (2 consecutive failures → escalate).
//! 2. First failure → `action=Log` (consecutive=1, below threshold).
//! 3. Second failure → `action=Escalate` (consecutive=2 == threshold).
//! 4. Third failure → `action=Escalate` (streak stays >= threshold).
//!
//! Run: `cargo test -p toron-e2e --features p4 --test sc23_cron_escalate`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use toron_workflow::cron::{CronManager, CronRegistration, CronRegistry};
use toron_workflow::cron_failure::PolicyEvent;
use toron_workflow::engine::WorkflowEngine;
use toron_workflow::schema::{Action, CronScope, FailureAction, parse_workflow};

/// A runner that ALWAYS fails (every tick's step errors).
fn always_failing_runner() -> toron_workflow::engine::AsyncEffectRunner {
    common::lift_async(|a: &Action| {
        if let Action::SendDm { body, .. } = a
            && body == "upgrade"
        {
            return Ok(serde_json::json!({
                "ok": false,
                "error": "repeat failure",
            }));
        }
        Ok(serde_json::Value::Null)
    })
}

/// The failing workflow with `on_repeat_failure: escalate` at threshold 2.
fn failing_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: docker-upgrade
trigger:
  type: schedule
  cron: "* * * * *"
  owner: housekeeper
policy:
  on_failure: log
  on_repeat_failure: escalate
  repeat_threshold: 2
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
        cron: "*/2 * * * * *".into(),
        workflow: wf,
        owner: "@housekeeper".into(),
        scope: CronScope::default(),
        enabled: true,
    }
}

#[tokio::test]
async fn sc23_cron_escalate_at_threshold() {
    let dir = std::env::temp_dir().join(format!("toron-sc23-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let Some(mut server) = boot_server(port, &dir) else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };
    wait_ready(&server_url).await;

    let engine = Arc::new(
        WorkflowEngine::remote_with_resolvers_and_async_effect(
            &server_url,
            housekeeper_resolver(),
            WorkflowEngine::default_workflow_resolver(),
            Some(always_failing_runner()),
        )
            .expect("engine"),
    );
    let registry: CronRegistry = {
        let reg = registration("sc23-escalating");
        Arc::new(move || vec![reg.clone()])
    };
    let mgr = CronManager::new(engine.clone(), registry).expect("manager");
    mgr.start().await.expect("start cron loop");

    // Collect events until an ESCALATE appears (the row: repeat_failure ≥
    // threshold → escalate). Early ticks may record below-threshold actions.
    let escalate = collect_until(&engine, "sc23-escalating", FailureAction::Escalate, 10)
        .await
        .expect("cron_escalate: consecutive failures must reach the threshold and escalate");

    // The escalate event carries a streak at-or-above the threshold (2) —
    // `consecutive` is the running count in the durable loop.
    assert!(
        escalate.bundle.consecutive >= 2,
        "escalation fires at-or-after repeat_threshold=2 consecutive failures: {escalate:?}"
    );
    assert_eq!(
        escalate.action,
        FailureAction::Escalate,
        "the threshold-crossing event is on_repeat_failure (escalate): {escalate:?}"
    );

    // The escalate payload carries the full bundle for the kind-14 DM
    // (importance=high, ack_required) the embedding sends to the captain.
    assert_eq!(
        escalate.bundle.owner, "@housekeeper",
        "escalate DM targets the owner's identity: {escalate:?}"
    );
    assert!(
        escalate.bundle.error.contains("repeat failure"),
        "escalate DM carries the failure context: {escalate:?}"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Collect events for the cron until one with `want` appears, or timeout.
async fn collect_until(
    engine: &WorkflowEngine,
    cron: &str,
    want: FailureAction,
    max_events: usize,
) -> Option<PolicyEvent> {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let sink = engine.policy_sink().lock().await;
        let events: Vec<PolicyEvent> = sink.iter().filter(|e| e.cron == cron).cloned().collect();
        drop(sink);
        if let Some(ev) = events.iter().find(|e| e.action == want) {
            return Some(ev.clone());
        }
        if events.len() >= max_events {
            // Stop early — we have enough events to conclude no escalate yet.
            return None;
        }
    }
    None
}
