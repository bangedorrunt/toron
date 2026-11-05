// governed-by: ADR-0005 (P4.2 — spawn await failure, Behavior Test Matrix row
#![forbid(unsafe_code)]

//! SC-20 — `spawn` + `await: true` FAILS on the job's error (Behavior Test
//! Matrix row `spawn_await_failure`, ADR-0005 Spec 4a).
//!
//! A spawn-await failure is NOT a decision: the mock launcher returns an
//! error, the host feeds `spawn_failed`, and the fold lands
//! `{ok:false, error}` under the step — Spec 0 propagates the failure
//! (unlike `request_approval`, where rejection is a DECISION and the run
//! continues). No server, no promise sink, no bus subscriber.
//!
//! Run: `cargo test -p toron-e2e --test sc20_spawn_await_failure`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::durable;
use toron_core::kernel::OurEngine;
use toron_workflow::engine::durable::{RunEffect, RunPhase};
use toron_workflow::engine::durable_host::DurableRun;
use toron_workflow::engine::spawn::{SpawnLauncher, SpawnSeams};
use toron_workflow::schema::parse_workflow;

/// A launcher whose child FAILS (the job agent reports the error).
fn failing_launcher() -> SpawnLauncher {
    Arc::new(|_spec| {
        Box::pin(async move {
            Err(anyhow::anyhow!(
                "docker-upgrade failed: health-gate timeout"
            ))
        })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

#[tokio::test]
async fn sc20_spawn_await_fails_on_job_error() {
    let (pool, db) = durable::test_pool("sc20").await;
    let wf = parse_workflow(
        r##"
name: spawn-failure
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    workflow: docker-upgrade
    await: true
    no_claim_timeout: 30
"##,
    )
    .expect("parse");
    let trigger = serde_json::json!({});
    let seams = SpawnSeams {
        resolver: Some(common::housekeeper_resolver()),
        launcher: Some(failing_launcher()),
    };

    let (engine_store, journal, state) = durable::handles(&pool);
    let (wf_open, trig_open) = (wf.clone(), trigger.clone());
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open_with_seams(
            engine,
            state,
            "sc20-spawn-failure",
            wf_open,
            trig_open,
            durable::now_ms(),
            &seams,
        )
    })
    .await
    .expect("open task")
    .expect("open with launcher");
    let [RunEffect::StartSpawn { node, spec }, ..] = effects.as_slice() else {
        panic!("spawn step parks with StartSpawn: {effects:?}");
    };
    assert_eq!(node, "build");
    assert!(
        matches!(run.model().phase, RunPhase::AwaitingSpawn { .. }),
        "run is genuinely SUSPENDED (not done) before the error"
    );

    let error = failing_launcher()(spec.clone())
        .await
        .expect_err("mock launcher fails");
    let node = node.clone();
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let mut run = run;
        run.spawn_failed(&node, format!("{error:#}"), durable::now_ms())
            .map(|eff| (run, eff))
    })
    .await
    .expect("fail task")
    .unwrap_or_else(|e| panic!("spawn_failed failed: {e}"));
    let _ = effects;
    drop(run);

    let state_store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    let json = state_store
        .load("sc20-spawn-failure")
        .await
        .expect("state read")
        .expect("model row exists");
    let model: serde_json::Value = serde_json::from_str(&json).expect("model parses");
    assert_eq!(
        model["outputs"]["build"]["ok"], false,
        "spawn-await failure fails the step, NOT a decision: {model}"
    );
    let err = model["outputs"]["build"]["error"]
        .as_str()
        .unwrap_or_default();
    assert!(
        err.contains("health-gate timeout"),
        "the job's error surfaces in the step failure: {err}"
    );

    let _ = std::fs::remove_file(&db);
}
