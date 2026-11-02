// governed-by: ADR-0005 (P4.2 — spawn await resume, Behavior Test Matrix row
// `spawn_await_resume`)
#![forbid(unsafe_code)]

//! SC-19 — `spawn` + `await: true` SUSPENDS and resumes on the job's receipt
//! (Behavior Test Matrix row `spawn_await_resume`, ADR-0005 Spec 4a).
//!
//! A spawn-await is NOT an approval: the resolver is a LAUNCHER that starts
//! a child and returns its receipt. The run parks (`AwaitingSpawn`,
//! `StartSpawn` effect carries the self-contained spec); the host awaits the
//! mock launcher and feeds `spawn_settled` — the run RESUMES with
//! `{ok:true, spawn_key, result}` under `steps.<name>`. No server, no
//! promise sink, no bus subscriber.
//!
//! Run: `cargo test -p toron-e2e --test sc19_spawn_await_resume`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::durable;
use toron_core::kernel::OurEngine;
use toron_workflow::engine::durable::{RunEffect, RunPhase};
use toron_workflow::engine::durable_host::DurableRun;
use toron_workflow::engine::spawn::{SpawnLauncher, SpawnSeams};
use toron_workflow::schema::parse_workflow;

/// A launcher that settles immediately with the job's receipt (the agent
/// claimed and completed the job).
fn receipt_launcher() -> SpawnLauncher {
    Arc::new(|_spec| {
        Box::pin(async move { Ok(serde_json::json!({ "exit": 0, "checks": "all-healthy" })) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

#[tokio::test]
async fn sc19_spawn_await_resumes_on_job_result() {
    let (pool, db) = durable::test_pool("sc19").await;
    let wf = parse_workflow(
        r##"
name: spawn-resume
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
        launcher: Some(receipt_launcher()),
    };

    // 1. Open with seams: the spawn step prepares and parks (StartSpawn).
    let (engine_store, journal, state) = durable::handles(&pool);
    let (wf_open, trig_open) = (wf.clone(), trigger.clone());
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open_with_seams(
            engine,
            state,
            "sc19-spawn-resume",
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
        "run is genuinely SUSPENDED (not done) before the receipt"
    );
    assert!(
        !spec.spawn_key.is_empty(),
        "the spec carries the run:node:attempt correlation key"
    );

    // 2. The job agent completes: await the launcher (a plain future — no
    //    blocking, so no spawn_blocking needed) and feed the receipt.
    //    (A timeout arm like sc18's would fire first on a hung launcher;
    //    this one settles immediately.)
    let receipt = receipt_launcher()(spec.clone())
        .await
        .expect("mock launcher settles");
    // Owned copy: the feed closure must be 'static for spawn_blocking.
    let node = node.clone();
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let mut run = run;
        run.spawn_settled(&node, receipt, durable::now_ms())
            .map(|eff| (run, eff))
    })
    .await
    .expect("settle task")
    .unwrap_or_else(|e| panic!("spawn_settled failed: {e}"));
    let _ = effects;
    drop(run);

    // 3. The parked run resumed and completed: read the settled model.
    let state_store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    let json = state_store
        .load("sc19-spawn-resume")
        .await
        .expect("state read")
        .expect("model row exists");
    let model: serde_json::Value = serde_json::from_str(&json).expect("model parses");
    assert_eq!(
        model["phase"]["phase"], "succeeded",
        "spawn-await resumes to SUCCESS with the job result: {model}"
    );
    assert_eq!(
        model["outputs"]["build"]["ok"], true,
        "spawn-await resumes to SUCCESS with the job result: {model}"
    );
    assert_eq!(
        model["outputs"]["build"]["result"]["exit"], 0,
        "job result payload flows into steps.build.result: {model}"
    );
    assert_eq!(
        model["outputs"]["build"]["result"]["checks"], "all-healthy",
        "job result payload flows into steps.build.result: {model}"
    );

    let _ = std::fs::remove_file(&db);
}
