// governed-by: ADR-0005 (P4.2 — spawn no_claim_timeout abort, SC-18)
#![forbid(unsafe_code)]

//! SC-18 — `spawn` `no_claim_timeout` abort (Behavior Test Matrix row
//! `spawn_no_claim_timeout`, ADR-0005 Spec 4a).
//!
//! A spawn-await that is never claimed must ABORT after `no_claim_timeout` —
//! the run FAILS (a failure, not a decision). The abort is host-enforced:
//! the model parks the run with a deadline (`AwaitingSpawn`), the host
//! bounds the mock launcher with `tokio::time::timeout`, and on expiry feeds
//! `woke` at the deadline — the fold owns the canonical timeout text
//! (`spawn-await {key}: timed out after {N}s`) and fails the node. No server,
//! no promise timer loop.
//!
//! Run: `cargo test -p toron-e2e --test sc18_spawn_no_claim_timeout`

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::durable;
use toron_core::kernel::OurEngine;
use toron_workflow::engine::durable::{RunEffect, RunPhase};
use toron_workflow::engine::durable_host::DurableRun;
use toron_workflow::engine::spawn::{SpawnLauncher, SpawnSeams};
use toron_workflow::schema::parse_workflow;

/// A launcher that never settles (the child is started but never claimed).
/// The `started` flag proves the launch was invoked before the timeout.
fn pending_launcher(started: Arc<AtomicBool>) -> SpawnLauncher {
    Arc::new(move |_spec| {
        started.store(true, Ordering::SeqCst);
        Box::pin(async move {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok(serde_json::json!({}))
        })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

#[tokio::test]
async fn sc18_spawn_no_claim_timeout_aborts() {
    let (pool, db) = durable::test_pool("sc18").await;
    let wf = parse_workflow(
        r##"
name: spawn-timeout
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    await: true
    no_claim_timeout: 2
"##,
    )
    .expect("parse");
    let trigger = serde_json::json!({});
    let started = Arc::new(AtomicBool::new(false));
    let seams = SpawnSeams {
        resolver: Some(common::housekeeper_resolver()),
        launcher: Some(pending_launcher(started.clone())),
    };

    let (engine_store, journal, state) = durable::handles(&pool);
    let (wf_open, trig_open) = (wf.clone(), trigger.clone());
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open_with_seams(
            engine,
            state,
            "sc18-spawn-timeout",
            wf_open,
            trig_open,
            durable::now_ms(),
            &seams,
        )
    })
    .await
    .expect("open task")
    .expect("open with launcher");
    let [RunEffect::StartSpawn { node, .. }, ..] = effects.as_slice() else {
        panic!("spawn step parks with StartSpawn: {effects:?}");
    };
    assert_eq!(node, "build");
    let deadline_ms = match run.model().phase.clone() {
        RunPhase::AwaitingSpawn {
            deadline_ms: Some(deadline),
            ..
        } => deadline,
        other => panic!("run parked AwaitingSpawn with a deadline: {other:?}"),
    };

    let launcher = pending_launcher(started.clone());
    let spec = match effects.iter().find_map(|e| match e {
        RunEffect::StartSpawn { spec, .. } => Some(spec.clone()),
        _ => None,
    }) {
        Some(spec) => spec,
        None => panic!("StartSpawn carries the launch spec"),
    };
    let now_ms = durable::now_ms();
    assert!(deadline_ms > now_ms, "deadline is in the future");
    let launched = tokio::time::timeout(
        Duration::from_millis(deadline_ms - now_ms),
        (launcher)(spec),
    )
    .await;
    assert!(
        launched.is_err(),
        "the unclaimed launch outlives the deadline (timeout, not settle)"
    );
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let mut run = run;
        run.woke(deadline_ms).map(|eff| (run, eff))
    })
    .await
    .expect("woke task")
    .unwrap_or_else(|e| panic!("woke at deadline failed: {e}"));
    let _ = effects;
    drop(run);

    assert!(
        started.load(Ordering::SeqCst),
        "the child was launched before the timeout (not refused upfront)"
    );

    let (engine_store, journal, state) = durable::handles(&pool);
    let (wf_open, trig_open) = (wf.clone(), trigger.clone());
    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open(
            engine,
            state,
            "sc18-spawn-timeout",
            wf_open,
            trig_open,
            durable::now_ms(),
        )
    })
    .await
    .expect("reopen task")
    .expect("reopen");
    drop(run);
    assert!(
        effects.is_empty(),
        "terminal run re-issues nothing: {effects:?}"
    );

    let state_store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    let json = state_store
        .load("sc18-spawn-timeout")
        .await
        .expect("state read")
        .expect("model row exists");
    let model: serde_json::Value = serde_json::from_str(&json).expect("model parses");
    assert_eq!(
        model["state"]["phase"]["phase"], "succeeded",
        "the run completes (Continue) with the step failed, not hung: {model}"
    );
    assert_eq!(
        model["state"]["outputs"]["build"]["ok"], false,
        "unclaimed spawn-await aborts and fails the step: {model}"
    );
    let err = model["state"]["outputs"]["build"]["error"]
        .as_str()
        .unwrap_or_default();
    assert!(
        err.contains("timed out"),
        "timeout surfaced in the canonical text: {err}"
    );

    let _ = std::fs::remove_file(&db);
}
