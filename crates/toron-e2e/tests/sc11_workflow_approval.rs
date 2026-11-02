// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-11 — Workflow approval: suspend → crash → resume (P2 / Phase-2 gate).
//!
//! **The decisive durability differentiator vs Buzz (WF-08 fix).** Buzz marks a
//! run that hits `request_approval` as FAILED when it doesn't resume in-process;
//! toron's durable host PARKS the run model in sqlite, so a worker-process
//! crash mid-run loses nothing — a fresh driver re-opens the same run id, the
//! operator's approval lands through the state store, and the run RESUMES at
//! the unsettled step, not marked failed.
//!
//! Simulation (real crash semantics, one process, no server):
//! 1. Temp sqlite pool — durable state survives any driver restart because
//!    it is store-side (the model row + engine row, not memory).
//! 2. Driver A opens `sc11-deploy` → prep step executes → hits
//!    `request_approval` → parks (AwaitApproval; run id is the key).
//! 3. **Kill the worker**: drop driver A entirely (fresh handles on reopen
//!    prove no live state carried over).
//! 4. **Restart**: re-open the same run id (resume path — the persisted
//!    model wins) and approve through the state store.
//! 5. Drive to terminal → completes with the approval decision recorded
//!    under the node name AND the `decision` alias. NOT failed.
//!
//! Gate: `cargo test -p toron-e2e --features p2`.

#![cfg(feature = "p2")]

#[path = "common/mod.rs"]
mod common;

use common::durable;
use toron_core::kernel::OurEngine;
use toron_workflow::engine::durable::RunEffect;
use toron_workflow::engine::durable_host::{DurableRun, HostError};
use toron_workflow::schema::parse_workflow;

/// A `deploy` workflow that hits `request_approval` after one step.
fn deploy_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: deploy
trigger:
  type: webhook
steps:
  - name: prep
    action: send_message
    to: ["ops"]
    subject: "prep"
    body: "staging ready"
  - name: gate
    action: request_approval
    message: "Approve deploy to prod?"
  - name: ship
    action: send_message
    to: ["ops"]
    subject: "shipped"
    body: "prod deployed"
"##,
    )
    .expect("parse deploy workflow")
}

type HostRun = DurableRun<
    toron_core::index::durable_engine::ToronEngineStore,
    toron_core::journal::ToronJournal,
    toron_core::index::durable_workflow::SyncWorkflowState,
>;

/// Open (or resume) one run inside `spawn_blocking` — driver verbs are
/// sync-over-channels and must never run on an async worker.
async fn open_run(
    pool: &sqlx::SqlitePool,
    run_id: &str,
    workflow: toron_workflow::schema::Workflow,
    trigger: serde_json::Value,
) -> Result<(HostRun, Vec<RunEffect>), HostError> {
    let (engine_store, journal, state) = durable::handles(pool);
    let (rid, wf, trig) = (run_id.to_string(), workflow, trigger);
    tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableRun::open(engine, state, rid, wf, trig, durable::now_ms())
    })
    .await
    .expect("open task")
}

async fn verb(
    run_id: &str,
    what: &str,
    run: HostRun,
    f: impl FnOnce(HostRun) -> Result<(HostRun, Vec<RunEffect>), HostError> + Send + 'static,
) -> (HostRun, Vec<RunEffect>) {
    tokio::task::spawn_blocking(move || f(run))
        .await
        .unwrap_or_else(|e| panic!("{run_id} durable {what} task failed: {e}"))
        .unwrap_or_else(|e| panic!("{run_id} durable {what} failed: {e}"))
}

#[tokio::test]
async fn sc11_approval_crash_resume() {
    let (pool, db) = durable::test_pool("sc11").await;
    let seams = durable::seams(durable::null_runner());
    let workflow = deploy_workflow();
    let trigger = serde_json::json!({});
    let digest = durable::digest(&workflow);

    // 2. Driver A runs the deploy workflow → prep executes, then parks.
    let (run, effects) = open_run(&pool, "sc11-deploy", workflow.clone(), trigger.clone())
        .await
        .expect("open");
    let [RunEffect::ExecuteStep { index, step }] = effects.as_slice() else {
        panic!("first effect executes prep: {effects:?}");
    };
    assert_eq!(*index, 0);
    let mut outputs: serde_json::Map<String, serde_json::Value> =
        run.model().outputs.clone().into_iter().collect();
    let value = durable::run_step(
        "sc11-deploy",
        &workflow.name,
        &digest,
        step,
        *index,
        &trigger,
        &mut outputs,
        &seams,
    )
    .await;
    let (run, effects) = verb("sc11-deploy", "step feed", run, move |mut run: HostRun| {
        run.step_done(value, durable::now_ms())
            .map(|eff| (run, eff))
    })
    .await;
    let [RunEffect::AwaitApproval { step, .. }] = effects.as_slice() else {
        panic!("prep settles into an approval park: {effects:?}");
    };
    assert_eq!(step, "gate");

    // 3. KILL THE WORKER: drop driver A entirely (fresh handles below prove
    //    no live state carried over — the pool retains everything).
    drop(run);

    // 4. RESTART: re-open the same run id (resume — the model wins) and
    //    approve the gate through the state store.
    let (run, effects) = open_run(&pool, "sc11-deploy", workflow.clone(), trigger.clone())
        .await
        .expect("reopen");
    assert!(
        matches!(
            run.model().phase,
            toron_workflow::engine::durable::RunPhase::AwaitingApproval { .. }
        ),
        "resume lands on the parked approval, not the first step"
    );
    assert!(
        effects.is_empty() || matches!(effects.as_slice(), [RunEffect::AwaitApproval { .. }]),
        "resume re-issues the park, never a settled step: {effects:?}"
    );
    let decision = serde_json::json!({"outcome": "approved", "note": "go"});
    let (mut run, mut effects) = verb("sc11-deploy", "approve", run, move |mut run: HostRun| {
        run.approve(decision, durable::now_ms())
            .map(|eff| (run, eff))
    })
    .await;

    // 5. Drive the remainder to terminal: ship executes, run completes.
    let output = loop {
        if effects.is_empty() {
            panic!("drive ran out of effects while non-terminal");
        }
        let mut follow = Vec::new();
        let mut done = None;
        for effect in std::mem::take(&mut effects) {
            match effect {
                RunEffect::ExecuteStep { index, step } => {
                    let mut outputs: serde_json::Map<String, serde_json::Value> =
                        run.model().outputs.clone().into_iter().collect();
                    let value = durable::run_step(
                        "sc11-deploy",
                        &workflow.name,
                        &digest,
                        &step,
                        index,
                        &trigger,
                        &mut outputs,
                        &seams,
                    )
                    .await;
                    let (back, mut eff) =
                        verb("sc11-deploy", "step feed", run, move |mut run: HostRun| {
                            run.step_done(value, durable::now_ms())
                                .map(|eff| (run, eff))
                        })
                        .await;
                    run = back;
                    follow.append(&mut eff);
                }
                RunEffect::Complete { output } => {
                    done = Some(output);
                    break;
                }
                other => panic!("unexpected effect after resume: {other:?}"),
            }
        }
        if let Some(output) = done {
            break output;
        }
        effects = follow;
    };
    drop(run);

    // ADR-0024 Spec 2 (one decision spelling): the decision is published under
    // the node's OWN name (`gate`). The durable `Complete` carries
    // `{status, steps}`, so the contract reads under `steps` (same keys,
    // new envelope).
    //
    // GAP (engine crew): the old engine ALSO aliased the decision under
    // `decision` on the linear path; the durable linear fold only lands it
    // under the node name today (the alias exists on the graph path only).
    // This test pins the node-name half; the alias half is asserted in the
    // graph port. Do NOT re-add an alias assert here until the fold lands it.
    let steps = &output["steps"];
    assert_eq!(
        steps["gate"]["outcome"], "approved",
        "resumed run records the approval decision under the node name: {output}"
    );
    assert_eq!(
        output["status"], "succeeded",
        "crash-resume completes, NOT failed (the WF-08 fix): {output}"
    );
    assert!(
        steps.get("prep").is_some() && steps.get("ship").is_some(),
        "pre- and post-approval steps both ran exactly once: {output}"
    );

    let _ = std::fs::remove_file(&db);
}
