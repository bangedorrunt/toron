// governed-by: ADR-0005
//! The durable driver against a real engine store and journal: restart resume,
//! park/wake across a restart, approval, unsupported shapes, and phase guards.
//!
//! Both durable halves are the production ones: the engine store and journal
//! over SQLite, and `SyncWorkflowState` for the model. A "restart" here is a
//! dropped engine and a fresh one over the same database, so the recovery rule
//! (model first, engine second) is proven against what ships.

use std::collections::BTreeMap;

use serde_json::json;
use toron_core::index::durable_engine::ToronEngineStore;
use toron_core::index::durable_workflow::SyncWorkflowState;
use toron_core::index::schema::{connect, migrate};
use toron_core::journal::{JournalDb, ToronJournal};
use toron_core::kernel::{DurableEngine, EngineMsg, OurEngine, RunPhase as EnginePhase};
use toron_workflow::engine::durable::{RunEffect, RunModel, RunPhase};
use toron_workflow::engine::durable_host::{DurableRun, HostError};
use toron_workflow::schema::{Action, Graph, Step, StepOnError, Workflow};

fn workflow(steps: Vec<Step>) -> Workflow {
    Workflow {
        name: "durable-host-test".into(),
        description: None,
        trigger: None,
        policy: None,
        guard: None,
        steps,
        graph: None,
        inputs: Vec::new(),
        crew: None,
        principles: Vec::new(),
    }
}

fn step(action: Action) -> Step {
    Step {
        name: None,
        guard: None,
        on_error: StepOnError::Continue,
        build_slot: false,
        action,
    }
}

fn verify(command: &str) -> Step {
    step(Action::Verify {
        command: command.into(),
    })
}

fn unique_db(tag: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "toron-durable-host-{tag}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_resumes_at_the_unsettled_step() {
    let path = unique_db("restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());

    // First process: start, settle step 1, hand back step 2, then die.
    let phase_one = {
        let pool = pool.clone();
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || -> Result<RunModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "host", 64), &handle),
            );
            let (mut run, effects) = DurableRun::open(
                engine,
                state,
                "run-restart",
                workflow(vec![verify("one"), verify("two")]),
                json!({"source": "test"}),
                1_000,
            )?;
            assert!(
                matches!(
                    effects.as_slice(),
                    [RunEffect::ExecuteStep { index: 0, .. }]
                ),
                "first effect runs step 1: {effects:?}"
            );
            let effects = run.step_ok(json!({"stdout": "one"}), 1_000)?;
            assert!(
                matches!(
                    effects.as_slice(),
                    [RunEffect::ExecuteStep { index: 1, .. }]
                ),
                "step 2 is next: {effects:?}"
            );
            Ok(run.model().clone())
        })
        .await
        .expect("phase one join")
        .expect("phase one")
    };
    assert_eq!(phase_one.step_index, 1);
    assert_eq!(phase_one.phase, RunPhase::Running);

    // Second process: a fresh engine and journal over the same database. The
    // settled step must not run again.
    let phase_two = {
        let pool = pool.clone();
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || -> Result<RunModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "host", 64), &handle),
            );
            let (mut run, effects) = DurableRun::open(
                engine,
                state,
                "run-restart",
                workflow(vec![verify("one"), verify("two")]),
                json!({"source": "test"}),
                2_000,
            )?;
            assert_eq!(run.model().step_index, 1, "resume at the next step");
            assert_eq!(
                run.model().outputs["step_1"],
                json!({"stdout": "one"}),
                "the settled step kept its output"
            );
            assert!(
                matches!(
                    effects.as_slice(),
                    [RunEffect::ExecuteStep { index: 1, .. }]
                ),
                "only step 2 re-issues: {effects:?}"
            );
            let effects = run.step_ok(json!({"stdout": "two"}), 2_000)?;
            assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
            assert_eq!(run.model().phase, RunPhase::Succeeded);
            assert_eq!(
                run.engine().query("run-restart").expect("query").phase,
                EnginePhase::Settled
            );
            Ok(run.model().clone())
        })
        .await
        .expect("phase two join")
        .expect("phase two")
    };
    assert_eq!(phase_two.step_index, 2);
    assert_eq!(phase_two.outputs.len(), 2);

    drop(state);
    drop(pool);
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delay_park_survives_restart_and_rewakes() {
    let path = unique_db("park");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let park_workflow = || workflow(vec![step(Action::Delay { secs: 5 })]);

    let until_ms = {
        let pool = pool.clone();
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || -> Result<u64, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "park", 64), &handle),
            );
            let (run, effects) =
                DurableRun::open(engine, state, "run-park", park_workflow(), json!({}), 1_000)?;
            let [RunEffect::ParkUntil { until_ms, .. }] = effects.as_slice() else {
                panic!("delay parks: {effects:?}");
            };
            assert_eq!(run.engine().parked().expect("parked").len(), 1);
            Ok(*until_ms)
        })
        .await
        .expect("phase one join")
        .expect("phase one")
    };
    assert_eq!(until_ms, 6_000);

    // Restart before the wake: the model re-parks to the same instant.
    let pool_after = pool.clone();
    let state_after = state.clone();
    let handle = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || -> Result<(), HostError> {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool_after.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool_after, "park", 64), &handle),
        );
        let (mut run, effects) = DurableRun::open(
            engine,
            state_after,
            "run-park",
            park_workflow(),
            json!({}),
            3_000,
        )?;
        assert!(
            matches!(
                effects.as_slice(),
                [RunEffect::ParkUntil { until_ms: parked, .. }] if *parked == until_ms
            ),
            "the restored run re-states its park, it does not wake: {effects:?}"
        );
        assert_eq!(
            run.engine().query("run-park").expect("query").until_ms,
            Some(until_ms)
        );

        let wakes = run.engine_mut().drain_due(until_ms).expect("drain");
        assert!(
            matches!(wakes.as_slice(), [EngineMsg::Woke { key, .. }] if key == "run-park"),
            "the wake loop finds it: {wakes:?}"
        );
        let effects = run.woke(until_ms)?;
        assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
        assert_eq!(run.model().phase, RunPhase::Succeeded);
        Ok(())
    })
    .await
    .expect("phase two join")
    .expect("phase two");

    drop(state);
    drop(pool);
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn approval_parks_with_a_watchdog_and_lands_the_decision() {
    let path = unique_db("approval");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || -> Result<(), HostError> {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool.clone(), "approval", 64), &handle),
        );
        let (mut run, effects) = DurableRun::open(
            engine,
            state,
            "run-approval",
            workflow(vec![step(Action::RequestApproval {
                message: "ship it?".into(),
            })]),
            json!({}),
            1_000,
        )?;
        assert!(
            matches!(effects.as_slice(), [RunEffect::AwaitApproval { step, .. }] if step == "step_1"),
            "approval awaits: {effects:?}"
        );
        assert_eq!(
            run.engine().query("run-approval").expect("query").until_ms,
            Some(1_000 + 86_400_000),
            "a watchdog park, so a forgotten approval resurfaces"
        );

        let effects = run.approve(json!({"outcome": "approved"}), 2_000)?;
        assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
        assert_eq!(run.model().outputs["step_1"]["outcome"], json!("approved"));
        assert_eq!(
            run.engine().query("run-approval").expect("query").phase,
            EnginePhase::Settled
        );
        Ok(())
    })
    .await
    .expect("join")
    .expect("approval");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_shapes_stay_on_the_old_engine() {
    let path = unique_db("unsupported");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    let graph_pool = pool.clone();
    let graph_state = state.clone();
    let graph_handle = handle.clone();
    let graph = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(graph_pool.clone(), &graph_handle),
            ToronJournal::spawn(JournalDb::new(graph_pool, "unsupported", 64), &graph_handle),
        );
        let mut shaped = workflow(Vec::new());
        shaped.graph = Some(Graph {
            nodes: BTreeMap::new(),
        });
        match DurableRun::open(engine, graph_state, "run-graph", shaped, json!({}), 0) {
            Ok(_) => panic!("graphs are not linear"),
            Err(e) => e,
        }
    })
    .await
    .expect("join");
    assert!(matches!(graph, HostError::Unsupported(_)), "{graph:?}");

    let branch_pool = pool.clone();
    let branch_state = state.clone();
    let branch_handle = handle.clone();
    let branch = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(branch_pool.clone(), &branch_handle),
            ToronJournal::spawn(
                JournalDb::new(branch_pool, "unsupported", 64),
                &branch_handle,
            ),
        );
        let shaped = workflow(vec![step(Action::Branch {
            if_guard: "true".into(),
            then: vec![verify("one")],
            else_steps: Vec::new(),
        })]);
        match DurableRun::open(engine, branch_state, "run-branch", shaped, json!({}), 0) {
            Ok(_) => panic!("branches are not linear"),
            Err(e) => e,
        }
    })
    .await
    .expect("join");
    assert!(matches!(branch, HostError::Unsupported(_)), "{branch:?}");

    // Refused before any engine or state row exists, so the host can hand the
    // workflow to the old engine without cleaning up after this one.
    let state_store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    assert!(
        state_store
            .load("run-graph")
            .await
            .expect("state")
            .is_none()
    );
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "unsupported", 64), &handle),
    );
    assert!(engine.query("run-graph").is_err());
    assert!(engine.query("run-branch").is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn out_of_phase_transitions_are_typed_errors() {
    let path = unique_db("phase");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || -> Result<(), HostError> {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool, "phase", 64), &handle),
        );
        let (mut run, _) = DurableRun::open(
            engine,
            state,
            "run-phase",
            workflow(vec![step(Action::RequestApproval {
                message: "wait".into(),
            })]),
            json!({}),
            0,
        )?;
        assert!(matches!(
            run.step_ok(json!({}), 0),
            Err(HostError::WrongPhase { .. })
        ));
        assert!(matches!(run.woke(0), Err(HostError::WrongPhase { .. })));
        run.cancel(0)?;
        assert!(matches!(run.cancel(0), Err(HostError::WrongPhase { .. })));
        Ok(())
    })
    .await
    .expect("join")
    .expect("phase guards");
}
