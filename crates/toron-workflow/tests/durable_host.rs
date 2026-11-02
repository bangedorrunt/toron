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
use toron_workflow::engine::ActionInvocation;
use toron_workflow::engine::durable::{
    GraphEffect, GraphModel, GraphPhase, NodeParkReason, NodeState, RunEffect, RunModel, RunPhase,
};
use toron_workflow::engine::durable_host::{ChildSeams, DurableGraphRun, DurableRun, HostError};
use toron_workflow::engine::leaf::LeafSeams;
use toron_workflow::engine::linear_step::{GraphNodeStep, run_graph_node_step};
use toron_workflow::schema::{Action, Graph, GraphNode, Step, StepOnError, Workflow};

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
            let effects = run.step_done(json!({"stdout": "one"}), 1_000)?;
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
            let effects = run.step_done(json!({"stdout": "two"}), 2_000)?;
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

    // `branch` is linear as of bd-durable-branch-hhgc; `parallel` still is not.
    let parallel_pool = pool.clone();
    let parallel_state = state.clone();
    let parallel_handle = handle.clone();
    let parallel = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(parallel_pool.clone(), &parallel_handle),
            ToronJournal::spawn(
                JournalDb::new(parallel_pool, "unsupported", 64),
                &parallel_handle,
            ),
        );
        let shaped = workflow(vec![step(Action::Parallel {
            over: "2".into(),
            each: Box::new(verify("one")),
        })]);
        match DurableRun::open(engine, parallel_state, "run-parallel", shaped, json!({}), 0) {
            Ok(_) => panic!("parallel is not linear"),
            Err(e) => e,
        }
    })
    .await
    .expect("join");
    assert!(
        matches!(parallel, HostError::Unsupported(_)),
        "{parallel:?}"
    );

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
    assert!(engine.query("run-parallel").is_err());
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
            run.step_done(json!({}), 0),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_park_already_behind_now_resumes_in_the_same_step() {
    let path = unique_db("zero-delay");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    tokio::task::spawn_blocking(move || -> Result<(), HostError> {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool, "zero-delay", 64), &handle),
        );
        let (run, effects) = DurableRun::open(
            engine,
            state,
            "run-zero",
            workflow(vec![step(Action::Delay { secs: 0 })]),
            json!({}),
            5_000,
        )?;
        // The run must not stay parked on a deadline the engine refuses to
        // schedule: the same call completes it.
        assert!(
            matches!(effects.last(), Some(RunEffect::Complete { .. })),
            "a zero delay finishes immediately: {effects:?}"
        );
        assert_eq!(run.model().phase, RunPhase::Succeeded);
        assert_eq!(
            run.engine().query("run-zero").expect("query").phase,
            EnginePhase::Settled
        );
        Ok(())
    })
    .await
    .expect("join")
    .expect("zero delay");
}

// ---------------------------------------------------------------------------
// Graph driver (bd-durable-graph-impl-r2x1)
// ---------------------------------------------------------------------------

type HostGraphRun = DurableGraphRun<ToronEngineStore, ToronJournal, SyncWorkflowState>;

fn graph_workflow(nodes: &[(&str, Action, &[&str])]) -> Workflow {
    let mut map = BTreeMap::new();
    for (name, action, needs) in nodes {
        map.insert(
            (*name).to_string(),
            GraphNode {
                action: action.clone(),
                needs: needs.iter().map(|n| (*n).to_string()).collect(),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
            },
        );
    }
    let mut wf = workflow(Vec::new());
    wf.graph = Some(Graph { nodes: map });
    wf
}

fn diamond() -> Workflow {
    graph_workflow(&[
        (
            "scout",
            Action::Verify {
                command: "scout".into(),
            },
            &[],
        ),
        (
            "plan",
            Action::Verify {
                command: "plan".into(),
            },
            &["scout"],
        ),
        (
            "fix_a",
            Action::Verify {
                command: "fix_a".into(),
            },
            &["plan"],
        ),
        (
            "fix_b",
            Action::Verify {
                command: "fix_b".into(),
            },
            &["plan"],
        ),
        (
            "ship",
            Action::Verify {
                command: "ship".into(),
            },
            &["fix_a", "fix_b"],
        ),
    ])
}

/// Seams with no Context anywhere: the graph host's own substrate. Every leaf
/// answers `{ok:true, node:<command>}`.
fn recording_seams() -> LeafSeams {
    LeafSeams {
        async_runner: Some(std::sync::Arc::new(|inv: &ActionInvocation| {
            let name = match &inv.action {
                Action::Verify { command } => command.clone(),
                other => other.type_name().to_string(),
            };
            Box::pin(async move { Ok(json!({ "ok": true, "node": name })) })
        })),
        ..LeafSeams::default()
    }
}

/// Feed one success back through the driver.
async fn node_ok(
    run: HostGraphRun,
    node: &str,
    value: serde_json::Value,
) -> (HostGraphRun, Vec<GraphEffect>) {
    let node = node.to_string();
    tokio::task::spawn_blocking(move || {
        let mut run = run;
        let effects = run.node_done(&node, value, 2_000)?;
        Ok::<_, HostError>((run, effects))
    })
    .await
    .expect("join")
    .expect("node_done")
}

/// Driver-level diamond through the real single-shot executor: every node
/// dispatches once, in wave order, and the terminal is the `run_graph` outputs
/// map.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_diamond_runs_each_node_once_on_the_driver() {
    let path = unique_db("graph-diamond");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let seams = recording_seams();
    let wf = diamond();

    let (mut run, mut effects) = {
        let pool = pool.clone();
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        let wf = wf.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "graph", 64), &handle),
            );
            DurableGraphRun::open(
                engine,
                state,
                "run-graph-diamond",
                wf,
                json!({"source": "test"}),
                1_000,
            )
        })
        .await
        .expect("join")
        .expect("open")
    };

    let mut dispatched: Vec<String> = Vec::new();
    let mut terminal = None;
    while terminal.is_none() {
        let mut batch = Vec::new();
        for effect in std::mem::take(&mut effects) {
            match effect {
                GraphEffect::ExecuteNode { node, inputs, .. } => batch.push((node, inputs)),
                GraphEffect::Complete { output } => terminal = Some(output),
                GraphEffect::Fail { fail } => panic!("unexpected failure: {fail:?}"),
                other => panic!("unexpected effect: {other:?}"),
            }
        }
        for (node, inputs) in batch {
            let node_def = wf
                .graph
                .as_ref()
                .and_then(|g| g.nodes.get(&node))
                .expect("node")
                .clone();
            let snapshot: serde_json::Map<String, serde_json::Value> = inputs.into_iter().collect();
            let value = run_graph_node_step(
                GraphNodeStep {
                    run_id: "run-graph-diamond",
                    workflow: &wf.name,
                    workflow_digest: "digest",
                    node_name: &node,
                    node: &node_def,
                    attempt: 1,
                    trigger: &json!({}),
                    inputs: &snapshot,
                    item: None,
                },
                &seams,
            )
            .await;
            dispatched.push(node.clone());
            let (back, more) = node_ok(run, &node, value).await;
            run = back;
            effects = more;
        }
    }

    assert_eq!(
        dispatched,
        ["scout", "plan", "fix_a", "fix_b", "ship"],
        "each node exactly once, wave by wave"
    );
    let output = terminal.expect("terminal");
    for name in ["scout", "plan", "fix_a", "fix_b", "ship"] {
        assert_eq!(output[name]["node"], json!(name), "output under {name}");
    }
    assert_eq!(run.model().phase, GraphPhase::Succeeded);
    assert_eq!(
        run.engine()
            .query("run-graph-diamond")
            .expect("query")
            .phase,
        EnginePhase::Settled
    );
}

/// A restart after wave 1 settles re-issues only the unsettled node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_restart_between_waves_runs_no_settled_node_twice() {
    let path = unique_db("graph-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let wf = diamond();

    let open = {
        let pool = pool.clone();
        let state = state.clone();
        let handle = tokio::runtime::Handle::current();
        let wf = wf.clone();
        move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool.clone(), "graph", 64), &handle),
            );
            DurableGraphRun::open(
                engine,
                state.clone(),
                "run-graph-restart",
                wf.clone(),
                json!({"source": "test"}),
                1_000,
            )
        }
    };

    let (run, effects) = tokio::task::spawn_blocking(open.clone())
        .await
        .expect("join")
        .expect("open");
    let [GraphEffect::ExecuteNode { node, .. }] = effects.as_slice() else {
        panic!("wave 1 dispatches one node: {effects:?}");
    };
    assert_eq!(node, "scout");
    let (run, effects) = node_ok(run, "scout", json!({"ok": true, "node": "scout"})).await;
    let [GraphEffect::ExecuteNode { node, .. }] = effects.as_slice() else {
        panic!("wave 2 dispatches plan: {effects:?}");
    };
    assert_eq!(node, "plan");

    // Drop the driver mid-wave: `plan` was issued but never settled.
    drop(run);

    let (run, effects) = tokio::task::spawn_blocking(open)
        .await
        .expect("join")
        .expect("reopen");
    assert_eq!(
        run.model().outputs["scout"]["node"],
        json!("scout"),
        "the settled node kept its output"
    );
    let resumed: Vec<&str> = effects
        .iter()
        .filter_map(|e| match e {
            GraphEffect::ExecuteNode { node, .. } => Some(node.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(resumed, ["plan"], "only the unsettled node re-issues");
}

/// Whole-graph refusal leaves no state row and no engine row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_refused_shape_leaves_no_row() {
    let path = unique_db("graph-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    for (run_id, action, label) in [
        (
            "run-graph-parallel",
            Action::Parallel {
                over: "{{trigger.items}}".into(),
                each: Box::new(step(Action::Verify {
                    command: "x".into(),
                })),
            },
            "parallel",
        ),
        (
            "run-graph-spawn",
            Action::Spawn {
                profile: "coder".into(),
                harness: None,
                workflow: None,
                await_: false,
                no_claim_timeout: 600,
                context: None,
            },
            "spawn",
        ),
    ] {
        let mut wf = graph_workflow(&[(
            "fine",
            Action::Verify {
                command: "fine".into(),
            },
            &[],
        )]);
        if let Some(graph) = wf.graph.as_mut() {
            graph.nodes.insert(
                "bad".to_string(),
                GraphNode {
                    action,
                    needs: vec!["fine".into()],
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                },
            );
        }
        let pool = pool.clone();
        let state = state.clone();
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "graph", 64), &handle),
            );
            DurableGraphRun::open(engine, state, rid, wf, json!({}), 0)
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: a {label} node must refuse the whole graph");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let state_store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in ["run-graph-parallel", "run-graph-spawn"] {
        assert!(
            state_store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool.clone(), "graph", 64), &handle),
    );
    assert!(engine.query("run-graph-parallel").is_err());
    assert!(engine.query("run-graph-spawn").is_err());
}

/// Captain review fix 1: one wave holding a short delay AND an approval parks
/// the engine once, at the EARLIEST deadline. `park_until` overwrites, so a
/// per-effect park would leave the run waking in a day.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_delay_and_approval_park_at_the_earliest_deadline() {
    let path = unique_db("graph-park-one");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let wf = graph_workflow(&[
        ("quick", Action::Delay { secs: 5 }, &[]),
        (
            "gate",
            Action::RequestApproval {
                message: "ship?".into(),
            },
            &[],
        ),
    ]);
    let handle = tokio::runtime::Handle::current();

    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool.clone(), "graph", 64), &handle),
        );
        DurableGraphRun::open(engine, state, "run-graph-park", wf, json!({}), 1_000)
    })
    .await
    .expect("join")
    .expect("open");

    assert_eq!(effects.len(), 2, "a delay and an approval: {effects:?}");
    assert_eq!(
        run.engine()
            .query("run-graph-park")
            .expect("query")
            .until_ms,
        Some(6_000),
        "the 5s delay wins over the 1-day approval watchdog"
    );
}

/// Captain review fix 2: a run that already failed ignores late node results,
/// so a sibling's success can never land an output or re-schedule a wave over
/// the failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graph_late_node_done_after_a_terminal_is_ignored() {
    let path = unique_db("graph-terminal-guard");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let mut wf = graph_workflow(&[
        (
            "aa",
            Action::Verify {
                command: "aa".into(),
            },
            &[],
        ),
        (
            "zz",
            Action::Verify {
                command: "zz".into(),
            },
            &[],
        ),
    ]);
    if let Some(graph) = wf.graph.as_mut() {
        for node in graph.nodes.values_mut() {
            node.on_error = StepOnError::FailRun;
        }
    }
    let handle = tokio::runtime::Handle::current();

    let (run, effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool.clone(), "graph", 64), &handle),
        );
        DurableGraphRun::open(engine, state, "run-graph-guard", wf, json!({}), 1_000)
    })
    .await
    .expect("join")
    .expect("open");
    assert_eq!(effects.len(), 2, "both nodes of wave 1 are dispatched");

    // A non-retryable failure fails the whole run while `zz` is still out.
    let (run, effects) = {
        let mut run = run;
        tokio::task::spawn_blocking(move || {
            let effects = run.node_done(
                "aa",
                json!({"ok": false, "error": "boom", "kind": "invalid_output"}),
                2_000,
            )?;
            Ok::<_, HostError>((run, effects))
        })
        .await
        .expect("join")
        .expect("node_done")
    };
    assert!(matches!(effects.as_slice(), [GraphEffect::Fail { .. }]));
    assert_eq!(
        run.model().phase,
        GraphPhase::Failed {
            error: "boom".into()
        }
    );

    // The late sibling result changes nothing.
    let (run, effects) = {
        let mut run = run;
        tokio::task::spawn_blocking(move || {
            let effects = run.node_done("zz", json!({"ok": true, "node": "zz"}), 3_000)?;
            Ok::<_, HostError>((run, effects))
        })
        .await
        .expect("join")
        .expect("late node_done")
    };
    assert!(
        effects.is_empty(),
        "a terminal run emits nothing: {effects:?}"
    );
    assert_eq!(
        run.model().phase,
        GraphPhase::Failed {
            error: "boom".into()
        },
        "the failure stands"
    );
    assert!(
        !run.model().outputs.contains_key("zz"),
        "a late success never lands an output: {:?}",
        run.model().outputs
    );
    assert!(
        !run.model().outputs.contains_key("aa"),
        "a failed node lands nothing either: {:?}",
        run.model().outputs
    );
    assert_eq!(
        run.engine().query("run-graph-guard").expect("query").phase,
        EnginePhase::Failed
    );
}

// ---------------------------------------------------------------------------
// Spawn admission and the launch seam (bd-spawn-launcher-flip-d8mv)
// ---------------------------------------------------------------------------

fn spawn_action(await_: bool, no_claim_timeout: u64) -> Action {
    Action::Spawn {
        profile: "coder".into(),
        harness: None,
        workflow: None,
        await_,
        no_claim_timeout,
        context: None,
    }
}

fn spawn_workflow(await_: bool, no_claim_timeout: u64) -> Workflow {
    workflow(vec![Step {
        name: Some("spawn_it".into()),
        guard: None,
        on_error: StepOnError::Continue,
        build_slot: false,
        action: spawn_action(await_, no_claim_timeout),
    }])
}

/// The seams a host with (or without) a launcher supplies. `DurableRun` never
/// calls the launcher; it only decides admission.
fn spawn_seams(launcher: bool) -> toron_workflow::engine::spawn::SpawnSeams {
    let launcher: Option<toron_workflow::engine::spawn::SpawnLauncher> = launcher.then(|| {
        std::sync::Arc::new(
            |spec: toron_workflow::engine::spawn::SpawnSpec| -> std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            > {
                Box::pin(async move { Ok(json!({"pane": "w1:p9", "spawn_key": spec.spawn_key})) })
            },
        ) as toron_workflow::engine::spawn::SpawnLauncher
    });
    toron_workflow::engine::spawn::SpawnSeams {
        resolver: Some(std::sync::Arc::new(|name: &str| {
            (name == "coder").then(|| json!({"agent": "coder"}))
        })),
        launcher,
    }
}

/// A direct caller with no launcher is refused BEFORE any state or engine row,
/// not parked on a launch nothing can perform.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_without_a_launcher_is_refused_before_any_row() {
    let path = unique_db("spawn-no-launcher");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    let refused = {
        let pool = pool.clone();
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "spawn", 64), &handle),
            );
            DurableRun::open_with_seams(
                engine,
                state,
                "run-spawn-refused",
                spawn_workflow(true, 30),
                json!({}),
                1_000,
                &spawn_seams(false),
            )
        })
        .await
        .expect("join")
    };
    let Err(err) = refused else {
        panic!("a host with no launcher must refuse a spawn");
    };
    let HostError::Unsupported(reason) = err else {
        panic!("wrong refusal: {err:?}");
    };
    assert!(reason.contains("launcher"), "{reason}");

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    assert!(
        store
            .load("run-spawn-refused")
            .await
            .expect("state")
            .is_none(),
        "no state row"
    );
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "spawn", 64), &handle),
    );
    assert!(engine.query("run-spawn-refused").is_err(), "no engine row");
}

/// The toron half of the restart witness: the persisted `AwaitingSpawn` model
/// re-emits `StartSpawn` with the SAME `spawn_key`, so a consumer's durable
/// claim can adopt instead of starting a second child.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_park_reemits_the_same_key_after_restart() {
    let path = unique_db("spawn-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let state = SyncWorkflowState::spawn(pool.clone(), &tokio::runtime::Handle::current());
    let handle = tokio::runtime::Handle::current();

    let first = {
        let pool = pool.clone();
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "spawn", 64), &handle),
            );
            DurableRun::open_with_seams(
                engine,
                state,
                "run-spawn-restart",
                spawn_workflow(true, 600),
                json!({}),
                1_000,
                &spawn_seams(true),
            )
        })
        .await
        .expect("join")
        .expect("first open")
    };
    let keys: Vec<String> = first
        .1
        .iter()
        .filter_map(|e| match e {
            RunEffect::StartSpawn { spec, .. } => Some(spec.spawn_key.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(keys, ["run-spawn-restart:spawn_it:1"]);
    drop(first);

    // A fresh engine + state handle over the same database: the restart.
    let (mut run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "spawn", 64), &handle),
            );
            DurableRun::open_with_seams(
                engine,
                state,
                "run-spawn-restart",
                spawn_workflow(true, 600),
                json!({}),
                2_000,
                &spawn_seams(true),
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    let [RunEffect::StartSpawn { spec, .. }] = effects.as_slice() else {
        panic!("the resumed run re-emits exactly one StartSpawn: {effects:?}");
    };
    assert_eq!(
        spec.spawn_key, "run-spawn-restart:spawn_it:1",
        "the same key, so the consumer's claim can adopt"
    );

    let effects = run
        .spawn_settled("spawn_it", json!({"pane": "w1:p9"}), 3_000)
        .expect("settle");
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(
        run.model().outputs["spawn_it"],
        json!({
            "ok": true,
            "spawn_key": "run-spawn-restart:spawn_it:1",
            "result": {"pane": "w1:p9"},
        })
    );
}

// ---------------------------------------------------------------------------
// Branch in the linear plan (bd-durable-branch-hhgc)
// ---------------------------------------------------------------------------

fn dm(tag: &str) -> Action {
    Action::SendDm {
        to: "housekeeper".into(),
        body: format!("arm:{tag}"),
        project: None,
    }
}

fn named(name: &str, action: Action, on_error: StepOnError) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action,
    }
}

fn branch(guard: &str, then: Vec<Step>, else_steps: Vec<Step>, on_error: StepOnError) -> Step {
    Step {
        name: Some("choose".into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Branch {
            if_guard: guard.into(),
            then,
            else_steps,
        },
    }
}

fn branch_workflow(guard: &str) -> Workflow {
    workflow(vec![branch(
        guard,
        vec![
            named("yes", dm("yes"), StepOnError::Continue),
            named("yes-2", dm("yes-2"), StepOnError::Continue),
        ],
        vec![named("no", dm("no"), StepOnError::Continue)],
        StepOnError::Continue,
    )])
}

/// A restart mid-arm resumes the SAME arm from the store: the marker is not
/// re-evaluated, and the unselected arm never dispatches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn branch_restart_resumes_the_same_arm() {
    let path = unique_db("branch-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();
    let trigger = json!({"gate": true});

    let phase_one = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let trigger = trigger.clone();
        tokio::task::spawn_blocking(move || -> Result<RunModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "branch", 64), &handle),
            );
            let (mut run, effects) = DurableRun::open_with_seams(
                engine,
                state,
                "run-branch",
                branch_workflow("{{trigger.gate}} == true"),
                trigger,
                1_000,
                &spawn_seams(false),
            )?;
            assert!(
                matches!(effects.as_slice(), [RunEffect::ExecuteStep { .. }]),
                "the then arm's first step runs: {effects:?}"
            );
            let effects = run.step_done(json!({"ok": true}), 2_000)?;
            let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
                panic!("the then arm's second step is next: {effects:?}");
            };
            assert_eq!(step.name.as_deref(), Some("yes-2"));
            Ok(run.model().clone())
        })
        .await
        .expect("join")
        .expect("phase one")
    };
    assert_eq!(
        phase_one.outputs.get("choose"),
        None,
        "the branch value lands only when the arm ends"
    );
    assert!(!phase_one.outputs.contains_key("no"));

    // The restart: a fresh engine and state handle over the same database.
    let (run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "branch", 64), &handle),
            );
            DurableRun::open_with_seams(
                engine,
                state,
                "run-branch",
                branch_workflow("{{trigger.gate}} == true"),
                json!({"gate": false}),
                3_000,
                &spawn_seams(false),
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("resume re-issues the same arm step: {effects:?}");
    };
    assert_eq!(
        step.name.as_deref(),
        Some("yes-2"),
        "the persisted arm cursor wins, never the marker"
    );

    let (mut run, effects) = (run, effects);
    let _ = effects;
    let effects = run.step_done(json!({"ok": true}), 4_000).expect("settle");
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(run.model().outputs["choose"], json!({"branch": "then"}));
    assert!(run.model().outputs.contains_key("yes"));
    assert!(run.model().outputs.contains_key("yes-2"));
    assert!(
        !run.model().outputs.contains_key("no"),
        "the unselected arm never dispatched: {:?}",
        run.model().outputs
    );
}

/// Impure guards and nested control flow refuse the WHOLE workflow before any
/// state or engine row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn branch_refusals_leave_no_row() {
    let path = unique_db("branch-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let nested = workflow(vec![branch(
        "{{trigger.gate}} == true",
        vec![step(Action::Delay { secs: 1 })],
        vec![],
        StepOnError::Continue,
    )]);
    let cases = [
        (
            "run-branch-impure",
            branch_workflow("1 + 1 == 2"),
            "not pure",
        ),
        ("run-branch-nested", nested, "inside an arm"),
    ];
    for (run_id, wf, label) in cases {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "branch", 64), &handle),
            );
            DurableRun::open_with_seams(
                engine,
                state,
                rid,
                wf,
                json!({"gate": true}),
                0,
                &spawn_seams(false),
            )
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in ["run-branch-impure", "run-branch-nested"] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "branch", 64), &handle),
    );
    assert!(engine.query("run-branch-impure").is_err());
    assert!(engine.query("run-branch-nested").is_err());
}

// ---------------------------------------------------------------------------
// Parallel fan-out lowered onto the graph (bd-durable-parallel-53um)
// ---------------------------------------------------------------------------

fn parallel_step(name: &str, over: &str, each: Step, on_error: StepOnError) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Parallel {
            over: over.into(),
            each: Box::new(each),
        },
    }
}

fn fanout_workflow(over: &str) -> Workflow {
    workflow(vec![
        parallel_step(
            "fan",
            over,
            Step {
                name: Some("copy".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: dm("copy"),
            },
            StepOnError::Continue,
        ),
        named("after", dm("after"), StepOnError::Continue),
    ])
}

fn executed_nodes(effects: &[GraphEffect]) -> Vec<&str> {
    effects
        .iter()
        .filter_map(|e| match e {
            GraphEffect::ExecuteNode { node, .. } => Some(node.as_str()),
            _ => None,
        })
        .collect()
}

/// A restart mid-wave re-emits only the pending copy, then the join collects
/// and the next step runs — through the store, not just the fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_restart_reruns_only_pending_copies() {
    let path = unique_db("parallel-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let phase_one = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || -> Result<GraphModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "parallel", 64), &handle),
            );
            let (mut run, effects) = DurableGraphRun::open(
                engine,
                state,
                "run-parallel",
                fanout_workflow("2"),
                json!({}),
                1_000,
            )?;
            assert_eq!(executed_nodes(&effects), ["fan:0", "fan:1"]);
            let effects = run.node_done("fan:0", json!({"ok": true, "copy": 0}), 2_000)?;
            assert!(effects.is_empty(), "still mid-wave: {effects:?}");
            Ok(run.model().clone())
        })
        .await
        .expect("join")
        .expect("phase one")
    };
    assert_eq!(phase_one.outputs["fan:0"], json!({"ok": true, "copy": 0}));
    assert!(
        !phase_one.outputs.contains_key("fan"),
        "no join on a partial wave"
    );

    let (mut run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "parallel", 64), &handle),
            );
            DurableGraphRun::open(
                engine,
                state,
                "run-parallel",
                fanout_workflow("2"),
                json!({}),
                3_000,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    assert_eq!(
        executed_nodes(&effects),
        ["fan:1"],
        "only the pending copy re-issues"
    );

    let effects = run
        .node_done("fan:1", json!({"ok": true, "copy": 1}), 4_000)
        .expect("settle the last copy");
    assert_eq!(
        executed_nodes(&effects),
        ["after"],
        "the join settles inline and the next step dispatches"
    );
    assert_eq!(
        run.model().outputs["fan"],
        json!([{"ok": true, "copy": 0}, {"ok": true, "copy": 1}])
    );
    let effects = run
        .node_done("after", json!({"ok": true, "sent": true}), 5_000)
        .expect("settle the last step");
    let [GraphEffect::Complete { output }] = effects.as_slice() else {
        panic!("the run completes: {effects:?}");
    };
    assert_eq!(
        output["fan"],
        json!([{"ok": true, "copy": 0}, {"ok": true, "copy": 1}])
    );
    assert_eq!(run.model().phase, GraphPhase::Succeeded);
}

/// Whole-workflow refusals leave no state and no engine row: the spawn/branch
/// mix and a count that does not resolve at open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_refusals_leave_no_row() {
    let path = unique_db("parallel-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let mixed = workflow(vec![
        named("spawn_it", spawn_action(true, 600), StepOnError::Continue),
        parallel_step(
            "fan",
            "2",
            Step {
                name: Some("copy".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: dm("copy"),
            },
            StepOnError::Continue,
        ),
    ]);
    let cases = [
        ("run-parallel-mixed", mixed, "mixes graph-lowered steps"),
        (
            "run-parallel-count",
            fanout_workflow("{{inputs.n}}"),
            "must resolve to a positive count",
        ),
    ];
    for (run_id, wf, label) in cases {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "parallel", 64), &handle),
            );
            DurableGraphRun::open(engine, state, rid, wf, json!({}), 0)
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in ["run-parallel-mixed", "run-parallel-count"] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "parallel", 64), &handle),
    );
    assert!(engine.query("run-parallel-mixed").is_err());
    assert!(engine.query("run-parallel-count").is_err());
}

// ---------------------------------------------------------------------------
// Foreach on the shared fan-out path (bd-durable-foreach-jpgx)
// ---------------------------------------------------------------------------

fn foreach_step(name: &str, over: &str, each: Step, on_error: StepOnError) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Foreach {
            over: over.into(),
            each: Box::new(each),
        },
    }
}

fn foreach_workflow(over: &str) -> Workflow {
    workflow(vec![
        foreach_step(
            "fan",
            over,
            Step {
                name: Some("copy".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: dm("copy"),
            },
            StepOnError::Continue,
        ),
        named("after", dm("after"), StepOnError::Continue),
    ])
}

fn item_carrying(effects: &[GraphEffect]) -> Vec<(String, Option<serde_json::Value>)> {
    effects
        .iter()
        .filter_map(|e| match e {
            GraphEffect::ExecuteNode { node, item, .. } => Some((node.clone(), item.clone())),
            _ => None,
        })
        .collect()
}

/// Through the store: the list expands, a restart keeps the settled item and
/// its per-copy data, and only the pending item re-issues.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreach_restart_keeps_settled_items() {
    let path = unique_db("foreach-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();
    let trigger = json!({"inputs": {"items": ["a", "b"]}});

    let phase_one = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let trigger = trigger.clone();
        tokio::task::spawn_blocking(move || -> Result<GraphModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "foreach", 64), &handle),
            );
            let (mut run, effects) = DurableGraphRun::open(
                engine,
                state,
                "run-foreach",
                foreach_workflow("{{inputs.items}}"),
                trigger,
                1_000,
            )?;
            assert_eq!(
                item_carrying(&effects),
                [
                    ("fan:0".to_string(), Some(json!("a"))),
                    ("fan:1".to_string(), Some(json!("b"))),
                ]
            );
            let effects = run.node_done("fan:0", json!({"ok": true, "i": 0}), 2_000)?;
            assert!(effects.is_empty(), "still mid-wave: {effects:?}");
            Ok(run.model().clone())
        })
        .await
        .expect("join")
        .expect("phase one")
    };
    assert_eq!(phase_one.outputs["fan:0"], json!({"ok": true, "i": 0}));
    assert_eq!(phase_one.items["fan:1"], json!("b"));

    let (mut run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "foreach", 64), &handle),
            );
            DurableGraphRun::open(
                engine,
                state,
                "run-foreach",
                foreach_workflow("{{inputs.items}}"),
                json!({}),
                3_000,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    assert_eq!(
        item_carrying(&effects),
        [("fan:1".to_string(), Some(json!("b")))],
        "only the pending item re-issues, with its item"
    );

    let effects = run
        .node_done("fan:1", json!({"ok": true, "i": 1}), 4_000)
        .expect("settle the last item");
    assert_eq!(executed_nodes(&effects), ["after"]);
    assert_eq!(
        run.model().outputs["fan"],
        json!([{"ok": true, "i": 0}, {"ok": true, "i": 1}])
    );
    let effects = run
        .node_done("after", json!({"ok": true, "sent": true}), 5_000)
        .expect("settle the last step");
    let [GraphEffect::Complete { output }] = effects.as_slice() else {
        panic!("the run completes: {effects:?}");
    };
    assert_eq!(
        output["fan"],
        json!([{"ok": true, "i": 0}, {"ok": true, "i": 1}])
    );
    assert_eq!(run.model().phase, GraphPhase::Succeeded);
}

/// A list that cannot be materialized refuses whole: no state row, no engine
/// row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn foreach_unmaterializable_iterator_refuses_without_row() {
    let path = unique_db("foreach-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let cases = [
        (
            "run-foreach-number",
            foreach_workflow("42"),
            json!({}),
            "must resolve to a list",
        ),
        (
            "run-foreach-missing",
            foreach_workflow("{{inputs.items}}"),
            json!({}),
            "does not resolve at open",
        ),
    ];
    for (run_id, wf, trigger, label) in cases {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "foreach", 64), &handle),
            );
            DurableGraphRun::open(engine, state, rid, wf, trigger, 0)
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in ["run-foreach-number", "run-foreach-missing"] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "foreach", 64), &handle),
    );
    assert!(engine.query("run-foreach-number").is_err());
    assert!(engine.query("run-foreach-missing").is_err());
}

// ---------------------------------------------------------------------------
// Retry policy on graph nodes (bd-durable-retry-policy-rrck)
// ---------------------------------------------------------------------------

fn retry_step(name: &str, inner: Step, max_attempts: u32, on_error: StepOnError) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Retry {
            max_attempts,
            backoff_secs: 0,
            step: Box::new(inner),
        },
    }
}

fn retry_workflow(max_attempts: u32) -> Workflow {
    workflow(vec![retry_step(
        "flaky",
        Step {
            name: Some("call".into()),
            guard: None,
            on_error: StepOnError::Continue,
            build_slot: false,
            action: dm("call"),
        },
        max_attempts,
        StepOnError::FailRun,
    )])
}

/// Through the store: the backoff park and the attempt survive a restart, and
/// the wake re-dispatches the SAME attempt identity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_backoff_park_is_restart_safe() {
    let path = unique_db("retry-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let (parked, until_ms) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || -> Result<(GraphModel, u64), HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "retry", 64), &handle),
            );
            let (mut run, effects) = DurableGraphRun::open(
                engine,
                state,
                "run-retry",
                retry_workflow(3),
                json!({}),
                1_000,
            )?;
            assert_eq!(executed_nodes(&effects), ["flaky"]);
            let effects = run.node_done(
                "flaky",
                json!({"ok": false, "kind": "timeout", "error": "late"}),
                2_000,
            )?;
            let [
                GraphEffect::ParkUntil {
                    reason: NodeParkReason::Retry,
                    until_ms,
                    ..
                },
            ] = effects.as_slice()
            else {
                panic!("attempt 1 parks: {effects:?}");
            };
            Ok((run.model().clone(), *until_ms))
        })
        .await
        .expect("join")
        .expect("phase one")
    };
    assert!(matches!(
        parked.node_state["flaky"],
        NodeState::Parked { attempt: 2, .. }
    ));

    let (mut run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "retry", 64), &handle),
            );
            // Reopen BEFORE the backoff deadline, so the run is still parked.
            DurableGraphRun::open(
                engine,
                state,
                "run-retry",
                retry_workflow(3),
                json!({}),
                2_500,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    let [
        GraphEffect::ParkUntil {
            reason: NodeParkReason::Retry,
            until_ms: resumed_until,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("resume re-issues the park, not a dispatch: {effects:?}");
    };
    assert_eq!(*resumed_until, until_ms);

    let effects = run.woke(until_ms).expect("wake");
    let [
        GraphEffect::ExecuteNode {
            node, attempt: 2, ..
        },
    ] = effects.as_slice()
    else {
        panic!("the wake re-dispatches attempt 2: {effects:?}");
    };
    assert_eq!(node, "flaky");

    let effects = run
        .node_done("flaky", json!({"ok": true, "done": true}), until_ms + 1_000)
        .expect("settle");
    assert!(matches!(effects.as_slice(), [GraphEffect::Complete { .. }]));
    assert_eq!(run.model().phase, GraphPhase::Succeeded);
}

/// An unparseable policy refuses whole: no state row, no engine row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retry_unparseable_policy_refuses_without_row() {
    let path = unique_db("retry-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    for (run_id, max_attempts, label) in [
        ("run-retry-zero", 0u32, "at least 1"),
        ("run-retry-cap", 11u32, "exceeds the bounded cap"),
    ] {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "retry", 64), &handle),
            );
            DurableGraphRun::open(
                engine,
                state,
                rid,
                retry_workflow(max_attempts),
                json!({}),
                0,
            )
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in ["run-retry-zero", "run-retry-cap"] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "retry", 64), &handle),
    );
    assert!(engine.query("run-retry-zero").is_err());
    assert!(engine.query("run-retry-cap").is_err());
}

// ---------------------------------------------------------------------------
// call_workflow as a child run (bd-durable-call-workflow-mg6y)
// ---------------------------------------------------------------------------

fn named_workflow(name: &str, steps: Vec<Step>) -> Workflow {
    let mut wf = workflow(steps);
    wf.name = name.into();
    wf
}

fn callee_workflow() -> Workflow {
    named_workflow(
        "child",
        vec![step(Action::SendDm {
            to: "pi".into(),
            body: "child runs".into(),
            project: None,
        })],
    )
}

fn calling_workflow(name: &str, callee: &str, on_error: StepOnError) -> Workflow {
    named_workflow(
        name,
        vec![Step {
            name: Some("call".into()),
            guard: None,
            on_error,
            build_slot: false,
            action: Action::CallWorkflow {
                workflow: callee.into(),
                inputs: json!({"goal": "ship"}),
            },
        }],
    )
}

fn child_seams(child: Workflow) -> ChildSeams {
    ChildSeams {
        resolver: Some(std::sync::Arc::new(move |name: &str| {
            (name == "child").then(|| child.clone())
        })),
    }
}

/// The child runs exactly once and its terminal joins the parent node.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_workflow_child_runs_once_and_joins() {
    let path = unique_db("child-join");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();
    let seams = child_seams(callee_workflow());

    let (mut parent, child_id, child_trigger) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let seams = seams.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            let (parent, effects) = DurableGraphRun::open_with_children(
                engine,
                state,
                "run-parent",
                calling_workflow("parent", "child", StepOnError::Continue),
                json!({}),
                0,
                &seams,
            )
            .expect("parent opens");
            let [
                GraphEffect::ChildRun {
                    child_run_id,
                    trigger,
                    ..
                },
            ] = effects.as_slice()
            else {
                panic!("the call opens one child: {effects:?}");
            };
            (parent, child_run_id.clone(), trigger.clone())
        })
        .await
        .expect("join")
    };
    assert_eq!(child_id, "run-parent:call:1");

    // Open the child idempotently and drive it to its terminal.
    let child_output = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            let (mut child, effects) = DurableRun::open(
                engine,
                state,
                "run-parent:call:1",
                callee_workflow(),
                child_trigger,
                1,
            )
            .expect("child opens");
            let [RunEffect::ExecuteStep { .. }] = effects.as_slice() else {
                panic!("the child dispatches one step: {effects:?}");
            };
            let effects = child
                .step_done(json!({"ok": true, "done": 1}), 2)
                .expect("child settles");
            let [RunEffect::Complete { output }] = effects.as_slice() else {
                panic!("the child completes: {effects:?}");
            };
            output.clone()
        })
        .await
        .expect("join")
    };

    let effects = parent
        .child_succeeded("call", child_output.clone(), 3)
        .expect("the terminal joins");
    let [
        GraphEffect::ChildJoin { node, .. },
        GraphEffect::Complete { .. },
    ] = effects.as_slice()
    else {
        panic!("join then complete: {effects:?}");
    };
    assert_eq!(node, "call");
    assert_eq!(parent.model().outputs["call"], child_output);
    assert_eq!(parent.model().phase, GraphPhase::Succeeded);

    // Re-opening the completed child re-attaches: no effects, no second run.
    let reused = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            DurableRun::open(
                engine,
                state,
                "run-parent:call:1",
                callee_workflow(),
                json!({}),
                4,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    assert!(
        reused.1.is_empty(),
        "a terminal child emits nothing: {:?}",
        reused.1
    );
    assert_eq!(reused.0.model().phase, RunPhase::Succeeded);
}

/// A restarted parent re-attaches to the SAME child id and a completed child
/// is reused, never re-opened as a second run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_workflow_restart_reuses_completed_child() {
    let path = unique_db("child-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();
    let seams = child_seams(callee_workflow());

    // Phase one: parent emits the child, the child completes, the parent dies
    // before the terminal reaches it.
    let (child_id, child_trigger) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let seams = seams.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            let (_parent, effects) = DurableGraphRun::open_with_children(
                engine,
                state,
                "run-parent",
                calling_workflow("parent", "child", StepOnError::Continue),
                json!({}),
                0,
                &seams,
            )
            .expect("parent opens");
            let [
                GraphEffect::ChildRun {
                    child_run_id,
                    trigger,
                    ..
                },
            ] = effects.as_slice()
            else {
                panic!("one child: {effects:?}");
            };
            (child_run_id.clone(), trigger.clone())
        })
        .await
        .expect("join")
    };
    {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            let (mut child, _) = DurableRun::open(
                engine,
                state,
                "run-parent:call:1",
                callee_workflow(),
                child_trigger,
                1,
            )
            .expect("child opens");
            child
                .step_done(json!({"ok": true, "done": 1}), 2)
                .expect("child settles");
        })
        .await
        .expect("join");
    }

    // Phase two: the parent resumes and re-attaches to the same child id.
    let (mut parent, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let seams = seams.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            DurableGraphRun::open_with_children(
                engine,
                state,
                "run-parent",
                calling_workflow("parent", "child", StepOnError::Continue),
                json!({}),
                10,
                &seams,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    let [
        GraphEffect::ChildRun {
            child_run_id: resumed_id,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("the restarted parent re-attaches: {effects:?}");
    };
    assert_eq!(*resumed_id, child_id, "the same child, never a second one");

    // Re-attaching to the completed child yields no effects.
    let reused = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            DurableRun::open(
                engine,
                state,
                "run-parent:call:1",
                callee_workflow(),
                json!({}),
                11,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    assert!(reused.1.is_empty(), "a completed child is reused");
    let child_output = json!({"status": "succeeded", "steps": reused.0.model().outputs});

    let effects = parent
        .child_succeeded("call", child_output.clone(), 12)
        .expect("join");
    assert!(
        matches!(
            effects.as_slice(),
            [GraphEffect::ChildJoin { .. }, GraphEffect::Complete { .. }]
        ),
        "{effects:?}"
    );
    assert_eq!(parent.model().outputs["call"], child_output);
}

/// A failed child's FULL bundle reaches the parent node's `on_error`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_workflow_child_failure_carries_bundle_to_parent_on_error() {
    let path = unique_db("child-fail");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();
    let bundle = json!({
        "ok": false,
        "kind": "terminal",
        "error": "child exploded",
        "leaf_preview": "{\"ok\":false}",
    });

    for (run_id, on_error, expected_fail) in [
        ("run-call-continue", StepOnError::Continue, false),
        ("run-call-fail", StepOnError::FailRun, true),
    ] {
        let seams = child_seams(callee_workflow());
        let (mut parent, _) = {
            let pool = pool.clone();
            let state = SyncWorkflowState::spawn(pool.clone(), &handle);
            let handle = handle.clone();
            let seams = seams.clone();
            let run_id = run_id.to_string();
            tokio::task::spawn_blocking(move || {
                let engine = OurEngine::with_journal(
                    ToronEngineStore::spawn(pool.clone(), &handle),
                    ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
                );
                DurableGraphRun::open_with_children(
                    engine,
                    state,
                    run_id,
                    calling_workflow("parent", "child", on_error),
                    json!({}),
                    0,
                    &seams,
                )
            })
            .await
            .expect("join")
            .expect("parent opens")
        };
        let effects = parent
            .child_failed("call", bundle.clone(), 2)
            .expect("terminal");
        if expected_fail {
            let [GraphEffect::ChildJoin { .. }, GraphEffect::Fail { fail }] = effects.as_slice()
            else {
                panic!("{run_id}: fail_run fails: {effects:?}");
            };
            assert_eq!(fail.node, "call");
            assert_eq!(fail.kind, "terminal");
            assert_eq!(fail.error, "child exploded");
        } else {
            let [GraphEffect::ChildJoin { .. }, GraphEffect::Complete { .. }] = effects.as_slice()
            else {
                panic!("{run_id}: continue completes: {effects:?}");
            };
            assert_eq!(parent.model().outputs["call"], bundle);
        }
    }
}

/// §Recursive, unknown, unsequencable, and resolver-less callees refuse the
/// parent before any state or engine row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn call_workflow_refusals_leave_no_row() {
    let path = unique_db("child-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let self_call = calling_workflow("self", "self", StepOnError::Continue);
    let cyclic_b = calling_workflow("b", "a", StepOnError::Continue);
    let cyclic_a = calling_workflow("a", "b", StepOnError::Continue);
    let unsequencable_child = named_workflow(
        "child",
        vec![step(Action::CallWebhook {
            url: "http://127.0.0.1:9/x".into(),
            method: None,
            body: None,
        })],
    );

    let cases: Vec<(&str, Workflow, ChildSeams, &str)> = vec![
        (
            "run-recursive-self",
            self_call.clone(),
            ChildSeams {
                resolver: Some(std::sync::Arc::new(move |name: &str| {
                    (name == "self").then(|| self_call.clone())
                })),
            },
            "recursive",
        ),
        (
            "run-recursive-cycle",
            cyclic_a.clone(),
            ChildSeams {
                resolver: Some(std::sync::Arc::new(move |name: &str| match name {
                    "a" => Some(cyclic_a.clone()),
                    "b" => Some(cyclic_b.clone()),
                    _ => None,
                })),
            },
            "recursive",
        ),
        (
            "run-unknown",
            calling_workflow("parent", "ghost", StepOnError::Continue),
            child_seams(callee_workflow()),
            "unknown workflow",
        ),
        (
            "run-unsequencable",
            calling_workflow("parent", "child", StepOnError::Continue),
            child_seams(unsequencable_child),
            "cannot run on the durable models",
        ),
        (
            "run-no-resolver",
            calling_workflow("parent", "child", StepOnError::Continue),
            ChildSeams::default(),
            "needs a workflow resolver",
        ),
    ];

    for (run_id, wf, seams, label) in cases {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
            );
            DurableGraphRun::open_with_children(engine, state, rid, wf, json!({}), 0, &seams)
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in [
        "run-recursive-self",
        "run-recursive-cycle",
        "run-unknown",
        "run-unsequencable",
        "run-no-resolver",
    ] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "child", 64), &handle),
    );
    for run_id in [
        "run-recursive-self",
        "run-recursive-cycle",
        "run-unknown",
        "run-unsequencable",
        "run-no-resolver",
    ] {
        assert!(engine.query(run_id).is_err(), "{run_id} left an engine row");
    }
}

// ---------------------------------------------------------------------------
// Bounded loop in the linear plan (bd-loop-rewrites-fap8)
// ---------------------------------------------------------------------------

fn loop_step(
    name: &str,
    max: &str,
    until: Option<&str>,
    body: Vec<Step>,
    on_error: StepOnError,
) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Loop {
            max: max.into(),
            until: until.map(str::to_string),
            body,
        },
    }
}

fn loop_workflow(max: &str, until: Option<&str>, on_error: StepOnError) -> Workflow {
    workflow(vec![loop_step(
        "spin",
        max,
        until,
        vec![named("check", dm("check"), StepOnError::Continue)],
        on_error,
    )])
}

/// Through the store: the persisted counter resumes the loop at the same
/// pass, and the iteration key is the same on the re-issued effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_restart_resumes_at_counter() {
    let path = unique_db("loop-restart");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let phase_one = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || -> Result<RunModel, HostError> {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "loop", 64), &handle),
            );
            let (mut run, effects) = DurableRun::open(
                engine,
                state,
                "run-loop",
                loop_workflow("5", Some("steps_check_ok == true"), StepOnError::Continue),
                json!({}),
                1_000,
            )?;
            assert!(matches!(
                effects.as_slice(),
                [RunEffect::ExecuteStep {
                    iteration: Some(1),
                    ..
                }]
            ));
            let effects = run.step_done(json!({"ok": false}), 2_000)?;
            assert!(matches!(
                effects.as_slice(),
                [RunEffect::ExecuteStep {
                    iteration: Some(2),
                    ..
                }]
            ));
            Ok(run.model().clone())
        })
        .await
        .expect("join")
        .expect("phase one")
    };
    assert_eq!(phase_one.loops["spin"].iteration, 2);

    let (mut run, effects) = {
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "loop", 64), &handle),
            );
            DurableRun::open(
                engine,
                state,
                "run-loop",
                loop_workflow("5", Some("steps_check_ok == true"), StepOnError::Continue),
                json!({}),
                3_000,
            )
        })
        .await
        .expect("join")
        .expect("reopen")
    };
    let [
        RunEffect::ExecuteStep {
            iteration: Some(2),
            step,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("resume re-issues pass 2: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("check"));

    let effects = run.step_done(json!({"ok": true}), 4_000).expect("settle");
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(
        run.model().outputs["spin"],
        json!({"ok": true, "iterations": 2, "stopped": "until"})
    );
}

/// An unbounded loop, an over-cap bound, an upstream-output bound, an impure
/// `until`, and an unsequencable body all refuse before any state or engine
/// row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_unbounded_refuses_without_row() {
    let path = unique_db("loop-refused");
    let pool = connect(path.to_str().expect("path"))
        .await
        .expect("connect");
    migrate(&pool).await.expect("migrate");
    let handle = tokio::runtime::Handle::current();

    let cases: Vec<(&str, Workflow, serde_json::Value, &str)> = vec![
        (
            "run-loop-missing",
            loop_workflow("{{inputs.n}}", None, StepOnError::Continue),
            json!({}),
            "must render to a positive integer",
        ),
        (
            "run-loop-zero",
            loop_workflow("0", None, StepOnError::Continue),
            json!({}),
            "must render to a positive integer",
        ),
        (
            "run-loop-cap",
            loop_workflow("33", None, StepOnError::Continue),
            json!({}),
            "exceeds the bounded cap",
        ),
        (
            "run-loop-output-bound",
            loop_workflow("{{steps.plan.n}}", None, StepOnError::Continue),
            json!({}),
            "upstream output",
        ),
        (
            "run-loop-impure",
            loop_workflow("3", Some("len(\"x\") > 0"), StepOnError::Continue),
            json!({}),
            "until` is not pure",
        ),
        (
            "run-loop-body",
            loop_workflow("3", None, StepOnError::Continue),
            json!({}),
            "inside a loop",
        ),
    ];
    for (run_id, wf, trigger, label) in cases {
        // The body case needs an unsequencable body step; rebuild it here.
        let wf = if run_id == "run-loop-body" {
            workflow(vec![loop_step(
                "spin",
                "3",
                None,
                vec![step(Action::CallWebhook {
                    url: "http://127.0.0.1:9/x".into(),
                    method: None,
                    body: None,
                })],
                StepOnError::Continue,
            )])
        } else {
            wf
        };
        let pool = pool.clone();
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        let handle = handle.clone();
        let rid = run_id.to_string();
        let refused = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool, "loop", 64), &handle),
            );
            DurableRun::open(engine, state, rid, wf, trigger, 0)
        })
        .await
        .expect("join");
        let Err(err) = refused else {
            panic!("{run_id}: must refuse");
        };
        let HostError::Unsupported(reason) = err else {
            panic!("{run_id}: wrong refusal: {err:?}");
        };
        assert!(reason.contains(label), "{run_id}: {reason}");
    }

    let store = toron_core::index::durable_workflow::WorkflowStateStore::new(pool.clone());
    for run_id in [
        "run-loop-missing",
        "run-loop-zero",
        "run-loop-cap",
        "run-loop-output-bound",
        "run-loop-impure",
        "run-loop-body",
    ] {
        assert!(
            store.load(run_id).await.expect("state").is_none(),
            "{run_id} left no state row"
        );
    }
    let engine = OurEngine::with_journal(
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool, "loop", 64), &handle),
    );
    for run_id in [
        "run-loop-missing",
        "run-loop-zero",
        "run-loop-cap",
        "run-loop-output-bound",
        "run-loop-impure",
        "run-loop-body",
    ] {
        assert!(engine.query(run_id).is_err(), "{run_id} left an engine row");
    }
}
