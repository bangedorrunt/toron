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
use toron_workflow::engine::durable::{GraphEffect, GraphPhase, RunEffect, RunModel, RunPhase};
use toron_workflow::engine::durable_host::{DurableGraphRun, DurableRun, HostError};
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
