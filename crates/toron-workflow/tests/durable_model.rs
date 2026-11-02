// governed-by: ADR-0005
//! Pure linear run model: execute, delay/wake, approval, failure policy,
//! cancel, graph fallback, and state round-trip.

use std::collections::BTreeMap;

use serde_json::json;
use toron_workflow::engine::durable::{
    GraphEffect, GraphModel, GraphMsg, GraphPhase, NodeParkReason, NodeState, ParkReason,
    RunEffect, RunModel, RunMsg, RunPhase, advance, advance_graph, advance_with, durable_graph,
    graph_unsupported_reason, needs_graph_model, resume_effects, resume_graph_effects,
    unsupported_reason,
};
use toron_workflow::engine::spawn::{SpawnSpec, spawn_launcher_refusal};
use toron_workflow::schema::{Action, Graph, GraphNode, Step, StepOnError, Workflow};

fn workflow(steps: Vec<Step>) -> Workflow {
    Workflow {
        name: "test".into(),
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

fn step(action: Action, on_error: StepOnError) -> Step {
    Step {
        name: None,
        guard: None,
        on_error,
        build_slot: false,
        action,
    }
}

fn model(steps: Vec<Step>) -> RunModel {
    RunModel::new("run-1", workflow(steps), json!({"source": "test"}))
}

#[test]
fn execute_then_complete() {
    let model = model(vec![step(
        Action::Verify {
            command: "true".into(),
        },
        StepOnError::Continue,
    )]);
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::ExecuteStep { index: 0, .. }]
    ));

    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 1_000,
        },
    );
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert_eq!(model.step_index, 1);
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.outputs["step_1"], json!({"ok": true}));
}

#[test]
fn delay_parks_then_wakes() {
    let model = model(vec![step(Action::Delay { secs: 2 }, StepOnError::Continue)]);
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::ParkUntil {
            until_ms: 3_000,
            ..
        }]
    ));
    assert!(matches!(
        model.phase,
        RunPhase::Parked {
            until_ms: 3_000,
            ..
        }
    ));

    let (model, effects) = advance(&model, &RunMsg::Woke { now_ms: 3_000 });
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.outputs["step_1"]["waited"], json!(true));
}

#[test]
fn approval_awaits_then_lands_decision() {
    let model = model(vec![step(
        Action::RequestApproval {
            message: "approve?".into(),
        },
        StepOnError::Continue,
    )]);
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::AwaitApproval { step, .. }] if step == "step_1"
    ));
    assert!(matches!(model.phase, RunPhase::AwaitingApproval { .. }));

    let (model, effects) = advance(
        &model,
        &RunMsg::Approval {
            decision: json!({"outcome": "approved"}),
            now_ms: 2_000,
        },
    );
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.outputs["step_1"]["outcome"], json!("approved"));
    // Parity with the old linear Context path: the canonical alias is
    // published alongside the node name, so either template ref resolves.
    assert_eq!(model.outputs["decision"]["outcome"], json!("approved"));
}

#[test]
fn failure_policy_continue_vs_fail_run() {
    let continue_model = model(vec![step(
        Action::Verify {
            command: "false".into(),
        },
        StepOnError::Continue,
    )]);
    let (continue_model, _) = advance(&continue_model, &RunMsg::Start { now_ms: 0 });
    let (continue_model, effects) = advance(
        &continue_model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "error": "exit 1"}),
            now_ms: 0,
        },
    );
    assert_eq!(continue_model.phase, RunPhase::Succeeded);
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(continue_model.outputs["step_1"]["ok"], json!(false));

    let fail_model = model(vec![step(
        Action::Verify {
            command: "false".into(),
        },
        StepOnError::FailRun,
    )]);
    let (fail_model, _) = advance(&fail_model, &RunMsg::Start { now_ms: 0 });
    let (fail_model, effects) = advance(
        &fail_model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "error": "exit 1"}),
            now_ms: 0,
        },
    );
    assert!(matches!(fail_model.phase, RunPhase::Failed { .. }));
    assert!(matches!(effects.as_slice(), [RunEffect::Fail { .. }]));
}

#[test]
fn cancel_is_terminal_and_graph_is_unsupported() {
    let model = model(vec![step(
        Action::Verify {
            command: "true".into(),
        },
        StepOnError::Continue,
    )]);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 0 });
    let (model, effects) = advance(&model, &RunMsg::Cancel);
    assert_eq!(model.phase, RunPhase::Cancelled);
    assert!(matches!(effects.as_slice(), [RunEffect::Cancelled]));

    let mut graph_workflow = workflow(Vec::new());
    graph_workflow.graph = Some(Graph {
        nodes: BTreeMap::new(),
    });
    let model = RunModel::new("graph-run", graph_workflow, json!({}));
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 0 });
    assert_eq!(model.phase, RunPhase::Ready);
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::Unsupported { .. }]
    ));
}

#[test]
fn state_serde_round_trips() {
    let model = model(vec![step(Action::Delay { secs: 5 }, StepOnError::Continue)]);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let encoded = serde_json::to_string(&model).expect("serialize");
    let decoded: RunModel = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(decoded.run_id, "run-1");
    assert_eq!(decoded.step_index, 0);
    assert!(matches!(
        decoded.phase,
        RunPhase::Parked {
            until_ms: 6_000,
            ..
        }
    ));
}

// ---------------------------------------------------------------------------
// Graph model (bd-durable-graph-impl-r2x1)
// ---------------------------------------------------------------------------

fn graph_workflow(nodes: &[(&str, Action, &[&str], StepOnError)]) -> Workflow {
    let mut map = BTreeMap::new();
    for (name, action, needs, on_error) in nodes {
        map.insert(
            (*name).to_string(),
            GraphNode {
                action: action.clone(),
                needs: needs.iter().map(|n| (*n).to_string()).collect(),
                guard: None,
                on_error: *on_error,
                build_slot: false,
            },
        );
    }
    let mut wf = workflow(Vec::new());
    wf.graph = Some(Graph { nodes: map });
    wf
}

fn graph_model(nodes: &[(&str, Action, &[&str], StepOnError)]) -> GraphModel {
    GraphModel::new("graph-1", graph_workflow(nodes), json!({"source": "test"}))
}

fn verify_action(command: &str) -> Action {
    Action::Verify {
        command: command.into(),
    }
}

fn executed(effects: &[GraphEffect]) -> Vec<&str> {
    effects
        .iter()
        .filter_map(|e| match e {
            GraphEffect::ExecuteNode { node, .. } => Some(node.as_str()),
            _ => None,
        })
        .collect()
}

/// Dijkstra diamond: scout → plan → {fix_a, fix_b} → ship. Each node is
/// scheduled exactly once, in wave order, and the run ends with every output.
#[test]
fn graph_diamond_join_runs_each_node_once() {
    let diamond = [
        (
            "scout",
            verify_action("scout"),
            &[][..],
            StepOnError::Continue,
        ),
        (
            "plan",
            verify_action("plan"),
            &["scout"][..],
            StepOnError::Continue,
        ),
        (
            "fix_a",
            verify_action("fix_a"),
            &["plan"][..],
            StepOnError::Continue,
        ),
        (
            "fix_b",
            verify_action("fix_b"),
            &["plan"][..],
            StepOnError::Continue,
        ),
        (
            "ship",
            verify_action("ship"),
            &["fix_a", "fix_b"][..],
            StepOnError::Continue,
        ),
    ];
    let mut model = graph_model(&diamond);
    let (next, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    model = next;
    assert_eq!(executed(&effects), ["scout"], "wave 1 is the root");

    for (node, expected) in [
        ("scout", vec!["plan"]),
        ("plan", vec!["fix_a", "fix_b"]),
        ("fix_a", vec![]),
        ("fix_b", vec!["ship"]),
        ("ship", vec![]),
    ] {
        let (next, effects) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: node.to_string(),
                value: json!({ "ok": true, "node": node }),
                now_ms: 1_000,
            },
        );
        model = next;
        assert_eq!(executed(&effects), expected, "after {node}");
    }

    assert_eq!(model.phase, GraphPhase::Succeeded);
    assert_eq!(model.wave, 4, "four scheduling rounds");
    for name in ["scout", "plan", "fix_a", "fix_b", "ship"] {
        assert_eq!(model.outputs[name]["node"], json!(name));
    }
}

/// Two approvals in one wave: the alias belongs to the highest
/// `(wave, node-name)` no matter which decision lands first.
#[test]
fn graph_approval_alias_is_wave_ordered() {
    let approvals = [
        (
            "aa",
            Action::RequestApproval {
                message: "approve aa?".into(),
            },
            &[][..],
            StepOnError::Continue,
        ),
        (
            "zz",
            Action::RequestApproval {
                message: "approve zz?".into(),
            },
            &[][..],
            StepOnError::Continue,
        ),
    ];
    let mut model = graph_model(&approvals);
    let (next, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    model = next;
    assert_eq!(
        effects.len(),
        2,
        "both approvals are in flight: {effects:?}"
    );
    assert!(model.is_in_flight());

    let (next, effects) = advance_graph(
        &model,
        &GraphMsg::Approval {
            node: "zz".into(),
            decision: json!({"outcome": "approved", "node": "zz"}),
            now_ms: 2_000,
        },
    );
    model = next;
    assert!(
        effects.is_empty(),
        "a sibling is still awaiting: {effects:?}"
    );
    assert_eq!(model.outputs["decision"]["node"], json!("zz"));

    let (next, _) = advance_graph(
        &model,
        &GraphMsg::Approval {
            node: "aa".into(),
            decision: json!({"outcome": "rejected", "node": "aa"}),
            now_ms: 3_000,
        },
    );
    model = next;
    assert_eq!(model.phase, GraphPhase::Succeeded);
    assert_eq!(
        model.outputs["decision"]["node"],
        json!("zz"),
        "the alias is (wave, name)-max, not arrival order"
    );
}

/// A retryable failure burns the default three attempts, then reports the
/// enriched bundle with the attempt count.
#[test]
fn graph_fail_run_exhaustion_reports_attempts() {
    let mut model = graph_model(&[(
        "flaky",
        verify_action("flaky"),
        &[][..],
        StepOnError::FailRun,
    )]);
    let (next, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    model = next;
    assert_eq!(executed(&effects), ["flaky"]);

    let failure = json!({
        "ok": false,
        "error": "provider down",
        "kind": "timeout",
        "retry_after_ms": 0,
    });
    for attempt in 1..=3 {
        let (next, effects) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "flaky".into(),
                value: failure.clone(),
                now_ms: 1_000,
            },
        );
        model = next;
        if attempt < 3 {
            assert!(
                matches!(
                    effects.as_slice(),
                    [GraphEffect::ParkUntil {
                        reason: NodeParkReason::Retry,
                        ..
                    }]
                ),
                "attempt {attempt} parks for a retry: {effects:?}"
            );
            let (next, effects) = advance_graph(&model, &GraphMsg::Woke { now_ms: 1_000 });
            model = next;
            assert_eq!(
                executed(&effects),
                ["flaky"],
                "attempt {} re-runs",
                attempt + 1
            );
        } else {
            let [GraphEffect::Fail { fail }] = effects.as_slice() else {
                panic!("exhaustion fails the run: {effects:?}");
            };
            let value = fail.to_value();
            assert_eq!(value["status"], json!("failed"));
            assert_eq!(value["failed_step"], json!("flaky"));
            assert_eq!(value["kind"], json!("timeout"));
            assert_eq!(value["attempts"], json!(3));
            assert_eq!(
                value["disposition"],
                json!("terminal_failed"),
                "the same disposition the old scheduler wrote"
            );
            let preview = value["leaf_preview"].as_str().expect("leaf preview");
            assert!(
                preview.contains("\"kind\":\"timeout\""),
                "the bundle carries the leaf evidence: {preview}"
            );
        }
    }
    assert!(matches!(model.phase, GraphPhase::Failed { .. }));
}

/// Whole-graph refusal: one unsequencable node keeps the entire graph on the
/// old engine, and the fold refuses it before any state exists.
#[test]
fn graph_refuses_a_control_flow_node_whole() {
    for (label, action) in [
        (
            "parallel",
            Action::Parallel {
                over: "{{trigger.items}}".into(),
                each: Box::new(step(verify_action("x"), StepOnError::Continue)),
            },
        ),
        (
            "spawn",
            Action::Spawn {
                profile: "coder".into(),
                harness: None,
                workflow: None,
                await_: false,
                no_claim_timeout: 600,
                context: None,
            },
        ),
    ] {
        let wf = graph_workflow(&[
            (
                "fine",
                verify_action("fine"),
                &[][..],
                StepOnError::Continue,
            ),
            ("bad", action, &["fine"][..], StepOnError::Continue),
        ]);
        let reason = graph_unsupported_reason(&wf).expect("refused");
        assert!(reason.contains(label), "names the node action: {reason}");

        let model = GraphModel::new("graph-refused", wf, json!({}));
        let (next, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 0 });
        assert!(
            matches!(effects.as_slice(), [GraphEffect::Unsupported { .. }]),
            "the fold refuses it too: {effects:?}"
        );
        assert_eq!(next.phase, GraphPhase::Ready, "a refusal starts nothing");
    }
}

/// Required change 1, reachable half: the wave's effects are issued in
/// `BTreeMap` order and a terminal failure stops the wave — no later node is
/// issued after it.
///
/// The render-failure arm itself is preserved in `schedule` but cannot be
/// witnessed: `render_action_lenient` only substitutes inside strings, so a
/// typed `Action` always re-deserializes. The arm stays for the same reason the
/// stall gate does — it is the old scheduler's shape, kept rather than deleted.
#[test]
fn graph_wave_order_is_btreemap_and_a_terminal_stops_it() {
    let nodes = [
        (
            "zz_second",
            verify_action("second"),
            &[][..],
            StepOnError::Continue,
        ),
        (
            "aa_first",
            verify_action("first"),
            &[][..],
            StepOnError::Continue,
        ),
    ];
    let model = graph_model(&nodes);
    let (model, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 0 });
    assert_eq!(
        executed(&effects),
        ["aa_first", "zz_second"],
        "a wave is emitted in node-name order, not author order"
    );

    // Both nodes are in flight; a terminal on the first one still lands it and
    // leaves the sibling alone.
    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::NodeDone {
            node: "aa_first".into(),
            value: json!({"ok": false, "error": "boom", "kind": "rate_limit"}),
            now_ms: 0,
        },
    );
    assert!(
        effects.is_empty(),
        "the wave is not re-scheduled: {effects:?}"
    );
    assert_eq!(model.outputs["aa_first"]["kind"], json!("rate_limit"));
    assert!(matches!(
        model.node_state["zz_second"],
        NodeState::Executing { .. }
    ));
}

/// Addendum (failure carrier): a `continue` failure keeps its full value —
/// kind and leaf_preview included — instead of collapsing to `{ok,error}`.
#[test]
fn linear_continue_failure_keeps_the_bundle() {
    let model = model(vec![step(
        Action::Verify {
            command: "flaky".into(),
        },
        StepOnError::Continue,
    )]);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 0 });
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({
                "ok": false,
                "error": "provider down",
                "kind": "timeout",
                "leaf_preview": "{\"ok\":false}",
            }),
            now_ms: 0,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.outputs["step_1"]["kind"], json!("timeout"));
    assert_eq!(
        model.outputs["step_1"]["leaf_preview"],
        json!("{\"ok\":false}")
    );

    // And the bundle survives the state round-trip a restart performs.
    let encoded = serde_json::to_string(&model).expect("serialize");
    let decoded: RunModel = serde_json::from_str(&encoded).expect("deserialize");
    assert_eq!(decoded.outputs["step_1"]["kind"], json!("timeout"));
    assert_eq!(
        decoded.outputs["step_1"]["leaf_preview"],
        json!("{\"ok\":false}")
    );
}

/// Addendum: a `fail_run` exhaustion keeps `attempts` in the settled output.
#[test]
fn linear_fail_run_keeps_attempts() {
    let model = model(vec![step(
        Action::Verify {
            command: "flaky".into(),
        },
        StepOnError::FailRun,
    )]);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 0 });
    let (_model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({
                "status": "failed",
                "error": "provider down",
                "failed_step": "step_1",
                "kind": "timeout",
                "disposition": "terminal_failed",
                "leaf_preview": "{\"ok\":false}",
                "attempts": 3,
            }),
            now_ms: 0,
        },
    );
    let [RunEffect::Fail { output, .. }] = effects.as_slice() else {
        panic!("fail_run emits the bundle: {effects:?}");
    };
    assert_eq!(output["attempts"], json!(3));
    assert_eq!(output["kind"], json!("timeout"));
    assert_eq!(output["leaf_preview"], json!("{\"ok\":false}"));
}

/// Addendum: a bare failure value is enriched by the fold, not passed through.
#[test]
fn linear_bare_failure_is_enriched_on_fail_run() {
    let model = model(vec![step(
        Action::Verify {
            command: "x".into(),
        },
        StepOnError::FailRun,
    )]);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 0 });
    let (_model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "error": "render: no such ref"}),
            now_ms: 0,
        },
    );
    let [RunEffect::Fail { step, output }] = effects.as_slice() else {
        panic!("fail_run emits the bundle: {effects:?}");
    };
    assert_eq!(step, "step_1");
    assert_eq!(output["status"], json!("failed"));
    assert_eq!(output["failed_step"], json!("step_1"));
    assert_eq!(output["error"], json!("render: no such ref"));
    assert_eq!(
        output["leaf_preview"],
        json!("{\"error\":\"render: no such ref\",\"ok\":false}")
    );
    assert!(output.get("attempts").is_none(), "{output}");
}

// ---------------------------------------------------------------------------
// Durable spawn park (bd-spawn-park-toron-vwu3)
// ---------------------------------------------------------------------------

fn spawn_step(await_: bool, no_claim_timeout: u64) -> Step {
    Step {
        name: Some("spawn_it".into()),
        guard: None,
        on_error: StepOnError::Continue,
        build_slot: false,
        action: Action::Spawn {
            profile: "coder".into(),
            harness: None,
            workflow: None,
            await_,
            no_claim_timeout,
            context: None,
        },
    }
}

fn spawn_spec(run_id: &str, node: &str, await_: bool, timeout: u64) -> SpawnSpec {
    SpawnSpec {
        spawn_key: format!("{run_id}:{node}:1"),
        run_id: run_id.into(),
        node: node.into(),
        workflow: "test".into(),
        project: None,
        generation: 1,
        profile: "coder".into(),
        resolved: json!({"agent": "coder"}),
        harness: None,
        workflow_ref: None,
        await_,
        no_claim_timeout: timeout,
        context: None,
    }
}

/// An awaited spawn parks on `AwaitingSpawn` and settles into
/// `{ok:true, spawn_key, result}`; the spec rides both the phase and the
/// effect so a restart republishes the same key.
#[test]
fn spawn_awaits_then_settles() {
    let model = model(vec![spawn_step(true, 30)]);
    let spec = spawn_spec("run-1", "spawn_it", true, 30);
    let (model, effects) = advance_with(
        &model,
        &RunMsg::Start { now_ms: 1_000 },
        &[Some(spec.clone())],
    );

    let [
        RunEffect::StartSpawn {
            node,
            spec: carried,
        },
        RunEffect::ParkUntil { until_ms, reason },
    ] = effects.as_slice()
    else {
        panic!("an awaited spawn parks: {effects:?}");
    };
    assert_eq!(node, "spawn_it");
    assert_eq!(carried.spawn_key, "run-1:spawn_it:1");
    assert_eq!(*until_ms, 1_000 + 30_000, "the claim deadline is a park");
    assert_eq!(*reason, ParkReason::Spawn);
    assert!(matches!(
        &model.phase,
        RunPhase::AwaitingSpawn {
            deadline_ms: Some(_),
            ..
        }
    ));

    let (model, effects) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "spawn_it".into(),
            value: json!({"pane": "p1"}),
            now_ms: 2_000,
        },
        &[],
    );
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert_eq!(
        model.outputs["spawn_it"],
        json!({"ok": true, "spawn_key": "run-1:spawn_it:1", "result": {"pane": "p1"}})
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
}

/// A settled event for another node is stale and changes nothing.
#[test]
fn duplicate_spawn_completion_is_ignored() {
    let model = model(vec![spawn_step(true, 0)]);
    let spec = spawn_spec("run-1", "spawn_it", true, 0);
    let (model, _) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[Some(spec)]);
    let (model, _) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "spawn_it".into(),
            value: json!({"pane": "p1"}),
            now_ms: 2_000,
        },
        &[],
    );
    let before = model.clone();
    let (model, effects) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "spawn_it".into(),
            value: json!({"pane": "p2"}),
            now_ms: 3_000,
        },
        &[],
    );
    assert!(effects.is_empty(), "a second settlement emits nothing");
    assert_eq!(model.outputs, before.outputs, "the first settlement stands");
}

/// No completion by the deadline fails the node with the old timeout text.
#[test]
fn spawn_timeout_fails_the_node() {
    let mut step = spawn_step(true, 5);
    step.on_error = StepOnError::Continue;
    let model = model(vec![step]);
    let spec = spawn_spec("run-1", "spawn_it", true, 5);
    let (model, _) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[Some(spec)]);

    let (model, effects) = advance_with(&model, &RunMsg::Woke { now_ms: 6_000 }, &[]);
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert_eq!(
        model.outputs["spawn_it"],
        json!({"ok": false, "error": "spawn-await run-1:spawn_it:1: timed out after 5s"}),
        "the old error text, keyed by spawn_key"
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
}

/// `await: false` publishes the job and lands immediately — no park.
#[test]
fn detached_spawn_does_not_park() {
    let model = model(vec![spawn_step(false, 0)]);
    let spec = spawn_spec("run-1", "spawn_it", false, 0);
    let (model, effects) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[Some(spec)]);

    let [
        RunEffect::SpawnDetached { node, .. },
        RunEffect::Complete { .. },
    ] = effects.as_slice()
    else {
        panic!("a detached spawn fires and completes: {effects:?}");
    };
    assert_eq!(node, "spawn_it");
    assert_eq!(
        model.outputs["spawn_it"],
        json!({"ok": true, "spawned": true})
    );
    assert_eq!(model.phase, RunPhase::Succeeded, "no phase change");
}

/// A profile the resolver cannot build a spec for is a step VALUE, never a
/// run failure — and the run continues.
#[test]
fn unknown_profile_lands_a_value() {
    let model = model(vec![spawn_step(true, 30)]);
    let (model, effects) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[]);

    assert!(
        matches!(effects.as_slice(), [RunEffect::Complete { .. }]),
        "no park for an unbuildable spec: {effects:?}"
    );
    assert_eq!(model.phase, RunPhase::Succeeded);
    assert_eq!(
        model.outputs["spawn_it"]["error"],
        json!("spawn: unknown profile \"coder\" (ProfileResolver returned None)")
    );
}

/// The cutover (bd-spawn-launcher-flip-d8mv): `spawn` is a SHAPE the linear
/// model sequences, so the shape router admits it. Capability lives with the
/// host that must perform the launch, not with the fold.
#[test]
fn spawn_is_admitted_by_shape_and_needs_a_launcher() {
    let workflow = workflow(vec![spawn_step(true, 30)]);
    assert!(
        unsupported_reason(&workflow).is_none(),
        "shape admits spawn"
    );
    assert!(
        spawn_launcher_refusal(&workflow).is_some(),
        "a host with no launcher still refuses it"
    );
}

/// A restart re-issues the SAME `StartSpawn` (same `spawn_key`), which is what
/// makes the consumer's claim — not a stored promise — the exactly-once gate.
#[test]
fn spawn_restart_reissues_the_same_key() {
    let model = model(vec![spawn_step(true, 30)]);
    let spec = spawn_spec("run-1", "spawn_it", true, 30);
    let (model, _) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[Some(spec)]);

    let resumed = resume_effects(&model);
    let [RunEffect::StartSpawn { node, spec }] = resumed.as_slice() else {
        panic!("a restart re-issues exactly one start: {resumed:?}");
    };
    assert_eq!(node, "spawn_it");
    assert_eq!(spec.spawn_key, "run-1:spawn_it:1");

    // And the stored phase carries the same spec, so the republish is
    // byte-identical across processes.
    let stored = match &model.phase {
        RunPhase::AwaitingSpawn { spec, .. } => spec.clone(),
        other => panic!("still awaiting: {other:?}"),
    };
    assert_eq!(stored, spec.clone());
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

fn branch_workflow(on_error: StepOnError) -> Workflow {
    workflow(vec![branch(
        "{{trigger.gate}} == true",
        vec![
            named("yes", dm("yes"), StepOnError::Continue),
            named("yes-2", dm("yes-2"), StepOnError::Continue),
        ],
        vec![named("no", dm("no"), StepOnError::Continue)],
        on_error,
    )])
}

fn started(workflow: Workflow, trigger: serde_json::Value) -> (RunModel, Vec<RunEffect>) {
    let model = RunModel::new("run-branch", workflow, trigger);
    advance(&model, &RunMsg::Start { now_ms: 1_000 })
}

/// The chosen arm runs and the other one never dispatches.
#[test]
fn branch_true_arm_runs_false_skips() {
    for (gate, taken, skipped, side) in [
        (json!({"gate": true}), "yes", "no", "then"),
        (json!({"gate": false}), "no", "yes", "else"),
    ] {
        let (model, effects) = started(branch_workflow(StepOnError::Continue), gate);
        let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
            panic!("the chosen arm's first step runs: {effects:?}");
        };
        assert_eq!(step.name.as_deref(), Some(taken));
        // Drive the chosen arm to completion (the then arm has two steps).
        let (model, effects) = {
            let (mut model, mut effects) = (model, effects);
            for tick in 0..4 {
                match effects.as_slice() {
                    [RunEffect::ExecuteStep { .. }] => {
                        let (next, follow) = advance(
                            &model,
                            &RunMsg::StepDone {
                                value: json!({"ok": true}),
                                now_ms: 2_000 + tick,
                            },
                        );
                        model = next;
                        effects = follow;
                    }
                    _ => break,
                }
            }
            (model, effects)
        };
        assert!(
            matches!(effects.as_slice(), [RunEffect::Complete { .. }]),
            "the run completes after the chosen arm: {effects:?}"
        );
        assert!(model.outputs.contains_key(taken), "{:?}", model.outputs);
        assert!(
            !model.outputs.contains_key(skipped),
            "the unselected arm never dispatched: {:?}",
            model.outputs
        );
        assert_eq!(model.outputs["choose"], json!({"branch": side}));
    }
}

/// A restart mid-arm resumes the SAME arm: the marker is not re-evaluated.
#[test]
fn branch_restart_never_flips_arm() {
    let (model, effects) = started(
        branch_workflow(StepOnError::Continue),
        json!({"gate": true}),
    );
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::ExecuteStep { .. }]
    ));
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 2_000,
        },
    );
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("the then arm's second step is next: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("yes-2"));

    // The restart: the persisted model carries the arm cursor, not the choice.
    let json_text = serde_json::to_string(&model).expect("serialize");
    let restored: RunModel = serde_json::from_str(&json_text).expect("restore");
    let effects = resume_effects(&restored);
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("resume re-issues the same arm step: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("yes-2"), "never the else arm");

    let (restored, effects) = advance(
        &restored,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 3_000,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(restored.outputs["choose"], json!({"branch": "then"}));
    assert!(restored.outputs.contains_key("yes"));
    assert!(restored.outputs.contains_key("yes-2"));
    assert!(!restored.outputs.contains_key("no"));
}

/// An undecidable guard is a step FAILURE, never a silent `else`: `continue`
/// lands the value and skips both arms; `fail_run` emits the enriched bundle.
#[test]
fn branch_guard_error_applies_on_error() {
    let broken = || branch_workflow_with_guard("{{steps.missing.value}} == 1");

    let (model, effects) = started(broken(), json!({}));
    assert!(
        matches!(effects.as_slice(), [RunEffect::Complete { .. }]),
        "on_error: continue skips both arms: {effects:?}"
    );
    let landed = &model.outputs["choose"];
    assert_eq!(landed["ok"], json!(false));
    assert!(
        landed["error"]
            .as_str()
            .is_some_and(|e| e.contains("unknown reference")),
        "{landed}"
    );
    assert!(!model.outputs.contains_key("yes"));
    assert!(!model.outputs.contains_key("no"));

    let mut wf = broken();
    wf.steps[0].on_error = StepOnError::FailRun;
    let model = RunModel::new("run-branch-fail", wf, json!({}));
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let [RunEffect::Fail { step, output }] = effects.as_slice() else {
        panic!("fail_run emits the bundle: {effects:?}");
    };
    assert_eq!(step, "choose");
    assert_eq!(output["status"], json!("failed"));
    assert_eq!(output["failed_step"], json!("choose"));
    assert!(matches!(model.phase, RunPhase::Failed { .. }));
}

fn branch_workflow_with_guard(guard: &str) -> Workflow {
    workflow(vec![branch(
        guard,
        vec![named("yes", dm("yes"), StepOnError::Continue)],
        vec![named("no", dm("no"), StepOnError::Continue)],
        StepOnError::Continue,
    )])
}

/// Spawn BEFORE a branch keeps the linear model: the spawn park settles, the
/// marker picks the arm, and the whole run stays in one plan.
#[test]
fn spawn_before_branch_stays_linear() {
    let wf = workflow(vec![
        named("spawn_it", spawn_action(true, 600), StepOnError::Continue),
        branch_workflow_with_guard("{{trigger.gate}} == true")
            .steps
            .remove(0),
    ]);
    assert!(
        unsupported_reason(&wf).is_none(),
        "spawn-before-branch is one linear shape"
    );
    let model = RunModel::new("run-spawn-branch", wf, json!({"gate": true}));
    let spec = spawn_spec("run-spawn-branch", "spawn_it", true, 600);
    let (model, effects) = advance_with(&model, &RunMsg::Start { now_ms: 1_000 }, &[Some(spec)]);
    let [
        RunEffect::StartSpawn { node, .. },
        RunEffect::ParkUntil { .. },
    ] = effects.as_slice()
    else {
        panic!("the spawn parks before the branch: {effects:?}");
    };
    assert_eq!(node, "spawn_it");

    let (model, effects) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "spawn_it".into(),
            value: json!({"patch": "p1"}),
            now_ms: 2_000,
        },
        &[],
    );
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("the branch picks the then arm after the spawn: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("yes"));
    assert_eq!(model.phase, RunPhase::Running);
}

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

/// Only the pure comparison grammar is admitted; anything else — a function
/// call, arithmetic, a nested control-flow or spawn step in an arm — refuses
/// the whole workflow.
#[test]
fn branch_refuses_impure_guards_and_unsupported_arms() {
    for guard in [
        "len(\"x\") > 0",
        "1 + 1 == 2",
        "shell(\"rm -rf /\") == 0",
        "{{trigger.gate}} == true ? true : false",
    ] {
        let wf = branch_workflow_with_guard(guard);
        let reason = unsupported_reason(&wf).expect("an impure guard refuses");
        assert!(reason.contains("not pure"), "{guard}: {reason}");
    }

    // A nested control-flow step in an arm refuses whole.
    let wf = workflow(vec![branch(
        "{{trigger.gate}} == true",
        vec![step(Action::Delay { secs: 1 }, StepOnError::Continue)],
        vec![],
        StepOnError::Continue,
    )]);
    let reason = unsupported_reason(&wf).expect("nested control flow refuses");
    assert!(reason.contains("inside an arm"), "{reason}");

    // An unsequencable action in an arm refuses whole. (A `spawn` IS
    // sequenced now: its spec is prepared on the plan entries.)
    let wf = workflow(vec![branch(
        "{{trigger.gate}} == true",
        vec![step(
            Action::CallWebhook {
                url: "http://127.0.0.1:9/x".into(),
                method: None,
                body: None,
            },
            StepOnError::Continue,
        )],
        vec![],
        StepOnError::Continue,
    )]);
    let reason = unsupported_reason(&wf).expect("an unsequencable arm refuses");
    assert!(reason.contains("inside an arm"), "{reason}");
}

/// The two shipped toron workflows that spawn BEFORE they branch are one
/// linear shape each (bd-durable-branch-hhgc): the spawn keeps the model
/// linear, the branch is a pure-guard choice, and every arm step is a
/// host-sink leaf.
#[test]
fn shipped_spawn_then_branch_workflows_are_linear() {
    for (name, yaml) in [
        (
            "housekeeper-diagnose",
            include_str!("../../../workflows/housekeeper-diagnose.yaml"),
        ),
        (
            "agent-reach-upgrade",
            include_str!("../../../workflows/agent-reach-upgrade.yaml"),
        ),
    ] {
        let wf = toron_workflow::schema::parse_workflow(yaml).expect("parse workflow");
        assert_eq!(wf.name, name);
        assert!(
            matches!(wf.steps[0].action, Action::Spawn { .. }),
            "{name}: spawn first keeps spawn-before-branch"
        );
        assert!(
            matches!(wf.steps[1].action, Action::Branch { .. }),
            "{name}: the branch follows the spawn"
        );
        assert!(
            unsupported_reason(&wf).is_none(),
            "{name}: refused: {:?}",
            unsupported_reason(&wf)
        );
    }
}

/// The shipped `agent-reach-upgrade` fail-closed arm reaches a FAILED run, not
/// a green one: the successful `send_dm` is not the last step, and the
/// deterministic `verify: "false"` with `on_error: fail_run` emits the
/// failure bundle (bd-durable-branch-hhgc, replacing the unportable
/// `call_webhook`).
#[test]
fn shipped_agent_reach_else_arm_fails_closed() {
    let parse = || {
        toron_workflow::schema::parse_workflow(include_str!(
            "../../../workflows/agent-reach-upgrade.yaml"
        ))
        .expect("parse agent-reach-upgrade")
    };

    // verified = false → else arm → notify-fail succeeds → fail-closed fails.
    let model = RunModel::new("run-reach", parse(), json!({}));
    let spec = spawn_spec("run-reach", "upgrade", true, 600);
    let (model, effects) = advance_with(&model, &RunMsg::Start { now_ms: 0 }, &[Some(spec)]);
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::StartSpawn { .. }, ..]
    ));
    let (model, effects) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "upgrade".into(),
            value: json!({"verified": false}),
            now_ms: 1_000,
        },
        &[],
    );
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("the else arm's notification runs: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("notify-fail"));

    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 2_000,
        },
    );
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("a second else-arm step must follow, or the run goes green: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("fail-closed"));

    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "error": "exit status 1"}),
            now_ms: 3_000,
        },
    );
    let [RunEffect::Fail { step, output }] = effects.as_slice() else {
        panic!("the else arm fails the run: {effects:?}");
    };
    assert_eq!(step, "fail-closed");
    assert_eq!(output["status"], json!("failed"));
    assert_eq!(output["failed_step"], json!("fail-closed"));
    assert!(matches!(model.phase, RunPhase::Failed { .. }));

    // verified = true → then arm → notify-ok → green.
    let model = RunModel::new("run-reach-ok", parse(), json!({}));
    let spec = spawn_spec("run-reach-ok", "upgrade", true, 600);
    let (model, _) = advance_with(&model, &RunMsg::Start { now_ms: 0 }, &[Some(spec)]);
    let (model, effects) = advance_with(
        &model,
        &RunMsg::SpawnSettled {
            node: "upgrade".into(),
            value: json!({"verified": true}),
            now_ms: 1_000,
        },
        &[],
    );
    let [RunEffect::ExecuteStep { step, .. }] = effects.as_slice() else {
        panic!("the then arm runs: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("notify-ok"));
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 2_000,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.phase, RunPhase::Succeeded);
}

// ---------------------------------------------------------------------------
// Parallel fan-out lowered onto the graph (bd-durable-parallel-53um)
// ---------------------------------------------------------------------------

fn parallel(name: &str, over: &str, each: Step, on_error: StepOnError) -> Step {
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

fn fanout_workflow(over: &str, copy_on_error: StepOnError, join_on_error: StepOnError) -> Workflow {
    workflow(vec![
        parallel(
            "fan",
            over,
            named("copy", dm("copy"), copy_on_error),
            join_on_error,
        ),
        named("after", dm("after"), StepOnError::Continue),
    ])
}

/// The lowered graph model the driver opens: `durable_graph` then `new` with
/// the graph set, exactly as `DurableGraphRun::open` does.
fn lowered_model(run_id: &str, workflow: Workflow, trigger: serde_json::Value) -> GraphModel {
    let graph = durable_graph(&workflow, &trigger)
        .expect("lower")
        .expect("a graph workflow");
    let mut lowered = workflow;
    lowered.graph = Some(graph);
    GraphModel::new(run_id, lowered, trigger)
}

/// Settle the named copies in order, returning the last transition's model and
/// effects.
fn settle_copies(
    model: &GraphModel,
    copies: &[&str],
    value: serde_json::Value,
    now_ms: u64,
) -> (GraphModel, Vec<GraphEffect>) {
    let mut model = model.clone();
    let mut effects = Vec::new();
    for (index, copy) in copies.iter().enumerate() {
        let (next, follow) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: (*copy).into(),
                value: value.clone(),
                now_ms: now_ms + index as u64,
            },
        );
        model = next;
        effects = follow;
    }
    (model, effects)
}

/// Three copies dispatch in ONE wave, each exactly once, then the join collects
/// and the next step runs once.
#[test]
fn parallel_fanout_runs_each_copy_once() {
    let model = lowered_model(
        "run-par",
        fanout_workflow("3", StepOnError::Continue, StepOnError::Continue),
        json!({}),
    );
    assert_eq!(model.phase, GraphPhase::Ready);
    let (model, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    assert_eq!(executed(&effects), ["fan:0", "fan:1", "fan:2"]);
    assert!(
        effects.iter().all(|e| matches!(
            e,
            GraphEffect::ExecuteNode {
                wave: 1,
                attempt: 1,
                ..
            }
        )),
        "one wave, first attempt: {effects:?}"
    );

    // The first two settlements are still mid-wave: no join, no next step.
    let (model, effects) = settle_copies(&model, &["fan:0", "fan:1"], json!({"ok": true}), 2_000);
    assert!(
        effects.is_empty(),
        "a partial wave emits nothing: {effects:?}"
    );
    assert!(
        !model.outputs.contains_key("fan"),
        "no join on a partial wave"
    );

    let (model, effects) = settle_copies(&model, &["fan:2"], json!({"ok": true}), 3_000);
    assert_eq!(
        executed(&effects),
        ["after"],
        "the join settles inline and the next step dispatches: {effects:?}"
    );
    assert_eq!(
        model.outputs["fan"],
        json!([{"ok": true}, {"ok": true}, {"ok": true}]),
        "outputs={:?}",
        model.outputs
    );
    assert_eq!(model.node_state["fan"], NodeState::Settled);

    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::NodeDone {
            node: "after".into(),
            value: json!({"ok": true, "sent": true}),
            now_ms: 4_000,
        },
    );
    let [GraphEffect::Complete { output }] = effects.as_slice() else {
        panic!("the run completes: {effects:?}");
    };
    assert_eq!(
        output["fan"],
        json!([{"ok": true}, {"ok": true}, {"ok": true}])
    );
    assert_eq!(output["after"], json!({"ok": true, "sent": true}));
    assert_eq!(model.phase, GraphPhase::Succeeded);
}

/// A restart re-issues only the copies still executing; settled copies never
/// re-dispatch.
#[test]
fn parallel_restart_reruns_only_pending_copies() {
    let model = lowered_model(
        "run-par-restart",
        fanout_workflow("3", StepOnError::Continue, StepOnError::Continue),
        json!({}),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], json!({"ok": true, "copy": 0}), 2_000);

    let json_text = serde_json::to_string(&model).expect("serialize");
    let restored: GraphModel = serde_json::from_str(&json_text).expect("restore");
    let effects = resume_graph_effects(&restored);
    assert_eq!(
        executed(&effects),
        ["fan:1", "fan:2"],
        "only the pending copies re-issue"
    );
    assert!(
        !executed(&effects).contains(&"fan:0"),
        "the settled copy is never re-dispatched"
    );
    assert_eq!(restored.outputs["fan:0"], json!({"ok": true, "copy": 0}));
}

/// The join waits for a partial wave across a restart: no join effect and no
/// re-dispatch until the last copy settles, then it collects in authored order.
#[test]
fn parallel_join_waits_on_a_partial_wave() {
    let model = lowered_model(
        "run-par-wait",
        fanout_workflow("2", StepOnError::Continue, StepOnError::Continue),
        json!({}),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], json!({"ok": true, "copy": 0}), 2_000);

    let restored: GraphModel =
        serde_json::from_str(&serde_json::to_string(&model).expect("serialize")).expect("restore");
    let effects = resume_graph_effects(&restored);
    assert_eq!(executed(&effects), ["fan:1"]);

    // A wake while a copy is in flight must not fire the join.
    let (restored, effects) = advance_graph(&restored, &GraphMsg::Woke { now_ms: 3_000 });
    assert!(
        effects.is_empty(),
        "no join effect on a partial wave: {effects:?}"
    );
    assert!(!restored.outputs.contains_key("fan"));

    let (restored, effects) =
        settle_copies(&restored, &["fan:1"], json!({"ok": true, "copy": 1}), 4_000);
    assert_eq!(executed(&effects), ["after"]);
    assert_eq!(
        restored.outputs["fan"],
        json!([{"ok": true, "copy": 0}, {"ok": true, "copy": 1}]),
        "authored order, settled values only"
    );
    assert!(!restored.outputs.contains_key("fan:0:dup"));
}

/// A `continue` copy failure lands its FULL value in the collected envelope;
/// the join step's `fail_run` then fails the run with the bundle.
#[test]
fn parallel_copy_failure_continue_lands_bundle() {
    let model = lowered_model(
        "run-par-fail",
        fanout_workflow("2", StepOnError::Continue, StepOnError::Continue),
        json!({}),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let bundle = json!({
        "ok": false,
        "kind": "timeout",
        "error": "copy timed out",
        "retryable": true,
    });
    let (model, _) = settle_copies(&model, &["fan:0"], bundle.clone(), 2_000);
    let (model, effects) = settle_copies(&model, &["fan:1"], json!({"ok": true}), 3_000);
    assert_eq!(
        executed(&effects),
        ["after"],
        "continue keeps the run going"
    );
    let collected = &model.outputs["fan"];
    assert_eq!(collected["ok"], json!(false));
    assert_eq!(collected["error"], json!("copy timed out"));
    assert_eq!(
        collected["results"][0], bundle,
        "the full failure bundle rides the results"
    );

    // The same collected value with `fail_run` on the join fails the run.
    let model = lowered_model(
        "run-par-fail-run",
        fanout_workflow("2", StepOnError::Continue, StepOnError::FailRun),
        json!({}),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], bundle.clone(), 2_000);
    let (model, effects) = settle_copies(&model, &["fan:1"], json!({"ok": true}), 3_000);
    let [GraphEffect::Fail { fail }] = effects.as_slice() else {
        panic!("the join's fail_run fails the run: {effects:?}");
    };
    assert_eq!(fail.node, "fan");
    assert_eq!(fail.error, "copy timed out");
    assert_eq!(fail.kind, "timeout");
    assert!(matches!(model.phase, GraphPhase::Failed { .. }));
}

/// Whole-workflow refusals: the mix with spawn/branch, an unsequencable copy,
/// and a count that does not resolve at open.
#[test]
fn parallel_refuses_mixes_and_unsequencable_copies() {
    // Mix: spawn + parallel.
    let mixed = workflow(vec![
        named("spawn_it", spawn_action(true, 600), StepOnError::Continue),
        parallel(
            "fan",
            "2",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
    ]);
    assert!(needs_graph_model(&mixed));
    let reason = unsupported_reason(&mixed).expect("the mix refuses");
    assert!(reason.contains("mixes graph-lowered steps"), "{reason}");
    let reason = durable_graph(&mixed, &json!({})).expect_err("lowering refuses the mix");
    assert!(reason.contains("mixes graph-lowered steps"), "{reason}");

    // Mix: branch + parallel (the linear branch is the same refusal).
    let mixed = workflow(vec![
        branch(
            "{{trigger.gate}} == true",
            vec![named("yes", dm("yes"), StepOnError::Continue)],
            vec![],
            StepOnError::Continue,
        ),
        parallel(
            "fan",
            "2",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
    ]);
    let reason = durable_graph(&mixed, &json!({})).expect_err("branch + parallel refuses");
    assert!(reason.contains("mixes graph-lowered steps"), "{reason}");

    // Unsequencable copy action.
    let bad = workflow(vec![parallel(
        "fan",
        "2",
        step(
            Action::CallWebhook {
                url: "http://127.0.0.1:9/x".into(),
                method: None,
                body: None,
            },
            StepOnError::Continue,
        ),
        StepOnError::Continue,
    )]);
    let reason = unsupported_reason(&bad).expect("call_webhook copy refuses");
    assert!(reason.contains("call_webhook"), "{reason}");

    // A count that cannot resolve at open.
    let unresolvable = workflow(vec![parallel(
        "fan",
        "{{inputs.n}}",
        named("copy", dm("copy"), StepOnError::Continue),
        StepOnError::Continue,
    )]);
    assert!(
        unsupported_reason(&unresolvable).is_none(),
        "the shape route admits it; the count resolves at open"
    );
    let reason = durable_graph(&unresolvable, &json!({})).expect_err("no count at open");
    assert!(
        reason.contains("must resolve to a positive count"),
        "{reason}"
    );
    assert!(
        durable_graph(&unresolvable, &json!({"inputs": {"n": 2}})).is_ok(),
        "a trigger-carried count resolves at open"
    );
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

fn foreach_workflow(
    over: &str,
    item_on_error: StepOnError,
    join_on_error: StepOnError,
) -> Workflow {
    workflow(vec![
        foreach_step(
            "fan",
            over,
            named("copy", dm("copy"), item_on_error),
            join_on_error,
        ),
        named("after", dm("after"), StepOnError::Continue),
    ])
}

fn with_items() -> serde_json::Value {
    json!({"inputs": {"items": ["a", "b", "c"]}})
}

fn dispatched_items(effects: &[GraphEffect]) -> Vec<Option<serde_json::Value>> {
    effects
        .iter()
        .filter_map(|e| match e {
            GraphEffect::ExecuteNode { item, .. } => Some(item.clone()),
            _ => None,
        })
        .collect()
}

/// The list expands into one named node per item, all in one wave, each
/// carrying its item.
#[test]
fn foreach_expands_list_to_named_nodes() {
    let model = lowered_model(
        "run-foreach",
        foreach_workflow(
            "{{inputs.items}}",
            StepOnError::Continue,
            StepOnError::Continue,
        ),
        with_items(),
    );
    assert!(
        needs_graph_model(&model.workflow),
        "a foreach body routes to the graph model"
    );
    let (model, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    assert_eq!(executed(&effects), ["fan:0", "fan:1", "fan:2"]);
    assert_eq!(
        dispatched_items(&effects),
        [Some(json!("a")), Some(json!("b")), Some(json!("c"))]
    );
    assert!(
        effects
            .iter()
            .all(|e| matches!(e, GraphEffect::ExecuteNode { wave: 1, .. })),
        "one wave: {effects:?}"
    );
    assert_eq!(model.items["fan:1"], json!("b"));

    let (model, effects) = settle_copies(
        &model,
        &["fan:0", "fan:1", "fan:2"],
        json!({"ok": true}),
        2_000,
    );
    assert_eq!(
        executed(&effects),
        ["after"],
        "the join waits for every item"
    );
    assert_eq!(
        model.outputs["fan"],
        json!([{"ok": true}, {"ok": true}, {"ok": true}])
    );
}

/// A restart keeps the settled items and the per-copy item data; only the
/// pending copies re-issue.
#[test]
fn foreach_restart_keeps_settled_items() {
    let model = lowered_model(
        "run-foreach-restart",
        foreach_workflow(
            "{{inputs.items}}",
            StepOnError::Continue,
            StepOnError::Continue,
        ),
        with_items(),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], json!({"ok": true, "i": 0}), 2_000);

    let restored: GraphModel =
        serde_json::from_str(&serde_json::to_string(&model).expect("serialize")).expect("restore");
    let effects = resume_graph_effects(&restored);
    assert_eq!(executed(&effects), ["fan:1", "fan:2"]);
    assert_eq!(
        dispatched_items(&effects),
        [Some(json!("b")), Some(json!("c"))],
        "the item data survives the restart"
    );
    assert_eq!(restored.outputs["fan:0"], json!({"ok": true, "i": 0}));
    assert_eq!(restored.items["fan:2"], json!("c"));
}

/// An item failure applies the copy's `on_error`; the join's `fail_run` turns
/// the collected envelope into a failed run.
#[test]
fn foreach_item_failure_applies_on_error() {
    let bundle = json!({"ok": false, "kind": "timeout", "error": "item timed out"});
    let model = lowered_model(
        "run-foreach-fail",
        foreach_workflow(
            "{{inputs.items}}",
            StepOnError::Continue,
            StepOnError::Continue,
        ),
        with_items(),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], bundle.clone(), 2_000);
    let (model, effects) = settle_copies(&model, &["fan:1", "fan:2"], json!({"ok": true}), 3_000);
    assert_eq!(
        executed(&effects),
        ["after"],
        "continue keeps the run going"
    );
    let collected = &model.outputs["fan"];
    assert_eq!(collected["ok"], json!(false));
    assert_eq!(collected["error"], json!("item timed out"));
    assert_eq!(collected["results"][0], bundle);

    // The join's fail_run fails the run with the item's kind.
    let model = lowered_model(
        "run-foreach-fail-run",
        foreach_workflow(
            "{{inputs.items}}",
            StepOnError::Continue,
            StepOnError::FailRun,
        ),
        with_items(),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, _) = settle_copies(&model, &["fan:0"], bundle, 2_000);
    let (model, effects) = settle_copies(&model, &["fan:1", "fan:2"], json!({"ok": true}), 3_000);
    let [GraphEffect::Fail { fail }] = effects.as_slice() else {
        panic!("the join's fail_run fails the run: {effects:?}");
    };
    assert_eq!(fail.node, "fan");
    assert_eq!(fail.kind, "timeout");
    assert!(matches!(model.phase, GraphPhase::Failed { .. }));
}

/// A list that cannot be materialized refuses whole before any row: a
/// non-list source, a missing open-resolvable value, and an iterator that
/// names a step which is not upstream.
#[test]
fn foreach_unmaterializable_iterator_refuses_without_row() {
    // Literal, but not a list.
    let wf = foreach_workflow("42", StepOnError::Continue, StepOnError::Continue);
    let reason = durable_graph(&wf, &json!({})).expect_err("a number is not a list");
    assert!(reason.contains("must resolve to a list"), "{reason}");

    // A trigger value that is not a list.
    let wf = foreach_workflow("{{inputs.n}}", StepOnError::Continue, StepOnError::Continue);
    let reason =
        durable_graph(&wf, &json!({"inputs": {"n": 3}})).expect_err("a count is not a list");
    assert!(reason.contains("must resolve to a list"), "{reason}");

    // A trigger ref that does not resolve at open.
    let reason = durable_graph(&wf, &json!({})).expect_err("missing list");
    assert!(reason.contains("does not resolve at open"), "{reason}");

    // A non-template, non-JSON spelling.
    let wf = foreach_workflow("not a list", StepOnError::Continue, StepOnError::Continue);
    let reason = durable_graph(&wf, &json!({})).expect_err("not a ref");
    assert!(reason.contains("must be a JSON list"), "{reason}");

    // Upstream output: allowed only for an EARLIER step.
    let upstream = workflow(vec![
        named("plan", dm("plan"), StepOnError::Continue),
        foreach_step(
            "fan",
            "{{steps.plan.items}}",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
    ]);
    assert!(
        durable_graph(&upstream, &json!({})).is_ok(),
        "an upstream list is materialized by the fold"
    );
    let forward = workflow(vec![
        foreach_step(
            "fan",
            "{{steps.later.items}}",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
        named("later", dm("later"), StepOnError::Continue),
    ]);
    let reason = durable_graph(&forward, &json!({})).expect_err("not earlier");
    assert!(reason.contains("not an earlier step"), "{reason}");

    // The mix with spawn refuses whole (same rule as parallel).
    let mixed = workflow(vec![
        named("spawn_it", spawn_action(true, 600), StepOnError::Continue),
        foreach_step(
            "fan",
            "{{inputs.items}}",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
    ]);
    let reason = durable_graph(&mixed, &json!({})).expect_err("the mix refuses");
    assert!(
        reason.contains("mixes graph-lowered steps with 'spawn'"),
        "{reason}"
    );
}

// ---------------------------------------------------------------------------
// Retry policy on graph nodes (bd-durable-retry-policy-rrck)
// ---------------------------------------------------------------------------

fn retry_step(
    name: &str,
    inner: Step,
    max_attempts: u32,
    backoff_secs: u64,
    on_error: StepOnError,
) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::Retry {
            max_attempts,
            backoff_secs,
            step: Box::new(inner),
        },
    }
}

fn retry_workflow(max_attempts: u32, on_error: StepOnError) -> Workflow {
    workflow(vec![retry_step(
        "flaky",
        named("call", dm("call"), StepOnError::Continue),
        max_attempts,
        0,
        on_error,
    )])
}

/// A `retry:` policy retries ANY failure until `max_attempts`, then the
/// wrapper's `on_error` fails the run with the attempt count.
#[test]
fn retry_policy_exhaustion_reports_attempts() {
    let model = lowered_model(
        "run-retry",
        retry_workflow(2, StepOnError::FailRun),
        json!({}),
    );
    assert!(
        needs_graph_model(&model.workflow),
        "retry runs on the graph"
    );
    let (model, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let [
        GraphEffect::ExecuteNode {
            node, attempt: 1, ..
        },
    ] = effects.as_slice()
    else {
        panic!("attempt 1 dispatches: {effects:?}");
    };
    assert_eq!(node, "flaky");

    let failure = json!({"ok": false, "kind": "rate_limit", "error": "slow down"});
    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::NodeDone {
            node: "flaky".into(),
            value: failure.clone(),
            now_ms: 2_000,
        },
    );
    let [
        GraphEffect::ParkUntil {
            reason: NodeParkReason::Retry,
            until_ms,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("a failed attempt parks for retry: {effects:?}");
    };
    let until = *until_ms;

    let (model, effects) = advance_graph(&model, &GraphMsg::Woke { now_ms: until });
    let [
        GraphEffect::ExecuteNode {
            node, attempt: 2, ..
        },
    ] = effects.as_slice()
    else {
        panic!("attempt 2 re-dispatches: {effects:?}");
    };
    assert_eq!(node, "flaky");

    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::NodeDone {
            node: "flaky".into(),
            value: failure,
            now_ms: until + 1_000,
        },
    );
    let [GraphEffect::Fail { fail }] = effects.as_slice() else {
        panic!("exhaustion fails the run: {effects:?}");
    };
    assert_eq!(fail.node, "flaky");
    assert_eq!(fail.kind, "rate_limit");
    assert_eq!(fail.attempts, Some(2), "the outer attempt count");
    assert!(matches!(model.phase, GraphPhase::Failed { .. }));
}

/// The attempt and the backoff park survive a restart: resume re-issues the
/// park (never a second dispatch of the same attempt), and the wake re-emits
/// `ExecuteNode` with the SAME attempt identity.
#[test]
fn retry_backoff_park_is_restart_safe() {
    let model = lowered_model(
        "run-retry-restart",
        retry_workflow(3, StepOnError::FailRun),
        json!({}),
    );
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::NodeDone {
            node: "flaky".into(),
            value: json!({"ok": false, "kind": "timeout", "error": "late"}),
            now_ms: 2_000,
        },
    );
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
    assert!(matches!(
        model.node_state["flaky"],
        NodeState::Parked { attempt: 2, .. }
    ));

    let restored: GraphModel =
        serde_json::from_str(&serde_json::to_string(&model).expect("serialize")).expect("restore");
    let effects = resume_graph_effects(&restored);
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
    assert_eq!(*resumed_until, *until_ms);

    let (restored, effects) = advance_graph(&restored, &GraphMsg::Woke { now_ms: *until_ms });
    let [
        GraphEffect::ExecuteNode {
            node,
            attempt: 2,
            inputs,
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("the wake re-dispatches attempt 2: {effects:?}");
    };
    assert_eq!(node, "flaky");
    assert!(inputs.is_empty(), "the first-step snapshot is unchanged");

    // A success on the last attempt completes the run.
    let (restored, effects) = advance_graph(
        &restored,
        &GraphMsg::NodeDone {
            node: "flaky".into(),
            value: json!({"ok": true, "done": true}),
            now_ms: *until_ms + 1_000,
        },
    );
    assert!(matches!(effects.as_slice(), [GraphEffect::Complete { .. }]));
    assert_eq!(restored.outputs["flaky"], json!({"ok": true, "done": true}));
}

/// An unparseable policy refuses whole: zero attempts, over the cap, a nested
/// wrapper, a wrapper around a graph-lowered shape, and the spawn mix.
#[test]
fn retry_unparseable_policy_refuses_without_row() {
    for (max_attempts, label) in [(0u32, "at least 1"), (11u32, "exceeds the bounded cap")] {
        let wf = retry_workflow(max_attempts, StepOnError::Continue);
        let reason = durable_graph(&wf, &json!({})).expect_err("bad policy");
        assert!(reason.contains(label), "{max_attempts}: {reason}");
        assert!(unsupported_reason(&wf).is_some(), "{max_attempts}");
    }

    // Nested retry.
    let nested = workflow(vec![retry_step(
        "outer",
        retry_step(
            "inner",
            named("call", dm("call"), StepOnError::Continue),
            2,
            0,
            StepOnError::Continue,
        ),
        2,
        0,
        StepOnError::Continue,
    )]);
    let reason = durable_graph(&nested, &json!({})).expect_err("nested");
    assert!(reason.contains("nested retry"), "{reason}");

    // A wrapper around a graph-lowered shape.
    let around_parallel = workflow(vec![retry_step(
        "outer",
        parallel(
            "fan",
            "2",
            named("copy", dm("copy"), StepOnError::Continue),
            StepOnError::Continue,
        ),
        2,
        0,
        StepOnError::Continue,
    )]);
    let reason = durable_graph(&around_parallel, &json!({})).expect_err("fan-out wrap");
    assert!(reason.contains("retry wraps 'parallel'"), "{reason}");

    // The spawn mix refuses whole.
    let mixed = workflow(vec![
        named("spawn_it", spawn_action(true, 600), StepOnError::Continue),
        retry_step(
            "flaky",
            named("call", dm("call"), StepOnError::Continue),
            2,
            0,
            StepOnError::Continue,
        ),
    ]);
    let reason = durable_graph(&mixed, &json!({})).expect_err("mix");
    assert!(
        reason.contains("mixes graph-lowered steps with 'spawn'"),
        "{reason}"
    );
}

// ---------------------------------------------------------------------------
// call_workflow as a child run (bd-durable-call-workflow-mg6y)
// ---------------------------------------------------------------------------

fn call_step(name: &str, callee: &str, inputs: serde_json::Value, on_error: StepOnError) -> Step {
    Step {
        name: Some(name.into()),
        guard: None,
        on_error,
        build_slot: false,
        action: Action::CallWorkflow {
            workflow: callee.into(),
            inputs,
        },
    }
}

fn call_workflow_model(on_error: StepOnError) -> GraphModel {
    lowered_model(
        "run-call",
        workflow(vec![call_step(
            "call",
            "child",
            json!({"goal": "{{inputs.objective}}"}),
            on_error,
        )]),
        json!({"inputs": {"objective": "ship it"}}),
    )
}

/// The call node awaits a child run whose id and trigger are deterministic;
/// a resume re-emits the same `ChildRun` (re-attach), and the child's terminal
/// joins exactly once.
#[test]
fn call_workflow_node_awaits_child() {
    let model = call_workflow_model(StepOnError::Continue);
    assert!(
        needs_graph_model(&model.workflow),
        "a child call is graph-native"
    );
    let (model, effects) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let [
        GraphEffect::ChildRun {
            node,
            child_run_id,
            workflow: callee,
            trigger,
        },
    ] = effects.as_slice()
    else {
        panic!("the call opens a child run: {effects:?}");
    };
    assert_eq!(node, "call");
    assert_eq!(child_run_id, "run-call:call:1");
    assert_eq!(callee, "child");
    assert_eq!(trigger["inputs"]["goal"], json!("ship it"));

    // A resume re-attaches to the same child instead of opening a second one.
    let restored: GraphModel =
        serde_json::from_str(&serde_json::to_string(&model).expect("serialize")).expect("restore");
    let resumed = resume_graph_effects(&restored);
    let [
        GraphEffect::ChildRun {
            child_run_id: resumed_id,
            trigger: resumed_trigger,
            ..
        },
    ] = resumed.as_slice()
    else {
        panic!("resume re-emits the child run: {resumed:?}");
    };
    assert_eq!(*resumed_id, "run-call:call:1");
    assert_eq!(resumed_trigger["inputs"]["goal"], json!("ship it"));

    let output = json!({"ok": true, "patched": "yes"});
    let (model, effects) = advance_graph(
        &restored,
        &GraphMsg::ChildSucceeded {
            node: "call".into(),
            output: output.clone(),
            now_ms: 2_000,
        },
    );
    let [
        GraphEffect::ChildJoin { node, child_run_id },
        GraphEffect::Complete { .. },
    ] = effects.as_slice()
    else {
        panic!("the child terminal joins then completes: {effects:?}");
    };
    assert_eq!(node, "call");
    assert_eq!(child_run_id, "run-call:call:1");
    assert_eq!(model.outputs["call"], output);
    assert_eq!(model.phase, GraphPhase::Succeeded);
}

/// A failed child carries its FULL bundle to the parent node's `on_error`:
/// `continue` lands it and the run proceeds; `fail_run` fails the run with the
/// bundle's kind.
#[test]
fn call_workflow_child_failure_carries_bundle_to_parent_on_error() {
    let bundle = json!({
        "ok": false,
        "kind": "terminal",
        "error": "child exploded",
        "leaf_preview": "{\"ok\":false}",
    });
    let model = call_workflow_model(StepOnError::Continue);
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::ChildFailed {
            node: "call".into(),
            output: bundle.clone(),
            now_ms: 2_000,
        },
    );
    let [GraphEffect::ChildJoin { .. }, GraphEffect::Complete { .. }] = effects.as_slice() else {
        panic!("continue joins and completes: {effects:?}");
    };
    assert_eq!(model.outputs["call"], bundle, "the full bundle lands");

    let model = call_workflow_model(StepOnError::FailRun);
    let (model, _) = advance_graph(&model, &GraphMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance_graph(
        &model,
        &GraphMsg::ChildFailed {
            node: "call".into(),
            output: bundle,
            now_ms: 2_000,
        },
    );
    let [GraphEffect::ChildJoin { .. }, GraphEffect::Fail { fail }] = effects.as_slice() else {
        panic!("fail_run fails the run: {effects:?}");
    };
    assert_eq!(fail.node, "call");
    assert_eq!(fail.kind, "terminal");
    assert_eq!(fail.error, "child exploded");
    assert!(matches!(model.phase, GraphPhase::Failed { .. }));
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

fn loop_model(max: &str, until: Option<&str>, on_error: StepOnError) -> RunModel {
    RunModel::new(
        "run-loop",
        loop_workflow(max, until, on_error),
        serde_json::json!({}),
    )
}

/// The `until` guard is evaluated over the outputs after each body pass; the
/// loop stops on the first true and reports the pass count.
#[test]
fn loop_until_true_after_n_iterations() {
    let model = loop_model("5", Some("steps_check_ok == true"), StepOnError::Continue);
    assert!(
        unsupported_reason(&model.workflow).is_none(),
        "bounded loop admits"
    );
    let (model, effects) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let [
        RunEffect::ExecuteStep {
            step,
            iteration: Some(1),
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("pass 1 runs the body: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("check"));

    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 2_000,
        },
    );
    let [
        RunEffect::ExecuteStep {
            iteration: Some(2), ..
        },
    ] = effects.as_slice()
    else {
        panic!("pass 2 runs the body: {effects:?}");
    };
    assert_eq!(model.loops["spin"].iteration, 2);

    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 3_000,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(
        model.outputs["spin"],
        json!({"ok": true, "iterations": 2, "stopped": "until"})
    );
    assert_eq!(model.phase, RunPhase::Succeeded);
}

/// The counter persists: a restart resumes at the same pass with the same
/// iteration key, never replaying the settled body pass.
#[test]
fn loop_restart_resumes_at_counter() {
    let model = loop_model("5", Some("steps_check_ok == true"), StepOnError::Continue);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 2_000,
        },
    );
    assert!(matches!(
        effects.as_slice(),
        [RunEffect::ExecuteStep {
            iteration: Some(2),
            ..
        }]
    ));

    let restored: RunModel =
        serde_json::from_str(&serde_json::to_string(&model).expect("serialize")).expect("restore");
    assert_eq!(restored.loops["spin"].iteration, 2, "the counter persists");
    let effects = resume_effects(&restored);
    let [
        RunEffect::ExecuteStep {
            step,
            iteration: Some(2),
            ..
        },
    ] = effects.as_slice()
    else {
        panic!("resume re-issues pass 2: {effects:?}");
    };
    assert_eq!(step.name.as_deref(), Some("check"));

    let (restored, effects) = advance(
        &restored,
        &RunMsg::StepDone {
            value: json!({"ok": true}),
            now_ms: 3_000,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(
        restored.outputs["spin"],
        json!({"ok": true, "iterations": 2, "stopped": "until"})
    );
}

/// Exhaustion applies the loop step's `on_error` with the full bundle.
#[test]
fn loop_max_iterations_fails_with_bundle() {
    let model = loop_model("2", Some("steps_check_ok == true"), StepOnError::FailRun);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let (model, _) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 2_000,
        },
    );
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 3_000,
        },
    );
    let [RunEffect::Fail { step, output }] = effects.as_slice() else {
        panic!("exhaustion fails the run: {effects:?}");
    };
    assert_eq!(step, "spin");
    assert_eq!(output["status"], json!("failed"));
    assert_eq!(output["failed_step"], json!("spin"));
    assert_eq!(output["iterations"], json!(2));
    assert!(
        output["error"]
            .as_str()
            .is_some_and(|e| e.contains("exhausted after 2 iterations")),
        "{output}"
    );
    assert!(matches!(model.phase, RunPhase::Failed { .. }));

    // `continue` lands the exhaustion value and the run proceeds.
    let model = loop_model("2", Some("steps_check_ok == true"), StepOnError::Continue);
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let (model, _) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 2_000,
        },
    );
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false}),
            now_ms: 3_000,
        },
    );
    assert!(matches!(effects.as_slice(), [RunEffect::Complete { .. }]));
    assert_eq!(model.outputs["spin"]["ok"], json!(false));
    assert_eq!(model.outputs["spin"]["iterations"], json!(2));
    assert_eq!(model.phase, RunPhase::Succeeded);
}

/// A failing body step follows its own carrier: `continue` keeps looping,
/// `fail_run` fails the run with that step's bundle.
#[test]
fn loop_body_failure_applies_on_error() {
    // A `continue` body failure lands and the loop keeps its own policy.
    let model = RunModel::new(
        "run-loop-body",
        workflow(vec![loop_step(
            "spin",
            "3",
            None,
            vec![named("check", dm("check"), StepOnError::Continue)],
            StepOnError::Continue,
        )]),
        serde_json::json!({}),
    );
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "error": "flaky"}),
            now_ms: 2_000,
        },
    );
    assert!(
        matches!(
            effects.as_slice(),
            [RunEffect::ExecuteStep {
                iteration: Some(2),
                ..
            }]
        ),
        "a continue body failure does not stop the loop: {effects:?}"
    );
    assert_eq!(
        model.outputs["check"],
        json!({"ok": false, "error": "flaky"})
    );

    // A `fail_run` body failure ends the run at the body step.
    let model = RunModel::new(
        "run-loop-body-fail",
        workflow(vec![loop_step(
            "spin",
            "3",
            None,
            vec![named("check", dm("check"), StepOnError::FailRun)],
            StepOnError::Continue,
        )]),
        serde_json::json!({}),
    );
    let (model, _) = advance(&model, &RunMsg::Start { now_ms: 1_000 });
    let (model, effects) = advance(
        &model,
        &RunMsg::StepDone {
            value: json!({"ok": false, "kind": "terminal", "error": "broke"}),
            now_ms: 2_000,
        },
    );
    let [RunEffect::Fail { step, output }] = effects.as_slice() else {
        panic!("the body's fail_run ends the run: {effects:?}");
    };
    assert_eq!(step, "check");
    assert_eq!(output["error"], json!("broke"));
    assert!(matches!(model.phase, RunPhase::Failed { .. }));
}
