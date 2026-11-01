// governed-by: ADR-0005
//! Pure linear run model: execute, delay/wake, approval, failure policy,
//! cancel, graph fallback, and state round-trip.

use std::collections::BTreeMap;

use serde_json::json;
use toron_workflow::engine::durable::{
    GraphEffect, GraphModel, GraphMsg, GraphPhase, NodeParkReason, NodeState, RunEffect, RunModel,
    RunMsg, RunPhase, advance, advance_graph, graph_unsupported_reason,
};
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
