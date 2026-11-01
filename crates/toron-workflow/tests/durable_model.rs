// governed-by: ADR-0005
//! Pure linear run model: execute, delay/wake, approval, failure policy,
//! cancel, graph fallback, and state round-trip.

use std::collections::BTreeMap;

use serde_json::json;
use toron_workflow::engine::durable::{RunEffect, RunModel, RunMsg, RunPhase, advance};
use toron_workflow::schema::{Action, Graph, Step, StepOnError, Workflow};

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
        &RunMsg::StepSucceeded {
            output: json!({"ok": true}),
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
        &RunMsg::StepFailed {
            error: "exit 1".into(),
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
        &RunMsg::StepFailed {
            error: "exit 1".into(),
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
