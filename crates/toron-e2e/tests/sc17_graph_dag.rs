// governed-by: ADR-0005 (P5.5 — Behavior Test Matrix row `graph_dag`)
#![forbid(unsafe_code)]

//! SC-17 — `graph:` DAG runs through the real engine (Behavior Test Matrix
//! row `graph_dag`, ADR-0005 Spec 4b).
//!
//! A `graph:` workflow with independent nodes + a dependency chain runs
//! through `WorkflowEngine` (real `Resonate::local`, no mocks). Every node's
//! output lands under `steps.<name>`; a dependent node only runs after its
//! `needs`. This is the SC-style companion to the `lint_cyclic` unit test:
//! lint rejects bad graphs at rest, this proves a good graph RUNS.
//!
//! Gate: `cargo test -p toron-e2e --features p5 --test sc17_graph_dag`

use std::sync::Arc;

use toron_workflow::engine::{ActionRunner, WorkflowEngine};
use toron_workflow::schema::{Action, parse_workflow};

/// Returns `{n: 42}` for `send_dm` (a value-returning action — P0.1), Null
/// otherwise.
fn value_runner() -> ActionRunner {
    Arc::new(|a: &Action| {
        match a {
            Action::SendDm { .. } => Ok(serde_json::json!({ "n": 42 })),
            _ => Ok(serde_json::Value::Null),
        }
    })
}

#[tokio::test]
async fn graph_dag_runs_through_engine() {
    let engine = WorkflowEngine::local(value_runner()).expect("engine");
    let wf = parse_workflow(
        r##"
name: sc17-ship
trigger:
  type: webhook
graph:
  nodes:
    scout: { action: send_dm, to: x, body: y }
    plan:  { action: send_dm, to: x, body: y, needs: [scout] }
    fix_a: { action: send_dm, to: x, body: y, needs: [plan] }
    fix_b: { action: send_dm, to: x, body: y, needs: [plan] }
    ship:  { action: send_dm, to: x, body: y, needs: [fix_a, fix_b] }
"##,
    )
    .expect("parse graph workflow");

    let output = engine
        .run(
            "sc17-graph-1",
            toron_workflow::engine::RunInput {
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("graph run completes");

    // Every node produced output under steps.<name>.
    for node in ["scout", "plan", "fix_a", "fix_b", "ship"] {
        assert!(
            output.get(node).is_some(),
            "graph node {node} produced output: {output}"
        );
    }
    // Value-returning actions flow through the DAG (P0.1 x P5.2).
    assert_eq!(output["scout"]["n"], 42, "node value flows through: {output}");
}
