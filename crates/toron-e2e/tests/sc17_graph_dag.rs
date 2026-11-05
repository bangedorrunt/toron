// governed-by: ADR-0005 (P5.5 — Behavior Test Matrix row `graph_dag`)
#![forbid(unsafe_code)]

//! SC-17 — `graph:` DAG runs on the durable host (Behavior Test Matrix row
//! `graph_dag`, ADR-0005 Spec 4b).
//!
//! A `graph:` workflow with independent nodes + a dependency chain runs
//! through `DurableGraphRun` over a temp sqlite pool — no server. Every
//! node's output lands under its node name; a dependent node only runs after
//! its `needs`. Waves fan out in `BTreeMap` order; each node executes once
//! through the leaf boundary with the wave's outputs snapshot. This is the
//! SC-style companion to the `lint_cyclic` unit test: lint rejects bad graphs
//! at rest, this proves a good graph RUNS.
//!
//! Gate: `cargo test -p toron-e2e --test sc17_graph_dag`

#[path = "common/mod.rs"]
mod common;

use common::durable;
use toron_core::kernel::OurEngine;
use toron_workflow::engine::durable::GraphEffect;
use toron_workflow::engine::durable_host::DurableGraphRun;
use toron_workflow::engine::linear_step::{GraphNodeStep, run_graph_node_step};
use toron_workflow::schema::{Action, parse_workflow};

/// Returns `{n: 42}` for `send_dm` (a value-returning action — P0.1), Null
/// otherwise. Same runner contract as the pre-port scenario.
fn value_runner() -> toron_workflow::engine::AsyncEffectRunner {
    use std::sync::Arc;
    Arc::new(|inv: &toron_workflow::engine::ActionInvocation| {
        let value = match &inv.action {
            Action::SendDm { .. } => serde_json::json!({ "n": 42 }),
            _ => serde_json::Value::Null,
        };
        Box::pin(async move { Ok(value) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

#[tokio::test]
async fn graph_dag_runs_on_durable_host() {
    let (pool, db) = durable::test_pool("sc17").await;
    let seams = durable::seams(value_runner());
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
    let trigger = serde_json::json!({});
    let digest = durable::digest(&wf);

    let (engine_store, journal, state) = durable::handles(&pool);
    let (wf_open, trigger_open) = (wf.clone(), trigger.clone());
    let (mut run, mut effects) = tokio::task::spawn_blocking(move || {
        let engine = OurEngine::with_journal(engine_store, journal);
        DurableGraphRun::open(
            engine,
            state,
            "sc17-graph-1",
            wf_open,
            trigger_open,
            durable::now_ms(),
        )
    })
    .await
    .expect("open task")
    .expect("open graph run");

    let mut executed: Vec<String> = Vec::new();
    let output = loop {
        if effects.is_empty() {
            panic!("drive ran out of effects while non-terminal");
        }
        let mut follow: Vec<GraphEffect> = Vec::new();
        let mut done = None;
        for effect in std::mem::take(&mut effects) {
            match effect {
                GraphEffect::ExecuteNode {
                    node,
                    wave: _,
                    attempt,
                    inputs,
                    item,
                } => {
                    executed.push(node.clone());
                    let node_def = wf
                        .graph
                        .as_ref()
                        .and_then(|g| g.nodes.get(&node))
                        .cloned()
                        .unwrap_or_else(|| panic!("effect names unknown node {node}"));
                    let snapshot: serde_json::Map<String, serde_json::Value> =
                        inputs.into_iter().collect();
                    let value = run_graph_node_step(
                        GraphNodeStep {
                            run_id: "sc17-graph-1",
                            workflow: &wf.name,
                            workflow_digest: &digest,
                            node_name: &node,
                            node: &node_def,
                            attempt,
                            trigger: &trigger,
                            inputs: &snapshot,
                            item: item.as_ref(),
                        },
                        &seams,
                    )
                    .await;
                    let (back, mut eff) = tokio::task::spawn_blocking(move || {
                        run.node_done(&node, value, durable::now_ms())
                            .map(|eff| (run, eff))
                    })
                    .await
                    .expect("node feed task")
                    .unwrap_or_else(|e| panic!("node feed failed: {e}"));
                    run = back;
                    follow.append(&mut eff);
                }
                GraphEffect::Complete { output } => {
                    done = Some(output);
                    break;
                }
                other => panic!("unexpected graph effect in a leaves-only diamond: {other:?}"),
            }
        }
        if let Some(output) = done {
            break output;
        }
        effects = follow;
    };
    drop(run);

    for node in ["scout", "plan", "fix_a", "fix_b", "ship"] {
        assert!(
            output.get(node).is_some(),
            "graph node {node} produced output: {output}"
        );
    }
    assert_eq!(
        output["scout"]["n"], 42,
        "node value flows through: {output}"
    );
    assert_eq!(
        executed,
        vec!["scout", "plan", "fix_a", "fix_b", "ship"],
        "waves fan out in order, every node once: {executed:?}"
    );

    let _ = std::fs::remove_file(&db);
}
