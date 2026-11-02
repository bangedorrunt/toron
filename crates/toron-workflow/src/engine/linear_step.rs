// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! One linear step, no Resonate Context (bd-linear-step-6j0y).
//!
//! The durable host runs a single HostSink step outside the workflow engine's
//! Context, so everything the engine would read from a `Context` arrives here
//! as data or as an explicit [`LeafSeams`]. The step POLICY is not re-authored:
//! the guard evaluator, the strict renderer, the retry coordinator, the
//! invocation builder, the leaf dispatcher, and the outcome classifier are the
//! same functions the Context path calls, so a linear step and a workflow step
//! reach the same verdict on the same inputs.
//!
//! What is genuinely different is the substrate, not the policy:
//! - the backoff sleep is `tokio::time::sleep` (there is no durable clock to
//!   suspend on outside a run), and
//! - an engine-owned control action (delay, park, approval, branch, retry,
//!   foreach, parallel, reduce, call_workflow, loop, spawn) is REFUSED through
//!   the boundary's own fail-closed answer instead of being sequenced. A linear
//!   host has no scheduler, so quietly sequencing one would be the lie.

use super::control_flow::{
    GuardVerdict, RetryCoordinator, StepOutcome, classify_step_outcome, evaluate_step_guard,
    invocation_for_leaf, skipped_value,
};
use super::leaf::{LeafSeams, dispatch_leaf};
use super::template::{RenderContext, render_action};
use super::{ActionPartition, invoke_compat, partition};

use crate::schema::{Action, Step, StepOnError};

/// One step for a host with no `Context`.
///
/// `index` is the host's own step position: the policy deliberately derives
/// its node path from `step.name` exactly as the Context path does, so
/// `node_path` and `failed_step` read the same on both substrates.
pub struct LinearStep<'a> {
    pub run_id: &'a str,
    pub workflow: &'a str,
    pub workflow_digest: &'a str,
    pub step: &'a Step,
    pub index: usize,
    pub trigger: &'a serde_json::Value,
    pub outputs: &'a mut serde_json::Map<String, serde_json::Value>,
    /// The bounded-loop pass this step belongs to, when it is a loop body step
    /// (bd-loop-rewrites-fap8). Rides the invocation's iteration key so each
    /// pass is a distinct effect.
    pub iteration: Option<u32>,
}

/// Run one linear step to a value.
///
/// Returns the step's value, never a `Result`: the engine's contract is that a
/// failure is a VALUE (`{ok:false,…}` / the `status: failed` fail_run shape),
/// so a linear host reads the same field the workflow engine writes.
pub async fn run_linear_step(input: LinearStep<'_>, seams: &LeafSeams) -> serde_json::Value {
    let LinearStep {
        run_id,
        workflow,
        workflow_digest,
        step,
        index: _,
        trigger,
        outputs,
        iteration,
    } = input;

    let node = step.name.as_deref().unwrap_or("<unnamed>");
    match evaluate_step_guard(step.guard.as_deref(), trigger, outputs, node).await {
        GuardVerdict::Proceed => {}
        GuardVerdict::Skip { reason } => return skipped_value(node, reason),
        GuardVerdict::Fail { error } => {
            return serde_json::json!({ "ok": false, "error": error });
        }
    }

    // No enclosing `Retry` action exists outside a run: the coordinator's
    // default policy (3 attempts, exponential backoff) applies.
    let coordinator = RetryCoordinator::from_enclosing(None);
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        // Render FIRST, like the engine's leaf arm: a runner must see rendered
        // values, and a render failure is a per-step error with
        // workflow/node/field/ref attribution, never a run failure.
        let render_ctx = RenderContext::new(workflow, node);
        let value = match render_action(&step.action, trigger, outputs, &render_ctx) {
            Ok(action) => {
                let iteration_key = iteration.map(|i| format!("i{i}"));
                let invocation = invocation_for_leaf(
                    action,
                    run_id,
                    workflow_digest,
                    node,
                    attempt,
                    // Parity with the Context path: no enclosing `Retry`
                    // action means a per-step policy of one attempt.
                    1,
                    iteration_key.as_deref(),
                );
                match partition(&invocation.action) {
                    ActionPartition::HostSink { sink } => {
                        dispatch_leaf(seams, &invocation, sink).await
                    }
                    // Engine control and unsupported actions get the
                    // boundary's own fail-closed value: a linear host does not
                    // sequence, so it must refuse rather than pretend.
                    _ => invoke_compat(&invocation.action, |_| None),
                }
            }
            Err(e) => serde_json::json!({ "ok": false, "error": format!("render: {e}") }),
        };
        match classify_step_outcome(step, value, attempt, &coordinator) {
            StepOutcome::Done(value) => return value,
            StepOutcome::Retry { backoff } => tokio::time::sleep(backoff).await,
        }
    }
}

/// One graph node, single-shot, with no `Context`.
///
/// Deliberately NOT [`run_linear_step`]: a graph leaf gets exactly ONE dispatch
/// plus scheduler-level retry, while the linear step owns an inner retry loop.
/// Reusing the looping variant here would multiply executions up to 9×.
pub struct GraphNodeStep<'a> {
    /// Run / journal / engine key.
    pub run_id: &'a str,
    /// Workflow name (render attribution + node path scope).
    pub workflow: &'a str,
    /// Canonical workflow digest for run-scoped effect keys.
    pub workflow_digest: &'a str,
    /// Node name (the graph map key; the render/node path).
    pub node_name: &'a str,
    /// The node definition.
    pub node: &'a crate::schema::GraphNode,
    /// Scheduler-level attempt (1-based).
    pub attempt: u32,
    /// Trigger payload.
    pub trigger: &'a serde_json::Value,
    /// The spawn-time outputs snapshot this node renders against.
    pub inputs: &'a serde_json::Map<String, serde_json::Value>,
    /// The foreach item this copy renders as `{{item}}`, when it is one
    /// (bd-durable-foreach-jpgx).
    pub item: Option<&'a serde_json::Value>,
}

/// Run one graph node to a value.
///
/// Same order as the Context path's leaf arm — guard, strict render, one
/// invocation, partition, one dispatch — so a node reaches the same verdict on
/// either substrate.
pub async fn run_graph_node_step(input: GraphNodeStep<'_>, seams: &LeafSeams) -> serde_json::Value {
    // A foreach copy renders against the enclosing trigger plus its item,
    // exactly like `run_foreach`'s per-iteration trigger copy.
    let mut iter_trigger;
    let trigger = match input.item {
        Some(item) => {
            iter_trigger = input.trigger.clone();
            if let Some(obj) = iter_trigger.as_object_mut() {
                obj.insert("item".into(), item.clone());
            }
            &iter_trigger
        }
        None => input.trigger,
    };
    match evaluate_step_guard(
        input.node.guard.as_deref(),
        trigger,
        input.inputs,
        input.node_name,
    )
    .await
    {
        GuardVerdict::Proceed => {}
        GuardVerdict::Skip { reason } => return skipped_value(input.node_name, reason),
        GuardVerdict::Fail { error } => {
            return serde_json::json!({ "ok": false, "error": error });
        }
    }

    // A `retry:` wrapper is a POLICY, not an action: this call is ONE attempt
    // of the inner step, and the scheduler owns the attempts
    // (bd-durable-retry-policy-rrck). The wrapper's guard already ran above;
    // the inner step's own guard runs per attempt and its failure is a VALUE
    // (Continue) so the wrapper's `on_error` decides exhaustion.
    let mut inner;
    let node_action = match &input.node.action {
        Action::Retry { step, .. } => {
            let mut flat: &Step = step;
            while let Action::Retry { step: nested, .. } = &flat.action {
                flat = nested;
            }
            inner = flat.clone();
            inner.on_error = StepOnError::Continue;
            match evaluate_step_guard(
                inner.guard.as_deref(),
                trigger,
                input.inputs,
                input.node_name,
            )
            .await
            {
                GuardVerdict::Proceed => {}
                GuardVerdict::Skip { reason } => return skipped_value(input.node_name, reason),
                GuardVerdict::Fail { error } => {
                    return serde_json::json!({ "ok": false, "error": error });
                }
            }
            &inner.action
        }
        action => action,
    };

    if let Action::Reduce { .. } = node_action {
        let outputs: std::collections::BTreeMap<String, serde_json::Value> = input
            .inputs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        return crate::engine::reduce::evaluate(
            node_action,
            trigger,
            &outputs,
            input.workflow,
            input.node_name,
        );
    }

    // The planning render in the fold is LENIENT; this one is strict, so an
    // unresolvable ref fails HERE with workflow/node/field/ref attribution
    // rather than reaching a runner as a verbatim placeholder.
    let render_ctx = RenderContext::new(input.workflow, input.node_name);
    let action = match render_action(node_action, trigger, input.inputs, &render_ctx) {
        Ok(action) => action,
        Err(e) => {
            return serde_json::json!({ "ok": false, "error": format!("render: {e}") });
        }
    };
    let invocation = invocation_for_leaf(
        action,
        input.run_id,
        input.workflow_digest,
        input.node_name,
        input.attempt,
        1,
        None,
    );
    match partition(&invocation.action) {
        ActionPartition::HostSink { sink } => dispatch_leaf(seams, &invocation, sink).await,
        // The graph model refuses these before any row; reaching one here is a
        // routing bug, so answer with the boundary's own fail-closed value.
        _ => invoke_compat(&invocation.action, |_| None),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    use crate::engine::{ActionInvocation, RunContext};
    use crate::schema::{Action, StepOnError};

    fn send_dm(body: &str) -> Step {
        Step {
            name: Some("notify".into()),
            guard: None,
            on_error: StepOnError::Continue,
            build_slot: false,
            action: Action::SendDm {
                to: "pi".into(),
                body: body.into(),
                project: None,
            },
        }
    }

    /// The runner sees a RENDERED action and a fully-populated invocation: this
    /// is the surface the durable host hands its journal.
    /// bd-durable-retry-policy-rrck: a `retry:` graph node runs ONE attempt of
    /// the inner step (the scheduler owns attempts), not a refusal.
    #[tokio::test]
    async fn graph_node_unwraps_a_retry_policy() {
        let inner = send_dm("retry-body");
        let node = crate::schema::GraphNode {
            action: Action::Retry {
                max_attempts: 3,
                backoff_secs: 0,
                step: Box::new(inner),
            },
            needs: Vec::new(),
            guard: None,
            on_error: StepOnError::FailRun,
            build_slot: false,
        };
        let trigger = serde_json::json!({});
        let inputs = serde_json::Map::new();
        let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&seen);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |inv: &ActionInvocation| {
                let slot = Arc::clone(&slot);
                let body = match &inv.action {
                    Action::SendDm { body, .. } => body.clone(),
                    other => panic!("runner saw {}", other.type_name()),
                };
                Box::pin(async move {
                    *slot.lock().expect("slot") = Some(body);
                    Ok(serde_json::json!({ "ok": true }))
                })
            })),
            ..LeafSeams::default()
        };
        let value = run_graph_node_step(
            GraphNodeStep {
                run_id: "run-retry",
                workflow: "retry-wf",
                workflow_digest: "digest",
                node_name: "flaky",
                node: &node,
                attempt: 2,
                trigger: &trigger,
                inputs: &inputs,
                item: None,
            },
            &seams,
        )
        .await;
        assert_eq!(value["ok"], serde_json::json!(true));
        assert_eq!(
            seen.lock().expect("seen").as_deref(),
            Some("retry-body"),
            "the inner step ran, not a boundary refusal"
        );
    }

    /// bd-durable-foreach-jpgx: a foreach copy renders `{{item}}` from its
    /// per-copy item, exactly like `run_foreach`'s iteration trigger.
    #[tokio::test]
    async fn graph_node_renders_the_foreach_item() {
        let step = send_dm("item={{item.name}}");
        let node = crate::schema::GraphNode {
            action: step.action.clone(),
            needs: Vec::new(),
            guard: None,
            on_error: StepOnError::Continue,
            build_slot: false,
        };
        let trigger = serde_json::json!({ "source": "test" });
        let inputs = serde_json::Map::new();
        let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&seen);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |inv: &ActionInvocation| {
                let slot = Arc::clone(&slot);
                let body = match &inv.action {
                    Action::SendDm { body, .. } => body.clone(),
                    other => panic!("runner saw {}", other.type_name()),
                };
                Box::pin(async move {
                    *slot.lock().expect("slot") = Some(body);
                    Ok(serde_json::json!({ "ok": true, "sent": true }))
                })
            })),
            ..LeafSeams::default()
        };
        let item = serde_json::json!({ "name": "alpha" });
        let value = run_graph_node_step(
            GraphNodeStep {
                run_id: "run-foreach",
                workflow: "foreach-wf",
                workflow_digest: "digest",
                node_name: "fan:0",
                node: &node,
                attempt: 1,
                trigger: &trigger,
                inputs: &inputs,
                item: Some(&item),
            },
            &seams,
        )
        .await;
        assert_eq!(value["ok"], serde_json::json!(true));
        assert_eq!(
            seen.lock().expect("seen").as_deref(),
            Some("item=alpha"),
            "the copy saw its own item"
        );
    }

    #[tokio::test]
    async fn dispatches_a_rendered_action_with_the_invocation_shape() {
        let step = send_dm("{{trigger.payload}}");
        let trigger = serde_json::json!({ "payload": "rendered-body" });
        let mut outputs = serde_json::Map::new();

        let seen: Arc<Mutex<Option<(String, RunContext, u32)>>> = Arc::new(Mutex::new(None));
        let slot = Arc::clone(&seen);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |inv: &ActionInvocation| {
                let slot = Arc::clone(&slot);
                let body = match &inv.action {
                    Action::SendDm { body, .. } => body.clone(),
                    other => panic!("runner saw {}", other.type_name()),
                };
                let run = inv.run.clone();
                let max_attempts = inv.execution.max_attempts;
                Box::pin(async move {
                    *slot.lock().expect("slot") = Some((body, run, max_attempts));
                    Ok(serde_json::json!({ "ok": true }))
                })
            })),
            ..LeafSeams::default()
        };

        let value = run_linear_step(
            LinearStep {
                run_id: "run-7",
                workflow: "linear-wf",
                workflow_digest: "digest-7",
                step: &step,
                index: 0,
                trigger: &trigger,
                outputs: &mut outputs,
                iteration: None,
            },
            &seams,
        )
        .await;

        assert_eq!(value["ok"], serde_json::json!(true), "{value}");
        let (body, run, max_attempts) = seen
            .lock()
            .expect("slot")
            .clone()
            .expect("the runner saw one invocation");
        assert_eq!(body, "rendered-body", "the runner sees rendered values");
        assert_eq!(run.run_id, "run-7");
        assert_eq!(run.workflow_digest, "digest-7");
        assert_eq!(run.node_path, "notify");
        assert_eq!(run.attempt, 1);
        assert_eq!(
            run.effect_key, None,
            "effect identity is the journal runner's to derive, as on the Context path"
        );
        assert_eq!(max_attempts, 1);
    }

    /// A false guard skips the step WITHOUT dispatching anything.
    #[tokio::test]
    async fn a_false_guard_skips_without_dispatching() {
        let mut step = send_dm("hi");
        step.guard = Some("false".into());
        let trigger = serde_json::json!({});
        let mut outputs = serde_json::Map::new();

        let calls = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&calls);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |_: &ActionInvocation| {
                let counter = Arc::clone(&counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(serde_json::json!({ "ok": true }))
                })
            })),
            ..LeafSeams::default()
        };

        let value = run_linear_step(
            LinearStep {
                run_id: "run-8",
                workflow: "linear-wf",
                workflow_digest: "digest-8",
                step: &step,
                index: 0,
                trigger: &trigger,
                outputs: &mut outputs,
                iteration: None,
            },
            &seams,
        )
        .await;

        assert_eq!(value["skipped"], serde_json::json!(true), "{value}");
        assert_eq!(value["node"], serde_json::json!("notify"), "{value}");
        assert_eq!(calls.load(Ordering::SeqCst), 0, "a skip dispatches nothing");
    }

    /// `on_error: fail_run` on a retryable failure exhausts the default three
    /// attempts and reports them — the same shape the engine returns.
    #[tokio::test]
    async fn fail_run_exhausts_the_default_attempts_and_reports_them() {
        let mut step = send_dm("hi");
        step.on_error = StepOnError::FailRun;
        let trigger = serde_json::json!({});
        let mut outputs = serde_json::Map::new();

        let calls = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&calls);
        let seams = LeafSeams {
            async_runner: Some(Arc::new(move |_: &ActionInvocation| {
                let counter = Arc::clone(&counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    // `retry_after_ms: 0` keeps the exponential backoff at zero
                    // so the test exercises the bound, not the wall clock.
                    Ok(serde_json::json!({
                        "ok": false,
                        "error": "provider down",
                        "kind": "timeout",
                        "retry_after_ms": 0,
                    }))
                })
            })),
            ..LeafSeams::default()
        };

        let value = run_linear_step(
            LinearStep {
                run_id: "run-9",
                workflow: "linear-wf",
                workflow_digest: "digest-9",
                step: &step,
                index: 0,
                trigger: &trigger,
                outputs: &mut outputs,
                iteration: None,
            },
            &seams,
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 3, "the default cap is 3");
        assert_eq!(value["status"], serde_json::json!("failed"), "{value}");
        assert_eq!(value["attempts"], serde_json::json!(3), "{value}");
        assert_eq!(value["failed_step"], serde_json::json!("notify"), "{value}");
        assert_eq!(value["kind"], serde_json::json!("timeout"), "{value}");
        assert_eq!(
            value["error"],
            serde_json::json!("provider down"),
            "{value}"
        );
    }
}
