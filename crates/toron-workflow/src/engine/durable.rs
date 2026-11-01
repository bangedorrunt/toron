// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Pure linear run model for the durable workflow cutover.
//!
//! The Resonate `run_workflow` function cannot be replaced by a compatible
//! async `Context`: our engine schedules runs, it does not replay a function
//! body. This module is the explicit model the ADR calls for:
//!
//! ```text
//! (RunModel, RunMsg) -> (RunModel, Vec<RunEffect>)
//! ```
//!
//! `Delay` parks, `RequestApproval` awaits a decision, every other action is
//! an `ExecuteStep` the host runs through the existing boundary. Graph
//! workflows return [`RunEffect::Unsupported`] so the host keeps them on the
//! old engine until their turn. No clock, no I/O.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::schema::{Action, Step, StepOnError, Workflow};

use super::boundary::{ActionPartition, is_failure_value, partition};
use super::control_flow::{
    RetryCoordinator, build_approval_payload, disposition_for_kind, fail_run_value,
    is_approval_decision, leaf_preview_for_value, validate_graph_runtime,
};

use super::spawn::{SpawnSpec, unknown_profile_error};
use super::template::render_action_lenient;

/// Where one run is in the linear model.
// The phase carries the spawn spec by value: that record IS the persisted park,
// and boxing it would make the restart path indirect for no gain.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
pub enum RunPhase {
    /// Created, not started.
    Ready,
    /// The host may execute the current step.
    Running,
    /// Waiting until `until_ms` (delay/park).
    Parked {
        /// Absolute wake time.
        until_ms: u64,
        /// Why the run parked.
        reason: ParkReason,
    },
    /// Waiting for a spawned job to settle (bd-spawn-park-toron-vwu3).
    ///
    /// The spec is stored, not recomputed, so a restart republishes the same
    /// `spawn_key` and the consumer's claim makes a second start impossible.
    AwaitingSpawn {
        /// Step name.
        node: String,
        /// The self-contained spawn spec.
        spec: SpawnSpec,
        /// Absolute claim deadline, when the step set one.
        deadline_ms: Option<u64>,
    },
    /// Waiting for a human decision on `step`.
    AwaitingApproval {
        /// Step name.
        step: String,
        /// Prompt shown to the operator.
        message: String,
    },
    /// Terminal success.
    Succeeded,
    /// Terminal typed failure.
    Failed {
        /// Human-readable failure.
        error: String,
    },
    /// Terminal cancel.
    Cancelled,
}

/// Why a run parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParkReason {
    /// `delay` action.
    Delay,
    /// `park` action.
    Park,
    /// A spawn-await claim deadline (bd-spawn-park-toron-vwu3).
    Spawn,
}

/// The persisted linear run state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunModel {
    /// Run / journal / engine key.
    pub run_id: String,
    /// Workflow definition (stored so a restart has the same steps).
    pub workflow: Workflow,
    /// Trigger payload.
    pub trigger: serde_json::Value,
    /// Next step to execute.
    pub step_index: usize,
    /// Outputs by step name.
    pub outputs: BTreeMap<String, serde_json::Value>,
    /// Current phase.
    pub phase: RunPhase,
}

/// One input to the pure transition.
#[derive(Clone, Debug)]
pub enum RunMsg {
    /// Begin the run at this host time.
    Start {
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// The current step finished, whatever the value is.
    ///
    /// One carrier, never a classified pair: the fold applies
    /// `is_failure_value` + `on_error`, so a `continue` failure lands its FULL
    /// value (kind, disposition, leaf_preview intact) instead of collapsing to
    /// `{ok:false,error}` (Captain addendum, failure carrier).
    StepDone {
        /// The step's value, exactly as the executor returned it.
        value: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A parked delay/park woke.
    Woke {
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A spawned job settled.
    SpawnSettled {
        /// Step name.
        node: String,
        /// The child's terminal value.
        value: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A spawned job failed.
    SpawnFailed {
        /// Step name.
        node: String,
        /// Failure text.
        error: String,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// An approval decision arrived.
    Approval {
        /// Decision value.
        decision: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// Terminal cancel.
    Cancel,
}

/// One effect the host must run. Data only.
#[derive(Clone, Debug)]
pub enum RunEffect {
    /// Execute one step through the existing boundary.
    ExecuteStep {
        /// Step index.
        index: usize,
        /// The step.
        step: Box<Step>,
    },
    /// Park the run until this absolute time.
    ParkUntil {
        /// Absolute wake time.
        until_ms: u64,
        /// Why.
        reason: ParkReason,
    },
    /// Publish a spawn-await job; the run parks until it settles.
    StartSpawn {
        /// Step name.
        node: String,
        /// The self-contained spec the consumer reads.
        spec: SpawnSpec,
    },
    /// Publish a fire-and-forget spawn; the run does not park.
    SpawnDetached {
        /// Step name.
        node: String,
        /// The self-contained spec the consumer reads.
        spec: SpawnSpec,
    },
    /// Wait for an approval decision.
    AwaitApproval {
        /// Step name.
        step: String,
        /// Prompt.
        message: String,
    },
    /// Terminal success.
    Complete {
        /// Terminal output.
        output: serde_json::Value,
    },
    /// Terminal failure: the enriched `{status:failed,…}` bundle.
    Fail {
        /// Failed step name.
        step: String,
        /// The bundle the host settles the terminal from.
        output: serde_json::Value,
    },
    /// Terminal cancel.
    Cancelled,
    /// This workflow shape is not on the linear model yet.
    Unsupported {
        /// Why.
        reason: String,
    },
}

impl RunModel {
    /// New model at `Ready`.
    pub fn new(run_id: impl Into<String>, workflow: Workflow, trigger: serde_json::Value) -> Self {
        Self {
            run_id: run_id.into(),
            workflow,
            trigger,
            step_index: 0,
            outputs: BTreeMap::new(),
            phase: RunPhase::Ready,
        }
    }

    /// True for terminal phases.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            RunPhase::Succeeded | RunPhase::Failed { .. } | RunPhase::Cancelled
        )
    }
}

/// Pure transition: one model in, new model + effects out.
pub fn advance(model: &RunModel, msg: &RunMsg) -> (RunModel, Vec<RunEffect>) {
    advance_with(model, msg, &[])
}

/// Pure transition with the spawn specs the driver prepared for this
/// workflow's spawn steps, indexed by step.
///
/// Profile resolution is the one impure part of building a [`SpawnSpec`], so it
/// happens in the host ([`super::spawn::prepare_spawn`]) and arrives here as
/// data: the fold stays pure and total, and a spawn step whose spec is absent
/// lands the old unknown-profile value instead of parking.
pub fn advance_with(
    model: &RunModel,
    msg: &RunMsg,
    spawns: &[Option<SpawnSpec>],
) -> (RunModel, Vec<RunEffect>) {
    let mut next = model.clone();
    match msg {
        RunMsg::Start { now_ms } => {
            if next.phase != RunPhase::Ready {
                return (next, Vec::new());
            }
            if let Some(reason) = model_shape_refusal(&next.workflow) {
                return (next, vec![RunEffect::Unsupported { reason }]);
            }
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms, spawns);
            (next, effects)
        }
        RunMsg::StepDone { value, now_ms } => {
            if next.phase != RunPhase::Running {
                return (next, Vec::new());
            }
            let Some(step) = next.workflow.steps.get(next.step_index) else {
                return (next, Vec::new());
            };
            let name = step_name(step, next.step_index);
            if step.on_error == StepOnError::FailRun && is_failure_value(value) {
                // The executor's fail_run arm already returns the enriched
                // bundle; a guard/render refusal does not, so build it here with
                // the ONE builder.
                let output = if value.get("status").and_then(|v| v.as_str()) == Some("failed") {
                    value.clone()
                } else {
                    fail_run_value(&name, value, None)
                };
                next.phase = RunPhase::Failed {
                    error: output
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("step failed")
                        .to_string(),
                };
                return (next, vec![RunEffect::Fail { step: name, output }]);
            }
            next.outputs.insert(name, value.clone());
            next.step_index += 1;
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms, spawns);
            (next, effects)
        }
        RunMsg::Woke { now_ms } => {
            // A spawn-await deadline is the one park whose wake can FAIL the
            // node, so it is handled before the delay arm.
            if let RunPhase::AwaitingSpawn {
                node,
                spec,
                deadline_ms,
            } = next.phase.clone()
            {
                let Some(deadline) = deadline_ms else {
                    return (next, Vec::new());
                };
                if deadline > *now_ms {
                    return (
                        next,
                        vec![RunEffect::ParkUntil {
                            until_ms: deadline,
                            reason: ParkReason::Spawn,
                        }],
                    );
                }
                let step = next.workflow.steps.get(next.step_index);
                let error = format!("spawn-await {}: timed out after {}s", spec.spawn_key, {
                    step.map_or(0, |s| match &s.action {
                        Action::Spawn {
                            no_claim_timeout, ..
                        } => *no_claim_timeout,
                        _ => 0,
                    })
                });
                return settle_spawn(
                    &mut next,
                    &node,
                    serde_json::json!({"ok": false, "error": error}),
                    *now_ms,
                    spawns,
                );
            }
            if !matches!(next.phase, RunPhase::Parked { .. }) {
                return (next, Vec::new());
            }
            let Some(step) = next.workflow.steps.get(next.step_index) else {
                return (next, Vec::new());
            };
            let name = step_name(step, next.step_index);
            next.outputs
                .insert(name, serde_json::json!({"ok": true, "waited": true}));
            next.step_index += 1;
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms, spawns);
            (next, effects)
        }
        RunMsg::SpawnSettled {
            node,
            value,
            now_ms,
        } => {
            let RunPhase::AwaitingSpawn {
                node: awaiting,
                spec,
                ..
            } = next.phase.clone()
            else {
                return (next, Vec::new());
            };
            if &awaiting != node {
                // A settlement for another node/key is stale.
                return (next, Vec::new());
            }
            let landed = serde_json::json!({
                "ok": true,
                "spawn_key": spec.spawn_key,
                "result": value,
            });
            settle_spawn(&mut next, node, landed, *now_ms, spawns)
        }
        RunMsg::SpawnFailed {
            node,
            error,
            now_ms,
        } => {
            let RunPhase::AwaitingSpawn { node: awaiting, .. } = next.phase.clone() else {
                return (next, Vec::new());
            };
            if &awaiting != node {
                return (next, Vec::new());
            }
            let landed = serde_json::json!({ "ok": false, "error": error });
            settle_spawn(&mut next, node, landed, *now_ms, spawns)
        }
        RunMsg::Approval { decision, now_ms } => {
            if !matches!(next.phase, RunPhase::AwaitingApproval { .. }) {
                return (next, Vec::new());
            }
            let Some(step) = next.workflow.steps.get(next.step_index) else {
                return (next, Vec::new());
            };
            let name = step_name(step, next.step_index);
            next.outputs.insert(name, decision.clone());
            next.step_index += 1;
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms, spawns);
            (next, effects)
        }
        RunMsg::Cancel => {
            next.phase = RunPhase::Cancelled;
            (next, vec![RunEffect::Cancelled])
        }
    }
}

fn next_effects(model: &mut RunModel, now_ms: u64, spawns: &[Option<SpawnSpec>]) -> Vec<RunEffect> {
    if model.step_index >= model.workflow.steps.len() {
        model.phase = RunPhase::Succeeded;
        return vec![RunEffect::Complete {
            output: serde_json::json!({"status": "succeeded", "steps": model.outputs}),
        }];
    }
    let step = model.workflow.steps[model.step_index].clone();
    let name = step_name(&step, model.step_index);
    match &step.action {
        Action::Delay { secs } => {
            let until_ms = now_ms.saturating_add(secs.saturating_mul(1_000));
            model.phase = RunPhase::Parked {
                until_ms,
                reason: ParkReason::Delay,
            };
            vec![RunEffect::ParkUntil {
                until_ms,
                reason: ParkReason::Delay,
            }]
        }
        Action::RequestApproval { message } => {
            model.phase = RunPhase::AwaitingApproval {
                step: name.clone(),
                message: message.clone(),
            };
            vec![RunEffect::AwaitApproval {
                step: name,
                message: message.clone(),
            }]
        }
        Action::Spawn {
            profile,
            harness: _,
            workflow: _,
            await_,
            no_claim_timeout: _,
            context: _,
        } => {
            let name = step_name(&step, model.step_index);
            let Some(spec) = spawns.get(model.step_index).and_then(Option::as_ref) else {
                // Design (f): a profile that cannot be prepared is a step
                // VALUE, never a run failure.
                let error = unknown_profile_error(profile);
                model.outputs.insert(
                    name.clone(),
                    serde_json::json!({ "ok": false, "error": error }),
                );
                model.step_index += 1;
                model.phase = RunPhase::Running;
                return next_effects(model, now_ms, spawns);
            };
            if !*await_ {
                // Fire-and-forget: the job is published, the run does not park.
                model.outputs.insert(
                    name.clone(),
                    serde_json::json!({ "ok": true, "spawned": true }),
                );
                model.step_index += 1;
                model.phase = RunPhase::Running;
                let mut effects = vec![RunEffect::SpawnDetached {
                    node: name,
                    spec: spec.clone(),
                }];
                effects.extend(next_effects(model, now_ms, spawns));
                return effects;
            }
            let deadline_ms = spec.deadline_ms(now_ms);
            model.phase = RunPhase::AwaitingSpawn {
                node: name.clone(),
                spec: spec.clone(),
                deadline_ms,
            };
            let mut effects = vec![RunEffect::StartSpawn {
                node: name,
                spec: spec.clone(),
            }];
            if let Some(deadline_ms) = deadline_ms {
                effects.push(RunEffect::ParkUntil {
                    until_ms: deadline_ms,
                    reason: ParkReason::Spawn,
                });
            }
            effects
        }
        action => {
            if matches!(partition(action), ActionPartition::HostSink { .. }) {
                model.phase = RunPhase::Running;
                return vec![RunEffect::ExecuteStep {
                    index: model.step_index,
                    step: Box::new(step),
                }];
            }
            // Unreachable through `advance(RunMsg::Start)`, which refuses the
            // shape first. Kept so a direct caller cannot mistake a control
            // action for a host step.
            let error = format!(
                "step '{name}' uses '{}', which the linear model cannot sequence",
                action.type_name()
            );
            model.phase = RunPhase::Failed {
                error: error.clone(),
            };
            vec![RunEffect::Fail {
                step: name,
                output: serde_json::json!({ "status": "failed", "error": error }),
            }]
        }
    }
}

/// `None` when the durable models can sequence this workflow end to end.
///
/// The engine partitions every action. A `steps:` workflow is sequenced by the
/// linear model (host-sink actions plus `delay` and `request_approval`); a
/// `graph:` workflow by the graph model under [`graph_unsupported_reason`].
/// Everything else is a shape the host keeps on the old engine, so it is
/// refused before a run row or a state row exists.
pub fn unsupported_reason(workflow: &Workflow) -> Option<String> {
    if workflow.graph.is_some() {
        return graph_unsupported_reason(workflow);
    }
    for (index, step) in workflow.steps.iter().enumerate() {
        if !is_linear(&step.action) {
            return Some(format!(
                "step '{}' uses '{}', which the linear model cannot sequence yet",
                step_name(step, index),
                step.action.type_name()
            ));
        }
    }
    None
}

fn is_linear(action: &Action) -> bool {
    matches!(
        action,
        Action::Delay { .. } | Action::RequestApproval { .. }
    ) || matches!(partition(action), ActionPartition::HostSink { .. })
}

/// What the MODEL can sequence, which is a superset of what the router admits.
///
/// `spawn` has fold arms as of bd-spawn-park-toron-vwu3, but the router still
/// withholds it until the flywheel pane consumer reads the spec (separate flip
/// bead). `Start` therefore asks this, not [`unsupported_reason`]: a shape the
/// model can sequence must not answer `Unsupported` merely because the cutover
/// has not flipped.
fn model_shape_refusal(workflow: &Workflow) -> Option<String> {
    if workflow.graph.is_some() {
        return graph_unsupported_reason(workflow);
    }
    for (index, step) in workflow.steps.iter().enumerate() {
        if !is_model_linear(&step.action) {
            return Some(format!(
                "step '{}' uses '{}', which the linear model cannot sequence yet",
                step_name(step, index),
                step.action.type_name()
            ));
        }
    }
    None
}

fn is_model_linear(action: &Action) -> bool {
    matches!(action, Action::Spawn { .. }) || is_linear(action)
}

/// Land one spawn settlement and continue.
///
/// `on_error: fail_run` on a failed settlement ends the run with the same
/// enriched bundle the other steps produce; anything else lands the value and
/// advances.
fn settle_spawn(
    model: &mut RunModel,
    node: &str,
    value: serde_json::Value,
    now_ms: u64,
    spawns: &[Option<SpawnSpec>],
) -> (RunModel, Vec<RunEffect>) {
    let step = model.workflow.steps.get(model.step_index).cloned();
    let name = step
        .as_ref()
        .map_or_else(|| node.to_string(), |s| step_name(s, model.step_index));
    if step
        .as_ref()
        .is_some_and(|s| s.on_error == StepOnError::FailRun)
        && is_failure_value(&value)
    {
        let output = fail_run_value(&name, &value, None);
        model.phase = RunPhase::Failed {
            error: output
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("step failed")
                .to_string(),
        };
        return (model.clone(), vec![RunEffect::Fail { step: name, output }]);
    }
    model.outputs.insert(name, value);
    model.step_index += 1;
    model.phase = RunPhase::Running;
    let effects = next_effects(model, now_ms, spawns);
    (model.clone(), effects)
}

/// Effects a restored run must re-issue.
///
/// Terminal runs re-issue nothing: the model already holds the outcome, and a
/// terminal engine verb is not idempotent in the way a park is. A `Running`
/// model re-issues its current step, which is why the host's step executor must
/// stay idempotent per effect key.
pub fn resume_effects(model: &RunModel) -> Vec<RunEffect> {
    match &model.phase {
        RunPhase::Ready | RunPhase::Succeeded | RunPhase::Failed { .. } | RunPhase::Cancelled => {
            Vec::new()
        }
        RunPhase::Parked { until_ms, reason } => vec![RunEffect::ParkUntil {
            until_ms: *until_ms,
            reason: *reason,
        }],
        RunPhase::AwaitingSpawn { node, spec, .. } => vec![RunEffect::StartSpawn {
            node: node.clone(),
            spec: spec.clone(),
        }],
        RunPhase::AwaitingApproval { step, message } => vec![RunEffect::AwaitApproval {
            step: step.clone(),
            message: message.clone(),
        }],
        RunPhase::Running => match model.workflow.steps.get(model.step_index) {
            Some(step) => vec![RunEffect::ExecuteStep {
                index: model.step_index,
                step: Box::new(step.clone()),
            }],
            None => Vec::new(),
        },
    }
}

fn step_name(step: &Step, index: usize) -> String {
    step.name
        .clone()
        .unwrap_or_else(|| format!("step_{}", index + 1))
}

// ---------------------------------------------------------------------------
// Graph run model (bd-durable-graph-impl-r2x1)
// ---------------------------------------------------------------------------

/// Why a node parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeParkReason {
    /// A `delay` node is waiting out its own author deadline.
    Delay,
    /// A retryable failure is waiting out its backoff before a re-dispatch.
    Retry,
}

/// Where one node of a graph run is.
///
/// `Pending` and `Settled` are the design's two states; the middle variants
/// carry the per-node wait so a restart re-issues exactly the effect the node
/// is owed (the design put that detail on a single `phase`, which cannot
/// describe two same-wave approvals — see the implementations's report).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum NodeState {
    /// Not scheduled yet: some `needs` are unsettled.
    Pending,
    /// Dispatched (or owed a dispatch after a restart).
    Executing {
        /// Wave the node was scheduled in.
        wave: usize,
        /// Scheduler-level attempt (1-based).
        attempt: u32,
        /// The spawn-time outputs snapshot the node must render against.
        inputs: BTreeMap<String, serde_json::Value>,
    },
    /// Parked until `until_ms`; `reason` says what lands on wake.
    Parked {
        /// Wave the node was scheduled in.
        wave: usize,
        /// Scheduler-level attempt (1-based).
        attempt: u32,
        /// Absolute wake time.
        until_ms: u64,
        /// What the wake means.
        reason: NodeParkReason,
    },
    /// Waiting on an operator decision. The payload is stored, not recomputed,
    /// so a resume republishes byte-identical evidence.
    AwaitingApproval {
        /// Wave the node was scheduled in.
        wave: usize,
        /// Scheduler-level attempt (1-based).
        attempt: u32,
        /// The exact promise data the host published.
        payload: serde_json::Value,
    },
    /// Landed.
    Settled,
}

impl NodeState {
    /// True while the node owes the host or the engine something.
    pub const fn is_in_flight(&self) -> bool {
        matches!(
            self,
            Self::Executing { .. } | Self::Parked { .. } | Self::AwaitingApproval { .. }
        )
    }
}

/// Coarse lifecycle of one graph run. Per-node detail lives in
/// [`GraphModel::node_state`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "phase")]
pub enum GraphPhase {
    /// Created, not started.
    Ready,
    /// At least one node is in flight.
    Running,
    /// Every node settled.
    Succeeded,
    /// Terminal typed failure.
    Failed {
        /// Human-readable failure.
        error: String,
    },
    /// Terminal cancel.
    Cancelled,
}

/// The persisted graph run state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphModel {
    /// Run / journal / engine key.
    pub run_id: String,
    /// Workflow definition (stored so a restart sees the same DAG).
    pub workflow: Workflow,
    /// Trigger payload.
    pub trigger: serde_json::Value,
    /// Current wave number (1-based, incremented once per scheduling round).
    pub wave: usize,
    /// Per-node state.
    pub node_state: BTreeMap<String, NodeState>,
    /// The wave each node was scheduled in — the `(wave, name)` alias order.
    pub node_wave: BTreeMap<String, usize>,
    /// Landed outputs.
    pub outputs: BTreeMap<String, serde_json::Value>,
    /// Current phase.
    pub phase: GraphPhase,
}

/// One input to the pure graph transition.
#[derive(Clone, Debug)]
pub enum GraphMsg {
    /// Begin the run at this host time.
    Start {
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// One node finished, whatever the value is.
    ///
    /// One carrier, never a classified pair: the fold applies
    /// `is_failure_value` + `on_error`, so a `continue` failure lands its FULL
    /// value and a bare refusal is enriched on `fail_run` (Captain addendum,
    /// failure carrier).
    NodeDone {
        /// Node name.
        node: String,
        /// The node's value, exactly as the executor returned it.
        value: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A park (delay or retry backoff) is due.
    Woke {
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// An operator decision arrived for one node.
    Approval {
        /// Node name.
        node: String,
        /// Decision value.
        decision: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// Terminal cancel.
    Cancel,
}

/// The enriched fail_run bundle, in the exact field set the old scheduler
/// writes. Empty `kind`/`disposition` and a null `leaf_preview` mean "the old
/// path did not write that key for this failure".
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphFail {
    /// Failed node (`<graph>` for whole-graph failures).
    pub node: String,
    /// Failure text.
    pub error: String,
    /// Frozen failure kind, when the value carried one.
    pub kind: String,
    /// Graph-admission disposition, when one applies.
    pub disposition: String,
    /// Bounded leaf evidence (the old path always writes this for a node
    /// failure; null means the whole-graph failures that never did).
    pub leaf_preview: serde_json::Value,
    /// Scheduler attempts. `None` for the non-retryable path, which never wrote
    /// the key.
    pub attempts: Option<u32>,
}

impl GraphFail {
    /// The `{status:failed,…}` value the old scheduler returns.
    pub fn to_value(&self) -> serde_json::Value {
        let mut failed = serde_json::json!({
            "status": "failed",
            "error": self.error,
            "failed_step": self.node,
        });
        if !self.kind.is_empty() {
            failed["kind"] = serde_json::json!(self.kind);
        }
        if !self.disposition.is_empty() {
            failed["disposition"] = serde_json::json!(self.disposition);
        }
        if let Some(attempts) = self.attempts {
            failed["attempts"] = serde_json::json!(attempts);
        }
        if !self.leaf_preview.is_null() {
            failed["leaf_preview"] = self.leaf_preview.clone();
        }
        failed
    }
}

/// One effect the host must run for a graph. Data only, per node.
#[derive(Clone, Debug)]
pub enum GraphEffect {
    /// Execute one node through the leaf boundary.
    ExecuteNode {
        /// Node name.
        node: String,
        /// Wave the node belongs to.
        wave: usize,
        /// Scheduler-level attempt.
        attempt: u32,
        /// The spawn-time outputs snapshot the node renders against.
        inputs: BTreeMap<String, serde_json::Value>,
    },
    /// Park the run until the earliest node deadline.
    ParkUntil {
        /// Node name.
        node: String,
        /// Absolute wake time.
        until_ms: u64,
        /// Why.
        reason: NodeParkReason,
    },
    /// Wait for an operator decision on one node.
    AwaitApproval {
        /// Node name.
        node: String,
        /// The exact promise data the host publishes.
        payload: serde_json::Value,
    },
    /// Terminal success: the outputs map (the `run_graph` shape).
    Complete {
        /// Outputs by node name.
        output: serde_json::Value,
    },
    /// Terminal failure.
    Fail {
        /// The enriched bundle.
        fail: GraphFail,
    },
    /// Terminal cancel.
    Cancelled,
    /// This graph is not on the durable model.
    Unsupported {
        /// Why.
        reason: String,
    },
}

impl GraphModel {
    /// New model at `Ready`.
    pub fn new(run_id: impl Into<String>, workflow: Workflow, trigger: serde_json::Value) -> Self {
        let mut node_state = BTreeMap::new();
        if let Some(graph) = &workflow.graph {
            for name in graph.nodes.keys() {
                node_state.insert(name.clone(), NodeState::Pending);
            }
        }
        Self {
            run_id: run_id.into(),
            workflow,
            trigger,
            wave: 0,
            node_state,
            node_wave: BTreeMap::new(),
            outputs: BTreeMap::new(),
            phase: GraphPhase::Ready,
        }
    }

    /// True for terminal phases.
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self.phase,
            GraphPhase::Succeeded | GraphPhase::Failed { .. } | GraphPhase::Cancelled
        )
    }

    /// True while any node owes the host or the engine something.
    ///
    /// The scheduler only starts the next wave when nothing is in flight, which
    /// is what makes a wave's spawn-time snapshot meaningful.
    pub fn is_in_flight(&self) -> bool {
        self.node_state.values().any(NodeState::is_in_flight)
    }

    fn is_settled(&self, name: &str) -> bool {
        matches!(self.node_state.get(name), Some(NodeState::Settled))
    }

    fn has_pending(&self) -> bool {
        self.node_state
            .values()
            .any(|s| matches!(s, NodeState::Pending))
    }

    /// Every node settled.
    pub fn all_settled(&self) -> bool {
        !self.node_state.is_empty()
            && self
                .node_state
                .values()
                .all(|s| matches!(s, NodeState::Settled))
    }
}

/// Pure transition for the graph model.
pub fn advance_graph(model: &GraphModel, msg: &GraphMsg) -> (GraphModel, Vec<GraphEffect>) {
    let mut next = model.clone();
    match msg {
        GraphMsg::Start { now_ms } => {
            if next.phase != GraphPhase::Ready {
                return (next, Vec::new());
            }
            if let Some(reason) = graph_unsupported_reason(&next.workflow) {
                return (next, vec![GraphEffect::Unsupported { reason }]);
            }
            next.phase = GraphPhase::Running;
            let effects = schedule(&mut next, *now_ms);
            (next, effects)
        }
        GraphMsg::NodeDone {
            node,
            value,
            now_ms,
        } => {
            // A wave can already be terminal when this arrives: a `fail_run`
            // render refusal returns its `Fail` while earlier nodes of the same
            // wave are already dispatched. A late result must never land an
            // output on a failed run or schedule a wave over it.
            if next.is_terminal() {
                return (next, Vec::new());
            }
            let Some(NodeState::Executing { wave, attempt, .. }) =
                next.node_state.get(node).cloned()
            else {
                return (next, Vec::new());
            };
            let on_error = next
                .workflow
                .graph
                .as_ref()
                .and_then(|g| g.nodes.get(node))
                .map(|n| n.on_error);
            if on_error != Some(StepOnError::FailRun) || !is_failure_value(value) {
                next.outputs.insert(node.clone(), value.clone());
                next.node_state.insert(node.clone(), NodeState::Settled);
                let effects = schedule(&mut next, *now_ms);
                return (next, effects);
            }
            let kind = value
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // Graphs admit no `retry:` node action, so the scheduler policy is
            // always the default coordinator (3 attempts, exponential backoff).
            let coordinator = RetryCoordinator::from_enclosing(None);
            if RetryCoordinator::is_retryable_value(value) {
                if coordinator.should_retry(attempt) {
                    let retry_after_ms = value
                        .get("retry_after_ms")
                        .and_then(serde_json::Value::as_u64);
                    let backoff = coordinator.backoff(attempt, retry_after_ms);
                    let until_ms = now_ms
                        .saturating_add(u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX));
                    next.node_state.insert(
                        node.clone(),
                        NodeState::Parked {
                            wave,
                            attempt: attempt + 1,
                            until_ms,
                            reason: NodeParkReason::Retry,
                        },
                    );
                    return (
                        next,
                        vec![GraphEffect::ParkUntil {
                            node: node.clone(),
                            until_ms,
                            reason: NodeParkReason::Retry,
                        }],
                    );
                }
                let disposition =
                    disposition_for_kind(&kind, value.get("disposition").and_then(|v| v.as_str()));
                let fail = GraphFail {
                    node: node.clone(),
                    error: value
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("step failed")
                        .to_string(),
                    kind,
                    disposition: disposition.as_str().to_string(),
                    leaf_preview: serde_json::json!(leaf_preview_for_value(value)),
                    attempts: Some(attempt),
                };
                next.phase = GraphPhase::Failed {
                    error: fail.error.clone(),
                };
                return (next, vec![GraphEffect::Fail { fail }]);
            }
            // A bare refusal (guard/render) is enriched here, exactly as the
            // linear fold does; a value that already carries the terminal
            // bundle passes through unchanged.
            let output = if value.get("status").and_then(|v| v.as_str()) == Some("failed") {
                value.clone()
            } else {
                fail_run_value(node, value, None)
            };
            let disposition = if kind.is_empty() {
                String::new()
            } else {
                disposition_for_kind(&kind, None).as_str().to_string()
            };
            let fail = GraphFail {
                node: node.clone(),
                error: output
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("step failed")
                    .to_string(),
                kind,
                disposition,
                leaf_preview: output
                    .get("leaf_preview")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                attempts: None,
            };
            next.phase = GraphPhase::Failed {
                error: fail.error.clone(),
            };
            (next, vec![GraphEffect::Fail { fail }])
        }
        GraphMsg::Woke { now_ms } => {
            if next.phase != GraphPhase::Running || next.is_terminal() {
                return (next, Vec::new());
            }
            let mut effects = Vec::new();
            let parked: Vec<(String, NodeState)> = next
                .node_state
                .iter()
                .filter(|(_, s)| matches!(s, NodeState::Parked { .. }))
                .map(|(n, s)| (n.clone(), s.clone()))
                .collect();
            for (node, state) in parked {
                let NodeState::Parked {
                    wave,
                    attempt,
                    until_ms,
                    reason,
                } = state
                else {
                    continue;
                };
                if until_ms > *now_ms {
                    effects.push(GraphEffect::ParkUntil {
                        node,
                        until_ms,
                        reason,
                    });
                    continue;
                }
                match reason {
                    NodeParkReason::Delay => {
                        next.outputs.insert(
                            node.clone(),
                            serde_json::json!({ "ok": true, "waited": true }),
                        );
                        next.node_state.insert(node, NodeState::Settled);
                    }
                    NodeParkReason::Retry => {
                        // The retry re-snapshots at re-dispatch, exactly as the
                        // old scheduler cloned `outputs` again on the re-spawn.
                        let inputs = next.outputs.clone();
                        next.node_state.insert(
                            node.clone(),
                            NodeState::Executing {
                                wave,
                                attempt,
                                inputs: inputs.clone(),
                            },
                        );
                        effects.push(GraphEffect::ExecuteNode {
                            node,
                            wave,
                            attempt,
                            inputs,
                        });
                    }
                }
            }
            if next.is_in_flight() {
                return (next, effects);
            }
            effects.extend(schedule(&mut next, *now_ms));
            (next, effects)
        }
        GraphMsg::Approval {
            node,
            decision,
            now_ms,
        } => {
            if next.is_terminal() {
                return (next, Vec::new());
            }
            if !matches!(
                next.node_state.get(node),
                Some(NodeState::AwaitingApproval { .. })
            ) {
                return (next, Vec::new());
            }
            next.outputs.insert(node.clone(), decision.clone());
            next.node_state.insert(node.clone(), NodeState::Settled);
            recompute_alias(&mut next);
            if next.is_in_flight() {
                return (next, Vec::new());
            }
            let effects = schedule(&mut next, *now_ms);
            (next, effects)
        }
        GraphMsg::Cancel => {
            next.phase = GraphPhase::Cancelled;
            (next, vec![GraphEffect::Cancelled])
        }
    }
}

/// Effects a restored graph run must re-issue, one per in-flight node.
pub fn resume_graph_effects(model: &GraphModel) -> Vec<GraphEffect> {
    let mut effects = Vec::new();
    for (node, state) in &model.node_state {
        match state {
            NodeState::Pending | NodeState::Settled => {}
            NodeState::Executing {
                wave,
                attempt,
                inputs,
            } => effects.push(GraphEffect::ExecuteNode {
                node: node.clone(),
                wave: *wave,
                attempt: *attempt,
                inputs: inputs.clone(),
            }),
            NodeState::Parked {
                until_ms, reason, ..
            } => effects.push(GraphEffect::ParkUntil {
                node: node.clone(),
                until_ms: *until_ms,
                reason: *reason,
            }),
            NodeState::AwaitingApproval { payload, .. } => {
                effects.push(GraphEffect::AwaitApproval {
                    node: node.clone(),
                    payload: payload.clone(),
                })
            }
        }
    }
    effects
}

/// Schedule the next wave, or settle the run.
///
/// Runs only when nothing is in flight: wave scheduling is what makes a wave's
/// spawn-time snapshot meaningful, and it is the old scheduler's own rule.
fn schedule(model: &mut GraphModel, now_ms: u64) -> Vec<GraphEffect> {
    if model.is_terminal() || model.is_in_flight() {
        return Vec::new();
    }
    let Some(graph) = model.workflow.graph.clone() else {
        return Vec::new();
    };
    loop {
        if model.all_settled() {
            model.phase = GraphPhase::Succeeded;
            return vec![GraphEffect::Complete {
                output: serde_json::Value::Object(outputs_map(&model.outputs)),
            }];
        }
        let ready: Vec<String> = graph
            .nodes
            .keys()
            .filter(|name| matches!(model.node_state.get(*name), Some(NodeState::Pending)))
            .filter(|name| {
                graph.nodes[*name]
                    .needs
                    .iter()
                    .all(|need| model.is_settled(need))
            })
            .cloned()
            .collect();
        if ready.is_empty() {
            if !model.has_pending() {
                // Every remaining node is settled; the terminal check above
                // covers the all-settled case, so this is a defensive stall.
                model.phase = GraphPhase::Succeeded;
                return vec![GraphEffect::Complete {
                    output: serde_json::Value::Object(outputs_map(&model.outputs)),
                }];
            }
            let mut remaining: Vec<&String> = model
                .node_state
                .iter()
                .filter(|(_, s)| !matches!(s, NodeState::Settled))
                .map(|(n, _)| n)
                .collect();
            remaining.sort();
            let error = format!("graph stalled with no ready nodes; remaining: {remaining:?}");
            model.phase = GraphPhase::Failed {
                error: error.clone(),
            };
            return vec![GraphEffect::Fail {
                fail: GraphFail {
                    node: "<graph>".to_string(),
                    error,
                    kind: String::new(),
                    disposition: String::new(),
                    leaf_preview: serde_json::Value::Null,
                    attempts: None,
                },
            }];
        }

        model.wave += 1;
        let wave = model.wave;
        // The spawn-time snapshot: a node never sees a same-wave sibling that
        // finished first, exactly like the old scheduler's `outputs.clone()`.
        let snapshot = model.outputs.clone();
        let snapshot_map = outputs_map(&snapshot);
        let mut effects = Vec::new();
        let mut dispatched = 0usize;

        for name in &ready {
            let node = &graph.nodes[name];
            // Required change 1 (Captain review): the planning render runs HERE,
            // when the wave is scheduled, so its failure keeps the old shape and
            // the rest of the wave is never spawned.
            if let Err(e) = render_action_lenient(&node.action, &model.trigger, &snapshot_map) {
                let error = format!("render: {e}");
                if node.on_error == StepOnError::FailRun {
                    model.phase = GraphPhase::Failed {
                        error: error.clone(),
                    };
                    effects.push(GraphEffect::Fail {
                        fail: GraphFail {
                            node: name.clone(),
                            error,
                            kind: String::new(),
                            disposition: String::new(),
                            leaf_preview: serde_json::Value::Null,
                            attempts: None,
                        },
                    });
                    return effects;
                }
                model.outputs.insert(
                    name.clone(),
                    serde_json::json!({ "ok": false, "error": error }),
                );
                model.node_wave.insert(name.clone(), wave);
                model.node_state.insert(name.clone(), NodeState::Settled);
                continue;
            }

            model.node_wave.insert(name.clone(), wave);
            match &node.action {
                Action::Delay { secs } => {
                    let until_ms = now_ms.saturating_add(secs.saturating_mul(1_000));
                    model.node_state.insert(
                        name.clone(),
                        NodeState::Parked {
                            wave,
                            attempt: 1,
                            until_ms,
                            reason: NodeParkReason::Delay,
                        },
                    );
                    effects.push(GraphEffect::ParkUntil {
                        node: name.clone(),
                        until_ms,
                        reason: NodeParkReason::Delay,
                    });
                    dispatched += 1;
                }
                Action::RequestApproval { message } => {
                    // Required change 2 (Captain review): the fold builds the
                    // same evidence payload the old graph approval publishes.
                    let payload = build_approval_payload(
                        &model.run_id,
                        &model.workflow.name,
                        name,
                        wave,
                        message,
                        &snapshot_map,
                        &graph,
                    );
                    model.node_state.insert(
                        name.clone(),
                        NodeState::AwaitingApproval {
                            wave,
                            attempt: 1,
                            payload: payload.clone(),
                        },
                    );
                    effects.push(GraphEffect::AwaitApproval {
                        node: name.clone(),
                        payload,
                    });
                    dispatched += 1;
                }
                action if matches!(partition(action), ActionPartition::HostSink { .. }) => {
                    model.node_state.insert(
                        name.clone(),
                        NodeState::Executing {
                            wave,
                            attempt: 1,
                            inputs: snapshot.clone(),
                        },
                    );
                    effects.push(GraphEffect::ExecuteNode {
                        node: name.clone(),
                        wave,
                        attempt: 1,
                        inputs: snapshot.clone(),
                    });
                    dispatched += 1;
                }
                action => {
                    // Unreachable through `open`, which refuses the shape first.
                    // Kept fail-closed so a direct caller cannot mistake an
                    // unsequencable node for a settled one.
                    let error = format!(
                        "node '{name}' uses '{}', which the graph model cannot sequence",
                        action.type_name()
                    );
                    model.phase = GraphPhase::Failed {
                        error: error.clone(),
                    };
                    effects.push(GraphEffect::Fail {
                        fail: GraphFail {
                            node: name.clone(),
                            error,
                            kind: String::new(),
                            disposition: String::new(),
                            leaf_preview: serde_json::Value::Null,
                            attempts: None,
                        },
                    });
                    return effects;
                }
            }
        }

        if dispatched == 0 {
            // Every ready node settled inline (planning-render failures);
            // schedule the next wave in the same transition.
            continue;
        }
        model.phase = GraphPhase::Running;
        return effects;
    }
}

/// Recompute the `decision` alias as the approval of the highest
/// `(wave, node-name)`.
///
/// The old scheduler inserted the alias as each approval landed (last in
/// await order wins), which for a wave-ordered await equals this max. Deriving
/// it from the model instead makes the alias independent of host completion
/// order, so a concurrent fan-out cannot make it restart-dependent.
fn recompute_alias(model: &mut GraphModel) {
    let Some(graph) = model.workflow.graph.clone() else {
        return;
    };
    let mut winner: Option<(usize, String)> = None;
    let mut value = None;
    for (name, state) in &model.node_state {
        if !matches!(state, NodeState::Settled) {
            continue;
        }
        let Some(node) = graph.nodes.get(name) else {
            continue;
        };
        let Some(output) = model.outputs.get(name) else {
            continue;
        };
        if !is_approval_decision(&node.action, output) {
            continue;
        }
        let wave = model.node_wave.get(name).copied().unwrap_or(0);
        if winner
            .as_ref()
            .is_none_or(|(w, n)| (wave, name.clone()) > (*w, n.clone()))
        {
            winner = Some((wave, name.clone()));
            value = Some(output.clone());
        }
    }
    if let Some(value) = value {
        model.outputs.insert("decision".into(), value);
    }
}

fn outputs_map(
    outputs: &BTreeMap<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    outputs
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// `None` when the graph model can sequence this graph end to end.
///
/// Whole-graph refusal, never partial: a graph is admitted only when every node
/// is a HostSink leaf, `delay`, or `request_approval`. An empty, cyclic, or
/// unknown-need graph is refused too — the old engine fails those identically,
/// and a durable row recording a doomed run is worse than a clean route.
pub fn graph_unsupported_reason(workflow: &Workflow) -> Option<String> {
    let Some(graph) = &workflow.graph else {
        return Some("not a graph workflow".into());
    };
    if let Err(error) = validate_graph_runtime(graph) {
        return Some(error);
    }
    for (name, node) in &graph.nodes {
        if !is_graph_sequencable(&node.action) {
            return Some(format!(
                "graph node '{name}' uses '{}', which the graph model cannot sequence",
                node.action.type_name()
            ));
        }
    }
    None
}

fn is_graph_sequencable(action: &Action) -> bool {
    matches!(
        action,
        Action::Delay { .. } | Action::RequestApproval { .. }
    ) || matches!(partition(action), ActionPartition::HostSink { .. })
}
