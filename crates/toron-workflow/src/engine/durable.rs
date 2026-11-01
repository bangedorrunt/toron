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

use super::boundary::{ActionPartition, partition};

/// Where one run is in the linear model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The current step succeeded.
    StepSucceeded {
        /// Step output.
        output: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// The current step failed.
    StepFailed {
        /// Failure text.
        error: String,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A parked delay/park woke.
    Woke {
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
    /// Terminal failure.
    Fail {
        /// Failed step name.
        step: String,
        /// Failure text.
        error: String,
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
    let mut next = model.clone();
    match msg {
        RunMsg::Start { now_ms } => {
            if next.phase != RunPhase::Ready {
                return (next, Vec::new());
            }
            if let Some(reason) = unsupported_reason(&next.workflow) {
                return (next, vec![RunEffect::Unsupported { reason }]);
            }
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms);
            (next, effects)
        }
        RunMsg::StepSucceeded { output, now_ms } => {
            if next.phase != RunPhase::Running {
                return (next, Vec::new());
            }
            let Some(step) = next.workflow.steps.get(next.step_index) else {
                return (next, Vec::new());
            };
            next.outputs
                .insert(step_name(step, next.step_index), output.clone());
            next.step_index += 1;
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms);
            (next, effects)
        }
        RunMsg::StepFailed { error, now_ms } => {
            if next.phase != RunPhase::Running {
                return (next, Vec::new());
            }
            let Some(step) = next.workflow.steps.get(next.step_index) else {
                return (next, Vec::new());
            };
            let name = step_name(step, next.step_index);
            if step.on_error == StepOnError::FailRun {
                next.phase = RunPhase::Failed {
                    error: error.clone(),
                };
                return (
                    next,
                    vec![RunEffect::Fail {
                        step: name,
                        error: error.clone(),
                    }],
                );
            }
            next.outputs
                .insert(name, serde_json::json!({"ok": false, "error": error}));
            next.step_index += 1;
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms);
            (next, effects)
        }
        RunMsg::Woke { now_ms } => {
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
            let effects = next_effects(&mut next, *now_ms);
            (next, effects)
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
            let effects = next_effects(&mut next, *now_ms);
            (next, effects)
        }
        RunMsg::Cancel => {
            next.phase = RunPhase::Cancelled;
            (next, vec![RunEffect::Cancelled])
        }
    }
}

fn next_effects(model: &mut RunModel, now_ms: u64) -> Vec<RunEffect> {
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
            vec![RunEffect::Fail { step: name, error }]
        }
    }
}

/// `None` when the linear model can sequence this workflow end to end.
///
/// The engine partitions every action; the linear model sequences exactly the
/// host-sink actions plus `delay` and `request_approval`. Everything else is a
/// shape the host keeps on the old engine, so it is refused before a run row
/// or a state row exists.
pub fn unsupported_reason(workflow: &Workflow) -> Option<String> {
    if workflow.graph.is_some() {
        return Some("graph workflows stay on the old engine until the graph model lands".into());
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
