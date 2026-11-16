// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Pure linear run model for the durable workflow cutover.
//!
//! The old vendor function cannot be replaced by a compatible async
//! `Context`: our engine schedules runs, it does not replay a function body.
//! This module is the explicit model the ADR calls for:
//!
//! ```text
//! (RunModel, RunMsg) -> (RunModel, Vec<RunEffect>)
//! ```
//!
//! `Delay` parks, `RequestApproval` awaits a decision, `Branch` chooses an arm
//! at its marker from the pure guard, every other action is an `ExecuteStep`
//! the host runs through the existing boundary. Graph workflows return
//! [`RunEffect::Unsupported`] so the host keeps them on the old engine until
//! their turn. No clock, no I/O.

use std::collections::BTreeMap;
use std::sync::Arc;

use nostr::prelude::EventId;
use serde::{Deserialize, Serialize};
use toron_core::bus::{Bus, Filter, SubscriptionEvent};
use toron_core::nostr::kinds::{JOB_ACCEPTED, JOB_ERROR, JOB_RESULT, toron_kind};

use crate::jobs::{JobError, JobResult, job_parent};

use crate::schema::{Action, DispatchAwait, Graph, GraphNode, Step, StepOnError, Workflow};

use super::boundary::{ActionPartition, is_failure_value, partition};
use super::branch::{PureGuard, check_pure_guard, evaluate_pure_guard};
use super::control_flow::{
    MAX_LOOP_ITERS, MAX_PARALLEL_FANOUT, RetryCoordinator, build_approval_payload,
    disposition_for_kind, durable_child_trigger, fail_run_value, is_approval_decision,
    parallel_fanout_count, resolve_template_ref, validate_graph_runtime,
};

use super::spawn::{SpawnSpec, unknown_profile_error};
use super::template::render_action_lenient;

/// Where one run is in the linear model.
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
    /// Waiting for a dispatched job's 43004/43006 (bd-ge1zo, ADR-0021 D4.1).
    ///
    /// The 43001 publication happens through the normal ExecuteStep path,
    /// so a restart does NOT republish — resume re-issues the watch and the
    /// park. A pre-publication crash therefore waits out the claim deadline
    /// and fails loud, never hangs.
    AwaitingJob {
        /// Step name.
        node: String,
        /// Published 43001 id (hex); empty until the StepDone receipt lands.
        job: String,
        /// Absolute claim deadline (None = explicit 0, waits unboundedly).
        claim_deadline_ms: Option<u64>,
        /// Absolute result deadline once claimed (None = waits unboundedly;
        /// the lint gate forbids omitting `result_timeout`).
        result_deadline_ms: Option<u64>,
        /// Whether a 43002 claim arrived (switches the active deadline).
        claimed: bool,
        /// Claim bound in seconds, for the timeout error text.
        no_claim_timeout: u64,
        /// Result bound in seconds, for the timeout error text.
        result_timeout: Option<u64>,
    },
    /// Waiting for a human decision on `step`.
    AwaitingApproval {
        /// Step name.
        step: String,
        /// Prompt shown to the operator.
        message: String,
        /// Absolute deadline (ms). `None` = unbounded (no `timeout_secs`).
        #[serde(default)]
        deadline_ms: Option<u64>,
        /// The authored bound, for the timeout error text.
        #[serde(default)]
        timeout_secs: Option<u64>,
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
    /// A dispatch-await claim/result deadline (bd-ge1zo, ADR-0021 D4.1).
    Job,
}

fn default_loop_min() -> u32 {
    1
}

/// One bounded loop's progress (bd-loop-rewrites-fap8).
///
/// Persisted with the model, so a restart resumes at the same iteration
/// instead of replaying or resetting the loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoopProgress {
    /// The iteration about to run / just ran (1-based).
    pub iteration: u32,
    /// The resolved bound (1..=MAX_LOOP_ITERS).
    pub max: u32,
    /// Minimum passes before `until` may stop the loop (1 = legacy behavior).
    #[serde(default = "default_loop_min")]
    pub min: u32,
}

/// Which arm a [`PlanEntry::ArmEnd`] closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchSide {
    /// The `then` arm.
    Then,
    /// The `else` arm.
    Else,
}

impl BranchSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Then => "then",
            Self::Else => "else",
        }
    }
}

/// One entry in the compiled linear plan (bd-durable-branch-hhgc).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PlanEntry {
    /// An ordinary linear step. `authored` is its position in the authored
    /// step list (top level, or its branch arm), so default step names and
    /// spawn-spec positions keep their authored identity.
    Step {
        /// Authored position.
        authored: usize,
        /// The authored step (boxed: `Step` is far larger than a marker).
        step: Box<Step>,
    },
    /// A branch marker: `then_len` entries follow, then the then arm's
    /// [`PlanEntry::ArmEnd`], then `else_len` entries and the else arm's
    /// `ArmEnd`.
    Branch {
        /// Authored position of the branch step.
        authored: usize,
        /// The normalized pure guard.
        guard: String,
        /// Entries in the `then` arm.
        then_len: usize,
        /// Entries in the `else` arm.
        else_len: usize,
    },
    /// A bounded loop marker (bd-loop-rewrites-fap8): `body_len` entries
    /// follow, then the [`PlanEntry::LoopEnd`].
    Loop {
        /// Authored position of the loop step.
        authored: usize,
        /// Entries in one body pass.
        body_len: usize,
        /// Entry after the whole loop (used when the bound cannot resolve).
        after: usize,
    },
    /// The end of a loop body: bump the counter, evaluate `until` over the
    /// outputs, and jump back or exit.
    LoopEnd {
        /// Authored position of the loop step.
        authored: usize,
        /// First body entry.
        body_start: usize,
        /// Entry after the whole loop.
        after: usize,
    },
    /// The end of a chosen arm: land the branch step's value and jump past
    /// the whole branch. Static, so a restart re-derives the same jump.
    ArmEnd {
        /// The branch step's output name.
        branch: String,
        /// Which arm ended.
        side: BranchSide,
        /// Entry position after the whole branch.
        after: usize,
    },
}

/// The compiled linear plan: authored steps expanded into a flat entry list
/// with branch markers and static arm ends.
///
/// `serde(default)` keeps a model written before the branch slice readable:
/// [`RunPlan::compile`] fills it deterministically at open.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunPlan {
    /// Entries in execution order.
    pub entries: Vec<PlanEntry>,
}

impl RunPlan {
    /// Compile the authored steps into entries. Deterministic: no clock, no
    /// environment, no run state.
    pub fn compile(workflow: &Workflow) -> Self {
        let mut entries: Vec<PlanEntry> = Vec::new();
        for (index, step) in workflow.steps.iter().enumerate() {
            match &step.action {
                Action::Loop { body, .. } => {
                    let marker = entries.len();
                    entries.push(PlanEntry::Loop {
                        authored: index,
                        body_len: body.len(),
                        after: 0,
                    });
                    for (arm_index, arm_step) in body.iter().enumerate() {
                        entries.push(PlanEntry::Step {
                            authored: arm_index,
                            step: Box::new(arm_step.clone()),
                        });
                    }
                    let after = entries.len() + 1;
                    entries.push(PlanEntry::LoopEnd {
                        authored: index,
                        body_start: marker + 1,
                        after,
                    });
                    if let PlanEntry::Loop { after: slot, .. } = &mut entries[marker] {
                        *slot = after;
                    }
                }
                Action::Branch {
                    if_guard,
                    then,
                    else_steps,
                } => {
                    let branch = step_name(step, index);
                    entries.push(PlanEntry::Branch {
                        authored: index,
                        guard: if_guard.clone(),
                        then_len: then.len(),
                        else_len: else_steps.len(),
                    });
                    let mut ends: Vec<usize> = Vec::new();
                    for (arm_index, arm_step) in then.iter().enumerate() {
                        entries.push(PlanEntry::Step {
                            authored: arm_index,
                            step: Box::new(arm_step.clone()),
                        });
                    }
                    ends.push(entries.len());
                    entries.push(PlanEntry::ArmEnd {
                        branch: branch.clone(),
                        side: BranchSide::Then,
                        after: 0,
                    });
                    for (arm_index, arm_step) in else_steps.iter().enumerate() {
                        entries.push(PlanEntry::Step {
                            authored: arm_index,
                            step: Box::new(arm_step.clone()),
                        });
                    }
                    ends.push(entries.len());
                    entries.push(PlanEntry::ArmEnd {
                        branch: branch.clone(),
                        side: BranchSide::Else,
                        after: 0,
                    });
                    let after = entries.len();
                    for end in ends {
                        if let PlanEntry::ArmEnd { after: slot, .. } = &mut entries[end] {
                            *slot = after;
                        }
                    }
                }
                _ => entries.push(PlanEntry::Step {
                    authored: index,
                    step: Box::new(step.clone()),
                }),
            }
        }
        Self { entries }
    }

    /// The compiled current step, when the cursor is on one.
    pub fn step_at(&self, index: usize) -> Option<(usize, &Step)> {
        match self.entries.get(index) {
            Some(PlanEntry::Step { authored, step }) => Some((*authored, step.as_ref())),
            _ => None,
        }
    }
}

/// The persisted linear run state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunModel {
    /// Run / journal / engine key.
    pub run_id: String,
    /// Workflow definition (stored so a restart has the same steps).
    pub workflow: Workflow,
    /// The compiled linear plan (bd-durable-branch-hhgc).
    #[serde(default)]
    pub plan: RunPlan,
    /// Trigger payload.
    pub trigger: serde_json::Value,
    /// Next plan entry to execute.
    pub step_index: usize,
    /// Outputs by step name.
    pub outputs: BTreeMap<String, serde_json::Value>,
    /// Bounded-loop progress by authored loop name (bd-loop-rewrites-fap8).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub loops: BTreeMap<String, LoopProgress>,
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
    /// A dispatched job was claimed (43002).
    JobClaimed {
        /// Step name.
        node: String,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A dispatched job settled (43004 with the step output value).
    JobSettled {
        /// Step name.
        node: String,
        /// The step output value (`{ok:true, job, witness, body}`).
        value: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A dispatched job errored (43006).
    JobFailed {
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

/// What the host watches for one dispatched job (bd-ge1zo, ADR-0021 D4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobWatchSpec {
    /// Published 43001 job id (hex).
    pub job_id: String,
    /// Absolute deadline for the current wait (claim or result), if any.
    pub deadline_ms: Option<u64>,
    /// True once the fold recorded the 43002: the re-watch skips the
    /// already-seen claim and waits for the 43004/43006.
    pub claimed: bool,
}

/// One watch wait's outcome, bounded by the model's deadline.
#[derive(Debug)]
pub enum JobWatchOutcome {
    /// A 43002 claim arrived.
    Claimed,
    /// A 43004 arrived; carries the step output value.
    Settled(serde_json::Value),
    /// A 43006 arrived; carries the job's message.
    Failed(String),
    /// The deadline passed with no terminal mail.
    TimedOut,
}

/// Watch one job's 43002/43004/43006 with the injected watcher, bounded by
/// the model's deadline. Mirrors the `await_spawn_launch` contract: the
/// watcher resolves the mail wait, the timeout stays a pure model deadline.
/// A missing (`None`) deadline waits unboundedly — the lint gate, not the
/// fold, forbids unbounded waits.
pub async fn await_job_watch(
    watcher: &JobWatcher,
    spec: JobWatchSpec,
    deadline_ms: Option<u64>,
    now_ms: u64,
) -> JobWatchOutcome {
    let Some(deadline) = deadline_ms else {
        return watcher(spec).await;
    };
    if deadline <= now_ms {
        return JobWatchOutcome::TimedOut;
    }
    match tokio::time::timeout(
        tokio::time::Duration::from_millis(deadline - now_ms),
        watcher(spec),
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(_) => JobWatchOutcome::TimedOut,
    }
}

/// Watch one published 43001 until its 43002/43004/43006 arrives (bd-ge1zo).
/// Returns the first terminal mail for this job; ignores other jobs'
/// traffic and EOSE markers. A closed stream ends loudly (typed failure),
/// never hangs: without a bus there is nothing left to wait for.
/// No deadline here — the caller bounds the wait through [`await_job_watch`].
pub(crate) async fn watch_job_once(bus: &Arc<dyn Bus>, spec: &JobWatchSpec) -> JobWatchOutcome {
    let job_id = spec.job_id.as_str();
    let Ok(want) = EventId::from_hex(job_id) else {
        return JobWatchOutcome::Failed(format!("dispatch await: unparseable job id {job_id:?}"));
    };
    let mut sub = match bus
        .subscribe(
            Filter::new()
                .kinds([
                    toron_kind(JOB_ACCEPTED),
                    toron_kind(JOB_RESULT),
                    toron_kind(JOB_ERROR),
                ])
                .event(want),
        )
        .await
    {
        Ok(sub) => sub,
        Err(e) => {
            return JobWatchOutcome::Failed(format!(
                "dispatch await: job watch subscribe failed: {e:#}"
            ));
        }
    };
    // The bus replays stored history before live mail, so the replay can
    // hold the 43002 AND the terminal. Drain to EOSE and keep the most
    // terminal mail seen (result/error beat claim); a re-watch after the
    // fold recorded the claim skips the already-seen claim and waits live.
    let mut replay_best: Option<JobWatchOutcome> = None;
    let mut live = false;
    loop {
        match sub.next().await {
            Some(SubscriptionEvent::Event(ev)) => {
                if job_parent(&ev).as_ref() != Some(&want) {
                    continue;
                }
                let outcome = match ev.kind.as_u16() {
                    JOB_ACCEPTED => JobWatchOutcome::Claimed,
                    JOB_RESULT => {
                        let r: JobResult = serde_json::from_str(&ev.content).unwrap_or(JobResult {
                            status: "done".into(),
                            body: ev.content.clone(),
                            witness: None,
                        });
                        JobWatchOutcome::Settled(serde_json::json!({
                            "ok": true,
                            "job": job_id,
                            "witness": r.witness,
                            "body": r.body,
                        }))
                    }
                    JOB_ERROR => {
                        let e: JobError = serde_json::from_str(&ev.content).unwrap_or(JobError {
                            message: ev.content.clone(),
                        });
                        JobWatchOutcome::Failed(e.message)
                    }
                    _ => continue,
                };
                if !live {
                    replay_best = Some(more_terminal(replay_best.take(), outcome));
                    continue;
                }
                if spec.claimed && matches!(outcome, JobWatchOutcome::Claimed) {
                    continue;
                }
                return outcome;
            }
            Some(SubscriptionEvent::Eose) => {
                if live {
                    continue;
                }
                live = true;
                match replay_best.take() {
                    Some(JobWatchOutcome::Settled(value)) => {
                        return JobWatchOutcome::Settled(value);
                    }
                    Some(JobWatchOutcome::Failed(error)) => {
                        return JobWatchOutcome::Failed(error);
                    }
                    Some(JobWatchOutcome::Claimed) if !spec.claimed => {
                        return JobWatchOutcome::Claimed;
                    }
                    // No terminal, or only the already-seen claim: wait live.
                    _ => continue,
                }
            }
            // Closed markers carry no terminal: keep waiting.
            Some(_) => continue,
            None => {
                return JobWatchOutcome::Failed("dispatch await: job watch stream closed".into());
            }
        }
    }
}

/// The most terminal of two watched mails: a result/error beats a claim.
/// Two terminals in one replay should not happen; last wins.
fn more_terminal(current: Option<JobWatchOutcome>, next: JobWatchOutcome) -> JobWatchOutcome {
    let next_terminal = matches!(
        next,
        JobWatchOutcome::Settled(_) | JobWatchOutcome::Failed(_)
    );
    match current {
        Some(cur) if !next_terminal => cur,
        _ => next,
    }
}

/// Build the mailbox-backed [`JobWatcher`] every host needs: subscribe on
/// the bus and resolve one [`JobWatchSpec`] wait against it. One
/// construction site — hosts call this instead of closing over
/// [`watch_job_once`] themselves.
pub fn bus_job_watcher(bus: Arc<dyn Bus>) -> JobWatcher {
    Arc::new(move |spec: JobWatchSpec| {
        let bus = bus.clone();
        Box::pin(async move { watch_job_once(&bus, &spec).await })
            as std::pin::Pin<Box<dyn std::future::Future<Output = JobWatchOutcome> + Send>>
    })
}

/// The host's job watcher: resolve one [`JobWatchSpec`] wait against the
/// mailbox. Injected like [`super::spawn::SpawnLauncher`]; tests fake it,
/// the embedding wires the bus subscription.
pub type JobWatcher = std::sync::Arc<
    dyn Fn(
            JobWatchSpec,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = JobWatchOutcome> + Send>>
        + Send
        + Sync,
>;

/// One effect the host must run. Data only.
#[derive(Clone, Debug)]
pub enum RunEffect {
    /// Execute one step through the existing boundary.
    ExecuteStep {
        /// Step index (authored position).
        index: usize,
        /// The step.
        step: Box<Step>,
        /// The loop iteration this pass belongs to, when the step is a loop
        /// body step. Rides the invocation's iteration key, so N passes are N
        /// effects (bd-loop-rewrites-fap8).
        iteration: Option<u32>,
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
    /// Watch a published 43001 for its 43002/43004/43006 terminal.
    /// Emitted once the publication receipt yields the job id (StepDone)
    /// and again after a claim, for the result wait. Watching never
    /// republishes: a restart re-issues the watch, never the dispatch.
    WatchJob {
        /// Step name.
        node: String,
        /// The published 43001 job id (hex).
        job_id: String,
    },
    /// Wait for an approval decision.
    AwaitApproval {
        /// Step name.
        step: String,
        /// Prompt.
        message: String,
        /// Absolute deadline (ms); `None` = unbounded.
        deadline_ms: Option<u64>,
        /// Authored bound in seconds.
        timeout_secs: Option<u64>,
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
        let plan = RunPlan::compile(&workflow);
        Self {
            run_id: run_id.into(),
            workflow,
            plan,
            trigger,
            step_index: 0,
            outputs: BTreeMap::new(),
            loops: BTreeMap::new(),
            phase: RunPhase::Ready,
        }
    }

    /// Fill a deserialized model's plan when it predates the branch slice.
    /// Deterministic, so a restart compiles the same entries.
    pub fn recompile_plan(&mut self) {
        if self.plan.entries.is_empty() && !self.workflow.steps.is_empty() {
            self.plan = RunPlan::compile(&self.workflow);
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
            if let Some(reason) = unsupported_reason(&next.workflow) {
                return (next, vec![RunEffect::Unsupported { reason }]);
            }
            next.phase = RunPhase::Running;
            let effects = next_effects(&mut next, *now_ms, spawns);
            (next, effects)
        }
        RunMsg::StepDone { value, now_ms } => {
            if let RunPhase::AwaitingJob { node, .. } = next.phase.clone() {
                // The 43001 publication receipt landed: record the job id
                // and start watching it. A failure value means publication
                // itself failed and settles loud; a receipt without a job
                // id (buffered/deferred/skipped) can never be watched, so
                // it fails fast instead of waiting out a dead deadline.
                let Some((authored, step)) = next
                    .plan
                    .step_at(next.step_index)
                    .map(|(authored, step)| (authored, step.clone()))
                else {
                    return (next, Vec::new());
                };
                let name = step_name(&step, authored);
                if node != name {
                    return (next, Vec::new());
                }
                if is_failure_value(value) {
                    return settle_spawn(&mut next, &node, value.clone(), *now_ms, spawns);
                }
                let Some(job_id) = value.get("job_id").and_then(|v| v.as_str()) else {
                    return settle_spawn(
                        &mut next,
                        &node,
                        serde_json::json!({
                            "ok": false,
                            "error": format!(
                                "dispatch await {node}: publish did not return a job id (deferred delivery)"
                            ),
                        }),
                        *now_ms,
                        spawns,
                    );
                };
                if let RunPhase::AwaitingJob { job, .. } = &mut next.phase {
                    *job = job_id.to_string();
                }
                return (
                    next,
                    vec![RunEffect::WatchJob {
                        node,
                        job_id: job_id.to_string(),
                    }],
                );
            }
            if next.phase != RunPhase::Running {
                return (next, Vec::new());
            }
            let Some((authored, step)) = next
                .plan
                .step_at(next.step_index)
                .map(|(authored, step)| (authored, step.clone()))
            else {
                return (next, Vec::new());
            };
            let name = step_name(&step, authored);
            if step.on_error == StepOnError::FailRun && is_failure_value(value) {
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
            if let RunPhase::AwaitingApproval {
                step,
                deadline_ms: Some(deadline),
                timeout_secs,
                ..
            } = next.phase.clone()
            {
                if *now_ms < deadline {
                    return (next, Vec::new());
                }
                let secs = timeout_secs.unwrap_or(0);
                let value = serde_json::json!({
                    "ok": false,
                    "kind": "timeout",
                    "step": step.clone(),
                    "error": format!("approval timed out after {secs}s: {step}"),
                });
                return settle_spawn(&mut next, &step, value, *now_ms, spawns);
            }
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
                let timeout =
                    next.plan
                        .step_at(next.step_index)
                        .map_or(0, |(_, s)| match &s.action {
                            Action::Spawn {
                                no_claim_timeout, ..
                            } => *no_claim_timeout,
                            _ => 0,
                        });
                let error = format!(
                    "spawn-await {}: timed out after {}s",
                    spec.spawn_key, timeout
                );
                return settle_spawn(
                    &mut next,
                    &node,
                    serde_json::json!({"ok": false, "error": error}),
                    *now_ms,
                    spawns,
                );
            }
            if let RunPhase::AwaitingJob {
                node,
                claim_deadline_ms,
                result_deadline_ms,
                claimed,
                no_claim_timeout,
                result_timeout,
                ..
            } = next.phase.clone()
            {
                let (deadline, error) = if claimed {
                    (
                        result_deadline_ms,
                        result_timeout.map(|t| {
                            format!(
                                "dispatch await {node}: result_timeout after {t}s with no 43004/43006"
                            )
                        }),
                    )
                } else {
                    (
                        claim_deadline_ms,
                        Some(format!(
                            "dispatch await {node}: no claim after {no_claim_timeout}s"
                        )),
                    )
                };
                let Some(deadline) = deadline else {
                    return (next, Vec::new());
                };
                if deadline > *now_ms {
                    return (
                        next,
                        vec![RunEffect::ParkUntil {
                            until_ms: deadline,
                            reason: ParkReason::Job,
                        }],
                    );
                }
                let Some(error) = error else {
                    return (next, Vec::new());
                };
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
            let Some((authored, step)) = next
                .plan
                .step_at(next.step_index)
                .map(|(authored, step)| (authored, step.clone()))
            else {
                return (next, Vec::new());
            };
            let name = step_name(&step, authored);
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
        RunMsg::JobClaimed { node, now_ms } => {
            // A duplicate claim re-arms the same wait: the fold never
            // answers a claim with nothing, so the run cannot stall with
            // no WatchJob and no park in flight. (A duplicate cannot loop:
            // the re-watch carries claimed and skips the seen 43002.)
            let (result_timeout, job_id) = match &next.phase {
                RunPhase::AwaitingJob {
                    node: awaiting,
                    result_timeout,
                    job,
                    ..
                } if awaiting == node => (*result_timeout, job.clone()),
                _ => return (next, Vec::new()),
            };
            let result_deadline_ms = result_timeout
                .filter(|t| *t > 0)
                .map(|t| now_ms.saturating_add(t.saturating_mul(1_000)));
            if let RunPhase::AwaitingJob {
                claimed,
                result_deadline_ms: slot,
                ..
            } = &mut next.phase
            {
                *claimed = true;
                *slot = result_deadline_ms;
            }
            // Keep watching for the terminal: the park alone would sleep
            // past the result in drivers that only wake on effects.
            let mut effects = vec![RunEffect::WatchJob {
                node: node.to_string(),
                job_id,
            }];
            if let Some(until_ms) = result_deadline_ms {
                effects.push(RunEffect::ParkUntil {
                    until_ms,
                    reason: ParkReason::Job,
                });
            }
            (next, effects)
        }
        RunMsg::JobSettled {
            node,
            value,
            now_ms,
        } => {
            let RunPhase::AwaitingJob { node: awaiting, .. } = next.phase.clone() else {
                return (next, Vec::new());
            };
            if &awaiting != node {
                return (next, Vec::new());
            }
            settle_spawn(&mut next, node, value.clone(), *now_ms, spawns)
        }
        RunMsg::JobFailed {
            node,
            error,
            now_ms,
        } => {
            let RunPhase::AwaitingJob { node: awaiting, .. } = next.phase.clone() else {
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
            let Some((authored, step)) = next
                .plan
                .step_at(next.step_index)
                .map(|(authored, step)| (authored, step.clone()))
            else {
                return (next, Vec::new());
            };
            let name = step_name(&step, authored);
            next.outputs.insert(name, decision.clone());
            next.outputs.insert("decision".into(), decision.clone());
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

pub(crate) fn next_effects(
    model: &mut RunModel,
    now_ms: u64,
    spawns: &[Option<SpawnSpec>],
) -> Vec<RunEffect> {
    loop {
        let Some(entry) = model.plan.entries.get(model.step_index).cloned() else {
            model.phase = RunPhase::Succeeded;
            return vec![RunEffect::Complete {
                output: serde_json::json!({"status": "succeeded", "steps": model.outputs}),
            }];
        };
        match entry {
            PlanEntry::Step { authored, step } => {
                if let Action::Reduce { .. } = &step.action {
                    let name = step_name(step.as_ref(), authored);
                    let value = crate::engine::reduce::evaluate(
                        &step.action,
                        &model.trigger,
                        &model.outputs,
                        &model.workflow.name,
                        &name,
                    );
                    model.outputs.insert(name.clone(), value.clone());
                    if step.on_error == StepOnError::FailRun && is_failure_value(&value) {
                        let output = fail_run_value(&name, &value, None);
                        model.phase = RunPhase::Failed {
                            error: output
                                .get("error")
                                .and_then(|e| e.as_str())
                                .unwrap_or("reduce failed")
                                .to_string(),
                        };
                        return vec![RunEffect::Fail { step: name, output }];
                    }
                    model.step_index += 1;
                    model.phase = RunPhase::Running;
                    continue;
                }
                let iteration = current_iteration(model);
                return step_effects(model, authored, *step, iteration, now_ms, spawns);
            }
            PlanEntry::Loop {
                authored,
                body_len: _,
                after,
            } => {
                let name = plan_step_name(model, authored);
                let step = model.workflow.steps.get(authored).cloned();
                let Some(Action::Loop { max, min, .. }) = step.as_ref().map(|s| s.action.clone())
                else {
                    continue;
                };
                let bound = resolve_loop_max(&max, &model.trigger, &outputs_map(&model.outputs))
                    .and_then(|max_n| {
                        resolve_loop_min(
                            min.as_deref(),
                            &model.trigger,
                            &outputs_map(&model.outputs),
                        )
                        .and_then(|min_n| {
                            if min_n > max_n {
                                Err(format!("loop min {min_n} exceeds max {max_n}"))
                            } else {
                                Ok((max_n, min_n))
                            }
                        })
                    });
                match bound {
                    Ok((max_n, min_n)) => {
                        model.loops.insert(
                            name,
                            LoopProgress {
                                iteration: 1,
                                max: max_n,
                                min: min_n,
                            },
                        );
                        model.step_index += 1;
                        model.phase = RunPhase::Running;
                    }
                    Err(error) => {
                        let value = serde_json::json!({ "ok": false, "error": error });
                        let fail_run = step
                            .as_ref()
                            .is_some_and(|s| s.on_error == StepOnError::FailRun);
                        model.outputs.insert(name.clone(), value.clone());
                        if fail_run {
                            let output = fail_run_value(&name, &value, None);
                            model.phase = RunPhase::Failed {
                                error: output
                                    .get("error")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("loop bound refused")
                                    .to_string(),
                            };
                            return vec![RunEffect::Fail { step: name, output }];
                        }
                        model.step_index = after;
                        model.phase = RunPhase::Running;
                    }
                }
            }
            PlanEntry::LoopEnd {
                authored,
                body_start,
                after,
            } => {
                let name = plan_step_name(model, authored);
                let step = model.workflow.steps.get(authored).cloned();
                let (until, fail_run) = match step.as_ref().map(|s| &s.action) {
                    Some(Action::Loop { until, .. }) => (
                        until.clone().filter(|g| !g.trim().is_empty()),
                        step.as_ref()
                            .is_some_and(|s| s.on_error == StepOnError::FailRun),
                    ),
                    _ => (None, false),
                };
                let progress = model.loops.get(&name).copied().unwrap_or(LoopProgress {
                    iteration: 1,
                    max: 1,
                    min: 1,
                });
                let verdict = if progress.iteration >= progress.min {
                    until
                        .as_deref()
                        .map(|guard| evaluate_pure_guard(guard, &model.trigger, &model.outputs))
                } else {
                    None
                };
                let exited = match verdict {
                    Some(PureGuard::True) => Some(serde_json::json!({
                        "ok": true,
                        "iterations": progress.iteration,
                        "stopped": "until",
                    })),
                    Some(PureGuard::Error(error)) => Some(serde_json::json!({
                        "ok": false,
                        "error": error,
                        "iterations": progress.iteration,
                    })),
                    Some(PureGuard::False) => None,
                    None => None,
                };
                let exited = exited.or_else(|| {
                    if progress.iteration < progress.max {
                        return None;
                    }
                    Some(if until.is_some() {
                        serde_json::json!({
                            "ok": false,
                            "error": format!(
                                "loop exhausted after {} iterations without until",
                                progress.max
                            ),
                            "iterations": progress.max,
                        })
                    } else {
                        serde_json::json!({ "ok": true, "iterations": progress.max })
                    })
                });
                match exited {
                    Some(value) => {
                        let failed = is_failure_value(&value);
                        model.outputs.insert(name.clone(), value.clone());
                        if failed && fail_run {
                            let mut output = fail_run_value(&name, &value, None);
                            if let Some(iterations) = value.get("iterations") {
                                output["iterations"] = iterations.clone();
                            }
                            model.phase = RunPhase::Failed {
                                error: output
                                    .get("error")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("loop failed")
                                    .to_string(),
                            };
                            return vec![RunEffect::Fail { step: name, output }];
                        }
                        model.step_index = after;
                        model.phase = RunPhase::Running;
                    }
                    None => {
                        model.loops.insert(
                            name,
                            LoopProgress {
                                iteration: progress.iteration + 1,
                                max: progress.max,
                                min: progress.min,
                            },
                        );
                        model.step_index = body_start;
                        model.phase = RunPhase::Running;
                    }
                }
            }
            PlanEntry::Branch {
                authored,
                guard,
                then_len,
                else_len,
            } => {
                let name = model.workflow.steps.get(authored).map_or_else(
                    || format!("step_{}", authored + 1),
                    |s| step_name(s, authored),
                );
                match evaluate_pure_guard(&guard, &model.trigger, &model.outputs) {
                    PureGuard::True => {
                        model.step_index += 1;
                        model.phase = RunPhase::Running;
                    }
                    PureGuard::False => {
                        model.step_index += then_len + 2;
                        model.phase = RunPhase::Running;
                    }
                    PureGuard::Error(error) => {
                        let value = serde_json::json!({"ok": false, "error": error});
                        let fail_run = model
                            .workflow
                            .steps
                            .get(authored)
                            .is_some_and(|s| s.on_error == StepOnError::FailRun);
                        model.outputs.insert(name.clone(), value.clone());
                        if fail_run {
                            let output = fail_run_value(&name, &value, None);
                            model.phase = RunPhase::Failed {
                                error: output
                                    .get("error")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("branch guard failed")
                                    .to_string(),
                            };
                            return vec![RunEffect::Fail { step: name, output }];
                        }
                        model.step_index += then_len + else_len + 3;
                        model.phase = RunPhase::Running;
                    }
                }
            }
            PlanEntry::ArmEnd {
                branch,
                side,
                after,
            } => {
                model
                    .outputs
                    .insert(branch, serde_json::json!({"branch": side.as_str()}));
                model.step_index = after;
                model.phase = RunPhase::Running;
            }
        }
    }
}

/// The authored name of a plan entry's step.
fn plan_step_name(model: &RunModel, authored: usize) -> String {
    model.workflow.steps.get(authored).map_or_else(
        || format!("step_{}", authored + 1),
        |s| step_name(s, authored),
    )
}

/// The loop pass the cursor is in, when it is inside a loop body.
fn current_iteration(model: &RunModel) -> Option<u32> {
    for (pos, entry) in model.plan.entries.iter().enumerate() {
        let PlanEntry::Loop {
            authored, body_len, ..
        } = entry
        else {
            continue;
        };
        if model.step_index > pos && model.step_index <= pos + body_len {
            let name = plan_step_name(model, *authored);
            return model.loops.get(&name).map(|progress| progress.iteration);
        }
    }
    None
}

/// Resolve a loop bound from the fold's pure inputs (trigger + outputs).
///
/// Admission runs this against the trigger before any row, so an unbounded or
/// non-trigger-resolvable loop refuses the workflow; the drive-time call is the
/// same read.
pub(crate) fn resolve_loop_max(
    max: &str,
    trigger: &serde_json::Value,
    outputs: &serde_json::Map<String, serde_json::Value>,
) -> Result<u32, String> {
    let rendered = resolve_template_ref(max, trigger, outputs)
        .and_then(|value| match value {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.trim().parse().ok(),
            _ => None,
        })
        .or_else(|| max.trim().parse().ok());
    let bound = rendered
        .filter(|n| *n >= 1)
        .ok_or_else(|| format!("loop max {max:?} must render to a positive integer"))?;
    if bound > u64::from(MAX_LOOP_ITERS) {
        return Err(format!(
            "loop max {bound} exceeds the bounded cap {MAX_LOOP_ITERS}"
        ));
    }
    Ok(bound as u32)
}

/// Resolve a loop minimum from the fold's pure inputs, mirroring
/// [`resolve_loop_max`]. Absent means 1: the guard may stop the loop after
/// the first pass, exactly as before `min` existed.
pub(crate) fn resolve_loop_min(
    min: Option<&str>,
    trigger: &serde_json::Value,
    outputs: &serde_json::Map<String, serde_json::Value>,
) -> Result<u32, String> {
    let Some(min) = min else {
        return Ok(1);
    };
    let rendered = resolve_template_ref(min, trigger, outputs)
        .and_then(|value| match value {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.trim().parse().ok(),
            _ => None,
        })
        .or_else(|| min.trim().parse().ok());
    let bound = rendered
        .filter(|n| *n >= 1)
        .ok_or_else(|| format!("loop min {min:?} must render to a positive integer"))?;
    if bound > u64::from(MAX_LOOP_ITERS) {
        return Err(format!(
            "loop min {bound} exceeds the bounded cap {MAX_LOOP_ITERS}"
        ));
    }
    Ok(bound as u32)
}

/// Effects for one authored linear step.
///
/// `authored` is the step's authored position (top level, or its branch arm),
/// so names and spawn-spec positions do not move when a branch expands above
/// them.
fn step_effects(
    model: &mut RunModel,
    authored: usize,
    step: Step,
    iteration: Option<u32>,
    now_ms: u64,
    spawns: &[Option<SpawnSpec>],
) -> Vec<RunEffect> {
    let name = step_name(&step, authored);
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
        Action::RequestApproval {
            message,
            timeout_secs,
        } => {
            let deadline_ms = timeout_secs
                .filter(|s| *s > 0)
                .map(|s| now_ms.saturating_add(s.saturating_mul(1_000)));
            model.phase = RunPhase::AwaitingApproval {
                step: name.clone(),
                message: message.clone(),
                deadline_ms,
                timeout_secs: *timeout_secs,
            };
            vec![RunEffect::AwaitApproval {
                step: name,
                message: message.clone(),
                deadline_ms,
                timeout_secs: *timeout_secs,
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
            let Some(spec) = spawns.get(model.step_index).and_then(Option::as_ref) else {
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
        Action::DispatchJob {
            await_: Some(DispatchAwait::Result),
            no_claim_timeout,
            result_timeout,
            ..
        } => {
            // D4.1: publish through the normal ExecuteStep path, then
            // suspend on the job's terminal. The 43001 goes out exactly as
            // a fire-and-forget dispatch would; the fold only adds the wait.
            let claim_deadline_ms = (*no_claim_timeout > 0)
                .then(|| now_ms.saturating_add(no_claim_timeout.saturating_mul(1_000)));
            model.phase = RunPhase::AwaitingJob {
                node: name.clone(),
                job: String::new(),
                claim_deadline_ms,
                result_deadline_ms: None,
                claimed: false,
                no_claim_timeout: *no_claim_timeout,
                result_timeout: *result_timeout,
            };
            let mut effects = vec![RunEffect::ExecuteStep {
                index: authored,
                step: Box::new(step),
                iteration,
            }];
            if let Some(until_ms) = claim_deadline_ms {
                effects.push(RunEffect::ParkUntil {
                    until_ms,
                    reason: ParkReason::Job,
                });
            }
            effects
        }
        action => {
            if matches!(partition(action), ActionPartition::HostSink { .. }) {
                model.phase = RunPhase::Running;
                return vec![RunEffect::ExecuteStep {
                    index: authored,
                    step: Box::new(step),
                    iteration,
                }];
            }
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
/// linear model (host-sink actions plus `delay`, `request_approval`, `spawn`,
/// and a pure-guard `branch` whose arms are host-sink leaves); a `graph:`
/// workflow by the graph model under [`graph_unsupported_reason`]. Everything
/// else is a shape the host keeps on the old engine, so it is refused before a
/// run row or a state row exists.
///
/// This is SHAPE only (bd-spawn-launcher-flip-d8mv). Admission of a spawn is
/// capability-based and lives with the host that must perform it: a host
/// without a `SpawnLauncher` refuses the workflow through
/// [`super::spawn::spawn_launcher_refusal`] before this model is opened.
/// The public engine's value shape for a shape refusal: the old engine
/// returned a `{status:failed,…}` VALUE, never an `Err`, so callers keep
/// reading failures from the output.
pub fn refusal_value(reason: &str, failed_step: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "failed",
        "error": reason,
        "failed_step": failed_step,
    })
}

pub fn unsupported_reason(workflow: &Workflow) -> Option<String> {
    if needs_graph_model(workflow) {
        return graph_unsupported_reason(workflow);
    }
    for (index, step) in workflow.steps.iter().enumerate() {
        match &step.action {
            Action::Branch {
                if_guard,
                then,
                else_steps,
            } => {
                if let Err(reason) = check_pure_guard(if_guard) {
                    return Some(format!(
                        "step '{}' branch guard is not pure: {reason}",
                        step_name(step, index)
                    ));
                }
                for arm_step in then.iter().chain(else_steps.iter()) {
                    let sequencable = matches!(
                        partition(&arm_step.action),
                        ActionPartition::HostSink { .. }
                    ) || matches!(
                        arm_step.action,
                        Action::Spawn { .. } | Action::Reduce { .. }
                    );
                    if !sequencable {
                        return Some(format!(
                            "step '{}' branch arm step '{}' uses '{}', which the linear model cannot sequence inside an arm",
                            step_name(step, index),
                            arm_step.name.as_deref().unwrap_or("<unnamed>"),
                            arm_step.action.type_name()
                        ));
                    }
                }
            }
            Action::Loop {
                max,
                min,
                until,
                body,
            } => {
                let literal = max.trim().parse::<u64>().is_ok();
                let template = max.trim().starts_with("{{") && max.trim().ends_with("}}");
                if !literal && !template {
                    return Some(format!(
                        "step '{}' loop `max` {max:?} must be a positive integer or a trigger/inputs template",
                        step_name(step, index)
                    ));
                }
                if max.contains("{{steps.") {
                    return Some(format!(
                        "step '{}' loop `max` reads an upstream output, which is not resolvable before the loop starts",
                        step_name(step, index)
                    ));
                }
                if let Some(min) = min {
                    let min_literal = min.trim().parse::<u64>().is_ok();
                    let min_template = min.trim().starts_with("{{") && min.trim().ends_with("}}");
                    if !min_literal && !min_template {
                        return Some(format!(
                            "step '{}' loop `min` {min:?} must be a positive integer or a trigger/inputs template",
                            step_name(step, index)
                        ));
                    }
                    if min.contains("{{steps.") {
                        return Some(format!(
                            "step '{}' loop `min` reads an upstream output, which is not resolvable before the loop starts",
                            step_name(step, index)
                        ));
                    }
                    if let Ok(min_n) = min.trim().parse::<u64>() {
                        if min_n < 1 {
                            return Some(format!(
                                "step '{}' loop `min` must be at least 1",
                                step_name(step, index)
                            ));
                        }
                        if let Ok(max_n) = max.trim().parse::<u64>()
                            && min_n > max_n
                        {
                            return Some(format!(
                                "step '{}' loop `min` {min_n} exceeds `max` {max_n}",
                                step_name(step, index)
                            ));
                        }
                    }
                }
                if let Some(guard) = until
                    && let Err(reason) = check_pure_guard(guard)
                {
                    return Some(format!(
                        "step '{}' loop `until` is not pure: {reason}",
                        step_name(step, index)
                    ));
                }
                for body_step in body {
                    if !matches!(
                        partition(&body_step.action),
                        ActionPartition::HostSink { .. }
                    ) {
                        return Some(format!(
                            "step '{}' loop body step '{}' uses '{}', which the linear model cannot sequence inside a loop",
                            step_name(step, index),
                            body_step.name.as_deref().unwrap_or("<unnamed>"),
                            body_step.action.type_name()
                        ));
                    }
                }
            }
            action if !is_linear(action) => {
                return Some(format!(
                    "step '{}' uses '{}', which the linear model cannot sequence yet",
                    step_name(step, index),
                    action.type_name()
                ));
            }
            _ => {}
        }
    }
    None
}

fn is_linear(action: &Action) -> bool {
    matches!(
        action,
        Action::Delay { .. }
            | Action::RequestApproval { .. }
            | Action::Spawn { .. }
            | Action::Reduce { .. }
    ) || matches!(partition(action), ActionPartition::HostSink { .. })
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
    let step = model
        .plan
        .step_at(model.step_index)
        .map(|(authored, step)| (authored, step.clone()));
    let name = step
        .as_ref()
        .map_or_else(|| node.to_string(), |(authored, s)| step_name(s, *authored));
    if step
        .as_ref()
        .is_some_and(|(_, s)| s.on_error == StepOnError::FailRun)
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
        RunPhase::AwaitingJob {
            node,
            job,
            claim_deadline_ms,
            result_deadline_ms,
            claimed,
            ..
        } => {
            // No republish: the 43001 went out through ExecuteStep before the
            // suspend, so resume re-issues the watch and the park. A
            // pre-publication crash (no job id yet) waits out the claim
            // deadline and fails loud, never hangs.
            let mut effects = Vec::new();
            if !job.is_empty() {
                effects.push(RunEffect::WatchJob {
                    node: node.clone(),
                    job_id: job.clone(),
                });
            }
            let deadline = if *claimed {
                *result_deadline_ms
            } else {
                *claim_deadline_ms
            };
            if let Some(until_ms) = deadline {
                effects.push(RunEffect::ParkUntil {
                    until_ms,
                    reason: ParkReason::Job,
                });
            }
            effects
        }
        RunPhase::AwaitingApproval {
            step,
            message,
            deadline_ms,
            timeout_secs,
        } => vec![RunEffect::AwaitApproval {
            step: step.clone(),
            message: message.clone(),
            deadline_ms: *deadline_ms,
            timeout_secs: *timeout_secs,
        }],
        RunPhase::Running => match model.plan.step_at(model.step_index) {
            Some((authored, step)) => vec![RunEffect::ExecuteStep {
                index: authored,
                step: Box::new(step.clone()),
                iteration: current_iteration(model),
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

/// The largest `retry:` policy the scheduler will honor
/// (bd-durable-retry-policy-rrck). A typo cannot park a run for days.
pub const MAX_RETRY_ATTEMPTS: u32 = 10;

/// Why a node parked.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeParkReason {
    /// A `delay` node is waiting out its own author deadline.
    Delay,
    /// A retryable failure is waiting out its backoff before a re-dispatch.
    Retry,
    /// A dispatched job is being awaited; the wake is its claim/result
    /// deadline (bd-jhz8x.5).
    Job,
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
    /// Waiting on a `call_workflow` child run (bd-durable-call-workflow-mg6y).
    /// The trigger and id are stored, so a resume RE-ATTACHES to the same child
    /// instead of opening a second one.
    AwaitingChild {
        /// Wave the node was scheduled in.
        wave: usize,
        /// Scheduler-level attempt (1-based; the child id embeds it).
        attempt: u32,
        /// `{parent_run_id}:{node}:{attempt}`.
        child_run_id: String,
        /// The callee workflow name (rendered).
        workflow: String,
        /// The exact trigger the child opens with.
        trigger: serde_json::Value,
    },
    /// Waiting for a dispatched job's terminal (bd-jhz8x.5), the graph model's
    /// twin of the linear `RunPhase::AwaitingJob`. The publish receipt landed,
    /// so the node keeps the job id and BOTH deadlines instead of settling on
    /// the receipt; a resume re-issues the watch, never the publish.
    AwaitingJob {
        /// Wave the node was scheduled in.
        wave: usize,
        /// Scheduler-level attempt (1-based).
        attempt: u32,
        /// Published 43001 id (hex).
        job: String,
        /// Absolute claim deadline; `None` = the authored bound was 0.
        #[serde(default)]
        claim_deadline_ms: Option<u64>,
        /// Absolute result deadline once claimed.
        #[serde(default)]
        result_deadline_ms: Option<u64>,
        /// Whether a 43002 claim arrived (switches the active deadline).
        claimed: bool,
        /// Claim bound in seconds, for the timeout error text.
        no_claim_timeout: u64,
        /// Result bound in seconds, for the timeout error text.
        #[serde(default)]
        result_timeout: Option<u64>,
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
        /// Absolute deadline (ms); `None` = unbounded.
        #[serde(default)]
        deadline_ms: Option<u64>,
        /// Authored bound in seconds.
        #[serde(default)]
        timeout_secs: Option<u64>,
    },
    /// Landed.
    Settled,
}

impl NodeState {
    /// True while the node owes the host or the engine something.
    pub const fn is_in_flight(&self) -> bool {
        matches!(
            self,
            Self::Executing { .. }
                | Self::Parked { .. }
                | Self::AwaitingJob { .. }
                | Self::AwaitingApproval { .. }
                | Self::AwaitingChild { .. }
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
    /// Per-copy foreach items, keyed by copy node name
    /// (bd-durable-foreach-jpgx). The item is what `{{item}}` resolves to
    /// inside that copy; it is run data, so it lives on the model rather than
    /// in the authored workflow.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub items: BTreeMap<String, serde_json::Value>,
    /// Current phase.
    pub phase: GraphPhase,
}

/// One persisted durable run, tagged by model shape.
///
/// Before this wrapper the store was shape-blind: every row was parsed as
/// `RunModel` first, so a parked DAG row looked corrupt and was left behind on
/// restart. The tag makes the shape explicit; [`Self::decode`] still accepts
/// bare legacy models so an upgrade resumes rows written by the old host.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "model", content = "state", rename_all = "snake_case")]
pub enum PersistedWorkflowModel {
    /// A `steps:` workflow.
    Linear(Box<RunModel>),
    /// A `graph:` workflow.
    Graph(Box<GraphModel>),
}

impl PersistedWorkflowModel {
    /// Decode a new tagged row, falling back to a bare pre-tag model.
    ///
    /// The fallback is deliberately one-way: new writes are always tagged, so
    /// a future shape cannot be mistaken for an old linear row.
    pub fn decode(json: &str) -> Result<Self, String> {
        if let Ok(model) = serde_json::from_str::<Self>(json) {
            return Ok(model);
        }
        if let Ok(model) = serde_json::from_str::<RunModel>(json) {
            return Ok(Self::Linear(Box::new(model)));
        }
        if let Ok(model) = serde_json::from_str::<GraphModel>(json) {
            return Ok(Self::Graph(Box::new(model)));
        }
        Err("neither a tagged durable model nor a legacy linear/graph model".to_string())
    }

    /// True for terminal phases in either shape.
    pub const fn is_terminal(&self) -> bool {
        match self {
            Self::Linear(model) => model.is_terminal(),
            Self::Graph(model) => model.is_terminal(),
        }
    }

    /// The stored workflow definition.
    pub fn workflow(&self) -> &Workflow {
        match self {
            Self::Linear(model) => &model.workflow,
            Self::Graph(model) => &model.workflow,
        }
    }

    /// The stored trigger payload.
    pub fn trigger(&self) -> &serde_json::Value {
        match self {
            Self::Linear(model) => &model.trigger,
            Self::Graph(model) => &model.trigger,
        }
    }

    /// The run key.
    pub fn run_id(&self) -> &str {
        match self {
            Self::Linear(model) => &model.run_id,
            Self::Graph(model) => &model.run_id,
        }
    }
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
    /// A dispatched job was claimed (43002): switch the active deadline to the
    /// result bound and keep watching.
    JobClaimed {
        /// Node name.
        node: String,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A dispatched job settled (43004 with the step output value).
    JobSettled {
        /// Node name.
        node: String,
        /// The settled step output value.
        value: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A dispatched job failed (43006, or a watched job that died).
    JobFailed {
        /// Node name.
        node: String,
        /// Why.
        error: String,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A park (delay or retry backoff) is due.
    Woke {
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A `call_workflow` child finished successfully.
    ChildSucceeded {
        /// Parent node name.
        node: String,
        /// The child's terminal value.
        output: serde_json::Value,
        /// Host time, unix millis.
        now_ms: u64,
    },
    /// A `call_workflow` child finished with a failure value (the full bundle).
    ChildFailed {
        /// Parent node name.
        node: String,
        /// The child's terminal failure value.
        output: serde_json::Value,
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
        /// The foreach item this copy renders as `{{item}}`, when it is one.
        item: Option<serde_json::Value>,
    },
    /// Watch one dispatched job for its terminal (bd-jhz8x.5). The node is in
    /// [`NodeState::AwaitingJob`]; a duplicate watch is safe, the claim/settle
    /// verbs are idempotent per node.
    WatchJob {
        /// Node name.
        node: String,
        /// Published job id.
        job_id: String,
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
        /// Absolute deadline (ms); `None` = unbounded.
        deadline_ms: Option<u64>,
        /// Authored bound in seconds.
        timeout_secs: Option<u64>,
    },
    /// Open (or re-attach to) one `call_workflow` child run.
    ChildRun {
        /// Parent node name.
        node: String,
        /// `{parent_run_id}:{node}:{attempt}`.
        child_run_id: String,
        /// The callee workflow name.
        workflow: String,
        /// The child trigger.
        trigger: serde_json::Value,
    },
    /// The child reached a terminal state (the join point).
    ChildJoin {
        /// Parent node name.
        node: String,
        /// The child run that finished.
        child_run_id: String,
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
            items: BTreeMap::new(),
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
            if next.is_terminal() {
                return (next, Vec::new());
            }
            let Some(NodeState::Executing { wave, attempt, .. }) =
                next.node_state.get(node).cloned()
            else {
                return (next, Vec::new());
            };
            let node_def = next
                .workflow
                .graph
                .as_ref()
                .and_then(|g| g.nodes.get(node))
                .cloned();
            let on_error = node_def.as_ref().map(|n| n.on_error);
            let retry_policy = node_def.as_ref().and_then(|n| match &n.action {
                Action::Retry {
                    max_attempts,
                    backoff_secs,
                    ..
                } => Some((*max_attempts, *backoff_secs)),
                _ => None,
            });
            let failed = is_failure_value(value);
            let retrying = failed
                && match retry_policy {
                    Some(_) => true,
                    None => {
                        on_error == Some(StepOnError::FailRun)
                            && RetryCoordinator::is_retryable_value(value)
                    }
                };
            if retrying {
                let coordinator = RetryCoordinator::from_enclosing(retry_policy);
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
            }
            // bd-jhz8x.5: a `dispatch_job await: result` node does NOT settle
            // on its publish receipt — it waits for the job's terminal, exactly
            // as the linear model's `AwaitingJob` does. A receipt with no job id
            // can never be watched, so it fails loud instead of waiting out a
            // deadline nothing will ever answer.
            let dispatch_await = match node_def.as_ref().map(|n| &n.action) {
                Some(Action::DispatchJob {
                    await_: Some(DispatchAwait::Result),
                    no_claim_timeout,
                    result_timeout,
                    ..
                }) => Some((*no_claim_timeout, *result_timeout)),
                _ => None,
            };
            let late_value;
            let value = match (dispatch_await, is_failure_value(value)) {
                (Some((no_claim_timeout, result_timeout)), false) => {
                    match value.get("job_id").and_then(serde_json::Value::as_str) {
                        Some(job_id) => {
                            let claim_deadline_ms = (no_claim_timeout > 0).then(|| {
                                now_ms.saturating_add(no_claim_timeout.saturating_mul(1_000))
                            });
                            next.node_state.insert(
                                node.clone(),
                                NodeState::AwaitingJob {
                                    wave,
                                    attempt,
                                    job: job_id.to_string(),
                                    claim_deadline_ms,
                                    result_deadline_ms: None,
                                    claimed: false,
                                    no_claim_timeout,
                                    result_timeout,
                                },
                            );
                            // Keep watching for the terminal: the park alone
                            // would sleep past a result in drivers that only
                            // wake on effects.
                            let mut effects = vec![GraphEffect::WatchJob {
                                node: node.clone(),
                                job_id: job_id.to_string(),
                            }];
                            if let Some(until_ms) = claim_deadline_ms {
                                effects.push(GraphEffect::ParkUntil {
                                    node: node.clone(),
                                    until_ms,
                                    reason: NodeParkReason::Job,
                                });
                            }
                            return (next, effects);
                        }
                        None => {
                            late_value = serde_json::json!({
                                "ok": false,
                                "error": format!(
                                    "dispatch await {node}: publish did not return a job id (deferred delivery)"
                                ),
                            });
                            &late_value
                        }
                    }
                }
                _ => value,
            };
            let effects =
                settle_graph_node(&mut next, node, value, *now_ms, retrying.then_some(attempt));
            (next, effects)
        }
        GraphMsg::JobClaimed { node, now_ms } => {
            // A duplicate claim re-arms the same wait, so the run cannot stall
            // with no WatchJob and no park in flight.
            let (job, result_timeout) = match next.node_state.get(node).cloned() {
                Some(NodeState::AwaitingJob {
                    job,
                    result_timeout,
                    ..
                }) => (job, result_timeout),
                _ => return (next, Vec::new()),
            };
            let result_deadline_ms = result_timeout
                .filter(|t| *t > 0)
                .map(|t| now_ms.saturating_add(t.saturating_mul(1_000)));
            if let Some(NodeState::AwaitingJob {
                claimed,
                result_deadline_ms: slot,
                ..
            }) = next.node_state.get_mut(node)
            {
                *claimed = true;
                *slot = result_deadline_ms;
            }
            let mut effects = vec![GraphEffect::WatchJob {
                node: node.clone(),
                job_id: job,
            }];
            if let Some(until_ms) = result_deadline_ms {
                effects.push(GraphEffect::ParkUntil {
                    node: node.clone(),
                    until_ms,
                    reason: NodeParkReason::Job,
                });
            }
            (next, effects)
        }
        GraphMsg::JobSettled {
            node,
            value,
            now_ms,
        } => {
            if !matches!(
                next.node_state.get(node),
                Some(NodeState::AwaitingJob { .. })
            ) {
                return (next, Vec::new());
            }
            let effects = settle_graph_node(&mut next, node, value, *now_ms, None);
            (next, effects)
        }
        GraphMsg::JobFailed {
            node,
            error,
            now_ms,
        } => {
            if !matches!(
                next.node_state.get(node),
                Some(NodeState::AwaitingJob { .. })
            ) {
                return (next, Vec::new());
            }
            let value = serde_json::json!({ "ok": false, "error": error });
            let effects = settle_graph_node(&mut next, node, &value, *now_ms, None);
            (next, effects)
        }
        GraphMsg::Woke { now_ms } => {
            if next.phase != GraphPhase::Running || next.is_terminal() {
                return (next, Vec::new());
            }
            let mut effects = Vec::new();
            let timed_out: Vec<(String, Option<u64>)> = next
                .node_state
                .iter()
                .filter_map(|(node, state)| match state {
                    NodeState::AwaitingApproval {
                        deadline_ms: Some(deadline),
                        timeout_secs,
                        ..
                    } if *deadline <= *now_ms => Some((node.clone(), *timeout_secs)),
                    _ => None,
                })
                .collect();
            for (node, secs) in timed_out {
                let error = format!("approval timed out after {}s: {node}", secs.unwrap_or(0));
                let on_error = next
                    .workflow
                    .graph
                    .as_ref()
                    .and_then(|g| g.nodes.get(&node))
                    .map(|n| n.on_error);
                if on_error == Some(StepOnError::FailRun) {
                    let fail = GraphFail {
                        node: node.clone(),
                        error: error.clone(),
                        kind: "timeout".to_string(),
                        disposition: disposition_for_kind("timeout", None).as_str().to_string(),
                        leaf_preview: serde_json::Value::Null,
                        attempts: None,
                    };
                    next.phase = GraphPhase::Failed {
                        error: fail.error.clone(),
                    };
                    return (next, vec![GraphEffect::Fail { fail }]);
                }
                next.outputs.insert(
                    node.clone(),
                    serde_json::json!({
                        "ok": false,
                        "kind": "timeout",
                        "node": node.clone(),
                        "error": error,
                    }),
                );
                next.node_state.insert(node, NodeState::Settled);
            }
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
                    // A job park is owned by the AwaitingJob state (handled
                    // above); the node itself is not parked.
                    NodeParkReason::Job => {}
                    NodeParkReason::Delay => {
                        next.outputs.insert(
                            node.clone(),
                            serde_json::json!({ "ok": true, "waited": true }),
                        );
                        next.node_state.insert(node, NodeState::Settled);
                    }
                    NodeParkReason::Retry => {
                        let inputs = next.outputs.clone();
                        next.node_state.insert(
                            node.clone(),
                            NodeState::Executing {
                                wave,
                                attempt,
                                inputs: inputs.clone(),
                            },
                        );
                        let item = next.items.get(&node).cloned();
                        effects.push(GraphEffect::ExecuteNode {
                            node,
                            wave,
                            attempt,
                            inputs,
                            item,
                        });
                    }
                }
            }
            // bd-jhz8x.5: an awaited job's deadline. Claim first, then result,
            // with the same typed texts the linear model uses.
            let job_deadlines: Vec<(String, u64, Option<String>)> = next
                .node_state
                .iter()
                .filter_map(|(node, state)| match state {
                    NodeState::AwaitingJob {
                        claim_deadline_ms,
                        result_deadline_ms,
                        claimed,
                        no_claim_timeout,
                        result_timeout,
                        ..
                    } => {
                        let (deadline, error) = if *claimed {
                            (
                                *result_deadline_ms,
                                result_timeout.map(|t| {
                                    format!(
                                        "dispatch await {node}: result_timeout after {t}s with no 43004/43006"
                                    )
                                }),
                            )
                        } else {
                            (
                                *claim_deadline_ms,
                                Some(format!(
                                    "dispatch await {node}: no claim after {no_claim_timeout}s"
                                )),
                            )
                        };
                        Some((node.clone(), deadline?, error))
                    }
                    _ => None,
                })
                .collect();
            for (node, until_ms, error) in job_deadlines {
                if until_ms > *now_ms {
                    // A wake that arrived early still owes the park: a driver
                    // that only continues on effects would otherwise stop.
                    effects.push(GraphEffect::ParkUntil {
                        node,
                        until_ms,
                        reason: NodeParkReason::Job,
                    });
                    continue;
                }
                let Some(error) = error else {
                    continue;
                };
                let value = serde_json::json!({ "ok": false, "error": error });
                effects.extend(settle_graph_node(&mut next, &node, &value, *now_ms, None));
                if next.is_terminal() {
                    return (next, effects);
                }
            }
            if next.is_in_flight() {
                return (next, effects);
            }
            effects.extend(schedule(&mut next, *now_ms));
            (next, effects)
        }
        GraphMsg::ChildSucceeded {
            node,
            output,
            now_ms,
        } => {
            if next.is_terminal() {
                return (next, Vec::new());
            }
            let Some(NodeState::AwaitingChild { child_run_id, .. }) =
                next.node_state.get(node).cloned()
            else {
                return (next, Vec::new());
            };
            next.outputs.insert(node.clone(), output.clone());
            next.node_state.insert(node.clone(), NodeState::Settled);
            let mut effects = vec![GraphEffect::ChildJoin {
                node: node.clone(),
                child_run_id,
            }];
            effects.extend(schedule(&mut next, *now_ms));
            (next, effects)
        }
        GraphMsg::ChildFailed {
            node,
            output,
            now_ms,
        } => {
            if next.is_terminal() {
                return (next, Vec::new());
            }
            let Some(NodeState::AwaitingChild { child_run_id, .. }) =
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
            let kind = output
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if on_error != Some(StepOnError::FailRun) {
                next.outputs.insert(node.clone(), output.clone());
                next.node_state.insert(node.clone(), NodeState::Settled);
                let mut effects = vec![GraphEffect::ChildJoin {
                    node: node.clone(),
                    child_run_id,
                }];
                effects.extend(schedule(&mut next, *now_ms));
                return (next, effects);
            }
            let bundle = if output.get("status").and_then(|v| v.as_str()) == Some("failed") {
                output.clone()
            } else {
                fail_run_value(node, output, None)
            };
            let disposition = if kind.is_empty() {
                String::new()
            } else {
                disposition_for_kind(&kind, None).as_str().to_string()
            };
            let fail = GraphFail {
                node: node.clone(),
                error: bundle
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("child failed")
                    .to_string(),
                kind,
                disposition,
                leaf_preview: bundle
                    .get("leaf_preview")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                attempts: None,
            };
            next.phase = GraphPhase::Failed {
                error: fail.error.clone(),
            };
            (
                next,
                vec![
                    GraphEffect::ChildJoin {
                        node: node.clone(),
                        child_run_id,
                    },
                    GraphEffect::Fail { fail },
                ],
            )
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
                item: model.items.get(node).cloned(),
            }),
            NodeState::Parked {
                until_ms, reason, ..
            } => effects.push(GraphEffect::ParkUntil {
                node: node.clone(),
                until_ms: *until_ms,
                reason: *reason,
            }),
            NodeState::AwaitingJob {
                job,
                claim_deadline_ms,
                result_deadline_ms,
                claimed,
                ..
            } => {
                if !job.is_empty() {
                    effects.push(GraphEffect::WatchJob {
                        node: node.clone(),
                        job_id: job.clone(),
                    });
                }
                let deadline = if *claimed {
                    *result_deadline_ms
                } else {
                    *claim_deadline_ms
                };
                if let Some(until_ms) = deadline {
                    effects.push(GraphEffect::ParkUntil {
                        node: node.clone(),
                        until_ms,
                        reason: NodeParkReason::Job,
                    });
                }
            }
            NodeState::AwaitingApproval {
                payload,
                deadline_ms,
                timeout_secs,
                ..
            } => effects.push(GraphEffect::AwaitApproval {
                node: node.clone(),
                payload: payload.clone(),
                deadline_ms: *deadline_ms,
                timeout_secs: *timeout_secs,
            }),
            NodeState::AwaitingChild {
                child_run_id,
                workflow,
                trigger,
                ..
            } => effects.push(GraphEffect::ChildRun {
                node: node.clone(),
                child_run_id: child_run_id.clone(),
                workflow: workflow.clone(),
                trigger: trigger.clone(),
            }),
        }
    }
    effects
}

/// Land one node's value: apply `is_failure_value` + `on_error`, then settle
/// the node or fail the run. Shared by `NodeDone` and the job-await verbs
/// (bd-jhz8x.5), so a WATCHED job's terminal lands exactly as a leaf's own
/// value would — one failure classifier, not two.
fn settle_graph_node(
    next: &mut GraphModel,
    node: &str,
    value: &serde_json::Value,
    now_ms: u64,
    attempts: Option<u32>,
) -> Vec<GraphEffect> {
    let on_error = next
        .workflow
        .graph
        .as_ref()
        .and_then(|g| g.nodes.get(node))
        .map(|n| n.on_error);
    if !is_failure_value(value) || on_error != Some(StepOnError::FailRun) {
        next.outputs.insert(node.to_string(), value.clone());
        next.node_state.insert(node.to_string(), NodeState::Settled);
        return schedule(next, now_ms);
    }
    let kind = value
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
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
        node: node.to_string(),
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
        attempts,
    };
    next.phase = GraphPhase::Failed {
        error: fail.error.clone(),
    };
    vec![GraphEffect::Fail { fail }]
}

/// Schedule the next wave, or settle the run.
///
/// Runs only when nothing is in flight: wave scheduling is what makes a wave's
/// spawn-time snapshot meaningful, and it is the old scheduler's own rule.
fn schedule(model: &mut GraphModel, now_ms: u64) -> Vec<GraphEffect> {
    if model.is_terminal() || model.is_in_flight() {
        return Vec::new();
    }
    loop {
        let Some(graph) = model.workflow.graph.clone() else {
            return Vec::new();
        };
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

        let mut expanded = false;
        {
            let mut expanded_graph = graph.clone();
            for name in &ready {
                let Some(node) = expanded_graph.nodes.get(name).cloned() else {
                    continue;
                };
                let Action::Foreach { over, each } = &node.action else {
                    continue;
                };
                if is_fanout_join(&expanded_graph, name, &node) {
                    continue;
                }
                let over = over.clone();
                let each = (**each).clone();
                let needs = node.needs.clone();
                let snapshot_map = outputs_map(&model.outputs);
                let materialized = resolve_template_ref(&over, &model.trigger, &snapshot_map);
                let items = match materialized {
                    Some(serde_json::Value::Array(items)) => items,
                    other => {
                        let error = match other {
                            Some(_) => format!(
                                "workflow {:?} node {name:?}: foreach `over` {over:?} must be a list",
                                model.workflow.name
                            ),
                            None => format!(
                                "workflow {:?} node {name:?}: foreach `over` {over:?} does not resolve to a value",
                                model.workflow.name
                            ),
                        };
                        let value = serde_json::json!({ "ok": false, "error": error });
                        if node.on_error == StepOnError::FailRun {
                            let output = fail_run_value(name, &value, None);
                            let error = output
                                .get("error")
                                .and_then(|e| e.as_str())
                                .unwrap_or("foreach failed")
                                .to_string();
                            model.phase = GraphPhase::Failed {
                                error: error.clone(),
                            };
                            return vec![GraphEffect::Fail {
                                fail: GraphFail {
                                    node: name.clone(),
                                    error,
                                    kind: String::new(),
                                    disposition: String::new(),
                                    leaf_preview: output
                                        .get("leaf_preview")
                                        .cloned()
                                        .unwrap_or(serde_json::Value::Null),
                                    attempts: None,
                                },
                            }];
                        }
                        model.outputs.insert(name.clone(), value);
                        model.node_state.insert(name.clone(), NodeState::Settled);
                        expanded = true;
                        continue;
                    }
                };
                if items.is_empty() {
                    model
                        .outputs
                        .insert(name.clone(), serde_json::Value::Array(Vec::new()));
                    model.node_state.insert(name.clone(), NodeState::Settled);
                    expanded = true;
                    continue;
                }
                let copies: Vec<String> = (0..items.len()).map(|i| format!("{name}:{i}")).collect();
                for (i, item) in items.into_iter().enumerate() {
                    let copy = copies[i].clone();
                    if expanded_graph.nodes.contains_key(&copy) {
                        let error = format!("foreach copy '{copy}' already exists");
                        model.phase = GraphPhase::Failed {
                            error: error.clone(),
                        };
                        return vec![GraphEffect::Fail {
                            fail: GraphFail {
                                node: copy,
                                error,
                                kind: String::new(),
                                disposition: String::new(),
                                leaf_preview: serde_json::Value::Null,
                                attempts: None,
                            },
                        }];
                    }
                    expanded_graph.nodes.insert(
                        copy.clone(),
                        GraphNode {
                            action: each.action.clone(),
                            needs: needs.clone(),
                            guard: each.guard.clone(),
                            on_error: each.on_error,
                            build_slot: each.build_slot,
                        },
                    );
                    model.node_state.insert(copy.clone(), NodeState::Pending);
                    model.items.insert(copy, item);
                }
                if let Some(join) = expanded_graph.nodes.get_mut(name) {
                    join.needs = copies;
                }
                expanded = true;
            }
            if expanded {
                model.workflow.graph = Some(expanded_graph);
                continue;
            }
        }

        model.wave += 1;
        let wave = model.wave;
        let snapshot = model.outputs.clone();
        let snapshot_map = outputs_map(&snapshot);
        let mut effects = Vec::new();
        let mut dispatched = 0usize;

        for name in &ready {
            let node = &graph.nodes[name];
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
                Action::RequestApproval {
                    message,
                    timeout_secs,
                } => {
                    let deadline_ms = timeout_secs
                        .filter(|s| *s > 0)
                        .map(|s| now_ms.saturating_add(s.saturating_mul(1_000)));
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
                            deadline_ms,
                            timeout_secs: *timeout_secs,
                        },
                    );
                    effects.push(GraphEffect::AwaitApproval {
                        node: name.clone(),
                        payload,
                        deadline_ms,
                        timeout_secs: *timeout_secs,
                    });
                    dispatched += 1;
                }
                _ if is_fanout_join(&graph, name, node) => {
                    let values: Vec<serde_json::Value> = node
                        .needs
                        .iter()
                        .filter_map(|copy| model.outputs.get(copy).cloned())
                        .collect();
                    let value = collect_parallel(values.clone());
                    if node.on_error == StepOnError::FailRun && is_failure_value(&value) {
                        let kind = values
                            .iter()
                            .find(|v| is_failure_value(v))
                            .and_then(|v| v.get("kind"))
                            .and_then(|k| k.as_str())
                            .unwrap_or("")
                            .to_string();
                        let output = fail_run_value(name, &value, None);
                        let error = output
                            .get("error")
                            .and_then(|e| e.as_str())
                            .unwrap_or("parallel copy failed")
                            .to_string();
                        model.phase = GraphPhase::Failed {
                            error: error.clone(),
                        };
                        effects.push(GraphEffect::Fail {
                            fail: GraphFail {
                                node: name.clone(),
                                error,
                                disposition: if kind.is_empty() {
                                    String::new()
                                } else {
                                    disposition_for_kind(&kind, None).as_str().to_string()
                                },
                                kind,
                                leaf_preview: output
                                    .get("leaf_preview")
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null),
                                attempts: None,
                            },
                        });
                        return effects;
                    }
                    model.outputs.insert(name.clone(), value);
                    model.node_wave.insert(name.clone(), wave);
                    model.node_state.insert(name.clone(), NodeState::Settled);
                    continue;
                }
                Action::CallWorkflow { .. } => {
                    let rendered = match render_action_lenient(
                        &node.action,
                        &model.trigger,
                        &snapshot_map,
                    ) {
                        Ok(action) => action,
                        Err(error) => {
                            let value = serde_json::json!({ "ok": false, "error": format!("render: {error}") });
                            if node.on_error == StepOnError::FailRun {
                                let output = fail_run_value(name, &value, None);
                                let error = output
                                    .get("error")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("child call refused")
                                    .to_string();
                                model.phase = GraphPhase::Failed {
                                    error: error.clone(),
                                };
                                effects.push(GraphEffect::Fail {
                                    fail: GraphFail {
                                        node: name.clone(),
                                        error,
                                        kind: String::new(),
                                        disposition: String::new(),
                                        leaf_preview: output
                                            .get("leaf_preview")
                                            .cloned()
                                            .unwrap_or(serde_json::Value::Null),
                                        attempts: None,
                                    },
                                });
                                return effects;
                            }
                            model.outputs.insert(name.clone(), value);
                            model.node_wave.insert(name.clone(), wave);
                            model.node_state.insert(name.clone(), NodeState::Settled);
                            continue;
                        }
                    };
                    let Action::CallWorkflow {
                        workflow: callee,
                        inputs,
                    } = rendered
                    else {
                        continue;
                    };
                    let child_run_id = format!("{}:{}:{}", model.run_id, name, 1);
                    match durable_child_trigger(
                        &model.workflow.name,
                        &model.trigger,
                        &callee,
                        &inputs,
                        &snapshot_map,
                    ) {
                        Ok(trigger) => {
                            model.node_state.insert(
                                name.clone(),
                                NodeState::AwaitingChild {
                                    wave,
                                    attempt: 1,
                                    child_run_id: child_run_id.clone(),
                                    workflow: callee.clone(),
                                    trigger: trigger.clone(),
                                },
                            );
                            effects.push(GraphEffect::ChildRun {
                                node: name.clone(),
                                child_run_id,
                                workflow: callee,
                                trigger,
                            });
                            dispatched += 1;
                        }
                        Err(error) => {
                            let value = serde_json::json!({ "ok": false, "error": error });
                            if node.on_error == StepOnError::FailRun {
                                let output = fail_run_value(name, &value, None);
                                let error = output
                                    .get("error")
                                    .and_then(|e| e.as_str())
                                    .unwrap_or("child call refused")
                                    .to_string();
                                model.phase = GraphPhase::Failed {
                                    error: error.clone(),
                                };
                                effects.push(GraphEffect::Fail {
                                    fail: GraphFail {
                                        node: name.clone(),
                                        error,
                                        kind: String::new(),
                                        disposition: String::new(),
                                        leaf_preview: output
                                            .get("leaf_preview")
                                            .cloned()
                                            .unwrap_or(serde_json::Value::Null),
                                        attempts: None,
                                    },
                                });
                                return effects;
                            }
                            model.outputs.insert(name.clone(), value);
                            model.node_wave.insert(name.clone(), wave);
                            model.node_state.insert(name.clone(), NodeState::Settled);
                        }
                    }
                }
                Action::Reduce { .. } => {
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
                        item: model.items.get(name).cloned(),
                    });
                    dispatched += 1;
                }
                Action::Retry { .. } => {
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
                        item: None,
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
                        item: model.items.get(name).cloned(),
                    });
                    dispatched += 1;
                }
                action => {
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

/// The value of a lowered parallel join, matching `run_parallel`: the copy
/// values in authored order, or the fail-after-collect envelope when any copy
/// is a failure value (the join step's `on_error` then decides).
fn collect_parallel(values: Vec<serde_json::Value>) -> serde_json::Value {
    if let Some(failed) = values.iter().find(|v| is_failure_value(v)) {
        let error = failed
            .get("error")
            .and_then(|e| e.as_str())
            .unwrap_or("parallel copy failed");
        return serde_json::json!({
            "ok": false,
            "error": error,
            "results": values,
        });
    }
    serde_json::Value::Array(values)
}

fn outputs_map(
    outputs: &BTreeMap<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    outputs
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// True when the authored steps contain a fan-out (`parallel` or `foreach`).
pub fn has_fanout_steps(workflow: &Workflow) -> bool {
    workflow.steps.iter().any(|step| {
        matches!(
            step.action,
            Action::Parallel { .. } | Action::Foreach { .. }
        )
    })
}

/// True when the authored steps contain a `call_workflow` child call.
pub fn has_call_workflow_steps(workflow: &Workflow) -> bool {
    workflow
        .steps
        .iter()
        .any(|step| matches!(step.action, Action::CallWorkflow { .. }))
}

/// True when the authored steps contain a `retry:` policy wrapper.
pub fn has_retry_steps(workflow: &Workflow) -> bool {
    workflow
        .steps
        .iter()
        .any(|step| matches!(step.action, Action::Retry { .. }))
}

/// True when the workflow must run on the graph model: an authored graph, a
/// `steps:` fan-out, or a `steps:` retry policy
/// (bd-durable-parallel-53um, bd-durable-foreach-jpgx,
/// bd-durable-retry-policy-rrck).
pub fn needs_graph_model(workflow: &Workflow) -> bool {
    workflow.graph.is_some()
        || has_fanout_steps(workflow)
        || has_retry_steps(workflow)
        || has_call_workflow_steps(workflow)
}

/// Why a `retry:` wrapper cannot be sequenced as a per-node policy, if it
/// cannot. An unparseable policy refuses before any row rather than silently
/// degrading to the scheduler default.
fn retry_policy_refusal(inner: &Step, max_attempts: u32) -> Option<String> {
    if max_attempts == 0 {
        return Some("retry `max_attempts` must be at least 1".to_string());
    }
    if max_attempts > MAX_RETRY_ATTEMPTS {
        return Some(format!(
            "retry `max_attempts` {max_attempts} exceeds the bounded cap {MAX_RETRY_ATTEMPTS}"
        ));
    }
    if matches!(inner.action, Action::Retry { .. }) {
        return Some("nested retry wrappers are not sequenced; set the outer policy".to_string());
    }
    if matches!(inner.action, Action::CallWorkflow { .. }) {
        return Some(
            "a retry wrapper around a child run is not sequenced; put the policy inside the callee"
                .to_string(),
        );
    }
    if !is_graph_sequencable(&inner.action) {
        return Some(format!(
            "retry wraps '{}', which the graph model cannot sequence as a single node",
            inner.action.type_name()
        ));
    }
    None
}

/// The `over` form a foreach accepts: a JSON literal or a
/// `{{trigger|inputs|steps}}` reference. Anything else cannot be materialized
/// from the fold's pure inputs, so it refuses before any row.
fn foreach_over_refusal(over: &str) -> Option<String> {
    let trimmed = over.trim();
    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        return None;
    }
    let Some(inner) = trimmed
        .strip_prefix("{{")
        .and_then(|rest| rest.strip_suffix("}}"))
    else {
        return Some(format!(
            "foreach `over` {over:?} must be a JSON list or a {{trigger|inputs|steps}} reference"
        ));
    };
    let root = inner.trim().split('.').next().unwrap_or_default();
    matches!(root, "trigger" | "inputs" | "steps" | "item")
        .then_some(())
        .map_or_else(
            || {
                Some(format!(
                    "foreach `over` {over:?} must read trigger, inputs, or upstream outputs — not {root:?}"
                ))
            },
            |()| None,
        )
}

/// The `{{steps.<node>…}}` root a foreach read names, when it names one.
fn foreach_over_steps_root(over: &str) -> Option<&str> {
    let inner = over.trim().strip_prefix("{{")?.strip_suffix("}}")?.trim();
    inner.strip_prefix("steps.")?.split('.').next()
}

/// Static refusal for a `steps:` workflow that needs the fan-out lowering.
///
/// Whole-workflow, never partial: the mix refusal keeps a spawn/branch
/// sequence linear (a graph cannot admit either), every copy/item must be a
/// leaf the graph can sequence, the fan-out must not carry a whole-step guard
/// (the lowering cannot skip the fan-out as one), and a foreach iterator must
/// name an EARLIER step when it reads upstream outputs.
fn lowered_shape_refusal(workflow: &Workflow) -> Option<String> {
    for (index, step) in workflow.steps.iter().enumerate() {
        let name = step_name(step, index);
        match &step.action {
            Action::Retry {
                max_attempts,
                backoff_secs: _,
                step: inner,
            } => {
                if let Some(reason) = retry_policy_refusal(inner, *max_attempts) {
                    return Some(format!("step '{name}': {reason}"));
                }
            }
            Action::Parallel { over, each } => {
                if step.guard.is_some() {
                    return Some(format!(
                        "step '{name}' is a fan-out with a step guard; the durable lowering cannot skip the whole fan-out yet — move the guard into the copies or split the workflow"
                    ));
                }
                let literal = over.trim().parse::<u64>().is_ok();
                let template = over.trim().starts_with("{{") && over.trim().ends_with("}}");
                if !literal && !template {
                    return Some(format!(
                        "step '{name}' parallel `over` {over:?} must be a positive count or a template ref"
                    ));
                }
                if !is_graph_sequencable(&each.action) {
                    return Some(format!(
                        "step '{name}' parallel copy uses '{}', which the graph model cannot sequence",
                        each.action.type_name()
                    ));
                }
            }
            Action::Foreach { over, each } => {
                if step.guard.is_some() {
                    return Some(format!(
                        "step '{name}' is a fan-out with a step guard; the durable lowering cannot skip the whole fan-out yet — move the guard into the items or split the workflow"
                    ));
                }
                if let Some(reason) = foreach_over_refusal(over) {
                    return Some(format!("step '{name}': {reason}"));
                }
                if let Some(upstream) = foreach_over_steps_root(over) {
                    let earlier = (0..index).any(|i| step_name(&workflow.steps[i], i) == upstream);
                    if !earlier {
                        return Some(format!(
                            "step '{name}' foreach `over` {over:?} names '{upstream}', which is not an earlier step"
                        ));
                    }
                }
                if !is_graph_sequencable(&each.action) {
                    return Some(format!(
                        "step '{name}' foreach item uses '{}', which the graph model cannot sequence",
                        each.action.type_name()
                    ));
                }
            }
            Action::CallWorkflow { .. } => {}
            Action::Spawn { .. } | Action::Branch { .. } => {
                return Some(format!(
                    "workflow mixes graph-lowered steps with '{}' at step '{name}': the durable graph admits neither; split the workflow",
                    step.action.type_name()
                ));
            }
            action if !is_linear(action) => {
                return Some(format!(
                    "step '{name}' uses '{}', which the graph lowering cannot sequence",
                    action.type_name()
                ));
            }
            _ => {}
        }
    }
    None
}

/// The graph this workflow runs on: an authored graph, or a `steps:` sequence
/// lowered into fan-out nodes + a collect join named after the authored step
/// (bd-durable-parallel-53um, bd-durable-foreach-jpgx).
///
/// `parallel` expands at open (its count is a literal or a trigger value).
/// `foreach` lowers to a MARKER and expands in the fold when it is ready,
/// because its list may read an upstream output that does not exist at open;
/// the fold owns the outputs map, so the expansion stays pure.
pub fn durable_graph(
    workflow: &Workflow,
    trigger: &serde_json::Value,
) -> Result<Option<Graph>, String> {
    if let Some(graph) = &workflow.graph {
        return Ok(Some(graph.clone()));
    }
    if !has_fanout_steps(workflow)
        && !has_retry_steps(workflow)
        && !has_call_workflow_steps(workflow)
    {
        return Ok(None);
    }
    if let Some(reason) = lowered_shape_refusal(workflow) {
        return Err(reason);
    }
    let mut nodes: BTreeMap<String, GraphNode> = BTreeMap::new();
    let mut foreach_names: Vec<String> = Vec::new();
    let mut prev: Option<String> = None;
    for (index, step) in workflow.steps.iter().enumerate() {
        let name = step_name(step, index);
        match &step.action {
            Action::Parallel { over, each } => {
                let count = parallel_fanout_count(over, trigger, &serde_json::Map::new())
                    .ok_or_else(|| {
                        format!(
                            "step '{name}' parallel `over` {over:?} must resolve to a positive count at open"
                        )
                    })?;
                if count > MAX_PARALLEL_FANOUT {
                    return Err(format!(
                        "step '{name}' parallel fan-out {count} exceeds the bounded cap {MAX_PARALLEL_FANOUT}"
                    ));
                }
                let copies: Vec<String> = (0..count).map(|i| format!("{name}:{i}")).collect();
                for copy in &copies {
                    let node = GraphNode {
                        action: each.action.clone(),
                        needs: prev.iter().cloned().collect(),
                        guard: each.guard.clone(),
                        on_error: each.on_error,
                        build_slot: each.build_slot,
                    };
                    if nodes.insert(copy.clone(), node).is_some() {
                        return Err(format!(
                            "parallel copy name '{copy}' collides with an authored step"
                        ));
                    }
                }
                let join = GraphNode {
                    action: step.action.clone(),
                    needs: copies,
                    guard: None,
                    on_error: step.on_error,
                    build_slot: false,
                };
                if nodes.insert(name.clone(), join).is_some() {
                    return Err(format!("step name '{name}' is not unique"));
                }
                prev = Some(name);
            }
            Action::Foreach { over, each: _ } => {
                let at_open = resolve_template_ref(over, trigger, &serde_json::Map::new());
                match (at_open, foreach_over_steps_root(over)) {
                    (Some(serde_json::Value::Array(_)), _) => {}
                    (Some(_), _) => {
                        return Err(format!(
                            "step '{name}' foreach `over` {over:?} must resolve to a list"
                        ));
                    }
                    (None, Some(_)) => {}
                    (None, None) => {
                        return Err(format!(
                            "step '{name}' foreach `over` {over:?} does not resolve at open"
                        ));
                    }
                }
                let marker = GraphNode {
                    action: step.action.clone(),
                    needs: prev.iter().cloned().collect(),
                    guard: None,
                    on_error: step.on_error,
                    build_slot: false,
                };
                if nodes.insert(name.clone(), marker).is_some() {
                    return Err(format!("step name '{name}' is not unique"));
                }
                foreach_names.push(name.clone());
                prev = Some(name);
            }
            action => {
                let node = GraphNode {
                    action: action.clone(),
                    needs: prev.iter().cloned().collect(),
                    guard: step.guard.clone(),
                    on_error: step.on_error,
                    build_slot: step.build_slot,
                };
                if nodes.insert(name.clone(), node).is_some() {
                    return Err(format!("step name '{name}' is not unique"));
                }
                prev = Some(name);
            }
        }
    }
    for fanout in &foreach_names {
        let prefix = format!("{fanout}:");
        for name in nodes.keys() {
            let collides = name
                .strip_prefix(&prefix)
                .is_some_and(|index| index.parse::<usize>().is_ok());
            if collides {
                return Err(format!(
                    "authored node '{name}' collides with a foreach copy of '{fanout}'"
                ));
            }
        }
    }
    Ok(Some(Graph { nodes }))
}

/// True when `node` is a fan-out JOIN: its `needs` are exactly its own copies.
fn is_fanout_join(graph: &Graph, name: &str, node: &GraphNode) -> bool {
    if !matches!(
        node.action,
        Action::Parallel { .. } | Action::Foreach { .. }
    ) {
        return false;
    }
    !node.needs.is_empty()
        && node.needs.iter().all(|need| {
            let copy = need
                .strip_prefix(name)
                .and_then(|rest| rest.strip_prefix(':'))
                .is_some_and(|index| index.parse::<usize>().is_ok());
            copy && graph.nodes.contains_key(need)
        })
}

/// `None` when the graph model can sequence this graph end to end.
///
/// Whole-graph refusal, never partial: a graph is admitted only when every node
/// is a HostSink leaf, `delay`, `request_approval`, or the JOIN MARKER of a
/// lowered parallel fan-out (a `parallel` node whose `needs` are its copies).
/// An empty, cyclic, or unknown-need graph is refused too — the old engine
/// fails those identically, and a durable row recording a doomed run is worse
/// than a clean route.
pub fn graph_unsupported_reason(workflow: &Workflow) -> Option<String> {
    let Some(graph) = &workflow.graph else {
        if has_fanout_steps(workflow)
            || has_retry_steps(workflow)
            || has_call_workflow_steps(workflow)
        {
            return lowered_shape_refusal(workflow);
        }
        return Some("not a graph workflow".into());
    };
    if let Err(error) = validate_graph_runtime(graph) {
        return Some(error);
    }
    for (name, node) in &graph.nodes {
        if let Action::Retry {
            max_attempts,
            step: inner,
            ..
        } = &node.action
        {
            if let Some(reason) = retry_policy_refusal(inner, *max_attempts) {
                return Some(format!("graph node '{name}': {reason}"));
            }
            continue;
        }
        if let Action::Foreach { over, each } = &node.action {
            if let Some(reason) = foreach_over_refusal(over) {
                return Some(format!("graph node '{name}': {reason}"));
            }
            if !is_graph_sequencable(&each.action) {
                return Some(format!(
                    "graph node '{name}' foreach item uses '{}', which the graph model cannot sequence",
                    each.action.type_name()
                ));
            }
            continue;
        }
        if let Action::Parallel { each, .. } = &node.action {
            if node.needs.is_empty() {
                return Some(format!(
                    "graph node '{name}' is a parallel join with no copies in `needs`"
                ));
            }
            for need in &node.needs {
                let copy_index = need
                    .strip_prefix(name.as_str())
                    .and_then(|rest| rest.strip_prefix(':'))
                    .and_then(|i| i.parse::<usize>().ok());
                if copy_index.is_none() {
                    return Some(format!(
                        "graph node '{name}' is a parallel join; need '{need}' is not one of its copies '{name}:<i>'"
                    ));
                }
            }
            if !is_graph_sequencable(&each.action) {
                return Some(format!(
                    "graph node '{name}' parallel copy uses '{}', which the graph model cannot sequence",
                    each.action.type_name()
                ));
            }
            continue;
        }
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
    match action {
        Action::Retry {
            max_attempts, step, ..
        } => retry_policy_refusal(step, *max_attempts).is_none(),
        Action::CallWorkflow { .. } => true,
        Action::Delay { .. } | Action::RequestApproval { .. } | Action::Reduce { .. } => true,
        _ => matches!(partition(action), ActionPartition::HostSink { .. }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Workflow;

    fn empty_workflow(name: &str) -> Workflow {
        Workflow {
            name: name.into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![],
            graph: None,
            inputs: vec![],
            crew: None,
        }
    }

    /// New rows carry a shape tag; bare rows written before the tag still
    /// decode. This is the upgrade contract for a restart mid-run.
    #[test]
    fn persisted_model_tag_roundtrips_and_accepts_legacy_rows() {
        let linear = RunModel::new(
            "run-linear",
            empty_workflow("linear"),
            serde_json::json!({}),
        );
        let legacy = serde_json::to_string(&linear).expect("legacy encode");
        assert!(!legacy.contains("\"model\""));
        match PersistedWorkflowModel::decode(&legacy).expect("legacy decode") {
            PersistedWorkflowModel::Linear(model) => assert_eq!(model.run_id, "run-linear"),
            PersistedWorkflowModel::Graph(_) => panic!("bare linear row decoded as a graph"),
        }
        let tagged = serde_json::to_string(&PersistedWorkflowModel::Linear(Box::new(linear)))
            .expect("tagged encode");
        assert!(tagged.contains("\"model\":\"linear\""), "{tagged}");
        match PersistedWorkflowModel::decode(&tagged).expect("tagged decode") {
            PersistedWorkflowModel::Linear(model) => assert_eq!(model.run_id, "run-linear"),
            PersistedWorkflowModel::Graph(_) => panic!("tagged linear row decoded as a graph"),
        }

        let graph = GraphModel::new("run-graph", empty_workflow("graph"), serde_json::json!({}));
        let legacy_graph = serde_json::to_string(&graph).expect("legacy graph encode");
        match PersistedWorkflowModel::decode(&legacy_graph).expect("legacy graph decode") {
            PersistedWorkflowModel::Graph(model) => assert_eq!(model.run_id, "run-graph"),
            PersistedWorkflowModel::Linear(_) => panic!("bare graph row decoded as linear"),
        }
        let tagged_graph = serde_json::to_string(&PersistedWorkflowModel::Graph(Box::new(graph)))
            .expect("tagged graph encode");
        match PersistedWorkflowModel::decode(&tagged_graph).expect("tagged graph decode") {
            PersistedWorkflowModel::Graph(model) => assert_eq!(model.run_id, "run-graph"),
            PersistedWorkflowModel::Linear(_) => panic!("tagged graph row decoded as linear"),
        }
    }

    fn empty_outputs() -> serde_json::Map<String, serde_json::Value> {
        serde_json::Map::new()
    }

    /// One graph node: `j` dispatches a job and awaits its result.
    fn await_graph_model() -> GraphModel {
        let mut wf = crate::schema::parse_workflow(
            "name: g\nsteps:\n  - name: j\n    action: dispatch_job\n    assignee: x\n    spec: { task: hi, stage: s }\n    await: result\n    no_claim_timeout: 600\n    result_timeout: 7200\n    on_error: fail_run\n",
        )
        .expect("parse dispatch await");
        wf.graph = Some(wf.to_graph());
        GraphModel::new("run-await", wf, serde_json::json!({}))
    }

    /// bd-jhz8x.5: a `dispatch_job await: result` graph node WAITS for the
    /// job's terminal instead of settling on its publish receipt — the graph
    /// model's twin of the linear `AwaitingJob`. This is what makes a wave's
    /// stage actually depend on its job.
    #[test]
    fn graph_dispatch_await_waits_for_the_job_terminal() {
        let (model, effects) =
            advance_graph(&await_graph_model(), &GraphMsg::Start { now_ms: 1_000 });
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::ExecuteNode { node, .. } if node == "j")),
            "{effects:?}"
        );

        // The publish receipt is NOT the result: the node holds the job id and
        // both deadlines, and the run cannot schedule past it.
        let (model, effects) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "job_id": "job-1", "published": true}),
                now_ms: 1_000,
            },
        );
        assert!(
            matches!(
                model.node_state.get("j"),
                Some(NodeState::AwaitingJob { job, claimed: false, .. }) if job == "job-1"
            ),
            "{:?}",
            model.node_state
        );
        assert!(
            model.outputs.is_empty(),
            "a publish receipt is not a result: {:?}",
            model.outputs
        );
        assert_eq!(model.phase, GraphPhase::Running);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::WatchJob { node, job_id } if node == "j" && job_id == "job-1")),
            "{effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                GraphEffect::ParkUntil {
                    until_ms: 601_000,
                    reason: NodeParkReason::Job,
                    ..
                }
            )),
            "the claim bound parks the run: {effects:?}"
        );

        // A claim switches the active deadline to the result bound.
        let (model, effects) = advance_graph(
            &model,
            &GraphMsg::JobClaimed {
                node: "j".into(),
                now_ms: 2_000,
            },
        );
        assert!(
            matches!(
                model.node_state.get("j"),
                Some(NodeState::AwaitingJob {
                    claimed: true,
                    result_deadline_ms: Some(7_202_000),
                    ..
                })
            ),
            "{:?}",
            model.node_state
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::WatchJob { .. })),
            "a claim keeps watching for the terminal: {effects:?}"
        );

        // The terminal lands the job's own value and the run completes.
        let (model, effects) = advance_graph(
            &model,
            &GraphMsg::JobSettled {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "witness": {"seen": true}}),
                now_ms: 3_000,
            },
        );
        assert!(matches!(
            model.node_state.get("j"),
            Some(NodeState::Settled)
        ));
        assert_eq!(
            model.outputs.get("j").and_then(|v| v.get("witness")),
            Some(&serde_json::json!({"seen": true}))
        );
        assert_eq!(model.phase, GraphPhase::Succeeded);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::Complete { .. })),
            "{effects:?}"
        );
    }

    /// The awaited job's deadline fails TYPED, naming the node and the bound,
    /// and an unclaimed job never settles green.
    #[test]
    fn graph_dispatch_await_times_out_typed() {
        let (model, _) = advance_graph(&await_graph_model(), &GraphMsg::Start { now_ms: 1_000 });
        let (model, _) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "job_id": "job-1", "published": true}),
                now_ms: 1_000,
            },
        );

        // Under the claim bound it is still just a wait.
        let (model, effects) = advance_graph(&model, &GraphMsg::Woke { now_ms: 600_999 });
        assert!(matches!(
            model.node_state.get("j"),
            Some(NodeState::AwaitingJob { .. })
        ));
        assert!(
            effects.iter().any(|e| matches!(
                e,
                GraphEffect::ParkUntil {
                    until_ms: 601_000,
                    ..
                }
            )),
            "{effects:?}"
        );

        // At the bound: the same typed refusal the linear model writes.
        let (model, effects) = advance_graph(&model, &GraphMsg::Woke { now_ms: 601_000 });
        assert!(
            matches!(model.phase, GraphPhase::Failed { .. }),
            "{:?}",
            model.phase
        );
        let fail = effects
            .iter()
            .find_map(|e| match e {
                GraphEffect::Fail { fail } => Some(fail),
                _ => None,
            })
            .expect("a typed failure");
        assert_eq!(fail.node, "j");
        assert!(fail.error.contains("no claim"), "{}", fail.error);
        assert!(fail.error.contains("600s"), "{}", fail.error);
    }

    /// A publish receipt with no job id can never be watched: it fails loud
    /// instead of waiting out a deadline nothing will answer.
    #[test]
    fn graph_dispatch_await_without_a_job_id_fails_loud() {
        let (model, _) = advance_graph(&await_graph_model(), &GraphMsg::Start { now_ms: 1_000 });
        let (model, effects) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "published": true, "deferred": true}),
                now_ms: 1_000,
            },
        );
        assert!(
            matches!(model.phase, GraphPhase::Failed { .. }),
            "{:?}",
            model.phase
        );
        let fail = effects
            .iter()
            .find_map(|e| match e {
                GraphEffect::Fail { fail } => Some(fail),
                _ => None,
            })
            .expect("a typed failure");
        assert!(
            fail.error.contains("did not return a job id"),
            "{}",
            fail.error
        );
    }

    /// A watched job that failed honours the node's `on_error`.
    #[test]
    fn graph_dispatch_await_job_failure_goes_to_on_error() {
        let (model, _) = advance_graph(&await_graph_model(), &GraphMsg::Start { now_ms: 1_000 });
        let (model, _) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "job_id": "job-1", "published": true}),
                now_ms: 1_000,
            },
        );
        let (model, effects) = advance_graph(
            &model,
            &GraphMsg::JobFailed {
                node: "j".into(),
                error: "agent blew up".into(),
                now_ms: 2_000,
            },
        );
        assert!(matches!(model.phase, GraphPhase::Failed { .. }));
        let fail = effects
            .iter()
            .find_map(|e| match e {
                GraphEffect::Fail { fail } => Some(fail),
                _ => None,
            })
            .expect("a typed failure");
        assert!(fail.error.contains("agent blew up"), "{}", fail.error);
    }

    /// A restart re-issues the WATCH, never the publish: the receipt is the
    /// one thing a resumed run must not repeat.
    #[test]
    fn graph_resume_reissues_the_watch_not_the_publish() {
        let (model, _) = advance_graph(&await_graph_model(), &GraphMsg::Start { now_ms: 1_000 });
        let (model, _) = advance_graph(
            &model,
            &GraphMsg::NodeDone {
                node: "j".into(),
                value: serde_json::json!({"ok": true, "job_id": "job-1", "published": true}),
                now_ms: 1_000,
            },
        );
        let effects = resume_graph_effects(&model);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::WatchJob { job_id, .. } if job_id == "job-1")),
            "{effects:?}"
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, GraphEffect::ParkUntil { .. })),
            "{effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, GraphEffect::ExecuteNode { .. })),
            "a resume never republishes: {effects:?}"
        );

        // The new state survives the store's own encoding.
        let row = serde_json::to_string(&PersistedWorkflowModel::Graph(Box::new(model.clone())))
            .expect("encode");
        let PersistedWorkflowModel::Graph(back) =
            PersistedWorkflowModel::decode(&row).expect("decode")
        else {
            panic!("a graph row roundtrips as a graph");
        };
        assert_eq!(back.node_state.get("j"), model.node_state.get("j"));
    }

    #[test]
    fn loop_min_defaults_to_one_when_absent() {
        let trigger = serde_json::json!({});
        assert_eq!(resolve_loop_min(None, &trigger, &empty_outputs()), Ok(1));
    }

    #[test]
    fn loop_min_parses_literal() {
        let trigger = serde_json::json!({});
        assert_eq!(
            resolve_loop_min(Some("3"), &trigger, &empty_outputs()),
            Ok(3)
        );
        assert_eq!(
            resolve_loop_min(Some(" 2 "), &trigger, &empty_outputs()),
            Ok(2)
        );
    }

    #[test]
    fn loop_min_rejects_nonpositive_and_noninteger() {
        let trigger = serde_json::json!({});
        assert!(resolve_loop_min(Some("0"), &trigger, &empty_outputs()).is_err());
        assert!(resolve_loop_min(Some(""), &trigger, &empty_outputs()).is_err());
        assert!(resolve_loop_min(Some("abc"), &trigger, &empty_outputs()).is_err());
    }

    #[test]
    fn loop_min_rejects_over_cap() {
        let trigger = serde_json::json!({});
        let over = (MAX_LOOP_ITERS + 1).to_string();
        let err = resolve_loop_min(Some(&over), &trigger, &empty_outputs())
            .expect_err("over cap must fail");
        assert!(err.contains("bounded cap"), "{err}");
    }

    #[test]
    fn loop_min_renders_trigger_template() {
        let trigger = serde_json::json!({"min_passes": "4"});
        assert_eq!(
            resolve_loop_min(Some("{{trigger.min_passes}}"), &trigger, &empty_outputs()),
            Ok(4)
        );
    }

    fn dispatch_await_workflow(extra: &str) -> Workflow {
        crate::schema::parse_workflow(&format!(
            "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: j\n    action: dispatch_job\n    assignee: x\n    spec: {{ task: hi }}\n{extra}"
        ))
        .expect("fixture parses")
    }

    fn suspend_await_job(wf: Workflow, now_ms: u64) -> RunModel {
        let mut model = RunModel::new("run", wf, serde_json::json!({}));
        let effects = next_effects(&mut model, now_ms, &[]);
        assert!(
            matches!(&model.phase, RunPhase::AwaitingJob { node, claimed: false, .. } if node == "j"),
            "suspends: {:?}",
            model.phase
        );
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, RunEffect::ExecuteStep { .. })),
            "dispatch still publishes: {effects:?}"
        );
        model
    }

    /// `await: result` suspends with a bounded claim park, still publishing.
    #[test]
    fn dispatch_await_suspends_with_bounded_claim_park() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let mut model = RunModel::new("run", wf, serde_json::json!({}));
        let effects = next_effects(&mut model, 1_000, &[]);
        assert!(
            matches!(&model.phase, RunPhase::AwaitingJob { node, claimed: false, .. } if node == "j"),
            "suspends: {:?}",
            model.phase
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                RunEffect::ParkUntil {
                    until_ms: 601_000,
                    ..
                }
            )),
            "bounded claim park: {effects:?}"
        );
    }

    /// No claim past the deadline is a typed failure; terminal is quiet.
    #[test]
    fn dispatch_await_no_claim_times_out() {
        let wf = dispatch_await_workflow(
            "    await: result\n    result_timeout: 60\n    on_error: fail_run\n",
        );
        let model = suspend_await_job(wf, 1_000);
        let (next, effects) = advance_with(&model, &RunMsg::Woke { now_ms: 601_001 }, &[]);
        assert!(
            matches!(&next.phase, RunPhase::Failed { .. }),
            "timed out: {:?}",
            next.phase
        );
        assert_eq!(effects.len(), 1, "one Fail cmd: {effects:?}");
        let (next, effects) = advance_with(&next, &RunMsg::Woke { now_ms: 602_000 }, &[]);
        assert!(effects.is_empty(), "terminal emits nothing: {effects:?}");
        let _ = next;
    }

    /// Claimed but no 43004/43006 past `result_timeout` names step + bound.
    #[test]
    fn dispatch_await_claimed_no_result_times_out() {
        let wf = dispatch_await_workflow(
            "    await: result\n    result_timeout: 60\n    on_error: fail_run\n",
        );
        let model = suspend_await_job(wf, 1_000);
        let (next, _) = advance_with(
            &model,
            &RunMsg::JobClaimed {
                node: "j".into(),
                now_ms: 2_000,
            },
            &[],
        );
        assert!(
            matches!(&next.phase, RunPhase::AwaitingJob { claimed: true, .. }),
            "claimed: {:?}",
            next.phase
        );
        let (next, effects) = advance_with(&next, &RunMsg::Woke { now_ms: 62_001 }, &[]);
        let RunPhase::Failed { error } = &next.phase else {
            panic!("timed out: {:?}", next.phase);
        };
        assert!(error.contains('j'), "names the step: {error}");
        assert!(error.contains("result_timeout"), "names the bound: {error}");
        assert_eq!(effects.len(), 1, "one Fail cmd");
    }

    /// A 43004 with witness lands as the step output, witness included.
    #[test]
    fn dispatch_await_result_with_witness_ok() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let model = suspend_await_job(wf, 1_000);
        let value = serde_json::json!({
            "ok": true, "job": "abc",
            "witness": {"contract": "signed"}, "body": "done",
        });
        let (next, _) = advance_with(
            &model,
            &RunMsg::JobSettled {
                node: "j".into(),
                value: value.clone(),
                now_ms: 2_000,
            },
            &[],
        );
        assert_eq!(next.outputs.get("j"), Some(&value));
        assert!(
            next.outputs["j"].get("witness").is_some(),
            "witness lifted: {:?}",
            next.outputs["j"]
        );
    }

    /// A 43006 is a typed failure carrying the job's message.
    #[test]
    fn dispatch_await_error_is_typed_failure() {
        let wf = dispatch_await_workflow(
            "    await: result\n    result_timeout: 60\n    on_error: fail_run\n",
        );
        let model = suspend_await_job(wf, 1_000);
        let (next, effects) = advance_with(
            &model,
            &RunMsg::JobFailed {
                node: "j".into(),
                error: "agent blew up".into(),
                now_ms: 2_000,
            },
            &[],
        );
        let RunPhase::Failed { error } = &next.phase else {
            panic!("failed: {:?}", next.phase);
        };
        assert!(error.contains("agent blew up"), "carries message: {error}");
        assert_eq!(effects.len(), 1, "one Fail cmd");
    }

    /// A StepDone carrying the 43001 receipt records the job id and emits
    /// a WatchJob for it — the hole D4 exists to close.
    #[test]
    fn dispatch_await_step_done_records_job_and_watches() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let model = suspend_await_job(wf, 1_000);
        let (next, effects) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": true, "job_id": "abc", "published": true}),
                now_ms: 1_001,
            },
            &[],
        );
        let RunPhase::AwaitingJob { node, job, .. } = &next.phase else {
            panic!("still awaiting: {:?}", next.phase);
        };
        assert_eq!(node, "j");
        assert_eq!(job, "abc");
        assert_eq!(effects.len(), 1, "one WatchJob: {effects:?}");
        let RunEffect::WatchJob { node, job_id } = &effects[0] else {
            panic!("watches: {effects:?}");
        };
        assert_eq!(node, "j");
        assert_eq!(job_id, "abc");
    }

    /// A StepDone receipt without a job id cannot be watched: fail loud
    /// instead of waiting out a claim deadline that can never fire.
    #[test]
    fn dispatch_await_step_done_without_job_id_fails() {
        let wf = dispatch_await_workflow(
            "    await: result\n    result_timeout: 60\n    on_error: fail_run\n",
        );
        let model = suspend_await_job(wf, 1_000);
        let (next, effects) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": true, "coalesced": true}),
                now_ms: 1_001,
            },
            &[],
        );
        let RunPhase::Failed { error } = &next.phase else {
            panic!("failed: {:?}", next.phase);
        };
        assert!(error.contains("did not return a job id"), "{error}");
        assert_eq!(effects.len(), 1, "one Fail cmd");
    }

    /// A failed publication settles as a typed failure.
    #[test]
    fn dispatch_await_step_done_failure_value_fails() {
        let wf = dispatch_await_workflow(
            "    await: result\n    result_timeout: 60\n    on_error: fail_run\n",
        );
        let model = suspend_await_job(wf, 1_000);
        let (next, _) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": false, "error": "publish boom"}),
                now_ms: 1_001,
            },
            &[],
        );
        let RunPhase::Failed { error } = &next.phase else {
            panic!("failed: {:?}", next.phase);
        };
        assert!(error.contains("publish boom"), "carries it: {error}");
    }

    /// A claim after the receipt re-arms the watch for the result wait.
    #[test]
    fn dispatch_await_claimed_watches_for_result() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let model = suspend_await_job(wf, 1_000);
        let (model, _) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": true, "job_id": "abc"}),
                now_ms: 1_001,
            },
            &[],
        );
        let (next, effects) = advance_with(
            &model,
            &RunMsg::JobClaimed {
                node: "j".into(),
                now_ms: 2_000,
            },
            &[],
        );
        let watches: Vec<_> = effects
            .iter()
            .filter_map(|e| match e {
                RunEffect::WatchJob { node, job_id } => Some((node.clone(), job_id.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            watches,
            vec![("j".to_string(), "abc".to_string())],
            "re-armed for the result wait: {effects:?}"
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                RunEffect::ParkUntil {
                    until_ms: 62_000,
                    ..
                }
            )),
            "result deadline parked: {effects:?}"
        );
        let _ = next;
    }

    /// A duplicate claim after the re-watch re-arms the wait again: the
    /// fold never answers a claim with nothing, so the run cannot stall
    /// with no WatchJob and no park in flight.
    #[test]
    fn dispatch_await_duplicate_claim_still_rewatches() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let model = suspend_await_job(wf, 1_000);
        let (model, _) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": true, "job_id": "abc"}),
                now_ms: 1_001,
            },
            &[],
        );
        let (model, _) = advance_with(
            &model,
            &RunMsg::JobClaimed {
                node: "j".into(),
                now_ms: 2_000,
            },
            &[],
        );
        assert!(
            matches!(&model.phase, RunPhase::AwaitingJob { claimed: true, .. }),
            "claimed: {:?}",
            model.phase
        );
        let (_, effects) = advance_with(
            &model,
            &RunMsg::JobClaimed {
                node: "j".into(),
                now_ms: 3_000,
            },
            &[],
        );
        assert!(
            effects.iter().any(|e| matches!(
                e,
                RunEffect::WatchJob { node, job_id } if node == "j" && job_id == "abc"
            )),
            "re-watch re-armed, never nothing: {effects:?}"
        );
    }

    /// A restart mid-wait re-issues the watch and the park — never a
    /// republish (no ExecuteStep), with or without a recorded job id.
    #[test]
    fn dispatch_await_resume_watches_without_republish() {
        let wf = dispatch_await_workflow("    await: result\n    result_timeout: 60\n");
        let model = suspend_await_job(wf, 1_000);
        let effects = resume_effects(&model);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, RunEffect::ExecuteStep { .. })),
            "never republishes: {effects:?}"
        );
        let (model, _) = advance_with(
            &model,
            &RunMsg::StepDone {
                value: serde_json::json!({"ok": true, "job_id": "abc"}),
                now_ms: 1_001,
            },
            &[],
        );
        let effects = resume_effects(&model);
        assert!(
            effects.iter().any(|e| matches!(
                e,
                RunEffect::WatchJob {
                    node,
                    job_id,
                } if node == "j" && job_id == "abc"
            )),
            "watch resumes: {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, RunEffect::ExecuteStep { .. })),
            "still no republish: {effects:?}"
        );
    }

    /// No `await` dispatches exactly as today: ExecuteStep, stays Running.
    #[test]
    fn dispatch_without_await_matches_today() {
        let wf = dispatch_await_workflow("");
        let mut model = RunModel::new("run", wf, serde_json::json!({}));
        let effects = next_effects(&mut model, 1_000, &[]);
        assert!(matches!(model.phase, RunPhase::Running));
        assert!(
            matches!(effects.as_slice(), [RunEffect::ExecuteStep { .. }]),
            "today's effects exactly: {effects:?}"
        );
    }

    /// Bus-backed watch tests (bd-ge1zo rework 2): the bus replays stored
    /// history before live mail, so a watch opened after the claim must
    /// drain the replay to EOSE and settle on the stored 43004 — never
    /// return the stale 43002 and hang the run.
    mod watch_bus_tests {
        use super::super::{JobWatchOutcome, JobWatchSpec, bus_job_watcher, watch_job_once};
        use std::sync::Arc;
        use toron_core::bus::in_process::InProcessBusConfig;
        use toron_core::bus::{Bus, InProcessBus};
        use toron_core::index::schema;
        use toron_core::nostr::event::build_event;
        use toron_core::nostr::kinds::{JOB_ACCEPTED, JOB_REQUEST, JOB_RESULT, toron_kind};
        use toron_core::nostr::{Keys, Tag, Timestamp};

        async fn open_bus() -> (Arc<dyn Bus>, std::path::PathBuf) {
            let path =
                std::env::temp_dir().join(format!("toron-watch-{}.sqlite", uuid::Uuid::new_v4()));
            let pool = schema::connect(path.to_str().unwrap())
                .await
                .expect("connect");
            schema::migrate(&pool).await.expect("migrate");
            let bus = InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus");
            (Arc::new(bus) as Arc<dyn Bus>, path)
        }

        async fn publish_at(
            bus: &Arc<dyn Bus>,
            keys: &Keys,
            kind: u16,
            job_hex: &str,
            content: &str,
            created_at: Timestamp,
        ) -> String {
            let ev = build_event(
                keys,
                toron_kind(kind),
                content,
                [Tag::parse(["e", job_hex]).expect("e tag")],
                created_at,
            )
            .expect("build");
            let hex = ev.id.to_hex();
            bus.publish(ev).await.expect("publish");
            hex
        }

        /// 43001, then 43002, then 43004 — all stored before the watch opens.
        /// Staggered whole-second `created_at`s so the replay order is
        /// deterministic: the stale claim always sorts before the result.
        async fn seed_claimed_then_settled(bus: &Arc<dyn Bus>) -> String {
            let requester = Keys::generate();
            let claimer = Keys::generate();
            let base = Timestamp::now().as_secs();
            let at = |ago: u64| Timestamp::from_secs(base.saturating_sub(ago));
            let job = publish_at(
                bus,
                &requester,
                JOB_REQUEST,
                "parent",
                r#"{"task":"hi"}"#,
                at(20),
            )
            .await;
            publish_at(bus, &claimer, JOB_ACCEPTED, &job, "claimed", at(10)).await;
            let result = serde_json::json!({
                "status": "done",
                "body": "did it",
                "witness": {"contract": "signed"},
            });
            publish_at(bus, &claimer, JOB_RESULT, &job, &result.to_string(), at(0)).await;
            job
        }

        fn spec(job: &str, claimed: bool) -> JobWatchSpec {
            JobWatchSpec {
                job_id: job.into(),
                deadline_ms: None,
                claimed,
            }
        }

        async fn watch(bus: &Arc<dyn Bus>, job: &str, claimed: bool) -> JobWatchOutcome {
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                watch_job_once(bus, &spec(job, claimed)),
            )
            .await
            .expect("watch must not hang")
        }

        #[tokio::test]
        async fn watch_before_claim_settles_on_stored_result() {
            let (bus, path) = open_bus().await;
            let job = seed_claimed_then_settled(&bus).await;
            let JobWatchOutcome::Settled(value) = watch(&bus, &job, false).await else {
                panic!("unclaimed watch must settle on the stored 43004");
            };
            assert_eq!(value.get("body").and_then(|v| v.as_str()), Some("did it"));
            assert!(value.get("witness").is_some(), "witness lifted: {value}");
            let _ = std::fs::remove_file(&path);
        }

        /// The public host constructor over the same bus yields the same
        /// terminal: one construction site for every host (bd-ge1zo 2a).
        #[tokio::test]
        async fn bus_job_watcher_settles_replayed_result() {
            let (bus, path) = open_bus().await;
            let job = seed_claimed_then_settled(&bus).await;
            let watcher = bus_job_watcher(bus);
            let outcome = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                watcher(spec(&job, false)),
            )
            .await
            .expect("watch must not hang");
            let JobWatchOutcome::Settled(value) = outcome else {
                panic!("bus_job_watcher must settle on the stored 43004");
            };
            assert_eq!(value.get("body").and_then(|v| v.as_str()), Some("did it"));
            let _ = std::fs::remove_file(&path);
        }

        #[tokio::test]
        async fn rewatch_after_claim_skips_stale_claim() {
            let (bus, path) = open_bus().await;
            let job = seed_claimed_then_settled(&bus).await;
            let JobWatchOutcome::Settled(value) = watch(&bus, &job, true).await else {
                panic!("re-watch must skip the seen claim and settle");
            };
            assert_eq!(value.get("body").and_then(|v| v.as_str()), Some("did it"));
            let _ = std::fs::remove_file(&path);
        }
    }
}
