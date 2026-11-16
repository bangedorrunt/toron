// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Host driver for the pure linear run model.
//!
//! [`DurableRun`] owns one [`RunModel`], the [`OurEngine`] row for its key, and
//! the [`RunState`] seam that persists the model. Every verb is one pure
//! [`advance`] plus, in this order:
//!
//! 1. persist the next model;
//! 2. drive the engine verb (`park_until`, `wake`, terminal settlement);
//! 3. hand the effects back to the embedding.
//!
//! The order is the recovery rule. A model that says `Parked` with no park row
//! is repaired by the next [`DurableRun::open`], which re-parks to the same
//! instant. A park row with no model is not repairable from the row alone, so
//! the model goes first.
//!
//! The driver performs no I/O of its own and executes no action. `ExecuteStep`
//! goes back to the embedding, which dispatches it through the existing action
//! boundary and feeds the result back with [`DurableRun::step_ok`] or
//! [`DurableRun::step_failed`]. Park, approval, completion, failure and cancel
//! are engine verbs here because the engine owns them.
//!
//! Everything is synchronous, by the same rule as [`toron_core::kernel`]: the
//! engine store and the state store answer through request/reply channels, so
//! the embedding calls a `DurableRun` from a blocking fiber or
//! `spawn_blocking`, never straight from a runtime worker.

use toron_core::index::durable_workflow::RunState;
use toron_core::kernel::{
    DurableEngine, EngineError, EngineStorage, JournalStore, OurEngine, ParkOutcome, RunEnd,
    RunPhase as EnginePhase, StartOutcome, end_run,
};

use super::control_flow::validate_workflow_lineage;
use super::durable::{
    GraphEffect, GraphModel, GraphMsg, GraphPhase, NodeState, PersistedWorkflowModel, RunEffect,
    RunModel, RunMsg, RunPhase as ModelPhase, advance_graph, advance_with, durable_graph,
    graph_unsupported_reason, needs_graph_model, next_effects, resolve_loop_max, resume_effects,
    resume_graph_effects, unsupported_reason,
};
use super::spawn::{SpawnSeams, SpawnSpec, prepare_spawn, spawn_key, spawn_launcher_refusal};

use crate::engine::WorkflowResolver;
use crate::schema::Workflow;

use crate::schema::Action;

/// How long an approval parks before the wake loop re-drives it.
///
/// An approval has no author deadline, so the engine parks it under a watchdog:
/// a run whose decision never arrives resurfaces in `drain_due` instead of
/// disappearing from the schedule. A restart re-arms it.
pub const APPROVAL_WAIT_SECS: u64 = 86_400;

/// `APPROVAL_WAIT_SECS` in milliseconds.
const APPROVAL_WAIT_MS: u64 = APPROVAL_WAIT_SECS * 1_000;

/// Typed failure of one driver verb.
#[derive(Debug)]
pub enum HostError {
    /// The engine store or journal refused the verb.
    Engine(EngineError),
    /// The persisted model could not be read or written.
    State(String),
    /// The persisted model does not parse as a [`RunModel`].
    Corrupt(String),
    /// This workflow shape stays on the old engine.
    Unsupported(String),
    /// The engine row is already terminal and no model exists to resume.
    Finished {
        /// Terminal engine phase.
        phase: EnginePhase,
    },
    /// The model is in a phase that cannot accept this transition.
    WrongPhase {
        /// The phase found instead.
        actual: String,
    },
    /// The park was longer than the engine ceiling.
    ParkRefused {
        /// Asked wait, seconds.
        asked_secs: u64,
        /// Ceiling that refused it.
        ceiling_secs: u64,
    },
}

impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Engine(e) => write!(f, "durable engine: {e}"),
            Self::State(e) => write!(f, "run state: {e}"),
            Self::Corrupt(e) => write!(f, "run state is not a run model: {e}"),
            Self::Unsupported(reason) => write!(f, "not a linear workflow: {reason}"),
            Self::Finished { phase } => write!(f, "engine run is already terminal ({phase})"),
            Self::WrongPhase { actual } => write!(f, "transition refused in phase {actual}"),
            Self::ParkRefused {
                asked_secs,
                ceiling_secs,
            } => write!(
                f,
                "park of {asked_secs}s exceeds the engine ceiling of {ceiling_secs}s"
            ),
        }
    }
}

impl std::error::Error for HostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(e) => Some(e),
            _ => None,
        }
    }
}

impl From<EngineError> for HostError {
    fn from(e: EngineError) -> Self {
        Self::Engine(e)
    }
}

/// One driven run: model + engine row + state seam.
pub struct DurableRun<S: EngineStorage, J: JournalStore, R: RunState> {
    model: RunModel,
    engine: OurEngine<S, J>,
    state: R,
    /// One entry per workflow step: the prepared spawn spec, when the step is
    /// an `Action::Spawn`. Built by [`Self::prepare_spawns`]; empty means no
    /// resolver was injected, and a spawn step lands the old unknown-profile
    /// value.
    spawns: Vec<Option<SpawnSpec>>,
}

impl<S: EngineStorage, J: JournalStore, R: RunState> DurableRun<S, J, R> {
    /// Open, resume, or start one run.
    ///
    /// A persisted model wins: the run resumes from it and the workflow and
    /// trigger arguments are ignored. Otherwise the shape is checked first — a
    /// graph or control-flow workflow returns [`HostError::Unsupported`] before
    /// any engine or state row exists, so the host can keep it on the old
    /// engine without cleaning up.
    ///
    /// # Errors
    ///
    /// Returns [`HostError`] when the shape is unsupported, the store fails, or
    /// the engine refuses the start.
    pub fn open(
        engine: OurEngine<S, J>,
        state: R,
        run_id: impl Into<String>,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
        now_ms: u64,
    ) -> Result<(Self, Vec<RunEffect>), HostError> {
        Self::open_with_seams(
            engine,
            state,
            run_id,
            workflow,
            trigger,
            now_ms,
            &SpawnSeams::default(),
        )
    }

    /// [`Self::open`] with the spawn seams the workflow's `spawn` steps need.
    ///
    /// The specs are prepared BEFORE the first transition, because a run whose
    /// first step spawns must park on a spec that already exists (the
    /// model-first rule).
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_seams(
        mut engine: OurEngine<S, J>,
        state: R,
        run_id: impl Into<String>,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
        now_ms: u64,
        spawn_seams: &SpawnSeams,
    ) -> Result<(Self, Vec<RunEffect>), HostError> {
        let run_id = run_id.into();
        if let Some(json) = state.load(&run_id).map_err(state_error)? {
            let PersistedWorkflowModel::Linear(model) = PersistedWorkflowModel::decode(&json)
                .map_err(|e| HostError::Corrupt(format!("{}: {e}", run_id)))?
            else {
                return Err(HostError::Corrupt(format!(
                    "{}: persisted graph model opened by the linear driver",
                    run_id
                )));
            };
            let mut model = *model;
            model.recompile_plan();
            let mut run = Self {
                model,
                engine,
                state,
                spawns: Vec::new(),
            };
            // Resume must rebuild the spawn specs from the injected seams;
            // an empty vector made every post-park Action::Spawn fail with
            // the unknown-profile value (bd-engine-spawn-resume).
            run.prepare_spawns(spawn_seams);
            return run.resume(now_ms).map(|effects| (run, effects));
        }

        if needs_graph_model(&workflow) {
            return Err(HostError::Unsupported("not a linear workflow".into()));
        }
        if let Some(reason) = unsupported_reason(&workflow) {
            return Err(HostError::Unsupported(reason));
        }
        if let Some(reason) = spawn_launcher_refusal(&workflow)
            && spawn_seams.launcher.is_none()
        {
            return Err(HostError::Unsupported(reason));
        }
        for step in &workflow.steps {
            if let crate::schema::Action::Loop { max, .. } = &step.action {
                resolve_loop_max(max, &trigger, &serde_json::Map::new())
                    .map_err(HostError::Unsupported)?;
            }
        }

        match engine.start(&run_id, now_ms)? {
            StartOutcome::Started { .. } => {}
            StartOutcome::Already { status } if status.phase.is_terminal() => {
                return Err(HostError::Finished {
                    phase: status.phase,
                });
            }
            StartOutcome::Already { status } if status.phase == EnginePhase::Parked => {
                engine.wake(&run_id, now_ms)?;
            }
            StartOutcome::Already { .. } => {}
        }

        let mut run = Self {
            model: RunModel::new(run_id, workflow, trigger),
            engine,
            state,
            spawns: Vec::new(),
        };
        run.prepare_spawns(spawn_seams);
        let model = run.model.clone();
        persist_linear(&run.state, &model.run_id, &model)?;
        let effects = run.apply(RunMsg::Start { now_ms }, now_ms)?;
        Ok((run, effects))
    }

    /// The current model.
    pub fn model(&self) -> &RunModel {
        &self.model
    }

    /// The engine, for host verbs the driver does not own (`drain_due`,
    /// `query`).
    pub fn engine(&self) -> &OurEngine<S, J> {
        &self.engine
    }

    /// Mutable engine, for the wake drain.
    pub fn engine_mut(&mut self) -> &mut OurEngine<S, J> {
        &mut self.engine
    }

    /// Prepare the spawn spec for every `spawn` step of this workflow.
    ///
    /// Profile resolution is the one impure part of a spec, so it happens here
    /// (the host boundary) and the fold receives data. A step whose profile the
    /// resolver does not know keeps `None` and lands the old unknown-profile
    /// value, never a run failure.
    pub fn prepare_spawns(&mut self, seams: &SpawnSeams) {
        self.spawns = (0..self.model.plan.entries.len())
            .map(|index| {
                let (authored, step) = self.model.plan.step_at(index)?;
                let Action::Spawn {
                    profile,
                    harness,
                    workflow,
                    await_,
                    no_claim_timeout,
                    context,
                } = &step.action
                else {
                    return None;
                };
                prepare_spawn(
                    seams,
                    super::spawn::SpawnInput {
                        run_id: self.model.run_id.clone(),
                        node: step
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("step_{}", authored + 1)),
                        attempt: 1,
                        profile: profile.clone(),
                        harness: harness.clone(),
                        workflow_ref: workflow.clone(),
                        await_: *await_,
                        no_claim_timeout: *no_claim_timeout,
                        context: *context,
                        project: None,
                        generation: 1,
                    },
                )
                .ok()
            })
            .collect();
    }

    /// The prepared spec for one step, when it is a spawn.
    pub fn spawn_spec(&self, index: usize) -> Option<&SpawnSpec> {
        self.spawns.get(index).and_then(Option::as_ref)
    }

    /// The current spawn-await node, when the run is parked on one.
    pub fn awaiting_spawn(&self) -> Option<(&str, &SpawnSpec)> {
        match &self.model.phase {
            ModelPhase::AwaitingSpawn { node, spec, .. } => Some((node.as_str(), spec)),
            _ => None,
        }
    }

    /// A spawned job settled.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a spawn.
    pub fn spawn_settled(
        &mut self,
        node: &str,
        value: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_awaiting_spawn(node, "spawn_settled")?;
        self.apply(
            RunMsg::SpawnSettled {
                node: node.to_string(),
                value,
                now_ms,
            },
            now_ms,
        )
    }

    /// A spawned job failed.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a spawn.
    pub fn spawn_failed(
        &mut self,
        node: &str,
        error: impl Into<String>,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_awaiting_spawn(node, "spawn_failed")?;
        self.apply(
            RunMsg::SpawnFailed {
                node: node.to_string(),
                error: error.into(),
                now_ms,
            },
            now_ms,
        )
    }

    /// The correlation key of the parked spawn, for host bookkeeping.
    pub fn awaiting_spawn_key(&self) -> Option<String> {
        self.awaiting_spawn()
            .map(|(_, spec)| spawn_key(&spec.run_id, &spec.node, 1))
    }

    /// The current dispatch-await node, when the run is parked on one.
    pub fn awaiting_job(&self) -> Option<&str> {
        match &self.model.phase {
            ModelPhase::AwaitingJob { node, .. } => Some(node.as_str()),
            _ => None,
        }
    }

    /// A dispatched job was claimed (43002).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_claimed(&mut self, node: &str, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        self.require_awaiting_job(node, "job_claimed")?;
        self.apply(
            RunMsg::JobClaimed {
                node: node.to_string(),
                now_ms,
            },
            now_ms,
        )
    }

    /// A dispatched job settled (43004 with the step output value).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_settled(
        &mut self,
        node: &str,
        value: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_awaiting_job(node, "job_settled")?;
        self.apply(
            RunMsg::JobSettled {
                node: node.to_string(),
                value,
                now_ms,
            },
            now_ms,
        )
    }

    /// A dispatched job failed (43006).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_failed(
        &mut self,
        node: &str,
        error: impl Into<String>,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_awaiting_job(node, "job_failed")?;
        self.apply(
            RunMsg::JobFailed {
                node: node.to_string(),
                error: error.into(),
                now_ms,
            },
            now_ms,
        )
    }

    fn require_awaiting_job(&self, node: &str, verb: &str) -> Result<(), HostError> {
        if matches!(&self.model.phase, ModelPhase::AwaitingJob { node: awaiting, .. } if awaiting == node)
        {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    fn require_awaiting_spawn(&self, node: &str, verb: &str) -> Result<(), HostError> {
        if matches!(&self.model.phase, ModelPhase::AwaitingSpawn { node: awaiting, .. } if awaiting == node)
        {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    /// The current step finished.
    ///
    /// One verb, one value: `on_error: continue` lands it whole (kind,
    /// disposition, leaf_preview intact); `on_error: fail_run` emits the
    /// enriched `{status:failed,…}` bundle for the host to settle from.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model is running.
    pub fn step_done(
        &mut self,
        value: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_running("step_done")?;
        self.apply(RunMsg::StepDone { value, now_ms }, now_ms)
    }

    /// A parked delay or park woke.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model is parked.
    pub fn woke(&mut self, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        if !matches!(
            self.model.phase,
            ModelPhase::Parked { .. }
                | ModelPhase::AwaitingSpawn { .. }
                | ModelPhase::AwaitingJob { .. }
                | ModelPhase::AwaitingApproval { .. }
        ) {
            return Err(self.wrong_phase("woke"));
        }
        self.apply(RunMsg::Woke { now_ms }, now_ms)
    }

    /// An approval decision arrived.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model awaits an approval.
    pub fn approve(
        &mut self,
        decision: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        if !matches!(self.model.phase, ModelPhase::AwaitingApproval { .. }) {
            return Err(self.wrong_phase("approve"));
        }
        self.apply(RunMsg::Approval { decision, now_ms }, now_ms)
    }

    /// Terminal cancel.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] when the run is already terminal.
    pub fn cancel(&mut self, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        if self.model.is_terminal() {
            return Err(self.wrong_phase("cancel"));
        }
        self.apply(RunMsg::Cancel, now_ms)
    }

    /// Re-drive a restored run: re-park what the model says is parked, wake it
    /// when due, then re-issue the effects the model still owes.
    fn resume(&mut self, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        match self.model.phase.clone() {
            ModelPhase::Parked { until_ms, .. } => {
                self.park(until_ms, 0, now_ms)?;
                if until_ms <= now_ms {
                    return self.apply(RunMsg::Woke { now_ms }, now_ms);
                }
            }
            ModelPhase::AwaitingSpawn {
                deadline_ms: Some(deadline),
                ..
            } => {
                self.park(deadline, 0, now_ms)?;
                if deadline <= now_ms {
                    return self.apply(RunMsg::Woke { now_ms }, now_ms);
                }
            }
            ModelPhase::AwaitingSpawn {
                deadline_ms: None, ..
            } => {}
            ModelPhase::AwaitingApproval {
                deadline_ms,
                timeout_secs,
                ..
            } => {
                let (until, secs) = match deadline_ms {
                    Some(deadline) => (deadline, timeout_secs.unwrap_or(APPROVAL_WAIT_SECS)),
                    None => (now_ms.saturating_add(APPROVAL_WAIT_MS), APPROVAL_WAIT_SECS),
                };
                self.park(until, secs, now_ms)?;
                if until <= now_ms {
                    return self.apply(RunMsg::Woke { now_ms }, now_ms);
                }
            }
            ModelPhase::Running if self.model.plan.step_at(self.model.step_index).is_none() => {
                let mut next = self.model.clone();
                let effects = next_effects(&mut next, now_ms, &self.spawns);
                self.persist(&next)?;
                self.model = next;
                return Ok(effects);
            }
            _ => {}
        }
        Ok(resume_effects(&self.model))
    }

    /// One pure transition: persist, then drive the engine.
    ///
    /// A park whose deadline is already past resumes in the same step. Without
    /// this, `delay: 0` (or any park rendered behind `now`) would persist a
    /// `Parked` model that the engine never schedules, and nothing would ever
    /// wake it.
    fn apply(&mut self, msg: RunMsg, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        let (next, effects) = advance_with(&self.model, &msg, &self.spawns);
        self.persist(&next)?;
        self.model = next;
        for effect in &effects {
            self.engine_verb(effect, now_ms)?;
        }
        let spawn_due = match self.model.phase {
            ModelPhase::AwaitingSpawn {
                deadline_ms: Some(deadline),
                ..
            } => deadline <= now_ms,
            _ => false,
        };
        if let ModelPhase::Parked { until_ms, .. } = self.model.phase
            && until_ms <= now_ms
        {
            let mut all = effects;
            all.extend(self.apply(RunMsg::Woke { now_ms }, now_ms)?);
            return Ok(all);
        }
        if spawn_due {
            let mut all = effects;
            all.extend(self.apply(RunMsg::Woke { now_ms }, now_ms)?);
            return Ok(all);
        }
        Ok(effects)
    }

    fn engine_verb(&mut self, effect: &RunEffect, now_ms: u64) -> Result<(), HostError> {
        let key = self.model.run_id.clone();
        match effect {
            RunEffect::ExecuteStep { .. }
            | RunEffect::Unsupported { .. }
            | RunEffect::StartSpawn { .. }
            | RunEffect::SpawnDetached { .. }
            | RunEffect::WatchJob { .. } => Ok(()),
            RunEffect::ParkUntil { until_ms, .. } => self.park(*until_ms, 0, now_ms),
            RunEffect::AwaitApproval {
                deadline_ms,
                timeout_secs,
                ..
            } => {
                let (until, secs) = match deadline_ms {
                    Some(deadline) => (*deadline, timeout_secs.unwrap_or(APPROVAL_WAIT_SECS)),
                    None => (now_ms.saturating_add(APPROVAL_WAIT_MS), APPROVAL_WAIT_SECS),
                };
                self.park(until, secs, now_ms)
            }
            RunEffect::Complete { .. } => {
                end_run(&mut self.engine, &key, RunEnd::Succeeded).map_err(HostError::from)
            }
            RunEffect::Fail { .. } => {
                end_run(&mut self.engine, &key, RunEnd::Failed).map_err(HostError::from)
            }
            RunEffect::Cancelled => self.engine.cancel(&key, now_ms).map_err(HostError::from),
        }
    }

    fn park(&mut self, until_ms: u64, max_wait_secs: u64, now_ms: u64) -> Result<(), HostError> {
        park_engine(
            &mut self.engine,
            &self.model.run_id,
            until_ms,
            max_wait_secs,
            now_ms,
        )
    }

    fn persist(&self, model: &RunModel) -> Result<(), HostError> {
        persist_linear(&self.state, &model.run_id, model)
    }

    fn require_running(&self, verb: &str) -> Result<(), HostError> {
        // AwaitingJob also accepts step completions: the StepDone receipt
        // carries the 43001 job id the watch needs (bd-ge1zo).
        if matches!(
            self.model.phase,
            ModelPhase::Running | ModelPhase::AwaitingJob { .. }
        ) {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    fn wrong_phase(&self, verb: &str) -> HostError {
        HostError::WrongPhase {
            actual: format!("{} during {verb}", phase_name(&self.model.phase)),
        }
    }
}

fn phase_name(phase: &ModelPhase) -> &'static str {
    match phase {
        ModelPhase::Ready => "ready",
        ModelPhase::Running => "running",
        ModelPhase::Parked { .. } => "parked",
        ModelPhase::AwaitingSpawn { .. } => "awaiting_spawn",
        ModelPhase::AwaitingJob { .. } => "awaiting_job",
        ModelPhase::AwaitingApproval { .. } => "awaiting_approval",
        ModelPhase::Succeeded => "succeeded",
        ModelPhase::Failed { .. } => "failed",
        ModelPhase::Cancelled => "cancelled",
    }
}

fn state_error(e: anyhow::Error) -> HostError {
    HostError::State(format!("{e:#}"))
}

/// The capabilities a `call_workflow` graph needs (bd-durable-call-workflow-mg6y).
///
/// The fold names the callee; only the host can resolve a name to a definition
/// and prove the call graph acyclic. Without a resolver the parent refuses
/// BEFORE any row, so it falls back to the old engine instead of parking on a
/// child nobody can open.
#[derive(Clone, Default)]
pub struct ChildSeams {
    /// Workflow name -> definition.
    pub resolver: Option<WorkflowResolver>,
}

/// One driven graph run: model + engine row + state seam.
///
/// Same recovery rule as [`DurableRun`], and the two share it through
/// [`persist_run_state`] and [`park_engine`]: persist the model first, then
/// drive the engine verb. A graph row and a linear row never share a run id, so
/// the two drivers never race for one engine key.
pub struct DurableGraphRun<S: EngineStorage, J: JournalStore, R: RunState> {
    model: GraphModel,
    engine: OurEngine<S, J>,
    state: R,
}

/// Static `call_workflow` targets of one workflow (top-level steps or graph
/// nodes). A templated name is dynamic and resolves only in the fold at
/// schedule time, so it is not validated here.
fn static_child_calls(workflow: &Workflow) -> Vec<String> {
    fn push(out: &mut Vec<String>, action: &crate::schema::Action) {
        if let crate::schema::Action::CallWorkflow { workflow: name, .. } = action
            && !name.contains("{{")
        {
            out.push(name.clone());
        }
    }
    let mut out = Vec::new();
    for step in &workflow.steps {
        push(&mut out, &step.action);
    }
    if let Some(graph) = &workflow.graph {
        for node in graph.nodes.values() {
            push(&mut out, &node.action);
        }
    }
    out
}

/// Validate the STATIC child graph before any row: every callee must resolve,
/// no call chain may repeat a name, and every callee must be a shape the
/// durable models can sequence (bd-durable-call-workflow-mg6y).
fn validate_children(workflow: &Workflow, seams: &ChildSeams) -> Result<(), String> {
    let calls = static_child_calls(workflow);
    if calls.is_empty() {
        return Ok(());
    }
    let Some(resolver) = &seams.resolver else {
        return Err(
            "call_workflow needs a workflow resolver on this host (no child run can be opened)"
                .to_string(),
        );
    };
    let mut chain = vec![workflow.name.clone()];
    validate_child_calls(&calls, resolver, &mut chain)
}

fn validate_child_calls(
    calls: &[String],
    resolver: &WorkflowResolver,
    chain: &mut Vec<String>,
) -> Result<(), String> {
    if chain.len() >= super::control_flow::MAX_WORKFLOW_DEPTH {
        return Err(format!(
            "workflow nesting depth {} reaches the bound {}",
            chain.len(),
            super::control_flow::MAX_WORKFLOW_DEPTH
        ));
    }
    for callee in calls {
        if chain.iter().any(|name| name == callee) {
            return Err(format!(
                "recursive workflow call refused: call chain {chain:?} -> {callee:?}"
            ));
        }
        let Some(child) = resolver(callee) else {
            return Err(format!("unknown workflow: {callee}"));
        };
        if let Some(reason) = unsupported_reason(&child) {
            return Err(format!(
                "callee {callee:?} cannot run on the durable models: {reason}"
            ));
        }
        chain.push(callee.clone());
        let nested = static_child_calls(&child);
        if !nested.is_empty() {
            validate_child_calls(&nested, resolver, chain)?;
        }
        chain.pop();
    }
    Ok(())
}

impl<S: EngineStorage, J: JournalStore, R: RunState> DurableGraphRun<S, J, R> {
    /// Open, resume, or start one graph run.
    ///
    /// A persisted model wins: the run resumes from it and the workflow and
    /// trigger arguments are ignored. Otherwise the shape and the lineage are
    /// checked first — a refused graph returns [`HostError::Unsupported`] before
    /// any engine or state row exists.
    ///
    /// # Errors
    ///
    /// Returns [`HostError`] when the shape is unsupported, the store fails, or
    /// the engine refuses the start.
    pub fn open(
        engine: OurEngine<S, J>,
        state: R,
        run_id: impl Into<String>,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
        now_ms: u64,
    ) -> Result<(Self, Vec<GraphEffect>), HostError> {
        Self::open_with_children(
            engine,
            state,
            run_id,
            workflow,
            trigger,
            now_ms,
            &ChildSeams::default(),
        )
    }

    /// [`Self::open`] with the workflow resolver a `call_workflow` graph needs.
    ///
    /// Static callees are resolved and walked transitively here, so a recursive,
    /// unknown, or unsequencable callee refuses the parent before any row.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_children(
        mut engine: OurEngine<S, J>,
        state: R,
        run_id: impl Into<String>,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
        now_ms: u64,
        children: &ChildSeams,
    ) -> Result<(Self, Vec<GraphEffect>), HostError> {
        let run_id = run_id.into();
        if let Some(json) = state.load(&run_id).map_err(state_error)? {
            let PersistedWorkflowModel::Graph(model) = PersistedWorkflowModel::decode(&json)
                .map_err(|e| HostError::Corrupt(format!("{}: {e}", run_id)))?
            else {
                return Err(HostError::Corrupt(format!(
                    "{}: persisted linear model opened by the graph driver",
                    run_id
                )));
            };
            let mut run = Self {
                model: *model,
                engine,
                state,
            };
            return run.resume(now_ms).map(|effects| (run, effects));
        }

        validate_workflow_lineage(&trigger, &workflow.name).map_err(HostError::Unsupported)?;
        let workflow = match durable_graph(&workflow, &trigger) {
            Ok(Some(graph)) => {
                let mut lowered = workflow;
                lowered.graph = Some(graph);
                lowered
            }
            Ok(None) => return Err(HostError::Unsupported("not a graph workflow".into())),
            Err(reason) => return Err(HostError::Unsupported(reason)),
        };
        if let Some(reason) = graph_unsupported_reason(&workflow) {
            return Err(HostError::Unsupported(reason));
        }
        validate_children(&workflow, children).map_err(HostError::Unsupported)?;

        match engine.start(&run_id, now_ms)? {
            StartOutcome::Started { .. } => {}
            StartOutcome::Already { status } if status.phase.is_terminal() => {
                return Err(HostError::Finished {
                    phase: status.phase,
                });
            }
            StartOutcome::Already { status } if status.phase == EnginePhase::Parked => {
                engine.wake(&run_id, now_ms)?;
            }
            StartOutcome::Already { .. } => {}
        }

        let mut run = Self {
            model: GraphModel::new(run_id, workflow, trigger),
            engine,
            state,
        };
        let model = run.model.clone();
        persist_graph(&run.state, &model.run_id, &model)?;
        let effects = run.apply(GraphMsg::Start { now_ms }, now_ms)?;
        Ok((run, effects))
    }

    /// The current model.
    pub const fn model(&self) -> &GraphModel {
        &self.model
    }

    /// The engine, for host verbs the driver does not own.
    pub const fn engine(&self) -> &OurEngine<S, J> {
        &self.engine
    }

    /// Mutable engine, for the wake drain.
    pub const fn engine_mut(&mut self) -> &mut OurEngine<S, J> {
        &mut self.engine
    }

    /// One node finished.
    ///
    /// One verb, one value: the fold applies `is_failure_value` + `on_error`,
    /// so a `continue` failure lands whole and a `fail_run` refusal produces
    /// the enriched bundle.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is executing.
    pub fn node_done(
        &mut self,
        node: &str,
        value: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        self.require_executing(node, "node_done")?;
        self.apply(
            GraphMsg::NodeDone {
                node: node.to_string(),
                value,
                now_ms,
            },
            now_ms,
        )
    }

    /// A dispatched job was claimed (43002) for `node`.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_claimed(&mut self, node: &str, now_ms: u64) -> Result<Vec<GraphEffect>, HostError> {
        self.require_awaiting_job(node, "job_claimed")?;
        self.apply(
            GraphMsg::JobClaimed {
                node: node.to_string(),
                now_ms,
            },
            now_ms,
        )
    }

    /// A dispatched job settled (43004 with the step output value).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_settled(
        &mut self,
        node: &str,
        value: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        self.require_awaiting_job(node, "job_settled")?;
        self.apply(
            GraphMsg::JobSettled {
                node: node.to_string(),
                value,
                now_ms,
            },
            now_ms,
        )
    }

    /// A dispatched job failed (43006, or a watched job that died).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node is awaiting a job.
    pub fn job_failed(
        &mut self,
        node: &str,
        error: impl Into<String>,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        self.require_awaiting_job(node, "job_failed")?;
        self.apply(
            GraphMsg::JobFailed {
                node: node.to_string(),
                error: error.into(),
                now_ms,
            },
            now_ms,
        )
    }

    /// A `call_workflow` child finished successfully
    /// (bd-durable-call-workflow-mg6y).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node awaits a child.
    pub fn child_succeeded(
        &mut self,
        node: &str,
        output: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        self.require_awaiting_child(node, "child_succeeded")?;
        self.apply(
            GraphMsg::ChildSucceeded {
                node: node.to_string(),
                output,
                now_ms,
            },
            now_ms,
        )
    }

    /// A `call_workflow` child reached a failure value (the full bundle).
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node awaits a child.
    pub fn child_failed(
        &mut self,
        node: &str,
        output: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        self.require_awaiting_child(node, "child_failed")?;
        self.apply(
            GraphMsg::ChildFailed {
                node: node.to_string(),
                output,
                now_ms,
            },
            now_ms,
        )
    }

    fn require_awaiting_child(&self, node: &str, verb: &str) -> Result<(), HostError> {
        if matches!(
            self.model.node_state.get(node),
            Some(NodeState::AwaitingChild { .. })
        ) {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    /// A park (delay or retry backoff) is due.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the run is running.
    pub fn woke(&mut self, now_ms: u64) -> Result<Vec<GraphEffect>, HostError> {
        if self.model.phase != GraphPhase::Running {
            return Err(self.wrong_phase("woke"));
        }
        self.apply(GraphMsg::Woke { now_ms }, now_ms)
    }

    /// An operator decision arrived for one node.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless that node awaits an approval.
    pub fn approve(
        &mut self,
        node: &str,
        decision: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<GraphEffect>, HostError> {
        if !matches!(
            self.model.node_state.get(node),
            Some(NodeState::AwaitingApproval { .. })
        ) {
            return Err(self.wrong_phase("approve"));
        }
        self.apply(
            GraphMsg::Approval {
                node: node.to_string(),
                decision,
                now_ms,
            },
            now_ms,
        )
    }

    /// Terminal cancel.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] when the run is already terminal.
    pub fn cancel(&mut self, now_ms: u64) -> Result<Vec<GraphEffect>, HostError> {
        if self.model.is_terminal() {
            return Err(self.wrong_phase("cancel"));
        }
        self.apply(GraphMsg::Cancel, now_ms)
    }

    /// Re-drive a restored run: re-park what the model says is parked (earliest
    /// deadline wins — the engine holds one wake per run), re-arm the approval
    /// watchdog, then re-issue the effects the model still owes.
    fn resume(&mut self, now_ms: u64) -> Result<Vec<GraphEffect>, HostError> {
        let effects = resume_graph_effects(&self.model);
        let Some((until_ms, max_wait_secs)) = park_deadline(&self.model, now_ms) else {
            return Ok(effects);
        };
        self.park(until_ms, max_wait_secs, now_ms)?;
        if until_ms <= now_ms {
            return self.apply(GraphMsg::Woke { now_ms }, now_ms);
        }
        Ok(effects)
    }

    /// One pure transition: persist, then drive the engine.
    fn apply(&mut self, msg: GraphMsg, now_ms: u64) -> Result<Vec<GraphEffect>, HostError> {
        let (next, effects) = advance_graph(&self.model, &msg);
        persist_graph(&self.state, &next.run_id, &next)?;
        self.model = next;
        self.engine_verb(&effects, now_ms)?;
        self.settle_past_park(now_ms, effects)
    }

    /// A park whose deadline is already past wakes in the same call: a
    /// `delay: 0` (or a backoff of zero) would otherwise persist a `Parked`
    /// node the engine never schedules, and nothing would ever wake it.
    fn settle_past_park(
        &mut self,
        now_ms: u64,
        effects: Vec<GraphEffect>,
    ) -> Result<Vec<GraphEffect>, HostError> {
        let due = self.model.node_state.values().any(
            |state| matches!(state, NodeState::Parked { until_ms, .. } if *until_ms <= now_ms),
        );
        if !due {
            return Ok(effects);
        }
        let mut all = effects;
        all.extend(self.apply(GraphMsg::Woke { now_ms }, now_ms)?);
        Ok(all)
    }

    fn engine_verb(&mut self, effects: &[GraphEffect], now_ms: u64) -> Result<(), HostError> {
        let key = self.model.run_id.clone();
        if let Some((until_ms, max_wait_secs)) = park_deadline(&self.model, now_ms) {
            self.park(until_ms, max_wait_secs, now_ms)?;
        }
        for effect in effects {
            match effect {
                GraphEffect::ExecuteNode { .. }
                | GraphEffect::Unsupported { .. }
                | GraphEffect::WatchJob { .. }
                | GraphEffect::ParkUntil { .. }
                | GraphEffect::AwaitApproval { .. }
                | GraphEffect::ChildRun { .. }
                | GraphEffect::ChildJoin { .. } => {}
                GraphEffect::Complete { .. } => {
                    end_run(&mut self.engine, &key, RunEnd::Succeeded).map_err(HostError::from)?;
                }
                GraphEffect::Fail { .. } => {
                    end_run(&mut self.engine, &key, RunEnd::Failed).map_err(HostError::from)?;
                }
                GraphEffect::Cancelled => {
                    self.engine.cancel(&key, now_ms).map_err(HostError::from)?;
                }
            }
        }
        Ok(())
    }

    fn park(&mut self, until_ms: u64, max_wait_secs: u64, now_ms: u64) -> Result<(), HostError> {
        let key = self.model.run_id.clone();
        park_engine(&mut self.engine, &key, until_ms, max_wait_secs, now_ms)
    }

    fn require_executing(&self, node: &str, verb: &str) -> Result<(), HostError> {
        if matches!(
            self.model.node_state.get(node),
            Some(NodeState::Executing { .. })
        ) {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    fn require_awaiting_job(&self, node: &str, verb: &str) -> Result<(), HostError> {
        if matches!(
            self.model.node_state.get(node),
            Some(NodeState::AwaitingJob { .. })
        ) {
            return Ok(());
        }
        Err(self.wrong_phase(verb))
    }

    fn wrong_phase(&self, verb: &str) -> HostError {
        HostError::WrongPhase {
            actual: format!("{} during {verb}", graph_phase_name(&self.model.phase)),
        }
    }
}

const fn graph_phase_name(phase: &GraphPhase) -> &'static str {
    match phase {
        GraphPhase::Ready => "ready",
        GraphPhase::Running => "running",
        GraphPhase::Succeeded => "succeeded",
        GraphPhase::Failed { .. } => "failed",
        GraphPhase::Cancelled => "cancelled",
    }
}

/// The one persist step both drivers use: model first, engine second.
fn persist_model<R: RunState>(
    state: &R,
    run_id: &str,
    model: &PersistedWorkflowModel,
) -> Result<(), HostError> {
    let json = serde_json::to_string(model)
        .map_err(|e| HostError::Corrupt(format!("encode {run_id}: {e}")))?;
    state.save(run_id, &json).map_err(state_error)
}

fn persist_linear<R: RunState>(state: &R, run_id: &str, model: &RunModel) -> Result<(), HostError> {
    persist_model(
        state,
        run_id,
        &PersistedWorkflowModel::Linear(Box::new(model.clone())),
    )
}

fn persist_graph<R: RunState>(
    state: &R,
    run_id: &str,
    model: &GraphModel,
) -> Result<(), HostError> {
    persist_model(
        state,
        run_id,
        &PersistedWorkflowModel::Graph(Box::new(model.clone())),
    )
}

/// The single wake this model owes the engine: the EARLIEST deadline across
/// every in-flight parked or awaiting node, with that node's own ceiling.
///
/// One park per run is an engine invariant (`park_until` overwrites), so the
/// deadline is derived from the model's state rather than from how many park
/// effects a transition happened to emit.
fn park_deadline(model: &GraphModel, now_ms: u64) -> Option<(u64, u64)> {
    model
        .node_state
        .values()
        .filter_map(|state| match state {
            NodeState::Parked { until_ms, .. } => Some((*until_ms, 0)),
            NodeState::AwaitingApproval {
                deadline_ms,
                timeout_secs,
                ..
            } => match deadline_ms {
                Some(deadline) => Some((*deadline, timeout_secs.unwrap_or(APPROVAL_WAIT_SECS))),
                None => Some((now_ms.saturating_add(APPROVAL_WAIT_MS), APPROVAL_WAIT_SECS)),
            },
            _ => None,
        })
        .min_by_key(|(until_ms, _)| *until_ms)
}

/// The one park verb both drivers use.
fn park_engine<S: EngineStorage, J: JournalStore>(
    engine: &mut OurEngine<S, J>,
    key: &str,
    until_ms: u64,
    max_wait_secs: u64,
    now_ms: u64,
) -> Result<(), HostError> {
    match engine.park_until(key, until_ms, max_wait_secs, now_ms, None)? {
        ParkOutcome::Wait { .. } | ParkOutcome::ResumeNow { .. } => Ok(()),
        ParkOutcome::RefusedOverCeiling {
            asked_secs,
            ceiling_secs,
        } => Err(HostError::ParkRefused {
            asked_secs,
            ceiling_secs,
        }),
    }
}
