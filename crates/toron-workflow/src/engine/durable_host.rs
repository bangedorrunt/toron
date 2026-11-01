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

use super::durable::{
    RunEffect, RunModel, RunMsg, RunPhase as ModelPhase, advance, resume_effects,
    unsupported_reason,
};

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
        mut engine: OurEngine<S, J>,
        state: R,
        run_id: impl Into<String>,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
        now_ms: u64,
    ) -> Result<(Self, Vec<RunEffect>), HostError> {
        let run_id = run_id.into();
        if let Some(json) = state.load(&run_id).map_err(state_error)? {
            let model: RunModel = serde_json::from_str(&json)
                .map_err(|e| HostError::Corrupt(format!("{}: {e}", run_id)))?;
            let mut run = Self {
                model,
                engine,
                state,
            };
            return run.resume(now_ms).map(|effects| (run, effects));
        }

        if let Some(reason) = unsupported_reason(&workflow) {
            return Err(HostError::Unsupported(reason));
        }

        match engine.start(&run_id, now_ms)? {
            StartOutcome::Started { .. } => {}
            StartOutcome::Already { status } if status.phase.is_terminal() => {
                return Err(HostError::Finished {
                    phase: status.phase,
                });
            }
            // A parked row without a model is a run whose state was lost; its
            // wake is free, so take it and re-drive from the start.
            StartOutcome::Already { status } if status.phase == EnginePhase::Parked => {
                engine.wake(&run_id, now_ms)?;
            }
            StartOutcome::Already { .. } => {}
        }

        let mut run = Self {
            model: RunModel::new(run_id, workflow, trigger),
            engine,
            state,
        };
        let model = run.model.clone();
        run.persist(&model)?;
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

    /// The current step succeeded.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model is running.
    pub fn step_ok(
        &mut self,
        output: serde_json::Value,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_running("step_ok")?;
        self.apply(RunMsg::StepSucceeded { output, now_ms }, now_ms)
    }

    /// The current step failed.
    ///
    /// `on_error: continue` records the failure and advances; `on_error:
    /// fail_run` ends the run.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model is running.
    pub fn step_failed(
        &mut self,
        error: impl Into<String>,
        now_ms: u64,
    ) -> Result<Vec<RunEffect>, HostError> {
        self.require_running("step_failed")?;
        self.apply(
            RunMsg::StepFailed {
                error: error.into(),
                now_ms,
            },
            now_ms,
        )
    }

    /// A parked delay or park woke.
    ///
    /// # Errors
    ///
    /// [`HostError::WrongPhase`] unless the model is parked.
    pub fn woke(&mut self, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        if !matches!(self.model.phase, ModelPhase::Parked { .. }) {
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
                // Same instant, same park generation: a restart re-parks
                // idempotently instead of extending the wait.
                self.park(until_ms, 0, now_ms)?;
                if until_ms <= now_ms {
                    return self.apply(RunMsg::Woke { now_ms }, now_ms);
                }
            }
            ModelPhase::AwaitingApproval { .. } => {
                self.park(
                    now_ms.saturating_add(APPROVAL_WAIT_MS),
                    APPROVAL_WAIT_SECS,
                    now_ms,
                )?;
            }
            _ => {}
        }
        Ok(resume_effects(&self.model))
    }

    /// One pure transition: persist, then drive the engine.
    fn apply(&mut self, msg: RunMsg, now_ms: u64) -> Result<Vec<RunEffect>, HostError> {
        let (next, effects) = advance(&self.model, &msg);
        self.persist(&next)?;
        self.model = next;
        for effect in &effects {
            self.engine_verb(effect, now_ms)?;
        }
        Ok(effects)
    }

    fn engine_verb(&mut self, effect: &RunEffect, now_ms: u64) -> Result<(), HostError> {
        let key = self.model.run_id.clone();
        match effect {
            RunEffect::ExecuteStep { .. } | RunEffect::Unsupported { .. } => Ok(()),
            RunEffect::ParkUntil { until_ms, .. } => self.park(*until_ms, 0, now_ms),
            RunEffect::AwaitApproval { .. } => self.park(
                now_ms.saturating_add(APPROVAL_WAIT_MS),
                APPROVAL_WAIT_SECS,
                now_ms,
            ),
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
        let key = self.model.run_id.clone();
        match self
            .engine
            .park_until(&key, until_ms, max_wait_secs, now_ms, None)?
        {
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

    fn persist(&self, model: &RunModel) -> Result<(), HostError> {
        let json = serde_json::to_string(model)
            .map_err(|e| HostError::Corrupt(format!("encode {}: {e}", model.run_id)))?;
        self.state.save(&model.run_id, &json).map_err(state_error)
    }

    fn require_running(&self, verb: &str) -> Result<(), HostError> {
        if matches!(self.model.phase, ModelPhase::Running) {
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
        ModelPhase::AwaitingApproval { .. } => "awaiting_approval",
        ModelPhase::Succeeded => "succeeded",
        ModelPhase::Failed { .. } => "failed",
        ModelPhase::Cancelled => "cancelled",
    }
}

fn state_error(e: anyhow::Error) -> HostError {
    HostError::State(format!("{e:#}"))
}
