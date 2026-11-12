// governed-by: ADR-0040, ADR-0035
//! The comms kernel port: the ONE module in toron that names
//! `elm_effect`, `elm_effect_elm` or `elm_effect_journal` (ADR-0040).
//!
//! Everything else in toron imports the Elm loop, the effect journal and the
//! durable-log trait from here, so an upstream API move changes this file
//! and the thin `Program` adapters, never the protocol code. The fence is
//! `tests/kernel_fence.rs`: any other file under `crates/` that names an
//! elm-effect crate, and any other crate manifest that depends on one,
//! fails the build.
//!
//! What is re-exported is the Elm + journal + durable-engine layer
//! (ADR-0040 §3, ADR-0005):
//! - the loop: [`Program`], [`Update`], [`Cmd`], [`Durability`],
//!   [`Failure`], [`Interpret`], [`Loop`], [`Producer`], [`Subscriber`],
//!   [`Boot`], replay [`Player`] and [`Snapshot`];
//! - the journal: [`EffectState`], [`can_transition`], [`JournalStore`]
//!   (with its [`JournalRow`], [`JournalRead`], [`JournalDisposition`],
//!   [`JournalError`]), [`plan_attempt`], [`plan_park`], [`may_settle`];
//! - the durable engine: [`DurableEngine`], [`OurEngine`], [`EngineStorage`],
//!   [`MemStorage`], [`RunRow`], [`ParkedRow`], [`RunPhase`], [`StartOutcome`],
//!   [`ParkOutcome`], [`EngineMsg`], [`StorageError`], [`CronSchedule`],
//!   [`CronError`], [`fire_key`], [`due_wakes`], and [`end_run`] with its
//!   [`RunEnd`];
//! - the L16 log: [`DurableLog`], [`Offset`], [`AppendError`] (with
//!   `StaleEpoch`), and [`durable_log_laws`] so toron's own log runs the
//!   upstream trait laws;
//! - from the carrier, only what an `Interpret` impl and a host need to
//!   name: [`Effect`], [`Exit`], [`Cause`], [`ManagedRuntime`]. Carrier
//!   combinators (race, timeout, schedule, stm, ...) are deliberately NOT
//!   re-exported: protocol code stays pure `std + serde` (ADR-0040 §4).
//!
//! `Loop` is `!Send`: host one per dedicated OS thread and pump inputs
//! through a channel into `Loop::send`. A `DurableLog` over toron's async
//! sqlx store answers through a request/reply channel; it never calls
//! `block_on` inside a Tokio context (the F2 nested-runtime panic).

pub use elm_effect::{
    BreakerConfig, BuiltServices, Cause, ClockImpl, Defect, Duration, Effect, Executor, Exit,
    Layer, ManagedRuntime, Remote, RemoteError, RemotePolicy, Runtime, ServiceTag, SleepOutcome,
    SystemClock, TestClock, TokioExecutor, use_service,
};
pub use elm_effect_durable::{
    AwaitOutcome, BackoffPolicy, CancelReport, ClaimOutcome, ContinueOutcome, CronError, CronFire,
    CronPolicy, CronSchedule, DurableEngine, EngineError, EngineMsg, EngineStorage, GuardedStart,
    HeartbeatOutcome, JoinOutcome, MemStorage, OurEngine, ParkOutcome, ParkedRow,
    ParseRunPhaseError, RetryOutcome, RetryPlan, RunPhase, RunRow, RunStatus, SettleOutcome,
    SignalOutcome, SignalRow, StartOptions, StartOutcome, StorageError, WakeIntent, WakeOutcome,
    cancel_tree_laws, child_key, digest_storage_laws, due_wakes, engine_laws, fire_key, guard_laws,
    heartbeat_laws, lineage_laws, plan_cancel_children, plan_cron_tick, plan_join, plan_retry,
    retry_laws, rollover_key, signal_laws, signal_storage_laws, step_key, storage_laws,
};
pub use elm_effect_elm::replay::{Player, Snapshot};
pub use elm_effect_elm::{
    Boot, Cmd, Durability, Failure, Interpret, InterpretError, Loop, Producer, Program, Subscriber,
    Update,
};
pub use elm_effect_journal::durable_log::{
    AppendError, DurableLog, FileLog, MemLog, Offset, Page, SCHEMA_VERSION, Status,
};
pub use elm_effect_journal::laws as durable_log_laws;
pub use elm_effect_journal::laws::journal_store_all;
pub use elm_effect_journal::plan::{
    ClaimRejection, DispatchPlan, EffectView, HeartbeatRejection, ParkDecision, ProviderCapability,
    ReplayMismatch, SettleRejection, check_claim, check_heartbeat, check_replay, check_settle,
    may_claim, may_settle, plan_attempt, plan_park,
};
pub use elm_effect_journal::saga::{
    SagaPlan, SagaStep, SagaStepStatus, compensate_key, plan_saga_rollback,
};
pub use elm_effect_journal::store::{
    Disposition as JournalDisposition, InMemoryStore, JournalError, JournalStore,
    Read as JournalRead, Row as JournalRow,
};
pub use elm_effect_journal::{EffectState, TransitionError, can_transition, transition};
pub use elm_effect_xstate::*;

/// Terminal settlement for a run whose lifecycle the host owns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunEnd {
    /// The run finished its work.
    Succeeded,
    /// The run failed.
    Failed,
}

/// Settle one run's journal row and project the terminal phase onto its run
/// row.
///
/// The host calls this once per terminal transition. A stale generation is a
/// typed refusal, never a silent success: the caller's model and the engine's
/// fence disagree, and retrying with the same generation cannot fix that.
///
/// # Errors
///
/// Returns [`EngineError`] when the store or journal refuses the settlement.
pub fn end_run<S: EngineStorage, J: JournalStore>(
    engine: &mut OurEngine<S, J>,
    key: &str,
    end: RunEnd,
) -> Result<(), EngineError> {
    let status = engine.query(key)?;
    let to = match end {
        RunEnd::Succeeded => EffectState::Settled,
        RunEnd::Failed => EffectState::Failed,
    };
    match engine.settle_checked(key, status.generation, to)? {
        SettleOutcome::Settled(_) => Ok(()),
        SettleOutcome::Rejected(r) => Err(EngineError::Journal(JournalError::Rejected(format!(
            "stale generation settling {key} as {end:?}: {r}"
        )))),
    }
}
