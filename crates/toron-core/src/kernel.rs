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
//!   [`CronError`], [`fire_key`], [`due_wakes`];
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
    CronError, CronSchedule, DurableEngine, EngineError, EngineMsg, EngineStorage, MemStorage,
    OurEngine, ParkOutcome, ParkedRow, RunPhase, RunRow, StartOutcome, StorageError, WakeIntent,
    due_wakes, fire_key,
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
pub use elm_effect_journal::plan::{
    DispatchPlan, EffectView, ParkDecision, ProviderCapability, may_claim, may_settle,
    plan_attempt, plan_park,
};
pub use elm_effect_journal::store::{
    Disposition as JournalDisposition, InMemoryStore, JournalError, JournalStore,
    Read as JournalRead, Row as JournalRow,
};
pub use elm_effect_journal::{EffectState, TransitionError, can_transition, transition};
