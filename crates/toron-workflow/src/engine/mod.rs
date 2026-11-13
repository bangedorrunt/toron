// governed-by: ADR-0024
// governed-by: ADR-0003, ADR-0009
#![forbid(unsafe_code)]

//! The workflow engine façade (ADR-0043).
//!
//! A run is a durable model (`DurableRun` / `DurableGraphRun`) driven by this
//! module: the engine stores the injected seams, `run`/`invoke` open the run,
//! drive its effects through the seams, and return the terminal value. Parks
//! announce an id on the approval/spawn sink and await a oneshot, so the
//! existing drainers resolve them exactly as they resolved vendor promises.
//! There is no vendor runtime and no server: a local engine keeps its runs in
//! the in-memory kernel, an embedding with a store attaches SQLite.
//!
//! **Layout:** this module is the façade + core types; [`drive`] is the
//! in-process driver, [`control_flow`] the kept render/guard/leaf helpers,
//! [`durable`]/[`durable_host`] the pure models and their host drivers.

pub mod armor;
mod boundary;
pub mod branch;
mod control_flow;
pub mod dispatch;
mod drive;
pub mod durable;
pub mod durable_host;
pub mod elm;
pub mod host;
pub mod leaf;
pub mod linear_step;
pub mod park;
mod reconcile;
mod reduce;
mod sinks;
pub mod spawn;
mod template;
use std::sync::Arc;

use leaf::LeafSeams;

use serde::{Deserialize, Serialize};

pub use boundary::{
    ActionErrorCode, ActionFailure, ActionInvocation, ActionOutcome, ActionPartition, ActionStage,
    DEPRECATED_ACTION_TYPES, Disposition, ENGINE_ACTION_TYPES, EffectSettlement, EvidenceRef,
    ExecutionPolicy, FailureKind, MAX_EVIDENCE_DETAIL, ProviderErrorClass, RunContext,
    advertised_action_names, classify_provider_error_msg, derive_effect_key,
    failure_for_control_misdispatch, failure_for_provider_error, failure_for_unsupported, invoke,
    invoke_compat, is_failure_value, normalize_dispatch_result, partition,
};
pub use control_flow::{EFFECT_JOURNAL_BYPASSED, EFFECT_JOURNAL_SINK_UNCOVERED};
pub use control_flow::{build_workflow_run_terminal, stamp_workflow_emitted};
pub use reconcile::{
    CleanupKind, CleanupRequest, DispatchPlan, EffectView, FORCED_CLEANUP_CAPABILITY,
    ProviderCapability, ProviderOutcome, ReplayMismatch, RetentionPolicy, UnknownReport,
    capability_for_action_type, check_replay, cleanup_effect_key, corr_for_effect,
    describe_unknown, effect_view, may_settle, plan_attempt,
};
pub use sinks::{
    ApprovalSink, MAX_PENDING_QUESTION, ParkKind, PendingEntry, PendingRegistry, Resolution,
    ResolutionMismatch, ResolutionVerdict, SpawnSink, TerminalLedger, TerminalNote, TerminalRecord,
    VerifyRequest, VerifyResult, VerifySink, attribute_parks, terminal_digest, validate_resolution,
};

/// An approval decision (the promise payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    /// `approved` or `rejected`.
    pub outcome: String,
    /// Optional note from the approver.
    #[serde(default)]
    pub note: String,
}

impl Decision {
    /// Approved decision.
    pub fn approved(note: impl Into<String>) -> Self {
        Self {
            outcome: "approved".into(),
            note: note.into(),
        }
    }

    /// Rejected decision.
    pub fn rejected(note: impl Into<String>) -> Self {
        Self {
            outcome: "rejected".into(),
            note: note.into(),
        }
    }

    /// True when approved.
    pub fn is_approved(&self) -> bool {
        self.outcome == "approved"
    }
}

/// The durable function's input: a workflow + the trigger payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunInput {
    /// The workflow definition (serialized; replayed on resume).
    pub workflow: crate::schema::Workflow,
    /// The trigger payload (available to `{{trigger.*}}` templates).
    pub trigger: serde_json::Value,
    /// The durable run id (for evidence-bearing approvals, Spec 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// The context-bearing effect surface (bd-zczp, ADR-0032 Spec 1): every
/// effectful invocation carries its [`ActionInvocation`] (run/node/effect
/// context + policy). The engine routes effectful HostSink actions through
/// it whenever one is injected; pure leaves compute in the boundary with no
/// runner at all.
pub type EffectRunner =
    Arc<dyn Fn(&ActionInvocation) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// The async context-bearing effect surface (bd-ckkf, ADR-0035 Spec 2.2):
/// same contract as [`EffectRunner`], but the closure returns a future the
/// engine awaits inline — no thread-hop bridge, no `block_on`. The engine
/// prefers this over [`EffectRunner`] for effectful HostSink actions
/// whenever one is injected; the sync runner stays for pure leaves,
/// journal-less embeddings, and existing flywheel constructors (untouched).
// governed-by: ADR-0035
pub type AsyncEffectRunner = Arc<
    dyn Fn(
            &ActionInvocation,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
        > + Send
        + Sync,
>;

/// A run-lifecycle phase (bd-mi9n, ADR-0032 Spec 5/6).
///
/// The engine's run row is the INDEX its effect rows hang off: `Running` and
/// its terminals mirror `workflow_runs.status` exactly, so an engine-driven
/// run is visible without the embedding's own `create_run`/`cas_run_status`
/// calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunPhase {
    /// A `workflow_runs` row exists (`workflow_runs` starts here).
    Started,
    /// The run is executing steps.
    Running,
    /// Terminal: the run completed (`workflow_runs.status = succeeded`).
    Succeeded,
    /// Terminal: the run failed or errored.
    Failed,
}

/// One run-lifecycle transition (bd-mi9n): `run_id` is the SAME id the
/// effect rows carry, so the run row and its effects agree by construction.
///
/// Strings are owned so the seam stays a plain closure alias (the
/// [`EffectRunner`] house style) instead of a trait object with a lifetime
/// parameter.
#[derive(Clone, Debug)]
pub struct RunTransition {
    pub run_id: String,
    pub workflow: String,
    pub workflow_digest: String,
    pub phase: RunPhase,
}

/// The injected run-lifecycle seam (bd-mi9n).
///
/// The engine calls it at run start (`Started`, then `Running`) and once at
/// the end (`Succeeded`/`Failed`). Sync by the same house rule as
/// [`EffectRunner`]: the embedding owns the blocking. The implementation MUST
/// be idempotent — a durable run replays, so the same phase arrives more than
/// once and a correct journal no-ops on the repeat.
///
/// Absent journal = no run rows (a journal-less embedding keeps its own
/// lifecycle calls, exactly as before).
pub type RunJournal = Arc<dyn Fn(&RunTransition) + Send + Sync>;

/// The async run-lifecycle seam (bd-ckkf, ADR-0035 Spec 2.2): same contract
/// as [`RunJournal`], awaited inline by the engine — no `block_on`. The
/// engine prefers this over [`RunJournal`] whenever one is injected.
// governed-by: ADR-0035
pub type AsyncRunJournal = Arc<
    dyn Fn(RunTransition) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// How the engine treats an effectful HostSink leaf that has no journaling
/// [`EffectRunner`] to go through (bd-engine-run-path-journaling-fwlx, the
/// ADR-0032 Spec 6 flip).
///
/// - [`EffectJournalMode::Recording`] (default): the legacy warn + tag
///   behavior. The leaf runs unjournaled through the compat runner and its
///   value carries the machine-readable bypass marker, so a journal-less
///   embedding is visible in the run ledger instead of looking journaled.
///   This is the mode for DRY arms by design: `workflow run` without
///   `--execute` and `cron run` without `--live` record instead of perform,
///   and their leaves must stay unjournaled.
/// - [`EffectJournalMode::Enforced`]: fail closed. An effectful leaf with no
///   runner is refused — the step fails loud naming the missing journal seam —
///   instead of performing an external effect nobody can reconcile. This is
///   the mode for every engine that really dispatches (toron-mcp's host,
///   flywheel's journaled constructors).
///
/// There is no mode where an unjournaled effect reads as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EffectJournalMode {
    /// Warn + tag: the leaf runs unjournaled, visibly (dry arms, tests).
    #[default]
    Recording,
    /// Fail closed: an effectful leaf with no journal seam refuses to run.
    Enforced,
}

/// The `spawn` profile resolver DI seam (ADR-0005 Spec 4a). Resolves a
/// profile name (`@housekeeper`) to the (npub, spawn spec) the embedding
/// spawns. The ENGINE never imports flywheel — this seam is injected.
/// `None` → unknown profile (fail the step).
pub type ProfileResolver = Arc<dyn Fn(&str) -> Option<serde_json::Value> + Send + Sync>;

/// ADR-0007 R4.1: resolve a workflow **name** to a full [`crate::schema::Workflow`] for
/// `call_workflow` nested runs. Injected DI (engine never imports flywheel
/// catalogs). `None` → unknown workflow → step fails with `{ok:false}`.
pub type WorkflowResolver = Arc<dyn Fn(&str) -> Option<crate::schema::Workflow> + Send + Sync>;

/// The workflow engine: an in-process driver over the durable models.
///
/// ADR-0043 replaced the vendor runtime with `OurEngine` + `DurableRun` /
/// `DurableGraphRun`. This façade keeps the public door flywheel, toron-cli and
/// the MCP host already call: constructors store the injected seams,
/// `run`/`invoke` open the durable run, drive it to terminal, and return the
/// output. Parked approvals and spawns still announce an id on the sinks, so
/// the existing drainers resolve them exactly as they resolved promises.
#[derive(Clone)]
pub struct WorkflowEngine {
    profile_resolver: ProfileResolver,
    workflow_resolver: WorkflowResolver,
    effect_runner: Option<EffectRunner>,
    async_effect_runner: Option<AsyncEffectRunner>,
    journal_mode: EffectJournalMode,
    run_journal: Option<RunJournal>,
    async_run_journal: Option<AsyncRunJournal>,
    approval_sink: ApprovalSink,
    spawn_sink: SpawnSink,
    policy_sink: crate::cron_failure::PolicySink,
    verify_sink: VerifySink,
    /// SQLite store the embedding already owns. `None` = the in-memory kernel
    /// (a local engine: process-lifetime runs, no cross-process resume).
    store: Option<toron_core::index::workflow_effects::WorkflowEffectStore>,
    /// The local engine's model seam. Shared for the engine's lifetime so a
    /// redelivered run id resumes its recorded terminal instead of starting
    /// over (steer chain redelivery contract).
    mem_state: drive::MemRunState,
    approvals: drive::Waiters,
    spawns: drive::Waiters,
    /// The spec behind each announced spawn key, so an embedding's drainer can
    /// build the launch plan without a vendor promise record
    /// (bd-engine-facade-74nw).
    spawn_specs: std::sync::Arc<
        std::sync::Mutex<std::collections::BTreeMap<String, crate::engine::spawn::SpawnSpec>>,
    >,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for WorkflowEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowEngine")
            .field("store", &self.store.is_some())
            .field("approvals", &self.approvals)
            .field("spawns", &self.spawns)
            .field("verify_sink", &self.verify_sink)
            .finish()
    }
}

impl WorkflowEngine {
    /// New engine with the default (empty) resolvers. No vendor runtime: a
    /// local engine keeps its runs in the in-memory kernel.
    pub fn local() -> anyhow::Result<Self> {
        Self::local_with(Self::default_resolver())
    }

    /// Like [`Self::local`] with a `ProfileResolver` for the `spawn` action
    /// (ADR-0005 Spec 4a DI seam).
    pub fn local_with(profile_resolver: ProfileResolver) -> anyhow::Result<Self> {
        Self::local_with_resolvers(profile_resolver, Self::default_workflow_resolver())
    }

    /// Like [`Self::local_with`] with an explicit [`WorkflowResolver`] for
    /// `call_workflow` (ADR-0007 R4.1).
    pub fn local_with_resolvers(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(profile_resolver, workflow_resolver, None)
    }

    /// Like [`Self::local_with_resolvers`] with a context-bearing
    /// [`EffectRunner`].
    pub fn local_with_effect_runner(effect_runner: Option<EffectRunner>) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            effect_runner,
        )
    }

    /// Like [`Self::local_with_effect_runner`] with an [`AsyncEffectRunner`]
    /// the driver awaits inline (bd-ckkf, ADR-0035 Spec 2.2).
    pub fn local_with_async_effect_runner(
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            None,
            async_effect_runner,
            EffectJournalMode::default(),
            None,
            None,
        ))
    }

    /// Resolvers plus the one async effect seam, Recording mode, no journal.
    pub fn local_with_resolvers_and_async_effect(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            profile_resolver,
            workflow_resolver,
            None,
            async_effect_runner,
            EffectJournalMode::default(),
            None,
            None,
        ))
    }

    /// Resolvers plus a sync effect runner.
    pub fn local_with_resolvers_and_effect(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(profile_resolver, workflow_resolver, effect_runner)
    }

    /// Resolvers, a journaling runner, AND [`EffectJournalMode::Enforced`].
    pub fn local_with_resolvers_and_effect_mode(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            profile_resolver,
            workflow_resolver,
            effect_runner,
            None,
            journal_mode,
            None,
            None,
        ))
    }

    /// [`Self::local_with_resolvers_and_effect_mode`] + the injected sync
    /// run-lifecycle journal (bd-mi9n).
    pub fn local_with_resolvers_effect_mode_and_run_journal(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            profile_resolver,
            workflow_resolver,
            effect_runner,
            None,
            journal_mode,
            run_journal,
            None,
        ))
    }

    /// The full async local constructor: resolvers, an [`AsyncEffectRunner`],
    /// an explicit [`EffectJournalMode`], and an [`AsyncRunJournal`].
    pub fn local_with_resolvers_async_effect_mode_and_journal(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            profile_resolver,
            workflow_resolver,
            None,
            async_effect_runner,
            journal_mode,
            None,
            async_run_journal,
        ))
    }

    /// The `remote_*` name is kept for call sites written against the vendor
    /// runtime. There is no server any more (ADR-0043): the URL is ignored and
    /// the engine drives the in-memory kernel, exactly like [`Self::local`].
    pub fn remote(url: impl Into<String>) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local()
    }

    /// See [`Self::remote`].
    pub fn remote_with(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with(profile_resolver)
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers(profile_resolver, workflow_resolver)
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers_and_async_effect(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers_and_async_effect(
            profile_resolver,
            workflow_resolver,
            async_effect_runner,
        )
    }

    /// See [`Self::remote`].
    pub fn remote_with_async_effect_runner(
        url: impl Into<String>,
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_async_effect_runner(async_effect_runner)
    }

    /// See [`Self::remote`].
    pub fn remote_with_effect_runner(
        url: impl Into<String>,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_effect_runner(effect_runner)
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers_and_effect(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers_and_effect(profile_resolver, workflow_resolver, effect_runner)
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers_and_effect_mode(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers_and_effect_mode(
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
        )
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers_effect_mode_and_run_journal(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers_effect_mode_and_run_journal(
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
            run_journal,
        )
    }

    /// See [`Self::remote`].
    pub fn remote_with_resolvers_async_effect_mode_and_journal(
        url: impl Into<String>,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        let _ = url.into();
        Self::local_with_resolvers_async_effect_mode_and_journal(
            profile_resolver,
            workflow_resolver,
            async_effect_runner,
            journal_mode,
            async_run_journal,
        )
    }

    fn local_with_resolvers_effect(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Ok(Self::build(
            profile_resolver,
            workflow_resolver,
            effect_runner,
            None,
            EffectJournalMode::default(),
            None,
            None,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> Self {
        Self {
            profile_resolver,
            workflow_resolver,
            effect_runner,
            async_effect_runner,
            journal_mode,
            run_journal,
            async_run_journal,
            approval_sink: ApprovalSink::default(),
            spawn_sink: SpawnSink::default(),
            policy_sink: crate::cron_failure::PolicySink::new(),
            verify_sink: VerifySink::default(),
            store: None,
            mem_state: drive::MemRunState::default(),
            approvals: drive::Waiters::default(),
            spawns: drive::Waiters::default(),
            spawn_specs: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::BTreeMap::new(),
            )),
            stop: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Attach the embedding's durable store. Without one the engine runs on
    /// the in-memory kernel (local semantics); with one, runs live in the same
    /// SQLite the host already owns and survive a restart.
    pub fn with_effect_store(
        mut self,
        store: toron_core::index::workflow_effects::WorkflowEffectStore,
    ) -> Self {
        self.store = Some(store);
        self
    }

    /// The default `ProfileResolver`: every profile is unknown.
    pub fn default_resolver() -> ProfileResolver {
        Arc::new(|_name: &str| None)
    }

    /// The default `WorkflowResolver`: no named workflows.
    pub fn default_workflow_resolver() -> WorkflowResolver {
        Arc::new(|_name: &str| None)
    }

    /// The approval side-channel: ids of parked approvals.
    pub const fn approval_sink(&self) -> &ApprovalSink {
        &self.approval_sink
    }

    /// The spawn side-channel: spawn keys of parked spawn-awaits.
    pub const fn spawn_sink(&self) -> &SpawnSink {
        &self.spawn_sink
    }

    /// The cron failure-policy side-channel.
    pub const fn policy_sink(&self) -> &crate::cron_failure::PolicySink {
        &self.policy_sink
    }

    /// The verify side-channel (ADR-0024 Spec 2).
    pub const fn verify_sink(&self) -> &VerifySink {
        &self.verify_sink
    }

    /// The spawn spec announced under `key`, for the embedding's drainer.
    /// Without a vendor promise record this is how the pane launcher gets the
    /// profile/harness/args the engine already prepared.
    pub fn spawn_spec(&self, key: &str) -> Option<crate::engine::spawn::SpawnSpec> {
        self.spawn_specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .cloned()
    }

    /// Process-scoped stop: every parked run's waiter is dropped, so an engine
    /// awaiting an approval/spawn returns an error instead of hanging. There is
    /// no runtime to shut down (ADR-0043); this is the whole surface.
    pub fn stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        self.approvals.cancel_all();
        self.spawns.cancel_all();
    }

    fn leaf_seams(&self) -> LeafSeams {
        LeafSeams {
            async_runner: self.async_effect_runner.clone(),
            sync_runner: self.effect_runner.clone(),
            verify_sink: self.verify_sink.clone(),
            journal_mode: self.journal_mode,
        }
    }

    fn drive_seams(&self) -> drive::DriveSeams {
        let me = self.clone();
        let spawn_sink = self.spawn_sink.clone();
        let spawn_waiters = self.spawns.clone();
        let specs = self.spawn_specs.clone();
        let launcher: crate::engine::spawn::SpawnLauncher = Arc::new(move |spec| {
            let sink = spawn_sink.clone();
            let waiters = spawn_waiters.clone();
            let specs = specs.clone();
            Box::pin(async move {
                let key = spec.spawn_key.clone();
                specs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(key.clone(), spec.clone());
                let rx = waiters.register(&key);
                sink.lock().await.push(key);
                rx.await
                    .map_err(|_| anyhow::anyhow!("spawn {} cancelled", spec.spawn_key))
            })
        });
        let children: drive::ChildDriver = Arc::new(move |run_id, workflow, trigger| {
            let me = me.clone();
            Box::pin(async move { me.drive_run(&run_id, workflow, trigger).await })
        });
        drive::DriveSeams {
            leaf: self.leaf_seams(),
            workflows: self.workflow_resolver.clone(),
            spawn: launcher,
            // No bus handle here: production driving goes through the host
            // runner (which watches from the mailbox bus); the drive path
            // takes an injected watcher, and tests fake it.
            job_watcher: None,
            approval_sink: self.approval_sink.clone(),
            spawn_sink: self.spawn_sink.clone(),
            spawn_specs: self.spawn_specs.clone(),
            approvals: self.approvals.clone(),
            children,
            stop: self.stop.clone(),
        }
    }

    /// Drive one workflow (linear or graph) to its terminal value.
    async fn drive_run(
        &self,
        run_id: &str,
        workflow: crate::schema::Workflow,
        trigger: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let at = drive::now_ms();
        let mut trigger = trigger;
        if let Some(inputs) = trigger
            .get("inputs")
            .and_then(serde_json::Value::as_object)
            .cloned()
        {
            let merged =
                Self::bind_input_defaults(&workflow.inputs, &serde_json::Value::Object(inputs));
            trigger["inputs"] = serde_json::Value::Object(merged);
        }
        let active = crate::identity::workflow_digest(&workflow);
        let rid = run_id.to_string();
        let seams = self.drive_seams();
        if drive::uses_graph(&workflow) {
            let child_seams = crate::engine::durable_host::ChildSeams {
                resolver: Some(self.workflow_resolver.clone()),
            };
            let spawn_seams = crate::engine::spawn::SpawnSeams {
                resolver: Some(self.profile_resolver.clone()),
                launcher: Some(seams.spawn.clone()),
            };
            let _ = (&spawn_seams, &active);
            match self.store.clone() {
                Some(store) => {
                    let (engine_store, journal, state) = store_handles(&store);
                    let engine = toron_core::kernel::OurEngine::with_journal(engine_store, journal);
                    let opened = tokio::task::spawn_blocking(move || {
                        crate::engine::durable_host::DurableGraphRun::open_with_children(
                            engine,
                            state,
                            rid,
                            workflow,
                            trigger,
                            at,
                            &child_seams,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("graph open task: {e}"))?;
                    let (run, effects) = match opened {
                        Ok(pair) => pair,
                        Err(crate::engine::durable_host::HostError::Unsupported(reason)) => {
                            return Ok(crate::engine::durable::refusal_value(&reason, "<graph>"));
                        }
                        Err(e) => return Err(anyhow::anyhow!("graph open: {e}")),
                    };
                    drive::drive_graph(run, effects, &seams).await
                }
                None => {
                    let engine = toron_core::kernel::OurEngine::new();
                    let state = self.mem_state.clone();
                    let opened = tokio::task::spawn_blocking(move || {
                        crate::engine::durable_host::DurableGraphRun::open_with_children(
                            engine,
                            state,
                            rid,
                            workflow,
                            trigger,
                            at,
                            &child_seams,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("graph open task: {e}"))?;
                    let (run, effects) = match opened {
                        Ok(pair) => pair,
                        Err(crate::engine::durable_host::HostError::Unsupported(reason)) => {
                            return Ok(crate::engine::durable::refusal_value(&reason, "<graph>"));
                        }
                        Err(e) => return Err(anyhow::anyhow!("graph open: {e}")),
                    };
                    drive::drive_graph(run, effects, &seams).await
                }
            }
        } else {
            let spawn_seams = crate::engine::spawn::SpawnSeams {
                resolver: Some(self.profile_resolver.clone()),
                launcher: Some(seams.spawn.clone()),
            };
            match self.store.clone() {
                Some(store) => {
                    let (engine_store, journal, state) = store_handles(&store);
                    let engine = toron_core::kernel::OurEngine::with_journal(engine_store, journal);
                    let opened = tokio::task::spawn_blocking(move || {
                        crate::engine::durable_host::DurableRun::open_with_seams(
                            engine,
                            state,
                            rid,
                            workflow,
                            trigger,
                            at,
                            &spawn_seams,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("run open task: {e}"))?;
                    let (run, effects) = match opened {
                        Ok(pair) => pair,
                        Err(crate::engine::durable_host::HostError::Unsupported(reason)) => {
                            return Ok(crate::engine::durable::refusal_value(
                                &reason,
                                "<workflow>",
                            ));
                        }
                        Err(e) => return Err(anyhow::anyhow!("run open: {e}")),
                    };
                    drive::drive_linear(run, effects, &seams).await
                }
                None => {
                    let engine = toron_core::kernel::OurEngine::new();
                    let state = self.mem_state.clone();
                    let opened = tokio::task::spawn_blocking(move || {
                        crate::engine::durable_host::DurableRun::open_with_seams(
                            engine,
                            state,
                            rid,
                            workflow,
                            trigger,
                            at,
                            &spawn_seams,
                        )
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("run open task: {e}"))?;
                    let (run, effects) = match opened {
                        Ok(pair) => pair,
                        Err(crate::engine::durable_host::HostError::Unsupported(reason)) => {
                            return Ok(crate::engine::durable::refusal_value(
                                &reason,
                                "<workflow>",
                            ));
                        }
                        Err(e) => return Err(anyhow::anyhow!("run open: {e}")),
                    };
                    drive::drive_linear(run, effects, &seams).await
                }
            }
        }
    }

    /// Start a workflow run, idempotent on `run_id` for a SQLite-backed
    /// engine (a persisted model resumes), in-memory otherwise.
    pub async fn run(
        &self,
        run_id: &str,
        mut input: RunInput,
    ) -> anyhow::Result<serde_json::Value> {
        input.run_id = Some(run_id.to_string());
        let workflow = input.workflow.clone();
        let workflow_name = workflow.name.clone();
        let digest = crate::identity::workflow_digest(&workflow);
        self.report(run_id, &workflow_name, &digest, RunPhase::Started)
            .await;
        self.report(run_id, &workflow_name, &digest, RunPhase::Running)
            .await;
        let outcome = self.drive_run(run_id, workflow, input.trigger).await;
        let phase = match &outcome {
            Ok(value)
                if value.get("status").and_then(serde_json::Value::as_str) == Some("failed") =>
            {
                RunPhase::Failed
            }
            Ok(_) => RunPhase::Succeeded,
            Err(_) => RunPhase::Failed,
        };
        self.report(run_id, &workflow_name, &digest, phase).await;
        outcome
    }

    async fn report(&self, run_id: &str, workflow: &str, digest: &str, phase: RunPhase) {
        let transition = RunTransition {
            run_id: run_id.to_string(),
            workflow: workflow.to_string(),
            workflow_digest: digest.to_string(),
            phase,
        };
        if let Some(journal) = &self.async_run_journal {
            journal(transition).await;
        } else if let Some(journal) = &self.run_journal {
            journal(&transition);
        }
    }

    /// Declared defaults (`workflow.inputs[].default`) bind UNDER provided
    /// values: provided wins, missing keys fall back to the declaration.
    pub fn bind_input_defaults(
        decls: &[crate::schema::InputDecl],
        provided: &serde_json::Value,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut merged = serde_json::Map::new();
        for decl in decls {
            if let Some(default) = &decl.default {
                merged.insert(decl.name.clone(), default.clone());
            }
        }
        if let Some(obj) = provided.as_object() {
            for (k, v) in obj {
                merged.insert(k.clone(), v.clone());
            }
        }
        merged
    }

    /// On-demand invoke (ADR-0005 Spec 2, P3): run a workflow with NO trigger
    /// event. The synthesized trigger carries the merged inputs both as
    /// `{{inputs.x}}` (under `inputs`) and flattened at the top level.
    pub async fn invoke(
        &self,
        run_id: &str,
        workflow: crate::schema::Workflow,
        inputs: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let merged = Self::bind_input_defaults(&workflow.inputs, &inputs);
        let mut trigger = serde_json::Map::new();
        for (k, v) in &merged {
            trigger.insert(k.clone(), v.clone());
        }
        if inputs.as_object().is_some() {
            trigger.insert("inputs".into(), serde_json::Value::Object(merged));
        } else {
            trigger.insert("inputs".into(), inputs.clone());
        }
        let input = RunInput {
            run_id: Some(run_id.to_string()),
            workflow,
            trigger: serde_json::Value::Object(trigger),
        };
        self.run(run_id, input).await
    }

    /// Resolve a parked approval (the kind-46011 listener calls this).
    pub async fn resolve_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = serde_json::to_value(decision)
            .map_err(|e| anyhow::anyhow!("serialize decision: {e}"))?;
        if self.approvals.signal(promise_id, value) {
            Ok(())
        } else {
            anyhow::bail!("no parked approval {promise_id}")
        }
    }

    /// Reject a parked approval: the run's decision value is
    /// `{approved:false, error}`.
    pub async fn reject_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = serde_json::json!({
            "approved": false,
            "outcome": decision.outcome,
            "error": decision.note,
        });
        if self.approvals.signal(promise_id, value) {
            Ok(())
        } else {
            anyhow::bail!("no parked approval {promise_id}")
        }
    }

    /// Resolve a parked spawn (the bus subscriber calls this on a job result).
    pub async fn resolve_spawn(
        &self,
        promise_id: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
        if self.spawns.signal(promise_id, value) {
            Ok(())
        } else {
            anyhow::bail!("no parked spawn {promise_id}")
        }
    }

    /// Reject a parked spawn (the bus subscriber calls this on a job error).
    pub async fn reject_spawn(&self, promise_id: &str, error: &str) -> anyhow::Result<()> {
        let value = serde_json::json!({ "ok": false, "error": error });
        if self.spawns.signal(promise_id, value) {
            Ok(())
        } else {
            anyhow::bail!("no parked spawn {promise_id}")
        }
    }
}

/// Store-backed handles over the embedding's pool. Every verb runs in
/// `spawn_blocking`, never on an async worker.
fn store_handles(
    store: &toron_core::index::workflow_effects::WorkflowEffectStore,
) -> (
    toron_core::index::durable_engine::ToronEngineStore,
    toron_core::journal::ToronJournal,
    toron_core::index::durable_workflow::SyncWorkflowState,
) {
    let pool = store.pool().clone();
    let handle = tokio::runtime::Handle::current();
    (
        toron_core::index::durable_engine::ToronEngineStore::spawn(pool.clone(), &handle),
        toron_core::journal::ToronJournal::spawn(
            toron_core::journal::JournalDb::new(pool.clone(), "engine", 64),
            &handle,
        ),
        toron_core::index::durable_workflow::SyncWorkflowState::spawn(pool, &handle),
    )
}
