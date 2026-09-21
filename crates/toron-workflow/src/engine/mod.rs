// governed-by: ADR-0024
// governed-by: ADR-0003, ADR-0009
#![forbid(unsafe_code)]

//! Resonate-backed workflow engine (bd-oge) — the Buzz WF-08 fix.
//!
//! The decisive property: a run that hits `request_approval` SUSPENDS, survives
//! a toron process crash, and RESUMES when the approval event arrives — NOT
//! marked failed (Buzz marks it failed; that is the bug toron fixes).
//!
//! Each run is a Resonate durable function. Durable state (execution + promise
//! records) lives in the Resonate runtime (`Resonate::local()` in-process, or a
//! real server via `ResonateConfig::url` for cross-process crash survival).
//!
//! **Approval binding (promise_id ↔ kind-46011):**
//! 1. The run hits `request_approval` → creates a Resonate promise, publishes
//!    kind-46011 `approval-pending` carrying the promise_id.
//! 2. The operator signs kind-46011 approval/rejection (echoing the
//!    promise_id) — the toron bus subscriber resolves it via
//!    `resonate.promises.resolve/reject`.
//! 3. The parked durable function unblocks → continues → kind-46012.
//!
//! **Consistency rule:** workflow ACTIONS call the same `Mailbox` methods the
//! MCP/CLI tools call — never raw bus writes. The action runner is injected as
//! a Resonate dependency; the workflow function reads it via `ctx.get_dependency`.
//!
//! **Layout (F21.ENGINE-SPLIT, bd-p4-refactor-has5.1):** this module is the
//! engine facade + core types + dispatch impl; the durable control-flow
//! functions live in [`control_flow`], template interpolation in [`template`],
//! the DI sink newtypes in [`sinks`], and the test suite in `tests.rs`
//! (move-only split of the former single-file engine — no behavior change).

mod boundary;
mod control_flow;
mod reconcile;
mod sinks;
mod template;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use resonate_sdk::prelude::{Resonate, ResonateConfig};
use resonate_sdk::types::Value;

use crate::schema::Action;

// ---------------------------------------------------------------------------
// Resonate server selection: remote Tailscale default, local fallback.
// ---------------------------------------------------------------------------

/// Default (primary) Resonate server: the Tailscale node running the durable
/// promise store (`resonate serve` behind tailscale-serve).
pub const REMOTE_RESONATE_URL: &str = "https://resonate.alpine-tilapia.ts.net";
/// Local fallback server (the launchd `com.flywheel.resonate` service).
pub const LOCAL_RESONATE_URL: &str = "http://localhost:8001";
/// Explicit pin env — beats the probe dance entirely.
pub const RESONATE_URL_ENV: &str = "RESONATE_URL";

/// Resolve the Resonate server URL once per process: `RESONATE_URL` pin >
/// remote default (TCP-probed) > local fallback (TCP-probed) > `None`
/// (embedded local mode). The probe budget is 2s per candidate and the result
/// is cached — daemon startup pays it once.
pub fn resonate_server_url() -> Option<String> {
    static CACHE: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    CACHE
        .get_or_init(|| resonate_server_url_with(probe_candidate))
        .clone()
}

/// Pure resolution core (testable with a fake probe).
pub fn resonate_server_url_with(probe: impl Fn(&str) -> bool) -> Option<String> {
    if let Ok(pin) = std::env::var(RESONATE_URL_ENV)
        && !pin.trim().is_empty()
    {
        return Some(pin);
    }
    if probe(REMOTE_RESONATE_URL) {
        return Some(REMOTE_RESONATE_URL.to_string());
    }
    if probe(LOCAL_RESONATE_URL) {
        return Some(LOCAL_RESONATE_URL.to_string());
    }
    None
}

/// TCP liveness probe for a resonate server URL (2s budget). Parses
/// `https?://host[:port]`; https implies 443, http implies 80.
pub fn probe_candidate(url: &str) -> bool {
    let Some((host, port)) = host_port(url) else {
        return false;
    };
    use std::net::{TcpStream, ToSocketAddrs};
    let Some(addr) = (host.as_str(), port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut it| it.next())
    else {
        return false;
    };
    TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(2000)).is_ok()
}

/// Parse `https?://host[:port]` into `(host, port)` (pure).
pub fn host_port(url: &str) -> Option<(String, u16)> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let (host, port) = match rest.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>().ok()?),
        None => (rest.to_string(), 0),
    };
    if host.is_empty() {
        return None;
    }
    let port = if port != 0 {
        port
    } else if url.starts_with("https://") {
        443
    } else {
        80
    };
    Some((host, port))
}

// Re-export the split-out surface exactly as the former single file exposed it
// (lib.rs + toron-mcp/cli/e2e import via `engine::*` — zero external edits).
pub use boundary::{
    ActionErrorCode, ActionFailure, ActionInvocation, ActionOutcome, ActionPartition, ActionStage,
    DEPRECATED_ACTION_TYPES, Disposition, ENGINE_ACTION_TYPES, EffectSettlement, EffectState,
    EvidenceRef, ExecutionPolicy, FailureKind, MAX_EVIDENCE_DETAIL, ProviderErrorClass, RunContext,
    advertised_action_names, classify_provider_error_msg, derive_effect_key,
    failure_for_provider_error, invoke, invoke_compat, is_failure_value, partition,
};
pub use control_flow::{
    GraphNodeInput, ParallelCopyInput, build_workflow_run_terminal, run_graph_node,
    run_parallel_copy, run_workflow, stamp_workflow_emitted,
};
// bd-engine-run-path-journaling-fwlx: the effect-journal bypass markers are
// part of the contract an embedding reports against — an injected
// `EffectRunner` that does not cover a host sink must carry the SAME marker the
// engine's own fall-through uses, or the ledger grows two vocabularies for one
// gap.
pub use control_flow::{EFFECT_JOURNAL_BYPASSED, EFFECT_JOURNAL_SINK_UNCOVERED};
pub use reconcile::{
    CleanupKind, CleanupRequest, DispatchPlan, EffectView, FORCED_CLEANUP_CAPABILITY,
    ProviderCapability, ProviderOutcome, ReplayMismatch, RetentionPolicy, UnknownReport,
    capability_for_action_type, check_replay, cleanup_effect_key, corr_for_effect,
    describe_unknown, may_settle, plan_attempt,
};
pub use sinks::{
    ApprovalSink, MAX_PENDING_QUESTION, ParkKind, PendingEntry, PendingRegistry, Resolution,
    ResolutionMismatch, ResolutionVerdict, SpawnSink, TerminalLedger, TerminalNote, TerminalRecord,
    VerifyRequest, VerifyResult, VerifySink, attribute_parks, terminal_digest, validate_resolution,
};
// Crate-internal: the split modules' private helpers, re-exported so the
// tests.rs suite (moved verbatim, `use super::*`) keeps resolving them.
// Some names are exercised only indirectly — the allow documents that the
// block exists to serve the verbatim test suite, not the prod surface.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use control_flow::{
    rendered_approval_message, resolve_template_ref, run_branch, run_call_workflow, run_foreach,
    run_graph, run_parallel, run_retry, run_spawn, run_step, run_steps,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use template::{
    RenderContext, descend, render_action, render_action_lenient, render_scalar, resolve_steps,
    resolve_template,
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
    /// The Resonate run id (for evidence-bearing approvals, Spec 3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// The shared action-execution surface (the Mailbox). Run by the workflow
/// function via a Resonate dependency (consistency rule).
///
/// Every action returns a [`serde_json::Value`] (ADR-0005 Spec 0): the value
/// flows into `steps.<name>` for downstream templates/guards. `Null` is a
/// valid "no output" action.
pub type ActionRunner = Arc<dyn Fn(&Action) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// The context-bearing effect surface (bd-zczp, ADR-0032 Spec 1): every
/// effectful invocation carries its [`ActionInvocation`] (run/node/effect
/// context + policy). The engine prefers this over [`ActionRunner`] for
/// effectful HostSink actions whenever one is injected; the legacy
/// runner stays for pure actions, journal-less embeddings, and tests.
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
/// spawns. The ENGINE never imports flywheel — this seam is injected, same
/// idiom as `ActionRunner`. `None` → unknown profile (fail the step).
pub type ProfileResolver = Arc<dyn Fn(&str) -> Option<serde_json::Value> + Send + Sync>;

/// ADR-0007 R4.1: resolve a workflow **name** to a full [`crate::schema::Workflow`] for
/// `call_workflow` nested runs. Injected DI (engine never imports flywheel
/// catalogs). `None` → unknown workflow → step fails with `{ok:false}`.
pub type WorkflowResolver = Arc<dyn Fn(&str) -> Option<crate::schema::Workflow> + Send + Sync>;

/// The workflow engine: owns the Resonate runtime + registers the durable
/// function + the approval resolution bridge.
pub struct WorkflowEngine {
    resonate: Arc<Resonate>,
    approval_sink: ApprovalSink,
    spawn_sink: SpawnSink,
    policy_sink: crate::cron_failure::PolicySink,
    verify_sink: VerifySink,
}

impl std::fmt::Debug for WorkflowEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowEngine")
            .field("resonate", &"<runtime>")
            .field(
                "approval_sink_len",
                &self.approval_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .field(
                "spawn_sink_len",
                &self.spawn_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .field(
                "policy_sink_len",
                &self.policy_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .field("verify_sink", &self.verify_sink)
            .finish()
    }
}

impl WorkflowEngine {
    /// New engine over an in-process Resonate runtime (`Resonate::local`),
    /// with the shared action runner + an approval sink injected.
    pub fn local(action_runner: ActionRunner) -> anyhow::Result<Self> {
        Self::local_with(action_runner, Self::default_resolver())
    }

    /// Like [`Self::local`] with a `ProfileResolver` for the `spawn` action
    /// (ADR-0005 Spec 4a DI seam).
    pub fn local_with(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers(
            action_runner,
            profile_resolver,
            Self::default_workflow_resolver(),
        )
    }

    /// Like [`Self::local_with`] with an explicit [`WorkflowResolver`] for
    /// `call_workflow` (ADR-0007 R4.1).
    pub fn local_with_resolvers(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(action_runner, profile_resolver, workflow_resolver, None)
    }

    /// Like [`Self::local_with_resolvers`] with a context-bearing
    /// [`EffectRunner`] (bd-zczp): the engine prefers it over the legacy
    /// runner for mailbox/job HostSink actions. `None` preserves the
    /// legacy compat path.
    pub fn local_with_effect_runner(
        action_runner: ActionRunner,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(
            action_runner,
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            effect_runner,
        )
    }

    /// Like [`Self::local_with_effect_runner`] with an [`AsyncEffectRunner`]
    /// (bd-ckkf, ADR-0035 Spec 2.2): the engine awaits it inline for
    /// effectful HostSink leaves — no thread-hop bridge, no `block_on`.
    /// `None` preserves the legacy compat path.
    pub fn local_with_async_effect_runner(
        action_runner: ActionRunner,
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_async_effect(
            action_runner,
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            async_effect_runner,
        )
    }

    /// Like [`Self::local_with_resolvers`] **and** [`Self::local_with_effect_runner`]
    /// at once (bd-engine-run-path-journaling-fwlx): an embedding that has both
    /// resolvers and a journaling runner previously had to drop one — the
    /// combined funnel was private, so `spawn`/`call_workflow` resolution or the
    /// effect journal was lost. `None` preserves the legacy compat path.
    pub fn local_with_resolvers_and_effect(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
        )
    }

    /// [`Self::local`] with an explicit [`EffectJournalMode`]
    /// (bd-engine-run-path-journaling-fwlx, the Spec 6 flip): `Enforced` makes
    /// an effectful HostSink leaf with no [`EffectRunner`] FAIL the step
    /// (naming the missing journal seam) instead of running unjournaled.
    /// `Recording` keeps the warn + tag behavior for dry arms.
    ///
    /// Test-only: a real embedding injects a runner, so it uses
    /// [`Self::local_with_resolvers_and_effect_mode`]; this is the no-runner
    /// shape the mode-refusal tests need, and `#[cfg(test)]` keeps it out of
    /// the public surface.
    #[cfg(test)]
    pub(crate) fn local_with_journal_mode(
        action_runner: ActionRunner,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode(
            action_runner,
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            None,
            journal_mode,
        )
    }

    /// The mode-carrying twin of [`Self::local_with_resolvers_and_effect`] —
    /// the constructor a real (effect-performing) embedding uses: resolvers,
    /// journaling runner, AND [`EffectJournalMode::Enforced`].
    pub fn local_with_resolvers_and_effect_mode(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
        )
    }

    /// Like [`Self::local_with_resolvers_and_effect_mode`] with an injected
    /// [`RunJournal`] (bd-mi9n): the engine writes its own run-lifecycle rows
    /// (`Started` → `Running` → terminal) through the same journal its effects
    /// go through, so an engine-driven run needs no host lifecycle calls.
    #[allow(clippy::too_many_arguments)]
    pub fn local_with_resolvers_effect_mode_and_run_journal(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode_run_journal(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
            run_journal,
        )
    }

    /// The mode-carrying twin of [`Self::remote_with_resolvers_and_effect`].
    pub fn remote_with_resolvers_and_effect_mode(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
        )
    }

    /// The remote twin of
    /// [`Self::local_with_resolvers_effect_mode_and_run_journal`] (bd-mi9n).
    #[allow(clippy::too_many_arguments)]
    pub fn remote_with_resolvers_effect_mode_and_run_journal(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode_run_journal(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
            run_journal,
        )
    }

    /// Funnel for the local constructors: all four established above.
    fn local_with_resolvers_effect(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            EffectJournalMode::default(),
        )
    }

    /// Async funnel twin of [`Self::local_with_resolvers_effect`] (bd-ckkf):
    /// resolvers + an [`AsyncEffectRunner`], Recording mode, no journal.
    fn local_with_resolvers_async_effect(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_async_effect_mode_and_journal(
            action_runner,
            profile_resolver,
            workflow_resolver,
            async_effect_runner,
            EffectJournalMode::default(),
            None,
        )
    }

    /// The full async local constructor (bd-ckkf, ADR-0035 Spec 2.2):
    /// resolvers, an [`AsyncEffectRunner`] the engine awaits inline, an
    /// explicit [`EffectJournalMode`], and an [`AsyncRunJournal`] for the
    /// engine's own run-lifecycle rows. The constructor a real
    /// (effect-performing) async embedding uses.
    #[allow(clippy::too_many_arguments)]
    pub fn local_with_resolvers_async_effect_mode_and_journal(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode_run_journal_async(
            action_runner,
            profile_resolver,
            workflow_resolver,
            None,
            async_effect_runner,
            journal_mode,
            None,
            async_run_journal,
        )
    }

    /// The mode-carrying funnel: every local constructor tail. The journal
    /// mode is an engine dependency so the dispatch path can enforce Spec 6
    /// without a global.
    fn local_with_resolvers_effect_mode(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode_run_journal(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
            None,
        )
    }

    /// [`Self::local_with_resolvers_effect_mode`] + the injected run-lifecycle
    /// journal (bd-mi9n).
    fn local_with_resolvers_effect_mode_run_journal(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        Self::local_with_resolvers_effect_mode_run_journal_async(
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            None,
            journal_mode,
            run_journal,
            None,
        )
    }

    /// Terminal local funnel (bd-ckkf): every local constructor tails here.
    /// Sync seams keep their exact behavior (`None` async = legacy paths);
    /// `Some` async seams are registered as context dependencies the engine
    /// prefers and awaits inline.
    #[allow(clippy::too_many_arguments)]
    fn local_with_resolvers_effect_mode_run_journal_async(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let verify_sink = VerifySink::default();
        let resonate = Resonate::local()
            .with_dependency(action_runner)
            .with_dependency(effect_runner)
            .with_dependency(async_effect_runner)
            .with_dependency(profile_resolver)
            .with_dependency(workflow_resolver)
            .with_dependency(journal_mode)
            .with_dependency(run_journal)
            .with_dependency(async_run_journal)
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone())
            .with_dependency(verify_sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        resonate
            .register(run_graph_node)
            .map_err(|e| anyhow::anyhow!("register graph node function: {e}"))?;
        resonate
            .register(run_parallel_copy)
            .map_err(|e| anyhow::anyhow!("register parallel copy function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
            spawn_sink,
            policy_sink,
            verify_sink,
        })
    }

    /// New engine over a remote Resonate server (`ResonateConfig.url`),
    /// enabling cross-process crash survival (SC-11 uses a real server).
    pub fn remote(url: impl Into<String>, action_runner: ActionRunner) -> anyhow::Result<Self> {
        Self::remote_with(url, action_runner, Self::default_resolver())
    }

    /// Like [`Self::remote`] with a `ProfileResolver` for the `spawn` action.
    pub fn remote_with(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
    ) -> anyhow::Result<Self> {
        // Delegates to the funnel so every construction site injects the
        // same dependency set (bd-zczp: a directly-built Resonate once
        // missed the effect-runner dep and panicked leaf dispatch).
        Self::remote_with_resolvers_effect(
            url,
            action_runner,
            profile_resolver,
            Self::default_workflow_resolver(),
            None,
        )
    }

    /// Like [`Self::remote_with`] with an explicit [`WorkflowResolver`] for
    /// `call_workflow` (ADR-0007 R4.1; mirrors `local_with_resolvers`).
    pub fn remote_with_resolvers(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            None,
        )
    }

    /// Like [`Self::remote_with_resolvers`] with a context-bearing
    /// [`EffectRunner`] (bd-zczp). `None` preserves the legacy compat path.
    pub fn remote_with_effect_runner(
        url: impl Into<String>,
        action_runner: ActionRunner,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect(
            url,
            action_runner,
            Self::default_resolver(),
            Self::default_workflow_resolver(),
            effect_runner,
        )
    }

    /// Like [`Self::remote_with_resolvers`] **and**
    /// [`Self::remote_with_effect_runner`] at once
    /// (bd-engine-run-path-journaling-fwlx): the remote (durable-server) twin of
    /// [`Self::local_with_resolvers_and_effect`].
    pub fn remote_with_resolvers_and_effect(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
        )
    }

    /// The remote twin of
    /// [`Self::local_with_resolvers_async_effect_mode_and_journal`] (bd-ckkf).
    #[allow(clippy::too_many_arguments)]
    pub fn remote_with_resolvers_async_effect_mode_and_journal(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode_run_journal_async(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            None,
            async_effect_runner,
            journal_mode,
            None,
            async_run_journal,
        )
    }

    /// Funnel for the remote constructors.
    fn remote_with_resolvers_effect(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            EffectJournalMode::default(),
        )
    }

    /// The mode-carrying remote funnel.
    fn remote_with_resolvers_effect_mode(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode_run_journal(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            journal_mode,
            None,
        )
    }

    /// [`Self::remote_with_resolvers_effect_mode`] + the injected
    /// run-lifecycle journal (bd-mi9n).
    #[allow(clippy::too_many_arguments)]
    fn remote_with_resolvers_effect_mode_run_journal(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
    ) -> anyhow::Result<Self> {
        Self::remote_with_resolvers_effect_mode_run_journal_async(
            url,
            action_runner,
            profile_resolver,
            workflow_resolver,
            effect_runner,
            None,
            journal_mode,
            run_journal,
            None,
        )
    }

    /// Terminal remote funnel (bd-ckkf): every remote constructor tails
    /// here. See the local terminal funnel for the seam contract.
    #[allow(clippy::too_many_arguments)]
    fn remote_with_resolvers_effect_mode_run_journal_async(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
        workflow_resolver: WorkflowResolver,
        effect_runner: Option<EffectRunner>,
        async_effect_runner: Option<AsyncEffectRunner>,
        journal_mode: EffectJournalMode,
        run_journal: Option<RunJournal>,
        async_run_journal: Option<AsyncRunJournal>,
    ) -> anyhow::Result<Self> {
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let verify_sink = VerifySink::default();
        let config = ResonateConfig {
            url: Some(url.into()),
            ..Default::default()
        };
        let resonate = Resonate::new(config)
            .with_dependency(action_runner)
            .with_dependency(effect_runner)
            .with_dependency(async_effect_runner)
            .with_dependency(profile_resolver)
            .with_dependency(workflow_resolver)
            .with_dependency(journal_mode)
            .with_dependency(run_journal)
            .with_dependency(async_run_journal)
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone())
            .with_dependency(verify_sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        resonate
            .register(run_graph_node)
            .map_err(|e| anyhow::anyhow!("register graph node function: {e}"))?;
        resonate
            .register(run_parallel_copy)
            .map_err(|e| anyhow::anyhow!("register parallel copy function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
            spawn_sink,
            policy_sink,
            verify_sink,
        })
    }

    /// The default `ProfileResolver`: every profile is unknown (the embedding
    /// must inject a real one for `spawn` to succeed).
    pub fn default_resolver() -> ProfileResolver {
        Arc::new(|_name: &str| None)
    }

    /// The default `WorkflowResolver`: no named workflows (call_workflow fails
    /// until the embedding injects a catalog).
    pub fn default_workflow_resolver() -> WorkflowResolver {
        Arc::new(|_name: &str| None)
    }

    /// The Resonate runtime (for the approval listener to resolve promises).
    pub const fn resonate(&self) -> &Arc<Resonate> {
        &self.resonate
    }

    /// The approval side-channel: promise-ids of parked approvals.
    pub const fn approval_sink(&self) -> &ApprovalSink {
        &self.approval_sink
    }

    /// The spawn side-channel: promise-ids of parked spawn-awaits (the
    /// embedding reads these to emit the kind-43001 job carrying them).
    pub const fn spawn_sink(&self) -> &SpawnSink {
        &self.spawn_sink
    }

    /// The cron failure-policy side-channel (ADR-0005 Spec 3, P4.4): policy
    /// decisions recorded by the cron loop, executed by the embedding (bead /
    /// escalate DM).
    pub const fn policy_sink(&self) -> &crate::cron_failure::PolicySink {
        &self.policy_sink
    }

    /// The verify side-channel (ADR-0024 Spec 2).
    pub const fn verify_sink(&self) -> &VerifySink {
        &self.verify_sink
    }

    /// Start a workflow run, idempotent on `run_id` (a redelivered trigger
    /// must not double-start — Resonate dedups on the run id).
    pub async fn run(
        &self,
        run_id: &str,
        mut input: RunInput,
    ) -> anyhow::Result<serde_json::Value> {
        input.run_id = Some(run_id.to_string());
        self.resonate
            .run(run_id, run_workflow, input)
            .await
            .map_err(|e| anyhow::anyhow!("resonate run {run_id}: {e}"))
    }

    /// On-demand invoke (ADR-0005 Spec 2, P3): run a workflow with NO trigger
    /// event. The engine synthesizes a `trigger.*` template context from the
    /// provided `inputs` so `{{inputs.x}}` / `{{trigger.x}}` resolve on the
    /// invoke path — the SAME durable function as the trigger path, two entry
    /// doors.
    ///
    /// Declared defaults (`workflow.inputs[].default`) bind UNDER provided
    /// values (bd-engine-invoke-binds-defaults-o1ud, GS-028 NEW-8 residual):
    /// provided wins, missing keys fall back to the declaration.
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

    pub async fn invoke(
        &self,
        run_id: &str,
        workflow: crate::schema::Workflow,
        inputs: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        // Synthesized trigger payload: the merged inputs are visible both as
        // `{{inputs.<name>}}` (under `inputs`) and `{{trigger.<name>}}`
        // (flattened at the top level).
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
            run_id: None,
            workflow,
            trigger: serde_json::Value::Object(trigger),
        };
        self.run(run_id, input).await
    }

    /// Resolve an approval promise (the kind-46011 listener calls this).
    pub async fn resolve_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = Value::from_serializable(decision)
            .map_err(|e| anyhow::anyhow!("serialize decision: {e}"))?;
        self.resonate
            .promises
            .resolve(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("resolve promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Reject an approval promise.
    pub async fn reject_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = Value::from_serializable(decision)
            .map_err(|e| anyhow::anyhow!("serialize decision: {e}"))?;
        self.resonate
            .promises
            .reject(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("reject promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Resolve a spawn-await promise (the bus subscriber calls this on
    /// kind-43004 result — ADR-0005 Spec 4a).
    pub async fn resolve_spawn(
        &self,
        promise_id: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
        let v = Value::from_serializable(value)
            .map_err(|e| anyhow::anyhow!("serialize spawn result: {e}"))?;
        self.resonate
            .promises
            .resolve(promise_id, v)
            .await
            .map_err(|e| anyhow::anyhow!("resolve spawn promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Reject a spawn-await promise (the bus subscriber calls this on
    /// kind-43006 error — the job FAILED, so the run fails via Spec 0).
    pub async fn reject_spawn(&self, promise_id: &str, error: &str) -> anyhow::Result<()> {
        let value = Value::from_serializable(serde_json::json!({ "error": error }))
            .map_err(|e| anyhow::anyhow!("serialize spawn error: {e}"))?;
        self.resonate
            .promises
            .reject(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("reject spawn promise {promise_id}: {e}"))?;
        Ok(())
    }
}
