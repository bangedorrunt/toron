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

mod control_flow;
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
    let Some(addr) = (host.as_str(), port).to_socket_addrs().ok().and_then(|mut it| it.next()) else {
        return false;
    };
    TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(2000)).is_ok()
}

/// Parse `https?://host[:port]` into `(host, port)` (pure).
pub fn host_port(url: &str) -> Option<(String, u16)> {
    let rest = url.strip_prefix("https://").or_else(|| url.strip_prefix("http://"))?;
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
pub use control_flow::{
    GraphNodeInput, ParallelCopyInput, build_workflow_run_terminal, run_graph_node,
    run_parallel_copy, run_workflow, stamp_workflow_emitted,
};
pub use sinks::{ApprovalSink, SpawnSink};

// Crate-internal: the split modules' private helpers, re-exported so the
// tests.rs suite (moved verbatim, `use super::*`) keeps resolving them.
// Some names are exercised only indirectly — the allow documents that the
// block exists to serve the verbatim test suite, not the prod surface.
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use control_flow::{
    resolve_template_ref, run_branch, run_call_workflow, run_foreach, run_graph, run_parallel,
    run_retry, run_spawn, run_step, run_steps,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use template::{
    descend, render_action, render_json_value, render_scalar, render_template, resolve_steps,
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
}

/// The shared action-execution surface (the Mailbox). Run by the workflow
/// function via a Resonate dependency (consistency rule).
///
/// Every action returns a [`serde_json::Value`] (ADR-0005 Spec 0): the value
/// flows into `steps.<name>` for downstream templates/guards. `Null` is a
/// valid "no output" action.
pub type ActionRunner = Arc<dyn Fn(&Action) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// The `spawn` profile resolver DI seam (ADR-0005 Spec 4a). Resolves a
/// profile name (`@housekeeper`) to the (npub, spawn spec) the embedding
/// spawns. The ENGINE never imports flywheel — this seam is injected, same
/// idiom as `ActionRunner`. `None` → unknown profile (fail the step).
pub type ProfileResolver =
    Arc<dyn Fn(&str) -> Option<serde_json::Value> + Send + Sync>;

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
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let resonate = Resonate::local()
            .with_dependency(action_runner)
            .with_dependency(profile_resolver)
            .with_dependency(workflow_resolver)
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone());
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
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let config = ResonateConfig {
            url: Some(url.into()),
            ..Default::default()
        };
        let resonate = Resonate::new(config)
            .with_dependency(action_runner)
            .with_dependency(profile_resolver)
            .with_dependency(Self::default_workflow_resolver())
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone());
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
        })
    }

    /// The default `ProfileResolver`: every profile is unknown (the embedding
    /// must inject a real one for `spawn` to succeed).
    fn default_resolver() -> ProfileResolver {
        Arc::new(|_name: &str| None)
    }

    /// The default `WorkflowResolver`: no named workflows (call_workflow fails
    /// until the embedding injects a catalog).
    fn default_workflow_resolver() -> WorkflowResolver {
        Arc::new(|_name: &str| None)
    }

    /// The Resonate runtime (for the approval listener to resolve promises).
    pub fn resonate(&self) -> &Arc<Resonate> {
        &self.resonate
    }

    /// The approval side-channel: promise-ids of parked approvals.
    pub fn approval_sink(&self) -> &ApprovalSink {
        &self.approval_sink
    }

    /// The spawn side-channel: promise-ids of parked spawn-awaits (the
    /// embedding reads these to emit the kind-43001 job carrying them).
    pub fn spawn_sink(&self) -> &SpawnSink {
        &self.spawn_sink
    }

    /// The cron failure-policy side-channel (ADR-0005 Spec 3, P4.4): policy
    /// decisions recorded by the cron loop, executed by the embedding (bead /
    /// escalate DM).
    pub fn policy_sink(&self) -> &crate::cron_failure::PolicySink {
        &self.policy_sink
    }

    /// Start a workflow run, idempotent on `run_id` (a redelivered trigger
    /// must not double-start — Resonate dedups on the run id).
    pub async fn run(&self, run_id: &str, input: RunInput) -> anyhow::Result<serde_json::Value> {
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
    pub async fn invoke(
        &self,
        run_id: &str,
        workflow: crate::schema::Workflow,
        inputs: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        // Synthesized trigger payload: the inputs are visible both as
        // `{{inputs.<name>}}` (under `inputs`) and `{{trigger.<name>}}`
        // (flattened at the top level).
        let mut trigger = serde_json::Map::new();
        if let Some(obj) = inputs.as_object() {
            for (k, v) in obj {
                trigger.insert(k.clone(), v.clone());
            }
        }
        trigger.insert("inputs".into(), inputs.clone());
        let input = RunInput {
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
