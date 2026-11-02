// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Context-free spawn-spec construction (bd-spawn-park-toron-vwu3).
//!
//! `Action::Spawn` is the last shipped action whose park lives in the vendor
//! store. This module is the seam that survives that store: profile resolution
//! and spec construction, with no promise, no sink, and no await. The old
//! `run_spawn` and the durable model both call it, so the payload a consumer
//! reads has one author.
//!
//! The linear model sequences `Action::Spawn`; ADMISSION is capability-based
//! (bd-spawn-launcher-flip-d8mv): a host without a [`SpawnLauncher`] refuses
//! the workflow before any row, so a run can never park on a launch nobody
//! can perform.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::schema::{Action, ContextMode, Workflow};

use super::ProfileResolver;

/// The old promise data, made serializable and self-contained.
///
/// `spawn_key` replaces the promise id everywhere the durable path
/// correlates: pending event, claim, completion, resolution validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpawnSpec {
    /// `"{run_id}:{node}:{attempt}"` — the correlation key.
    pub spawn_key: String,
    /// Run this spawn belongs to.
    pub run_id: String,
    /// Step/node name.
    pub node: String,
    /// Workflow name.
    pub workflow: String,
    /// Project scope, when the run has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Park generation (1 in this slice).
    pub generation: u64,
    /// Profile name as authored.
    pub profile: String,
    /// `ProfileResolver` output.
    pub resolved: serde_json::Value,
    /// Harness override, trimmed. `None` = the profile's harness.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    /// Sub-workflow the spawned agent runs.
    #[serde(
        default,
        rename = "workflow_ref",
        skip_serializing_if = "Option::is_none"
    )]
    pub workflow_ref: Option<String>,
    /// `await: true` parks the run; `false` fires and returns.
    #[serde(rename = "await")]
    pub await_: bool,
    /// Deadline for a claim, in seconds. `0` = none.
    pub no_claim_timeout: u64,
    /// Execution context mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ContextMode>,
}

/// Everything `prepare_spawn` needs, as plain data.
#[derive(Debug, Clone)]
pub struct SpawnInput {
    /// Run identity.
    pub run_id: String,
    /// Step/node name.
    pub node: String,
    /// Scheduler attempt (1-based).
    pub attempt: u32,
    /// Profile name.
    pub profile: String,
    /// Harness override.
    pub harness: Option<String>,
    /// Sub-workflow name.
    pub workflow_ref: Option<String>,
    /// Park on completion.
    pub await_: bool,
    /// Claim deadline in seconds.
    pub no_claim_timeout: u64,
    /// Execution context mode.
    pub context: Option<ContextMode>,
    /// Project scope.
    pub project: Option<String>,
    /// Park generation.
    pub generation: u64,
}

/// Starts one child and resolves with its receipt (bd-spawn-launcher-flip-d8mv).
///
/// The host awaits this on `RunEffect::StartSpawn` and feeds the outcome back
/// as `SpawnSettled` / `SpawnFailed` — the same shape [`super::AsyncEffectRunner`]
/// has for leaves. The launcher owns the process that can actually start a
/// child (flywheel's pane plane); the model never calls it.
pub type SpawnLauncher = Arc<
    dyn Fn(SpawnSpec) -> Pin<Box<dyn Future<Output = anyhow::Result<serde_json::Value>> + Send>>
        + Send
        + Sync,
>;

/// The host capabilities the seam needs.
#[derive(Clone, Default)]
pub struct SpawnSeams {
    /// Profile name → resolved profile. `None` = unknown profile.
    pub resolver: Option<ProfileResolver>,
    /// Starts the child. `None` = this host cannot perform a spawn, which is
    /// what makes the router refuse a spawn workflow.
    pub launcher: Option<SpawnLauncher>,
}

/// Why a spawn workflow cannot run on this host: no launcher.
///
/// One spelling for the router ([`super::durable_host`]) and the host's
/// `durable_route`, so the refusal text cannot drift.
pub fn launcher_refusal() -> String {
    "spawn needs a launcher: this host cannot start a child pane".into()
}

/// `Some(reason)` when `workflow` has a spawn step and the host has no
/// launcher. Graph nodes cannot spawn, so only `steps:` is scanned.
pub fn spawn_launcher_refusal(workflow: &Workflow) -> Option<String> {
    workflow
        .steps
        .iter()
        .any(|step| matches!(step.action, Action::Spawn { .. }))
        .then(launcher_refusal)
}

/// Why a spawn could not be prepared. The run lands it as a step VALUE, never
/// a run failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnError {
    /// The value's `error` text (identical to the old `run_spawn` text).
    pub message: String,
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SpawnError {}

/// The one spelling of an unknown-profile failure.
pub fn unknown_profile_error(profile: &str) -> String {
    format!("spawn: unknown profile {profile:?} (ProfileResolver returned None)")
}

/// `"{run_id}:{node}:{attempt}"` — the correlation key that replaces the
/// promise id.
pub fn spawn_key(run_id: &str, node: &str, attempt: u32) -> String {
    format!("{run_id}:{node}:{attempt}")
}

/// Resolve the profile and build the spec. No promise, no sink, no await.
///
/// # Errors
///
/// [`SpawnError`] when the resolver does not know the profile (or none is
/// injected) — the caller lands that as `{ok:false, error}`.
pub fn prepare_spawn(seams: &SpawnSeams, input: SpawnInput) -> Result<SpawnSpec, SpawnError> {
    let resolver = seams.resolver.as_ref().ok_or_else(|| SpawnError {
        message: unknown_profile_error(&input.profile),
    })?;
    let resolved = resolver(&input.profile).ok_or_else(|| SpawnError {
        message: unknown_profile_error(&input.profile),
    })?;
    Ok(SpawnSpec {
        spawn_key: spawn_key(&input.run_id, &input.node, input.attempt),
        run_id: input.run_id,
        node: input.node,
        workflow: String::new(),
        project: input.project,
        generation: input.generation,
        profile: input.profile,
        resolved,
        harness: input
            .harness
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty()),
        workflow_ref: input.workflow_ref,
        await_: input.await_,
        no_claim_timeout: input.no_claim_timeout,
        context: input.context,
    })
}

impl SpawnSpec {
    /// The promise payload the old path published, reproduced byte for byte.
    ///
    /// Pre-flip runs and the old engine read this shape; it must keep the
    /// keys they always did.
    pub fn legacy_payload(&self) -> serde_json::Value {
        let mut spec = serde_json::json!({
            "profile": self.profile,
            "workflow": self.workflow_ref,
            "resolved": self.resolved,
            "await": self.await_,
        });
        if let Some(harness) = &self.harness {
            spec["harness"] = serde_json::json!(harness);
        }
        if let Some(context) = self.context {
            spec["context"] = serde_json::to_value(context)
                .unwrap_or(serde_json::Value::String("inherit".into()));
        }
        spec
    }

    /// True when the run must park on this spawn.
    pub const fn parks(&self) -> bool {
        self.await_
    }

    /// The absolute claim deadline, when the step set one.
    pub fn deadline_ms(&self, now_ms: u64) -> Option<u64> {
        (self.no_claim_timeout > 0)
            .then(|| now_ms.saturating_add(self.no_claim_timeout.saturating_mul(1_000)))
    }
}
