// governed-by: ADR-0024
// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Durable control-flow functions (F21.ENGINE-SPLIT move from the former
//! single-file engine): `run_workflow` + step dispatch, retry/branch/foreach/
//! parallel/call_workflow/spawn, the DAG scheduler, the parallel/graph durable
//! children, and the workflow-emission helpers (stamp + kind-46012 terminal).
//! All items are re-exported from the parent module.

use std::time::Duration;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use resonate_sdk::prelude::Context;

use crate::schema::{Action, Graph, Step, StepOnError};

use super::template::{RenderContext, render_action, render_action_lenient, render_text};
use super::{
    ActionInvocation, ActionPartition, ApprovalSink, AsyncEffectRunner, AsyncRunJournal, Decision,
    EffectJournalMode, EffectRunner, ExecutionPolicy, FailureKind, ProfileResolver, RunContext,
    RunInput, RunJournal, RunPhase, RunTransition, SpawnSink, WorkflowEngine, WorkflowResolver,
    invoke_compat, partition,
};

use sha2::{Digest, Sha256};

/// Spec 3 helper: extract artifacts from completed node outputs (paths, not blobs)
fn extract_artifacts(
    outputs: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut artifacts = Vec::new();
    for (node, value) in outputs {
        if let Some(obj) = value.as_object() {
            // Look for sha-like fields
            for key in ["sha", "commit_sha", "commit"] {
                if let Some(sha) = obj.get(key).and_then(|v| v.as_str()) {
                    artifacts.push(serde_json::json!({
                        "node": node,
                        "kind": "commit",
                        "sha": sha,
                    }));
                }
            }
            // Look for path-like fields
            for key in [
                "path",
                "receipt_path",
                "ledger_path",
                "stdout_path",
                "stderr_path",
            ] {
                if let Some(path) = obj.get(key).and_then(|v| v.as_str()) {
                    let kind = if key == "ledger_path" {
                        "ledger"
                    } else {
                        "receipt"
                    };
                    let mut art = serde_json::json!({
                        "node": node,
                        "kind": kind,
                        "path": path,
                    });
                    if let Some(exit) = obj.get("exit").and_then(serde_json::Value::as_i64) {
                        art["exit"] = serde_json::json!(exit);
                    }
                    artifacts.push(art);
                }
            }
            // If the node is a verify or commit-like, but the output is just {"sha": "..."} without kind, we already handled
            // Also handle case where output is {"ok": true, "sha": "..."} - we already handled sha above
            // For generic path without sha, we already handled
        }
    }
    // Sort by node for stable hash
    artifacts.sort_by(|a, b| a["node"].as_str().cmp(&b["node"].as_str()));
    artifacts
}

/// Spec 3: compute evidence_hash as SHA256 hex of canonical JSON of {run_id, node, artifacts, ledger_path}
pub(crate) fn compute_evidence_hash(
    run_id: &str,
    node: &str,
    artifacts: &[serde_json::Value],
    ledger_path: &str,
) -> String {
    // Build canonical object with sorted keys via BTreeMap
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "run_id".to_string(),
        serde_json::Value::String(run_id.to_string()),
    );
    map.insert(
        "node".to_string(),
        serde_json::Value::String(node.to_string()),
    );
    // For artifacts, only include path and sha for each, sorted
    let arts: Vec<serde_json::Value> = artifacts
        .iter()
        .map(|a| {
            let mut m = std::collections::BTreeMap::new();
            if let Some(p) = a.get("path") {
                m.insert("path".to_string(), p.clone());
            }
            if let Some(s) = a.get("sha") {
                m.insert("sha".to_string(), s.clone());
            }
            serde_json::Value::Object(m.into_iter().collect())
        })
        .collect();
    map.insert("artifacts".to_string(), serde_json::Value::Array(arts));
    map.insert(
        "ledger_path".to_string(),
        serde_json::Value::String(ledger_path.to_string()),
    );
    let canonical = serde_json::to_string(&map).expect("canonical evidence JSON serializes");
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    hex::encode(hasher.finalize())
}

/// Spec 3: build the full approval promise data
fn build_approval_payload(
    run_id: &str,
    workflow_name: &str,
    node_name: &str,
    wave: usize,
    question: &str,
    outputs: &serde_json::Map<String, serde_json::Value>,
    graph: &Graph,
) -> serde_json::Value {
    let nodes_completed: Vec<String> = outputs.keys().cloned().collect();
    let nodes_failed: Vec<String> = outputs
        .iter()
        .filter(|(_, v)| v.get("ok").and_then(serde_json::Value::as_bool) == Some(false))
        .map(|(k, _)| k.clone())
        .collect();
    let all_nodes: std::collections::BTreeSet<String> = graph.nodes.keys().cloned().collect();
    let done_set: std::collections::BTreeSet<String> = outputs.keys().cloned().collect();
    let mut pending: Vec<String> = all_nodes.difference(&done_set).cloned().collect();
    // Remove the current node from pending (it's the one being requested)
    pending.retain(|n| n != node_name);
    pending.sort();

    let mut completed_sorted = nodes_completed;
    completed_sorted.sort();
    let mut failed_sorted = nodes_failed;
    failed_sorted.sort();

    let artifacts = extract_artifacts(outputs);
    let ledger_path = format!(".flywheel/receipts/{run_id}-progress-ledger.json");
    let blast_radius = workflow_name.to_string();
    let timeout = "PT24H".to_string();

    let evidence_hash = compute_evidence_hash(run_id, node_name, &artifacts, &ledger_path);

    serde_json::json!({
        "run_id": run_id,
        "workflow": workflow_name,
        "suspended_at": {
            "node": node_name,
            "wave": wave,
        },
        "question": question,
        "context": {
            "nodes_completed": completed_sorted,
            "nodes_failed": failed_sorted,
            "nodes_pending": pending,
            "artifacts": artifacts,
            "ledger_path": ledger_path,
            "blast_radius": blast_radius,
            "timeout": timeout,
        },
        "evidence_hash": evidence_hash,
        "message": question,
    })
}

/// The durable workflow function: executes steps, durable-sleeps on `delay`,
/// and parks on `request_approval` until the promise resolves (WF-08 fix).
///
/// Action failures are captured per-step (`{"ok":false,"error":…}`) rather
/// than aborting the run — the run completes so durable state stays
/// consistent; the error is observable in the kind-46012 output. (ADR-0005
/// Spec 0 adds an opt-in `on_error: fail_run` in a later phase.)
#[resonate_sdk::function]
pub async fn run_workflow(
    ctx: &Context,
    input: RunInput,
) -> resonate_sdk::Result<serde_json::Value> {
    let workflow = &input.workflow;
    let graph = workflow.to_graph();
    let run_id = input
        .run_id
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let workflow_name = workflow.name.clone();
    if let Err(error) = validate_workflow_lineage(&input.trigger, &workflow_name) {
        return Ok(serde_json::json!({
            "status": "failed",
            "error": error,
            "failed_step": "<workflow-lineage>",
        }));
    }
    // bd-zczp: canonical digest travels with the run so every leaf can
    // mint run-scoped effect keys without re-canonicalizing per step.
    let workflow_digest = crate::identity::workflow_digest(workflow);
    // bd-mi9n: the run reports its OWN lifecycle through the same injected
    // journal its effects go through, so an engine-driven run has a
    // `workflow_runs` row without the embedding calling
    // `create_run`/`cas_run_status` itself — and the run row and the effect
    // rows agree on `run_id` by construction (same field). Absent journal =
    // no run rows (the embedding owns its lifecycle, as before).
    let run_journal_dep = ctx.get_dependency::<Option<RunJournal>>();
    let run_journal: Option<&RunJournal> = run_journal_dep.as_ref().as_ref();
    // bd-ckkf: the async journal wins — awaited inline, never bridged.
    let async_journal_dep = ctx.get_dependency::<Option<AsyncRunJournal>>();
    let async_run_journal: Option<AsyncRunJournal> = (*async_journal_dep).clone();
    let report_phase =
        |run_id: &str, workflow: &str, workflow_digest: &str, phase: RunPhase| RunTransition {
            run_id: run_id.to_string(),
            workflow: workflow.to_string(),
            workflow_digest: workflow_digest.to_string(),
            phase,
        };
    if let Some(journal) = async_run_journal.clone() {
        for phase in [RunPhase::Started, RunPhase::Running] {
            journal(report_phase(
                &run_id,
                &workflow_name,
                &workflow_digest,
                phase,
            ))
            .await;
        }
    } else if let Some(journal) = run_journal {
        for phase in [RunPhase::Started, RunPhase::Running] {
            journal(&report_phase(
                &run_id,
                &workflow_name,
                &workflow_digest,
                phase,
            ));
        }
    }
    let outcome = run_graph(
        ctx,
        &input.trigger,
        &graph,
        &run_id,
        &workflow_name,
        &workflow_digest,
    )
    .await;
    if async_run_journal.clone().is_some() || run_journal.is_some() {
        // The engine's ONE outcome rule (same one `build_workflow_run_terminal`
        // reads): an outcome carrying `status: "failed"` is the failure, any
        // other Ok is success, and an Err is a failure.
        let phase = match &outcome {
            Ok(v) if v.get("status").and_then(serde_json::Value::as_str) == Some("failed") => {
                RunPhase::Failed
            }
            Ok(_) => RunPhase::Succeeded,
            Err(_) => RunPhase::Failed,
        };
        let transition = report_phase(&run_id, &workflow_name, &workflow_digest, phase);
        if let Some(journal) = async_run_journal {
            journal(transition).await;
        } else if let Some(journal) = run_journal {
            journal(&transition);
        }
    }
    outcome
}

/// Execute an ordered list of steps, accumulating outputs into `step_outputs`
/// (under each step's `name`). Recursed into by control-flow actions
/// (ADR-0005 Spec 4): `retry`/`branch`/`foreach`/`parallel`/`call_workflow`.
///
/// Returns `Ok(None)` when the run completed normally; `Ok(Some(failed))`
/// when a step with `on_error: fail_run` failed — the failed-outcome value is
/// the run's terminal result (propagated to the caller, never stored under a
/// step name).
// Threaded dispatch context (run identity for leaf invocations) exceeds the
// default arity cap; see run_retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_steps(
    ctx: &Context,
    trigger: &serde_json::Value,
    steps: &[Step],
    step_outputs: &mut serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<Option<serde_json::Value>> {
    for step in steps {
        let value = run_step(
            ctx,
            trigger,
            step,
            step_outputs,
            workflow,
            run_id,
            workflow_digest,
        )
        .await?;
        // fail_run short-circuit: the failed outcome IS the run's result.
        if value.get("status").and_then(serde_json::Value::as_str) == Some("failed") {
            return Ok(Some(value));
        }
        if let Some(name) = &step.name {
            step_outputs.insert(name.clone(), value);
        }
    }
    Ok(None)
}

/// Execute ONE step (the linear body extracted from `run_workflow`, plus the
/// P5.3 control-flow actions). Returns the step's output value.
///
/// Render a `request_approval` message through the template engine — the
/// same render-first contract the mailbox actions follow. Returns the raw
/// message unchanged when the action is not `request_approval` (callers
/// treat that as a contract bug) and surfaces render errors to let the
/// caller fall back (bd-adr0006-cosmetic-sweep-q0g0).
pub(crate) fn rendered_approval_message(
    action: &Action,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
) -> anyhow::Result<String> {
    let render_ctx = RenderContext::new(workflow, node);
    match render_action(action, trigger, step_outputs, &render_ctx)? {
        Action::RequestApproval { message } => Ok(message),
        other => Err(anyhow::anyhow!(
            "rendered approval action is not request_approval: {other:?}"
        )),
    }
}

/// ADR-0032 Spec 7: ONE retry coordinator. Every retry loop in this module
/// (linear fail_run, explicit `Retry`, graph-node fail_run) reads bounds,
/// backoff, and retry candidacy from one owner. `Action::Retry` contributes
/// an explicit policy; otherwise the default (3 attempts, exponential
/// backoff) applies. Nested `Retry` wrappers flatten at runtime (lint
/// rejects them statically) so bounds never multiply.
pub(crate) struct RetryCoordinator {
    max_attempts: u32,
    /// Fixed inter-attempt backoff from the enclosing `Retry` action, if any.
    /// `None` → exponential 1s/2s/4s… capped at 60s.
    fixed_backoff_secs: Option<u64>,
}

impl RetryCoordinator {
    /// Default policy: 3 attempts, exponential backoff.
    const fn default_policy() -> Self {
        Self {
            max_attempts: 3,
            fixed_backoff_secs: None,
        }
    }

    /// Explicit policy from an enclosing `Action::Retry`.
    fn from_retry_action(max_attempts: u32, backoff_secs: u64) -> Self {
        Self {
            max_attempts,
            fixed_backoff_secs: (backoff_secs > 0).then_some(backoff_secs),
        }
    }

    /// Policy from an enclosing retry context (`run_step_with_enclosing`),
    /// or the default when there is none.
    fn from_enclosing(enclosing: Option<(u32, u64)>) -> Self {
        match enclosing {
            Some((max, backoff)) => Self::from_retry_action(max, backoff),
            None => Self::default_policy(),
        }
    }

    /// Whether another attempt is allowed after `attempt` (1-based) failed.
    fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }

    /// Retry candidacy for a step value: `rate_limit`/`timeout` (the frozen
    /// [`FailureKind`] vocabulary) are candidates; everything else escalates.
    /// Unknown kind strings are never retryable.
    fn is_retryable_value(value: &serde_json::Value) -> bool {
        value
            .get("kind")
            .and_then(|v| v.as_str())
            .and_then(|k| k.parse::<FailureKind>().ok())
            .is_some_and(super::boundary::FailureKind::is_retry_candidate)
    }

    /// Backoff before the next attempt: provider `retry_after_ms` wins, then
    /// the enclosing fixed backoff, else exponential.
    fn backoff(&self, attempt: u32, retry_after_ms: Option<u64>) -> Duration {
        backoff_for_attempt(attempt, retry_after_ms, self.fixed_backoff_secs)
    }
}

/// Parse one canonical wire spelling back into its enum (local inverse of
/// [`Disposition::as_str`]; kept beside its only caller so boundary.rs
/// stays untouched). `None` is unknown — the caller normalizes via kind.
fn parse_disposition(s: &str) -> Option<super::boundary::Disposition> {
    use super::boundary::Disposition as D;
    Some(match s {
        "continue" => D::Continue,
        "retry" => D::Retry,
        "reroute" => D::Reroute,
        "blocked" => D::Blocked,
        "terminal_failed" => D::TerminalFailed,
        "cancelled" => D::Cancelled,
        "reconcile" => D::Reconcile,
        _ => return None,
    })
}

/// Engine-internal failure routing as data (ADR-0035 Spec 6): a prior
/// disposition wins when it parses; otherwise the kind maps. `rate_limit`
/// normalizes to `Blocked` (the legacy string spelling had exactly one
/// producer — this fn — and zero readers, so the spelling is unified,
/// not migrated).
fn disposition_for_kind(kind: &str, existing: Option<&str>) -> super::boundary::Disposition {
    use super::boundary::Disposition as D;
    if let Some(d) = existing.and_then(parse_disposition) {
        return d;
    }
    match kind {
        "rate_limit" => D::Blocked,
        "timeout" => D::TerminalFailed,
        _ => D::TerminalFailed,
    }
}

/// bd-90eb: truncated preview of a failed leaf value for the fail_run
/// terminal artifact — sorted top-level keys, 512-char bound with the
/// marker appended past the bound, bare leaves emit the fixed `empty-leaf`
/// token so terminal artifacts never again carry zero evidence. Pure
/// value-render; kept out of the async dispatch.
fn leaf_preview_for_value(value: &serde_json::Value) -> String {
    const BOUND: usize = 512;
    const MARKER: &str = "\u{2026}(truncated)";
    const EMPTY: &str = "empty-leaf";
    let is_bare = match value {
        serde_json::Value::Null => true,
        serde_json::Value::Object(m) => m.is_empty(),
        _ => false,
    };
    if is_bare {
        return EMPTY.to_string();
    }
    let canonical = match value {
        serde_json::Value::Object(m) => {
            let mut sorted = serde_json::Map::new();
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            for k in keys {
                sorted.insert(k.clone(), m[k].clone());
            }
            serde_json::Value::Object(sorted).to_string()
        }
        _ => value.to_string(),
    };
    let chars: Vec<char> = canonical.chars().collect();
    if chars.len() > BOUND {
        let head: String = chars[..BOUND].iter().collect();
        format!("{head}{MARKER}")
    } else {
        canonical
    }
}

fn backoff_for_attempt(
    attempt: u32,
    retry_after_ms: Option<u64>,
    enclosing_backoff: Option<u64>,
) -> Duration {
    if let Some(ms) = retry_after_ms {
        return Duration::from_millis(ms);
    }
    if let Some(b) = enclosing_backoff.filter(|&b| b > 0) {
        return Duration::from_secs(b);
    }
    let secs = 1u64.checked_shl(attempt - 1).unwrap_or(60).min(60);
    Duration::from_secs(secs)
}

/// Build the context-bearing invocation for one rendered leaf action
/// (bd-zczp, Spec 1): run/node identity plus the attempt the retry
/// coordinator is on. The host derives the stable effect key from
/// (workflow_digest, run_id, node, iteration, action_digest) and honors
/// a pre-set `RunContext.effect_key` when the caller already minted one.
pub(crate) fn invocation_for_leaf(
    action: Action,
    run_id: &str,
    workflow_digest: &str,
    node: &str,
    attempt: u32,
    max_attempts: u32,
    iteration_key: Option<&str>,
) -> ActionInvocation {
    ActionInvocation {
        action,
        run: RunContext {
            run_id: run_id.to_string(),
            workflow_digest: workflow_digest.to_string(),
            node_path: node.to_string(),
            iteration_key: iteration_key.map(str::to_string),
            effect_key: None,
            attempt,
            provider_result: None,
        },
        execution: ExecutionPolicy {
            max_attempts,
            timeout_secs: None,
        },
    }
}

/// ADR-0032 Spec 6 (bd-engine-run-path-journaling-fwlx): an effectful leaf
/// dispatched with NO `EffectRunner` wrote no journal row. `Recording` marks it
/// (below) so a journal-less embedding is visible in the run ledger instead of
/// looking journaled; `Enforced` refuses it outright.
///
/// The fail-closed flip has LANDED (bd-engine-run-path-journaling-fwlx):
/// `EffectJournalMode::Enforced` refuses uncovered effectful leaves, and the
/// engines that really dispatch (toron-mcp's host, flywheel's journaled
/// constructors) are built Enforced. `Recording` — the default — stays the
/// dry-arm carve-out: `workflow run` without `--execute` and `cron run`
/// without `--live` record instead of perform, so their leaves must stay
/// unjournaled, visibly.
pub const EFFECT_JOURNAL_BYPASSED: &str = "bypassed-no-effect-runner";

/// The same gap, one step narrower: an `EffectRunner` IS injected and simply
/// does not cover this host sink — a different gap with a different fix, so a
/// different marker. Emitted by HOST runners that cover only part of their
/// sinks (e.g. flywheel's mailbox-without-an-in-process-plane path), never by
/// the engine: since the verify flip every HostSink leaf routes through the
/// injected runner, so the engine can no longer observe a partial runner.
pub const EFFECT_JOURNAL_SINK_UNCOVERED: &str = "bypassed-sink-uncovered";

/// Dispatch one rendered leaf action through the effect surface (bd-zczp,
/// bd-engine-run-path-journaling-fwlx).
///
/// ONE function owns the whole contract, because a half-applied contract is
/// exactly how a bypass gets re-introduced:
///
/// - a pure / engine-control / unsupported leaf computes in the boundary
///   with no runner at all (bd-ctj7: the sync compat path is deleted — there
///   is no runner to misdispatch through, the boundary rejects on its own);
/// - an effectful HostSink leaf (mailbox · job · verify) goes through the
///   context-bearing async runner when one is injected, else the sync
///   [`EffectRunner`];
/// - `verify` is a HostSink too, so its embedding-owned `VerifySink` executes
///   FIRST and its result rides [`RunContext::provider_result`]: the sink stays
///   the executor (ADR-0024 Spec 2), the journal settles AROUND an effect that
///   already happened, and the command never runs twice;
/// - with NO injected runner, [`EffectJournalMode`] decides — see
///   [`without_journal_seam`].
///
/// Returns the leaf's value, never an `Option`: a refusal is a value, so no
/// caller can mistake "refused" for "not my concern" and re-dispatch it.
///
/// Async: every branch that touches a runner is awaited inline — the one
/// async boundary in the leaf path (bd-ckkf, ADR-0035 Spec 2.2).
async fn dispatch_leaf_or_compat(
    ctx: &Context,
    invocation: &ActionInvocation,
) -> serde_json::Value {
    let ActionPartition::HostSink { sink } = partition(&invocation.action) else {
        return invoke_compat(&invocation.action, |_| None);
    };
    // bd-ckkf: the async seam wins for HostSink leaves — awaited inline,
    // never thread-hop bridged. Absent = the legacy sync surface below.
    let async_stored: Arc<Option<AsyncEffectRunner>> =
        ctx.get_dependency::<Option<AsyncEffectRunner>>();
    if let Some(afx) = (*async_stored).clone() {
        return dispatch_leaf_async(ctx, &afx, invocation).await;
    }
    let stored: Arc<Option<EffectRunner>> = ctx.get_dependency::<Option<EffectRunner>>();
    let Some(fx) = (*stored).clone() else {
        return without_journal_seam(ctx, invocation, sink);
    }; // Only `verify` needs a second, carrier-bearing invocation (the clone is
    // avoided on the common path: an effectful leaf dispatches on every run).
    let outcome = match &invocation.action {
        Action::Verify { command } => {
            let sink: Arc<crate::engine::VerifySink> =
                ctx.get_dependency::<crate::engine::VerifySink>();
            let mut journaled = invocation.clone();
            journaled.run.provider_result = Some(sink.project(command));
            fx(&journaled)
        }
        _ => fx(invocation),
    };
    match outcome {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
    }
}

/// The async twin of the sync effect branch above (bd-ckkf): the same
/// verify-first carrier rule, but the runner is awaited inline instead of
/// bridged. Runner-shaped values throughout (`{ok:false,…}` failures, never
/// bare `Err`), so the engine's failure predicate keeps working.
async fn dispatch_leaf_async(
    ctx: &Context,
    afx: &AsyncEffectRunner,
    invocation: &ActionInvocation,
) -> serde_json::Value {
    // Only `verify` needs a second, carrier-bearing invocation (the clone is
    // avoided on the common path: an effectful leaf dispatches on every run).
    let outcome = match &invocation.action {
        Action::Verify { command } => {
            let sink: Arc<crate::engine::VerifySink> =
                ctx.get_dependency::<crate::engine::VerifySink>();
            let mut journaled = invocation.clone();
            journaled.run.provider_result = Some(sink.project(command));
            afx(&journaled).await
        }
        _ => afx(invocation).await,
    };
    match outcome {
        Ok(value) => value,
        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
    }
}

/// bd-ynef: "now" for the park decision, as INJECTED HOST DATA.
///
/// The SDK deliberately gives a leaf no wall clock, so the host supplies the
/// clock as a dependency — the same shape as `LoopParams` policy arriving
/// serialized rather than being read from the environment inside a reducer. The
/// leaf CALLS the host clock; it never reads one itself.
///
/// A host that injects nothing fails loudly. It must never default: `now = 0`
/// would make every park look 56 years overdue and resume instantly, which is
/// the silent-wrong-wake-time this bead exists to prevent.
fn park_now_ms(ctx: &Context) -> resonate_sdk::prelude::Result<u64> {
    let clock: Arc<crate::engine::park::ParkClock> =
        ctx.get_dependency::<crate::engine::park::ParkClock>();
    Ok(clock())
}

/// bd-ynef: render a park's `until_ms` and parse it to an absolute time.
///
/// STRICT rendering, unlike the approval message: an unrenderable template
/// here would silently become a garbage wake time (0, or a parse failure
/// coerced to "now"), and a park that resumes immediately because its template
/// did not resolve is exactly the silent-wrong-wake-time this bead exists to
/// prevent. A literal integer needs no template resolution.
fn render_park_until_ms(
    until_ms: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> resonate_sdk::prelude::Result<u64> {
    let rendered = if until_ms.contains("{{") {
        crate::engine::template::render_text(until_ms, trigger, step_outputs).map_err(|e| {
            resonate_sdk::error::Error::Application {
                message: format!(
                    "park: until_ms template {until_ms:?} did not render ({e}) — a park must \
                     carry a resolvable wake time, never a silently-defaulted one (bd-ynef)"
                ),
            }
        })?
    } else {
        until_ms.to_string()
    };
    rendered
        .trim()
        .parse::<u64>()
        .map_err(|_| resonate_sdk::error::Error::Application {
            message: format!(
                "park: until_ms {rendered:?} is not an absolute unix-millis integer (bd-ynef). \
                 The wake time is DATA on the step, not a duration the engine computes."
            ),
        })
}

/// bd-ynef: journal the park intent BEFORE the provider waits.
///
/// Returns `Some(value)` when the journal owns the outcome: under
/// `EffectJournalMode::Enforced` a park with no journal seam is REFUSED before
/// any wait happens, because an unjournaled park is a wait that a crash can
/// erase with nothing left to resume from. Under `Recording` the park proceeds
/// and the value carries the bypass marker, so a dry arm never reads as a
/// durable park.
async fn journal_park_intent(
    ctx: &Context,
    intent: &crate::engine::park::ParkIntent,
    workflow_digest: &str,
    attempt: u32,
    max_attempts: u32,
) -> resonate_sdk::prelude::Result<Option<serde_json::Value>> {
    let mut invocation = invocation_for_leaf(
        Action::Park {
            until_ms: intent.until_ms.to_string(),
            max_wait_secs: crate::engine::park::PARK_MAX_WAIT_SECS,
            reason: intent.reason.clone(),
        },
        &intent.run_id,
        workflow_digest,
        &intent.node,
        attempt,
        max_attempts,
        None,
    );
    // The re-drive key IS the effect key: parking the same step to the same T
    // twice must land on the same journal row, which is what makes the
    // re-drive idempotent rather than a second park.
    invocation.run.effect_key = Some(intent.key.clone());
    let mode: Arc<EffectJournalMode> = ctx.get_dependency::<EffectJournalMode>();
    // The option lives INSIDE the Arc (the dispatch_leaf_or_compat seam), so
    // this lookup never panics on an embedding that injected no runner.
    let stored: Arc<Option<EffectRunner>> = ctx.get_dependency::<Option<EffectRunner>>();
    let Some(runner) = (*stored).clone() else {
        if *mode == EffectJournalMode::Enforced {
            return Err(resonate_sdk::error::Error::Application {
                message:
                    "park REFUSED — no EffectRunner is injected (EffectJournalMode::Enforced): \
                     the park intent must be journaled before the wait, or a crash mid-park \
                     erases the resume point (ADR-0032 Spec 6, bd-ynef)"
                        .to_string(),
            });
        }
        eprintln!(
            "toron-workflow: park wrote NO effect-journal row ({EFFECT_JOURNAL_BYPASSED}) — \
             a park without its intent is a wait that cannot be resumed (bd-ynef)"
        );
        return Ok(None);
    };
    let value = runner(&invocation).map_err(|e| resonate_sdk::error::Error::Application {
        message: format!("park: journaling the intent failed: {e} (bd-ynef)"),
    })?;
    if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        // A refused journal insert refuses the park: the intent is the thing
        // that makes the wait resumable.
        return Err(resonate_sdk::error::Error::Application {
            message: format!(
                "park REFUSED — the journal did not accept the intent: {value}. A park whose \
                 intent is not journaled is a wait that vanishes on crash (bd-ynef)."
            ),
        });
    }
    let mut out = serde_json::Map::new();
    out.insert("ok".into(), serde_json::Value::Bool(true));
    out.insert("parked".into(), serde_json::Value::Bool(true));
    out.insert("resumed".into(), serde_json::Value::Bool(false));
    out.insert("journaled".into(), serde_json::Value::Bool(true));
    out.insert("redrive_key".into(), serde_json::json!(intent.key));
    out.insert("until_ms".into(), serde_json::json!(intent.until_ms));
    Ok(Some(serde_json::Value::Object(out)))
}

/// An effectful HostSink leaf with NO injected runner.
///
/// `Enforced` REFUSES it — before any provider runs, because the provider
/// performs the very external effect the journal exists to cover.
/// `Recording` records the refusal visibly (bd-ctj7: Recording never
/// performs — the old compat-runner execution is deleted): the leaf's value
/// carries [`EFFECT_JOURNAL_BYPASSED`], so a dry arm reads as unperformed,
/// never as success.
fn without_journal_seam(
    ctx: &Context,
    invocation: &ActionInvocation,
    sink: &'static str,
) -> serde_json::Value {
    let mode: Arc<EffectJournalMode> = ctx.get_dependency::<EffectJournalMode>();
    if *mode == EffectJournalMode::Enforced {
        eprintln!(
            "toron-workflow: effectful '{sink}' action '{}' REFUSED — no EffectRunner is \
             injected (EffectJournalMode::Enforced): an external effect is never performed \
             without its journal (ADR-0032 Spec 6, bd-engine-run-path-journaling-fwlx)",
            invocation.action.type_name()
        );
        return serde_json::json!({
            "ok": false,
            "error": format!(
                "effectful '{sink}' action '{}' refused: no EffectRunner injected \
                 (EffectJournalMode::Enforced, ADR-0032 Spec 6) — build the engine with a \
                 journaling runner or drop to Recording for dry arms",
                invocation.action.type_name()
            ),
            "kind": "effect_journal_unavailable",
        });
    }
    eprintln!(
        "toron-workflow: effectful '{sink}' action '{}' wrote NO effect-journal row \
         ({EFFECT_JOURNAL_BYPASSED}) — ADR-0032 Spec 6 forbids a silent bypass; inject an \
         EffectRunner covering this sink",
        invocation.action.type_name()
    );
    let mut value = match &invocation.action {
        // `Recording` records instead of performs (bd-ctj7): the verify
        // sink still projects (it already executed), every other HostSink
        // leaf reports its missing sink — nothing performs here, ever.
        Action::Verify { command } => {
            let sink: Arc<crate::engine::VerifySink> =
                ctx.get_dependency::<crate::engine::VerifySink>();
            sink.project(command)
        }
        _ => invoke_compat(&invocation.action, |_| None),
    };
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "effect_journal".into(),
            serde_json::json!(EFFECT_JOURNAL_BYPASSED),
        );
    }
    value
}

pub(crate) fn run_step<'a>(
    ctx: &'a Context,
    trigger: &'a serde_json::Value,
    step: &'a Step,
    step_outputs: &'a mut serde_json::Map<String, serde_json::Value>,
    workflow: &'a str,
    run_id: &'a str,
    workflow_digest: &'a str,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = resonate_sdk::prelude::Result<serde_json::Value>>
            + Send
            + 'a,
    >,
> {
    run_step_with_enclosing(
        ctx,
        trigger,
        step,
        step_outputs,
        None,
        workflow,
        run_id,
        workflow_digest,
        None,
    )
}

// Threaded dispatch context (run identity for leaf invocations) exceeds the
// default arity cap; see run_retry.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_step_with_enclosing<'a>(
    ctx: &'a Context,
    trigger: &'a serde_json::Value,
    step: &'a Step,
    step_outputs: &'a mut serde_json::Map<String, serde_json::Value>,
    enclosing_retry: Option<(u32, u64)>,
    workflow: &'a str,
    run_id: &'a str,
    workflow_digest: &'a str,
    iteration_key: Option<&'a str>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = resonate_sdk::prelude::Result<serde_json::Value>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        // ADR-0032 Spec 4: the step guard runs through the ONE evaluator
        // before anything else executes (nested steps included — the graph
        // path evaluates node guards in run_graph_node instead).
        let node = step.name.as_deref().unwrap_or("<unnamed>");
        match evaluate_step_guard(step.guard.as_deref(), trigger, step_outputs, node).await {
            GuardVerdict::Proceed => {}
            GuardVerdict::Skip { reason } => return Ok(skipped_value(node, reason)),
            GuardVerdict::Fail { error } => {
                return Ok(serde_json::json!({ "ok": false, "error": error }));
            }
        }
        // ADR-0032 Spec 7: this loop is a client of the ONE retry
        // coordinator — bounds, backoff, and retry candidacy all resolve
        // there. A retryable runtime failure retries the SAME logical node.
        let coordinator = RetryCoordinator::from_enclosing(enclosing_retry);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let mut value = match &step.action {
                Action::Delay { secs } => {
                    // bd-zbnw (ADR-0035 Spec 2.5): the lint advisory cap is
                    // now enforced — an over-cap delay sleeps the cap, says
                    // so loud (stderr + the value), and never sleeps
                    // unbounded.
                    let requested = *secs;
                    let capped = requested > crate::lint::DELAY_CAP_SECS;
                    let applied = requested.min(crate::lint::DELAY_CAP_SECS);
                    if capped {
                        eprintln!(
                            "toron-workflow: delay {requested}s exceeds the cap {}s — sleeping the cap (bd-zbnw, ADR-0035 Spec 2.5)",
                            crate::lint::DELAY_CAP_SECS
                        );
                    }
                    ctx.sleep(Duration::from_secs(applied)).await?;
                    // bd-a5ky: a typed Unit, never Null past the output
                    // contract.
                    let mut unit = serde_json::Map::new();
                    unit.insert("ok".into(), serde_json::Value::Bool(true));
                    unit.insert("unit".into(), serde_json::json!({ "delay_secs": applied }));
                    if capped {
                        unit.insert("capped".into(), serde_json::Value::Bool(true));
                        unit.insert("requested_secs".into(), serde_json::json!(requested));
                    }
                    serde_json::Value::Object(unit)
                }
                Action::Park {
                    until_ms,
                    max_wait_secs,
                    reason,
                } => {
                    // bd-ynef: the durable half of DispatchAcquire::Park.
                    //
                    // `until_ms` is DATA on the step (it may be an author
                    // template), never a clock read here: the wake time
                    // arrives the way LoopParams policy arrives serialized, so
                    // a replayed run reaches the same decision. The only
                    // clock in this leaf is the injected `now`, used to
                    // compare against that data.
                    let rendered = render_park_until_ms(until_ms, trigger, step_outputs)?;
                    // The clock is INJECTED, never read here. The SDK exposes
                    // no wall clock to a leaf, and that is the point: the wake
                    // time is data (on the step) and "now" is data (injected by
                    // the host), the way LoopParams policy arrives serialized.
                    // A leaf that read the clock itself would make a replayed
                    // run reach a different decision.
                    let now_ms = park_now_ms(ctx)?;
                    let decision = crate::engine::park::plan_park(
                        rendered,
                        *max_wait_secs,
                        now_ms,
                        crate::engine::park::PARK_MAX_WAIT_SECS,
                    );
                    let reason = reason.as_deref().map(|r| {
                        crate::engine::template::render_text(r, trigger, step_outputs)
                            .unwrap_or_else(|_| r.to_string())
                    });
                    match &decision {
                        // T already passed: resume NOW. Re-driving an elapsed
                        // park must not park again, or the run re-sleeps on
                        // every re-drive and never progresses.
                        crate::engine::park::ParkDecision::ResumeNow { overdue_ms } => {
                            let mut unit = serde_json::Map::new();
                            unit.insert("ok".into(), serde_json::Value::Bool(true));
                            unit.insert("parked".into(), serde_json::Value::Bool(false));
                            unit.insert("resumed".into(), serde_json::Value::Bool(true));
                            unit.insert("overdue_ms".into(), serde_json::json!(overdue_ms));
                            serde_json::Value::Object(unit)
                        }
                        // Park: the intent is journaled BEFORE the wait, so a
                        // crash mid-park leaves a visible Parked row naming the
                        // re-drive key rather than a wait that vanished.
                        crate::engine::park::ParkDecision::Park {
                            until_ms,
                            capped_to_ms,
                        } => {
                            let intent = crate::engine::park::park_intent(
                                run_id,
                                node,
                                &decision,
                                reason.clone(),
                            )
                            .expect("a Park decision yields an intent");
                            if let Some(journal) = journal_park_intent(
                                ctx,
                                &intent,
                                workflow_digest,
                                attempt,
                                coordinator.max_attempts,
                            )
                            .await?
                            {
                                return Ok(journal);
                            }
                            // The wait itself is the one bounded blocking
                            // call, and it is bounded by max_wait_secs.
                            let wait_ms = until_ms.saturating_sub(now_ms);
                            ctx.sleep(Duration::from_millis(wait_ms)).await?;
                            let mut unit = serde_json::Map::new();
                            unit.insert("ok".into(), serde_json::Value::Bool(true));
                            // `parked` is true and `ok` is true TOGETHER, and
                            // that is the point: a park is not a failure and
                            // not a completed step. It is an interruption that
                            // resumed. Callers distinguish them by this flag,
                            // not by guessing from ok.
                            unit.insert("parked".into(), serde_json::Value::Bool(true));
                            unit.insert("resumed".into(), serde_json::Value::Bool(true));
                            unit.insert("until_ms".into(), serde_json::json!(until_ms));
                            unit.insert("redrive_key".into(), serde_json::json!(intent.key));
                            if let Some(c) = capped_to_ms {
                                unit.insert("capped".into(), serde_json::Value::Bool(true));
                                unit.insert("capped_to_ms".into(), serde_json::json!(c));
                            }
                            if let Some(r) = reason {
                                unit.insert("reason".into(), serde_json::json!(r));
                            }
                            serde_json::Value::Object(unit)
                        }
                        // Refused, never silently truncated: a truncated park
                        // resumes early and reads as a successful step.
                        crate::engine::park::ParkDecision::RefusedTooLong {
                            requested_secs,
                            max_secs,
                        } => {
                            return Err(resonate_sdk::error::Error::Application {
                                message: format!(
                                    "park: requested wait {requested_secs}s exceeds the hard cap \
                                     {max_secs}s — refused (bd-ynef). A park must be bounded; \
                                     shorten the requested until_ms or park in steps."
                                ),
                            });
                        }
                    }
                }
                Action::RequestApproval { message } => {
                    // Render FIRST (the same contract as the mailbox actions):
                    // the message becomes the promise payload + the operator's
                    // signoff prompt, so a raw `{{inputs.*}}` there is an
                    // unrendered template a captain actually reads
                    // (bd-adr0006-cosmetic-sweep-q0g0).
                    let message = rendered_approval_message(
                        &step.action,
                        trigger,
                        step_outputs,
                        workflow,
                        node,
                    )
                    .unwrap_or_else(|_| message.clone());
                    // Create the promise; its id is the approval key. The 0.6 SDK's
                    // PromiseTask is created eagerly (.create()); the handle's id()
                    // confirms server-side creation.
                    let pending = ctx
                        .promise::<Decision>()
                        .data(&serde_json::json!({ "message": message }))?
                        .create()?;
                    let promise_id = pending.id().await?;
                    // The binding: the embedding publishes kind-46011 with this
                    // promise_id; the operator's approval/rejection resolves it.
                    step_outputs.insert(
                        "approval".into(),
                        serde_json::json!({
                            "promise_id": promise_id,
                            "message": message,
                        }),
                    );
                    // Side-channel the promise_id (embedding publishes kind-46011).
                    ctx.get_dependency::<ApprovalSink>()
                        .lock()
                        .await
                        .push(promise_id.clone());
                    // Resolve OR rejection both complete the run — a rejected
                    // approval is a DECISION, never a run failure (the WF-08 fix:
                    // Buzz marks it failed; toron records the rejection).
                    //
                    // 0.6 semantics: awaiting a PENDING promise returns
                    // `Err(Error::Suspended)`, which must PROPAGATE so the runtime
                    // parks the run. A REJECTED promise (operator `promises.reject`)
                    // returns `Err(Error::Application)` — that is the decision.
                    let decision: Decision = match pending.await {
                        Ok(d) => d,
                        Err(resonate_sdk::error::Error::Suspended) => {
                            return Err(resonate_sdk::error::Error::Suspended);
                        }
                        Err(e) => Decision::rejected(format!("rejected: {e}")),
                    };
                    let decision = serde_json::to_value(&decision)?;
                    step_outputs.insert("decision".into(), decision.clone());
                    decision
                }
                // Every leaf that reaches an embedding dispatches through the
                // SAME effect surface (bd-zczp + bd-engine-run-path-journaling-
                // fwlx): `verify` belongs here now that it routes through the
                // injected EffectRunner like mailbox/job — the render, the
                // invocation identity, and the mode/refusal/marker contract are
                // identical, so a separate arm would only invite drift.
                Action::SendMessage { .. }
                | Action::SendDm { .. }
                | Action::React { .. }
                | Action::DispatchJob { .. }
                | Action::CallWebhook { .. }
                | Action::Verify { .. } => {
                    // Consistency rule: templates are rendered FIRST so the
                    // runner sees rendered values (a `verify: "{{inputs.x}}"`
                    // command must reach the sink resolved, not verbatim); a
                    // render failure (unresolved ref) is a per-step error with
                    // workflow/node/field/ref attribution, not a run failure.
                    let render_ctx = RenderContext::new(workflow, node);
                    match render_action(&step.action, trigger, step_outputs, &render_ctx) {
                        Ok(action) => {
                            let invocation = invocation_for_leaf(
                                action,
                                run_id,
                                workflow_digest,
                                node,
                                attempt,
                                enclosing_retry.map(|(m, _)| m).unwrap_or(1),
                                iteration_key,
                            );
                            dispatch_leaf_or_compat(ctx, &invocation).await
                        }
                        Err(e) => {
                            serde_json::json!({ "ok": false, "error": format!("render: {e}") })
                        }
                    }
                }
                Action::Retry {
                    max_attempts,
                    backoff_secs,
                    step: inner,
                } => {
                    run_retry(
                        ctx,
                        trigger,
                        inner,
                        *max_attempts,
                        *backoff_secs,
                        step_outputs,
                        workflow,
                        run_id,
                        workflow_digest,
                    )
                    .await?
                }
                Action::Branch {
                    if_guard,
                    then,
                    else_steps,
                } => {
                    run_branch(
                        ctx,
                        trigger,
                        if_guard,
                        then,
                        else_steps,
                        step_outputs,
                        workflow,
                        node,
                        run_id,
                        workflow_digest,
                    )
                    .await?
                }
                Action::Foreach { over, each } => {
                    run_foreach(
                        ctx,
                        trigger,
                        over,
                        each,
                        step_outputs,
                        workflow,
                        node,
                        run_id,
                        workflow_digest,
                    )
                    .await?
                }
                Action::Parallel { over, each } => {
                    run_parallel(
                        ctx,
                        trigger,
                        over,
                        each,
                        step_outputs,
                        workflow,
                        node,
                        run_id,
                        workflow_digest,
                    )
                    .await?
                }
                Action::Reduce { .. } => {
                    run_reduce(ctx, trigger, &step.action, step_outputs, workflow, node).await?
                }
                Action::CallWorkflow {
                    workflow: child,
                    inputs,
                } => run_call_workflow(ctx, workflow, child, inputs, step_outputs, trigger).await?,
                Action::Loop { max, until, body } => {
                    run_loop(
                        ctx,
                        trigger,
                        max,
                        until.as_deref(),
                        body,
                        step_outputs,
                        workflow,
                        node,
                        run_id,
                        workflow_digest,
                    )
                    .await?
                }
                Action::Spawn {
                    profile,
                    harness,
                    workflow,
                    await_,
                    no_claim_timeout,
                    context,
                } => {
                    run_spawn(
                        ctx,
                        profile,
                        harness.as_deref(),
                        workflow.as_deref(),
                        *await_,
                        *no_claim_timeout,
                        *context,
                    )
                    .await?
                }
            };
            // A dispatch that answered with a JSON object (or one fenced
            // block) publishes those fields on the step. Guards such as
            // `steps.measure.improved` can then see `improved`.
            promote_response_fields(&mut value);

            // P0.2 (ADR-0005 Spec 0 + ADR-0024 Spec 1): `on_error: fail_run`
            // converts a step failure into a RUN failure — a deterministic durable
            // RETURN, never a panic. ADR-0032 Spec 7: retry candidacy resolves
            // in the ONE coordinator (frozen FailureKind vocabulary); a
            // retryable runtime failure retries the SAME node, bounded.
            if step.on_error == StepOnError::FailRun && crate::engine::is_failure_value(&value) {
                let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                if RetryCoordinator::is_retryable_value(&value) {
                    if !coordinator.should_retry(attempt) {
                        let error = value
                            .get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("step failed")
                            .to_string();
                        let disposition = disposition_for_kind(
                            kind,
                            value.get("disposition").and_then(|v| v.as_str()),
                        );
                        return Ok(serde_json::json!({
                            "status": "failed",
                            "error": error,
                            "failed_step": step.name.clone().unwrap_or_else(|| "<unnamed>".into()),
                            "kind": kind,
                            "disposition": disposition,
                            "leaf_preview": leaf_preview_for_value(&value),
                            "attempts": attempt,
                        }));
                    }
                    let retry_after_ms = value
                        .get("retry_after_ms")
                        .and_then(serde_json::Value::as_u64);
                    ctx.sleep(coordinator.backoff(attempt, retry_after_ms))
                        .await?;
                    continue;
                }
                let error = value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("step failed")
                    .to_string();
                let mut failed = serde_json::json!({
                    "status": "failed",
                    "error": error,
                    "failed_step": step.name.clone().unwrap_or_else(|| "<unnamed>".into()),
                });
                if let Some(k) = value.get("kind") {
                    failed["kind"] = k.clone();
                }
                if let Some(d) = value.get("disposition") {
                    failed["disposition"] = d.clone();
                } else if !kind.is_empty() {
                    failed["disposition"] = serde_json::json!(disposition_for_kind(kind, None));
                }
                failed["leaf_preview"] = serde_json::json!(leaf_preview_for_value(&value));
                return Ok(failed);
            }
            return Ok(value);
        }
        // unreachable — loop always returns
    })
}

/// Run a `retry` action: attempt the inner step up to `max_attempts` times,
/// waiting `backoff_secs` between failures. Returns the inner step's output;
/// if all attempts fail, returns `{ok:false,error}` (the on_error policy at
/// the OUTER step decides run fate).
///
/// ADR-0032 Spec 7: this loop is a client of the ONE retry coordinator.
/// A `Retry` nested directly inside another is FLATTENED at runtime — the
/// outermost policy owns the whole subtree, so bounds never multiply
/// (lint rejects new nests statically; this covers old payloads).
// Threaded dispatch context (ctx/runner/trigger/target/policy/attrs) exceeds
// the default arity cap; bundling would churn every call site for no gain.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_retry(
    ctx: &Context,
    trigger: &serde_json::Value,
    inner: &Step,
    max_attempts: u32,
    backoff_secs: u64,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // Flatten nested wrappers: the outer policy wins for the whole subtree.
    // The inner step is run as Continue so its own fail_run does not turn
    // the first miss into a run-level `{status:failed}` before this loop
    // has spent `max_attempts`. The outer step's on_error still decides
    // what exhaustion means for the run.
    let mut owned = inner.clone();
    while let Action::Retry { step: nested, .. } = &owned.action {
        owned = (**nested).clone();
    }
    owned.on_error = crate::schema::StepOnError::Continue;
    let coordinator = RetryCoordinator::from_retry_action(max_attempts, backoff_secs);
    let mut last_value: Option<serde_json::Value> = None;
    for attempt in 1..=max_attempts.max(1) {
        // Attempts resolve against the enclosing scope.
        let mut outputs = step_outputs.clone();
        let value = run_step_with_enclosing(
            ctx,
            trigger,
            &owned,
            &mut outputs,
            Some((max_attempts, backoff_secs)),
            workflow,
            run_id,
            workflow_digest,
            None,
        )
        .await?;
        let failed = crate::engine::is_failure_value(&value);
        if !failed {
            return Ok(value);
        }
        // Suspension is an Err and never reaches here. A terminal
        // `{status:failed}` from a nested control-flow action is one
        // attempt, not an excuse to skip the remaining attempts.
        last_value = Some(value);
        if coordinator.should_retry(attempt) && backoff_secs > 0 {
            ctx.sleep(Duration::from_secs(backoff_secs)).await?;
        }
    }
    let last = last_value
        .unwrap_or_else(|| serde_json::json!({ "ok": false, "error": "retry exhausted" }));
    // Preserve kind/disposition if present, else generic
    let mut out = serde_json::json!({
        "ok": false,
        "error": last.get("error").and_then(|v| v.as_str()).unwrap_or("retry exhausted"),
        "retried": true,
    });
    if let Some(k) = last.get("kind") {
        out["kind"] = k.clone();
    }
    if let Some(d) = last.get("disposition") {
        out["disposition"] = d.clone();
    }
    if let Some(ms) = last.get("retry_after_ms") {
        out["retry_after_ms"] = ms.clone();
    }
    Ok(out)
}

/// Build the guard variable map shared by Branch `if:` guards and graph-node
/// `guard:` fields (bd-adr0008-guard-fix-16fl): every trigger-payload key
/// becomes `trigger_<key>` (the invoke path flattens caller inputs onto the
/// trigger, so an input-gated intent is spelled `trigger_x` in guards — raw
/// `{{inputs.*}}` templates do NOT resolve in guard position), and every
/// recorded step output becomes `steps_<name>[_<key>]`. Bool JSON and the
/// strings `true`/`false` become booleans. Every other value stays text.
/// Bool JSON and the strings `true`/`false` become evalexpr booleans so
/// `steps_verify_ok == true` and `trigger_gate_before_implement == "true"`
/// are the same comparison. Every other value stays text.
fn guard_eval_value(v: &serde_json::Value) -> evalexpr::Value {
    match v {
        serde_json::Value::Bool(b) => evalexpr::Value::Boolean(*b),
        serde_json::Value::String(s) if s == "true" => evalexpr::Value::Boolean(true),
        serde_json::Value::String(s) if s == "false" => evalexpr::Value::Boolean(false),
        serde_json::Value::String(s) => evalexpr::Value::from(s.clone()),
        other => evalexpr::Value::from(other.to_string()),
    }
}

pub(crate) fn guard_vars(
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> std::collections::HashMap<String, evalexpr::Value> {
    let mut vars = std::collections::HashMap::new();
    if let Some(obj) = trigger.as_object() {
        for (k, v) in obj {
            let name = format!("trigger_{k}");
            vars.insert(name, guard_eval_value(v));
        }
    }
    for (step_name, output) in step_outputs {
        if let Some(obj) = output.as_object() {
            for (k, v) in obj {
                let name = format!(
                    "steps_{}_{}",
                    step_name.replace('-', "_"),
                    k.replace('-', "_")
                );
                vars.insert(name, guard_eval_value(v));
            }
        } else {
            let name = format!("steps_{}", step_name.replace('-', "_"));
            vars.insert(name, guard_eval_value(output));
        }
    }
    vars
}

/// ADR-0032 Spec 4: ONE guard/ref evaluator for graph nodes,
/// linear-desugared steps, branch guards, and nested steps.
///
/// - `false` is an explicit skip (never a silent Null, never an else path);
/// - syntax/type/unresolved-ref/timeout/complexity errors FAIL LOUD as an
///   action/contract failure (never false, never else, never placeholder).
pub(crate) enum GuardVerdict {
    Proceed,
    Skip { reason: String },
    Fail { error: String },
}

pub(crate) async fn evaluate_step_guard(
    guard: Option<&str>,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    node: &str,
) -> GuardVerdict {
    let Some(g) = guard.filter(|g| !g.trim().is_empty()) else {
        return GuardVerdict::Proceed;
    };
    // Dotted paths (`steps.gate.ok`, `inputs.mode`) and quoted bools
    // (`== "true"`) are the spellings compose and the playbooks use.
    // The binder stores flat names and real booleans.
    let flattened = crate::guard::normalize_guard_paths(g);
    let rewritten = crate::guard::rewrite_bool_string_literals(&flattened);
    let vars = guard_vars(trigger, step_outputs);
    match crate::guard::evaluate_guard_default(Some(&rewritten), &vars).await {
        crate::guard::GuardOutcome::Bool(true) => GuardVerdict::Proceed,
        crate::guard::GuardOutcome::Unset => GuardVerdict::Proceed,
        crate::guard::GuardOutcome::Bool(false) => GuardVerdict::Skip {
            reason: format!("guard false on node {node:?}"),
        },
        // Every non-boolean outcome is a loud contract failure (I10).
        crate::guard::GuardOutcome::TypeError { got } => GuardVerdict::Fail {
            error: format!("guard {g:?} on node {node:?} evaluated to non-boolean ({got})"),
        },
        crate::guard::GuardOutcome::EvalError { err } => GuardVerdict::Fail {
            error: format!("guard {g:?} on node {node:?} failed to evaluate: {err}"),
        },
        crate::guard::GuardOutcome::TimedOut { after_ms } => GuardVerdict::Fail {
            error: format!("guard {g:?} on node {node:?} timed out after {after_ms}ms"),
        },
        crate::guard::GuardOutcome::TooComplex { reason } => GuardVerdict::Fail {
            error: format!("guard {g:?} on node {node:?} rejected as too complex: {reason}"),
        },
    }
}

/// The explicit skipped-step value (ADR-0032 Spec 4): a false guard names
/// the node and reason instead of vanishing into Null.
pub(crate) fn skipped_value(node: &str, reason: String) -> serde_json::Value {
    serde_json::json!({ "skipped": true, "node": node, "reason": reason })
}

/// Run a `branch` action: evaluate the guard through the ONE evaluator; run
/// the `then` or `else` steps. A guard error FAILS the step loudly — it
/// never silently takes `else` (ADR-0032 Spec 4).
// Threaded dispatch context exceeds the default arity cap; see run_retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_branch(
    ctx: &Context,
    trigger: &serde_json::Value,
    guard: &str,
    then: &[Step],
    else_steps: &[Step],
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let take_then = match evaluate_step_guard(Some(guard), trigger, step_outputs, node).await {
        GuardVerdict::Proceed => true,
        GuardVerdict::Skip { .. } => false,
        GuardVerdict::Fail { error } => {
            return Ok(serde_json::json!({ "ok": false, "error": error }));
        }
    };
    let branch_steps = if take_then { then } else { else_steps };
    // Nested bodies resolve against the enclosing scope (same `steps.*`
    // visibility as top-level steps); only newly produced outputs return.
    let known: std::collections::BTreeSet<String> = step_outputs.keys().cloned().collect();
    let mut outputs = step_outputs.clone();
    if let Some(failed) = run_steps(
        ctx,
        trigger,
        branch_steps,
        &mut outputs,
        workflow,
        run_id,
        workflow_digest,
    )
    .await?
    {
        return Ok(failed);
    }
    outputs.retain(|k, _| !known.contains(k));
    Ok(serde_json::json!({
        "branch": if take_then { "then" } else { "else" },
        "outputs": serde_json::Value::Object(outputs),
    }))
}

/// Run a `foreach` action: resolve `over` to a list; run `each` once per
/// item; collect the sub-run outputs into a list.
///
/// ADR-0032 Spec 3 (I10): `over` MUST resolve to a proper list. A missing
/// ref or a non-list value FAILS LOUD — never coerces to `[]`.
// Threaded dispatch context exceeds the default arity cap; see run_retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_foreach(
    ctx: &Context,
    trigger: &serde_json::Value,
    over: &str,
    each: &Step,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let resolved = resolve_template_ref(over, trigger, step_outputs);
    let Some(value) = resolved else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: foreach `over` {over:?} does not resolve to a value"),
        }));
    };
    let Some(list) = value.as_array() else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: foreach `over` {over:?} must be a list"),
        }));
    };
    let mut collected = Vec::new();
    for item in list {
        // Each iteration sees the current item as `{{item}}` (ADR-0007 F4):
        // inject it into the iteration's trigger copy so the template
        // renderer can resolve it.
        let mut iter_trigger = trigger.as_object().map_or_else(
            || serde_json::Value::Object(serde_json::Map::new()),
            |obj| serde_json::Value::Object(obj.clone()),
        );
        iter_trigger["item"] = item.clone();
        // Iterations resolve against the enclosing scope plus the item.
        let mut item_outputs = step_outputs.clone();
        let value = run_step(
            ctx,
            &iter_trigger,
            each,
            &mut item_outputs,
            workflow,
            run_id,
            workflow_digest,
        )
        .await?;
        collected.push(value);
    }
    if let Some(failed) = collected
        .iter()
        .find(|v| crate::engine::is_failure_value(v))
    {
        let error = failed
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("foreach item failed");
        return Ok(serde_json::json!({
            "ok": false,
            "error": error,
            "results": collected,
        }));
    }
    Ok(serde_json::Value::Array(collected))
}

/// Elastick fan-out cap (ADR-0032 Spec 3): `parallel` concurrency is bounded
/// by explicit policy. Counts above this fail loud instead of fanning out.
pub(crate) const MAX_PARALLEL_FANOUT: usize = 32;

/// Run a `parallel` action: fan `each` out across N elastic copies
/// (`over: "<N>"`); collected into a list.
///
/// ADR-0032 Spec 3 (I10): `over` MUST be a valid positive bounded count. A
/// crew role (`@crew.r`) needs the orchestrator to concretize it before
/// invoke; anything else FAILS LOUD — never coerces to 1.
// Threaded dispatch context (run identity for leaf invocations) exceeds the
// default arity cap; see run_retry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_parallel(
    ctx: &Context,
    trigger: &serde_json::Value,
    over: &str,
    each: &Step,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // Elastic N: `over: "4"`, `over: "{{inputs.n}}"`, or a number already
    // rendered by the scheduler. `@crew.role` stays unresolved here. The
    // orchestrator rewrites that to a count before invoke.
    let parsed = parallel_fanout_count(over, trigger, step_outputs);
    let Some(n) = parsed else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: parallel `over` {over:?} must be a positive count (a crew role needs orchestrator concretization first)"),
        }));
    };
    if n > MAX_PARALLEL_FANOUT {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: parallel `over` {n} exceeds the bounded fan-out cap {MAX_PARALLEL_FANOUT}"),
        }));
    }
    let mut futures = Vec::new();
    for i in 0..n {
        let each = each.clone();
        let trigger = trigger.clone();
        let workflow = workflow.to_string();
        let future = ctx
            .run(
                run_parallel_copy,
                ParallelCopyInput {
                    step: each,
                    trigger,
                    workflow,
                    run_id: run_id.to_string(),
                    workflow_digest: workflow_digest.to_string(),
                    // Prior steps are visible inside each copy. The copy
                    // index keeps effect keys distinct so N copies are N
                    // effects, not one journal row replayed N times.
                    prior_outputs: step_outputs.clone(),
                    iteration_key: Some(format!("p{i}")),
                },
            )
            .spawn()?;
        futures.push(future);
    }
    let mut collected = Vec::new();
    for future in futures {
        collected.push(future.await?);
    }
    if let Some(failed) = collected
        .iter()
        .find(|v| crate::engine::is_failure_value(v))
    {
        let error = failed
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("parallel copy failed");
        return Ok(serde_json::json!({
            "ok": false,
            "error": error,
            "results": collected,
        }));
    }
    Ok(serde_json::Value::Array(collected))
}

/// The input for one parallel copy (a durable child).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelCopyInput {
    pub step: Step,
    pub trigger: serde_json::Value,
    /// Workflow name for render/guard error attribution (`#[serde(default)]`
    /// keeps in-flight serialized inputs readable).
    #[serde(default)]
    pub workflow: String,
    /// Run identity for context-bearing leaf dispatch (bd-zczp).
    #[serde(default)]
    pub run_id: String,
    /// Canonical workflow digest for run-scoped effect keys (bd-zczp).
    #[serde(default)]
    pub workflow_digest: String,
    /// Outputs from steps that finished before this fan-out. Empty on
    /// replay of a payload written before the field existed.
    #[serde(default)]
    pub prior_outputs: serde_json::Map<String, serde_json::Value>,
    /// Distinguishes this copy's effect key from its siblings.
    #[serde(default)]
    pub iteration_key: Option<String>,
}

/// A durable child running one `parallel` copy of a step. The runner is
/// re-fetched from the runtime dependency (the consistency rule).
#[resonate_sdk::function]
pub async fn run_parallel_copy(
    ctx: &Context,
    input: ParallelCopyInput,
) -> resonate_sdk::Result<serde_json::Value> {
    let mut outputs = input.prior_outputs.clone();
    let value = run_step_with_enclosing(
        ctx,
        &input.trigger,
        &input.step,
        &mut outputs,
        None,
        &input.workflow,
        &input.run_id,
        &input.workflow_digest,
        input.iteration_key.as_deref(),
    )
    .await?;
    Ok(value)
}

/// Bounded reduce cap (bd-3pti): inputs longer than this fail loud instead
/// of folding unboundedly.
pub(crate) const MAX_REDUCE_ITEMS: usize = 1024;

/// Join-output cap (bd-3pti): the accumulated string never grows past this;
/// longer folds fail loud instead of materializing unbounded text.
pub(crate) const MAX_JOIN_LEN: usize = 65536;

/// Run a `reduce` action: resolve `over` to a proper list and fold it in
/// authored order into a single scalar (bd-3pti). Pure in-engine work.
pub(crate) async fn run_reduce(
    _ctx: &Context,
    trigger: &serde_json::Value,
    action: &Action,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let Action::Reduce {
        over,
        op,
        key,
        template,
        separator,
        init,
    } = action
    else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: reduce dispatch reached a non-reduce action"),
        }));
    };
    let ctx_name = format!("workflow {workflow:?} node {node:?}");
    // Join takes its operand from the template, never from a key.
    if *op == crate::schema::ReduceOp::Join && key.is_some() {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("{ctx_name}: join uses `template`, not `key`"),
        }));
    }
    // Resolve the collection through the canonical ref path: the rendered
    // `over` must parse to a proper JSON list. Anything else — missing ref,
    // scalar, object — fails the list contract loudly, never a silent fold.
    let resolved = resolve_template_ref(over, trigger, step_outputs);
    let list = match resolved.as_ref().and_then(|v| v.as_array()) {
        Some(list) => list.clone(),
        None => {
            return Ok(serde_json::json!({
                "ok": false,
                "error": format!("{ctx_name}: reduce `over` {over:?} must resolve to a proper list"),
            }));
        }
    };
    if list.len() > MAX_REDUCE_ITEMS {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("{ctx_name}: reduce input has {} items, over the {MAX_REDUCE_ITEMS} bound", list.len()),
        }));
    }
    // Operand selection: the key dot-path into each item, or the item
    // itself. A missing path on any item fails naming the key and index.
    let operand = |item: &serde_json::Value, index: usize| -> Result<serde_json::Value, String> {
        key.as_ref().map_or_else(
            || Ok(item.clone()),
            |path| {
                super::template::descend(item, path.split('.'))
                    .cloned()
                    .ok_or_else(|| {
                        format!("{ctx_name}: reduce key {path:?} missing on item {index}")
                    })
            },
        )
    };
    // Initial accumulator: op-typed literal (templates are rejected
    // statically), or the op identity when absent.
    let init_value = init.clone();
    match op {
        crate::schema::ReduceOp::All => {
            let mut acc = match init_value {
                Some(serde_json::Value::Bool(b)) => b,
                Some(other) => {
                    return Ok(serde_json::json!({
                        "ok": false,
                        "error": format!("{ctx_name}: all `init` must be a boolean, got {other}"),
                    }));
                }
                None => true,
            };
            for (i, item) in list.iter().enumerate() {
                let v = operand(item, i);
                match v {
                    Ok(v) => acc = acc && is_truthy(&v),
                    Err(e) => {
                        return Ok(serde_json::json!({ "ok": false, "error": e }));
                    }
                }
            }
            Ok(serde_json::Value::Bool(acc))
        }
        crate::schema::ReduceOp::Any => {
            let mut acc = match init_value {
                Some(serde_json::Value::Bool(b)) => b,
                Some(other) => {
                    return Ok(serde_json::json!({
                        "ok": false,
                        "error": format!("{ctx_name}: any `init` must be a boolean, got {other}"),
                    }));
                }
                None => false,
            };
            for (i, item) in list.iter().enumerate() {
                let v = operand(item, i);
                match v {
                    Ok(v) => acc = acc || is_truthy(&v),
                    Err(e) => {
                        return Ok(serde_json::json!({ "ok": false, "error": e }));
                    }
                }
            }
            Ok(serde_json::Value::Bool(acc))
        }
        crate::schema::ReduceOp::Count => {
            let mut acc = match init_value {
                Some(serde_json::Value::Number(n)) => match n.as_i64() {
                    Some(i) => i,
                    None => {
                        return Ok(serde_json::json!({
                            "ok": false,
                            "error": format!("{ctx_name}: count `init` must be an integer"),
                        }));
                    }
                },
                Some(other) => {
                    return Ok(serde_json::json!({
                        "ok": false,
                        "error": format!("{ctx_name}: count `init` must be a number, got {other}"),
                    }));
                }
                None => 0,
            };
            for (i, item) in list.iter().enumerate() {
                let v = operand(item, i);
                match v {
                    Ok(v) => {
                        if is_truthy(&v) {
                            acc += 1;
                        }
                    }
                    Err(e) => {
                        return Ok(serde_json::json!({ "ok": false, "error": e }));
                    }
                }
            }
            Ok(serde_json::json!(acc))
        }
        crate::schema::ReduceOp::Sum => {
            // Integer path when every operand and init are i64; otherwise
            // f64. Integers stay integers in the output.
            let mut acc_i: Option<i64> = match init_value {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        Some(i)
                    } else if n.as_f64().is_some() {
                        None
                    } else {
                        return Ok(serde_json::json!({
                            "ok": false,
                            "error": format!("{ctx_name}: sum `init` must be a number"),
                        }));
                    }
                }
                Some(other) => {
                    return Ok(serde_json::json!({
                        "ok": false,
                        "error": format!("{ctx_name}: sum `init` must be a number, got {other}"),
                    }));
                }
                None => Some(0),
            };
            let mut acc_f = acc_i.map(|i| i as f64).unwrap_or(0.0);
            let mut floating = acc_i.is_none();
            for (i, item) in list.iter().enumerate() {
                let v = operand(item, i);
                let v = match v {
                    Ok(v) => v,
                    Err(e) => {
                        return Ok(serde_json::json!({ "ok": false, "error": e }));
                    }
                };
                match v {
                    serde_json::Value::Number(n) => {
                        if let (Some(a), Some(b)) = (acc_i, n.as_i64()) {
                            acc_i = a.checked_add(b);
                            if acc_i.is_none() {
                                return Ok(serde_json::json!({
                                    "ok": false,
                                    "error": format!("{ctx_name}: sum overflow at item {i}"),
                                }));
                            }
                            acc_f = acc_i.unwrap_or(0) as f64;
                        } else if let Some(f) = n.as_f64() {
                            floating = true;
                            acc_i = None;
                            acc_f += f;
                        } else {
                            return Ok(serde_json::json!({
                                "ok": false,
                                "error": format!("{ctx_name}: sum operand at item {i} is not a number"),
                            }));
                        }
                    }
                    other => {
                        return Ok(serde_json::json!({
                            "ok": false,
                            "error": format!("{ctx_name}: sum operand at item {i} must be a number, got {other}"),
                        }));
                    }
                }
            }
            if floating {
                Ok(serde_json::json!(acc_f))
            } else {
                Ok(serde_json::json!(acc_i.unwrap_or(0)))
            }
        }
        crate::schema::ReduceOp::Join => {
            let Some(tpl) = template else {
                return Ok(serde_json::json!({
                    "ok": false,
                    "error": format!("{ctx_name}: join requires a `template`"),
                }));
            };
            let mut acc = match init_value {
                Some(serde_json::Value::String(s)) => s,
                Some(other) => {
                    return Ok(serde_json::json!({
                        "ok": false,
                        "error": format!("{ctx_name}: join `init` must be a string, got {other}"),
                    }));
                }
                None => String::new(),
            };
            let sep = separator.as_deref().unwrap_or("");
            let mut first = acc.is_empty();
            for (i, item) in list.iter().enumerate() {
                // Each item renders with itself as `{{item}}` (same deferred
                // context as foreach); missing refs fail naming item index.
                let mut iter_trigger = trigger.as_object().map_or_else(
                    || serde_json::Value::Object(serde_json::Map::new()),
                    |obj| serde_json::Value::Object(obj.clone()),
                );
                iter_trigger["item"] = item.clone();
                iter_trigger["index"] = serde_json::json!(i);
                match render_text(tpl, &iter_trigger, step_outputs) {
                    Ok(rendered) => {
                        if !first {
                            acc.push_str(sep);
                        }
                        first = false;
                        acc.push_str(&rendered);
                        if acc.len() > MAX_JOIN_LEN {
                            return Ok(serde_json::json!({
                                "ok": false,
                                "error": format!("{ctx_name}: join output over the {MAX_JOIN_LEN} bound at item {i}"),
                            }));
                        }
                    }
                    Err(token) => {
                        return Ok(serde_json::json!({
                            "ok": false,
                            "error": format!("{ctx_name}: join template ref {{{{{token}}}}} does not resolve on item {i}"),
                        }));
                    }
                }
            }
            Ok(serde_json::Value::String(acc))
        }
    }
}

/// JSON truthiness for reduce folds: `Null`/`false`/`0`/`""`/`[]`/`{}`
/// are falsy; everything else is truthy.
pub(crate) fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
        // Null is the only variant not listed above, and it is falsy.
        _ => false,
    }
}

/// Reserved trigger key carrying the durable workflow lineage envelope.
/// The envelope is deliberately part of `trigger`, rather than `Context`, so
/// Resonate serializes it with the child input and replay sees the same guard.
const WORKFLOW_LINEAGE_KEY: &str = "__flywheel_workflow_lineage";
/// Maximum call depth, including the root workflow.
pub const MAX_WORKFLOW_DEPTH: usize = 8;
/// Maximum child calls along one durable lineage branch.
pub const MAX_WORKFLOW_CHILDREN: u32 = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowLineage {
    pub(crate) chain: Vec<String>,
    pub(crate) remaining_children: u32,
}

pub(crate) fn lineage_for_trigger(
    trigger: &serde_json::Value,
    workflow: &str,
) -> std::result::Result<WorkflowLineage, String> {
    let Some(raw) = trigger.get(WORKFLOW_LINEAGE_KEY) else {
        return Ok(WorkflowLineage {
            chain: vec![workflow.to_string()],
            remaining_children: MAX_WORKFLOW_CHILDREN,
        });
    };
    let lineage: WorkflowLineage = serde_json::from_value(raw.clone())
        .map_err(|e| format!("invalid workflow lineage for {workflow:?}: {e}"))?;
    if lineage.chain.last().map(String::as_str) != Some(workflow) {
        return Err(format!(
            "workflow lineage tail {:?} does not match running workflow {workflow:?}",
            lineage.chain.last()
        ));
    }
    if lineage.chain.len() > MAX_WORKFLOW_DEPTH {
        return Err(format!(
            "workflow nesting depth {} exceeds the bound {MAX_WORKFLOW_DEPTH}",
            lineage.chain.len()
        ));
    }
    if lineage
        .chain
        .iter()
        .enumerate()
        .any(|(index, name)| lineage.chain[..index].contains(name))
    {
        return Err(format!(
            "workflow lineage contains a recursive edge at {workflow:?}"
        ));
    }
    Ok(lineage)
}

fn validate_workflow_lineage(
    trigger: &serde_json::Value,
    workflow: &str,
) -> std::result::Result<(), String> {
    lineage_for_trigger(trigger, workflow).map(|_| ())
}

/// Run a `call_workflow` action: invoke another workflow as a sub-run.
/// The host rejects cycles, excessive nesting, and exhausted branch budget
/// before `ctx.run` creates a durable child. Outputs flow into `steps.<name>`.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_call_workflow(
    ctx: &Context,
    parent_workflow: &str,
    child_name: &str,
    inputs: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
    trigger: &serde_json::Value,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // ADR-0007 R4.1: nested durable run via WorkflowResolver DI + ctx.run.
    let resolver = ctx.get_dependency::<WorkflowResolver>();
    let Some(child_wf) = (resolver)(child_name) else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("unknown workflow: {child_name}"),
        }));
    };
    let lineage = match lineage_for_trigger(trigger, parent_workflow) {
        Ok(value) => value,
        Err(error) => return Ok(serde_json::json!({ "ok": false, "error": error })),
    };
    if lineage.chain.iter().any(|name| name == child_name) {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!(
                "recursive workflow call refused: {parent_workflow:?} -> {child_name:?}; call chain: {:?}",
                lineage.chain
            ),
        }));
    }
    if lineage.chain.len() >= MAX_WORKFLOW_DEPTH {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!(
                "workflow child {child_name:?} refused: nesting depth bound {MAX_WORKFLOW_DEPTH} reached"
            ),
        }));
    }
    if lineage.remaining_children == 0 {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!(
                "workflow child {child_name:?} refused: child budget exhausted (bound {MAX_WORKFLOW_CHILDREN})"
            ),
        }));
    }
    // ADR-0032 Spec 3: bind declared defaults exactly like direct invoke.
    // Parent inputs flow into the child. Explicit call inputs win key by
    // key, so `workflow:bug-fix` keeps symptom/verify from the compose run
    // and still overrides `goal` with the objective string.
    let provided = merge_parent_inputs(trigger, inputs);
    let merged = WorkflowEngine::bind_input_defaults(&child_wf.inputs, &provided);
    let mut trigger_map = serde_json::Map::new();
    for (k, v) in &merged {
        trigger_map.insert(k.clone(), v.clone());
    }
    trigger_map.insert("inputs".into(), serde_json::Value::Object(merged.clone()));
    trigger_map.insert(
        "parent_steps".into(),
        serde_json::Value::Object(step_outputs.clone()),
    );
    let mut child_lineage = lineage;
    child_lineage.chain.push(child_name.to_string());
    child_lineage.remaining_children -= 1;
    trigger_map.insert(
        WORKFLOW_LINEAGE_KEY.into(),
        serde_json::to_value(child_lineage).expect("workflow lineage serializes"),
    );
    let input = RunInput {
        workflow: child_wf,
        trigger: serde_json::Value::Object(trigger_map),
        run_id: None,
    };
    let future = ctx.run(run_workflow, input).spawn()?;
    future.await
}

/// Parent `trigger.inputs` plus the call's explicit object. Explicit keys win.
fn merge_parent_inputs(
    trigger: &serde_json::Value,
    explicit: &serde_json::Value,
) -> serde_json::Value {
    let mut merged = serde_json::Map::new();
    if let Some(parent) = trigger.get("inputs").and_then(serde_json::Value::as_object) {
        for (k, v) in parent {
            merged.insert(k.clone(), v.clone());
        }
    }
    if let Some(obj) = explicit.as_object() {
        for (k, v) in obj {
            merged.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(merged)
}

/// Run a `spawn` action (ADR-0005 Spec 4a).
///
/// 1. Resolve `profile` via the injected `ProfileResolver` DI seam.
/// 2. Create a promise; its id is the job's promise↔job binding key. The
///    embedding emits kind-43001 carrying the promise_id in its `spec`.
/// 3. `await: true` → PARK on the promise until a terminal job event
///    (43004/43006) resolves it — a FAILURE, not a decision. `no_claim_timeout`
///    rejects an unclaimed job with `timeout` → the step fails → Spec 0
///    propagates.
///
/// 0.6 semantics: awaiting a pending promise returns `Err(Error::Suspended)`,
/// which must PROPAGATE so the runtime parks the run.
pub(crate) async fn run_spawn(
    ctx: &Context,
    profile: &str,
    harness: Option<&str>,
    workflow: Option<&str>,
    await_: bool,
    no_claim_timeout: u64,
    context: Option<crate::schema::ContextMode>,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // 1. Resolve the profile (DI seam).
    let resolver = ctx.get_dependency::<ProfileResolver>();
    let resolved = match (resolver)(profile) {
        Some(v) => v,
        None => {
            return Ok(serde_json::json!({
                "ok": false,
                "error": format!("spawn: unknown profile {profile:?} (ProfileResolver returned None)"),
            }));
        }
    };

    // 2. Create the promise — the job's binding key.
    let mut spec = serde_json::json!({
        "profile": profile,
        "workflow": workflow,
        "resolved": resolved,
        // bd-omwg: the embedding's drainer must know whether the run parks on
        // this job (resolve on completion) or fired it detached (fire-and-
        // forget, never resolve).
        "await": await_,
    });
    if let Some(h) = harness.map(str::trim).filter(|h| !h.is_empty()) {
        spec["harness"] = serde_json::json!(h);
    }
    if let Some(ctx_mode) = context {
        spec["context"] =
            serde_json::to_value(ctx_mode).unwrap_or(serde_json::Value::String("inherit".into()));
    }
    let task = ctx.promise::<serde_json::Value>().data(&spec)?;
    let task = if await_ && no_claim_timeout > 0 {
        task.timeout(Duration::from_secs(no_claim_timeout))
    } else {
        task
    };
    let pending = task.create()?;
    let promise_id = pending.id().await?;

    // Side-channel the promise_id (embedding emits kind-43001 with it in spec).
    ctx.get_dependency::<SpawnSink>()
        .lock()
        .await
        .push(promise_id.clone());

    // 3. No await → fire the job and return immediately (no park).
    if !await_ {
        return Ok(serde_json::json!({
            "ok": true,
            "promise_id": promise_id,
            "spawned": true,
        }));
    }

    match pending.await {
        Ok(value) => Ok(serde_json::json!({
            "ok": true,
            "promise_id": promise_id,
            "result": value,
        })),
        Err(resonate_sdk::error::Error::Suspended) => Err(resonate_sdk::error::Error::Suspended),
        Err(e) => {
            let msg = format!("spawn-await {promise_id}: {e}");
            Ok(serde_json::json!({ "ok": false, "error": msg }))
        }
    }
}

/// Resolve a template reference (`{{steps.x}}` or a literal path) to a JSON
/// value from the current outputs/trigger.
pub(crate) fn resolve_template_ref(
    token: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let trimmed = token.trim();
    if let Some(inner) = trimmed
        .strip_prefix("{{")
        .and_then(|s| s.strip_suffix("}}"))
    {
        return super::template::resolve_template(inner, trigger, step_outputs)
            .and_then(|s| serde_json::from_str(&s).ok())
            .or_else(|| {
                super::template::resolve_template(inner, trigger, step_outputs)
                    .map(serde_json::Value::String)
            });
    }
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return Some(value);
    }
    // `@items` and a bare name are the compose spelling of "the list called
    // items". JSON literals and `{{...}}` are handled above.
    let name = trimmed.trim_start_matches('@');
    if name.is_empty() || name == trimmed && trimmed.contains(' ') {
        return None;
    }
    if let Some(v) = trigger.get("inputs").and_then(|i| i.get(name)) {
        return Some(v.clone());
    }
    if let Some(v) = trigger.get(name) {
        return Some(v.clone());
    }
    step_outputs.get(name).cloned()
}

/// Cap on `loop` iterations. A template that renders above this fails loud.
pub(crate) const MAX_LOOP_ITERS: u32 = 32;

/// Run a `loop` action. `until` true stops as success. A fail_run body step
/// aborts the loop with that failure. Exhaustion is `{ok:false}` so the
/// loop step's own on_error decides the run. A recorded stub (dry run,
/// `{action: ...}` and no `ok`) is one preview pass, not a fake verify.
// bd-x4qw: the identity quadruple (workflow/node/run_id/digest) is one concept
// passed as four strings, matching the 7 sibling fns in this file that carry the
// same allow. A struct on 2 of 11 signatures would be the alien, not the fix.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_loop(
    ctx: &Context,
    trigger: &serde_json::Value,
    max: &str,
    until: Option<&str>,
    body: &[Step],
    step_outputs: &mut serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    node: &str,
    run_id: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let rendered_max = resolve_template_ref(max, trigger, step_outputs)
        .and_then(|v| match v {
            serde_json::Value::Number(n) => n.as_u64(),
            serde_json::Value::String(s) => s.trim().parse().ok(),
            _ => None,
        })
        .or_else(|| max.trim().parse().ok());
    let Some(max_n) = rendered_max.filter(|n| *n >= 1) else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: loop max {max:?} must render to a positive integer"),
        }));
    };
    if max_n > u64::from(MAX_LOOP_ITERS) {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("workflow {workflow:?} node {node:?}: loop max {max_n} exceeds the cap {MAX_LOOP_ITERS}"),
        }));
    }
    let max_n = max_n as u32;
    for i in 1..=max_n {
        let iteration = format!("i{i}");
        if let Some(failed) = run_steps_iter(
            ctx,
            trigger,
            body,
            step_outputs,
            workflow,
            run_id,
            workflow_digest,
            Some(&iteration),
        )
        .await?
        {
            return Ok(failed);
        }
        let Some(until_expr) = until.filter(|g| !g.trim().is_empty()) else {
            continue;
        };
        match evaluate_step_guard(Some(until_expr), trigger, step_outputs, node).await {
            GuardVerdict::Proceed => {
                return Ok(serde_json::json!({
                    "ok": true,
                    "iterations": i,
                    "stopped": "until",
                }));
            }
            GuardVerdict::Skip { .. } => {}
            GuardVerdict::Fail { error } => {
                if recorded_preview(step_outputs) {
                    return Ok(serde_json::json!({
                        "ok": true,
                        "recorded": true,
                        "iterations": i,
                    }));
                }
                // The attempt did not publish a field the guard names.
                // That is "not yet", so the loop spends another pass.
                // A syntax or type error still stops on this pass.
                if error.contains("Variable identifier is not bound to anything by context") {
                    continue;
                }
                return Ok(serde_json::json!({ "ok": false, "error": error, "iterations": i }));
            }
        }
    }
    if until.is_some() {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("loop exhausted after {max_n} iterations without until"),
            "iterations": max_n,
        }));
    }
    Ok(serde_json::json!({ "ok": true, "iterations": max_n }))
}

/// A dry-run leaf is `{action: "..."}` with no verdict. That is a preview,
/// not a passed verify and not a failed one.
fn recorded_preview(outputs: &serde_json::Map<String, serde_json::Value>) -> bool {
    outputs.values().any(|v| {
        v.get("action")
            .and_then(serde_json::Value::as_str)
            .is_some()
            && v.get("ok").is_none()
            && v.get("status").is_none()
    })
}

// bd-x4qw: same identity quadruple as run_loop, and the same deliberate
// allow its 7 siblings carry.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_steps_iter(
    ctx: &Context,
    trigger: &serde_json::Value,
    steps: &[Step],
    step_outputs: &mut serde_json::Map<String, serde_json::Value>,
    workflow: &str,
    run_id: &str,
    workflow_digest: &str,
    iteration: Option<&str>,
) -> resonate_sdk::prelude::Result<Option<serde_json::Value>> {
    for step in steps {
        let value = run_step_with_enclosing(
            ctx,
            trigger,
            step,
            step_outputs,
            None,
            workflow,
            run_id,
            workflow_digest,
            iteration,
        )
        .await?;
        if value.get("status").and_then(serde_json::Value::as_str) == Some("failed") {
            return Ok(Some(value));
        }
        if let Some(name) = &step.name {
            step_outputs.insert(name.clone(), value);
        }
    }
    Ok(None)
}

/// ADR-0032 Spec 3: runtime graph validation — the second honesty gate.
/// Lint rejects bad graphs statically, but `engine.run`/`invoke` bypass
/// lint. Direct execution validates upfront: unknown `needs` refs and
/// cycles/stalls FAIL with named nodes instead of returning partial success.
pub(crate) fn validate_graph_runtime(graph: &Graph) -> Result<(), String> {
    let nodes = &graph.nodes;
    if nodes.is_empty() {
        return Err("graph is empty — no nodes".to_string());
    }
    // Unknown needs first (names the missing dependency).
    for (name, node) in nodes {
        for need in &node.needs {
            if !nodes.contains_key(need) {
                return Err(format!(
                    "graph node {name:?} needs {need:?}, which is not a node"
                ));
            }
        }
    }
    // Cycle check via Kahn's algorithm: peel nodes with no incoming edges;
    // leftovers participate in a cycle. (All needs are known nodes here —
    // unknown refs returned above.)
    let mut indegree: std::collections::HashMap<&str, usize> = nodes
        .iter()
        .map(|(name, node)| (name.as_str(), node.needs.len()))
        .collect();
    let mut queue: Vec<&str> = indegree
        .iter()
        .filter(|(_, d)| **d == 0)
        .map(|(n, _)| *n)
        .collect();
    let mut peeled = 0usize;
    while let Some(n) = queue.pop() {
        peeled += 1;
        for (name, node) in nodes {
            if node.needs.iter().any(|need| need == n)
                && let Some(deg) = indegree.get_mut(name.as_str())
            {
                *deg -= 1;
                if *deg == 0 {
                    queue.push(name.as_str());
                }
            }
        }
    }
    if peeled != nodes.len() {
        let mut cyclic: Vec<&str> = indegree
            .iter()
            .filter(|(_, d)| **d > 0)
            .map(|(n, _)| *n)
            .collect();
        cyclic.sort();
        return Err(format!("graph has a cycle involving nodes: {cyclic:?}"));
    }
    Ok(())
}

/// The DAG scheduler (ADR-0005 Spec 4b / P5.2). Runs a `graph:` workflow:
///
/// 1. Schedule nodes in waves — a wave is the set of nodes whose `needs` are
///    all satisfied (roots first).
/// 2. Within a wave, spawn every node as a concurrent durable child
///    (`ctx.run(run_graph_node).spawn()`) — true concurrency inside one
///    deterministic-replay function (the P5.1 spike proved this primitive).
/// 3. Await the wave's futures, collect outputs under `steps.<name>`.
/// 4. Repeat until all nodes done.
///
/// The spike's bit-identical-replay guarantee carries over: child spawn order
/// is the deterministic per-context seq, so a crash mid-run replays to the
/// same outputs, promise ids, and child-spawn order.
pub(crate) async fn run_graph(
    ctx: &Context,
    trigger: &serde_json::Value,
    graph: &Graph,
    run_id: &str,
    workflow_name: &str,
    workflow_digest: &str,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    use crate::schema::GraphNode;
    use std::collections::BTreeSet;

    let nodes = &graph.nodes;
    let mut outputs: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let mut done: BTreeSet<String> = BTreeSet::new();
    let mut wave: usize = 0;

    // ADR-0032 Spec 3: runtime validation even when lint is bypassed — a
    // cyclic/unresolvable graph fails here with named nodes, never partial
    // success.
    if let Err(error) = validate_graph_runtime(graph) {
        return Ok(serde_json::json!({
            "status": "failed",
            "error": error,
            "failed_step": "<graph>",
        }));
    }

    loop {
        wave += 1;
        // Compute the next ready wave: not done, all needs done.
        let mut ready: Vec<(&String, &GraphNode)> = Vec::new();
        for (name, node) in nodes {
            if done.contains(name) {
                continue;
            }
            let needs_satisfied = node.needs.iter().all(|need| done.contains(need));
            if needs_satisfied {
                ready.push((name, node));
            }
        }
        if ready.is_empty() {
            break; // All done (cycles/unknown-needs fail in validation above).
        }

        // Spawn the whole wave concurrently as durable children.
        let mut futures = Vec::new();
        for (name, node) in &ready {
            // Planning render (lenient): nested bodies may carry deferred
            // context enforced strictly at execution in run_graph_node.
            let rendered = match render_action_lenient(&node.action, trigger, &outputs) {
                Ok(a) => a,
                Err(e) => {
                    // A render failure is a per-node error (continue unless
                    // fail_run).
                    let err = serde_json::json!({ "ok": false, "error": format!("render: {e}") });
                    if node.on_error == StepOnError::FailRun {
                        return Ok(serde_json::json!({
                            "status": "failed",
                            "error": format!("render: {e}"),
                            "failed_step": name,
                        }));
                    }
                    outputs.insert((*name).clone(), err);
                    done.insert((*name).clone());
                    continue;
                }
            };
            let future = ctx
                .run(
                    run_graph_node,
                    GraphNodeInput {
                        action: rendered,
                        guard: node.guard.clone(),
                        on_error: node.on_error,
                        trigger: trigger.clone(),
                        run_id: run_id.to_string(),
                        workflow_name: workflow_name.to_string(),
                        node_name: (*name).clone(),
                        wave,
                        outputs: outputs.clone(),
                        graph: Some(graph.clone()),
                        workflow_digest: workflow_digest.to_string(),
                        attempt: 1,
                    },
                )
                .spawn()?;
            futures.push(((*name).clone(), future));
        }

        // Await the wave's children (they ran concurrently; order of await is
        // deterministic). ADR-0024 Spec 1: rate_limit/timeout under fail_run
        // retries the SAME node with backoff.
        // We keep per-node attempt state; retries re-spawn the same durable child.
        let mut pending_futures = futures;
        // Map name -> rendered action for retry re-spawns
        let mut rendered_map: std::collections::HashMap<String, Action> =
            std::collections::HashMap::new();
        for (name, node) in &ready {
            if let Ok(a) = render_action_lenient(&node.action, trigger, &outputs) {
                rendered_map.insert((*name).clone(), a);
            }
        }
        // Track attempts per node (first spawn = attempt 1)
        let mut attempts: std::collections::HashMap<String, u32> = pending_futures
            .iter()
            .map(|(n, _)| (n.clone(), 1u32))
            .collect();
        let idx = 0;
        while idx < pending_futures.len() {
            let (name, future) = pending_futures.remove(idx);
            let value = future.await?;
            let node = nodes.get(&name).expect("node in map");
            // ADR-0032 Spec 7: the graph fail_run path is a client of the ONE
            // coordinator. An explicit `Retry` node action contributes its
            // policy; otherwise the default applies. A retryable failure
            // re-spawns the SAME node (never a repair graph).
            let coordinator = match &node.action {
                Action::Retry {
                    max_attempts,
                    backoff_secs,
                    ..
                } => RetryCoordinator::from_retry_action(*max_attempts, *backoff_secs),
                _ => RetryCoordinator::default_policy(),
            };
            if node.on_error == StepOnError::FailRun && crate::engine::is_failure_value(&value) {
                let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                if RetryCoordinator::is_retryable_value(&value) {
                    let attempt = *attempts.get(&name).unwrap_or(&1);
                    if !coordinator.should_retry(attempt) {
                        let err = value
                            .get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("step failed")
                            .to_string();
                        let disposition = disposition_for_kind(
                            kind,
                            value.get("disposition").and_then(|v| v.as_str()),
                        );
                        return Ok(serde_json::json!({
                            "status": "failed",
                            "error": err,
                            "failed_step": name,
                            "kind": kind,
                            "disposition": disposition,
                            "leaf_preview": leaf_preview_for_value(&value),
                            "attempts": attempt,
                        }));
                    }
                    let retry_after_ms = value
                        .get("retry_after_ms")
                        .and_then(serde_json::Value::as_u64);
                    ctx.sleep(coordinator.backoff(attempt, retry_after_ms))
                        .await?;
                    // Increment attempt and re-spawn same node
                    let next_attempt = attempt + 1;
                    attempts.insert(name.clone(), next_attempt);
                    let rendered = rendered_map.get(&name).cloned().unwrap_or_else(|| {
                        render_action_lenient(&node.action, trigger, &outputs)
                            .unwrap_or(node.action.clone())
                    });
                    let new_future = ctx
                        .run(
                            run_graph_node,
                            GraphNodeInput {
                                action: rendered,
                                guard: node.guard.clone(),
                                on_error: node.on_error,
                                trigger: trigger.clone(),
                                run_id: run_id.to_string(),
                                workflow_name: workflow_name.to_string(),
                                node_name: name.clone(),
                                wave,
                                outputs: outputs.clone(),
                                graph: Some(graph.clone()),
                                workflow_digest: workflow_digest.to_string(),
                                attempt: next_attempt,
                            },
                        )
                        .spawn()?;
                    pending_futures.insert(idx, (name, new_future));
                    continue;
                }
                let err = value
                    .get("error")
                    .and_then(|v| v.as_str())
                    .unwrap_or("step failed")
                    .to_string();
                let mut failed = serde_json::json!({
                    "status": "failed",
                    "error": err,
                    "failed_step": name,
                });
                if let Some(k) = value.get("kind") {
                    failed["kind"] = k.clone();
                }
                if let Some(d) = value.get("disposition") {
                    failed["disposition"] = d.clone();
                } else if !kind.is_empty() {
                    failed["disposition"] = serde_json::json!(disposition_for_kind(kind, None));
                }
                failed["leaf_preview"] = serde_json::json!(leaf_preview_for_value(&value));
                return Ok(failed);
            }
            land_node_output(&mut outputs, &name, &node.action, value);
            done.insert(name);
            // Do not increment idx — next pending shifted into current idx
        }
    }

    // Defensive stall gate: validation above makes an empty ready-set with
    // remaining nodes unreachable, but a stall must FAIL (never partial
    // success) if it ever happens.
    if done.len() != nodes.len() {
        let mut remaining: Vec<&String> = nodes.keys().filter(|n| !done.contains(*n)).collect();
        remaining.sort();
        return Ok(serde_json::json!({
            "status": "failed",
            "error": format!("graph stalled with no ready nodes; remaining: {remaining:?}"),
            "failed_step": "<graph>",
        }));
    }

    Ok(serde_json::Value::Object(outputs))
}

/// Land one completed graph node's value into the run's output map.
///
/// ADR-0024 Spec 2 (ONE decision spelling): a completed `request_approval`
/// node publishes its [`Decision`] under `decision` as well as under its own
/// node name. The linear path has always inserted the `decision` alias
/// ([`run_step`]'s request_approval arm); the DAG scheduler landed the value
/// under the node name ONLY, so an identical workflow routed through
/// `to_graph()` served a silent Null to every consumer reading `decision`.
/// Both paths now agree because the alias is inserted in the ONE place graph
/// node values land — see [`is_approval_decision`].
///
/// With more than one approval node the LAST completed one owns the alias,
/// matching the linear path's last-insert-wins behavior.
fn land_node_output(
    outputs: &mut serde_json::Map<String, serde_json::Value>,
    name: &str,
    action: &Action,
    value: serde_json::Value,
) {
    if is_approval_decision(action, &value) {
        outputs.insert("decision".into(), value.clone());
    }
    outputs.insert(name.to_string(), value);
}

/// ADR-0024 Spec 2: did this completed node produce a real approval DECISION?
///
/// Only the `request_approval` action's resolved promise qualifies, and only
/// when the value actually carries a [`Decision`] payload (`outcome`) — a
/// guard skip (`skipped`), a refusal, or a failure is NOT a decision and must
/// never be aliased as one.
fn is_approval_decision(action: &Action, value: &serde_json::Value) -> bool {
    matches!(action, Action::RequestApproval { .. })
        && value
            .get("outcome")
            .and_then(serde_json::Value::as_str)
            .is_some()
}

/// The input for one graph node execution (the durable child's input).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNodeInput {
    /// The (template-rendered) action to execute.
    pub action: Action,
    /// Per-node `guard:` evaluated at the top of `run_graph_node` through
    /// the ONE evaluator (ADR-0032 Spec 4): false is an explicit skip,
    /// guard errors fail the node. `#[serde(default)]` keeps
    /// in-flight serialized inputs readable.
    #[serde(default)]
    pub guard: Option<String>,
    /// The node's failure policy. Default Continue keeps a payload from
    /// before this field readable. New runs pass the authored policy.
    #[serde(default)]
    pub on_error: crate::schema::StepOnError,
    /// The trigger payload (bd-m7ac): control-flow nodes (parallel/foreach/
    /// branch) need it to render their inner steps — the raw runner path did
    /// not. Serialized with the action so the child is self-contained.
    pub trigger: serde_json::Value,
    /// Spec 3: run_id for evidence-bearing approval payload (default empty for backward compat)
    #[serde(default)]
    pub run_id: String,
    /// Spec 3: workflow name
    #[serde(default)]
    pub workflow_name: String,
    /// Spec 3: node name for suspended_at
    #[serde(default)]
    pub node_name: String,
    /// Spec 3: wave number for suspended_at
    #[serde(default)]
    pub wave: usize,
    /// Spec 3: current outputs snapshot for building context (default empty)
    #[serde(default)]
    pub outputs: serde_json::Map<String, serde_json::Value>,
    /// Spec 3: graph topology for pending nodes (default None for backward compat)
    #[serde(default)]
    pub graph: Option<Graph>,
    /// Canonical workflow digest for run-scoped effect keys (bd-zczp;
    /// default empty keeps in-flight serialized inputs readable).
    #[serde(default)]
    pub workflow_digest: String,
    /// Graph-level attempt counter for retries that re-spawn this node
    /// (bd-zczp; default 1 keeps in-flight inputs readable).
    #[serde(default = "default_attempt")]
    pub attempt: u32,
}

/// Default graph-node attempt (serde default fn — `1`, not `0`).
const fn default_attempt() -> u32 {
    1
}

/// A durable child executing ONE graph node's action. Spawned concurrently by
/// the DAG scheduler via `ctx.run(...).spawn()`. Runs through the shared
/// async effect seam (the Mailbox surface — the consistency rule). Failures are
/// returned as `{ok:false,error}` values (the parent applies on_error).
#[resonate_sdk::function]
pub async fn run_graph_node(
    ctx: &Context,
    input: GraphNodeInput,
) -> resonate_sdk::Result<serde_json::Value> {
    // bd-m7ac: graph nodes must route CONTROL-FLOW actions through the same
    // expansion as linear steps. A graph `parallel` node (the weekly-review
    // scout fan-out) needs `run_parallel` to spawn its copies — dispatching
    // the raw action to the engine would record `{action:"parallel"}` and
    // never fan out. Leaf actions keep the direct dispatch path (rendered
    // before the node ran, so no re-render).
    // ADR-0032 Spec 4: per-node `guard:` runs through the ONE evaluator.
    // False is an explicit skip; any guard error FAILS the node loudly.
    match evaluate_step_guard(
        input.guard.as_deref(),
        &input.trigger,
        &input.outputs,
        &input.node_name,
    )
    .await
    {
        GuardVerdict::Proceed => {}
        GuardVerdict::Skip { reason } => {
            return Ok(skipped_value(&input.node_name, reason));
        }
        GuardVerdict::Fail { error } => {
            return Ok(serde_json::json!({ "ok": false, "error": error }));
        }
    }
    use crate::schema::Step;

    // Spec 3: evidence-bearing approval payload (Wave 2)
    if let Action::RequestApproval { message } = &input.action {
        // Use the richer payload if we have the additional context (graph case)
        // Fallback to simple message-only payload for backward compat (e.g., tests that don't set run_id)
        let payload = if !input.run_id.is_empty()
            && !input.workflow_name.is_empty()
            && !input.node_name.is_empty()
            && input.graph.is_some()
        {
            let graph = input.graph.as_ref().unwrap();
            build_approval_payload(
                &input.run_id,
                &input.workflow_name,
                &input.node_name,
                input.wave,
                message,
                &input.outputs,
                graph,
            )
        } else {
            serde_json::json!({ "message": message })
        };
        let pending = ctx.promise::<Decision>().data(&payload)?.create()?;
        let promise_id = pending.id().await?;
        ctx.get_dependency::<ApprovalSink>()
            .lock()
            .await
            .push(promise_id.clone());
        let decision: Decision = match pending.await {
            Ok(d) => d,
            Err(resonate_sdk::error::Error::Suspended) => {
                return Err(resonate_sdk::error::Error::Suspended);
            }
            Err(e) => Decision::rejected(format!("rejected: {e}")),
        };
        let decision_val = serde_json::to_value(&decision)?;
        return Ok(decision_val);
    }

    // ADR-0024 Spec 0 park fix: graph nodes for RequestApproval/Delay/Spawn
    // must park via run_step (the linear park path), not the sync runner.
    // Control-flow and Verify also route through run_step.
    let is_park_or_control = matches!(
        input.action,
        Action::Parallel { .. }
            | Action::Foreach { .. }
            | Action::Reduce { .. }
            | Action::Branch { .. }
            | Action::Retry { .. }
            | Action::CallWorkflow { .. }
            | Action::Loop { .. }
            | Action::RequestApproval { .. }
            | Action::Delay { .. }
            | Action::Park { .. }
            | Action::Spawn { .. }
            | Action::Verify { .. }
    );
    if is_park_or_control {
        // Nested execution sees prior-wave outputs (the same ref scope as
        // top-level steps); the map itself is discarded after the node.
        let mut step_outputs = input.outputs.clone();
        let step = Step {
            name: None,
            guard: None,
            // The node's own policy. Forcing Continue here dropped every
            // inner `{status:failed}` on the floor and the graph finished green.
            on_error: input.on_error,
            build_slot: false,
            action: input.action,
        };
        return run_step(
            ctx,
            &input.trigger,
            &step,
            &mut step_outputs,
            &input.workflow_name,
            &input.run_id,
            &input.workflow_digest,
        )
        .await;
    }
    // Leaf actions re-render STRICTLY at execution: the scheduler's planning
    // render is lenient, so an unresolvable ref fails HERE with
    // workflow/node/field/ref attribution — never a verbatim placeholder.
    let render_ctx = RenderContext::new(&input.workflow_name, &input.node_name);
    let action = match render_action(&input.action, &input.trigger, &input.outputs, &render_ctx) {
        Ok(a) => a,
        Err(e) => {
            return Ok(serde_json::json!({ "ok": false, "error": format!("render: {e}") }));
        }
    };
    // bd-zczp: the graph leaf carries full run identity — effectful actions
    // prefer the context-bearing EffectRunner; the legacy compat path stays
    // for the rest.
    let invocation = invocation_for_leaf(
        action,
        &input.run_id,
        &input.workflow_digest,
        &input.node_name,
        input.attempt,
        1,
        None,
    );
    Ok(dispatch_leaf_or_compat(ctx, &invocation).await)
}

/// The `toron:workflow-emitted` stamp (ADR-0004 Ship 2 / bd-pkc part 3).
///
/// The host embedding applies this single-`t` tag to every event a
/// workflow action emits, so the trigger-ingress filter
/// ([`crate::trigger_filter::should_evaluate_trigger_event`]) rejects
/// workflow-emitted events by tag — loop-prevention defense-in-depth
/// (workflow emits never re-trigger the same workflow).
///
/// The stamp is a pure builder: given a tags vector, returns a new vector
/// with the marker `t`-tag appended (idempotent — never double-stamps).
pub fn stamp_workflow_emitted(tags: Vec<nostr_sdk::prelude::Tag>) -> Vec<nostr_sdk::prelude::Tag> {
    use crate::trigger_filter::WORKFLOW_EMITTED_TAG;
    if tags.iter().any(|t| {
        t.as_slice().first().map(String::as_str) == Some("t")
            && t.as_slice().get(1).map(String::as_str) == Some(WORKFLOW_EMITTED_TAG)
    }) {
        return tags;
    }
    let mut out = tags;
    out.push(nostr_sdk::prelude::Tag::parse(["t", WORKFLOW_EMITTED_TAG]).expect("stamp tag"));
    out
}

/// Build the kind-46012 workflow-run terminal event (ADR-0005 Spec 0, P0.3).
/// Maps the durable function's outcome Value to the wire `status` field —
/// additive content only (the kind is frozen; its content is toron-owned; old
/// readers ignore the new `status`/`run_id`/`error`/`failed_step` fields):
///
/// - outcome `{status:"failed", error, failed_step}` → 46012 `status:"failed"`.
/// - normal completion (no `status` key) → 46012 `status:"succeeded"`.
///
/// The full outcome is preserved under `output` so downstream readers (smart
/// cron's `on_failure`, `flywheel runs`) see step outputs on success and the
/// failure bundle on failure.
///
/// The event is addressable by run id (`d` = run_id), matching the
/// WORKFLOW_RUN_FIRST..=LAST registry contract.
pub fn build_workflow_run_terminal(
    keys: &nostr_sdk::prelude::Keys,
    run_id: &str,
    outcome: &serde_json::Value,
) -> anyhow::Result<nostr_sdk::prelude::Event> {
    use nostr_sdk::prelude::{Tag, Timestamp};
    use toron_core::nostr::event::build_event;
    use toron_core::nostr::kinds::{WORKFLOW_RUN_LAST, toron_kind};

    let status = match outcome.get("status").and_then(serde_json::Value::as_str) {
        Some("failed") => "failed",
        _ => "succeeded",
    };
    let mut terminal = serde_json::Map::new();
    terminal.insert("status".into(), serde_json::Value::String(status.into()));
    if let Some(err) = outcome.get("error") {
        terminal.insert("error".into(), err.clone());
    }
    if let Some(failed_step) = outcome.get("failed_step") {
        terminal.insert("failed_step".into(), failed_step.clone());
    }
    terminal.insert("run_id".into(), serde_json::Value::String(run_id.into()));
    terminal.insert("output".into(), outcome.clone());

    build_event(
        keys,
        toron_kind(WORKFLOW_RUN_LAST),
        serde_json::Value::Object(terminal).to_string(),
        vec![Tag::identifier(run_id)],
        Timestamp::now(),
    )
    .map_err(|e| anyhow::anyhow!("build kind-46012 terminal: {e}"))
}
/// Count for `parallel over`. A bare positive integer, a template that
/// renders to one, or a JSON number. `@crew.role` is not a count.
pub(crate) fn parallel_fanout_count(
    over: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<usize> {
    fn positive(n: u64) -> Option<usize> {
        usize::try_from(n).ok().filter(|n| *n > 0)
    }
    if let Ok(n) = over.trim().parse::<u64>() {
        return positive(n);
    }
    match resolve_template_ref(over, trigger, step_outputs)? {
        serde_json::Value::Number(n) => n.as_u64().and_then(positive),
        serde_json::Value::String(s) => s.trim().parse::<u64>().ok().and_then(positive),
        _ => None,
    }
}

/// If `response` is a JSON object, or one fenced block that is, copy its
/// fields onto the step value. Existing keys (`ok`, `error`) stay.
fn promote_response_fields(value: &mut serde_json::Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let Some(text) = obj.get("response").and_then(serde_json::Value::as_str) else {
        return;
    };
    let Some(parsed) = json_object_in_text(text) else {
        return;
    };
    let extras: Vec<(String, serde_json::Value)> = parsed.into_iter().collect();
    for (k, v) in extras {
        obj.entry(k).or_insert(v);
    }
}

/// The whole string, or a single ``` fence, when that text is a JSON object.
fn json_object_in_text(text: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    fn parse(s: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
        match serde_json::from_str::<serde_json::Value>(s.trim()) {
            Ok(serde_json::Value::Object(map)) => Some(map),
            _ => None,
        }
    }
    if let Some(map) = parse(text) {
        return Some(map);
    }
    let trimmed = text.trim();
    let rest = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))?;
    let body = rest.rsplit_once("```")?.0;
    parse(body)
}

#[cfg(test)]
mod control_flow_guard_tests {
    use super::guard_vars;

    /// bd-adr0008-guard-fix-16fl: the shared guard map flattens the trigger
    /// payload (caller inputs ride the trigger at invoke) and expands
    /// recorded step outputs into `steps_*` variables.
    #[test]
    fn guard_vars_flattens_trigger_and_outputs() {
        let trigger =
            serde_json::json!({"objective": "x", "until": "create", "inputs": {"until": "create"}});
        let mut outputs = serde_json::Map::new();
        outputs.insert("grill".to_string(), serde_json::json!({"ok": true}));
        let vars = guard_vars(&trigger, &outputs);
        assert_eq!(
            vars.get("trigger_until"),
            Some(&evalexpr::Value::from("create")),
            "inputs ride the trigger as trigger_<key>"
        );
        assert_eq!(
            vars.get("steps_grill_ok"),
            Some(&evalexpr::Value::Boolean(true)),
            "object outputs expand to steps_<name>_<key>"
        );
    }

    #[test]
    fn parallel_count_reads_a_template_and_refuses_a_crew_role() {
        let trigger = serde_json::json!({"inputs": {"n": 3}});
        let outputs = serde_json::Map::new();
        assert_eq!(
            super::parallel_fanout_count("{{inputs.n}}", &trigger, &outputs),
            Some(3)
        );
        assert_eq!(
            super::parallel_fanout_count("4", &trigger, &outputs),
            Some(4)
        );
        assert_eq!(
            super::parallel_fanout_count("@crew.coder", &trigger, &outputs),
            None
        );
    }

    #[test]
    fn response_json_promotes_fields_without_clobbering_ok() {
        let mut value = serde_json::json!({
            "ok": true,
            "response": "{\"improved\": true, \"ok\": false, \"number\": 2}"
        });
        super::promote_response_fields(&mut value);
        assert_eq!(value["ok"], true);
        assert_eq!(value["improved"], true);
        assert_eq!(value["number"], 2);
    }
}

#[cfg(test)]
mod control_flow_output_tests {
    use super::{Action, land_node_output};

    /// ADR-0024 Spec 2: a completed approval node lands under BOTH its node
    /// name and `decision` — the DAG scheduler used to publish only the node
    /// name, so a consumer reading `decision` saw a silent Null.
    #[test]
    fn approval_node_lands_the_decision_alias() {
        let mut outputs = serde_json::Map::new();
        let decision = serde_json::json!({ "outcome": "approved", "note": "ship it" });
        land_node_output(
            &mut outputs,
            "gate",
            &Action::RequestApproval {
                message: "ok?".into(),
            },
            decision.clone(),
        );
        assert_eq!(outputs["decision"], decision, "alias published");
        assert_eq!(outputs["gate"], decision, "node-name output published");
    }

    /// A guard SKIP on an approval node is not a decision: the node name lands,
    /// the alias does not — a skip must never read as an approval.
    #[test]
    fn skipped_approval_node_never_lands_the_alias() {
        let mut outputs = serde_json::Map::new();
        land_node_output(
            &mut outputs,
            "gate",
            &Action::RequestApproval {
                message: "ok?".into(),
            },
            serde_json::json!({ "skipped": true, "node": "gate", "reason": "guard false" }),
        );
        assert!(outputs.contains_key("gate"), "the node still lands");
        assert!(
            !outputs.contains_key("decision"),
            "a skip is not a decision: {outputs:?}"
        );
    }
}

#[cfg(test)]
mod fail_run_preview_tests_bd_90eb {
    use super::super::{ActionInvocation, AsyncEffectRunner, RunInput, WorkflowEngine};
    use super::leaf_preview_for_value;
    use crate::schema::{Action, parse_workflow};
    use std::sync::Arc;

    /// bd-90eb: bound is 512 chars, then the marker is appended.
    #[test]
    fn long_leaf_truncates_with_marker() {
        let v = serde_json::json!({"blob": "x".repeat(2000)});
        let preview = leaf_preview_for_value(&v);
        assert!(
            preview.ends_with("\u{2026}(truncated)"),
            "marker appended: {preview}"
        );
        assert_eq!(
            preview.chars().count(),
            512 + "\u{2026}(truncated)".chars().count()
        );
    }

    /// bd-90eb: the linear fail_run fallback embeds leaf preview plus
    /// kind/disposition when the leaf value carries them. `auth` parses
    /// but is not a retry candidate, so this hits the fallback directly.
    #[tokio::test]
    async fn fail_run_fallback_embeds_preview_kind_disposition() {
        let runner: AsyncEffectRunner = Arc::new(|inv: &ActionInvocation| {
            let value = if matches!(inv.action, Action::SendMessage { .. }) {
                serde_json::json!({"ok": false, "error": "bad creds", "kind": "auth"})
            } else {
                serde_json::Value::Null
            };
            Box::pin(async move { Ok(value) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
                >
        });
        let engine = WorkflowEngine::local_with_async_effect_runner(Some(runner)).expect("engine");
        let workflow = parse_workflow(
            r##"
name: upgrade-wf
trigger:
  type: webhook
steps:
  - name: upgrade
    action: send_message
    to: ["ops"]
    subject: "upgrade"
    body: "go"
    on_error: fail_run
"##,
        )
        .expect("parse fail_run workflow");
        let output = engine
            .run(
                "wf-fail-run-preview-1",
                RunInput {
                    run_id: None,
                    workflow,
                    trigger: serde_json::json!({}),
                },
            )
            .await
            .expect("run returns a value, not a panic");
        assert_eq!(output["status"], "failed", "run failed: {output}");
        assert_eq!(output["kind"], "auth", "kind carried: {output}");
        assert!(
            !output["disposition"].is_null(),
            "disposition carried: {output}"
        );
        let preview = output["leaf_preview"]
            .as_str()
            .expect("leaf_preview string");
        assert!(preview.contains("auth"), "preview embeds kind: {output}");
        assert!(
            preview.contains("bad creds"),
            "preview embeds error: {output}"
        );
    }
}
