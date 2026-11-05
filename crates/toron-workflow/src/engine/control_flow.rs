// governed-by: ADR-0024
// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Durable control-flow functions (F21.ENGINE-SPLIT move from the former
//! single-file engine): `run_workflow` + step dispatch, retry/branch/foreach/
//! parallel/call_workflow/spawn, the DAG scheduler, the parallel/graph durable
//! children, and the workflow-emission helpers (stamp + kind-46012 terminal).
//! All items are re-exported from the parent module.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::schema::{Action, Graph, Step, StepOnError};

use super::{ActionInvocation, ExecutionPolicy, FailureKind, RunContext};

use sha2::{Digest, Sha256};

/// Spec 3 helper: extract artifacts from completed node outputs (paths, not blobs)
fn extract_artifacts(
    outputs: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut artifacts = Vec::new();
    for (node, value) in outputs {
        if let Some(obj) = value.as_object() {
            for key in ["sha", "commit_sha", "commit"] {
                if let Some(sha) = obj.get(key).and_then(|v| v.as_str()) {
                    artifacts.push(serde_json::json!({
                        "node": node,
                        "kind": "commit",
                        "sha": sha,
                    }));
                }
            }
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
        }
    }
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
    let mut map = std::collections::BTreeMap::new();
    map.insert(
        "run_id".to_string(),
        serde_json::Value::String(run_id.to_string()),
    );
    map.insert(
        "node".to_string(),
        serde_json::Value::String(node.to_string()),
    );
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
pub(crate) fn build_approval_payload(
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
    pub(crate) fn from_enclosing(enclosing: Option<(u32, u64)>) -> Self {
        match enclosing {
            Some((max, backoff)) => Self::from_retry_action(max, backoff),
            None => Self::default_policy(),
        }
    }

    /// Whether another attempt is allowed after `attempt` (1-based) failed.
    pub(crate) fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }

    /// Retry candidacy for a step value: `rate_limit`/`timeout` (the frozen
    /// [`FailureKind`] vocabulary) are candidates; everything else escalates.
    /// Unknown kind strings are never retryable.
    pub(crate) fn is_retryable_value(value: &serde_json::Value) -> bool {
        value
            .get("kind")
            .and_then(|v| v.as_str())
            .and_then(|k| k.parse::<FailureKind>().ok())
            .is_some_and(super::boundary::FailureKind::is_retry_candidate)
    }

    /// Backoff before the next attempt: provider `retry_after_ms` wins, then
    /// the enclosing fixed backoff, else exponential.
    pub(crate) fn backoff(&self, attempt: u32, retry_after_ms: Option<u64>) -> Duration {
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
pub(crate) fn disposition_for_kind(
    kind: &str,
    existing: Option<&str>,
) -> super::boundary::Disposition {
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
pub(crate) fn leaf_preview_for_value(value: &serde_json::Value) -> String {
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

/// What the shared host-step policy says to do with one attempt's value.
pub(crate) enum StepOutcome {
    /// Terminal: this is the step's result.
    Done(serde_json::Value),
    /// The same logical node retries after `backoff`.
    Retry { backoff: Duration },
}

/// The `{status:failed,…}` bundle the on_error policy returns for one failure
/// value.
///
/// `attempts` is written only when the caller knows the count — the
/// retry-exhausted path carries it, the non-retryable path never wrote it. This
/// is the ONE builder: the workflow engine, the linear fold, and the graph fold
/// all reach the same value from the same inputs.
pub(crate) fn fail_run_value(
    step: &str,
    value: &serde_json::Value,
    attempts: Option<u32>,
) -> serde_json::Value {
    let kind = value.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let error = value
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or("step failed")
        .to_string();
    let mut failed = serde_json::json!({
        "status": "failed",
        "error": error,
        "failed_step": step,
    });
    if let Some(k) = value.get("kind") {
        failed["kind"] = k.clone();
    }
    if let Some(d) = value.get("disposition") {
        failed["disposition"] = d.clone();
    } else if !kind.is_empty() {
        failed["disposition"] = serde_json::json!(disposition_for_kind(kind, None));
    }
    failed["leaf_preview"] = serde_json::json!(leaf_preview_for_value(value));
    if let Some(attempts) = attempts {
        failed["attempts"] = serde_json::json!(attempts);
    }
    failed
}

/// The ONE host-step outcome policy (bd-linear-step-6j0y).
///
/// A dispatch that answered with a JSON object (or one fenced block) publishes
/// those fields on the step, so guards such as `steps.measure.improved` can see
/// `improved`. Then (P0.2, ADR-0005 Spec 0 + ADR-0024 Spec 1) `on_error:
/// fail_run` converts a step failure into a RUN failure — a deterministic
/// durable RETURN, never a panic. ADR-0032 Spec 7: retry candidacy resolves in
/// the ONE coordinator (frozen `FailureKind` vocabulary); a retryable runtime
/// failure retries the SAME node, bounded.
///
/// The Context engine and the linear host both call this, so the fail_run
/// shape has one author. The caller owns the sleep between attempts, because
/// that is the one substrate-specific effect (`ctx.sleep` vs `tokio::sleep`).
pub(crate) fn classify_step_outcome(
    step: &Step,
    mut value: serde_json::Value,
    attempt: u32,
    coordinator: &RetryCoordinator,
) -> StepOutcome {
    promote_response_fields(&mut value);

    if step.on_error != StepOnError::FailRun || !crate::engine::is_failure_value(&value) {
        return StepOutcome::Done(value);
    }
    if RetryCoordinator::is_retryable_value(&value) {
        if !coordinator.should_retry(attempt) {
            return StepOutcome::Done(fail_run_value(
                &step.name.clone().unwrap_or_else(|| "<unnamed>".into()),
                &value,
                Some(attempt),
            ));
        }
        let retry_after_ms = value
            .get("retry_after_ms")
            .and_then(serde_json::Value::as_u64);
        return StepOutcome::Retry {
            backoff: coordinator.backoff(attempt, retry_after_ms),
        };
    }
    StepOutcome::Done(fail_run_value(
        &step.name.clone().unwrap_or_else(|| "<unnamed>".into()),
        &value,
        None,
    ))
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
    let flattened = crate::guard::normalize_guard_paths(g);
    let rewritten = crate::guard::rewrite_bool_string_literals(&flattened);
    let vars = guard_vars(trigger, step_outputs);
    match crate::guard::evaluate_guard_default(Some(&rewritten), &vars).await {
        crate::guard::GuardOutcome::Bool(true) => GuardVerdict::Proceed,
        crate::guard::GuardOutcome::Unset => GuardVerdict::Proceed,
        crate::guard::GuardOutcome::Bool(false) => GuardVerdict::Skip {
            reason: format!("guard false on node {node:?}"),
        },
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

/// Elastick fan-out cap (ADR-0032 Spec 3): `parallel` concurrency is bounded
/// by explicit policy. Counts above this fail loud instead of fanning out.
pub(crate) const MAX_PARALLEL_FANOUT: usize = 32;

/// Bounded reduce cap (bd-3pti): inputs longer than this fail loud instead
/// Reserved trigger key carrying the durable workflow lineage envelope.
/// The envelope is deliberately part of `trigger`, rather than `Context`, so
/// it serializes with the child input and a resumed run sees the same guard.
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

pub(crate) fn validate_workflow_lineage(
    trigger: &serde_json::Value,
    workflow: &str,
) -> std::result::Result<(), String> {
    lineage_for_trigger(trigger, workflow).map(|_| ())
}

/// The trigger a durable `call_workflow` child receives
/// (bd-durable-call-workflow-mg6y).
///
/// The fold builds it (no host read): parent inputs merged with the call's
/// explicit inputs, the parent's outputs as `parent_steps`, and the lineage
/// envelope the child's own admission checks. Refusals are returned as
/// `Err`, so a static call can refuse the parent before any row.
pub(crate) fn durable_child_trigger(
    parent_workflow: &str,
    parent_trigger: &serde_json::Value,
    child_name: &str,
    inputs: &serde_json::Value,
    parent_outputs: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<serde_json::Value, String> {
    let lineage = lineage_for_trigger(parent_trigger, parent_workflow)?;
    if lineage.chain.iter().any(|name| name == child_name) {
        return Err(format!(
            "recursive workflow call refused: {parent_workflow:?} -> {child_name:?}; call chain: {:?}",
            lineage.chain
        ));
    }
    if lineage.chain.len() >= MAX_WORKFLOW_DEPTH {
        return Err(format!(
            "workflow child {child_name:?} refused: nesting depth bound {MAX_WORKFLOW_DEPTH} reached"
        ));
    }
    if lineage.remaining_children == 0 {
        return Err(format!(
            "workflow child {child_name:?} refused: child budget exhausted (bound {MAX_WORKFLOW_CHILDREN})"
        ));
    }
    let merged = merge_parent_inputs(parent_trigger, inputs);
    let mut trigger_map = serde_json::Map::new();
    if let Some(obj) = merged.as_object() {
        for (k, v) in obj {
            trigger_map.insert(k.clone(), v.clone());
        }
    }
    trigger_map.insert("inputs".into(), merged);
    trigger_map.insert(
        "parent_steps".into(),
        serde_json::Value::Object(parent_outputs.clone()),
    );
    let mut child_lineage = lineage;
    child_lineage.chain.push(child_name.to_string());
    child_lineage.remaining_children -= 1;
    trigger_map.insert(
        WORKFLOW_LINEAGE_KEY.into(),
        serde_json::to_value(child_lineage).map_err(|e| format!("workflow lineage: {e}"))?,
    );
    Ok(serde_json::Value::Object(trigger_map))
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

/// ADR-0032 Spec 3: runtime graph validation — the second honesty gate.
/// Lint rejects bad graphs statically, but `engine.run`/`invoke` bypass
/// lint. Direct execution validates upfront: unknown `needs` refs and
/// cycles/stalls FAIL with named nodes instead of returning partial success.
pub(crate) fn validate_graph_runtime(graph: &Graph) -> Result<(), String> {
    let nodes = &graph.nodes;
    if nodes.is_empty() {
        return Err("graph is empty — no nodes".to_string());
    }
    for (name, node) in nodes {
        for need in &node.needs {
            if !nodes.contains_key(need) {
                return Err(format!(
                    "graph node {name:?} needs {need:?}, which is not a node"
                ));
            }
        }
    }
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

/// ADR-0024 Spec 2: did this completed node produce a real approval DECISION?
///
/// Only the `request_approval` action's resolved promise qualifies, and only
/// when the value actually carries a [`Decision`] payload (`outcome`) — a
/// guard skip (`skipped`), a refusal, or a failure is NOT a decision and must
/// never be aliased as one.
pub(crate) fn is_approval_decision(action: &Action, value: &serde_json::Value) -> bool {
    matches!(action, Action::RequestApproval { .. })
        && value
            .get("outcome")
            .and_then(serde_json::Value::as_str)
            .is_some()
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
mod fail_run_preview_tests_bd_90eb {
    use std::sync::Arc;

    use super::super::{ActionInvocation, AsyncEffectRunner, RunInput, WorkflowEngine};
    use super::leaf_preview_for_value;
    use crate::schema::{Action, parse_workflow};

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
