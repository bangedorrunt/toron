//! Fenced durable-effect dispatch (moved from toron-mcp).
//!
//! [`EffectDispatcher`] runs the ADR-0032 dispatch algorithm against a
//! [`WorkflowEffectStore`]: insert-or-load intent, plan from the journaled
//! view, claim under fencing, invoke the provider exactly once, settle.
//! toron-mcp re-exports this surface; the wire format is unchanged.

use super::durable::unsupported_reason;
use super::spawn::spawn_launcher_refusal;
use super::{
    CleanupKind, CleanupRequest, DispatchPlan, FORCED_CLEANUP_CAPABILITY, ProviderCapability,
    ProviderOutcome, UnknownReport, capability_for_action_type, cleanup_effect_key,
    corr_for_effect, describe_unknown, effect_view, plan_attempt,
};
use crate::schema::Workflow;
use toron_core::index::workflow_effects::{
    ClaimAttempt, EffectState, NewAttempt, NewEffect, WorkflowEffect, WorkflowEffectStore, now_us,
};

/// Route one workflow to the durable host or the old engine (bd-host-cut-pqkg).
/// `Ok` means the durable path owns it; `Err(reason)` names why it stays on
/// `WorkflowEngine::run`. Pure: no row exists at this point either way.
///
/// `spawn_capable` is the caller's CAPABILITY, not the workflow's shape: a
/// spawn workflow is admitted only when this host can actually launch the
/// child (bd-spawn-launcher-flip-d8mv).
pub fn durable_route(
    workflow: &Workflow,
    has_store: bool,
    spawn_capable: bool,
) -> Result<(), String> {
    if !has_store {
        return Err("no durable store attached".into());
    }
    if let Some(reason) = unsupported_reason(workflow) {
        return Err(reason);
    }
    if !spawn_capable && let Some(reason) = spawn_launcher_refusal(workflow) {
        return Err(reason);
    }
    Ok(())
}

/// One effect dispatch request: identity inputs plus the provider's
/// declared capability. The effect key is minted by the caller (stable
/// across retries); the dispatcher never invents a second key.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub effect_key: String,
    pub run_id: String,
    pub project: Option<String>,
    pub workflow_digest: String,
    pub node_path: String,
    pub iteration_key: Option<String>,
    pub action_type: String,
    pub action_digest: String,
    pub redacted_action: Option<serde_json::Value>,
    pub capability: ProviderCapability,
}

/// The journaled result of one dispatch call.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchResult {
    pub effect_key: String,
    pub attempt: i64,
    pub state: EffectState,
    pub outcome: serde_json::Value,
}

/// Classify one provider outcome into (effect state, failure evidence,
/// outcome value). `Null` delivery and unproven results never read as
/// success: null is `invalid_output`, ambiguity is `unknown`.
/// (Moved with the dispatcher; kept `pub` for the mcp-side behavior tests.)
pub fn classify_outcome(
    outcome: &ProviderOutcome,
) -> (
    EffectState,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
) {
    match outcome {
        ProviderOutcome::Delivered(v) if v.is_null() => (
            EffectState::Failed,
            Some(serde_json::json!({"code": "invalid_output"})),
            None,
        ),
        ProviderOutcome::Delivered(v) => (EffectState::Settled, None, Some(v.clone())),
        ProviderOutcome::Deferred(v) => (EffectState::Accepted, None, Some(v.clone())),
        ProviderOutcome::ProvenFailed(msg) => (
            EffectState::Failed,
            Some(serde_json::json!({"error": msg})),
            None,
        ),
        ProviderOutcome::Ambiguous(msg) => (
            EffectState::Unknown,
            Some(serde_json::json!({"error": msg})),
            None,
        ),
    }
}

/// Dispatch-lease duration per attempt (5 minutes in µs). An attempt that
/// outlives its lease without settling is recoverable by a new owner; it is
/// never proof the provider was not called.
pub const DISPATCH_LEASE_US: i64 = 300_000_000;

/// Executes fenced effect dispatches against the workflow-effect journal.
#[derive(Debug, Clone)]
pub struct EffectDispatcher {
    store: WorkflowEffectStore,
}

/// A claimed attempt, ready to invoke (bd-ckkf): the claim-phase output
/// shared by [`EffectDispatcher::dispatch`] and
/// [`EffectDispatcher::dispatch_async`] so the fencing path exists once.
struct ClaimedAttempt {
    attempt_no: i64,
    attempt_id: String,
    token: String,
}

/// The claim-phase outcome: either an already-terminal row to return, or a
/// fenced attempt to invoke exactly once.
enum ClaimOutcome {
    /// Return this result without invoking: duplicate terminal or refusal
    /// (refusals arrive as `Err`).
    Done(anyhow::Result<DispatchResult>),
    /// Invoke the provider once under this fence, then settle.
    Claimed(ClaimedAttempt),
}

impl EffectDispatcher {
    pub const fn new(store: WorkflowEffectStore) -> Self {
        Self { store }
    }

    pub const fn store(&self) -> &WorkflowEffectStore {
        &self.store
    }

    /// Run the dispatch algorithm (ADR-0032 §5.3): render/validate aside,
    /// 1. insert-or-load the prepared intent (duplicate keys attach, never
    ///    fork);
    /// 2. return the stored outcome when the row is already terminal;
    /// 3. plan from the journaled view (refuse/reconcile/duplicate honored);
    /// 4. claim the attempt under fencing before invoking the provider;
    /// 5. link `transport_corr` by projection;
    /// 6. invoke the provider exactly once for this attempt;
    /// 7. settle, fail, or mark `unknown` from the classified outcome, with
    ///    append-only attempt evidence at every step.
    pub async fn dispatch(
        &self,
        req: &DispatchRequest,
        provider: impl FnOnce(&DispatchRequest) -> ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        match self.prepare_and_claim(req).await? {
            ClaimOutcome::Done(result) => result,
            ClaimOutcome::Claimed(claim) => {
                let outcome = provider(req);
                self.settle_and_record(req, claim, &outcome).await
            }
        }
    }

    /// The async-provider twin of [`dispatch`](Self::dispatch) (bd-ckkf,
    /// ADR-0035 Spec 2.2): identical fencing, but the provider is awaited
    /// inline — the path the async host runner uses, so no thread-hop
    /// bridge exists anywhere on the effect path.
    pub async fn dispatch_async<Fut>(
        &self,
        req: &DispatchRequest,
        provider: impl FnOnce(&DispatchRequest) -> Fut,
    ) -> anyhow::Result<DispatchResult>
    where
        Fut: std::future::Future<Output = ProviderOutcome> + Send,
    {
        match self.prepare_and_claim(req).await? {
            ClaimOutcome::Done(result) => result,
            ClaimOutcome::Claimed(claim) => {
                let outcome = provider(req).await;
                self.settle_and_record(req, claim, &outcome).await
            }
        }
    }

    /// Dispatch steps 1–5: insert-or-load the prepared intent (duplicate
    /// keys attach, never fork); return the stored outcome when already
    /// terminal; plan from the journaled view; claim the attempt under
    /// fencing; link `transport_corr` by projection.
    async fn prepare_and_claim(&self, req: &DispatchRequest) -> anyhow::Result<ClaimOutcome> {
        let prepared = self
            .store
            .prepare_effect(NewEffect {
                effect_key: req.effect_key.clone(),
                run_id: req.run_id.clone(),
                project: req.project.clone(),
                workflow_digest: req.workflow_digest.clone(),
                node_path: req.node_path.clone(),
                iteration_key: req.iteration_key.clone(),
                action_type: req.action_type.clone(),
                action_digest: req.action_digest.clone(),
                redacted_action: req.redacted_action.clone(),
                retention_until: None,
            })
            .await?;
        let row = prepared.effect;
        if row.state.is_terminal() {
            return Ok(ClaimOutcome::Done(Ok(self.result_from_row(&row))));
        }
        let now = now_us();
        let lease_live = matches!(row.lease_expires_at, Some(expires) if expires > now);
        let view = effect_view(row.state, row.attempt, lease_live);
        let attempt_no = match plan_attempt(Some(&view), &req.capability) {
            DispatchPlan::DispatchNew => row.attempt,
            DispatchPlan::RetrySameKey { attempt } => attempt,
            DispatchPlan::DuplicateTerminal => {
                return Ok(ClaimOutcome::Done(Ok(self.result_from_row(&row))));
            }
            DispatchPlan::ReconcileFirst { reason } | DispatchPlan::RefuseBlindRetry { reason } => {
                return Ok(ClaimOutcome::Done(Err(anyhow::anyhow!(
                    "dispatch refused for {}: {reason}",
                    req.effect_key
                ))));
            }
            DispatchPlan::ResumeParked { attempt } => attempt,
        };
        let token = format!("tok-{}-a{}", req.effect_key, attempt_no);
        let attempt_id = format!("att-{}-a{}", req.effect_key, attempt_no);
        let claimed = self
            .store
            .claim_attempt(
                &req.effect_key,
                ClaimAttempt {
                    attempt: attempt_no,
                    attempt_id: attempt_id.clone(),
                    claim_token: token.clone(),
                    lease_expires_at: now + DISPATCH_LEASE_US,
                    expected_claim_token: row.claim_token.clone(),
                },
            )
            .await?;
        if !claimed {
            return Ok(ClaimOutcome::Done(Err(anyhow::anyhow!(
                "dispatch fenced for {}: live owner holds the lease",
                req.effect_key
            ))));
        }
        self.store
            .record_attempt(NewAttempt {
                effect_key: req.effect_key.clone(),
                attempt: attempt_no,
                attempt_id: attempt_id.clone(),
                claim_token: Some(token.clone()),
                state: EffectState::InFlight,
                evidence: None,
            })
            .await?;
        self.store
            .link_transport_corr(&req.effect_key, &corr_for_effect(&req.effect_key))
            .await?;
        Ok(ClaimOutcome::Claimed(ClaimedAttempt {
            attempt_no,
            attempt_id,
            token,
        }))
    }

    /// Dispatch steps 6–7: invoke has happened (the caller invoked the
    /// provider exactly once for the claimed attempt); settle, fail, or mark
    /// `unknown` from the classified outcome, with append-only attempt
    /// evidence.
    async fn settle_and_record(
        &self,
        req: &DispatchRequest,
        claim: ClaimedAttempt,
        outcome: &ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        let ClaimedAttempt {
            attempt_no,
            attempt_id,
            token,
        } = claim;
        let (to, failure, value) = classify_outcome(outcome);
        let settled = self
            .store
            .settle_with_fence(&req.effect_key, &token, to, failure, value)
            .await?;
        if !settled {
            anyhow::bail!("dispatch lost fencing for {} mid-attempt", req.effect_key);
        }
        self.store
            .record_attempt(NewAttempt {
                effect_key: req.effect_key.clone(),
                attempt: attempt_no,
                attempt_id,
                claim_token: Some(token),
                state: to,
                evidence: Some(match outcome {
                    ProviderOutcome::Delivered(v) | ProviderOutcome::Deferred(v) => v.clone(),
                    ProviderOutcome::ProvenFailed(m) | ProviderOutcome::Ambiguous(m) => {
                        serde_json::json!({"error": m})
                    }
                }),
            })
            .await?;
        let row = self
            .store
            .get_effect(&req.effect_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("effect vanished after settle"))?;
        Ok(self.result_from_row(&row))
    }

    /// Settle one effect with an explicitly presented fencing token.
    /// Returns `false` (changing nothing) when the token is stale — the
    /// caller that lost the fence must reconcile, not overwrite.
    pub async fn settle_with_presented(
        &self,
        effect_key: &str,
        presented_token: &str,
        outcome: ProviderOutcome,
    ) -> anyhow::Result<bool> {
        let (to, failure, value) = classify_outcome(&outcome);
        self.store
            .settle_with_fence(effect_key, presented_token, to, failure, value)
            .await
    }

    fn result_from_row(&self, row: &WorkflowEffect) -> DispatchResult {
        let outcome = row
            .outcome_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .or_else(|| {
                row.failure_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
            })
            .unwrap_or(serde_json::Value::Null);
        DispatchResult {
            effect_key: row.effect_key.clone(),
            attempt: row.attempt,
            state: row.state,
            outcome,
        }
    }

    /// Run one fixed cleanup effect (ADR-0032 Spec 8.2, bd-v139.3): lease
    /// release, bounded captain evidence, or bead comment — never a
    /// user-authored undo, never a rollback claim, never a bead close.
    /// The attempt journals under `{parent}/cleanup:{kind}` with the
    /// kind's retry capability; the parent failure row is never written.
    /// `force` is the explicit operator override for ambiguous non-idempotent
    /// cleanups (out-of-band confirmation first): without it, an uncertain
    /// mail/comment refuses to re-fire.
    pub async fn run_cleanup(
        &self,
        parent_key: &str,
        kind: CleanupKind,
        note: Option<serde_json::Value>,
        force: bool,
        provider: impl FnOnce(&CleanupRequest) -> ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        let key = cleanup_effect_key(parent_key, kind);
        let mut capability = kind.capability();
        if force {
            let row = self.store.get_effect(&key).await?;
            let needs_override = match row {
                Some(r) if !r.state.is_terminal() => {
                    let now = now_us();
                    let lease_live = matches!(r.lease_expires_at, Some(e) if e > now);
                    let view = effect_view(r.state, r.attempt, lease_live);
                    matches!(
                        plan_attempt(Some(&view), &capability),
                        DispatchPlan::RefuseBlindRetry { .. } | DispatchPlan::ReconcileFirst { .. }
                    )
                }
                _ => false,
            };
            if needs_override {
                capability = FORCED_CLEANUP_CAPABILITY;
            }
        }
        let run_id = match self.store.get_effect(parent_key).await? {
            Some(parent) => parent.run_id,
            None => parent_key.to_string(),
        };
        let action_type = format!("cleanup:{}", kind.as_str());
        let req = DispatchRequest {
            effect_key: key,
            run_id,
            project: None,
            workflow_digest: String::new(),
            node_path: String::new(),
            iteration_key: None,
            action_type,
            action_digest: String::new(),
            redacted_action: note.clone(),
            capability,
        };
        let cleanup = CleanupRequest {
            parent_key: parent_key.to_string(),
            kind,
            note,
        };
        self.dispatch(&req, |_| provider(&cleanup)).await
    }

    /// Render the operator-facing report for an effect stuck in uncertainty.
    /// Returns `None` for settled/failed rows (nothing to reconcile) and
    /// unknown keys.
    pub async fn unknown_report(&self, effect_key: &str) -> Option<UnknownReport> {
        let row = self.store.get_effect(effect_key).await.ok()??;
        match row.state {
            EffectState::Unknown | EffectState::ReconcilePending => {
                let cap = capability_for_action_type(&row.action_type);
                Some(UnknownReport {
                    effect_key: row.effect_key,
                    run_id: row.run_id,
                    node_path: row.node_path,
                    state: row.state,
                    attempt: row.attempt,
                    evidence: row.failure_json.clone().or(row.outcome_json.clone()),
                    next_step: describe_unknown(row.state, cap.supports_reconcile_query)
                        .to_string(),
                })
            }
            _ => None,
        }
    }
}
