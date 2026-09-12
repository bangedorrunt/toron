// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Fenced dispatch + reconciliation policy (ADR-0032 Spec 5.2-5.4, bd-v139.2).
//!
//! Pure decision core: given an effect's journaled state, lease liveness,
//! and the provider's declared capability, decide whether this attempt may
//! dispatch, must reconcile first, or must refuse. No I/O, no clock reads —
//! the embedding supplies the view. The host [`EffectDispatcher`] (toron-mcp)
//! executes the plan against [`WorkflowEffectStore`]; this module is the
//! total, exhaustively tested policy it executes.
//!
//! Invariants: intent precedes dispatch; one logical effect keeps one key
//! across attempts; a timeout is never proof of absence; settlement needs
//! the current fencing token; mailbox `corr` may project the effect key but
//! never replaces the journal.

use toron_core::index::workflow_effects::EffectState;

/// What the embedding's provider promises (declared per action/provider,
/// never inferred from one call's outcome).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderCapability {
    /// The provider honors the stable effect key for dedupe (safe same-key
    /// re-dispatch after uncertainty).
    pub supports_idempotency_key: bool,
    /// The provider answers "what happened to this key?" (query before any
    /// retry after uncertainty).
    pub supports_reconcile_query: bool,
}

/// One provider invocation's classified result. `Ambiguous` covers
/// timeouts, process loss, and unparseable responses: the effect may or may
/// not have happened, and the journal must say `unknown` — never infer
/// no-send from it.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderOutcome {
    Delivered(serde_json::Value),
    ProvenFailed(String),
    Ambiguous(String),
}

/// The journaled facts one dispatch decision needs (no I/O to build).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectView {
    pub state: EffectState,
    pub attempt: i64,
    /// True when another owner may still hold the dispatch lease.
    pub lease_live: bool,
}

/// What the dispatcher may do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchPlan {
    /// No intent or a fresh intent: commit intent, then dispatch attempt 1.
    DispatchNew,
    /// Uncertainty with an idempotent provider: re-dispatch the SAME
    /// logical key as this next attempt number.
    RetrySameKey { attempt: i64 },
    /// Uncertainty with a queryable provider: ask what happened before any
    /// dispatch. Never a second dispatch first.
    ReconcileFirst { reason: String },
    /// No safe move exists (live owner elsewhere, reconciler owns the row,
    /// or no provider contract): do not dispatch, say why.
    RefuseBlindRetry { reason: String },
    /// The row is terminal: return the stored outcome, dispatch nothing.
    DuplicateTerminal,
}

/// Decide one attempt from the journaled view (ADR-0032 §5.3 + §7.4 crash
/// matrix). `None` (no intent row yet) dispatches fresh. Pure and total.
pub fn plan_attempt(current: Option<&EffectView>, cap: &ProviderCapability) -> DispatchPlan {
    let Some(view) = current else {
        return DispatchPlan::DispatchNew;
    };
    use EffectState as S;
    match view.state {
        S::Settled | S::Failed | S::Cancelled => DispatchPlan::DuplicateTerminal,
        S::Prepared => DispatchPlan::DispatchNew,
        S::ReconcilePending => DispatchPlan::RefuseBlindRetry {
            reason: "reconciliation owns this effect; the dispatcher waits for its verdict".into(),
        },
        S::InFlight | S::Accepted | S::Unknown => {
            if view.lease_live {
                DispatchPlan::RefuseBlindRetry {
                    reason: "dispatch lease is live under another owner; a second dispatch would fork the effect"
                        .into(),
                }
            } else if cap.supports_reconcile_query {
                DispatchPlan::ReconcileFirst {
                    reason:
                        "outcome uncertain after an expired lease; the provider answers key queries"
                            .into(),
                }
            } else if cap.supports_idempotency_key {
                DispatchPlan::RetrySameKey {
                    attempt: view.attempt + 1,
                }
            } else {
                DispatchPlan::RefuseBlindRetry {
                    reason: "outcome uncertain and the provider offers no query or idempotency contract; reconcile by operator evidence"
                        .into(),
                }
            }
        }
    }
}

/// Settlement needs the current fencing token: the presented token must
/// equal the live owner exactly. Empty tokens never settle; an unclaimed
/// row settles for nobody.
pub fn may_settle(current_token: Option<&str>, presented: &str) -> bool {
    match (current_token, presented) {
        (Some(current), p) if !p.is_empty() => current == p,
        _ => false,
    }
}

/// A replay that renders a different action digest must not mint a new
/// logical effect under the same run/node: version/replay error (or
/// explicit reconciliation) instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayMismatch {
    pub expected: String,
    pub got: String,
}

pub fn check_replay(prepared_digest: &str, redigest: &str) -> Result<(), ReplayMismatch> {
    if prepared_digest == redigest {
        Ok(())
    } else {
        Err(ReplayMismatch {
            expected: prepared_digest.to_string(),
            got: redigest.to_string(),
        })
    }
}

/// Mailbox `corr` projection for a workflow-originated effect: deliberately
/// identical to the effect key so reconciliation can join through the
/// explicit link — while the receipt store never becomes the journal.
pub fn corr_for_effect(effect_key: &str) -> String {
    effect_key.to_string()
}

#[cfg(test)]
mod effect_reconciliation_tests {
    use super::*;
    use toron_core::index::workflow_effects::EffectState;

    fn cap(idempotent: bool, query: bool) -> ProviderCapability {
        ProviderCapability {
            supports_idempotency_key: idempotent,
            supports_reconcile_query: query,
        }
    }

    fn view(state: EffectState, attempt: i64, lease_live: bool) -> EffectView {
        EffectView {
            state,
            attempt,
            lease_live,
        }
    }

    /// bd-v139.2: the ADR §7.4 crash matrix as a decision table — every
    /// ambiguous row resolves to settled, failed, or explicit unknown
    /// evidence, never to an inferred absence or a blind retry.
    #[test]
    fn effect_reconciliation_crash_matrix() {
        use EffectState as S;
        // Before prepared commits: no intent, no dispatch — start fresh.
        assert!(matches!(
            plan_attempt(None, &cap(false, false)),
            DispatchPlan::DispatchNew
        ));
        // After prepared, before in_flight: fresh intent dispatches.
        assert!(matches!(
            plan_attempt(Some(&view(S::Prepared, 1, false)), &cap(false, false)),
            DispatchPlan::DispatchNew
        ));
        // In-flight with a live lease: another owner may be dispatching.
        assert!(matches!(
            plan_attempt(Some(&view(S::InFlight, 1, true)), &cap(true, true)),
            DispatchPlan::RefuseBlindRetry { .. }
        ));
        // In-flight, lease expired, provider queryable: reconcile first.
        assert!(matches!(
            plan_attempt(Some(&view(S::InFlight, 1, false)), &cap(false, true)),
            DispatchPlan::ReconcileFirst { .. }
        ));
        // In-flight, lease expired, idempotent-only provider: same-key retry.
        assert!(matches!(
            plan_attempt(Some(&view(S::InFlight, 1, false)), &cap(true, false)),
            DispatchPlan::RetrySameKey { attempt: 2 }
        ));
        // In-flight, lease expired, no capability: refuse, stay unknown-able.
        assert!(matches!(
            plan_attempt(Some(&view(S::InFlight, 1, false)), &cap(false, false)),
            DispatchPlan::RefuseBlindRetry { .. }
        ));
        // Accepted with lost completion evidence: like in-flight uncertainty.
        assert!(matches!(
            plan_attempt(Some(&view(S::Accepted, 1, false)), &cap(false, true)),
            DispatchPlan::ReconcileFirst { .. }
        ));
        assert!(matches!(
            plan_attempt(Some(&view(S::Accepted, 1, false)), &cap(true, false)),
            DispatchPlan::RetrySameKey { attempt: 2 }
        ));
        // Unknown outcome: query, same-key idempotent retry, or refuse.
        assert!(matches!(
            plan_attempt(Some(&view(S::Unknown, 1, false)), &cap(false, true)),
            DispatchPlan::ReconcileFirst { .. }
        ));
        assert!(matches!(
            plan_attempt(Some(&view(S::Unknown, 2, false)), &cap(true, false)),
            DispatchPlan::RetrySameKey { attempt: 3 }
        ));
        assert!(matches!(
            plan_attempt(Some(&view(S::Unknown, 1, false)), &cap(false, false)),
            DispatchPlan::RefuseBlindRetry { .. }
        ));
        // Unknown with a live lease: the owner may still complete.
        assert!(matches!(
            plan_attempt(Some(&view(S::Unknown, 1, true)), &cap(true, true)),
            DispatchPlan::RefuseBlindRetry { .. }
        ));
        // Reconcile-pending belongs to a reconciler, never the dispatcher.
        assert!(matches!(
            plan_attempt(Some(&view(S::ReconcilePending, 1, false)), &cap(true, true)),
            DispatchPlan::RefuseBlindRetry { .. }
        ));
        // Terminal rows never dispatch again — the stored outcome stands.
        for terminal in [S::Settled, S::Failed, S::Cancelled] {
            assert!(
                matches!(
                    plan_attempt(Some(&view(terminal, 2, false)), &cap(true, true)),
                    DispatchPlan::DuplicateTerminal
                ),
                "{terminal:?} must not re-dispatch"
            );
        }
    }

    /// bd-v139.2: retries reuse one logical key while attempts stay fenced —
    /// the plan carries the next attempt number, never a new identity.
    #[test]
    fn effect_reconciliation_same_key_retry() {
        use EffectState as S;
        let plan = plan_attempt(Some(&view(S::Unknown, 4, false)), &cap(true, false));
        assert!(matches!(plan, DispatchPlan::RetrySameKey { attempt: 5 }));
    }

    /// bd-v139.2: a replay that renders a different action digest must not
    /// mint a new logical effect under the same run/node — version/replay
    /// error or explicit reconciliation instead.
    #[test]
    fn effect_reconciliation_changed_replay_rejected() {
        assert!(check_replay("digest-a", "digest-a").is_ok());
        let err = check_replay("digest-a", "digest-b").unwrap_err();
        assert_eq!(err.expected, "digest-a");
        assert_eq!(err.got, "digest-b");
    }

    /// bd-v139.2: settlement needs the current fencing token — a stale
    /// owner can neither settle nor steal, with or without a live lease.
    #[test]
    fn effect_reconciliation_settle_fence() {
        assert!(may_settle(Some("tok-a"), "tok-a"));
        assert!(!may_settle(Some("tok-a"), "tok-b"));
        assert!(!may_settle(Some("tok-a"), ""));
        assert!(!may_settle(None, "tok-a"));
    }

    /// bd-v139.2: no blind retry — unknown outcomes without provider
    /// query/idempotency support refuse with a reason naming reconciliation.
    #[test]
    fn effect_reconciliation_no_blind_retry() {
        use EffectState as S;
        match plan_attempt(Some(&view(S::Unknown, 1, false)), &cap(false, false)) {
            DispatchPlan::RefuseBlindRetry { reason } => {
                assert!(reason.contains("reconcile"), "{reason}");
            }
            other => panic!("must refuse, got {other:?}"),
        }
    }

    /// bd-v139.2: mailbox `corr` may project the effect key as a deliberate
    /// link — the projection is identity, and the stores stay separate.
    #[test]
    fn effect_reconciliation_corr_projection() {
        assert_eq!(corr_for_effect("ek-abc"), "ek-abc");
    }
}
