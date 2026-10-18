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
///
/// `Deferred` is the non-Settled hand-off (GS-062 T1): burst buffered or
/// Transient-requeued awaiting kind-43001 on the wire. Maps to
/// [`EffectState::Accepted`] — not Settled/Delivered, not Ambiguous→Unknown
/// force-retry dual-publish.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderOutcome {
    Delivered(serde_json::Value),
    /// Accepted into coalescer / Transient requeue; wire publish not yet proven.
    Deferred(serde_json::Value),
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
    /// bd-ynef: the row is parked awaiting its wake time. Resume the SAME
    /// logical key at T — not a retry (nothing failed) and not a new dispatch
    /// (the intent row already exists, so DispatchNew would duplicate it).
    ResumeParked { attempt: i64 },
}

/// Decide one attempt from the journaled view (ADR-0032 §5.3 + §7.4 crash
/// matrix). `None` (no intent row yet) dispatches fresh. Pure and total.
pub fn plan_attempt(current: Option<&EffectView>, cap: &ProviderCapability) -> DispatchPlan {
    let Some(view) = current else {
        return DispatchPlan::DispatchNew;
    };
    use EffectState as S;
    match view.state {
        // bd-ynef: a park is INTERRUPTION, so it is neither a terminal row to
        // return nor a retry to attempt. It resumes at its wake time under the
        // SAME re-drive key — never DispatchNew, which would double the very
        // effect the park was waiting to protect.
        S::Parked => DispatchPlan::ResumeParked {
            attempt: view.attempt.max(1),
        },
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

// ---------------------------------------------------------------------------
// Idempotent cleanup + retention policy (ADR-0032 Spec 8.2, bd-v139.3).
//
// Cleanup is a fixed set of host effects (never a user-authored undo DSL):
// each attempt journals under its own stable key, retries converge without
// touching the original failure, and rollback of completed external work is
// never claimed.
// ---------------------------------------------------------------------------

/// The fixed cleanup set (ADR-0032 Spec 8.2): lease release, bounded
/// captain evidence, bead comment. Nothing else is a cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupKind {
    ReleaseReservations,
    CaptainEvidence,
    BeadComment,
}

impl CleanupKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReleaseReservations => "release-reservations",
            Self::CaptainEvidence => "captain-evidence",
            Self::BeadComment => "bead-comment",
        }
    }

    /// Retry safety per kind: reservation release is natively idempotent
    /// (releasing twice converges); evidence mail and bead comments are
    /// not — an ambiguous attempt refuses an unforced retry so one timeout
    /// can never duplicate operator-visible evidence.
    pub const fn capability(self) -> ProviderCapability {
        match self {
            Self::ReleaseReservations => ProviderCapability {
                supports_idempotency_key: true,
                supports_reconcile_query: false,
            },
            Self::CaptainEvidence | Self::BeadComment => ProviderCapability {
                supports_idempotency_key: false,
                supports_reconcile_query: false,
            },
        }
    }
}

/// Capability override for an operator-forced cleanup retry: the operator
/// confirmed out-of-band that the ambiguous attempt sent nothing, so the
/// same key may re-dispatch once. The force is explicit at the call site,
/// never inferred.
pub const FORCED_CLEANUP_CAPABILITY: ProviderCapability = ProviderCapability {
    supports_idempotency_key: true,
    supports_reconcile_query: false,
};

/// Stable cleanup identity: `{parent}/cleanup:{kind}`. Retries reuse it;
/// the parent failure row is never written by cleanup.
pub fn cleanup_effect_key(parent_key: &str, kind: CleanupKind) -> String {
    format!("{parent_key}/cleanup:{}", kind.as_str())
}

/// One cleanup invocation (what the provider closure receives).
#[derive(Debug, Clone)]
pub struct CleanupRequest {
    pub parent_key: String,
    pub kind: CleanupKind,
    pub note: Option<serde_json::Value>,
}

/// Map a journaled action type back to its retry capability for reporting.
/// Unknown actions are conservative: no idempotency, no query.
pub fn capability_for_action_type(action_type: &str) -> ProviderCapability {
    match action_type {
        "cleanup:release-reservations" => CleanupKind::ReleaseReservations.capability(),
        _ => ProviderCapability {
            supports_idempotency_key: false,
            supports_reconcile_query: false,
        },
    }
}

/// Operator-facing report for an effect stuck in uncertainty.
#[derive(Debug, Clone, PartialEq)]
pub struct UnknownReport {
    pub effect_key: String,
    pub run_id: String,
    pub node_path: String,
    pub state: EffectState,
    pub attempt: i64,
    pub evidence: Option<String>,
    pub next_step: String,
}

/// The actionable next check for an uncertain effect. Names who moves
/// next — provider query, reconciler, or operator — never a bare state.
pub const fn describe_unknown(state: EffectState, has_reconcile_path: bool) -> &'static str {
    match (state, has_reconcile_path) {
        (EffectState::Unknown, true) => {
            "query the provider with the stable effect key, then settle or fail with the returned evidence"
        }
        (EffectState::Unknown, false) => {
            "operator decision required: confirm the external outcome out-of-band, then force-retry or fail the effect"
        }
        (EffectState::ReconcilePending, _) => "a reconciler owns the next check; do not dispatch",
        _ => "no reconciliation needed",
    }
}

/// Retention/compaction policy (ADR-0032 Spec 5.1 + §9 residual risks).
/// Terminal effects prune by their own `retention_until`; published
/// terminals and terminal runs prune by age. Parked pending rows and
/// uncertain effects (in-flight, unknown, reconcile-pending) are NEVER
/// pruned: live work and uncertainty outlive any evidence window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Maximum age (µs) for published terminals and terminal runs.
    pub terminal_max_age_us: i64,
    /// Maximum rows deleted per table per run (bounded compaction).
    pub batch_limit: i64,
}

impl Default for RetentionPolicy {
    /// Thirty days of terminal evidence, 500 rows per table per run.
    fn default() -> Self {
        Self {
            terminal_max_age_us: 30 * 24 * 3_600 * 1_000_000,
            batch_limit: 500,
        }
    }
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

    /// bd-b123: the framework's `plan_attempt` and toron's must agree on
    /// every vector. Reason strings are deliberately NOT compared (each
    /// side words its own refusal); the plan shape is the contract:
    /// Prepared dispatches, ReconcilePending refuses, query beats
    /// idempotency, ResumeParked takes attempt.max(1), RetrySameKey takes
    /// attempt + 1. A mismatch is a bug in the newer copy.
    #[test]
    fn plan_attempt_parity() {
        use EffectState as S;
        use toron_core::kernel as fw;

        fn shape_local(p: &DispatchPlan) -> String {
            match p {
                DispatchPlan::DispatchNew => "new".into(),
                DispatchPlan::RetrySameKey { attempt } => format!("retry:{attempt}"),
                DispatchPlan::ReconcileFirst { .. } => "reconcile".into(),
                DispatchPlan::RefuseBlindRetry { .. } => "refuse".into(),
                DispatchPlan::DuplicateTerminal => "terminal".into(),
                DispatchPlan::ResumeParked { attempt } => format!("park:{attempt}"),
            }
        }

        fn shape_fw(p: &fw::DispatchPlan) -> String {
            match p {
                fw::DispatchPlan::DispatchNew => "new".into(),
                fw::DispatchPlan::RetrySameKey { attempt } => format!("retry:{attempt}"),
                fw::DispatchPlan::ReconcileFirst { .. } => "reconcile".into(),
                fw::DispatchPlan::RefuseBlindRetry { .. } => "refuse".into(),
                fw::DispatchPlan::DuplicateTerminal => "terminal".into(),
                fw::DispatchPlan::ResumeParked { attempt } => format!("park:{attempt}"),
            }
        }

        fn state_fw(s: EffectState) -> fw::EffectState {
            match s {
                S::Prepared => fw::EffectState::Prepared,
                S::InFlight => fw::EffectState::InFlight,
                S::Accepted => fw::EffectState::Accepted,
                S::Settled => fw::EffectState::Settled,
                S::Failed => fw::EffectState::Failed,
                S::Unknown => fw::EffectState::Unknown,
                S::ReconcilePending => fw::EffectState::ReconcilePending,
                S::Cancelled => fw::EffectState::Cancelled,
                S::Parked => fw::EffectState::Parked,
            }
        }

        let states = [
            S::Prepared,
            S::InFlight,
            S::Accepted,
            S::Settled,
            S::Failed,
            S::Unknown,
            S::ReconcilePending,
            S::Cancelled,
            S::Parked,
        ];
        // No intent row yet: both dispatch fresh under every capability.
        for cap in [cap(false, false), cap(false, true), cap(true, false), cap(true, true)] {
            let fw_cap = fw::ProviderCapability {
                supports_idempotency_key: cap.supports_idempotency_key,
                supports_reconcile_query: cap.supports_reconcile_query,
            };
            assert_eq!(
                shape_local(&plan_attempt(None, &cap)),
                shape_fw(&fw::plan_attempt(None, &fw_cap)),
                "no-intent row must dispatch fresh"
            );
        }
        let mut vectors = 0;
        for state in states {
            for lease_live in [false, true] {
                for attempt in [0, 1, 3] {
                    for cap in [cap(false, false), cap(false, true), cap(true, false), cap(true, true)]
                    {
                        let fw_cap = fw::ProviderCapability {
                            supports_idempotency_key: cap.supports_idempotency_key,
                            supports_reconcile_query: cap.supports_reconcile_query,
                        };
                        let local = plan_attempt(Some(&view(state, attempt, lease_live)), &cap);
                        let fw_view = fw::EffectView {
                            state: state_fw(state),
                            attempt,
                            lease_live,
                        };
                        let other = fw::plan_attempt(Some(&fw_view), &fw_cap);
                        assert_eq!(
                            shape_local(&local),
                            shape_fw(&other),
                            "state={state:?} lease={lease_live} attempt={attempt} cap={cap:?}"
                        );
                        vectors += 1;
                    }
                }
            }
        }
        // Negative control: the table actually ran every vector.
        assert_eq!(vectors, 9 * 2 * 3 * 4);
    }
}
