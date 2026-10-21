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

/// The plan types are the kernel's (bd-6yjq): one spelling beside the
/// framework, not a local copy. Policy (`plan_attempt`, `may_settle`,
/// `capability_for_action_type`) stays here, pinned by the parity table.
pub use toron_core::kernel::{DispatchPlan, EffectView, ProviderCapability};

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
/// Core rows map into the kernel view with [`effect_view`]; the view
/// embeds the kernel state spelling.
pub fn effect_view(state: EffectState, attempt: i64, lease_live: bool) -> EffectView {
    EffectView {
        state: kernel_state(state),
        attempt,
        lease_live,
    }
}

/// Core journaled state to kernel state, variant for variant (bd-6yjq).
fn kernel_state(state: EffectState) -> toron_core::kernel::EffectState {
    use toron_core::kernel::EffectState as K;
    match state {
        EffectState::Prepared => K::Prepared,
        EffectState::InFlight => K::InFlight,
        EffectState::Accepted => K::Accepted,
        EffectState::Settled => K::Settled,
        EffectState::Failed => K::Failed,
        EffectState::Unknown => K::Unknown,
        EffectState::ReconcilePending => K::ReconcilePending,
        EffectState::Cancelled => K::Cancelled,
        EffectState::Parked => K::Parked,
    }
}

/// Decide one attempt from the journaled view (ADR-0032 §5.3 + §7.4 crash
/// matrix). `None` (no intent row yet) dispatches fresh. Pure and total.
pub fn plan_attempt(current: Option<&EffectView>, cap: &ProviderCapability) -> DispatchPlan {
    let Some(view) = current else {
        return DispatchPlan::DispatchNew;
    };
    use toron_core::kernel::EffectState as S;
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
/// row settles for nobody. The kernel's fence, re-exported (bd-g31f):
/// verified identical on all four empty-token corners, so the local copy
/// is deleted rather than kept agreeing by test.
pub use toron_core::kernel::may_settle;

/// A replay that renders a different action digest must not mint a new
/// logical effect under the same run/node: version/replay error (or
/// explicit reconciliation) instead.
///
/// bd-vt9y: the predicate lives in the journal store, because that is where
/// the production prepare path runs it — a re-render that changed the digest
/// for the same logical node now refuses instead of minting a second key.
/// This is a re-export, so the guard the engine exercises is the guard that
/// refuses.
pub use toron_core::index::workflow_effects::{ReplayMismatch, check_replay};

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

    /// bd-vt9y: ADR-0032 I8a (replay identity) — once an effect reaches
    /// `prepared`, its identity and key are fixed for that logical node. A
    /// re-drive that re-renders the node differently must REFUSE, not mint a
    /// second effect key: the second key made the external effect fire
    /// twice, and the guard that should have stopped it
    /// ([`check_replay`]) had no production caller at all.
    ///
    /// This drives the PRODUCTION prepare path (the journal store the MCP
    /// host dispatches through), not the predicate in isolation.
    #[tokio::test]
    async fn replay_refuses_a_changed_rerender_in_the_production_prepare_path() {
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::{NewEffect, WorkflowEffectStore};

        let dir = std::env::temp_dir().join(format!("toron-replay-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let db = dir.join("wfx.sqlite");
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let store = WorkflowEffectStore::new(pool);

        let node = |key: &str, digest: &str| NewEffect {
            effect_key: key.into(),
            run_id: "run-1".into(),
            project: Some("demo".into()),
            workflow_digest: "wf-digest".into(),
            node_path: "gate".into(),
            iteration_key: None,
            action_type: "send_message".into(),
            action_digest: digest.into(),
            redacted_action: None,
            retention_until: None,
        };

        let first = store
            .prepare_effect(node("ek-1", "digest-a"))
            .await
            .expect("the first prepare creates the row");
        assert!(first.created, "the first prepare creates the row");

        // The SAME render attaches: idempotent, never a second effect.
        let again = store
            .prepare_effect(node("ek-1", "digest-a"))
            .await
            .expect("the same render attaches");
        assert!(!again.created, "the same render is idempotent");

        // A CHANGED render for the same logical node must REFUSE.
        let err = store
            .prepare_effect(node("ek-2", "digest-b"))
            .await
            .expect_err("a changed re-render must refuse, not mint a second key");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("digest-a") && msg.contains("digest-b"),
            "the refusal names both digests: {msg}"
        );
        assert!(
            store.get_effect("ek-2").await.expect("read").is_none(),
            "the refused prepare left no second row"
        );
        assert!(
            store.get_effect("ek-1").await.expect("read").is_some(),
            "the prepared row survives the refusal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn cap(idempotent: bool, query: bool) -> ProviderCapability {
        ProviderCapability {
            supports_idempotency_key: idempotent,
            supports_reconcile_query: query,
        }
    }

    fn view(state: EffectState, attempt: i64, lease_live: bool) -> EffectView {
        effect_view(state, attempt, lease_live)
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

    /// The framework's `plan_attempt` and toron's must agree on every
    /// vector — over the SAME shared view value now that the plan types
    /// are the kernel's. Reason strings are deliberately NOT compared (each side words its own refusal); the plan shape is
    /// the contract. A mismatch is a bug in the newer copy.
    #[test]
    fn plan_attempt_parity() {
        use EffectState as S;
        use toron_core::kernel as fw;

        fn shape(p: &DispatchPlan) -> String {
            match p {
                DispatchPlan::DispatchNew => "new".into(),
                DispatchPlan::RetrySameKey { attempt } => format!("retry:{attempt}"),
                DispatchPlan::ReconcileFirst { .. } => "reconcile".into(),
                DispatchPlan::RefuseBlindRetry { .. } => "refuse".into(),
                DispatchPlan::DuplicateTerminal => "terminal".into(),
                DispatchPlan::ResumeParked { attempt } => format!("park:{attempt}"),
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
        for cap in [
            cap(false, false),
            cap(false, true),
            cap(true, false),
            cap(true, true),
        ] {
            assert_eq!(
                shape(&plan_attempt(None, &cap)),
                shape(&fw::plan_attempt(None, &cap)),
                "no-intent row must dispatch fresh"
            );
        }
        let mut vectors = 0;
        for state in states {
            for lease_live in [false, true] {
                for attempt in [0, 1, 3] {
                    for cap in [
                        cap(false, false),
                        cap(false, true),
                        cap(true, false),
                        cap(true, true),
                    ] {
                        let v = view(state, attempt, lease_live);
                        let local = plan_attempt(Some(&v), &cap);
                        let other = fw::plan_attempt(Some(&v), &cap);
                        assert_eq!(
                            shape(&local),
                            shape(&other),
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
