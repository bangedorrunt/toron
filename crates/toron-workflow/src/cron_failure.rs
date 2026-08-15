// governed-by: ADR-0005 (P4.4 — bead-on-failure, Spec 3 loop closure)
#![forbid(unsafe_code)]

//! Cron failure policy (ADR-0005 Spec 3, P4.4): decide what a FAILED cron run
//! does, and surface the decision to the embedding.
//!
//! toron never imports flywheel (ADR-0003 3A), and `br create` + kind-14 DMs
//! are flywheel capabilities. So this module owns the DECISION (pure, TDD)
//! and records it on a [`PolicySink`] side-channel (same idiom as
//! [`crate::engine::ApprovalSink`]); the embedding reads the sink and performs
//! the side effects (bead / escalate DM). The decision is:
//!
//! - `on_failure` for the first failure,
//! - `on_repeat_failure` once `repeat_threshold` CONSECUTIVE failures pass,
//! - `on_success` resets the streak.
//!
//! The consecutive counter lives in the durable cron loop (a local — replay
//! re-derives it deterministically), so the streak survives a crash but is
//! never double-counted.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::schema::{CronPolicy, FailureAction};

/// The failure bundle recorded on the sink (the `br create` payload when the
/// action is `Bead`; the DM payload when `Escalate`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureBundle {
    /// The failed run id (`cron:{name}:{tick}`).
    pub run_id: String,
    /// The workflow that failed.
    pub workflow: String,
    /// The owning profile (Spec 3 — the fixer's identity).
    pub owner: String,
    /// The step error message (from the run output's `error`).
    pub error: String,
    /// The step that failed (`failed_step`), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<String>,
    /// Consecutive failures so far (>= 1).
    pub consecutive: u32,
}

/// A policy decision recorded by the cron loop for the embedding to execute.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyEvent {
    /// The cron name.
    pub cron: String,
    /// The scheduled tick (unix seconds).
    pub tick: i64,
    /// The resolved action (bead / escalate / log).
    pub action: FailureAction,
    /// The failure bundle (log actions still carry it for observability).
    pub bundle: FailureBundle,
    /// The tick's full workflow run output (bd-m7ac weekly-review): present
    /// when the workflow ran to a terminal state. The embedding reads it for
    /// success side-effects (e.g. the weekly review bead from the oracle
    /// step's synthesis). Additive — absent in AM-Rust and for log-only
    /// consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
}

/// Side-channel for policy decisions (distinct newtype — the Resonate
/// dependency map keys by `TypeId`, so this must NOT be a bare
/// `Arc<Mutex<Vec<…>>>` alias, or it collides with the other sinks).
#[derive(Clone, Default)]
pub struct PolicySink(Arc<tokio::sync::Mutex<Vec<PolicyEvent>>>);

impl std::ops::Deref for PolicySink {
    type Target = tokio::sync::Mutex<Vec<PolicyEvent>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PolicySink {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Extract a failure from a workflow run's output, if it failed. A run output
/// `{"status":"failed","error":…,"failed_step":…}` (kind-46012 status=failed,
/// P0) is a failure; anything else is not.
pub fn is_failed(output: &serde_json::Value) -> bool {
    output.get("status").and_then(serde_json::Value::as_str) == Some("failed")
}

/// Extract the failure's error + failed step from a failed run output.
pub fn failure_details(output: &serde_json::Value) -> (String, Option<String>) {
    let error = output
        .get("error")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("run failed (no error field)")
        .to_string();
    let failed_step = output
        .get("failed_step")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    (error, failed_step)
}

/// Decide the policy action + the next consecutive-failure count.
///
/// - On failure: `consecutive` increments. If it has reached `repeat_threshold`,
///   the action is `on_repeat_failure`; otherwise `on_failure`.
/// - On success: the streak resets to 0; the action is `on_success`.
///
/// Pure (no IO) — the caller applies side effects via the sink.
pub fn evaluate(policy: &CronPolicy, failed: bool, consecutive: u32) -> (FailureAction, u32) {
    if !failed {
        return (policy.on_success, 0);
    }
    let next = consecutive.saturating_add(1);
    let action = if next >= policy.repeat_threshold.max(1) {
        policy.on_repeat_failure
    } else {
        policy.on_failure
    };
    (action, next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(on_failure: FailureAction, on_repeat: FailureAction, threshold: u32) -> CronPolicy {
        CronPolicy {
            on_failure,
            on_repeat_failure: on_repeat,
            repeat_threshold: threshold,
            on_success: FailureAction::Log,
            max_retries: 0,
            no_claim_timeout: 600,
        }
    }

    /// First failure → `on_failure` (bead), streak = 1.
    #[test]
    fn first_failure_uses_on_failure() {
        let p = policy(FailureAction::Bead, FailureAction::Escalate, 3);
        let (action, streak) = evaluate(&p, true, 0);
        assert_eq!(action, FailureAction::Bead);
        assert_eq!(streak, 1);
    }

    /// Failures below the threshold use `on_failure`; the N-th uses
    /// `on_repeat_failure` (threshold is exact — N-1 do NOT escalate).
    #[test]
    fn repeat_failure_triggers_at_exact_threshold() {
        let p = policy(FailureAction::Log, FailureAction::Escalate, 3);
        for streak in 0..2 {
            let (a, s) = evaluate(&p, true, streak);
            assert_eq!(a, FailureAction::Log, "below threshold uses on_failure");
            assert_eq!(s, streak + 1);
        }
        let (a, s) = evaluate(&p, true, 2);
        assert_eq!(a, FailureAction::Escalate, "3rd failure escalates");
        assert_eq!(s, 3);
    }

    /// A success resets the streak to 0 and fires `on_success`.
    #[test]
    fn success_resets_streak_and_fires_on_success() {
        let p = policy(FailureAction::Bead, FailureAction::Escalate, 3);
        let (action, streak) = evaluate(&p, false, 3);
        assert_eq!(action, FailureAction::Log);
        assert_eq!(streak, 0);
    }

    /// `is_failed` recognizes the kind-46012 failed shape and ignores success.
    #[test]
    fn is_failed_detects_the_46012_shape() {
        assert!(is_failed(
            &serde_json::json!({"status": "failed", "error": "boom"})
        ));
        assert!(!is_failed(&serde_json::json!({"status": "succeeded"})));
        assert!(!is_failed(&serde_json::json!({"notify": "ok"})));
        assert!(!is_failed(&serde_json::json!({})));
    }

    /// `failure_details` extracts error + failed_step.
    #[test]
    fn failure_details_extracts_error_and_step() {
        let out = serde_json::json!({"status": "failed", "error": "boom", "failed_step": "build"});
        let (err, step) = failure_details(&out);
        assert_eq!(err, "boom");
        assert_eq!(step.as_deref(), Some("build"));
    }

    /// A threshold of 0 is treated as 1 (always escalate on first failure).
    #[test]
    fn zero_threshold_clamps_to_one() {
        let p = policy(FailureAction::Bead, FailureAction::Escalate, 0);
        let (action, streak) = evaluate(&p, true, 0);
        assert_eq!(action, FailureAction::Escalate, "threshold 0 clamps to 1");
        assert_eq!(streak, 1);
    }

    /// bd-m7ac: a weekly-review tick's output flows to the embedding through
    /// the PolicyEvent (additive field, default-absent for log-only crons).
    #[test]
    fn policy_event_carries_tick_output() {
        let out = serde_json::json!({
            "oracle": { "idea": { "title": "t", "what": "w" } }
        });
        let ev = PolicyEvent {
            cron: "weekly-review".into(),
            tick: 1720000000,
            action: FailureAction::Log,
            bundle: FailureBundle {
                run_id: "cron:weekly-review:1720000000".into(),
                workflow: "weekly-review".into(),
                owner: "oracle".into(),
                error: String::new(),
                failed_step: None,
                consecutive: 0,
            },
            output: Some(out.clone()),
        };
        assert_eq!(ev.output.as_ref().unwrap(), &out);
        // Round-trips through serde (the sink's wire format).
        let s = serde_json::to_string(&ev).expect("serializes");
        let back: PolicyEvent = serde_json::from_str(&s).expect("deserializes");
        assert_eq!(back.output.as_ref().unwrap(), &out);
        // Absent output round-trips as None (backward compat).
        let mut ev2 = ev.clone();
        ev2.output = None;
        let s2 = serde_json::to_string(&ev2).expect("serializes");
        assert!(!s2.contains("\"output\""), "None output is skipped");
        let back2: PolicyEvent = serde_json::from_str(&s2).expect("deserializes");
        assert!(back2.output.is_none());
    }
}
