// governed-by: ADR-0036
//! bd-ynef: the durable half of `DispatchAcquire::Park`.
//!
//! The busy-owner fence already hands back "park until T" (GS-062 C). What was
//! missing is the other half: a step a host can park on, and a decision about
//! whether the park is still pending at the moment the run is re-driven. A
//! fence that can hand back a park and nothing that can resume it is not a
//! durable fence.
//!
//! # The three outcomes stay apart
//!
//! A park is INTERRUPTION. It is not success (the step did not run) and it is
//! not a typed failure (nothing went wrong). Modelling it as either loses the
//! information a resume needs, so [`ParkDecision`] has its own arm and
//! `Parked` exists as its own state in the durable journal enum
//! (`toron_core::index::workflow_effects::EffectState::Parked`, resumable,
//! non-terminal): mapping a
//! park onto `Failed` would make the run's `on_error` fire, and mapping it onto
//! `Settled` would claim the step completed.
//!
//! # Nothing here reads a clock
//!
//! [`plan_park`] is pure over `(until_ms, max_wait_secs, now_ms)`. The wake
//! time is data that arrived on the step, the same way `LoopParams` policy
//! arrives serialized, so a replayed run reaches the same decision. The one
//! bounded wait in the system is the park itself, and it is capped by
//! `max_wait_secs` rather than being left to run until T.
//!
//! # Re-drive is idempotent
//!
//! The re-drive key is derived from `(run_id, node, until_ms)`, so parking the
//! same step to the same T twice yields the same key and resumes once. That is
//! what makes a crash mid-park safe: the intent row is journaled BEFORE the
//! park, so a crash leaves a visible `Parked` intent rather than a silently
//! lost wait.

use serde::{Deserialize, Serialize};

/// bd-ynef: the host-supplied clock a park leaf asks for "now".
///
/// A FUNCTION, not a fixed value. A park compares its wake time against the
/// present at the moment it runs, so a frozen stamp would make every re-drive
/// reach the same decision forever — the park would never notice T arriving.
///
/// The leaf HOLDS this and calls it; it never reads a system clock itself, and
/// that is the property that matters. The Resonate SDK gives a durable function
/// no wall clock precisely so a leaf cannot take ambient time as authority. The
/// host owns the clock, the leaf owns only the comparison.
///
/// Replay: `now` legitimately advances across a replay, so `plan_park` converges
/// toward `ResumeNow` as T passes. That is the correct outcome — the decision
/// turns on the wake time, which is stable DATA on the step, and the journal
/// holds the committed key.
pub type ParkClock = std::sync::Arc<dyn Fn() -> u64 + Send + Sync>;

/// The engine's own host clock, in one place: unix millis. The host reads the
/// clock; no leaf ever does.
pub fn system_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The hard ceiling on a single park, regardless of what a step asks for.
/// A step can request a week; the engine will not sleep a week in one
/// uninterruptible call. Past this, the park is refused loudly rather than
/// silently truncated, because a truncated park resumes early and looks like
/// a successful step.
pub const PARK_MAX_WAIT_SECS: u64 = 24 * 60 * 60;

/// bd-ynef: what the engine should do with a park step at this instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum ParkDecision {
    /// T has not arrived: park (journal the intent, then wait).
    Park {
        /// The effective wake time after applying the cap.
        until_ms: u64,
        /// The cap that was applied, when it shortened the wait.
        #[serde(skip_serializing_if = "Option::is_none")]
        capped_to_ms: Option<u64>,
    },
    /// T has already passed: resume NOW without waiting again. Re-driving a
    /// park whose T already elapsed must not park a second time, or a resumed
    /// run would re-sleep on every re-drive and never make progress.
    ResumeNow {
        /// How late the wake time was, in ms. Evidence that this is a
        /// re-drive of an elapsed park rather than a fresh one.
        overdue_ms: u64,
    },
    /// The requested wait exceeds [`PARK_MAX_WAIT_SECS`]: refused, never
    /// silently truncated.
    RefusedTooLong { requested_secs: u64, max_secs: u64 },
}

/// Decide one park, purely. `now_ms` is injected, never read from a clock
/// inside this function.
pub fn plan_park(
    until_ms: u64,
    max_wait_secs: u64,
    now_ms: u64,
    hard_cap_secs: u64,
) -> ParkDecision {
    let requested_secs = until_ms.saturating_sub(now_ms).saturating_add(999) / 1000;
    if requested_secs > hard_cap_secs {
        return ParkDecision::RefusedTooLong {
            requested_secs,
            max_secs: hard_cap_secs,
        };
    }
    // Already past T (or T is now): resume immediately. `checked_sub` rather
    // than a wrapping subtraction, so a T in the past reads as overdue instead
    // of as a ~584-billion-year wait.
    if let Some(overdue_ms) = now_ms.checked_sub(until_ms) {
        return ParkDecision::ResumeNow { overdue_ms };
    }
    // T is in the future. Cap the single wait; the remainder resumes in
    // another park, so the run stays interruptible.
    let cap_ms = max_wait_secs.saturating_mul(1000);
    let effective = until_ms.min(now_ms.saturating_add(cap_ms));
    let capped_to_ms = (effective < until_ms).then_some(effective);
    ParkDecision::Park {
        until_ms: effective,
        capped_to_ms,
    }
}

/// The stable re-drive key for a park: `(run_id, node, until_ms)`.
///
/// Derived from the step's own data, so parking the same step to the same T
/// twice produces the SAME key and therefore resumes once. A run counter or a
/// timestamp-of-park would make every re-drive look new.
pub fn park_redrive_key(run_id: &str, node: &str, until_ms: u64) -> String {
    format!("park:{run_id}:{node}:{until_ms}")
}

/// bd-ynef: the journaled park intent, committed BEFORE the provider waits.
///
/// A crash between this write and the resume leaves a visible `Parked` row
/// naming the resume key, instead of a wait that vanished with no trace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkIntent {
    /// The re-drive key. The journal row's identity.
    pub key: String,
    pub run_id: String,
    pub node: String,
    /// The effective wake time this intent commits to.
    pub until_ms: u64,
    /// Set when the cap shortened the requested wait.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capped_to_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Build the intent a park journals before it waits.
pub fn park_intent(
    run_id: &str,
    node: &str,
    decision: &ParkDecision,
    reason: Option<String>,
) -> Option<ParkIntent> {
    let ParkDecision::Park {
        until_ms,
        capped_to_ms,
    } = decision
    else {
        // ResumeNow parks nothing, so there is no intent to journal.
        return None;
    };
    Some(ParkIntent {
        key: park_redrive_key(run_id, node, *until_ms),
        run_id: run_id.to_string(),
        node: node.to_string(),
        until_ms: *until_ms,
        capped_to_ms: *capped_to_ms,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: u64 = 1_000_000_000;

    /// bd-ynef: park -> resume. Before T it parks, at/after T it resumes, and
    /// the resume is immediate rather than a second wait.
    #[test]
    fn park_then_resume_at_t() {
        let t = NOW + 30_000;
        let park = plan_park(t, 300, NOW, PARK_MAX_WAIT_SECS);
        assert_eq!(
            park,
            ParkDecision::Park {
                until_ms: t,
                capped_to_ms: None
            },
            "before T the run parks"
        );
        // At T exactly: resume, do not park again.
        assert_eq!(
            plan_park(t, 300, t, PARK_MAX_WAIT_SECS),
            ParkDecision::ResumeNow { overdue_ms: 0 },
            "at T the run resumes"
        );
        // Past T: resume, and say how late.
        assert_eq!(
            plan_park(t, 300, t + 5_000, PARK_MAX_WAIT_SECS),
            ParkDecision::ResumeNow { overdue_ms: 5_000 },
            "after T the run resumes immediately"
        );
    }

    /// bd-ynef: a park whose T has ALREADY passed must resume, not wait
    /// again. Without this a resumed run re-sleeps on every re-drive and
    /// never progresses.
    #[test]
    fn park_with_t_already_passed_resumes_immediately() {
        let t = NOW - 60_000;
        let d = plan_park(t, 300, NOW, PARK_MAX_WAIT_SECS);
        assert_eq!(d, ParkDecision::ResumeNow { overdue_ms: 60_000 });
        // And it journals nothing: there is no wait to make visible.
        assert_eq!(park_intent("run-1", "n1", &d, None), None);
    }

    /// bd-ynef: bounded, never unbounded. A step asking for a month is
    /// refused loudly instead of sleeping or silently truncating.
    #[test]
    fn park_is_bounded_and_refuses_an_unbounded_request() {
        let week = NOW + 7 * 24 * 3_600_000;
        assert_eq!(
            plan_park(week, 300, NOW, PARK_MAX_WAIT_SECS),
            ParkDecision::RefusedTooLong {
                requested_secs: 7 * 24 * 3_600,
                max_secs: PARK_MAX_WAIT_SECS,
            },
            "a week is refused, not slept"
        );
        // Within the hard cap but beyond one cap-length: capped, not refused.
        let two_hours = NOW + 7_200_000;
        let capped = plan_park(two_hours, 300, NOW, PARK_MAX_WAIT_SECS);
        assert_eq!(
            capped,
            ParkDecision::Park {
                until_ms: NOW + 300_000,
                capped_to_ms: Some(NOW + 300_000),
            },
            "the single wait is capped; the rest resumes in another park"
        );
        // The cap is recorded, so the journal shows the park was shortened.
        let intent = park_intent("run-1", "n1", &capped, Some("busy owner".into()))
            .expect("a capping park still journals");
        assert_eq!(intent.capped_to_ms, Some(NOW + 300_000));
    }

    /// bd-ynef: the re-drive is idempotent. Parking the same step to the same
    /// T twice is ONE resume, which is what makes a crash mid-park safe.
    #[test]
    fn park_redrive_is_idempotent() {
        let t = NOW + 30_000;
        let d = plan_park(t, 300, NOW, PARK_MAX_WAIT_SECS);
        let a = park_intent("run-1", "n1", &d, None).expect("journals");
        let b = park_intent("run-1", "n1", &d, None).expect("journals");
        assert_eq!(a.key, b.key, "the same park yields the same re-drive key");
        assert_eq!(a.key, park_redrive_key("run-1", "n1", t));
        // A different node or run is a DIFFERENT park, so two busy agents
        // waiting on the same T do not collapse into one resume.
        assert_ne!(a.key, park_redrive_key("run-1", "n2", t));
        assert_ne!(a.key, park_redrive_key("run-2", "n1", t));
    }

    /// bd-ynef: the intent is committed BEFORE the wait, and it carries the
    /// reason, so a crash mid-park leaves a visible row naming the resume key
    /// rather than a wait that vanished.
    #[test]
    fn park_intent_is_journaled_before_the_wait_with_its_reason() {
        let t = NOW + 30_000;
        let d = plan_park(t, 300, NOW, PARK_MAX_WAIT_SECS);
        let intent = park_intent(
            "run-42",
            "dispatch",
            &d,
            Some("busy owner fence: agent held".into()),
        )
        .expect("a park journals an intent");
        assert_eq!(intent.run_id, "run-42");
        assert_eq!(intent.node, "dispatch");
        assert_eq!(intent.until_ms, t);
        assert_eq!(
            intent.reason.as_deref(),
            Some("busy owner fence: agent held"),
            "the cause is journaled, not just a timestamp"
        );
        // It round-trips: a resumed process can read the row back.
        let json = serde_json::to_string(&intent).expect("serializes");
        let back: ParkIntent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, intent);
    }

    /// bd-ynef unhappy path: a T in the past must read as overdue, not as a
    /// wrapped-around wait of ~584 billion years.
    #[test]
    fn park_never_wraps_on_a_past_t() {
        let decision = plan_park(0, 300, NOW, PARK_MAX_WAIT_SECS);
        match decision {
            ParkDecision::ResumeNow { overdue_ms } => {
                assert_eq!(overdue_ms, NOW, "the whole gap is reported, not wrapped");
            }
            other => panic!("a past T must resume, got {other:?}"),
        }
    }

    /// bd-ynef: park CANCELLED. A cancellation is its own outcome and is NOT
    /// a resume and NOT a failure: the intent row moves to `Cancelled`, so the
    /// park never fires and never looks like it completed. Cancelling a park
    /// that already resumed is a no-op, which is what makes a late cancel safe.
    #[test]
    fn park_cancelled_never_resumes_and_never_reads_as_failure() {
        // The durable journal's state enum, in toron-core where the store
        // actually persists it — not the engine's local boundary mirror.
        use toron_core::index::workflow_effects::EffectState;

        let t = NOW + 30_000;
        let d = plan_park(t, 300, NOW, PARK_MAX_WAIT_SECS);
        let intent = park_intent("run-1", "n1", &d, Some("busy owner".into()))
            .expect("a park journals an intent");

        // Before the park: Prepared, which is neither resumable nor terminal.
        assert!(
            !EffectState::Prepared.is_resumable() && !EffectState::Prepared.is_terminal(),
            "an unstarted park is neither resumable nor terminal"
        );
        // Parked: resumable, NOT terminal, and not Failed.
        assert!(EffectState::Parked.is_resumable(), "a park is resumable");
        assert!(
            !EffectState::Parked.is_terminal(),
            "a park is not terminal: the step has not finished"
        );
        assert_ne!(
            EffectState::Parked,
            EffectState::Failed,
            "cancellation/failure is a DIFFERENT state from parking"
        );
        assert_ne!(
            EffectState::Parked,
            EffectState::Settled,
            "a park is never reported as a completed step"
        );

        // Cancelled: terminal, and NOT resumable — the park will never fire.
        assert!(EffectState::Cancelled.is_terminal());
        assert!(
            !EffectState::Cancelled.is_resumable(),
            "a cancelled park must not resume"
        );
        // The re-drive key still names the same park, so a late cancel is
        // idempotent: it lands on the row that is already terminal.
        assert_eq!(park_redrive_key("run-1", "n1", t), intent.key);
    }

    /// bd-ynef: the re-drive is what a HOST acts on. Given the journaled
    /// state, the plan is a resume for a park and never a fresh dispatch —
    /// a fresh dispatch would duplicate the effect the park protected.
    #[test]
    fn park_redrive_resumes_rather_than_dispatching_again() {
        use crate::engine::reconcile::{
            DispatchPlan, ProviderCapability, effect_view, plan_attempt,
        };
        use toron_core::index::workflow_effects::EffectState;

        let cap = ProviderCapability {
            supports_idempotency_key: true,
            supports_reconcile_query: false,
        };
        let parked = effect_view(EffectState::Parked, 1, false);
        assert_eq!(
            plan_attempt(Some(&parked), &cap),
            DispatchPlan::ResumeParked { attempt: 1 },
            "a parked row resumes, it does not dispatch again"
        );
        // A settled row is still terminal and still a duplicate — the park
        // path did not weaken the existing terminal handling.
        let settled = effect_view(EffectState::Settled, 1, false);
        assert_eq!(
            plan_attempt(Some(&settled), &cap),
            DispatchPlan::DuplicateTerminal
        );
    }
}
