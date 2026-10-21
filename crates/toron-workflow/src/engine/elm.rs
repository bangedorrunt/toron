//! Framework pins for the toron workflow engine plane.
//!
//! Stack convention: every plane keeps its framework adapter in a module
//! named `elm` (beads `src/elm`, flywheel `src/elm`, chiebukuro pipeline
//! `elm` modules). `toron_core::kernel` stays the single re-export hub
//! for the vendored crates.
//!
//! The persisted journal lives in `toron-core` (the store) and the decision
//! shapes live in the framework journal (`toron_core::kernel`). The engine
//! already speaks kernel types (`reconcile` re-exports its `EffectView`,
//! `DispatchPlan` and `ProviderCapability` from the kernel). This module
//! pins that contract: one persisted `EffectState` (the core one), the park
//! ceiling both sides enforce, and the rule that a row already `Parked`
//! resumes instead of failing.

use toron_core::kernel::{self as kernel};

/// The ceiling the framework enforces when the caller passes no override.
/// Read off behavior (an unbounded ask is refused with the ceiling), not off
/// a constant, so a drift in either copy fails loudly here.
pub fn framework_ceiling_secs() -> u64 {
    match kernel::plan_park(1, u64::MAX, 0, None) {
        kernel::ParkDecision::RefusedOverCeiling { ceiling_secs, .. } => ceiling_secs,
        other => panic!("unbounded park ask must be refused, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::park::PARK_MAX_WAIT_SECS;
    use super::super::reconcile::{
        DispatchPlan, EffectView, ProviderCapability, plan_attempt,
    };
    use toron_core::index::workflow_effects::EffectState as CoreState;
    use toron_core::index::workflow_effects::can_transition as core_can_transition;
    use toron_core::kernel::EffectState as FrameworkState;
    use toron_core::kernel::can_transition as framework_can_transition;
    use super::super::reconcile::effect_view;

    /// Core journaled state spelling, via the engine's own mapping (no second
    /// mapping table beside `reconcile::effect_view`).
    fn to_framework(s: CoreState) -> FrameworkState {
        effect_view(s, 1, false).state
    }

    const ALL: [CoreState; 9] = [
        CoreState::Prepared,
        CoreState::InFlight,
        CoreState::Accepted,
        CoreState::Settled,
        CoreState::Failed,
        CoreState::Unknown,
        CoreState::ReconcilePending,
        CoreState::Cancelled,
        CoreState::Parked,
    ];

    #[test]
    fn ceiling_is_one_day_on_both_sides() {
        assert_eq!(PARK_MAX_WAIT_SECS, 86_400);
        assert_eq!(framework_ceiling_secs(), 86_400);
        assert_eq!(PARK_MAX_WAIT_SECS, framework_ceiling_secs());
    }

    #[test]
    fn transitions_match_the_framework_on_every_pair() {
        for from in ALL {
            for to in ALL {
                assert_eq!(
                    core_can_transition(from, to),
                    framework_can_transition(to_framework(from), to_framework(to)),
                    "transition {from:?} -> {to:?}"
                );
            }
        }
    }

    #[test]
    fn parked_row_resumes_it_never_fails() {
        let cap = ProviderCapability {
            supports_idempotency_key: true,
            supports_reconcile_query: true,
        };
        let view = EffectView {
            state: FrameworkState::Parked,
            attempt: 1,
            lease_live: false,
        };
        assert_eq!(
            plan_attempt(Some(&view), &cap),
            DispatchPlan::ResumeParked { attempt: 1 }
        );
    }
}
