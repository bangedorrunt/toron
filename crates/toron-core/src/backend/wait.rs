// governed-by: ADR-0031
//! Activity-gated, occupant-pinned prompt-wait (ADR-0031 S2/A3, bead bd-i7bh).
//!
//! Herdr lesson (§3.1–§3.2): a prompt that sent bytes is not a prompt that
//! started work. After submit, observe up to 5s for `working`/`blocked`
//! activity; unrelated `idle`/`done` churn never satisfies the gate. No
//! activity → [`WaitOutcome::Stalled`]; caller timeout first →
//! [`WaitOutcome::Timeout`]. Neither proves non-delivery — the caller rule,
//! enforced at every call site, is **read before retry, never blind
//! resubmit** (stalled/timeout never auto-resubmits).
//!
//! ## Completion-as-events convention
//!
//! Every spawned unit of work (prompt, inbox fetch, SLA scan, durable
//! invoke) completes as a typed message re-entering the decision tick —
//! `Completed { id, outcome }` / `Failed { id, reason }` — never a callback
//! that bypasses the journal. This helper is the prompt-wait embodiment:
//! the waiter pins its [`PromptTarget`] (pane + occupant at submit time), so
//! a replacement process can never satisfy another wait, and an
//! already-[`AgentStatus::Blocked`] target fails fast before any byte is
//! sent (`agent_blocked` owns recovery via state polls, never respawn).
//!
//! ## Ownership
//!
//! Lives in toron-core because it speaks backend vocabulary (`PaneId`,
//! `AgentStatus`) and nothing else: no tokio, no Nostr types, no
//! flywheel/doorbell imports. Flywheel prompt paths adopt it as callers;
//! other loops follow only if the pattern earns it.

use super::types::{AgentStatus, PaneId};

/// Post-submit activity-observation budget (herdr §3.1: 5s).
pub const ACTIVITY_GATE_MS: u64 = 5_000;

/// Pinned wait target: the pane plus the occupant recognised at submit
/// time. Waits resolve only for this occupant; a replacement mid-wait
/// cannot satisfy them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptTarget {
    /// Pane the prompt was submitted to (opaque backend id).
    pub pane: PaneId,
    /// Occupant name at submit time (`None` = anonymous shell pane).
    pub occupant: Option<String>,
}

impl PromptTarget {
    pub const fn new(pane: PaneId, occupant: Option<String>) -> Self {
        Self { pane, occupant }
    }

    /// Same pane AND same occupant — the only match that settles a wait.
    pub fn is_same_occupant(&self, other: &PromptTarget) -> bool {
        self.pane == other.pane && self.occupant == other.occupant
    }
}

/// Outcome of [`prompt_wait`]. `Stalled` (no post-submit activity) stays
/// distinct from `Timeout` (active but unsettled past deadline) — they
/// demand different recovery (read-then-decide vs keep-waiting-or-escalate),
/// and neither auto-resubmits. `Aborted` mirrors `waitFor`'s abort-first
/// semantics: the caller cancelled, so no outcome is claimed at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// Settled to the observed state for the pinned occupant.
    Settled(AgentStatus),
    /// No `working`/`blocked` activity within [`ACTIVITY_GATE_MS`].
    Stalled,
    /// Target already `blocked` at submit — nothing was sent.
    Blocked,
    /// Activity seen but no settled state within `timeout_ms`.
    Timeout,
    /// Aborted before submit or mid-wait — caller cancelled.
    Aborted,
}

/// Effect-free host surface the wait drives. Implementations (scripted fakes
/// in tests, socket-backed in production callers) perform the I/O; this
/// module owns only the decision order: blocked-check → submit → activity
/// gate → pinned settle.
pub trait PromptHost {
    /// Cancellation flag (`waitFor`'s AbortSignal analog). Checked before
    /// submit and after the settle wait; abort wins over any outcome.
    fn aborted(&mut self) -> bool;
    /// Current canonical state of the pinned target's pane.
    fn state(&self, target: &PromptTarget) -> AgentStatus;
    /// Current occupant of the pane (`None` = anonymous).
    fn occupant(&self, target: &PromptTarget) -> Option<String>;
    /// Ordered write of text + encoded Enter. Called at most once per
    /// [`prompt_wait`] — never retried internally.
    fn submit(&mut self, target: &PromptTarget, line: &str);
    /// True when `working`/`blocked` activity for the pinned occupant is
    /// observed within `budget_ms`.
    fn observe_activity(&mut self, target: &PromptTarget, budget_ms: u64) -> bool;
    /// True when the pinned occupant settles (reaches a non-`working`
    /// state) within `budget_ms`; reports the settled state.
    fn await_settled(&mut self, target: &PromptTarget, budget_ms: u64) -> Option<AgentStatus>;
}

/// Submit `line` to `target` and wait under the activity gate.
///
/// Order is the contract: already-`blocked` → [`WaitOutcome::Blocked`]
/// before any byte is sent; otherwise exactly one [`PromptHost::submit`],
/// then [`WaitOutcome::Stalled`] when the activity gate sees nothing, then
/// the pinned settle (a mid-wait occupant replacement can never satisfy it)
/// or [`WaitOutcome::Timeout`].
///
/// Caller rule: `Stalled`/`Timeout` → read → decide. Never blind resubmit.
pub fn prompt_wait(
    host: &mut impl PromptHost,
    target: &PromptTarget,
    line: &str,
    timeout_ms: u64,
) -> WaitOutcome {
    if host.aborted() {
        return WaitOutcome::Aborted;
    }
    if host.state(target) == AgentStatus::Blocked {
        return WaitOutcome::Blocked;
    }
    host.submit(target, line);
    if !host.observe_activity(target, ACTIVITY_GATE_MS) {
        return WaitOutcome::Stalled;
    }
    let pinned = PromptTarget::new(target.pane.clone(), host.occupant(target));
    let settled = host.await_settled(&pinned, timeout_ms);
    if host.aborted() {
        return WaitOutcome::Aborted;
    }
    match settled {
        Some(state)
            if pinned.is_same_occupant(&PromptTarget::new(
                target.pane.clone(),
                host.occupant(target),
            )) =>
        {
            WaitOutcome::Settled(state)
        }
        _ => WaitOutcome::Timeout,
    }
}

#[cfg(test)]
mod wait_for_tests {
    use super::*;
    use std::collections::VecDeque;

    /// Scripted host: queued states/activity/settles + submit counter (the
    /// anti-double-submit proof reads this).
    struct Scripted {
        states: VecDeque<AgentStatus>,
        occupants: VecDeque<Option<String>>,
        activity: VecDeque<bool>,
        settles: VecDeque<Option<(AgentStatus, Option<String>)>>,
        aborts: VecDeque<bool>,
        submits: usize,
    }

    impl Scripted {
        fn new(state: AgentStatus, occupant: Option<&str>) -> Self {
            Self {
                states: [state].into(),
                occupants: [occupant.map(str::to_string)].into(),
                activity: VecDeque::new(),
                settles: VecDeque::new(),
                aborts: VecDeque::new(),
                submits: 0,
            }
        }
        fn with_abort(mut self, v: bool) -> Self {
            self.aborts.push_back(v);
            self
        }
        fn with_activity(mut self, v: bool) -> Self {
            self.activity.push_back(v);
            self
        }
        fn with_settle(mut self, state: AgentStatus, occupant: Option<&str>) -> Self {
            self.settles
                .push_back(Some((state, occupant.map(str::to_string))));
            self
        }
        fn with_empty_settle(mut self) -> Self {
            self.settles.push_back(None);
            self
        }
    }

    impl PromptHost for Scripted {
        fn aborted(&mut self) -> bool {
            self.aborts.pop_front().unwrap_or(false)
        }
        fn state(&self, _t: &PromptTarget) -> AgentStatus {
            *self.states.back().unwrap_or(&AgentStatus::Unknown)
        }
        fn occupant(&self, _t: &PromptTarget) -> Option<String> {
            self.occupants.back().cloned().flatten()
        }
        fn submit(&mut self, _t: &PromptTarget, _line: &str) {
            self.submits += 1;
        }
        fn observe_activity(&mut self, _t: &PromptTarget, _b: u64) -> bool {
            self.activity.pop_front().unwrap_or(false)
        }
        fn await_settled(&mut self, _t: &PromptTarget, _b: u64) -> Option<AgentStatus> {
            match self.settles.pop_front().flatten() {
                Some((state, occ)) => {
                    self.occupants.push_back(occ);
                    Some(state)
                }
                None => None,
            }
        }
    }

    fn target() -> PromptTarget {
        PromptTarget::new(PaneId::new("w1:p1"), Some("PiAlpha".into()))
    }

    #[test]
    fn settled_happy_path() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha"))
            .with_activity(true)
            .with_settle(AgentStatus::Done, Some("PiAlpha"));
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Settled(AgentStatus::Done)
        );
        assert_eq!(h.submits, 1, "exactly one submit");
    }

    #[test]
    fn stalled_never_resubmits() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha")).with_activity(false);
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Stalled
        );
        assert_eq!(h.submits, 1, "stalled must not auto-resubmit");
    }

    #[test]
    fn blocked_fail_fast_sends_nothing() {
        let mut h = Scripted::new(AgentStatus::Blocked, Some("PiAlpha")).with_activity(true);
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Blocked
        );
        assert_eq!(h.submits, 0, "blocked sends zero bytes");
    }

    #[test]
    fn timeout_never_resubmits() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha"))
            .with_activity(true)
            .with_empty_settle();
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Timeout
        );
        assert_eq!(h.submits, 1, "timeout must not auto-resubmit");
    }

    #[test]
    fn occupant_replacement_cannot_satisfy() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha"))
            .with_activity(true)
            .with_settle(AgentStatus::Done, Some("PiBeta"));
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Timeout
        );
        assert_eq!(h.submits, 1);
    }

    #[test]
    fn aborted_before_submit_sends_nothing() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha")).with_abort(true);
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Aborted
        );
        assert_eq!(h.submits, 0, "abort-first: zero bytes sent");
    }

    #[test]
    fn aborted_during_wait_wins_over_settle() {
        let mut h = Scripted::new(AgentStatus::Working, Some("PiAlpha"))
            .with_abort(false)
            .with_activity(true)
            .with_settle(AgentStatus::Done, Some("PiAlpha"))
            .with_abort(true);
        assert_eq!(
            prompt_wait(&mut h, &target(), "do it", 1_000),
            WaitOutcome::Aborted
        );
        assert_eq!(h.submits, 1);
    }
}
