// governed-by: ADR-0041
//! The wake decision (redesign §4.1): one pure function, `std + serde`, no
//! clock, no I/O. The `InboxProgram` fold feeds it durable facts (head,
//! cursor, binding, pane state, ring bookkeeping) and a `now`; it answers
//! what to do for one recipient. Nothing here knows herdr, sockets or the
//! doorbell.
//!
//! A wake is a level-triggered, payload-free hint. Its success is measured
//! only by the recipient's cursor advancing, never by herdr's state:
//!
//! ```text
//! should_ring(r) =
//!      head(r) > cursor(r)
//!   ∧  binding(r).verified ∧ binding(r).gen == current_gen
//!   ∧  pane_state(r) ∈ {idle, done}          (urgent: working too)
//!   ∧  (fresh_mail(r)  ∨  now − last_ring_at(r) ≥ backoff(rings))
//!   ∧  rings_since_advance(r) < MAX_RINGS    (else escalate, never ring)
//!
//! fresh_mail(r) = rings_since_advance(r) == 0 ∧ last_ring_wm(r) < head(r)
//! ```
//!
//! `fresh_mail` is the one place this departs from a literal reading of
//! redesign §4.1 ("new mail since the last ring"). With the literal rule a
//! flood of mail to a merely slow recipient would ring on every message and
//! spend the whole ring budget in moments, declaring it deaf (and paging the
//! captain) for being busy. The wake line is constant per watermark and
//! names no payload, so an unanswered ring already tells the recipient to
//! run `toron next`, which reads everything up to the head at that time:
//! more mail adds nothing until the backoff says ring again. Mail after a
//! read (the cursor advanced, so the count is 0) still rings at once.
//!
//! **P6 (bounded wake).** With a verified binding and a ringable pane, if
//! `head > cursor` stays true the decision becomes [`RingDecision::Escalate`]
//! after exactly [`MAX_RINGS`] rings (urgent: [`MAX_RINGS_URGENT`]); the
//! caller emits the `deaf{r, wm}` fact then. No silent stall. The Lean twin
//! is `proto/Proto/Wake.lean`; `tests/proto_vectors.rs` replays its vectors
//! against [`should_ring`].
//!
//! Known edge, by design: a pane that stays `working` (or `blocked`) for
//! ever is never rung and so never goes deaf through this function. That
//! stall is the task lease's job (ack-by and lease expiry reach the
//! supervisor), not the wake's; typing into a pane mid-turn is the thing the
//! rule exists to avoid. Urgent mail may ring a `working` pane.

use serde::{Deserialize, Serialize};

/// Rings without a cursor advance before the recipient is declared deaf.
pub const MAX_RINGS: u32 = 5;
/// The same bound for urgent mail (steers, cancels): fewer, faster rings.
pub const MAX_RINGS_URGENT: u32 = 3;
/// First backoff after a ring, doubling per unanswered ring.
pub const BACKOFF_BASE_MS: u64 = 30_000;
/// Backoff ceiling.
pub const BACKOFF_CAP_MS: u64 = 900_000;
/// Urgent first backoff.
pub const BACKOFF_URGENT_BASE_MS: u64 = 5_000;
/// Urgent backoff ceiling.
pub const BACKOFF_URGENT_CAP_MS: u64 = 60_000;

/// How urgent the unread mail is. Urgent = a `steer@1` (Control) whose body
/// says `priority = "urgent"` ([`crate::inbox::urgent_pending`] reads it off
/// the log). `cancel@1` carries no `priority` and a notice (`notify@1`,
/// `escalate@1`) is never urgent, whatever its text. The class is the host's
/// reading of the log; this function only takes the result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Urgency {
    /// Ordinary mail.
    #[default]
    Normal,
    /// An unread urgent steer.
    Urgent,
}

impl Urgency {
    /// Rings allowed before escalation.
    pub fn max_rings(self) -> u32 {
        match self {
            Self::Normal => MAX_RINGS,
            Self::Urgent => MAX_RINGS_URGENT,
        }
    }
}

/// What herdr says the pane is doing. herdr's word is a precondition for
/// typing a line, never proof the line was read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PaneState {
    /// Waiting for input.
    Idle,
    /// A turn finished (the host also feeds this when `completion_seq`
    /// moved, to catch turns faster than a poll).
    Done,
    /// Mid-turn.
    Working,
    /// Waiting on a permission or a human; a typed line could answer it.
    Blocked,
    /// herdr does not know.
    Unknown,
    /// The pane no longer exists.
    Gone,
}

/// Backoff after `rings` unanswered rings: `base * 2^rings`, capped.
pub fn backoff_ms(rings: u32, urgency: Urgency) -> u64 {
    let (base, cap) = match urgency {
        Urgency::Normal => (BACKOFF_BASE_MS, BACKOFF_CAP_MS),
        Urgency::Urgent => (BACKOFF_URGENT_BASE_MS, BACKOFF_URGENT_CAP_MS),
    };
    (base << rings.min(20)).min(cap)
}

/// Everything the decision reads for one recipient. All durable except
/// `pane_state` and `now_ms`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingInput {
    /// Offset of the newest mail for the recipient (its watermark).
    pub head: u64,
    /// The recipient's committed cursor.
    pub cursor: u64,
    /// The binding was verified (env + `report_metadata`).
    pub binding_verified: bool,
    /// The generation the binding was verified at.
    pub binding_gen: u64,
    /// The current generation (bumped on herdr restart / `events_lost`).
    pub current_gen: u64,
    /// herdr's view of the pane.
    pub pane_state: PaneState,
    /// Watermark of the last ring, if any.
    pub last_ring_wm: Option<u64>,
    /// When the last ring went out, if any.
    pub last_ring_at_ms: Option<u64>,
    /// Rings since the cursor last advanced. Resets only on a delivery.
    pub rings_since_advance: u32,
    /// The caller's clock.
    pub now_ms: u64,
    /// Urgency of the unread mail.
    pub urgency: Urgency,
}

/// What to do for one recipient on this tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum RingDecision {
    /// Caught up: nothing to do.
    NothingUnread,
    /// Mail is waiting but the binding is unverified, stale, or its pane is
    /// gone: re-verify the binding, never ring. (The fold counts these ticks
    /// and escalates if they persist.)
    Unbound,
    /// Mail is waiting and the binding is good, but the pane is mid-turn
    /// or blocked: ring later.
    Busy,
    /// Rung recently; the next ring is due at `until_ms`.
    Hold {
        /// Earliest time the next ring may go out.
        until_ms: u64,
    },
    /// Type the constant wake line for this watermark.
    Ring {
        /// The watermark the line names.
        wm: u64,
    },
    /// The ring budget is spent: emit `deaf{r, wm}`, a `Control/notify` to
    /// the supervisor, and the captain notification. Do not ring.
    Escalate {
        /// The watermark nobody answered.
        wm: u64,
    },
}

/// The rule. Pure and total.
pub fn should_ring(i: &RingInput) -> RingDecision {
    if i.head <= i.cursor {
        return RingDecision::NothingUnread;
    }
    if !i.binding_verified || i.binding_gen != i.current_gen || i.pane_state == PaneState::Gone {
        return RingDecision::Unbound;
    }
    if i.rings_since_advance >= i.urgency.max_rings() {
        return RingDecision::Escalate { wm: i.head };
    }
    let ringable = match i.pane_state {
        PaneState::Idle | PaneState::Done => true,
        PaneState::Working => i.urgency == Urgency::Urgent,
        PaneState::Blocked | PaneState::Unknown | PaneState::Gone => false,
    };
    if !ringable {
        return RingDecision::Busy;
    }
    let fresh_mail = i.rings_since_advance == 0 && i.last_ring_wm.is_none_or(|wm| wm < i.head);
    if fresh_mail {
        return RingDecision::Ring { wm: i.head };
    }
    let due = i
        .last_ring_at_ms
        .unwrap_or(0)
        .saturating_add(backoff_ms(i.rings_since_advance, i.urgency));
    if i.now_ms >= due {
        RingDecision::Ring { wm: i.head }
    } else {
        RingDecision::Hold { until_ms: due }
    }
}

/// The constant wake line for a watermark: no brief, no payload, the same
/// text for the same watermark so a duplicate is harmless.
pub fn wake_line(wm: u64) -> String {
    format!("⟦toron⟧ new work (wm {wm:x}) — run: toron next")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> RingInput {
        RingInput {
            head: 7,
            cursor: 3,
            binding_verified: true,
            binding_gen: 2,
            current_gen: 2,
            pane_state: PaneState::Idle,
            last_ring_wm: None,
            last_ring_at_ms: None,
            rings_since_advance: 0,
            now_ms: 1_000_000,
            urgency: Urgency::Normal,
        }
    }

    #[test]
    fn caught_up_never_rings() {
        let mut i = base();
        i.cursor = 7;
        assert_eq!(should_ring(&i), RingDecision::NothingUnread);
        i.cursor = 9;
        assert_eq!(should_ring(&i), RingDecision::NothingUnread);
    }

    #[test]
    fn first_unread_mail_rings_with_the_watermark() {
        assert_eq!(should_ring(&base()), RingDecision::Ring { wm: 7 });
    }

    #[test]
    fn an_unverified_or_stale_binding_never_rings() {
        let mut i = base();
        i.binding_verified = false;
        assert_eq!(should_ring(&i), RingDecision::Unbound);
        let mut i = base();
        i.binding_gen = 1;
        assert_eq!(should_ring(&i), RingDecision::Unbound);
        let mut i = base();
        i.pane_state = PaneState::Gone;
        assert_eq!(should_ring(&i), RingDecision::Unbound);
    }

    #[test]
    fn only_idle_or_done_panes_are_rung() {
        for (s, want) in [
            (PaneState::Idle, true),
            (PaneState::Done, true),
            (PaneState::Working, false),
            (PaneState::Blocked, false),
            (PaneState::Unknown, false),
        ] {
            let mut i = base();
            i.pane_state = s;
            assert_eq!(
                matches!(should_ring(&i), RingDecision::Ring { .. }),
                want,
                "{s:?}"
            );
        }
    }

    #[test]
    fn urgent_mail_may_ring_a_working_pane_but_never_a_blocked_one() {
        let mut i = base();
        i.urgency = Urgency::Urgent;
        i.pane_state = PaneState::Working;
        assert_eq!(should_ring(&i), RingDecision::Ring { wm: 7 });
        i.pane_state = PaneState::Blocked;
        assert_eq!(should_ring(&i), RingDecision::Busy);
    }

    #[test]
    fn the_same_watermark_waits_out_the_backoff_then_rings_again() {
        let mut i = base();
        i.last_ring_wm = Some(7);
        i.last_ring_at_ms = Some(990_000);
        i.rings_since_advance = 1;
        let due = 990_000 + backoff_ms(1, Urgency::Normal);
        assert_eq!(should_ring(&i), RingDecision::Hold { until_ms: due });
        i.now_ms = due - 1;
        assert_eq!(should_ring(&i), RingDecision::Hold { until_ms: due });
        i.now_ms = due;
        assert_eq!(should_ring(&i), RingDecision::Ring { wm: 7 });
    }

    #[test]
    fn mail_after_a_read_rings_at_once_even_inside_the_backoff() {
        let mut i = base();
        i.last_ring_wm = Some(5);
        i.last_ring_at_ms = Some(999_999);
        i.rings_since_advance = 0;
        assert_eq!(should_ring(&i), RingDecision::Ring { wm: 7 });
    }

    #[test]
    fn more_mail_after_an_unanswered_ring_waits_for_the_backoff() {
        let mut i = base();
        i.last_ring_wm = Some(5);
        i.last_ring_at_ms = Some(999_999);
        i.rings_since_advance = 1;
        let due = 999_999 + backoff_ms(1, Urgency::Normal);
        assert_eq!(should_ring(&i), RingDecision::Hold { until_ms: due });
    }

    #[test]
    fn the_ring_budget_escalates_instead_of_ringing() {
        for (u, max) in [
            (Urgency::Normal, MAX_RINGS),
            (Urgency::Urgent, MAX_RINGS_URGENT),
        ] {
            let mut i = base();
            i.urgency = u;
            i.last_ring_wm = Some(5);
            i.rings_since_advance = max - 1;
            assert_eq!(should_ring(&i), RingDecision::Ring { wm: 7 });
            i.rings_since_advance = max;
            assert_eq!(should_ring(&i), RingDecision::Escalate { wm: 7 }, "{u:?}");
            i.rings_since_advance = max + 9;
            assert_eq!(should_ring(&i), RingDecision::Escalate { wm: 7 });
        }
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        assert_eq!(backoff_ms(0, Urgency::Normal), 30_000);
        assert_eq!(backoff_ms(1, Urgency::Normal), 60_000);
        assert_eq!(backoff_ms(4, Urgency::Normal), 480_000);
        assert_eq!(backoff_ms(5, Urgency::Normal), BACKOFF_CAP_MS);
        assert_eq!(backoff_ms(u32::MAX, Urgency::Normal), BACKOFF_CAP_MS);
        assert_eq!(backoff_ms(0, Urgency::Urgent), 5_000);
        assert_eq!(backoff_ms(9, Urgency::Urgent), BACKOFF_URGENT_CAP_MS);
        for r in 0..30 {
            assert!(backoff_ms(r, Urgency::Urgent) <= backoff_ms(r, Urgency::Normal));
        }
    }

    #[test]
    fn the_wake_line_is_constant_per_watermark_and_carries_no_payload() {
        assert_eq!(wake_line(0x7f3a), wake_line(0x7f3a));
        assert_ne!(wake_line(1), wake_line(2));
        assert_eq!(
            wake_line(0x7f3a),
            "⟦toron⟧ new work (wm 7f3a) — run: toron next"
        );
        assert!(wake_line(u64::MAX).len() < 80);
    }

    /// P6, simulated: a recipient that never reads. Every tick jumps the
    /// clock past the backoff; the rings stop at the budget and the next
    /// decision is Escalate: never a silent stall, never a ring past the
    /// budget.
    #[test]
    fn p6_a_deaf_recipient_is_escalated_after_exactly_the_budget() {
        for u in [Urgency::Normal, Urgency::Urgent] {
            let mut i = base();
            i.urgency = u;
            i.pane_state = PaneState::Idle;
            let mut rings = 0;
            let mut escalated_after = None;
            for tick in 0..50 {
                match should_ring(&i) {
                    RingDecision::Ring { wm } => {
                        rings += 1;
                        i.last_ring_wm = Some(wm);
                        i.last_ring_at_ms = Some(i.now_ms);
                        i.rings_since_advance += 1;
                    }
                    RingDecision::Escalate { .. } => {
                        escalated_after = Some(tick);
                        break;
                    }
                    RingDecision::Hold { until_ms } => i.now_ms = until_ms,
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert_eq!(rings, u.max_rings(), "{u:?}");
            assert!(escalated_after.is_some(), "{u:?}: never escalated");
        }
    }
}
