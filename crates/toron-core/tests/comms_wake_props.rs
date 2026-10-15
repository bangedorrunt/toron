// governed-by: ADR-0041
//! Property tests for `toron_core::wake::should_ring` (redesign §4.1, P6):
//! the rule's safety invariants hold for every input, not only the grid the
//! Lean vectors cover.

use proptest::prelude::*;
use toron_core::wake::{
    MAX_RINGS, MAX_RINGS_URGENT, PaneState, RingDecision, RingInput, Urgency, backoff_ms,
    should_ring,
};

fn pane() -> impl Strategy<Value = PaneState> {
    prop_oneof![
        Just(PaneState::Idle),
        Just(PaneState::Done),
        Just(PaneState::Working),
        Just(PaneState::Blocked),
        Just(PaneState::Unknown),
        Just(PaneState::Gone),
    ]
}

fn input() -> impl Strategy<Value = RingInput> {
    (
        (0u64..20, 0u64..20, any::<bool>(), 0u64..4, 0u64..4),
        pane(),
        (
            proptest::option::of(0u64..20),
            proptest::option::of(0u64..4_000_000),
            0u32..12,
            0u64..4_000_000,
            prop_oneof![Just(Urgency::Normal), Just(Urgency::Urgent)],
        ),
    )
        .prop_map(
            |(
                (head, cursor, verified, bgen, cgen),
                pane_state,
                (last_ring_wm, last_ring_at_ms, rings, now_ms, urgency),
            )| RingInput {
                head,
                cursor,
                binding_verified: verified,
                binding_gen: bgen,
                current_gen: cgen,
                pane_state,
                last_ring_wm,
                last_ring_at_ms,
                rings_since_advance: rings,
                now_ms,
                urgency,
            },
        )
}

proptest! {
    /// No ring, hold or escalation without unread mail.
    #[test]
    fn nothing_unread_means_nothing_happens(i in input()) {
        if i.head <= i.cursor {
            prop_assert_eq!(should_ring(&i), RingDecision::NothingUnread);
        }
    }

    /// A ring never goes to an unverified, stale or gone binding.
    #[test]
    fn a_ring_needs_a_verified_current_binding(i in input()) {
        if let RingDecision::Ring { .. } = should_ring(&i) {
            prop_assert!(i.binding_verified);
            prop_assert_eq!(i.binding_gen, i.current_gen);
            prop_assert!(i.pane_state != PaneState::Gone);
        }
    }

    /// No ring past the budget, whatever the mail or the clock.
    #[test]
    fn a_ring_never_exceeds_the_budget(i in input()) {
        if let RingDecision::Ring { .. } = should_ring(&i) {
            prop_assert!(i.rings_since_advance < i.urgency.max_rings());
        }
        if i.head > i.cursor && i.binding_verified && i.binding_gen == i.current_gen
            && i.pane_state != PaneState::Gone
            && i.rings_since_advance >= i.urgency.max_rings()
        {
            prop_assert_eq!(should_ring(&i), RingDecision::Escalate { wm: i.head });
        }
    }

    /// Typing into a mid-turn or blocked pane only ever happens for urgent
    /// mail on a working pane.
    #[test]
    fn rings_respect_pane_state(i in input()) {
        if let RingDecision::Ring { .. } = should_ring(&i) {
            match i.pane_state {
                PaneState::Idle | PaneState::Done => {}
                PaneState::Working => prop_assert_eq!(i.urgency, Urgency::Urgent),
                other => prop_assert!(false, "rang a {other:?} pane"),
            }
        }
    }

    /// Time never turns a hold into anything but a ring: once the clock
    /// reaches the reported due time the answer is Ring.
    #[test]
    fn a_hold_ends_at_its_due_time(i in input()) {
        if let RingDecision::Hold { until_ms } = should_ring(&i) {
            prop_assert!(until_ms > i.now_ms);
            let mut later = i.clone();
            later.now_ms = until_ms;
            prop_assert_eq!(should_ring(&later), RingDecision::Ring { wm: i.head });
        }
    }

    /// P6, over arbitrary starting bookkeeping: a recipient that never reads
    /// (cursor fixed) is escalated within a bounded number of ticks, and is
    /// rung at most `max_rings - rings_since_advance` more times.
    #[test]
    fn p6_never_a_silent_stall(mut i in input()) {
        i.binding_verified = true;
        i.binding_gen = i.current_gen;
        i.pane_state = PaneState::Idle;
        i.head = i.head.max(i.cursor + 1);
        let budget = i.urgency.max_rings();
        let start = i.rings_since_advance;
        let mut rings = 0u32;
        let mut done = false;
        for _ in 0..(2 * budget + 3) {
            match should_ring(&i) {
                RingDecision::Ring { wm } => {
                    rings += 1;
                    i.last_ring_wm = Some(wm);
                    i.last_ring_at_ms = Some(i.now_ms);
                    i.rings_since_advance += 1;
                }
                RingDecision::Hold { until_ms } => i.now_ms = until_ms,
                RingDecision::Escalate { wm } => {
                    prop_assert_eq!(wm, i.head);
                    done = true;
                    break;
                }
                other => prop_assert!(false, "unexpected {other:?}"),
            }
        }
        prop_assert!(done, "no escalation after {} ticks", 2 * budget + 3);
        prop_assert_eq!(rings, budget.saturating_sub(start));
    }

    /// The backoff never shrinks as rings pile up, and urgent never waits
    /// longer than normal.
    #[test]
    fn backoff_is_monotone(r in 0u32..64) {
        for u in [Urgency::Normal, Urgency::Urgent] {
            prop_assert!(backoff_ms(r, u) <= backoff_ms(r + 1, u));
        }
        prop_assert!(backoff_ms(r, Urgency::Urgent) <= backoff_ms(r, Urgency::Normal));
    }
}

#[test]
fn the_budgets_are_what_the_plan_says() {
    assert_eq!((MAX_RINGS, MAX_RINGS_URGENT), (5, 3));
    const { assert!(MAX_RINGS_URGENT < MAX_RINGS) };
}
