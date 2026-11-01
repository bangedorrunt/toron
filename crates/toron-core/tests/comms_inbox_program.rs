// governed-by: ADR-0041
//! `InboxProgram` (Phase 3): the recipient-side wake fold, driven through
//! its pure `update`. Covers the plan's exit: a flood loses nothing (every
//! recipient's cursor reaches head or a `deaf` fact exists), a restart from
//! `{model, offset}` decides the same, an unverified binding escalates
//! instead of ringing, and `events_lost` forces re-verification first.

use proptest::prelude::*;
use toron_core::inbox_program::{
    Binding, DeafCause, InboxCmd, InboxModel, InboxMsg, InboxProgram, Invalidate,
};
use toron_core::kernel::Program;
use toron_core::wake::{MAX_RINGS, MAX_RINGS_URGENT, PaneState, Urgency, wake_line};

const A: &str = "aaaa";
const B: &str = "bbbb";
const FAR: u64 = 10_000_000;

fn fold(model: &InboxModel, msgs: &[InboxMsg]) -> (InboxModel, Vec<InboxCmd>) {
    let mut m = model.clone();
    let mut out = Vec::new();
    for msg in msgs {
        let u = InboxProgram::update(&m, msg);
        m = u.model;
        out.extend(u.commands);
    }
    (m, out)
}

fn head(r: &str, h: u64) -> InboxMsg {
    InboxMsg::Head {
        recipient: r.into(),
        head: h,
        urgency: Urgency::Normal,
    }
}
fn cursor(r: &str, c: u64) -> InboxMsg {
    InboxMsg::Cursor {
        recipient: r.into(),
        cursor: c,
    }
}
fn bound(r: &str, generation: u64, verified: bool) -> InboxMsg {
    InboxMsg::Bound {
        recipient: r.into(),
        pane_id: format!("w1:{r}"),
        plane_session: "s1".into(),
        generation,
        verified,
        verified_by: "test".into(),
    }
}
fn pane(r: &str, s: PaneState) -> InboxMsg {
    InboxMsg::Pane {
        recipient: r.into(),
        state: s,
    }
}
fn tick(now_ms: u64) -> InboxMsg {
    InboxMsg::Tick { now_ms }
}

fn rings(cmds: &[InboxCmd]) -> usize {
    cmds.iter()
        .filter(|c| matches!(c, InboxCmd::Ring { .. }))
        .count()
}
fn deaf(cmds: &[InboxCmd]) -> Vec<(String, u64, DeafCause)> {
    cmds.iter()
        .filter_map(|c| match c {
            InboxCmd::RecordDeaf {
                recipient,
                wm,
                cause,
            } => Some((recipient.clone(), *wm, *cause)),
            _ => None,
        })
        .collect()
}

fn ready(r: &str) -> InboxModel {
    let (m, _) = fold(
        &InboxModel::default(),
        &[bound(r, 0, true), pane(r, PaneState::Idle)],
    );
    m
}

#[test]
fn unread_mail_rings_once_with_the_constant_line_by_pane_id() {
    let (m, cmds) = fold(&ready(A), &[head(A, 7), tick(1_000)]);
    assert_eq!(cmds.len(), 1, "{cmds:?}");
    match &cmds[0] {
        InboxCmd::Ring {
            recipient,
            pane_id,
            binding_gen,
            wm,
            line,
            ..
        } => {
            assert_eq!(recipient, A);
            assert_eq!(pane_id, "w1:aaaa");
            assert_eq!(*binding_gen, 0);
            assert_eq!(*wm, 7);
            assert_eq!(line, &wake_line(7));
            assert!(!line.contains("brief"));
        }
        other => panic!("{other:?}"),
    }
    // Same tick again inside the backoff: nothing. A hint is the same.
    let (_, again) = fold(
        &m,
        &[
            tick(1_001),
            InboxMsg::Hint {
                recipient: A.into(),
                now_ms: 1_002,
            },
        ],
    );
    assert!(again.is_empty(), "{again:?}");
}

#[test]
fn mail_after_a_read_rings_at_once_but_mail_after_an_unanswered_ring_waits() {
    let (m, _) = fold(&ready(A), &[head(A, 7), tick(1_000)]);
    // More mail while the first ring is unanswered: no new ring now ...
    let (m2, c) = fold(&m, &[head(A, 9), tick(1_001)]);
    assert_eq!(
        rings(&c),
        0,
        "an unanswered ring is not repeated per message"
    );
    // ... the backoff rings the newest mark.
    let (_, c) = fold(&m2, &[tick(1_001 + FAR)]);
    assert_eq!(rings(&c), 1);
    assert!(matches!(&c[0], InboxCmd::Ring { wm: 9, .. }));
    // After a read, fresh mail rings at once, inside any backoff.
    let (m3, _) = fold(&m, &[cursor(A, 7), head(A, 11)]);
    let (_, c) = fold(&m3, &[tick(1_002)]);
    assert_eq!(rings(&c), 1);
}

#[test]
fn a_ring_is_not_a_delivery_only_the_cursor_resets_the_count() {
    let mut m = ready(A);
    let mut now = 0;
    for _ in 0..3 {
        let (n, c) = fold(&m, &[head(A, 7), tick(now)]);
        m = n;
        assert_eq!(rings(&c), 1);
        now += FAR;
    }
    assert_eq!(m.recipients[A].rings_since_advance, 3);
    // herdr says idle again, mail re-announced, a stale cursor value: no reset.
    let (m2, _) = fold(&m, &[pane(A, PaneState::Done), cursor(A, 0), head(A, 7)]);
    assert_eq!(m2.recipients[A].rings_since_advance, 3);
    // The cursor advancing resets it.
    let (m3, _) = fold(&m2, &[cursor(A, 7)]);
    assert_eq!(m3.recipients[A].rings_since_advance, 0);
    assert_eq!(m3.recipients[A].deaf, None);
}

#[test]
fn a_recipient_that_never_reads_goes_deaf_after_exactly_the_budget() {
    let mut m = ready(A);
    let mut total_rings = 0;
    let mut deafs = Vec::new();
    let mut other = Vec::new();
    let mut now = 0;
    let (n, _) = fold(&m, &[head(A, 7)]);
    m = n;
    for _ in 0..40 {
        let (n, c) = fold(&m, &[tick(now)]);
        m = n;
        total_rings += rings(&c);
        deafs.extend(deaf(&c));
        other.extend(c.into_iter().filter(|c| {
            matches!(
                c,
                InboxCmd::NotifySupervisor { .. } | InboxCmd::NotifyCaptain { .. }
            )
        }));
        now += FAR;
    }
    assert_eq!(total_rings, MAX_RINGS as usize);
    assert_eq!(deafs, vec![(A.to_string(), 7, DeafCause::NoAnswer)]);
    assert_eq!(other.len(), 2, "one supervisor notify, one captain notify");
    // New mail to a deaf recipient: one more deaf fact for the new mark,
    // and still no ring.
    let (m, c) = fold(&m, &[head(A, 9), tick(now)]);
    assert_eq!(rings(&c), 0);
    assert_eq!(deaf(&c), vec![(A.to_string(), 9, DeafCause::NoAnswer)]);
    // Reading clears it; fresh mail rings again.
    let (m, _) = fold(&m, &[cursor(A, 9)]);
    assert_eq!(m.recipients[A].deaf, None, "reading clears the deaf mark");
    assert_eq!(m.recipients[A].rings_since_advance, 0);
    let (_, c) = fold(&m, &[head(A, 11), tick(now + FAR)]);
    assert_eq!(rings(&c), 1);
}

#[test]
fn urgent_mail_has_the_smaller_budget_and_may_ring_a_working_pane() {
    let (m, _) = fold(
        &ready(A),
        &[
            pane(A, PaneState::Working),
            InboxMsg::Head {
                recipient: A.into(),
                head: 3,
                urgency: Urgency::Urgent,
            },
        ],
    );
    let mut m = m;
    let mut total = 0;
    for i in 0..20 {
        let (n, c) = fold(&m, &[tick(i * FAR)]);
        m = n;
        total += rings(&c);
    }
    assert_eq!(total, MAX_RINGS_URGENT as usize);
    assert_eq!(m.recipients[A].deaf, Some(3));
}

#[test]
fn working_normal_mail_is_never_typed_into_and_never_goes_deaf_here() {
    let (mut m, _) = fold(&ready(A), &[pane(A, PaneState::Working), head(A, 3)]);
    for i in 0..20 {
        let (n, c) = fold(&m, &[tick(i * FAR)]);
        m = n;
        assert!(c.is_empty(), "{c:?}");
    }
    assert_eq!(m.recipients[A].rings_since_advance, 0);
}

#[test]
fn an_unverified_binding_is_never_rung_it_is_reverified_then_escalated() {
    let (mut m, _) = fold(
        &InboxModel::default(),
        &[bound(A, 0, false), pane(A, PaneState::Idle), head(A, 4)],
    );
    let mut reverify = 0;
    let mut deafs = Vec::new();
    for i in 0..10 {
        let (n, c) = fold(&m, &[tick(i * FAR)]);
        m = n;
        assert_eq!(
            rings(&c),
            0,
            "negative control: never ring an unverified binding"
        );
        reverify += c
            .iter()
            .filter(|c| matches!(c, InboxCmd::Reverify { .. }))
            .count();
        deafs.extend(deaf(&c));
    }
    assert_eq!(reverify, 10);
    assert_eq!(deafs, vec![(A.to_string(), 4, DeafCause::Unbound)]);
    // A verified binding arrives: it rings.
    let (_, c) = fold(&m, &[bound(A, 0, true), tick(11 * FAR)]);
    assert_eq!(rings(&c), 1);
}

#[test]
fn events_lost_stales_every_binding_before_any_ring() {
    let both = [
        bound(A, 0, true),
        pane(A, PaneState::Idle),
        bound(B, 0, true),
        pane(B, PaneState::Idle),
        head(A, 5),
        head(B, 5),
    ];
    let (m, c) = fold(
        &InboxModel::default(),
        &[both.as_slice(), &[tick(0)]].concat(),
    );
    assert_eq!(rings(&c), 2);
    let (m, c) = fold(
        &m,
        &[
            InboxMsg::Invalidate(Invalidate::EventsLost),
            head(A, 6),
            head(B, 6),
            tick(FAR),
        ],
    );
    assert_eq!(rings(&c), 0, "no ring on a stale generation");
    assert_eq!(
        c.iter()
            .filter(|c| matches!(c, InboxCmd::Reverify { .. }))
            .count(),
        2
    );
    assert_eq!(m.events_lost, 1);
    assert_eq!(m.generation, 1);
    // Only the one that re-verified at the new generation is rung.
    let (_, c) = fold(&m, &[bound(A, 1, true), tick(2 * FAR)]);
    assert_eq!(rings(&c), 1);
    assert!(matches!(&c[0], InboxCmd::Ring { recipient, binding_gen: 1, .. } if recipient == A));
}

#[test]
fn a_deaf_recipient_is_not_rung_again_until_a_new_pane_binds() {
    let (mut m, _) = fold(&ready(A), &[head(A, 7)]);
    for i in 0..10 {
        m = fold(&m, &[tick(i * FAR)]).0;
    }
    assert_eq!(m.recipients[A].deaf, Some(7));
    // Re-verifying the same pane after events_lost does not reset the budget.
    let (m2, c) = fold(
        &m,
        &[
            InboxMsg::Invalidate(Invalidate::EventsLost),
            bound(A, 1, true),
            tick(20 * FAR),
        ],
    );
    assert_eq!(rings(&c), 0);
    assert_eq!(m2.recipients[A].deaf, Some(7));
    // A different pane (restarted incarnation) gets a fresh budget.
    let (_, c) = fold(
        &m2,
        &[
            InboxMsg::Bound {
                recipient: A.into(),
                pane_id: "w2:new".into(),
                plane_session: "s1".into(),
                generation: 1,
                verified: true,
                verified_by: "test".into(),
            },
            tick(21 * FAR),
        ],
    );
    assert_eq!(rings(&c), 1);
    assert!(matches!(&c[0], InboxCmd::Ring { pane_id, .. } if pane_id == "w2:new"));
}

#[test]
fn a_plane_restart_stales_bindings_without_counting_lost_events() {
    let (m, _) = fold(&ready(A), &[head(A, 5), tick(0)]);
    let (m, c) = fold(
        &m,
        &[InboxMsg::Invalidate(Invalidate::PlaneRestarted), tick(FAR)],
    );
    assert_eq!(rings(&c), 0);
    assert_eq!(m.events_lost, 0);
    assert_eq!(m.generation, 1);
}

#[test]
fn a_failed_command_changes_nothing_the_next_tick_decides_again() {
    let (m, _) = fold(&ready(A), &[head(A, 5), tick(0)]);
    let before = m.recipients[A].clone();
    let (m, _) = fold(&m, &[InboxMsg::CmdFailed("stall".into())]);
    assert_eq!(m.recipients[A], before);
    assert_eq!(m.failed_cmds, 1);
    let (_, c) = fold(&m, &[tick(FAR)]);
    assert_eq!(rings(&c), 1);
}

#[test]
fn restart_from_a_serialized_model_decides_the_same() {
    let msgs: Vec<InboxMsg> = vec![
        bound(A, 0, true),
        pane(A, PaneState::Idle),
        head(A, 3),
        tick(0),
        head(A, 6),
        tick(5),
        tick(FAR),
        cursor(A, 3),
        tick(2 * FAR),
        tick(3 * FAR),
    ];
    let (whole, whole_cmds) = fold(&InboxModel::default(), &msgs);
    for k in 0..=msgs.len() {
        let (mid, mut cmds) = fold(&InboxModel::default(), &msgs[..k]);
        let snap = serde_json::to_string(&mid).unwrap();
        let restored: InboxModel = serde_json::from_str(&snap).unwrap();
        assert_eq!(restored, mid);
        let (end, rest) = fold(&restored, &msgs[k..]);
        cmds.extend(rest);
        assert_eq!(end, whole, "snapshot at {k}");
        assert_eq!(cmds, whole_cmds, "snapshot at {k}");
    }
}

#[test]
fn a_flood_of_hints_through_a_tiny_channel_loses_nothing() {
    // 10k heads over 8 recipients, every hint beyond the first 64 dropped
    // (the hint channel is tiny); ticks still read the durable heads.
    let names: Vec<String> = (0..8).map(|i| format!("r{i}")).collect();
    let mut m = InboxModel::default();
    for n in &names {
        let (n2, _) = fold(&m, &[bound(n, 0, true), pane(n, PaneState::Idle)]);
        m = n2;
    }
    let mut delivered_hints = 0;
    for i in 1..=10_000u64 {
        let r = &names[(i % 8) as usize];
        // Durable fact (the log's head) is always recorded; only the hint
        // that would trigger an early evaluation is dropped.
        let (n2, _) = fold(&m, &[head(r, i)]);
        m = n2;
        if delivered_hints < 64 {
            delivered_hints += 1;
            let (n3, _) = fold(
                &m,
                &[InboxMsg::Hint {
                    recipient: r.clone(),
                    now_ms: i,
                }],
            );
            m = n3;
        }
    }
    // The flood alone must not have spent anyone's ring budget.
    for (n, r) in &m.recipients {
        assert!(
            r.rings_since_advance <= 1,
            "{n}: the flood burned the budget: {r:?}"
        );
        assert_eq!(r.deaf, None, "{n}: declared deaf by a flood");
    }
    // Half the recipients read when rung; the rest are deaf.
    let mut now = 1_000_000;
    let mut seen_deaf = std::collections::BTreeSet::new();
    for _ in 0..40 {
        let (n2, cmds) = fold(&m, &[tick(now)]);
        m = n2;
        for c in &cmds {
            match c {
                InboxCmd::Ring { recipient, wm, .. }
                    if recipient.ends_with(['0', '2', '4', '6']) =>
                {
                    let (n3, _) = fold(&m, &[cursor(recipient, *wm)]);
                    m = n3;
                }
                InboxCmd::RecordDeaf { recipient, .. } => {
                    seen_deaf.insert(recipient.clone());
                }
                _ => {}
            }
        }
        now += FAR;
    }
    for (n, r) in &m.recipients {
        assert!(
            r.cursor >= r.head || r.deaf.is_some() || seen_deaf.contains(n),
            "{n}: cursor {} head {} and no deaf fact: a silent loss",
            r.cursor,
            r.head
        );
    }
    assert!(
        !seen_deaf.is_empty() && seen_deaf.len() < 8,
        "deaf: {seen_deaf:?}; model: {:?}",
        m.recipients
    );
}

#[test]
fn the_model_round_trips_and_the_binding_wire_name_is_gen() {
    let b = Binding {
        pane_id: "w1:p".into(),
        plane_session: "s".into(),
        generation: 3,
        verified: true,
        verified_by: "env".into(),
    };
    let v = serde_json::to_value(&b).unwrap();
    assert_eq!(v["gen"], 3);
    assert_eq!(serde_json::from_value::<Binding>(v).unwrap(), b);
}

#[test]
fn old_records_with_herdr_names_still_read() {
    // The `herdr_session` -> `plane_session` / `HerdrRestarted` ->
    // `PlaneRestarted` rename keeps wire compat through serde aliases:
    // a journal written before the rename deserializes into the new
    // model unchanged.
    let b: Binding = serde_json::from_str(
        r#"{"pane_id":"w1:p","herdr_session":"s","gen":3,"verified":true,"verified_by":"env"}"#,
    )
    .unwrap();
    assert_eq!(b.plane_session, "s");
    assert_eq!(b.generation, 3);
    let bound: InboxMsg = serde_json::from_str(
        r#"{"msg":"bound","recipient":"a","pane_id":"w1:a","herdr_session":"s1","gen":0,"verified":true,"verified_by":"t"}"#,
    )
    .unwrap();
    match bound {
        InboxMsg::Bound { plane_session, .. } => assert_eq!(plane_session, "s1"),
        other => panic!("unexpected {other:?}"),
    }
    // Old wire name is snake_case (`rename_all`); the Rust-case alias is
    // belt-and-braces for records written by other producers. The old
    // envelope is derived from the live one so the test tracks the real
    // journal shape, not a hand-written guess.
    let live = serde_json::to_string(&InboxMsg::Invalidate(Invalidate::PlaneRestarted)).unwrap();
    assert!(live.contains("plane_restarted"), "{live}");
    for old in ["herdr_restarted", "HerdrRestarted"] {
        let back: InboxMsg = serde_json::from_str(&live.replace("plane_restarted", old)).unwrap();
        assert_eq!(back, InboxMsg::Invalidate(Invalidate::PlaneRestarted));
    }
}

// ---- properties -----------------------------------------------------

fn arb_msg() -> impl Strategy<Value = InboxMsg> {
    let who = prop_oneof![Just(A.to_string()), Just(B.to_string())];
    let pane_state = prop_oneof![
        Just(PaneState::Idle),
        Just(PaneState::Done),
        Just(PaneState::Working),
        Just(PaneState::Blocked),
        Just(PaneState::Unknown),
        Just(PaneState::Gone),
    ];
    let urgency = prop_oneof![Just(Urgency::Normal), Just(Urgency::Urgent)];
    prop_oneof![
        (who.clone(), 0u64..30, urgency).prop_map(|(recipient, head, urgency)| InboxMsg::Head {
            recipient,
            head,
            urgency
        }),
        (who.clone(), 0u64..30)
            .prop_map(|(recipient, cursor)| InboxMsg::Cursor { recipient, cursor }),
        (who.clone(), 0u64..3, any::<bool>()).prop_map(|(r, g, v)| bound(&r, g, v)),
        (who.clone(), pane_state)
            .prop_map(|(recipient, state)| InboxMsg::Pane { recipient, state }),
        prop_oneof![
            Just(InboxMsg::Invalidate(Invalidate::EventsLost)),
            Just(InboxMsg::Invalidate(Invalidate::PlaneRestarted))
        ],
        (who.clone(), 0u64..5_000_000)
            .prop_map(|(recipient, now_ms)| InboxMsg::Hint { recipient, now_ms }),
        (0u64..5_000_000).prop_map(|now_ms| InboxMsg::Tick { now_ms }),
    ]
}

proptest! {
    /// Safety, over arbitrary interleavings: every Ring goes to a verified
    /// binding at the current generation, bookkeeping stays inside the
    /// budget, the cursor and head never go backwards.
    #[test]
    fn rings_are_only_ever_safe(msgs in proptest::collection::vec(arb_msg(), 0..120)) {
        let mut m = InboxModel::default();
        for msg in &msgs {
            let before = m.clone();
            let u = InboxProgram::update(&m, msg);
            m = u.model;
            for c in &u.commands {
                if let InboxCmd::Ring { recipient, binding_gen, wm, line, .. } = c {
                    let r = &m.recipients[recipient];
                    let b = r.binding.as_ref().expect("a ring needs a binding");
                    prop_assert!(b.verified);
                    prop_assert_eq!(*binding_gen, m.generation);
                    prop_assert_eq!(b.generation, m.generation);
                    prop_assert!(r.pane != PaneState::Gone && r.pane != PaneState::Blocked
                        && r.pane != PaneState::Unknown);
                    prop_assert!(r.head > r.cursor);
                    prop_assert_eq!(*wm, r.head);
                    prop_assert_eq!(line, &wake_line(*wm));
                }
            }
            for (n, r) in &m.recipients {
                prop_assert!(r.rings_since_advance <= MAX_RINGS);
                if let Some(old) = before.recipients.get(n) {
                    prop_assert!(r.head >= old.head);
                    prop_assert!(r.cursor >= old.cursor);
                    if r.cursor == old.cursor && !matches!(msg, InboxMsg::Bound { .. }) {
                        prop_assert!(r.rings_since_advance >= old.rings_since_advance);
                    }
                }
            }
        }
    }

    /// The Phase 3 exit, over arbitrary histories: from any state, once the
    /// world is good (binding verified at the current generation, pane idle)
    /// and the recipient keeps ignoring its mail, every recipient with
    /// unread mail has a deaf fact within a bounded number of ticks. If it
    /// reads when rung instead, its cursor reaches head. Never a silent loss.
    #[test]
    fn p6_every_recipient_reads_or_is_declared_deaf(
        msgs in proptest::collection::vec(arb_msg(), 0..80),
        readers in any::<bool>(),
    ) {
        let (mut m, _) = fold(&InboxModel::default(), &msgs);
        for who in [A, B] {
            let g = m.generation;
            let (n, _) = fold(&m, &[bound(who, g, true), pane(who, PaneState::Idle)]);
            m = n;
        }
        let mut now = 10_000_000u64;
        let mut deafs = std::collections::BTreeSet::new();
        for _ in 0..(2 * MAX_RINGS + 4) {
            now += FAR;
            let (n, cmds) = fold(&m, &[tick(now)]);
            m = n;
            for c in cmds {
                match c {
                    InboxCmd::Ring { recipient, wm, .. } if readers => {
                        let (n, _) = fold(&m, &[cursor(&recipient, wm)]);
                        m = n;
                    }
                    InboxCmd::RecordDeaf { recipient, .. } => { deafs.insert(recipient); }
                    _ => {}
                }
            }
        }
        for (n, r) in &m.recipients {
            let caught_up = r.cursor >= r.head;
            if readers {
                // A reader is caught up, unless history had already spent its
                // budget (deaf recipients are not rung again).
                prop_assert!(caught_up || r.deaf.is_some(), "{n}: reader neither caught up nor deaf: {r:?}");
            } else {
                prop_assert!(caught_up || deafs.contains(n) || r.deaf.is_some(),
                    "{n}: silent stall {r:?}");
            }
        }
    }

    /// Restart from {model, offset}: snapshot anywhere, same result.
    #[test]
    fn restart_anywhere_is_the_same(msgs in proptest::collection::vec(arb_msg(), 0..60), k in 0usize..60) {
        let k = k.min(msgs.len());
        let (whole, whole_cmds) = fold(&InboxModel::default(), &msgs);
        let (mid, mut cmds) = fold(&InboxModel::default(), &msgs[..k]);
        let restored: InboxModel = serde_json::from_str(&serde_json::to_string(&mid).unwrap()).unwrap();
        let (end, rest) = fold(&restored, &msgs[k..]);
        cmds.extend(rest);
        prop_assert_eq!(end, whole);
        prop_assert_eq!(cmds, whole_cmds);
    }
}
