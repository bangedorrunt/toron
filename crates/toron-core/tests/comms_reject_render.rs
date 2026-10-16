// governed-by: ADR-0041
//! Every typed refusal must render (bd-reject-badenvelope-panic-bhls).
//! `Reject` is internally tagged, and a tagged newtype holding a string
//! cannot be serialized: `serde_json::to_value` failed for `BadEnvelope`,
//! and the CLI's `json!({"refused": r})` unwrapped it, so `toron inbox
//! append` on a non-envelope panicked instead of refusing with exit 3.

use toron_core::authority::Role;
use toron_core::delivery::{Phase, Reject};
use toron_core::envelope::Class;

/// One of every variant. The `match` makes adding a variant a compile error
/// here, so a new refusal cannot ship unrendered.
fn every_reject() -> Vec<Reject> {
    let all = vec![
        Reject::UnsupportedEnvelope { v: 9 },
        Reject::UnsupportedContract {
            name: "x".into(),
            major: 2,
        },
        Reject::ContractClassMismatch {
            contract: "task@1".into(),
            class: Class::Notify,
        },
        Reject::BadBody {
            contract: "task@1".into(),
            error: "e".into(),
        },
        Reject::bad_envelope("not an envelope"),
        Reject::BadAddress {
            field: "to",
            value: "bob".into(),
        },
        Reject::SenderIsNotProducer {
            from: "a".into(),
            producer: "b".into(),
        },
        Reject::GroupNotifyOnly { class: Class::Task },
        Reject::NotRecipient {
            actor: "a".into(),
            recipient: "b".into(),
        },
        Reject::CursorBehind {
            cursor: 1,
            offset: 2,
        },
        Reject::StaleGen {
            sent: 1,
            current: 2,
        },
        Reject::NotHolder { actor: "a".into() },
        Reject::SelfVerdict { actor: "a".into() },
        Reject::NotApplicable {
            class: Class::Notify,
            event: "accept",
        },
        Reject::Terminal {
            phase: Phase::Proved,
        },
        Reject::IllegalTransition {
            phase: Phase::Sent,
            event: "done",
        },
        Reject::Unauthorized {
            role: Some(Role::Crew),
            class: Class::Control,
            contract: "verdict@1".into(),
            expected: vec!["notify/*".into(), "done".into()],
        },
        Reject::Unauthorized {
            role: None,
            class: Class::Task,
            contract: "task@1".into(),
            expected: vec![],
        },
        Reject::NotYet {
            what: "supervisor".into(),
        },
        Reject::SteerPending {
            id: "prod:0:1".into(),
            op: "redirect".into(),
        },
        Reject::StaleSupEpoch {
            sent: 1,
            current: 2,
        },
    ];
    for r in &all {
        match r {
            Reject::UnsupportedEnvelope { .. }
            | Reject::UnsupportedContract { .. }
            | Reject::ContractClassMismatch { .. }
            | Reject::BadBody { .. }
            | Reject::BadEnvelope { .. }
            | Reject::BadAddress { .. }
            | Reject::SenderIsNotProducer { .. }
            | Reject::GroupNotifyOnly { .. }
            | Reject::NotRecipient { .. }
            | Reject::CursorBehind { .. }
            | Reject::StaleGen { .. }
            | Reject::NotHolder { .. }
            | Reject::SelfVerdict { .. }
            | Reject::NotApplicable { .. }
            | Reject::Terminal { .. }
            | Reject::IllegalTransition { .. }
            | Reject::Unauthorized { .. }
            | Reject::NotYet { .. }
            | Reject::SteerPending { .. }
            | Reject::StaleSupEpoch { .. } => {}
        }
    }
    all
}

#[test]
fn every_refusal_serializes_with_its_tag() {
    for r in every_reject() {
        let v =
            serde_json::to_value(&r).unwrap_or_else(|e| panic!("{r:?} does not serialize: {e}"));
        assert!(v["reject"].is_string(), "{r:?}: {v}");
        assert!(!r.to_string().is_empty());
    }
}

#[test]
fn bad_envelope_carries_its_reason_on_the_wire() {
    let v = serde_json::to_value(Reject::bad_envelope("context is empty")).unwrap();
    assert_eq!(v["reject"], "bad_envelope");
    assert_eq!(v["why"], "context is empty");
    assert_eq!(
        Reject::bad_envelope("x").to_string(),
        "bad envelope: x",
        "the human message is unchanged"
    );
}

/// Phase 4.1: the wire shape flywheel matches on. The tag is `unauthorized`;
/// `role` is the snake_case role, or null when the sender holds none;
/// `expected` is the verb set to use instead.
#[test]
fn unauthorized_carries_role_class_contract_and_the_expected_set_on_the_wire() {
    let v = serde_json::to_value(Reject::Unauthorized {
        role: Some(Role::LoopSelf),
        class: Class::Control,
        contract: "role@1".into(),
        expected: vec!["task/*".into(), "control/tick".into()],
    })
    .unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "reject": "unauthorized",
            "role": "loop_self",
            "class": "control",
            "contract": "role@1",
            "expected": ["task/*", "control/tick"],
        })
    );
    let none = serde_json::to_value(Reject::Unauthorized {
        role: None,
        class: Class::Task,
        contract: "task@1".into(),
        expected: vec![],
    })
    .unwrap();
    assert!(none["role"].is_null(), "{none}");
    let human = Reject::Unauthorized {
        role: Some(Role::Crew),
        class: Class::Control,
        contract: "verdict@1".into(),
        expected: vec!["notify/*".into(), "done".into()],
    }
    .to_string();
    assert_eq!(
        human,
        "role crew may not send verdict@1 (Control); allowed: [notify/*, done]"
    );
    assert_eq!(
        Reject::Unauthorized {
            role: None,
            class: Class::Task,
            contract: "task@1".into(),
            expected: vec![]
        }
        .to_string(),
        "a sender with no role here may not send task@1 (Task); allowed: []"
    );
}

/// Phase 4.2: supervisor is stored and refused, not run as a swarm.
/// No new CLI verb surfaces this; `inbox append` already prints any `Reject`.
#[test]
fn not_yet_carries_what_on_the_wire() {
    let v = serde_json::to_value(Reject::NotYet {
        what: "supervisor".into(),
    })
    .unwrap();
    assert_eq!(
        v,
        serde_json::json!({"reject": "not_yet", "what": "supervisor"})
    );
    assert_eq!(
        Reject::NotYet {
            what: "supervisor".into()
        }
        .to_string(),
        "not yet: supervisor"
    );
}

/// Phase 4a.2: a crew verb blocked on an unacked steer names the steer and its op.
#[test]
fn steer_pending_carries_the_steer_id_and_op_on_the_wire() {
    let v = serde_json::to_value(Reject::SteerPending {
        id: "prod:0:1".into(),
        op: "redirect".into(),
    })
    .unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "reject": "steer_pending",
            "id": "prod:0:1",
            "op": "redirect"
        })
    );
    assert_eq!(
        Reject::SteerPending {
            id: "prod:0:1".into(),
            op: "pause".into()
        }
        .to_string(),
        "steer prod:0:1 (pause) is unacked"
    );
}

/// Phase 4a.3: a lead epoch that is not the one on record.
#[test]
fn stale_sup_epoch_carries_sent_and_current_on_the_wire() {
    let v = serde_json::to_value(Reject::StaleSupEpoch {
        sent: 1,
        current: 4,
    })
    .unwrap();
    assert_eq!(
        v,
        serde_json::json!({"reject": "stale_sup_epoch", "sent": 1, "current": 4})
    );
    assert_eq!(
        Reject::StaleSupEpoch {
            sent: 1,
            current: 4
        }
        .to_string(),
        "stale sup_epoch 1, current 4"
    );
}
