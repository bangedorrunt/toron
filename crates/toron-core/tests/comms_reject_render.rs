// governed-by: ADR-0041
//! Every typed refusal must render (bd-reject-badenvelope-panic-bhls).
//! `Reject` is internally tagged, and a tagged newtype holding a string
//! cannot be serialized: `serde_json::to_value` failed for `BadEnvelope`,
//! and the CLI's `json!({"refused": r})` unwrapped it, so `toron inbox
//! append` on a non-envelope panicked instead of refusing with exit 3.

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
            | Reject::IllegalTransition { .. } => {}
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
