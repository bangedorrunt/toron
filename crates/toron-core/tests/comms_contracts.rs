// governed-by: ADR-0041
//! Phase 4.3 contracts: request, reply, cancel, approval, not_ready.
//! An unknown body key is `Reject::BadBody`. Schemas are the `JsonSchema`
//! derive, the same export `task@1` uses (`schema_for!`).

use schemars::schema_for;
use toron_core::contracts::{
    approval_v1::ApprovalV1, cancel_v1::CancelV1, escalate_v1::EscalateV1, goal_v1::GoalV1,
    not_ready_v1::NotReadyV1, reply_v1::ReplyV1, request_v1::RequestV1, steer_v1::SteerV1,
};
use toron_core::delivery::Reject;
use toron_core::envelope::{
    Class, ContextId, ContractId, ENVELOPE_V, Envelope, MsgId, Recipient, TaskId,
};

const FROM: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";

fn msg(seq: u64) -> MsgId {
    MsgId {
        producer: FROM.into(),
        epoch: 0,
        seq,
    }
}

fn env(class: Class, name: &str, body: serde_json::Value) -> Envelope {
    Envelope {
        v: ENVELOPE_V,
        from: FROM.into(),
        msg: msg(0),
        to: Recipient::Npub(FROM.into()),
        class,
        contract: ContractId::new(name, 1),
        context: ContextId("ctx".into()),
        task: Some(TaskId("run-1".into())),
        generation: 0,
        causation: Some(msg(1)),
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

fn bad_body(r: Result<(), Reject>) -> String {
    match r {
        Err(Reject::BadBody { contract, .. }) => contract,
        other => panic!("expected BadBody, got {other:?}"),
    }
}

#[test]
fn phase43_unknown_body_key_is_bad_body() {
    let cases = [
        (
            Class::Request,
            "request",
            serde_json::json!({"goal": "ship", "extra": 1}),
            "request@1",
        ),
        (
            Class::Reply,
            "reply",
            serde_json::json!({"text": "ok", "extra": 1}),
            "reply@1",
        ),
        (
            Class::Control,
            "cancel",
            serde_json::json!({"extra": 1}),
            "cancel@1",
        ),
        (
            Class::Control,
            "approval",
            serde_json::json!({"decision": "yes", "extra": 1}),
            "approval@1",
        ),
        (
            Class::Receipt,
            "not_ready",
            serde_json::json!({"reason": "busy", "extra": 1}),
            "not_ready@1",
        ),
    ];
    for (class, name, body, contract) in cases {
        assert_eq!(bad_body(env(class, name, body).validate()), contract);
    }
}

#[test]
fn phase43_contracts_accept_their_body_and_refuse_a_missing_link() {
    assert!(
        env(
            Class::Request,
            "request",
            serde_json::json!({"goal": "ship"})
        )
        .validate()
        .is_ok()
    );
    let mut reply = env(Class::Reply, "reply", serde_json::json!({"text": "done"}));
    assert!(reply.validate().is_ok());
    reply.causation = None;
    assert!(matches!(reply.validate(), Err(Reject::BadEnvelope { .. })));

    // gen 0 is legal; task is what cancel requires.
    let mut cancel = env(Class::Control, "cancel", serde_json::json!({}));
    assert_eq!(cancel.generation, 0);
    assert!(cancel.validate().is_ok());
    cancel.task = None;
    assert!(matches!(cancel.validate(), Err(Reject::BadEnvelope { .. })));

    // decision omitted. env.task is the run id.
    let mut approval = env(Class::Control, "approval", serde_json::json!({}));
    assert!(approval.validate().is_ok());
    approval.task = None;
    assert!(matches!(
        approval.validate(),
        Err(Reject::BadEnvelope { .. })
    ));

    assert!(
        env(
            Class::Receipt,
            "not_ready",
            serde_json::json!({"reason": "lease"})
        )
        .validate()
        .is_ok()
    );

    // The envelope itself still denies an unknown field.
    let mut v = serde_json::to_value(env(
        Class::Request,
        "request",
        serde_json::json!({"goal": "ship"}),
    ))
    .unwrap();
    v["nope"] = serde_json::json!(1);
    assert!(matches!(
        Envelope::from_json(&v.to_string()),
        Err(Reject::BadEnvelope { .. })
    ));
}

/// Phase 4a.4: goal, escalate, and steer. An unknown body key, an unknown
/// steer op, and a redirect with no delta are all `BadBody` (not a loose parse).
#[test]
fn phase4a_unknown_body_key_unknown_op_and_redirect_without_delta_are_bad_body() {
    let cases = [
        (
            "goal",
            serde_json::json!({
                "objective": "ship",
                "acceptance": [],
                "constraints": [],
                "extra": 1
            }),
        ),
        (
            "escalate",
            serde_json::json!({
                "context": "blocked on the lease",
                "reason": "holder is gone",
                "evidence": [],
                "extra": 1
            }),
        ),
        (
            "steer",
            serde_json::json!({
                "op": "pause",
                "priority": "normal",
                "sup_epoch": 1,
                "extra": 1
            }),
        ),
    ];
    for (name, body) in cases {
        assert_eq!(
            bad_body(env(Class::Control, name, body).validate()),
            format!("{name}@1")
        );
    }

    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "steer",
                serde_json::json!({"op": "frob", "priority": "normal", "sup_epoch": 1})
            )
            .validate()
        ),
        "steer@1",
        "an unknown op is not a loose accept"
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "steer",
                serde_json::json!({"op": "redirect", "priority": "urgent", "sup_epoch": 1})
            )
            .validate()
        ),
        "steer@1"
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "steer",
                serde_json::json!({
                    "op": "redirect",
                    "priority": "urgent",
                    "sup_epoch": 1,
                    "delta": null
                })
            )
            .validate()
        ),
        "steer@1",
        "null delta is the same as omitted"
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "steer",
                serde_json::json!({
                    "op": "redirect",
                    "priority": "normal",
                    "sup_epoch": 1,
                    "delta": {"nope": 1}
                })
            )
            .validate()
        ),
        "steer@1"
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "steer",
                serde_json::json!({
                    "op": "pause",
                    "priority": "normal",
                    "sup_epoch": 1,
                    "delta": {"goal": "x"}
                })
            )
            .validate()
        ),
        "steer@1",
        "only redirect carries a delta"
    );

    assert!(
        env(
            Class::Control,
            "goal",
            serde_json::json!({"objective": "ship", "acceptance": ["cargo test"], "constraints": []})
        )
        .validate()
        .is_ok()
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "goal",
                serde_json::json!({"objective": "  ", "acceptance": [], "constraints": []})
            )
            .validate()
        ),
        "goal@1"
    );
    assert!(
        env(
            Class::Control,
            "escalate",
            serde_json::json!({"context": "the lease", "reason": "holder is gone", "evidence": ["log"]})
        )
        .validate()
        .is_ok()
    );
    assert_eq!(
        bad_body(
            env(
                Class::Control,
                "escalate",
                serde_json::json!({"context": "c", "reason": "", "evidence": []})
            )
            .validate()
        ),
        "escalate@1"
    );
    assert!(
        env(
            Class::Control,
            "steer",
            serde_json::json!({
                "op": "redirect",
                "priority": "urgent",
                "sup_epoch": 3,
                "delta": {"goal": "ship the fence", "budget": 1}
            })
        )
        .validate()
        .is_ok()
    );
    assert!(
        env(
            Class::Control,
            "steer",
            serde_json::json!({"op": "pause", "priority": "normal", "sup_epoch": 0})
        )
        .validate()
        .is_ok()
    );
    let mut steer = env(
        Class::Control,
        "steer",
        serde_json::json!({"op": "cancel", "priority": "normal", "sup_epoch": 1}),
    );
    steer.task = None;
    assert!(matches!(steer.validate(), Err(Reject::BadEnvelope { .. })));
}

#[test]
fn phase4a_schemas_export_like_task() {
    for schema in [
        schema_for!(GoalV1),
        schema_for!(EscalateV1),
        schema_for!(SteerV1),
    ] {
        let v = serde_json::to_value(schema).unwrap();
        assert_eq!(v["type"], "object", "{v}");
        assert_eq!(v["additionalProperties"], false, "{v}");
    }
}

#[test]
fn phase43_schemas_export_like_task() {
    for schema in [
        schema_for!(RequestV1),
        schema_for!(ReplyV1),
        schema_for!(CancelV1),
        schema_for!(ApprovalV1),
        schema_for!(NotReadyV1),
    ] {
        let v = serde_json::to_value(schema).unwrap();
        assert_eq!(v["type"], "object", "{v}");
        assert_eq!(v["additionalProperties"], false, "{v}");
    }
}
