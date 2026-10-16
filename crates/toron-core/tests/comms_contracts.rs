// governed-by: ADR-0041
//! Phase 4.3 contracts: request, reply, cancel, approval, not_ready.
//! An unknown body key is `Reject::BadBody`. Schemas are the `JsonSchema`
//! derive, the same export `task@1` uses (`schema_for!`).

use schemars::schema_for;
use toron_core::contracts::{
    approval_v1::ApprovalV1, cancel_v1::CancelV1, not_ready_v1::NotReadyV1, reply_v1::ReplyV1,
    request_v1::RequestV1,
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
