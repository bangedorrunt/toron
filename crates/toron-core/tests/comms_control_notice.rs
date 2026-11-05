// governed-by: ADR-0041
//! The toron half of `bd-toron-known-contracts-control-b7wx`, end to end on
//! the store: the Control class is in `KNOWN`, an urgent steer read back from
//! the log is what drives the smaller ring budget, and a captain notice can
//! ride a Control contract (`escalate@1`) from the system producer.
//!
//! The pieces were landed separately (KNOWN rows, `urgent_pending`,
//! `Urgency::max_rings`); each had its own unit test, and nothing pinned the
//! path through a real append and a real `next`. This file does.
//!
//! What urgency is NOT: a `notify@1` or `escalate@1` notice, whatever its
//! text says, and `cancel@1`, whose body carries no `priority` field. Only
//! `steer@1` with `priority = "urgent"` is urgent.

use toron_core::contracts::{self, KNOWN};
use toron_core::delivery::Reject;
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{InboxStore, urgent_pending};
use toron_core::index::schema;
use toron_core::wake::{MAX_RINGS, MAX_RINGS_URGENT, Urgency};

const OP: &str = "0000000000000000000000000000000000000000000000000000000000000001";
/// Stands in for flywheel's `system_producer(project)`: a stable 64-hex id
/// with no role in any context.
const SYS: &str = "5959595959595959595959595959595959595959595959595959595959595901";
const CAP: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const CTX: &str = "proj-notice";

async fn store(tag: &str) -> InboxStore {
    let path = std::env::temp_dir().join(format!(
        "toron-notice-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool).with_operator(OP)
}

#[allow(clippy::too_many_arguments)]
fn env(
    from: &str,
    seq: u64,
    to: &str,
    class: Class,
    name: &str,
    context: &str,
    task: Option<&str>,
    body: serde_json::Value,
) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: from.into(),
            epoch: 0,
            seq,
        },
        from: from.into(),
        to: Recipient::Npub(to.into()),
        class,
        contract: ContractId::new(name, 1),
        context: ContextId(context.into()),
        task: task.map(|t| TaskId(t.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

/// The urgency the host derives from one peeked batch.
fn urgency(items: &[toron_core::inbox::InboxItem]) -> Urgency {
    if urgent_pending(items) {
        Urgency::Urgent
    } else {
        Urgency::Normal
    }
}

#[test]
fn known_carries_the_control_class_and_it_is_not_interchangeable() {
    let control: Vec<&str> = KNOWN
        .iter()
        .filter(|(_, _, c)| *c == Class::Control)
        .map(|(n, _, _)| *n)
        .collect();
    for name in ["steer", "cancel", "escalate", "lead", "goal", "role"] {
        assert!(
            control.contains(&name),
            "{name}@1 is a KNOWN Control contract"
        );
    }
    for name in &control {
        let e = env(
            SYS,
            0,
            CAP,
            Class::Notify,
            name,
            CTX,
            Some("t"),
            serde_json::json!({}),
        );
        assert_eq!(
            contracts::parse(&e).unwrap_err(),
            Reject::ContractClassMismatch {
                contract: format!("{name}@1"),
                class: Class::Notify,
            }
        );
    }
}

#[tokio::test]
async fn an_urgent_steer_in_the_log_drives_the_smaller_ring_budget() {
    let s = store("urgent").await;
    s.append_envelope(&env(
        OP,
        0,
        SYS,
        Class::Control,
        "lead",
        CTX,
        None,
        serde_json::json!({"holder": SYS, "sup_epoch": 1}),
    ))
    .await
    .expect("operator opens the lead");

    let empty = s.next(CREW, None, true).await.unwrap();
    assert_eq!(urgency(&empty.items), Urgency::Normal);

    s.append_envelope(&env(
        SYS,
        0,
        CREW,
        Class::Control,
        "steer",
        CTX,
        Some("t-1"),
        serde_json::json!({"op": "resume", "priority": "normal", "sup_epoch": 1}),
    ))
    .await
    .unwrap();
    let batch = s.next(CREW, None, true).await.unwrap();
    assert_eq!(urgency(&batch.items), Urgency::Normal);
    assert_eq!(urgency(&batch.items).max_rings(), MAX_RINGS);

    s.append_envelope(&env(
        SYS,
        1,
        CREW,
        Class::Control,
        "steer",
        CTX,
        Some("t-1"),
        serde_json::json!({"op": "pause", "priority": "urgent", "sup_epoch": 1}),
    ))
    .await
    .unwrap();
    let batch = s.next(CREW, None, true).await.unwrap();
    assert_eq!(batch.items.len(), 2, "peek leaves both unread");
    assert_eq!(urgency(&batch.items), Urgency::Urgent);
    assert_eq!(urgency(&batch.items).max_rings(), MAX_RINGS_URGENT);
    const { assert!(MAX_RINGS_URGENT < MAX_RINGS) };

    let err = s
        .append_envelope(&env(
            SYS,
            2,
            CREW,
            Class::Control,
            "cancel",
            CTX,
            Some("t-1"),
            serde_json::json!({"priority": "urgent"}),
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            toron_core::inbox::InboxError::Reject(Reject::BadBody { .. })
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_captain_notice_rides_escalate_as_control_and_is_not_urgent() {
    let s = store("notice").await;
    let notice_ctx = "proj:deaf-captain:aa01:7";
    let notice = env(
        SYS,
        0,
        CAP,
        Class::Control,
        "escalate",
        notice_ctx,
        None,
        serde_json::json!({
            "context": "TealBirch was rung the whole budget and never ran `toron next` (wm 7)",
            "reason": "deaf: no_answer",
            "evidence": ["wm 7"]
        }),
    );
    let appended = s.append_envelope(&notice).await.expect("the notice lands");
    assert!(!appended.replay);
    let batch = s.next(CAP, None, true).await.unwrap();
    let item = batch
        .items
        .iter()
        .find(|i| i.envelope.msg == notice.msg)
        .expect("the captain has the notice");
    assert_eq!(item.envelope.class, Class::Control);
    assert_eq!(item.envelope.contract, ContractId::new("escalate", 1));
    assert_eq!(urgency(&batch.items), Urgency::Normal);

    let loud = env(
        SYS,
        1,
        CAP,
        Class::Notify,
        "notify",
        notice_ctx,
        None,
        serde_json::json!({"text": "priority: urgent"}),
    );
    s.append_envelope(&loud).await.unwrap();
    let batch = s.next(CAP, None, true).await.unwrap();
    assert_eq!(urgency(&batch.items), Urgency::Normal);
}
