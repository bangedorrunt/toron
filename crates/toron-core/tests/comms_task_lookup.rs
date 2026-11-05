// governed-by: ADR-0041
//! Task-id lookup (bd-n24w).
//!
//! A sender that dispatched a task needs the task's whole conversation:
//! the assignment, the replies, each delivery's phase. Before this, the only
//! task-id read was `done`'s internal "latest delivery for this recipient".
//! `InboxStore::by_task` lists every envelope of a task the viewer sent or
//! received, oldest first, through two indexes and no scan.

use sqlx::Row as _;
use toron_core::delivery::Phase;
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{BY_TASK_SQL, InboxStore};
use toron_core::index::schema;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

async fn store(tag: &str) -> InboxStore {
    let path =
        std::env::temp_dir().join(format!("toron-task-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
}

fn env(from: &str, to: &str, class: Class, seq: u64, task: Option<&str>) -> Envelope {
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
        contract: ContractId::new(
            match class {
                Class::Task => "task",
                _ => "notify",
            },
            1,
        ),
        context: ContextId("ctx-task".into()),
        task: task.map(|t| TaskId(t.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: match class {
            Class::Task => {
                serde_json::json!({"title": format!("m{seq}"), "brief": "do it", "scope_paths": ["src/**"]})
            }
            _ => serde_json::json!({"text": format!("m{seq}")}),
        },
    }
}

/// A: assigns t-1 to B (seq 0), then sends a follow-up on t-1 to C (seq 1),
/// and an unrelated task t-2 to B (seq 2).
async fn seeded(tag: &str) -> InboxStore {
    let s = store(tag).await;
    s.append_envelope(&env(A, B, Class::Task, 0, Some("t-1")))
        .await
        .unwrap();
    s.append_envelope(&env(A, C, Class::Task, 1, Some("t-1")))
        .await
        .unwrap();
    s.append_envelope(&env(A, B, Class::Task, 2, Some("t-2")))
        .await
        .unwrap();
    s
}

/// REPRO: the sender finds both messages of its task, in log order, and not
/// the other task's.
#[tokio::test]
async fn the_sender_sees_every_message_of_its_task_in_order() {
    let s = seeded("sender").await;
    let got = s.by_task("t-1", A).await.unwrap();
    let keys: Vec<_> = got.iter().map(|m| m.msg_key.as_str()).collect();
    assert_eq!(keys, [format!("{A}:0:0"), format!("{A}:0:1")]);
    assert_eq!(got[0].recipient, B);
    assert_eq!(got[1].recipient, C);
    assert!(got[0].offset < got[1].offset);
    assert!(got.iter().all(|m| m.phase == Phase::Sent));
    assert!(
        got.iter()
            .all(|m| m.envelope.task.as_ref().unwrap().0 == "t-1")
    );
}

/// REPRO: the phase is the live delivery state, not the append-time one.
#[tokio::test]
async fn the_phase_follows_the_delivery() {
    let s = seeded("phase").await;
    s.next(B, None, false).await.unwrap();
    s.ack(B, &format!("{A}:0:0"), true, None).await.unwrap();
    let got = s.by_task("t-1", A).await.unwrap();
    assert_eq!(got[0].phase, Phase::Acknowledged);
    assert_eq!(got[0].holder.as_deref(), Some(B));
    assert_eq!(got[1].phase, Phase::Sent, "C has not read its copy");
}

/// REPRO: a recipient sees only its own copy of a task that went to several.
#[tokio::test]
async fn a_recipient_sees_only_what_it_received() {
    let s = seeded("recipient").await;
    let got = s.by_task("t-1", B).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].recipient, B);
}

/// CONTROL: a viewer who neither sent nor received anything gets nothing,
/// the same answer as an unknown task. No leak, no error.
#[tokio::test]
async fn control_a_stranger_sees_nothing() {
    let s = seeded("stranger").await;
    assert!(s.by_task("t-1", D).await.unwrap().is_empty());
    assert!(s.by_task("t-unknown", A).await.unwrap().is_empty());
}

/// CONTROL: messages without a task id are never returned for any task id.
#[tokio::test]
async fn control_messages_without_a_task_are_not_matched() {
    let s = seeded("notask").await;
    s.append_envelope(&env(A, B, Class::Notify, 3, None))
        .await
        .unwrap();
    for t in ["t-1", "t-2", ""] {
        let got = s.by_task(t, A).await.unwrap();
        assert!(got.iter().all(|m| m.msg_key != format!("{A}:0:3")), "{t}");
    }
}

/// CONTROL: looking up does not move anything (read-only): deliveries stay
/// Sent and B's cursor stays at 0.
#[tokio::test]
async fn control_the_lookup_is_read_only() {
    let s = seeded("ro").await;
    let _ = s.by_task("t-1", B).await.unwrap();
    assert_eq!(s.cursor(B, B).await.unwrap(), 0);
    assert_eq!(s.by_task("t-1", A).await.unwrap()[0].phase, Phase::Sent);
}

/// The lookup reaches both tables by index. A plan line that SCANs either
/// table would make it O(log size) per call; SEARCH ... USING INDEX is the
/// contract.
#[tokio::test]
async fn the_query_plan_uses_both_indexes_and_scans_nothing() {
    let s = seeded("plan").await;
    let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {BY_TASK_SQL}"))
        .bind("t-1")
        .fetch_all(s.pool())
        .await
        .unwrap();
    let plan: Vec<String> = rows.iter().map(|r| r.get::<String, _>(3)).collect();
    let text = plan.join("\n");
    assert!(
        !plan.iter().any(|l| l.starts_with("SCAN")),
        "scan in plan:\n{text}"
    );
    assert!(
        plan[0].contains("inbox_delivery_by_task"),
        "the task index must drive:\n{text}"
    );
    assert!(text.contains("inbox_log_by_msg_key"), "{text}");
    assert!(
        !text.contains("inbox_log_by_recipient"),
        "the log must not be walked by stream:\n{text}"
    );
}
