// governed-by: ADR-0041
//! Phase 4a.1: a steer is a control fact at append.
//!
//! Crew may not send one, and the refusal writes no row. A captain's
//! `redirect` bumps the task generation, so `done` at the old gen is
//! `StaleGen` and writes nothing. `pause` does not bump that gen.

use std::collections::BTreeMap;

use toron_core::authority::Role;
use toron_core::contracts::role_v1::RoleV1;
use toron_core::delivery::{Phase, Reject};
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{InboxError, InboxStore};
use toron_core::index::schema;

const OP: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const CAP: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01";
const SUP: &str = "5555555555555555555555555555555555555555555555555555555555555501";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const CTX: &str = "ctx-steer";
const TASK: &str = "t-steer";

async fn store(tag: &str) -> InboxStore {
    let path =
        std::env::temp_dir().join(format!("toron-steer-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool).with_operator(OP)
}

struct Seqs(BTreeMap<String, u64>);

impl Seqs {
    fn new() -> Self {
        Self(BTreeMap::new())
    }
    fn msg(&mut self, from: &str) -> MsgId {
        let n = self.0.entry(from.to_string()).or_insert(0);
        MsgId {
            producer: from.into(),
            epoch: 0,
            seq: *n,
        }
    }
    fn used(&mut self, from: &str) {
        *self.0.get_mut(from).unwrap() += 1;
    }
}

async fn put(s: &InboxStore, q: &mut Seqs, env: Envelope) {
    let from = env.from.clone();
    s.append_envelope(&env)
        .await
        .unwrap_or_else(|e| panic!("append {} {} failed: {e}", env.class.as_str(), env.contract));
    q.used(&from);
}

fn role_fact(m: MsgId, subject: &str, role: Role) -> Envelope {
    RoleV1 {
        npub: subject.into(),
        role,
        generation: 0,
    }
    .into_envelope(m, ContextId(CTX.into()))
}

fn task_env(m: MsgId) -> Envelope {
    Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(CREW.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId(CTX.into()),
        task: Some(TaskId(TASK.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": "do it"}),
    }
}

fn steer_env(m: MsgId, op: &str, delta: Option<serde_json::Value>) -> Envelope {
    let mut body = serde_json::json!({
        "op": op,
        "priority": "normal",
        "sup_epoch": 1
    });
    if let Some(delta) = delta {
        body["delta"] = delta;
    }
    Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(CREW.into()),
        class: Class::Control,
        contract: ContractId::new("steer", 1),
        context: ContextId(CTX.into()),
        task: Some(TaskId(TASK.into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body,
    }
}

/// Operator names the captain, the captain names a supervisor, the supervisor
/// names the crew. No topology fact: a steer is not a supervisor-program send.
async fn swarm(tag: &str) -> (InboxStore, Seqs) {
    let s = store(tag).await;
    let mut q = Seqs::new();
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CAP, Role::Captain)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, SUP, Role::Supervisor)).await;
    let m = q.msg(SUP);
    put(&s, &mut q, role_fact(m, CREW, Role::Crew)).await;
    (s, q)
}

/// Append the task, let the crew read it, and accept it. Returns the task key.
async fn claimed(s: &InboxStore, q: &mut Seqs) -> String {
    let m = q.msg(CAP);
    put(s, q, task_env(m)).await;
    let batch = s.next(CREW, None, false).await.unwrap();
    let item = batch
        .items
        .iter()
        .find(|i| i.envelope.contract.name == "task")
        .expect("the task was delivered");
    let key = item.msg_key.clone();
    s.ack(CREW, &key, true, None).await.unwrap();
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!((d.phase, d.generation), (Phase::Acknowledged, 1));
    key
}

async fn steer_rows(s: &InboxStore) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM inbox_steer")
        .fetch_one(s.pool())
        .await
        .unwrap()
}

async fn touched_at(s: &InboxStore, msg_key: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT updated_ts_us FROM inbox_delivery WHERE msg_key = ?1 AND recipient = ?2",
    )
    .bind(msg_key)
    .bind(CREW)
    .fetch_one(s.pool())
    .await
    .unwrap()
}

#[tokio::test]
async fn crew_steer_is_refused_and_writes_no_row() {
    let (s, mut q) = swarm("crew").await;
    let key = claimed(&s, &mut q).await;
    let m = q.msg(CREW);
    let env = steer_env(
        m.clone(),
        "redirect",
        Some(serde_json::json!({"goal": "nope"})),
    );
    let err = s.append_envelope(&env).await.unwrap_err();
    assert!(
        matches!(
            err,
            InboxError::Reject(Reject::Unauthorized {
                role: Some(Role::Crew),
                ..
            })
        ),
        "{err:?}"
    );
    assert_eq!(steer_rows(&s).await, 0, "a refused steer leaves no row");
    assert!(s.lookup(&env.msg).await.unwrap().is_none());
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 1, "the refusal does not fence the task");
    // The same seq is still free: a notify the crew may send lands on it.
    let note = Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(CAP.into()),
        class: Class::Notify,
        contract: ContractId::new("notify", 1),
        context: ContextId(CTX.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"text": "still here"}),
    };
    put(&s, &mut q, note).await;
}

#[tokio::test]
async fn captain_redirect_then_done_at_the_old_gen_writes_nothing() {
    let (s, mut q) = swarm("redirect").await;
    let key = claimed(&s, &mut q).await;
    let m = q.msg(CAP);
    put(
        &s,
        &mut q,
        steer_env(m, "redirect", Some(serde_json::json!({"goal": "retarget"}))),
    )
    .await;
    assert_eq!(steer_rows(&s).await, 1);
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 2, "redirect bumps the task gen");
    assert_eq!(d.phase, Phase::Acknowledged);
    let touched = touched_at(&s, &key).await;
    let err = s.done(CREW, TASK, &["evidence".into()]).await.unwrap_err();
    assert!(
        matches!(
            err,
            InboxError::Reject(Reject::StaleGen {
                sent: 1,
                current: 2
            })
        ),
        "{err:?}"
    );
    let after = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(after.phase, Phase::Acknowledged);
    assert_eq!(after.generation, 2);
    assert_eq!(
        touched_at(&s, &key).await,
        touched,
        "stale done does not write the delivery"
    );
}

#[tokio::test]
async fn pause_does_not_make_the_old_gen_stale() {
    let (s, mut q) = swarm("pause").await;
    let key = claimed(&s, &mut q).await;
    let m = q.msg(CAP);
    put(&s, &mut q, steer_env(m, "pause", None)).await;
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 1, "pause does not bump gen");
    assert_eq!(d.phase, Phase::Acknowledged);
    let (_k, done) = s.done(CREW, TASK, &["evidence".into()]).await.unwrap();
    assert_eq!(done.phase, Phase::Acted);
    assert_eq!(done.generation, 1);
}
