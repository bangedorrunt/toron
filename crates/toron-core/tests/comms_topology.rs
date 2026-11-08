// governed-by: ADR-0041
//! Topology fact and projection (redesign §6.2, Phase 4.2).
//!
//! No context fact: `allowed_now_for` is today's phase table. A context fact
//! plus a role fact projects. Supervisor is stored and a send there is
//! `Reject::NotYet`, not a swarm send. A stale gen writes nothing.

use std::collections::BTreeMap;

use toron_core::authority::Role;
use toron_core::contracts::context_v1::ContextV1;
use toron_core::contracts::role_v1::RoleV1;
use toron_core::delivery::{DeliveryState, Phase, Reject};
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{InboxError, InboxStore, allowed_now, allowed_now_for};
use toron_core::index::schema;
use toron_core::topology::{self, Topology};

const OP: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const CAP: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const CTX: &str = "ctx-topo";

async fn store(tag: &str) -> InboxStore {
    let path =
        std::env::temp_dir().join(format!("toron-topo-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool).with_operator(OP)
}

/// The inbox sequences per `(producer, recipient)` scope. Give every fixture
/// message a fresh epoch and seq 0: each scope starts legitimately.
struct Seqs(BTreeMap<String, u64>);

impl Seqs {
    fn new() -> Self {
        Self(BTreeMap::new())
    }
    fn msg(&mut self, from: &str) -> MsgId {
        let epoch = self.0.entry(from.to_string()).or_insert(0);
        let msg = MsgId {
            producer: from.into(),
            epoch: *epoch,
            seq: 0,
        };
        *epoch += 1;
        msg
    }
}

fn role_fact(m: MsgId, subject: &str, role: Role, gen_: u32) -> Envelope {
    RoleV1 {
        npub: subject.into(),
        role,
        generation: gen_,
    }
    .into_envelope(m, ContextId(CTX.into()))
}

fn context_fact(m: MsgId, from_to: &str, topology: Topology, gen_: u32) -> Envelope {
    ContextV1 {
        topology,
        generation: gen_,
    }
    .into_envelope(m, ContextId(CTX.into()), from_to)
}

fn task(m: MsgId, to: &str) -> Envelope {
    Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(to.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId(CTX.into()),
        task: Some(TaskId("t-1".into())),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": "do it"}),
    }
}

async fn put(s: &InboxStore, _q: &mut Seqs, env: Envelope) {
    s.append_envelope(&env)
        .await
        .unwrap_or_else(|e| panic!("append {} failed: {e}", env.contract));
}

fn sample_delivery() -> (DeliveryState, Envelope) {
    let msg = MsgId {
        producer: CAP.into(),
        epoch: 0,
        seq: 0,
    };
    let env = task(msg, CREW);
    let mut state = DeliveryState::intent(Class::Task, CREW, 1);
    state.phase = Phase::Delivered;
    (state, env)
}

#[test]
fn no_topology_fact_matches_todays_allowed_now() {
    let (state, env) = sample_delivery();
    let key = "k";
    let today = allowed_now(&state, key, &env);
    assert_eq!(
        allowed_now_for(&state, key, &env, None, None),
        today,
        "no facts: the phase table, both calls"
    );
    assert_eq!(
        allowed_now_for(&state, key, &env, Some(Topology::Swarm), None),
        today,
        "a topology with no role fact does not project"
    );
    assert_eq!(
        allowed_now_for(&state, key, &env, None, Some(Role::Crew)),
        today,
        "a role with no topology fact does not project"
    );
    let mut sent = state.clone();
    sent.phase = Phase::Sent;
    assert_eq!(
        allowed_now_for(&sent, key, &env, None, None),
        allowed_now(&sent, key, &env)
    );
    let governed = allowed_now_for(&state, key, &env, Some(Topology::Swarm), Some(Role::Crew));
    assert_ne!(governed, today, "both facts leave the phase table");
    assert!(
        !governed.iter().any(|v| v == "task" || v == "verdict"),
        "crew projection is not allow-all: {governed:?}"
    );
    assert!(
        allowed_now_for(
            &state,
            key,
            &env,
            Some(Topology::Supervisor),
            Some(Role::Captain)
        )
        .is_empty(),
        "supervisor does not fall through to the swarm verb list"
    );
}

#[tokio::test]
async fn stale_topology_gen_writes_nothing() {
    let s = store("stale").await;
    let mut q = Seqs::new();
    let m = q.msg(OP);
    put(&s, &mut q, context_fact(m, OP, Topology::Swarm, 1)).await;
    let before = s.context_of(CTX).await.unwrap().expect("stored");
    assert_eq!(before.topology, Topology::Swarm);
    assert_eq!(before.generation, 1);
    let logs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox_log")
        .fetch_one(s.pool())
        .await
        .unwrap();
    let m = q.msg(OP);
    let stale = context_fact(m.clone(), OP, Topology::Open, 1);
    let err = s.append_envelope(&stale).await.unwrap_err();
    assert!(
        matches!(
            err,
            InboxError::Reject(Reject::StaleGen {
                sent: 1,
                current: 1
            })
        ),
        "{err:?}"
    );
    let after = s.context_of(CTX).await.unwrap().unwrap();
    assert_eq!(after, before, "stale gen does not replace the row");
    let logs_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox_log")
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(logs_after, logs, "stale gen writes no log row");
    assert!(s.lookup(&stale.msg).await.unwrap().is_none());
    put(&s, &mut q, context_fact(m, OP, Topology::Open, 2)).await;
    let bumped = s.context_of(CTX).await.unwrap().unwrap();
    assert_eq!(bumped.topology, Topology::Open);
    assert_eq!(bumped.generation, 2);
}

#[tokio::test]
async fn a_stranger_cannot_start_a_topology_and_a_captain_can_move_it() {
    let s = store("who").await;
    let mut q = Seqs::new();
    let m = q.msg(CREW);
    let err = s
        .append_envelope(&context_fact(m, CREW, Topology::Swarm, 0))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            InboxError::Reject(Reject::Unauthorized { role: None, .. })
        ),
        "{err:?}"
    );
    assert!(s.context_of(CTX).await.unwrap().is_none());
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CAP, Role::Captain, 0)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, context_fact(m, CAP, Topology::Open, 1)).await;
    assert_eq!(
        s.context_of(CTX).await.unwrap().unwrap().topology,
        Topology::Open
    );
}

#[tokio::test]
async fn supervisor_topology_send_is_not_yet_and_writes_nothing() {
    let s = store("sup").await;
    let mut q = Seqs::new();
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CAP, Role::Captain, 0)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, CREW, Role::Crew, 0)).await;
    let m = q.msg(OP);
    put(&s, &mut q, context_fact(m, OP, Topology::Supervisor, 1)).await;
    assert_eq!(
        s.context_of(CTX).await.unwrap().unwrap().topology,
        Topology::Supervisor
    );
    let m = q.msg(CAP);
    let sent = task(m.clone(), CREW);
    let err = s.append_envelope(&sent).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::NotYet { ref what }) if what == "supervisor"),
        "{err:?}"
    );
    assert!(s.lookup(&sent.msg).await.unwrap().is_none());
    assert_eq!(
        s.context_of(CTX).await.unwrap().unwrap().generation,
        1,
        "the refused send did not touch the context row"
    );
    put(&s, &mut q, context_fact(m, OP, Topology::Swarm, 2)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, task(m, CREW)).await;
    let batch = s.next(CREW, None, true).await.unwrap();
    let item = batch
        .items
        .iter()
        .find(|it| it.envelope.contract.name == "task")
        .expect("the task is in the crew inbox");
    assert!(
        !item.allowed_now.iter().any(|v| v == "task"),
        "next projects the crew, and a crew has no task: {:?}",
        item.allowed_now
    );
    let projected =
        topology::project(topology::global_of(Topology::Swarm).unwrap(), Role::Crew, 0).unwrap();
    assert_eq!(item.allowed_now, projected.verbs);
}

#[tokio::test]
async fn a_crew_task_stays_unauthorized_under_swarm_topology() {
    let s = store("crew").await;
    let mut q = Seqs::new();
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CAP, Role::Captain, 0)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, CREW, Role::Crew, 0)).await;
    let m = q.msg(OP);
    put(&s, &mut q, context_fact(m, OP, Topology::Swarm, 1)).await;
    let m = q.msg(CREW);
    let err = s.append_envelope(&task(m, CAP)).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::Unauthorized { .. })),
        "topology does not replace the authority table: {err:?}"
    );
}
