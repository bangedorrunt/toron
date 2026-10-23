// governed-by: ADR-0041
//! Phase 4a.1: a steer is a control fact at append.
//!
//! Crew may not send one, and the refusal writes no row. A captain's
//! `redirect` bumps the task generation, so `done` at the old gen is
//! `StaleGen` and writes nothing. `pause` does not bump that gen.

use std::collections::BTreeMap;

use toron_core::authority::Role;
use toron_core::contracts::context_v1::ContextV1;
use toron_core::contracts::role_v1::RoleV1;
use toron_core::delivery::{DeliveryEvent, Phase, Reject};
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{InboxError, InboxStore, steer_pending_verbs};
use toron_core::index::schema;
use toron_core::topology::Topology;

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

/// The operator opens the lead lease for the captain at epoch 1, the epoch
/// `steer_env` names. A steer needs the lead holder (F7): with no row, only
/// an operator steer is stored.
async fn led(s: &InboxStore, q: &mut Seqs) {
    let m = q.msg(OP);
    put(s, q, lead_env(m, CAP, 1)).await;
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

/// Deliver the steer `claimed` left unread and accept it. That is the ack
/// that clears `pending`; until it lands, crew verbs are `SteerPending`.
async fn ack_steer(s: &InboxStore) -> String {
    let batch = s.next(CREW, None, false).await.unwrap();
    let item = batch
        .items
        .iter()
        .find(|i| i.envelope.contract.name == "steer")
        .expect("the steer was delivered");
    let key = item.msg_key.clone();
    s.ack(CREW, &key, true, None).await.unwrap();
    key
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
    led(&s, &mut q).await;
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
    // The pending gate (4a.2) refuses done until this ack. After it, the
    // old gen is still stale and the done still writes nothing.
    ack_steer(&s).await;
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
    led(&s, &mut q).await;
    let key = claimed(&s, &mut q).await;
    let m = q.msg(CAP);
    put(&s, &mut q, steer_env(m, "pause", None)).await;
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 1, "pause does not bump gen");
    assert_eq!(d.phase, Phase::Acknowledged);
    ack_steer(&s).await;
    let (_k, done) = s.done(CREW, TASK, &["evidence".into()]).await.unwrap();
    assert_eq!(done.phase, Phase::Acted);
    assert_eq!(done.generation, 1);
}

fn is_steer_pending(err: &InboxError) -> bool {
    matches!(
        err,
        InboxError::Reject(Reject::SteerPending { op, .. }) if op == "redirect"
    )
}

#[test]
fn steer_pending_verbs_are_the_two_ack_lines() {
    assert_eq!(
        steer_pending_verbs("p:0:1"),
        vec![
            "toron ack p:0:1 accept".to_string(),
            "toron ack p:0:1 reject --reason <why>".to_string(),
        ]
    );
}

#[tokio::test]
async fn progress_and_done_wait_until_the_steer_is_acked() {
    let (s, mut q) = swarm("pending").await;
    led(&s, &mut q).await;
    let key = claimed(&s, &mut q).await;
    let m = q.msg(CAP);
    let steer = steer_env(m, "redirect", Some(serde_json::json!({"goal": "retarget"})));
    let steer_key = steer.msg.key();
    put(&s, &mut q, steer).await;

    let d = s.delivery(&key, CREW).await.unwrap();
    let touched = touched_at(&s, &key).await;
    let progress = s
        .step(
            &key,
            CREW,
            &DeliveryEvent::Progress {
                by: CREW.into(),
                generation: d.generation,
            },
            None,
        )
        .await
        .unwrap_err();
    assert!(is_steer_pending(&progress), "{progress:?}");
    // A non-steer ack of the task is the same refusal, ahead of the FSM.
    let ack = s.ack(CREW, &key, true, None).await.unwrap_err();
    assert!(is_steer_pending(&ack), "{ack:?}");
    let done = s.done(CREW, TASK, &["evidence".into()]).await.unwrap_err();
    assert!(is_steer_pending(&done), "{done:?}");
    assert_eq!(touched_at(&s, &key).await, touched);
    assert_eq!(
        s.delivery(&key, CREW).await.unwrap().phase,
        Phase::Acknowledged
    );

    // A swarm fact would project crew verbs ("done") over the ack lines.
    // The head item's pending steer replaces that with the two ack lines.
    let m = q.msg(OP);
    put(
        &s,
        &mut q,
        ContextV1 {
            topology: Topology::Swarm,
            generation: 1,
        }
        .into_envelope(m, ContextId(CTX.into()), OP),
    )
    .await;
    let batch = s.next(CREW, None, false).await.unwrap();
    assert_eq!(
        batch.items.len(),
        1,
        "only the unread steer is in this batch"
    );
    assert_eq!(batch.items[0].msg_key, steer_key);
    assert_eq!(
        batch.items[0].allowed_now,
        steer_pending_verbs(&steer_key),
        "pending steer on the head item, not the phase table and not the projection"
    );

    s.ack(CREW, &steer_key, true, None).await.unwrap();
    let after = s.done(CREW, TASK, &["evidence".into()]).await.unwrap_err();
    assert!(
        matches!(
            after,
            InboxError::Reject(Reject::StaleGen {
                sent: 1,
                current: 2
            })
        ),
        "acked redirect is no longer SteerPending: {after:?}"
    );
    assert_eq!(touched_at(&s, &key).await, touched);
}

fn lead_env(m: MsgId, holder: &str, epoch: u64) -> Envelope {
    Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(holder.into()),
        class: Class::Control,
        contract: ContractId::new("lead", 1),
        context: ContextId(CTX.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"holder": holder, "sup_epoch": epoch}),
    }
}

fn at_epoch(mut env: Envelope, epoch: u64) -> Envelope {
    env.body["sup_epoch"] = serde_json::json!(epoch);
    env
}

#[tokio::test]
async fn a_lower_sup_epoch_steer_writes_nothing_and_the_current_one_is_stored() {
    let (s, mut q) = swarm("epoch").await;
    // The first lead is operator-signed. A captain cannot open the lease.
    let m = q.msg(CAP);
    let early = lead_env(m.clone(), CAP, 1);
    let err = s.append_envelope(&early).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::Unauthorized { .. })),
        "{err:?}"
    );
    assert!(s.lookup(&early.msg).await.unwrap().is_none());

    let op = q.msg(OP);
    put(&s, &mut q, lead_env(op, CAP, 1)).await;
    // The captain may raise it. Epoch 2 is current; epoch 1 is now stale.
    put(&s, &mut q, lead_env(m, CAP, 2)).await;
    let current: i64 = sqlx::query_scalar("SELECT sup_epoch FROM inbox_lead WHERE context = ?1")
        .bind(CTX)
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert_eq!(current, 2);
    let row = s.lead_of(CTX).await.unwrap().expect("the lead row");
    assert_eq!(row.sup_epoch, current as u64);
    assert_eq!(row.holder, CAP);
    assert!(s.lead_of("no-such-context").await.unwrap().is_none());

    let low_m = q.msg(CAP);
    let low = at_epoch(steer_env(low_m.clone(), "pause", None), 1);
    let err = s.append_envelope(&low).await.unwrap_err();
    assert!(
        matches!(
            err,
            InboxError::Reject(Reject::StaleSupEpoch {
                sent: 1,
                current: 2
            })
        ),
        "{err:?}"
    );
    assert!(s.lookup(&low.msg).await.unwrap().is_none());
    let unseen = s.next(CREW, None, true).await.unwrap();
    assert!(
        unseen.items.iter().all(|i| i.envelope.msg != low.msg),
        "the lower epoch is not in the recipient log"
    );
    assert_eq!(steer_rows(&s).await, 0);

    let high = at_epoch(steer_env(low_m, "pause", None), 2);
    put(&s, &mut q, high.clone()).await;
    assert!(s.lookup(&high.msg).await.unwrap().is_some());
    assert_eq!(steer_rows(&s).await, 1);
    let seen = s.next(CREW, None, true).await.unwrap();
    assert!(
        seen.items.iter().any(|i| i.envelope.msg == high.msg),
        "the current epoch's steer is stored for the recipient"
    );
}

/// A supervisor context refuses ordinary sends (`NotYet`) and still stores
/// `goal@1` and a crew `escalate@1`. A crew `goal` stays unauthorized.
#[tokio::test]
async fn goal_and_escalate_land_in_a_supervisor_context() {
    let (s, mut q) = swarm("sup-facts").await;
    let m = q.msg(OP);
    put(
        &s,
        &mut q,
        ContextV1 {
            topology: Topology::Supervisor,
            generation: 1,
        }
        .into_envelope(m, ContextId(CTX.into()), OP),
    )
    .await;

    let m = q.msg(CAP);
    let task = task_env(m);
    let err = s.append_envelope(&task).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::NotYet { .. })),
        "a task in a supervisor context is still NotYet: {err:?}"
    );
    assert!(s.lookup(&task.msg).await.unwrap().is_none());

    let m = q.msg(CAP);
    let goal = Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(CREW.into()),
        class: Class::Control,
        contract: ContractId::new("goal", 1),
        context: ContextId(CTX.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({
            "objective": "land the facts",
            "acceptance": ["stored"],
            "constraints": []
        }),
    };
    put(&s, &mut q, goal.clone()).await;
    assert!(s.lookup(&goal.msg).await.unwrap().is_some());

    let m = q.msg(CREW);
    let crew_goal = Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(CAP.into()),
        class: Class::Control,
        contract: ContractId::new("goal", 1),
        context: ContextId(CTX.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({
            "objective": "crew must not set this",
            "acceptance": [],
            "constraints": []
        }),
    };
    let err = s.append_envelope(&crew_goal).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::Unauthorized { .. })),
        "{err:?}"
    );
    assert!(s.lookup(&crew_goal.msg).await.unwrap().is_none());

    let m = q.msg(CREW);
    let up = Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m.clone(),
        to: Recipient::Npub(CAP.into()),
        class: Class::Control,
        contract: ContractId::new("escalate", 1),
        context: ContextId(CTX.into()),
        task: None,
        generation: 0,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({
            "context": "the crew is stuck",
            "reason": "no path",
            "evidence": []
        }),
    };
    put(&s, &mut q, up.clone()).await;
    assert!(s.lookup(&up.msg).await.unwrap().is_some());
}

/// F7 (stack review 2026-10-23): with a lead row the fence binds the sender
/// to the lead holder at the current epoch; with none, only the operator
/// steers. Every refusal writes nothing.
async fn refused_and_absent(s: &InboxStore, env: &Envelope) -> Reject {
    let err = s.append_envelope(env).await.unwrap_err();
    assert!(
        s.lookup(&env.msg).await.unwrap().is_none(),
        "a refused steer leaves no log row"
    );
    match err {
        InboxError::Reject(r) => r,
        other => panic!("not a typed refusal: {other:?}"),
    }
}

#[tokio::test]
async fn with_no_lead_row_only_the_operator_steers() {
    let (s, mut q) = swarm("nolead").await;
    let key = claimed(&s, &mut q).await;
    // The captain and the supervisor may steer by role, and name the epoch a
    // lead would carry. No lead@1 row exists, so neither holds the lead.
    for who in [CAP, SUP] {
        let env = steer_env(q.msg(who), "pause", None);
        let r = refused_and_absent(&s, &env).await;
        assert_eq!(
            r,
            Reject::NoLead {
                context: CTX.into()
            },
            "{who}"
        );
    }
    assert_eq!(steer_rows(&s).await, 0);
    // The operator key steers with no lead: stored, projected, delivered.
    let op = steer_env(q.msg(OP), "pause", None);
    put(&s, &mut q, op.clone()).await;
    assert!(s.lookup(&op.msg).await.unwrap().is_some());
    assert_eq!(steer_rows(&s).await, 1);
    let seen = s.next(CREW, None, true).await.unwrap();
    assert!(seen.items.iter().any(|i| i.envelope.msg == op.msg));
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 1, "pause does not fence the task");
}

#[tokio::test]
async fn a_non_holder_at_the_current_epoch_is_refused() {
    let (s, mut q) = swarm("nonholder").await;
    led(&s, &mut q).await;
    let key = claimed(&s, &mut q).await;
    // SUP may steer by role and names the current epoch, but CAP holds it.
    let env = steer_env(
        q.msg(SUP),
        "redirect",
        Some(serde_json::json!({"goal": "hijack"})),
    );
    let r = refused_and_absent(&s, &env).await;
    assert_eq!(
        r,
        Reject::NotLeadHolder {
            actor: SUP.into(),
            holder: CAP.into(),
            sup_epoch: 1,
        }
    );
    // The operator is not the holder either.
    let op = steer_env(q.msg(OP), "cancel", None);
    let r = refused_and_absent(&s, &op).await;
    assert!(
        matches!(&r, Reject::NotLeadHolder { actor, .. } if actor == OP),
        "{r:?}"
    );
    assert_eq!(steer_rows(&s).await, 0);
    let d = s.delivery(&key, CREW).await.unwrap();
    assert_eq!(d.generation, 1, "the refused redirect did not bump the gen");
}

#[tokio::test]
async fn the_holder_at_the_current_epoch_is_accepted_and_a_stale_one_is_not() {
    let (s, mut q) = swarm("holder").await;
    led(&s, &mut q).await;
    let key = claimed(&s, &mut q).await;
    // The captain raises the lease to epoch 2 and stays the holder.
    let m = q.msg(CAP);
    put(&s, &mut q, lead_env(m, CAP, 2)).await;

    // Stale: the holder itself at the old epoch is still StaleSupEpoch, and
    // so is a non-holder (the epoch is checked first).
    let stale = at_epoch(steer_env(q.msg(CAP), "pause", None), 1);
    let r = refused_and_absent(&s, &stale).await;
    assert_eq!(
        r,
        Reject::StaleSupEpoch {
            sent: 1,
            current: 2
        }
    );
    let stale_sup = at_epoch(steer_env(q.msg(SUP), "pause", None), 1);
    let r = refused_and_absent(&s, &stale_sup).await;
    assert!(matches!(r, Reject::StaleSupEpoch { .. }), "{r:?}");
    assert_eq!(steer_rows(&s).await, 0);

    // Holder at the current epoch: stored, projected, delivered.
    let ok = at_epoch(steer_env(q.msg(CAP), "pause", None), 2);
    put(&s, &mut q, ok.clone()).await;
    assert!(s.lookup(&ok.msg).await.unwrap().is_some());
    assert_eq!(steer_rows(&s).await, 1);
    let batch = s.next(CREW, None, true).await.unwrap();
    assert!(batch.items.iter().any(|i| i.envelope.msg == ok.msg));
    assert_eq!(s.delivery(&key, CREW).await.unwrap().generation, 1);

    // The dry run (`authorize`) answers the same fence.
    let dry = steer_env(q.msg(SUP), "pause", None);
    let dry = at_epoch(dry, 2);
    let err = s.authorize(&dry).await.unwrap_err();
    assert!(
        matches!(err, InboxError::Reject(Reject::NotLeadHolder { .. })),
        "{err:?}"
    );
}
