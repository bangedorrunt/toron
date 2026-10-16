// governed-by: ADR-0041
//! Roles and authority at append (redesign §6.1, Phase 4.1).
//!
//! A context with no role fact is untouched. The first role fact (the
//! operator's `captain`) governs it; from then on `append_envelope` refuses,
//! before anything is written, what the sender's role may not send, with a
//! typed `Reject::Unauthorized` carrying the verbs it may.

use toron_core::authority::{Authority, Grant, Role, Standing};
use toron_core::contracts::{self, role_v1::RoleV1};
use toron_core::delivery::{Phase, Reject};
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{INBOX_STREAM, InboxError, InboxStore, l16_json};
use toron_core::index::schema;

const OP: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const CAP: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc01";
const SUP: &str = "5555555555555555555555555555555555555555555555555555555555555501";
const CREW: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa01";
const CREW2: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa02";
const PEER: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee01";
const VER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb01";
const LOOP: &str = "1111111111111111111111111111111111111111111111111111111111111101";
const STRANGER: &str = "9999999999999999999999999999999999999999999999999999999999999901";
const CTX: &str = "ctx-swarm";

async fn store(tag: &str) -> InboxStore {
    let path =
        std::env::temp_dir().join(format!("toron-auth-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool).with_operator(OP)
}

/// A sender's next sequence number; the log is gap-free per producer, so a
/// refused append must not consume one.
struct Seqs(std::collections::BTreeMap<String, u64>);

impl Seqs {
    fn new() -> Self {
        Self(Default::default())
    }
    fn msg(&mut self, from: &str) -> MsgId {
        let n = self.0.entry(from.to_string()).or_insert(0);
        MsgId {
            producer: from.into(),
            epoch: 0,
            seq: *n,
        }
    }
    /// Mark the sequence number as used (the append landed).
    fn used(&mut self, from: &str) {
        *self.0.get_mut(from).unwrap() += 1;
    }
}

fn role_fact(m: MsgId, ctx: &str, subject: &str, role: Role, gen_: u32) -> Envelope {
    RoleV1 {
        npub: subject.into(),
        role,
        generation: gen_,
    }
    .into_envelope(m, ContextId(ctx.into()))
}

fn base(m: MsgId, to: &str, class: Class, contract: ContractId, ctx: &str) -> Envelope {
    Envelope {
        v: 1,
        from: m.producer.clone(),
        msg: m,
        to: Recipient::Npub(to.into()),
        class,
        contract,
        context: ContextId(ctx.into()),
        task: None,
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::Value::Null,
    }
}

fn task(m: MsgId, to: &str, ctx: &str, id: &str) -> Envelope {
    let mut e = base(m, to, Class::Task, ContractId::new("task", 1), ctx);
    e.task = Some(TaskId(id.into()));
    e.body = serde_json::json!({"title": "t", "brief": "do it"});
    e
}

fn notify(m: MsgId, to: &str, ctx: &str) -> Envelope {
    let mut e = base(m, to, Class::Notify, ContractId::new("notify", 1), ctx);
    e.body = serde_json::json!({"text": "hello"});
    e
}

fn verdict(m: MsgId, to: &str, ctx: &str, id: &str) -> Envelope {
    let mut e = base(m, to, Class::Control, ContractId::new("verdict", 1), ctx);
    e.task = Some(TaskId(id.into()));
    e.body = serde_json::json!({"pass": true, "evidence_refs": ["cargo test"]});
    e
}

fn unauthorized(r: Result<impl std::fmt::Debug, InboxError>) -> Reject {
    match r {
        Err(InboxError::Reject(r @ Reject::Unauthorized { .. })) => r,
        other => panic!("expected Unauthorized, got {other:?}"),
    }
}

/// Append and consume the producer's sequence number.
async fn put(s: &InboxStore, q: &mut Seqs, env: Envelope) {
    let from = env.from.clone();
    s.append_envelope(&env)
        .await
        .unwrap_or_else(|e| panic!("append {} {} failed: {e}", env.class.as_str(), env.contract));
    q.used(&from);
}

/// A governed swarm: operator makes CAP captain; CAP makes SUP supervisor,
/// SUP makes CREW/CREW2 crew, PEER peer, VER verifier; CAP makes LOOP loop_self.
async fn swarm(tag: &str) -> (InboxStore, Seqs) {
    let s = store(tag).await;
    let mut q = Seqs::new();
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CTX, CAP, Role::Captain, 0)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, CTX, SUP, Role::Supervisor, 0)).await;
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, CTX, LOOP, Role::LoopSelf, 0)).await;
    for (who, role) in [
        (CREW, Role::Crew),
        (CREW2, Role::Crew),
        (PEER, Role::Peer),
        (VER, Role::Verifier),
    ] {
        let m = q.msg(SUP);
        put(&s, &mut q, role_fact(m, CTX, who, role, 0)).await;
    }
    (s, q)
}

/// Every denial §6.1 demands, through the real append. Returns what did NOT
/// happen as the spec says (empty = the table is enforced). Nothing here
/// consumes a sequence number: a refusal must not.
async fn denial_failures(s: &InboxStore, q: &mut Seqs) -> Vec<String> {
    let mut bad = Vec::new();
    let mut expect_refused = |what: &str, r: Result<toron_core::inbox::Appended, InboxError>| {
        if !matches!(r, Err(InboxError::Reject(Reject::Unauthorized { .. }))) {
            bad.push(format!("{what} was not refused: {r:?}"));
        }
    };
    let m = q.msg(CREW);
    expect_refused(
        "crew task",
        s.append_envelope(&task(m, CREW2, CTX, "t-x")).await,
    );
    let m = q.msg(CREW);
    expect_refused(
        "crew verdict",
        s.append_envelope(&verdict(m, CAP, CTX, "t-x")).await,
    );
    let m = q.msg(PEER);
    expect_refused(
        "peer task",
        s.append_envelope(&task(m, CREW, CTX, "t-x")).await,
    );
    let m = q.msg(PEER);
    expect_refused(
        "peer control",
        s.append_envelope(&verdict(m, CAP, CTX, "t-x")).await,
    );
    let m = q.msg(VER);
    expect_refused(
        "verifier task",
        s.append_envelope(&task(m, CREW, CTX, "t-x")).await,
    );
    let m = q.msg(VER);
    expect_refused(
        "verifier notify",
        s.append_envelope(&notify(m, CAP, CTX)).await,
    );
    let m = q.msg(CREW);
    expect_refused(
        "crew role fact",
        s.append_envelope(&role_fact(m, CTX, CREW, Role::Captain, 9))
            .await,
    );
    bad
}

#[tokio::test]
async fn an_ungoverned_context_is_untouched() {
    let s = store("ungoverned").await;
    let mut q = Seqs::new();
    // A "crew" that has no role anywhere tasks and verdicts freely: no role
    // fact, so nothing is checked (today's behaviour).
    let m = q.msg(STRANGER);
    put(&s, &mut q, task(m, CREW, "ctx-legacy", "t-1")).await;
    let m = q.msg(STRANGER);
    put(&s, &mut q, verdict(m, CAP, "ctx-legacy", "t-1")).await;
    assert!(s.roles("ctx-legacy").await.unwrap().is_empty());
}

#[tokio::test]
async fn the_first_role_fact_is_the_operators_captain() {
    let s = store("bootstrap").await;
    let mut q = Seqs::new();
    // A non-operator cannot start a context, whatever it asks for.
    let m = q.msg(STRANGER);
    let r = unauthorized(
        s.append_envelope(&role_fact(m, CTX, STRANGER, Role::Captain, 0))
            .await,
    );
    assert_eq!(
        r,
        Reject::Unauthorized {
            role: None,
            class: Class::Control,
            contract: "role@1".into(),
            expected: vec!["operator".into()],
        }
    );
    // The operator cannot start one with anything but a captain.
    let m = q.msg(OP);
    let r = unauthorized(
        s.append_envelope(&role_fact(m, CTX, CREW, Role::Crew, 0))
            .await,
    );
    assert!(matches!(
        r,
        Reject::Unauthorized { role: Some(Role::Captain), expected, .. }
            if expected == ["role@1:captain"]
    ));
    assert!(
        s.roles(CTX).await.unwrap().is_empty(),
        "nothing was written"
    );
    // The operator's captain governs the context.
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CTX, CAP, Role::Captain, 0)).await;
    let fact = s.role_of(CTX, CAP).await.unwrap().unwrap();
    assert_eq!(
        (fact.role, fact.generation, fact.issuer.as_str()),
        (Role::Captain, 0, OP)
    );
    assert_eq!(
        s.standing(CTX, CAP).await.unwrap(),
        Standing::Holds(Role::Captain)
    );
    assert_eq!(s.standing(CTX, OP).await.unwrap(), Standing::Operator);
    assert_eq!(s.standing(CTX, CREW).await.unwrap(), Standing::Unroled);
}

#[tokio::test]
async fn a_role_fact_from_a_non_operator_without_the_rank_is_refused() {
    let (s, mut q) = swarm("rolerank").await;
    let before = s.roles(CTX).await.unwrap();
    // Crew, peer, verifier, loop_self and a stranger cannot issue any role.
    for who in [CREW, PEER, VER, LOOP, STRANGER] {
        let m = q.msg(who);
        unauthorized(
            s.append_envelope(&role_fact(m, CTX, who, Role::Captain, 5))
                .await,
        );
    }
    // A supervisor cannot mint a captain or a supervisor, nor touch one.
    for (subject, role) in [
        (STRANGER, Role::Captain),
        (STRANGER, Role::Supervisor),
        (CAP, Role::Crew),
        (SUP, Role::Crew),
    ] {
        let m = q.msg(SUP);
        let r = unauthorized(
            s.append_envelope(&role_fact(m, CTX, subject, role, 9))
                .await,
        );
        assert!(
            matches!(&r, Reject::Unauthorized { expected, .. }
                if *expected == ["role@1:crew", "role@1:peer", "role@1:loop_self", "role@1:verifier"]),
            "{r:?}"
        );
    }
    // A captain cannot mint another captain or demote one.
    let m = q.msg(CAP);
    unauthorized(
        s.append_envelope(&role_fact(m, CTX, STRANGER, Role::Captain, 0))
            .await,
    );
    assert_eq!(
        s.roles(CTX).await.unwrap(),
        before,
        "no refusal wrote a role"
    );
    // The operator can do anything: demote the captain.
    let m = q.msg(OP);
    put(&s, &mut q, role_fact(m, CTX, CAP, Role::Peer, 1)).await;
    assert_eq!(s.role_of(CTX, CAP).await.unwrap().unwrap().role, Role::Peer);
}

#[tokio::test]
async fn a_role_fact_is_fenced_by_gen() {
    let (s, mut q) = swarm("rolegen").await;
    // gen 0 is on record for CREW; an equal or lower gen is stale.
    for g in [0u32] {
        let m = q.msg(SUP);
        let r = s
            .append_envelope(&role_fact(m, CTX, CREW, Role::Verifier, g))
            .await;
        assert!(
            matches!(
                r,
                Err(InboxError::Reject(Reject::StaleGen {
                    sent: 0,
                    current: 0
                }))
            ),
            "{r:?}"
        );
    }
    assert_eq!(
        s.role_of(CTX, CREW).await.unwrap().unwrap().role,
        Role::Crew
    );
    // A higher gen replaces it, and the fence moves up.
    let m = q.msg(SUP);
    put(&s, &mut q, role_fact(m, CTX, CREW, Role::Verifier, 3)).await;
    let m = q.msg(SUP);
    let r = s
        .append_envelope(&role_fact(m, CTX, CREW, Role::Crew, 2))
        .await;
    assert!(
        matches!(
            r,
            Err(InboxError::Reject(Reject::StaleGen {
                sent: 2,
                current: 3
            }))
        ),
        "{r:?}"
    );
    let f = s.role_of(CTX, CREW).await.unwrap().unwrap();
    assert_eq!((f.role, f.generation), (Role::Verifier, 3));
}

#[tokio::test]
async fn the_authority_table_is_enforced_at_append() {
    let (s, mut q) = swarm("enforced").await;
    let failures = denial_failures(&s, &mut q).await;
    assert!(failures.is_empty(), "{failures:#?}");

    // The refusals are typed and carry the expected set.
    let m = q.msg(CREW);
    let r = unauthorized(s.append_envelope(&task(m, CREW2, CTX, "t-x")).await);
    assert_eq!(
        r,
        Reject::Unauthorized {
            role: Some(Role::Crew),
            class: Class::Task,
            contract: "task@1".into(),
            expected: vec![
                "notify/*".into(),
                "request/*".into(),
                "reply/*".into(),
                "done".into()
            ],
        }
    );
    let m = q.msg(CREW);
    let r = unauthorized(s.append_envelope(&verdict(m, CAP, CTX, "t-x")).await);
    assert!(
        matches!(&r, Reject::Unauthorized { contract, role: Some(Role::Crew), .. } if contract == "verdict@1")
    );
    let m = q.msg(STRANGER);
    let r = unauthorized(s.append_envelope(&notify(m, CAP, CTX)).await);
    assert!(
        matches!(r, Reject::Unauthorized { role: None, .. }),
        "no role in a governed context"
    );

    // A refusal wrote nothing and consumed no sequence number: the same
    // sender's next message goes in at the seq it would have had.
    let m = q.msg(CREW);
    put(&s, &mut q, notify(m, SUP, CTX)).await;

    // What the table allows goes through.
    let m = q.msg(SUP);
    put(&s, &mut q, task(m, CREW, CTX, "t-1")).await;
    let m = q.msg(PEER);
    put(&s, &mut q, notify(m, CREW, CTX)).await;
    let m = q.msg(VER);
    put(&s, &mut q, verdict(m, CAP, CTX, "t-1")).await;
    let m = q.msg(LOOP);
    put(&s, &mut q, task(m, CREW2, CTX, "t-loop")).await;
    let m = q.msg(OP);
    put(&s, &mut q, task(m, CREW, CTX, "t-op")).await;
    let m = q.msg(CAP);
    put(&s, &mut q, verdict(m, CREW, CTX, "t-1")).await;
}

#[tokio::test]
async fn a_supervisor_cannot_verdict_its_own_task_but_can_verdict_a_captains() {
    let (s, mut q) = swarm("ownverdict").await;
    let m = q.msg(SUP);
    put(&s, &mut q, task(m, CREW, CTX, "t-sup")).await;
    let m = q.msg(CAP);
    put(&s, &mut q, task(m, CREW2, CTX, "t-cap")).await;

    let m = q.msg(SUP);
    let r = unauthorized(s.append_envelope(&verdict(m, CAP, CTX, "t-sup")).await);
    let Reject::Unauthorized {
        role,
        contract,
        expected,
        ..
    } = r
    else {
        unreachable!()
    };
    assert_eq!(
        (role, contract.as_str()),
        (Some(Role::Supervisor), "verdict@1")
    );
    assert!(
        !expected.contains(&"control/verdict".to_string()),
        "{expected:?}"
    );
    assert!(expected.contains(&"task/*".to_string()), "{expected:?}");

    // A task the captain sent is not the supervisor's own.
    let m = q.msg(SUP);
    put(&s, &mut q, verdict(m, CAP, CTX, "t-cap")).await;
}

#[tokio::test]
async fn a_verifier_cannot_done_and_a_crew_can() {
    let (s, mut q) = swarm("verdone").await;
    // The captain hands the verifier a task. It reads and accepts it, then
    // is refused `done`.
    let m = q.msg(CAP);
    put(&s, &mut q, task(m, VER, CTX, "t-ver")).await;
    let b = s.next(VER, None, false).await.unwrap();
    let it = b
        .items
        .iter()
        .find(|i| i.envelope.class == Class::Task)
        .expect("the task is in the verifier's inbox");
    let key = it.msg_key.clone();
    assert_eq!(it.phase, Phase::Delivered);
    s.ack(VER, &key, true, None).await.unwrap();
    let r = s.done(VER, "t-ver", &["x".into()]).await;
    let r = unauthorized(r);
    assert_eq!(
        r,
        Reject::Unauthorized {
            role: Some(Role::Verifier),
            class: Class::Task,
            contract: "done".into(),
            expected: vec!["control/verdict".into()],
        }
    );
    assert_eq!(
        s.delivery(&key, VER).await.unwrap().phase,
        Phase::Acknowledged,
        "nothing moved"
    );

    // The same sequence by a crew finishes the task.
    let m = q.msg(SUP);
    put(&s, &mut q, task(m, CREW, CTX, "t-crew")).await;
    let b = s.next(CREW, None, false).await.unwrap();
    let key = b
        .items
        .iter()
        .find(|i| i.envelope.class == Class::Task)
        .expect("the task is in the crew's inbox")
        .msg_key
        .clone();
    s.ack(CREW, &key, true, None).await.unwrap();
    let (_, st) = s.done(CREW, "t-crew", &["x".into()]).await.unwrap();
    assert_eq!(st.phase, Phase::Acted);
}

#[tokio::test]
async fn a_replay_of_an_authorised_append_stays_idempotent_after_the_role_changes() {
    let (s, mut q) = swarm("replay").await;
    let m = q.msg(SUP);
    let t = task(m, CREW, CTX, "t-1");
    put(&s, &mut q, t.clone()).await;
    // The captain demotes the supervisor; the old append is still a replay.
    let m = q.msg(CAP);
    put(&s, &mut q, role_fact(m, CTX, SUP, Role::Peer, 1)).await;
    let again = s.append_envelope(&t).await.unwrap();
    assert!(again.replay);
    // But the demoted sender can no longer task.
    let m = q.msg(SUP);
    unauthorized(s.append_envelope(&task(m, CREW, CTX, "t-2")).await);
}

#[tokio::test]
async fn the_kernel_entry_is_not_a_bypass() {
    let (s, mut q) = swarm("kernel").await;
    let m = q.msg(CREW);
    let e = task(m, CREW2, CTX, "t-x");
    let r = s.append_json(INBOX_STREAM, &l16_json(&e)).await;
    let err = r.unwrap_err().to_string();
    assert!(err.contains("may not send task@1"), "{err}");
    assert!(err.contains("role crew"), "{err}");
}

#[tokio::test]
async fn authorize_is_a_dry_run() {
    let (s, mut q) = swarm("dry").await;
    let head = s.head(CREW).await.unwrap();
    let m = q.msg(CREW);
    unauthorized(s.authorize(&task(m, CREW2, CTX, "t-x")).await);
    let m = q.msg(SUP);
    s.authorize(&task(m, CREW, CTX, "t-x")).await.unwrap();
    assert_eq!(
        s.head(CREW).await.unwrap(),
        head,
        "a dry run writes nothing"
    );
    // The allowed envelope really is appendable at the seq it was checked at.
    let m = q.msg(SUP);
    put(&s, &mut q, task(m, CREW, CTX, "t-x")).await;
    assert!(s.head(CREW).await.unwrap() > head);
}

#[tokio::test]
async fn an_allow_everything_stub_fails_the_authority_suite() {
    // Negative control. The same suite that passes on the standard table
    // must fail on a table that allows everything.
    static ALLOW_ALL: [Grant; 36] = {
        let mut out = [Grant {
            role: Role::Captain,
            class: Class::Notify,
            contract: None,
        }; 36];
        let mut i = 0;
        while i < 6 {
            let mut j = 0;
            while j < 6 {
                out[i * 6 + j] = Grant {
                    role: Role::ALL[i],
                    class: Class::ALL[j],
                    contract: None,
                };
                j += 1;
            }
            i += 1;
        }
        out
    };
    let (real, mut q) = swarm("stub-real").await;
    assert!(denial_failures(&real, &mut q).await.is_empty());

    let (s, mut q) = swarm("stub").await;
    let stub = s.with_authority(Authority::new(&ALLOW_ALL, &Role::ALL));
    // Under the stub the table's own spec check fails, and so does the
    // append-level suite (the crew task/verdict are not refused any more).
    assert!(
        !Authority::new(&ALLOW_ALL, &Role::ALL)
            .deviations()
            .is_empty()
    );
    let failures = denial_failures(&stub, &mut q).await;
    assert!(
        failures.iter().any(|f| f.starts_with("crew task"))
            && failures.iter().any(|f| f.starts_with("crew verdict")),
        "an allow-all table must be caught by the suite: {failures:#?}"
    );
}

#[test]
fn control_contracts_in_known_all_have_an_authority_row() {
    let control: Vec<&str> = contracts::KNOWN
        .iter()
        .filter(|(_, _, c)| *c == Class::Control)
        .map(|(n, _, _)| *n)
        .collect();
    assert!(
        control.contains(&"role") && control.contains(&"verdict"),
        "{control:?}"
    );
    let a = Authority::standard();
    // approval@1 and lead@1 are captain-only in the table (plus the operator,
    // who is not a role here). Do not grant either wider so this check stays green.
    const CAPTAIN_ONLY: &[&str] = &["approval", "lead"];
    for name in control {
        if CAPTAIN_ONLY.contains(&name) {
            assert!(a.allows(Role::Captain, Class::Control, name), "{name}");
            assert!(
                !a.allows(Role::Supervisor, Class::Control, name),
                "{name} must stay captain-only"
            );
            continue;
        }
        // Someone below the captain must be able to send it; a Control
        // contract only the captain's wildcard reaches is unclassified.
        assert!(
            Role::ALL
                .into_iter()
                .filter(|r| *r != Role::Captain)
                .any(|r| a.allows(r, Class::Control, name)),
            "KNOWN Control contract {name} has no authority row"
        );
    }
}

#[test]
fn role_and_verdict_bodies_are_strict() {
    let m = |n| MsgId {
        producer: CAP.into(),
        epoch: 0,
        seq: n,
    };
    // A role fact must be addressed to its subject.
    let mut e = role_fact(m(0), CTX, CREW, Role::Crew, 0);
    e.to = Recipient::Npub(CREW2.into());
    assert!(matches!(e.validate(), Err(Reject::BadBody { contract, .. }) if contract == "role@1"));
    // A subject that is not an npub: refused as an address, and as a body
    // when only the body names the pin.
    let e = role_fact(m(0), CTX, "alice", Role::Crew, 0);
    assert!(matches!(e.validate(), Err(Reject::BadAddress { .. })));
    let mut e = role_fact(m(0), CTX, CREW, Role::Crew, 0);
    e.body["npub"] = serde_json::json!("alice");
    assert!(matches!(e.validate(), Err(Reject::BadBody { .. })));
    // An unknown role and an unknown field are refused, not guessed.
    let mut e = role_fact(m(0), CTX, CREW, Role::Crew, 0);
    e.body["role"] = serde_json::json!("god");
    assert!(matches!(e.validate(), Err(Reject::BadBody { .. })));
    let mut e = role_fact(m(0), CTX, CREW, Role::Crew, 0);
    e.body["extra"] = serde_json::json!(1);
    assert!(matches!(e.validate(), Err(Reject::BadBody { .. })));
    // The wire name of the fence is `gen`.
    let e = role_fact(m(0), CTX, CREW, Role::LoopSelf, 4);
    assert_eq!(e.body["gen"], 4);
    assert_eq!(e.body["role"], "loop_self");
    assert!(e.body.get("generation").is_none());
    assert!(e.validate().is_ok());
    // A role contract on the wrong class is a class mismatch.
    let mut e = role_fact(m(0), CTX, CREW, Role::Crew, 0);
    e.class = Class::Notify;
    assert!(matches!(
        e.validate(),
        Err(Reject::ContractClassMismatch { .. })
    ));
    // A verdict names its task, and takes only its fields.
    let mut e = verdict(m(0), CAP, CTX, "t");
    e.task = None;
    assert!(matches!(e.validate(), Err(Reject::BadEnvelope { .. })));
    let mut e = verdict(m(0), CAP, CTX, "t");
    e.body["rework"] = serde_json::json!(true);
    assert!(matches!(e.validate(), Err(Reject::BadBody { .. })));
}
