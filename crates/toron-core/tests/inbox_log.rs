// governed-by: ADR-0041
//! Phase 1 exit tests (redesign §11 row 1, §11.1 acceptance), against the
//! real SQLite store on a throwaway database.

use std::sync::Arc;

use toron_core::delivery::{Phase, Reject};
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{INBOX_STREAM, InboxError, InboxStore, JOURNAL_STREAM, ToronLog};
use toron_core::index::schema;
use toron_core::kernel::{self, AppendError, Boot, DurableLog, Failure, Loop, Producer, Program};
use toron_core::outbox::{
    self, MAX_RECONCILE, Outbox, OutboxModel, OutboxMsg, OutboxRun, SendState,
};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

async fn store_at(path: &std::path::Path) -> InboxStore {
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
}

fn temp_db(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("toron-inbox-{tag}-{}.sqlite", uuid::Uuid::new_v4()))
}

async fn store(tag: &str) -> InboxStore {
    store_at(&temp_db(tag)).await
}

fn task(from: &str, to: &str, epoch: u64, seq: u64) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: from.into(),
            epoch,
            seq,
        },
        from: from.into(),
        to: Recipient::Npub(to.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId("ctx-p1".into()),
        task: Some(TaskId(format!("t-{epoch}-{seq}"))),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "port the fold", "brief": "do it", "scope_paths": ["src/**"]}),
    }
}

#[test]
fn urgent_pending_reads_the_steer_priority_field() {
    use toron_core::delivery::Phase;
    use toron_core::inbox::{InboxItem, urgent_pending};

    let mut steer = task("a", "b", 0, 1);
    steer.class = Class::Control;
    steer.contract = ContractId::new("steer", 1);
    steer.body = serde_json::json!({"op": "cancel", "priority": "urgent"});
    let item = |env: Envelope| InboxItem {
        offset: 1,
        msg_key: "k".into(),
        envelope: env,
        phase: Phase::Sent,
        allowed_now: Vec::new(),
    };
    assert!(urgent_pending(&[item(steer.clone())]));
    steer.body = serde_json::json!({"priority": "normal", "text": "priority: urgent"});
    assert!(!urgent_pending(&[item(steer.clone())]));
    let mut note = task("a", "b", 0, 2);
    note.contract = ContractId::new("notify", 1);
    note.body = serde_json::json!({"text": "urgent"});
    assert!(!urgent_pending(&[item(note)]));
}

/// Every upstream DurableLog law holds for ToronLog through the kernel port,
/// called from a dedicated OS thread while the store runs on Tokio.
#[test]
fn toron_log_satisfies_every_durable_log_law() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store("laws"));
    let log = ToronLog::spawn(s, JOURNAL_STREAM, rt.handle());
    let results = std::thread::spawn(move || {
        kernel::durable_log_laws::all(&log)
            .into_iter()
            .map(|(name, r)| (name, r.map_err(|e| e.to_string())))
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    assert!(!results.is_empty());
    for (name, r) in results {
        assert!(r.is_ok(), "law {name} failed on ToronLog: {r:?}");
    }
}

/// An unknown journal outcome is parked, never mapped to a refusal or a
/// blind retry: the fold emits no command and the send stays Unknown.
#[test]
fn an_unknown_journal_outcome_parks_without_a_command() {
    let msg = Outbox::failure(&Failure::Unknown(AppendError::Unavailable(
        "lost acknowledgement".into(),
    )));
    assert!(matches!(msg, OutboxMsg::JournalUnknown(_)));
    let update = Outbox::update(&OutboxModel::default(), &msg);
    assert!(update.commands.is_empty(), "{:?}", update.commands);
    assert!(matches!(
        update.model.sends.values().next(),
        Some(SendState::Unknown(MAX_RECONCILE))
    ));
}

/// The closure laws each close their log, so they run on fresh logs rather
/// than in `all`. This is the 0.4.0 explicit-close contract on ToronLog.
#[test]
fn toron_log_satisfies_every_closure_law() {
    for (name, law) in kernel::durable_log_laws::closing() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let s = rt.block_on(store(name));
        let log = ToronLog::spawn(s, JOURNAL_STREAM, rt.handle());
        std::thread::spawn(move || law(&log))
            .join()
            .unwrap()
            .unwrap_or_else(|e| panic!("closure law {name} failed on ToronLog: {e}"));
    }
}

/// `compact_before` drops records below a known offset and refuses
/// hand-built ones, through the same bridge as append/read.
#[test]
fn compact_before_drops_below_a_known_offset_only() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store("compact"));
    let log = ToronLog::spawn(s, JOURNAL_STREAM, rt.handle());
    std::thread::spawn(move || {
        let o0 = log
            .append_json(&kernel::durable_log_laws::envelope("w", 0, 0, "{}"))
            .unwrap();
        let o1 = log
            .append_json(&kernel::durable_log_laws::envelope("w", 0, 1, "{}"))
            .unwrap();
        log.append_json(&kernel::durable_log_laws::envelope("w", 0, 2, "{}"))
            .unwrap();
        assert!(matches!(
            log.compact_before(&kernel::Offset::Start),
            Err(AppendError::UnknownOffset)
        ));
        assert!(matches!(
            log.compact_before(&kernel::Offset::At("00000000000000000999".into())),
            Err(AppendError::UnknownOffset)
        ));
        assert_eq!(log.compact_before(&o1).unwrap(), 1);
        let rest = log.read_from(&kernel::Offset::Start).unwrap();
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].0, o1);
        assert!(matches!(
            log.read_from(&o0),
            Err(AppendError::UnknownOffset)
        ));
        assert_eq!(log.compact_before(&o1).unwrap(), 0);
    })
    .join()
    .unwrap();
}

#[tokio::test]
async fn same_msg_id_twice_is_one_row_with_the_same_offset() {
    let s = store("dup").await;
    let e = task(A, B, 0, 0);
    let first = s.append_envelope(&e).await.unwrap();
    let again = s.append_envelope(&e).await.unwrap();
    assert!(!first.replay && again.replay);
    assert_eq!(first.offset, again.offset);
    assert_eq!(s.read_stream(INBOX_STREAM, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn stale_epoch_append_is_rejected_and_writes_nothing() {
    let s = store("stale").await;
    s.append_envelope(&task(A, B, 0, 0)).await.unwrap();
    s.append_envelope(&task(A, B, 1, 0)).await.unwrap();
    let zombie = s.append_envelope(&task(A, B, 0, 1)).await;
    assert!(
        matches!(
            zombie,
            Err(InboxError::Append(AppendError::StaleEpoch {
                sent: 0,
                current: 1,
                ..
            }))
        ),
        "{zombie:?}"
    );
    let replay = s.append_envelope(&task(A, B, 0, 0)).await;
    assert!(
        matches!(
            replay,
            Err(InboxError::Append(AppendError::StaleEpoch { .. }))
        ),
        "{replay:?}"
    );
    assert_eq!(s.read_stream(INBOX_STREAM, None).await.unwrap().len(), 2);
}

#[tokio::test]
async fn seq_gap_and_bad_envelopes_are_refused() {
    let s = store("gap").await;
    assert!(matches!(
        s.append_envelope(&task(A, B, 0, 3)).await,
        Err(InboxError::Append(AppendError::SeqGap {
            got: 3,
            last: None
        }))
    ));
    let mut by_name = task(A, B, 0, 0);
    by_name.to = Recipient::Npub("BirchFixture".into());
    assert!(matches!(
        s.append_envelope(&by_name).await,
        Err(InboxError::Reject(Reject::BadAddress { .. }))
    ));
    let mut unknown = task(A, B, 0, 0);
    unknown.contract = ContractId::new("task", 9);
    assert!(matches!(
        s.append_envelope(&unknown).await,
        Err(InboxError::Reject(Reject::UnsupportedContract {
            major: 9,
            ..
        }))
    ));
    assert!(s.read_stream(INBOX_STREAM, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn another_npub_cannot_advance_the_recipients_cursor() {
    let s = store("cursor").await;
    s.append_envelope(&task(A, B, 0, 0)).await.unwrap();
    let refused = s.next(A, Some(B), false).await;
    assert!(
        matches!(
            refused,
            Err(InboxError::Reject(Reject::NotRecipient { .. }))
        ),
        "{refused:?}"
    );
    assert_eq!(
        s.cursor(B, B).await.unwrap(),
        0,
        "the refused read moved nothing"
    );
    let own = s.next(A, None, false).await.unwrap();
    assert!(own.items.is_empty());
    assert_eq!(s.cursor(B, B).await.unwrap(), 0);
    let mine = s.next(B, None, false).await.unwrap();
    assert_eq!(mine.items.len(), 1);
    assert_eq!(s.cursor(B, B).await.unwrap(), mine.items[0].offset);
}

#[tokio::test]
async fn next_commits_cursor_and_delivered_together_and_peek_commits_neither() {
    let s = store("next").await;
    let e = task(A, B, 0, 0);
    s.append_envelope(&e).await.unwrap();
    let peek = s.next(B, None, true).await.unwrap();
    assert_eq!(peek.items[0].phase, Phase::Sent);
    assert_eq!(s.cursor(B, B).await.unwrap(), 0);
    let read = s.next(B, None, false).await.unwrap();
    assert_eq!(read.items[0].phase, Phase::Delivered);
    assert_eq!(read.items[0].allowed_now.len(), 2, "ack accept | reject");
    assert_eq!(
        s.delivery(&e.msg.key(), B).await.unwrap().phase,
        Phase::Delivered
    );
    assert!(
        s.next(B, None, false).await.unwrap().items.is_empty(),
        "nothing past the cursor"
    );
}

#[tokio::test]
async fn ack_then_done_and_only_the_holder_may_finish() {
    let s = store("ack").await;
    let e = task(A, B, 0, 0);
    s.append_envelope(&e).await.unwrap();
    s.next(B, None, false).await.unwrap();
    let not_mine = s.ack(A, &e.msg.key(), true, None).await;
    assert!(
        matches!(not_mine, Err(InboxError::NoSuchDelivery { .. })),
        "{not_mine:?}"
    );
    assert_eq!(
        s.ack(B, &e.msg.key(), true, None).await.unwrap().phase,
        Phase::Acknowledged
    );
    let stranger = s.done(A, "t-0-0", &["x".into()]).await;
    assert!(stranger.is_err(), "{stranger:?}");
    let (_, done) = s.done(B, "t-0-0", &["commit:abc".into()]).await.unwrap();
    assert_eq!(done.phase, Phase::Acted);
}

fn boot(journal: ToronLog, inbox: ToronLog, epoch: u64) -> Loop<Outbox, OutboxRun> {
    Loop::<Outbox, OutboxRun>::boot(
        &Boot::default(),
        outbox::runtime(inbox),
        Arc::new(journal),
        Producer {
            producer_id: outbox::journal_producer(A),
            epoch,
            seq: 0,
        },
    )
    .expect("boot journals init on a fresh log")
}

/// F2 regression guard: the outbox loop runs inside a Tokio-hosted process
/// on its own OS thread, with no nested-runtime panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loop_hosted_under_tokio_has_no_nested_runtime_panic() {
    let s = store("hosted").await;
    let handle = tokio::runtime::Handle::current();
    let journal = ToronLog::spawn(s.clone(), JOURNAL_STREAM, &handle);
    let inbox = ToronLog::spawn(s.clone(), INBOX_STREAM, &handle);
    let e = task(A, B, 0, 0);
    let key = e.msg.key();
    let state = tokio::task::spawn_blocking(move || {
        std::thread::spawn(move || {
            let mut app = boot(journal, inbox, 0);
            app.send(OutboxMsg::Send(e)).expect("loop send");
            app.model().sends.get(&key).cloned()
        })
        .join()
        .expect("the loop thread did not panic")
    })
    .await
    .unwrap();
    assert!(matches!(state, Some(SendState::Settled(_))), "{state:?}");
    assert_eq!(s.read_stream(INBOX_STREAM, None).await.unwrap().len(), 1);
    assert_eq!(
        journal_kinds(&s).await,
        ["msg", "intent", "msg"],
        "one send journals its message, its append intent, and the reply"
    );
}

/// The journal record kinds on JOURNAL_STREAM, oldest first. The framework
/// journals inbound messages (`msg`) as well as durable intents (`intent`),
/// so one send leaves `["msg", "intent", "msg"]`, not one row.
async fn journal_kinds(s: &InboxStore) -> Vec<String> {
    s.read_stream(JOURNAL_STREAM, None)
        .await
        .unwrap()
        .iter()
        .map(|(_, p)| {
            serde_json::from_str::<serde_json::Value>(p)
                .ok()
                .and_then(|v| v.get("kind").and_then(|k| k.as_str()).map(str::to_string))
                .unwrap_or_else(|| "?".into())
        })
        .collect()
}

/// A direct call from a Tokio worker answers instead of refusing: the bridge
/// is a plain bounded wait, so no runtime is nested. The loop itself never
/// needs this path; it calls from a blocking fiber (elm-effect 0.4.0).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_on_a_tokio_worker_answers_without_nesting_a_runtime() {
    let s = store("worker").await;
    let log = ToronLog::spawn(s, JOURNAL_STREAM, &tokio::runtime::Handle::current());
    let r = log.append_json(&kernel::durable_log_laws::envelope("w", 0, 0, "{}"));
    assert!(r.is_ok(), "{r:?}");
}

/// The child half of the crash test: journal the intent, then abort at the
/// fault point named in TORON_FAULT. Does nothing unless the parent set it up.
#[test]
fn crash_child() {
    let Ok(db) = std::env::var("TORON_CRASH_CHILD_DB") else {
        return;
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store_at(std::path::Path::new(&db)));
    let journal = ToronLog::spawn(s.clone(), JOURNAL_STREAM, rt.handle());
    let inbox = ToronLog::spawn(s, INBOX_STREAM, rt.handle());
    std::thread::spawn(move || {
        let mut app = boot(journal, inbox, 0);
        app.send(OutboxMsg::Send(task(A, B, 0, 0)))
            .expect("loop send");
    })
    .join()
    .unwrap();
    panic!("TORON_FAULT should have aborted the process before this line");
}

fn crash_at(point: &str) -> (std::path::PathBuf, std::process::ExitStatus) {
    let db = temp_db(&format!("crash-{point}"));
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture", "--test-threads=1"])
        .env("TORON_CRASH_CHILD_DB", &db)
        .env("TORON_FAULT", point)
        .status()
        .unwrap();
    (db, status)
}

/// Reconcile `db` as a restarted sender and count the inbox rows. Every
/// restart takes a NEW journal epoch: the loop journals inbound messages,
/// so replaying the same epoch would collide with its own earlier records.
fn recover_and_count(db: &std::path::Path, epoch: u64) -> (usize, Vec<Option<SendState>>) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store_at(db));
    let journal = ToronLog::spawn(s.clone(), JOURNAL_STREAM, rt.handle());
    let inbox = ToronLog::spawn(s.clone(), INBOX_STREAM, rt.handle());
    let states = std::thread::spawn(move || {
        let intents = outbox::recover(&journal).unwrap();
        let mut app = boot(journal, inbox, epoch);
        let keys: Vec<String> = intents.iter().map(|e| e.msg.key()).collect();
        for e in intents {
            app.send(OutboxMsg::Recover(e)).expect("loop send");
        }
        keys.iter()
            .map(|k| app.model().sends.get(k).cloned())
            .collect::<Vec<_>>()
    })
    .join()
    .unwrap();
    let rows = rt
        .block_on(s.read_stream(INBOX_STREAM, None))
        .unwrap()
        .len();
    (rows, states)
}

/// Kill the sender between journal and append, reconcile: exactly one row.
#[test]
fn crash_between_intent_and_append_reconciles_to_exactly_one_row() {
    let (db, status) = crash_at("after_intent");
    assert!(
        !status.success(),
        "the child must die at the fault point: {status:?}"
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store_at(&db));
    assert_eq!(
        rt.block_on(journal_kinds(&s)),
        ["msg", "intent"],
        "the send message and its append intent journaled, nothing else"
    );
    assert_eq!(
        rt.block_on(s.read_stream(INBOX_STREAM, None))
            .unwrap()
            .len(),
        0,
        "append never ran"
    );
    drop(rt);
    let (rows, states) = recover_and_count(&db, 1);
    assert_eq!(rows, 1);
    assert!(
        matches!(states.as_slice(), [Some(SendState::Settled(_))]),
        "{states:?}"
    );
    let (rows_again, _) = recover_and_count(&db, 2);
    assert_eq!(rows_again, 1, "a second recovery appends nothing");
}

/// Kill the sender after the append but before it settled: reconcile finds
/// the row and appends nothing.
#[test]
fn crash_after_append_reconciles_without_a_second_row() {
    let (db, status) = crash_at("after_append");
    assert!(!status.success(), "{status:?}");
    let (rows, states) = recover_and_count(&db, 1);
    assert_eq!(rows, 1);
    assert!(
        matches!(states.as_slice(), [Some(SendState::Settled(_))]),
        "{states:?}"
    );
}

/// Negative control for the reconcile loop: with the store gone, a send
/// parks as `Unknown` after a bounded number of reconcile attempts. It
/// neither spins (stack overflow) nor retries blind.
#[test]
fn unreachable_inbox_parks_unknown_and_never_spins() {
    let dead = tokio::runtime::Runtime::new().unwrap();
    let s = dead.block_on(store("dead"));
    let inbox = ToronLog::spawn(s.clone(), INBOX_STREAM, dead.handle());
    let live = tokio::runtime::Runtime::new().unwrap();
    let journal = ToronLog::spawn(s.clone(), JOURNAL_STREAM, live.handle());
    drop(dead);
    let e = task(A, B, 0, 0);
    let key = e.msg.key();
    let state = std::thread::spawn(move || {
        let mut app = boot(journal, inbox, 0);
        app.send(OutboxMsg::Send(e)).expect("loop send");
        app.model().sends.get(&key).cloned()
    })
    .join()
    .expect("no stack overflow, no panic");
    assert_eq!(state, Some(SendState::Unknown(outbox::MAX_RECONCILE + 1)));
    assert_eq!(
        live.block_on(s.read_stream(INBOX_STREAM, None))
            .unwrap()
            .len(),
        0
    );
}
