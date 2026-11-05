// governed-by: ADR-0041
//! A producer triple names ONE message (bd-envelope-conflict-3d7u).
//!
//! `append_envelope` used to answer "replay" for any envelope whose
//! (producer, epoch, seq) was already in the log, without looking at the
//! payload. A producer that reused a triple for new content (a restarted
//! sender that lost its counter, a bug, a forged resend) then had that content
//! silently dropped while the caller was told it succeeded.
//!
//! Contract: the SAME bytes under an existing triple are an idempotent replay
//! (same offset, nothing written); DIFFERENT content under it is a hard
//! `PayloadConflict` that writes nothing and leaves the first message intact.
//! Fencing still comes first: a stale epoch is `StaleEpoch`, never a conflict.

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{INBOX_STREAM, InboxError, InboxStore, JOURNAL_STREAM, ToronLog};
use toron_core::index::schema;
use toron_core::kernel::{self, AppendError, DurableLog};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

async fn store(tag: &str) -> InboxStore {
    let path = std::env::temp_dir().join(format!(
        "toron-conflict-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
}

fn task(to: &str, epoch: u64, seq: u64, title: &str) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: A.into(),
            epoch,
            seq,
        },
        from: A.into(),
        to: Recipient::Npub(to.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId("ctx-conflict".into()),
        task: Some(TaskId(format!("t-{epoch}-{seq}"))),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": title, "brief": "do it", "scope_paths": ["src/**"]}),
    }
}

fn is_conflict(r: &Result<toron_core::inbox::Appended, InboxError>) -> bool {
    matches!(r, Err(InboxError::PayloadConflict { .. }))
}

async fn rows(s: &InboxStore) -> Vec<(u64, String)> {
    s.read_stream(INBOX_STREAM, None).await.unwrap()
}

/// REPRO: before the fix the second append came back `Ok(replay: true)` and
/// the new content vanished.
#[tokio::test]
async fn different_content_under_the_same_triple_is_a_hard_conflict() {
    let s = store("body").await;
    let first = s.append_envelope(&task(B, 0, 0, "original")).await.unwrap();
    let second = s.append_envelope(&task(B, 0, 0, "rewritten")).await;
    assert!(is_conflict(&second), "{second:?}");
    let log = rows(&s).await;
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].0, first.offset);
    assert!(log[0].1.contains("original") && !log[0].1.contains("rewritten"));
}

/// REPRO: the conflict can be in a routing field, not just the body. A resend
/// of the same triple to a different recipient must not be absorbed.
#[tokio::test]
async fn a_changed_recipient_under_the_same_triple_is_a_conflict() {
    let s = store("recipient").await;
    s.append_envelope(&task(B, 0, 0, "same")).await.unwrap();
    let to_c = s.append_envelope(&task(C, 0, 0, "same")).await;
    assert!(is_conflict(&to_c), "{to_c:?}");
    assert_eq!(rows(&s).await.len(), 1);
}

/// A refused conflict leaves the store usable: the producer's next real
/// message appends normally and keeps its place.
#[tokio::test]
async fn a_conflict_does_not_poison_the_producer() {
    let s = store("poison").await;
    s.append_envelope(&task(B, 0, 0, "m0")).await.unwrap();
    assert!(is_conflict(
        &s.append_envelope(&task(B, 0, 0, "m0-but-different")).await
    ));
    let next = s.append_envelope(&task(B, 0, 1, "m1")).await.unwrap();
    assert!(!next.replay);
    assert_eq!(rows(&s).await.len(), 2);
}

/// Through the kernel-facing entry (what a sender loop calls) the same triple
/// with different content is refused too, with the conflict named. The
/// journal stream is the raw-payload surface; the inbox stream only takes real
/// envelopes, whose conflict the typed tests above cover.
#[tokio::test]
async fn the_kernel_entry_names_the_conflict() {
    let s = store("kernel").await;
    let first = kernel::durable_log_laws::envelope("w-conf", 0, 0, "{\"n\":1}");
    let other = kernel::durable_log_laws::envelope("w-conf", 0, 0, "{\"n\":2}");
    s.append_json(JOURNAL_STREAM, &first).await.unwrap();
    match s.append_json(JOURNAL_STREAM, &other).await {
        Err(AppendError::NotAnEnvelope(why)) => {
            assert!(why.contains("payload conflict"), "{why}")
        }
        other => panic!("expected a named conflict, got {other:?}"),
    }
}

/// The bridge a `Loop` uses reports the conflict as an error, not an offset.
#[test]
fn toron_log_refuses_a_conflict_and_still_replays_identical_bytes() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let s = rt.block_on(store("bridge"));
    let log = ToronLog::spawn(s, JOURNAL_STREAM, rt.handle());
    let (first, replay, conflict) = std::thread::spawn(move || {
        let e1 = kernel::durable_log_laws::envelope("w-bridge", 0, 0, "{\"n\":1}");
        let e2 = kernel::durable_log_laws::envelope("w-bridge", 0, 0, "{\"n\":2}");
        (
            log.append_json(&e1),
            log.append_json(&e1),
            log.append_json(&e2),
        )
    })
    .join()
    .unwrap();
    let first = first.expect("first append");
    assert_eq!(replay.expect("identical replay"), first, "same offset");
    assert!(
        matches!(&conflict, Err(AppendError::NotAnEnvelope(w)) if w.contains("payload conflict")),
        "{conflict:?}"
    );
}

/// CONTROL: the same envelope again is still an idempotent replay.
#[tokio::test]
async fn control_identical_content_is_still_an_idempotent_replay() {
    let s = store("replay").await;
    let e = task(B, 0, 0, "same");
    let first = s.append_envelope(&e).await.unwrap();
    let again = s.append_envelope(&e).await.unwrap();
    assert!(!first.replay && again.replay);
    assert_eq!(first.offset, again.offset);
    assert_eq!(rows(&s).await.len(), 1);
}

/// CONTROL: the same bytes with JSON keys in another order are the same
/// content, not a conflict (the comparison is structural, not textual).
#[tokio::test]
async fn control_key_order_is_not_a_conflict() {
    let s = store("order").await;
    let a = r#"{"v":1,"producer":{"producer_id":"w-ord","epoch":0,"seq":0},"payload":{"x":1,"y":[1,2],"z":{"p":1,"q":2}}}"#;
    let b = r#"{"payload":{"z":{"q":2,"p":1},"y":[1,2],"x":1},"producer":{"seq":0,"epoch":0,"producer_id":"w-ord"},"v":1}"#;
    let first = s.append_json(JOURNAL_STREAM, a).await.unwrap();
    let again = s.append_json(JOURNAL_STREAM, b).await.unwrap();
    assert!(again.replay);
    assert_eq!(first.offset, again.offset);
}

/// CONTROL: identical content under a DIFFERENT seq is a new message.
#[tokio::test]
async fn control_same_content_under_a_new_seq_is_a_new_message() {
    let s = store("newseq").await;
    s.append_envelope(&task(B, 0, 0, "same title"))
        .await
        .unwrap();
    let second = s
        .append_envelope(&task(B, 0, 1, "same title"))
        .await
        .unwrap();
    assert!(!second.replay);
    assert_eq!(rows(&s).await.len(), 2);
}

/// CONTROL: fencing beats the conflict check. A zombie epoch is `StaleEpoch`
/// even when it also carries different content for an old triple.
#[tokio::test]
async fn control_fencing_still_comes_before_the_conflict_check() {
    let s = store("fence").await;
    s.append_envelope(&task(B, 0, 0, "old")).await.unwrap();
    s.append_envelope(&task(B, 1, 0, "new epoch"))
        .await
        .unwrap();
    let zombie = s.append_envelope(&task(B, 0, 0, "zombie rewrite")).await;
    assert!(
        matches!(
            zombie,
            Err(InboxError::Append(AppendError::StaleEpoch { .. }))
        ),
        "{zombie:?}"
    );
}
