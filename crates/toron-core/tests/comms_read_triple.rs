// governed-by: ADR-0041
//! Read an envelope by its `(producer, epoch, seq)` triple, even after the
//! epoch is fenced (bd-read-by-producer-seq-uozq).
//!
//! A claimed epoch fences every older one, so a sender recovering after a
//! crash cannot re-append an old triple to compare content: the append is
//! refused as `StaleEpoch` before the payload is looked at. The read answers
//! "what does the log say under this triple" without appending anything.

use std::path::PathBuf;

use sqlx::Row;
use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{BY_TRIPLE_SQL, InboxError, InboxStore};
use toron_core::index::schema;
use toron_core::kernel::AppendError;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

async fn store(tag: &str) -> InboxStore {
    let path: PathBuf = std::env::temp_dir().join(format!(
        "toron-triple-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
}

fn task(from: &str, to: &str, epoch: u64, seq: u64, brief: &str) -> Envelope {
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
        context: ContextId("ctx-triple".into()),
        task: Some(TaskId(format!("t-{epoch}-{seq}"))),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": brief, "scope_paths": ["src/**"]}),
    }
}

fn id(from: &str, epoch: u64, seq: u64) -> MsgId {
    MsgId {
        producer: from.into(),
        epoch,
        seq,
    }
}

/// The stored envelope comes back exactly as appended, to its sender and its
/// recipient, with its offset.
#[tokio::test]
async fn the_stored_envelope_comes_back_to_sender_and_recipient() {
    let s = store("roundtrip").await;
    let env = task(A, B, 0, 0, "the brief");
    let ap = s.append_envelope(&env).await.unwrap();
    for viewer in [A, B] {
        let got = s
            .read_triple(&env.msg, viewer)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{viewer:.4} must see it"));
        assert_eq!(got.envelope, env);
        assert_eq!(got.offset, ap.offset);
        assert_eq!(got.msg_key, env.msg.key());
        assert_eq!(got.recipient, B);
        assert!(!got.fenced, "the producer's current epoch is not fenced");
    }
}

/// THE POINT: after a newer epoch is claimed, the old triple is refused for
/// appending but still readable, and says it is fenced.
#[tokio::test]
async fn a_fenced_epoch_stays_readable_while_appends_are_refused() {
    let s = store("fenced").await;
    let old = task(A, B, 0, 0, "written before the crash");
    s.append_envelope(&old).await.unwrap();
    let claim = s.claim_epoch(A, "recovery-run").await.unwrap();
    assert_eq!(claim.epoch, 1);

    // The append path cannot be used to verify content any more: refused as
    // stale before the payload is compared, even for the identical envelope.
    let refused = s.append_envelope(&old).await.unwrap_err();
    assert!(
        matches!(refused, InboxError::Append(AppendError::StaleEpoch { .. })),
        "{refused:?}"
    );

    let got = s.read_triple(&old.msg, A).await.unwrap().expect("readable");
    assert_eq!(got.envelope, old, "the stored content is intact");
    assert!(got.fenced, "and the read says the epoch is fenced");
}

/// A claim that was never appended to fences the old epoch all the same
/// (claim precedes first append), and the read reports it.
#[tokio::test]
async fn fenced_is_true_for_a_claim_with_no_appends_yet() {
    let s = store("claim-only").await;
    s.append_envelope(&task(A, B, 0, 0, "x")).await.unwrap();
    assert!(
        !s.read_triple(&id(A, 0, 0), A)
            .await
            .unwrap()
            .unwrap()
            .fenced
    );
    s.claim_epoch(A, "next").await.unwrap();
    assert!(
        s.read_triple(&id(A, 0, 0), A)
            .await
            .unwrap()
            .unwrap()
            .fenced
    );
}

/// The read verifies content: a different payload under the same triple is
/// visible as different, which is what a recovering sender compares.
#[tokio::test]
async fn the_read_exposes_content_so_a_caller_can_compare() {
    let s = store("compare").await;
    let stored = task(A, B, 0, 0, "what the log holds");
    s.append_envelope(&stored).await.unwrap();
    s.claim_epoch(A, "later").await.unwrap();
    let wanted = task(A, B, 0, 0, "what the caller thinks it sent");
    let got = s.read_triple(&wanted.msg, A).await.unwrap().unwrap();
    assert_ne!(got.envelope.body, wanted.body);
    assert_eq!(got.envelope.body, stored.body);
}

/// A stranger gets exactly the answer an unknown triple gets.
#[tokio::test]
async fn a_stranger_cannot_tell_a_stored_triple_from_an_unknown_one() {
    let s = store("stranger").await;
    let env = task(A, B, 0, 0, "private");
    s.append_envelope(&env).await.unwrap();
    assert!(s.read_triple(&env.msg, C).await.unwrap().is_none());
    assert!(s.read_triple(&id(A, 9, 9), A).await.unwrap().is_none());
    assert!(s.read_triple(&id(A, 9, 9), C).await.unwrap().is_none());
    // Control: the same triple is visible to the people it concerns.
    assert!(s.read_triple(&env.msg, A).await.unwrap().is_some());
    assert!(s.read_triple(&env.msg, B).await.unwrap().is_some());
}

/// The sender's own journal rows are not envelopes in the inbox stream, so
/// the journal producer's triples are not readable here.
#[tokio::test]
async fn the_journal_stream_is_not_readable_as_an_envelope() {
    let s = store("journal").await;
    let r = toron_core::outbox::send_and_recover(s.clone(), A.into(), vec![task(A, B, 0, 0, "j")])
        .await
        .unwrap();
    assert!(!r.sent.is_empty());
    let jp = toron_core::outbox::journal_producer(A);
    assert!(
        s.read_triple(&id(&jp, r.journal_epoch, 0), A)
            .await
            .unwrap()
            .is_none()
    );
}

/// One index probe, no scan.
#[tokio::test]
async fn the_query_plan_is_one_index_probe() {
    let s = store("plan").await;
    let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {BY_TRIPLE_SQL}"))
        .bind(A)
        .bind(0i64)
        .bind(0i64)
        .fetch_all(s.pool())
        .await
        .unwrap();
    let plan: Vec<String> = rows.iter().map(|r| r.get::<String, _>(3)).collect();
    let text = plan.join("\n");
    assert!(
        !plan.iter().any(|l| l.starts_with("SCAN")),
        "scan in plan:\n{text}"
    );
    assert!(text.contains("USING INDEX") || text.contains("USING COVERING INDEX"));
}
