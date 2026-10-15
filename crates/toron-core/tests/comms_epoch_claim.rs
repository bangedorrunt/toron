// governed-by: ADR-0041
//! The atomic epoch claim (bd-epoch-claim-z5m5).
//!
//! flywheel used to pick a producer epoch from a local file lock. Two
//! processes (or one host that lost its lock file) could pick the same epoch
//! and then share a (producer, epoch, seq) triple, so one sender's message
//! replayed as the other's. `InboxStore::claim_epoch` moves the choice into
//! the log: one `BEGIN IMMEDIATE` transaction per claim, strictly above
//! everything the log has seen for that producer, and an appended-to fence
//! that bites older incarnations the moment the claim lands.

use std::collections::BTreeSet;
use std::path::PathBuf;

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::{InboxError, InboxStore};
use toron_core::index::schema;
use toron_core::kernel::AppendError;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

fn db_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("toron-claim-{tag}-{}.sqlite", uuid::Uuid::new_v4()))
}

async fn open(path: &std::path::Path) -> InboxStore {
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
}

async fn store(tag: &str) -> InboxStore {
    open(&db_path(tag)).await
}

fn task(from: &str, epoch: u64, seq: u64) -> Envelope {
    Envelope {
        v: 1,
        msg: MsgId {
            producer: from.into(),
            epoch,
            seq,
        },
        from: from.into(),
        to: Recipient::Npub(B.into()),
        class: Class::Task,
        contract: ContractId::new("task", 1),
        context: ContextId("ctx-claim".into()),
        task: Some(TaskId(format!("t-{from:.4}-{epoch}-{seq}"))),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": "b", "scope_paths": ["src/**"]}),
    }
}

/// A new producer starts at epoch 0, then every claim is strictly higher.
#[tokio::test]
async fn claims_start_at_zero_and_only_go_up() {
    let s = store("up").await;
    let first = s.claim_epoch(A, "run-1").await.unwrap();
    assert_eq!((first.epoch, first.previous), (0, None));
    let second = s.claim_epoch(A, "run-2").await.unwrap();
    assert_eq!((second.epoch, second.previous), (1, Some(0)));
    assert_eq!(s.claim_epoch(A, "run-3").await.unwrap().epoch, 2);
}

/// The claim is durable: a reopened store continues from it.
#[tokio::test]
async fn a_claim_survives_a_reopen() {
    let path = db_path("reopen");
    let s = open(&path).await;
    s.claim_epoch(A, "run-1").await.unwrap();
    s.claim_epoch(A, "run-2").await.unwrap();
    drop(s);
    let s = open(&path).await;
    assert_eq!(s.claim_epoch(A, "run-3").await.unwrap().epoch, 2);
}

/// The claim sits above epochs that were appended without it (an older
/// sender that chose its own epoch).
#[tokio::test]
async fn a_claim_is_above_every_appended_epoch() {
    let s = store("above").await;
    s.append_envelope(&task(A, 3, 0)).await.unwrap();
    let claim = s.claim_epoch(A, "run").await.unwrap();
    assert_eq!((claim.epoch, claim.previous), (4, Some(3)));
}

/// REPRO: two pools on one file (two processes) claiming at once never get
/// the same epoch. Before the claim API a caller chose its own epoch, so
/// racing callers could, and flywheel's file lock did not span hosts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_claimants_never_share_an_epoch() {
    let path = db_path("race");
    let (x, y) = (open(&path).await, open(&path).await);
    let mut tasks = Vec::new();
    for i in 0..40usize {
        let s = if i % 2 == 0 { x.clone() } else { y.clone() };
        tasks.push(tokio::spawn(async move {
            s.claim_epoch(A, &format!("racer-{i}")).await.unwrap().epoch
        }));
    }
    let mut got = Vec::new();
    for t in tasks {
        got.push(t.await.unwrap());
    }
    let unique: BTreeSet<u64> = got.iter().copied().collect();
    assert_eq!(
        unique.len(),
        got.len(),
        "duplicate epochs handed out: {got:?}"
    );
    assert_eq!(
        unique,
        (0..40u64).collect::<BTreeSet<_>>(),
        "epochs are contiguous when every claim is made"
    );
}

/// REPRO: a claim fences every older incarnation immediately, before the
/// claimant has appended anything.
#[tokio::test]
async fn a_claim_fences_older_epochs_at_once() {
    let s = store("fence").await;
    s.append_envelope(&task(A, 0, 0)).await.unwrap();
    let claim = s.claim_epoch(A, "run").await.unwrap();
    assert_eq!(claim.epoch, 1);
    let zombie = s.append_envelope(&task(A, 0, 1)).await;
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
    // The claimant starts at seq 0; a gap is still refused.
    let gap = s.append_envelope(&task(A, 1, 1)).await;
    assert!(
        matches!(
            gap,
            Err(InboxError::Append(AppendError::SeqGap {
                got: 1,
                last: None
            }))
        ),
        "{gap:?}"
    );
    assert!(!s.append_envelope(&task(A, 1, 0)).await.unwrap().replay);
    assert_eq!(s.producer_epoch(A).await.unwrap(), Some(1));
}

/// `producer_epoch` sees a claim that has not appended yet, so a sender that
/// derives its own epoch from it can never land under a fenced one.
#[tokio::test]
async fn producer_epoch_includes_an_unappended_claim() {
    let s = store("pe").await;
    assert_eq!(s.producer_epoch(A).await.unwrap(), None);
    s.claim_epoch(A, "run").await.unwrap();
    assert_eq!(s.producer_epoch(A).await.unwrap(), Some(0));
}

// --- negative controls ------------------------------------------------------

/// CONTROL: producers are independent; each starts at 0.
#[tokio::test]
async fn control_producers_claim_independently() {
    let s = store("indep").await;
    s.claim_epoch(A, "r").await.unwrap();
    s.claim_epoch(A, "r").await.unwrap();
    assert_eq!(s.claim_epoch(C, "r").await.unwrap().epoch, 0);
    assert_eq!(s.claim_epoch(A, "r").await.unwrap().epoch, 2);
}

/// CONTROL: with no claim at all, appends behave exactly as before (the
/// fence is the producer state's epoch alone).
#[tokio::test]
async fn control_appends_without_a_claim_are_unchanged() {
    let s = store("noclaim").await;
    s.append_envelope(&task(A, 0, 0)).await.unwrap();
    s.append_envelope(&task(A, 1, 0)).await.unwrap();
    assert!(matches!(
        s.append_envelope(&task(A, 0, 1)).await,
        Err(InboxError::Append(AppendError::StaleEpoch {
            current: 1,
            ..
        }))
    ));
}

/// CONTROL: a claim for one producer never fences another producer.
#[tokio::test]
async fn control_a_claim_does_not_fence_other_producers() {
    let s = store("other").await;
    s.claim_epoch(A, "r").await.unwrap();
    s.claim_epoch(A, "r").await.unwrap();
    assert!(!s.append_envelope(&task(C, 0, 0)).await.unwrap().replay);
}

/// CONTROL: appending at exactly the claimed epoch is accepted, and an
/// identical re-send is still an idempotent replay.
#[tokio::test]
async fn control_the_claimed_epoch_appends_and_replays() {
    let s = store("exact").await;
    let claim = s.claim_epoch(A, "r").await.unwrap();
    let first = s.append_envelope(&task(A, claim.epoch, 0)).await.unwrap();
    let again = s.append_envelope(&task(A, claim.epoch, 0)).await.unwrap();
    assert!(!first.replay && again.replay);
    assert_eq!(first.offset, again.offset);
}
