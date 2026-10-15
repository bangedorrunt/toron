// governed-by: ADR-0040, ADR-0041
//! The sender's journal epoch is claimed from the log (bd-journal-claim-epoch-4blu).
//!
//! `send_and_recover` used to take its journal epoch as `producer_epoch + 1`:
//! a read-then-add, not recorded anywhere until the run's first journal
//! append. Two runs of one sender that read before either wrote got the same
//! epoch (and the same journal triples), and a run that journaled nothing
//! left no trace, so the next run reused its epoch. The epoch now comes from
//! `InboxStore::claim_epoch`, one `BEGIN IMMEDIATE` transaction that records
//! it before the run starts.

use std::collections::BTreeSet;
use std::path::PathBuf;

use toron_core::envelope::{Class, ContextId, ContractId, Envelope, MsgId, Recipient, TaskId};
use toron_core::inbox::InboxStore;
use toron_core::index::schema;
use toron_core::outbox::{self, SendState};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

async fn store(tag: &str) -> InboxStore {
    let path: PathBuf = std::env::temp_dir().join(format!(
        "toron-jepoch-{tag}-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    InboxStore::new(pool)
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
        context: ContextId("ctx-jepoch".into()),
        task: Some(TaskId(format!("t-{epoch}-{seq}"))),
        generation: 1,
        causation: None,
        deadline_ts_us: None,
        lease_ms: None,
        body: serde_json::json!({"title": "t", "brief": "b", "scope_paths": ["src/**"]}),
    }
}

/// A run that journals nothing still claims its epoch, so the next run is
/// strictly above it (the old read-then-add gave both epoch 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_runs_never_reuse_a_journal_epoch_even_when_empty() {
    let s = store("seq-empty").await;
    let mut epochs = Vec::new();
    for _ in 0..3 {
        let r = outbox::send_and_recover(s.clone(), A.into(), Vec::new())
            .await
            .expect("run");
        epochs.push(r.journal_epoch);
    }
    assert_eq!(epochs, vec![0, 1, 2], "each run claims the next epoch");
}

/// Runs of one sender racing each other take distinct journal epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_runs_take_distinct_journal_epochs() {
    let s = store("conc").await;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let s = s.clone();
        tasks.push(tokio::spawn(async move {
            outbox::send_and_recover(s, A.into(), Vec::new())
                .await
                .expect("run")
                .journal_epoch
        }));
    }
    let mut epochs = Vec::new();
    for t in tasks {
        epochs.push(t.await.unwrap());
    }
    let distinct: BTreeSet<u64> = epochs.iter().copied().collect();
    assert_eq!(distinct.len(), 8, "eight runs, eight epochs: {epochs:?}");
}

/// The epoch a run reports is the epoch the log holds for the journal
/// producer, whether or not the run journaled anything, and the next run
/// starts above a run that did.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_reported_epoch_is_the_one_the_log_holds() {
    let s = store("held").await;
    let jp = outbox::journal_producer(A);
    assert_eq!(s.producer_epoch(&jp).await.unwrap(), None);

    let empty = outbox::send_and_recover(s.clone(), A.into(), Vec::new())
        .await
        .unwrap();
    assert_eq!(
        s.producer_epoch(&jp).await.unwrap(),
        Some(empty.journal_epoch)
    );

    let sent = outbox::send_and_recover(s.clone(), A.into(), vec![task(A, 0, 0)])
        .await
        .unwrap();
    assert!(sent.journal_epoch > empty.journal_epoch);
    assert!(
        matches!(sent.sent.values().next(), Some(SendState::Settled(_))),
        "the send itself still lands: {:?}",
        sent.sent
    );
    assert_eq!(
        s.producer_epoch(&jp).await.unwrap(),
        Some(sent.journal_epoch)
    );

    // The journal's claims never touch the sender's own inbox epoch.
    assert_eq!(s.producer_epoch(A).await.unwrap(), Some(0));

    let after = outbox::send_and_recover(s.clone(), A.into(), Vec::new())
        .await
        .unwrap();
    assert!(after.journal_epoch > sent.journal_epoch);
}

/// Different senders do not share an epoch space.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn senders_have_separate_journal_epochs() {
    let s = store("senders").await;
    let a1 = outbox::send_and_recover(s.clone(), A.into(), Vec::new())
        .await
        .unwrap();
    let a2 = outbox::send_and_recover(s.clone(), A.into(), Vec::new())
        .await
        .unwrap();
    let b1 = outbox::send_and_recover(s.clone(), B.into(), Vec::new())
        .await
        .unwrap();
    assert_eq!((a1.journal_epoch, a2.journal_epoch), (0, 1));
    assert_eq!(b1.journal_epoch, 0, "B's first run is B's epoch 0");
}
