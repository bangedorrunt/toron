// governed-by: ADR-0040, ADR-0041
//! `ToronJournal: kernel::JournalStore` (stack-comms Phase 2).
//!
//! 1. The upstream `journal_store` contract tests, run against BOTH the
//!    in-memory reference and the SQLite journal through one law suite.
//! 2. A differential run: the same random operation sequence on both
//!    stores yields the same row (state, owner, lease, generation,
//!    attempts, disposition) after every step, and the same "changed or
//!    not" answer.
//! 3. What only a database can show: rows survive a reopen, scopes are
//!    isolated, two processes' handles racing claims never mint the same
//!    generation, and the F2 guard (never block a Tokio worker).

use toron_core::index::schema;
use toron_core::journal::{JournalDb, JournalError, ToronJournal};
use toron_core::kernel::{
    EffectState, InMemoryStore, JournalDisposition, JournalRead, JournalRow, JournalStore,
};

struct Db {
    rt: tokio::runtime::Runtime,
    pool: sqlx::SqlitePool,
    path: std::path::PathBuf,
    /// Leave the file on drop (a second view of it, or a reopen test).
    keep: bool,
}

impl Db {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "toron-journal-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        Self::at(path)
    }

    fn at(path: std::path::PathBuf) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let pool = rt.block_on(async {
            let pool = schema::connect(path.to_str().unwrap())
                .await
                .expect("connect");
            schema::migrate(&pool).await.expect("migrate");
            pool
        });
        Self {
            rt,
            pool,
            path,
            keep: false,
        }
    }

    fn journal(&self, scope: &str, retention: usize) -> ToronJournal {
        ToronJournal::spawn(
            JournalDb::new(self.pool.clone(), scope, retention),
            self.rt.handle(),
        )
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

// --- 1. the upstream contract, as a law suite over any JournalStore -------

fn intent_before_provider_same_key_on_retry(db: &dyn JournalStore) {
    let first = db.record_intent("job-1", 3);
    assert_eq!(first.state, EffectState::Prepared);
    assert_eq!(first.generation, 0);
    assert_eq!(first, db.record_intent("job-1", 3));
}

fn claim_sets_owner_lease_generation_atomically(db: &dyn JournalStore) {
    db.record_intent("job-1", 3);
    let row = db.claim("job-1", "worker-a", 9999).unwrap();
    assert_eq!(row.owner.as_deref(), Some("worker-a"));
    assert_eq!(row.lease_deadline, Some(9999));
    assert_eq!(row.generation, 1);
    let row = db.claim("job-1", "worker-b", 10000).unwrap();
    assert_eq!(row.generation, 2);
    assert_eq!(row.owner.as_deref(), Some("worker-b"));
    assert!(db.claim("never-recorded", "w", 1).is_none());
}

fn settle_carries_generation_stale_settles_nothing(db: &dyn JournalStore) {
    db.record_intent("job-1", 3);
    db.claim("job-1", "worker-a", 9999);
    assert!(db.settle("job-1", 0, EffectState::InFlight).is_none());
    assert_eq!(db.get("job-1").unwrap().state, EffectState::Prepared);
    let row = db.settle("job-1", 1, EffectState::InFlight).unwrap();
    assert_eq!(row.state, EffectState::InFlight);
}

fn settle_to(db: &dyn JournalStore, key: &str, to: EffectState) -> Option<JournalRow> {
    let g = db.get(key).unwrap().generation;
    db.settle(key, g, to)
}

fn terminal_states_never_move(db: &dyn JournalStore) {
    db.record_intent("job-1", 3);
    db.claim("job-1", "w", 1);
    settle_to(db, "job-1", EffectState::InFlight).unwrap();
    settle_to(db, "job-1", EffectState::Settled).unwrap();
    assert!(settle_to(db, "job-1", EffectState::Failed).is_none());
    assert!(settle_to(db, "job-1", EffectState::Cancelled).is_none());
    assert_eq!(db.get("job-1").unwrap().state, EffectState::Settled);
}

fn unknown_has_no_default_exit(db: &dyn JournalStore) {
    db.record_intent("job-1", 3);
    db.claim("job-1", "w", 1);
    settle_to(db, "job-1", EffectState::InFlight).unwrap();
    settle_to(db, "job-1", EffectState::Unknown).unwrap();
    assert!(settle_to(db, "job-1", EffectState::Accepted).is_none());
    assert!(settle_to(db, "job-1", EffectState::Prepared).is_none());
    let row = settle_to(db, "job-1", EffectState::ReconcilePending).unwrap();
    assert_eq!(row.state, EffectState::ReconcilePending);
}

fn accepted_is_its_own_state(db: &dyn JournalStore) {
    db.record_intent("job-1", 3);
    db.claim("job-1", "w", 1);
    settle_to(db, "job-1", EffectState::InFlight).unwrap();
    let row = settle_to(db, "job-1", EffectState::Accepted).unwrap();
    assert_eq!(row.state, EffectState::Accepted);
    assert!(settle_to(db, "job-1", EffectState::Prepared).is_none());
}

fn exhausted_attempts_dead_letter(db: &dyn JournalStore) {
    db.record_intent("job-1", 1);
    assert_eq!(db.attempt("job-1").unwrap().state, EffectState::Prepared);
    let row = db.attempt("job-1").unwrap();
    assert_eq!(row.state, EffectState::Failed);
    assert_eq!(row.disposition, Some(JournalDisposition::TerminalFailed));
    assert!(db.attempt("never-recorded").is_none());
}

fn offset_and_state_commit_together(db: &dyn JournalStore) {
    db.record_intent("a", 3);
    db.record_intent("b", 3);
    let ra = db.get("a").unwrap();
    let rb = db.get("b").unwrap();
    assert!(rb.offset > ra.offset);
    match db.read(rb.offset).unwrap() {
        JournalRead::Entry { offset, row } => {
            assert_eq!(offset, rb.offset);
            assert_eq!(row.key, "b");
            assert_eq!(row.state, rb.state);
        }
        JournalRead::Gone { .. } => panic!("expected entry"),
    }
}

fn offsets_move_only_forward(db: &dyn JournalStore) {
    db.record_intent("a", 3);
    let first = db.get("a").unwrap().offset;
    db.claim("a", "w", 1);
    let second = db.get("a").unwrap().offset;
    assert!(second > first);
    // A refused settle commits nothing: the offset does not move.
    assert!(db.settle("a", 0, EffectState::InFlight).is_none());
    assert_eq!(db.get("a").unwrap().offset, second);
}

/// Needs a store built with retention 2.
fn old_offset_returns_gone_with_first_replayable(db: &dyn JournalStore) {
    db.record_intent("a", 3);
    let oldest = db.get("a").unwrap().offset;
    db.record_intent("b", 3);
    db.record_intent("c", 3);
    match db.read(oldest).unwrap() {
        JournalRead::Gone { first_replayable } => assert!(first_replayable > oldest),
        JournalRead::Entry { .. } => panic!("expected gone"),
    }
    match db.read(-1).unwrap() {
        JournalRead::Entry { row, .. } => assert_eq!(row.key, "c"),
        JournalRead::Gone { .. } => panic!("latest is never gone"),
    }
}

type Law = fn(&dyn JournalStore);
const LAWS: &[(&str, Law)] = &[
    (
        "intent_before_provider_same_key_on_retry",
        intent_before_provider_same_key_on_retry,
    ),
    (
        "claim_sets_owner_lease_generation_atomically",
        claim_sets_owner_lease_generation_atomically,
    ),
    (
        "settle_carries_generation_stale_settles_nothing",
        settle_carries_generation_stale_settles_nothing,
    ),
    ("terminal_states_never_move", terminal_states_never_move),
    ("unknown_has_no_default_exit", unknown_has_no_default_exit),
    ("accepted_is_its_own_state", accepted_is_its_own_state),
    (
        "exhausted_attempts_dead_letter",
        exhausted_attempts_dead_letter,
    ),
    (
        "offset_and_state_commit_together",
        offset_and_state_commit_together,
    ),
    ("offsets_move_only_forward", offsets_move_only_forward),
];

#[test]
fn the_reference_store_satisfies_the_laws() {
    for (_name, law) in LAWS {
        law(&InMemoryStore::new(8));
    }
    old_offset_returns_gone_with_first_replayable(&InMemoryStore::new(2));
}

#[test]
fn toron_journal_satisfies_the_same_laws() {
    let db = Db::new("laws");
    for (name, law) in LAWS {
        // A fresh scope per law: same isolation as a fresh in-memory store.
        law(&db.journal(name, 8));
    }
    old_offset_returns_gone_with_first_replayable(&db.journal("retention", 2));
}

// --- 2. differential against the reference ------------------------------

/// A tiny deterministic PRNG (no ambient randomness in a reproducible test).
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % n
    }
}

const STATES: [EffectState; 9] = [
    EffectState::Prepared,
    EffectState::InFlight,
    EffectState::Accepted,
    EffectState::Settled,
    EffectState::Failed,
    EffectState::Unknown,
    EffectState::ReconcilePending,
    EffectState::Cancelled,
    EffectState::Parked,
];

/// A row's comparable shape: everything but the offset.
type Shape = (
    String,
    EffectState,
    Option<String>,
    Option<i64>,
    u64,
    Option<JournalDisposition>,
    u32,
    u32,
);

/// Everything but the offset, which each store mints in its own space.
fn shape(r: &Option<JournalRow>) -> Option<Shape> {
    r.as_ref().map(|r| {
        (
            r.key.clone(),
            r.state,
            r.owner.clone(),
            r.lease_deadline,
            r.generation,
            r.disposition,
            r.attempts,
            r.max_attempts,
        )
    })
}

#[test]
fn random_operation_sequences_match_the_reference() {
    let db = Db::new("diff");
    let mut changed = 0;
    for seed in 1..=6u64 {
        let reference = InMemoryStore::new(1000);
        let toron = db.journal(&format!("diff-{seed}"), 1000);
        let mut rng = Lcg(seed);
        for step in 0..250 {
            let key = format!("k{}", rng.next(4));
            let (a, b) = match rng.next(5) {
                0 => {
                    let m = rng.next(3) as u32;
                    (
                        Some(reference.record_intent(&key, m)),
                        Some(toron.record_intent(&key, m)),
                    )
                }
                1 => {
                    let d = rng.next(100) as i64;
                    (reference.claim(&key, "w", d), toron.claim(&key, "w", d))
                }
                2 => {
                    // Mostly the current generation, sometimes a stale one.
                    let g = reference.get(&key).map_or(0, |r| r.generation);
                    let g = if rng.next(4) == 0 {
                        g.saturating_sub(1)
                    } else {
                        g
                    };
                    let to = STATES[rng.next(9) as usize];
                    (reference.settle(&key, g, to), toron.settle(&key, g, to))
                }
                3 => (reference.attempt(&key), toron.attempt(&key)),
                _ => (reference.get(&key), toron.get(&key)),
            };
            assert_eq!(shape(&a), shape(&b), "seed {seed} step {step} key {key}");
            assert_eq!(
                shape(&reference.get(&key)),
                shape(&toron.get(&key)),
                "seed {seed} step {step}"
            );
            if a.is_some() {
                changed += 1;
            }
        }
    }
    // Negative control on the generator: the sequences actually move rows.
    assert!(changed > 600, "only {changed} steps changed anything");
}

// --- 3. what only a database shows ---------------------------------------

#[test]
fn rows_survive_a_reopen_and_scopes_are_isolated() {
    let path = std::env::temp_dir().join(format!(
        "toron-journal-reopen-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let (offset, generation) = {
        let mut db = Db::at(path.clone());
        db.keep = true;
        let j = db.journal("loop-a", 8);
        j.record_intent("mail.append:x", 3);
        let row = j.claim("mail.append:x", "loop-a", 42).unwrap();
        assert!(
            db.journal("loop-b", 8).get("mail.append:x").is_none(),
            "scope b sees nothing of a"
        );
        (row.offset, row.generation)
    };
    let db = Db::at(path);
    let j = db.journal("loop-a", 8);
    let row = j.get("mail.append:x").expect("survives the reopen");
    assert_eq!(
        (row.offset, row.generation, row.owner.as_deref()),
        (offset, generation, Some("loop-a"))
    );
    // Idempotent after the reopen, too.
    assert_eq!(j.record_intent("mail.append:x", 3), row);
}

#[test]
fn racing_claims_from_two_processes_never_share_a_generation() {
    // Two pools on one file = two processes' views of one database.
    let a = Db::new("race");
    let mut b = Db::at(a.path.clone());
    b.keep = true;
    let ja = a.journal("race", 1000);
    let jb = b.journal("race", 1000);
    ja.record_intent("job", 3);
    let h1 = std::thread::spawn(move || {
        (0..40)
            .map(|_| ja.claim("job", "a", 1).unwrap().generation)
            .collect::<Vec<_>>()
    });
    let h2 = std::thread::spawn(move || {
        (0..40)
            .map(|_| jb.claim("job", "b", 1).unwrap().generation)
            .collect::<Vec<_>>()
    });
    let mut all: Vec<u64> = h1
        .join()
        .unwrap()
        .into_iter()
        .chain(h2.join().unwrap())
        .collect();
    all.sort_unstable();
    assert_eq!(
        all,
        (1..=80).collect::<Vec<u64>>(),
        "every claim minted a distinct generation"
    );
}

#[test]
fn a_tokio_worker_is_refused_not_blocked() {
    let db = Db::new("f2");
    let j = db.journal("f2", 8);
    let j2 = j.clone();
    let err = db
        .rt
        .block_on(async move {
            tokio::spawn(async move { j2.try_record_intent("k", 1) })
                .await
                .unwrap()
        })
        .expect_err("refused on a runtime thread");
    assert!(
        matches!(err, JournalError::Unavailable(ref m) if m.contains("F2")),
        "{err}"
    );
    // Through the trait the answer is honest: outcome unknown, reconcile.
    let j3 = j.clone();
    let row = db.rt.block_on(async move {
        tokio::spawn(async move { j3.record_intent("k", 1) })
            .await
            .unwrap()
    });
    assert_eq!(
        (row.state, row.disposition),
        (EffectState::Unknown, Some(JournalDisposition::Reconcile))
    );
    // And nothing was written.
    assert!(j.get("k").is_none());
}
