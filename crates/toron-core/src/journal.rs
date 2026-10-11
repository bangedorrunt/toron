// governed-by: ADR-0040, ADR-0041
//! [`ToronJournal`]: the effect journal as a [`kernel::JournalStore`] over
//! toron's SQLite store (redesign §11.1 item 3; build plan Phase 2).
//!
//! The contract is upstream's (`elm-effect-journal` `store.rs`): an intent
//! is recorded before the provider runs, idempotent on its key; a claim
//! sets owner, lease deadline and `generation + 1` in one step; a settle
//! carries the generation, and a stale generation, an illegal edge
//! ([`kernel::can_transition`]) or a terminal row settles nothing;
//! exhausted attempts dead-letter as `Failed` + `TerminalFailed`; the row's
//! offset and its state commit together and offsets only move forward;
//! reading below retention answers `Gone` with the first replayable offset.
//!
//! Durability: every operation is one `BEGIN IMMEDIATE` transaction, so
//! two processes on one database serialize and a crash leaves either the
//! old row or the new row with its log entry, never one without the other.
//!
//! Threading: like [`crate::inbox::ToronLog`], the trait is synchronous, so
//! a [`ToronJournal`] handle talks to a task on a Tokio runtime through a
//! request/reply channel. Call it from the loop's own OS thread; on a
//! Tokio worker it refuses instead of blocking (the F2 nested-runtime
//! guard). The `try_*` methods return the error; the trait methods, whose
//! signatures cannot, answer an unreachable store with "nothing changed"
//! (`None`), and `record_intent` with an `Unknown` row marked `Reconcile`:
//! the honest answer when the write's outcome is not known.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::kernel::{
    EffectState, JournalDisposition, JournalRead, JournalRow, JournalStore, can_transition,
};

/// How long a bridge call waits for the store.
pub const JOURNAL_BRIDGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Why a journal call did not answer.
#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    /// The store failed.
    #[error("journal store: {0}")]
    Store(#[from] sqlx::Error),
    /// The bridge could not reach the store task, or it did not answer.
    #[error("journal unavailable: {0}")]
    Unavailable(String),
}

fn disposition_str(d: JournalDisposition) -> &'static str {
    d.as_str()
}

fn parse_disposition(s: &str) -> Option<JournalDisposition> {
    use JournalDisposition as D;
    [
        D::Continue,
        D::Retry,
        D::Reroute,
        D::Blocked,
        D::TerminalFailed,
        D::Cancelled,
        D::Reconcile,
    ]
    .into_iter()
    .find(|d| d.as_str() == s)
}

/// The row as stored in the change log (snapshot at that offset).
#[derive(Serialize, Deserialize)]
struct RowWire {
    key: String,
    state: EffectState,
    owner: Option<String>,
    lease_deadline: Option<i64>,
    generation: u64,
    disposition: Option<String>,
    attempts: u32,
    max_attempts: u32,
}

impl RowWire {
    fn from_row(r: &JournalRow) -> Self {
        Self {
            key: r.key.clone(),
            state: r.state,
            owner: r.owner.clone(),
            lease_deadline: r.lease_deadline,
            generation: r.generation,
            disposition: r.disposition.map(|d| disposition_str(d).to_string()),
            attempts: r.attempts,
            max_attempts: r.max_attempts,
        }
    }

    fn into_row(self, offset: i64) -> JournalRow {
        JournalRow {
            key: self.key,
            state: self.state,
            owner: self.owner,
            lease_deadline: self.lease_deadline,
            generation: self.generation,
            offset,
            disposition: self.disposition.as_deref().and_then(parse_disposition),
            attempts: self.attempts,
            max_attempts: self.max_attempts,
        }
    }
}

type RowTuple = (
    String,
    String,
    Option<String>,
    Option<i64>,
    i64,
    i64,
    Option<String>,
    i64,
    i64,
);

fn row_from_tuple(t: RowTuple) -> Result<JournalRow, sqlx::Error> {
    let (key, state, owner, lease_deadline, generation, offset, disposition, attempts, max) = t;
    let state: EffectState = state
        .parse()
        .map_err(|e: String| sqlx::Error::Decode(e.into()))?;
    Ok(JournalRow {
        key,
        state,
        owner,
        lease_deadline,
        generation: generation as u64,
        offset,
        disposition: disposition.as_deref().and_then(parse_disposition),
        attempts: attempts as u32,
        max_attempts: max as u32,
    })
}

/// The async SQLite journal for one scope (one journal namespace; several
/// loops can share a database without sharing keys).
#[derive(Clone)]
pub struct JournalDb {
    pool: SqlitePool,
    scope: String,
    retention: i64,
}

impl JournalDb {
    /// A journal over `pool` for `scope`, keeping the last `retention`
    /// change-log entries (at least 1).
    pub fn new(pool: SqlitePool, scope: impl Into<String>, retention: usize) -> Self {
        Self {
            pool,
            scope: scope.into(),
            retention: retention.max(1) as i64,
        }
    }

    async fn load(
        &self,
        conn: &mut sqlx::SqliteConnection,
        key: &str,
    ) -> Result<Option<JournalRow>, sqlx::Error> {
        let t: Option<RowTuple> = sqlx::query_as(
            "SELECT key, state, owner, lease_deadline, generation, log_offset, disposition, \
             attempts, max_attempts FROM effect_journal WHERE scope = ?1 AND key = ?2",
        )
        .bind(&self.scope)
        .bind(key)
        .fetch_optional(&mut *conn)
        .await?;
        t.map(row_from_tuple).transpose()
    }

    /// Append the change-log entry and write the row at its offset, in the
    /// caller's transaction; then trim retention.
    async fn commit(
        &self,
        conn: &mut sqlx::SqliteConnection,
        row: &JournalRow,
    ) -> Result<JournalRow, sqlx::Error> {
        let wire = serde_json::to_string(&RowWire::from_row(row))
            .map_err(|e| sqlx::Error::Encode(e.into()))?;
        let offset = sqlx::query(
            "INSERT INTO effect_journal_log (scope, key, row_json) VALUES (?1, ?2, ?3)",
        )
        .bind(&self.scope)
        .bind(&row.key)
        .bind(&wire)
        .execute(&mut *conn)
        .await?
        .last_insert_rowid();
        sqlx::query(
            "INSERT INTO effect_journal (scope, key, state, owner, lease_deadline, generation, \
             log_offset, disposition, attempts, max_attempts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
             ON CONFLICT (scope, key) DO UPDATE SET state = excluded.state, \
             owner = excluded.owner, lease_deadline = excluded.lease_deadline, \
             generation = excluded.generation, log_offset = excluded.log_offset, \
             disposition = excluded.disposition, attempts = excluded.attempts, \
             max_attempts = excluded.max_attempts",
        )
        .bind(&self.scope)
        .bind(&row.key)
        .bind(row.state.to_string())
        .bind(&row.owner)
        .bind(row.lease_deadline)
        .bind(row.generation as i64)
        .bind(offset)
        .bind(row.disposition.map(disposition_str))
        .bind(i64::from(row.attempts))
        .bind(i64::from(row.max_attempts))
        .execute(&mut *conn)
        .await?;
        sqlx::query(
            "DELETE FROM effect_journal_log WHERE scope = ?1 AND log_offset <= \
             (SELECT log_offset FROM effect_journal_log WHERE scope = ?1 \
              ORDER BY log_offset DESC LIMIT 1 OFFSET ?2)",
        )
        .bind(&self.scope)
        .bind(self.retention)
        .execute(&mut *conn)
        .await?;
        let mut out = row.clone();
        out.offset = offset;
        Ok(out)
    }

    /// Run `step` on the current row inside one `BEGIN IMMEDIATE`
    /// transaction; `Some(next)` commits it, `None` changes nothing.
    async fn transact(
        &self,
        key: &str,
        step: impl FnOnce(Option<JournalRow>) -> Step,
    ) -> Result<Option<JournalRow>, sqlx::Error> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = self.load(&mut tx, key).await?;
        let out = match step(current) {
            Step::Keep(r) => r,
            Step::Commit(next) => Some(self.commit(&mut tx, &next).await?),
        };
        tx.commit().await?;
        Ok(out)
    }

    /// Record intent (idempotent on key).
    pub async fn record_intent(
        &self,
        key: &str,
        max_attempts: u32,
    ) -> Result<JournalRow, sqlx::Error> {
        let key_s = key.to_string();
        let row = self
            .transact(key, move |cur| match cur {
                Some(r) => Step::Keep(Some(r)),
                None => Step::Commit(JournalRow {
                    key: key_s,
                    state: EffectState::Prepared,
                    owner: None,
                    lease_deadline: None,
                    generation: 0,
                    offset: -1,
                    disposition: None,
                    attempts: 0,
                    max_attempts,
                }),
            })
            .await?;
        row.ok_or_else(|| sqlx::Error::RowNotFound)
    }

    /// Claim: owner + deadline + generation + 1, atomically.
    pub async fn claim(
        &self,
        key: &str,
        owner: &str,
        lease_deadline: i64,
    ) -> Result<Option<JournalRow>, sqlx::Error> {
        let owner = owner.to_string();
        self.transact(key, move |cur| match cur {
            None => Step::Keep(None),
            Some(mut r) => {
                r.owner = Some(owner);
                r.lease_deadline = Some(lease_deadline);
                r.generation += 1;
                Step::Commit(r)
            }
        })
        .await
    }

    /// Settle toward `to` carrying `generation`.
    pub async fn settle(
        &self,
        key: &str,
        generation: u64,
        to: EffectState,
    ) -> Result<Option<JournalRow>, sqlx::Error> {
        self.transact(key, move |cur| match cur {
            Some(mut r)
                if r.generation == generation
                    && !r.state.is_terminal()
                    && can_transition(r.state, to) =>
            {
                r.state = to;
                Step::Commit(r)
            }
            _ => Step::Keep(None),
        })
        .await
    }

    /// Note one provider attempt; exhaustion dead-letters the row.
    pub async fn attempt(&self, key: &str) -> Result<Option<JournalRow>, sqlx::Error> {
        self.transact(key, |cur| match cur {
            None => Step::Keep(None),
            Some(mut r) => {
                r.attempts += 1;
                if r.attempts > r.max_attempts && !r.state.is_terminal() {
                    r.state = EffectState::Failed;
                    r.disposition = Some(JournalDisposition::TerminalFailed);
                }
                Step::Commit(r)
            }
        })
        .await
    }

    /// Read one offset (`-1` = latest).
    pub async fn read(&self, offset: i64) -> Result<Option<JournalRead>, sqlx::Error> {
        let entry: Option<(i64, String)> = if offset == -1 {
            sqlx::query_as(
                "SELECT log_offset, row_json FROM effect_journal_log WHERE scope = ?1 \
                 ORDER BY log_offset DESC LIMIT 1",
            )
            .bind(&self.scope)
            .fetch_optional(&self.pool)
            .await?
        } else {
            let first: Option<i64> = sqlx::query_scalar(
                "SELECT MIN(log_offset) FROM effect_journal_log WHERE scope = ?1",
            )
            .bind(&self.scope)
            .fetch_one(&self.pool)
            .await?;
            if offset < first.unwrap_or(0) {
                return Ok(Some(JournalRead::Gone {
                    first_replayable: first.unwrap_or(0),
                }));
            }
            sqlx::query_as(
                "SELECT log_offset, row_json FROM effect_journal_log \
                 WHERE scope = ?1 AND log_offset = ?2",
            )
            .bind(&self.scope)
            .bind(offset)
            .fetch_optional(&self.pool)
            .await?
        };
        entry
            .map(|(o, json)| {
                serde_json::from_str::<RowWire>(&json)
                    .map(|w| JournalRead::Entry {
                        offset: o,
                        row: w.into_row(o),
                    })
                    .map_err(|e| sqlx::Error::Decode(e.into()))
            })
            .transpose()
    }

    /// Current row for a key.
    pub async fn get(&self, key: &str) -> Result<Option<JournalRow>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.load(&mut conn, key).await
    }
}

enum Step {
    Keep(Option<JournalRow>),
    Commit(JournalRow),
}

type Job = Box<dyn FnOnce(JournalDb) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// The SQLite journal as a synchronous [`JournalStore`] handle for a loop
/// on its own OS thread.
#[derive(Clone)]
pub struct ToronJournal {
    tx: tokio::sync::mpsc::UnboundedSender<Job>,
}

impl ToronJournal {
    /// Spawn the store task on `rt` and return the sync handle.
    pub fn spawn(db: JournalDb, rt: &tokio::runtime::Handle) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
        rt.spawn(async move {
            while let Some(job) = rx.recv().await {
                job(db.clone()).await;
            }
        });
        Self { tx }
    }

    fn call<T, F, Fut>(&self, f: F) -> Result<T, JournalError>
    where
        T: Send + 'static,
        F: FnOnce(JournalDb) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, sqlx::Error>> + Send + 'static,
    {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(JournalError::Unavailable(
                "ToronJournal called on a Tokio runtime thread: host the loop on a dedicated OS thread (F2)".into(),
            ));
        }
        let (reply, rx) = std::sync::mpsc::channel();
        let job: Job = Box::new(move |db| {
            Box::pin(async move {
                let _ = reply.send(f(db).await);
            })
        });
        self.tx
            .send(job)
            .map_err(|_| JournalError::Unavailable("journal store task is gone".into()))?;
        rx.recv_timeout(JOURNAL_BRIDGE_TIMEOUT)
            .map_err(|e| JournalError::Unavailable(format!("journal store did not answer: {e}")))?
            .map_err(JournalError::from)
    }

    /// [`JournalStore::record_intent`], with the error.
    pub fn try_record_intent(
        &self,
        key: &str,
        max_attempts: u32,
    ) -> Result<JournalRow, JournalError> {
        let key = key.to_string();
        self.call(move |db| async move { db.record_intent(&key, max_attempts).await })
    }

    /// [`JournalStore::claim`], with the error.
    pub fn try_claim(
        &self,
        key: &str,
        owner: &str,
        lease_deadline: i64,
    ) -> Result<Option<JournalRow>, JournalError> {
        let (key, owner) = (key.to_string(), owner.to_string());
        self.call(move |db| async move { db.claim(&key, &owner, lease_deadline).await })
    }

    /// [`JournalStore::settle`], with the error.
    pub fn try_settle(
        &self,
        key: &str,
        generation: u64,
        to: EffectState,
    ) -> Result<Option<JournalRow>, JournalError> {
        let key = key.to_string();
        self.call(move |db| async move { db.settle(&key, generation, to).await })
    }

    /// [`JournalStore::attempt`], with the error.
    pub fn try_attempt(&self, key: &str) -> Result<Option<JournalRow>, JournalError> {
        let key = key.to_string();
        self.call(move |db| async move { db.attempt(&key).await })
    }

    /// [`JournalStore::read`], with the error.
    pub fn try_read(&self, offset: i64) -> Result<Option<JournalRead>, JournalError> {
        self.call(move |db| async move { db.read(offset).await })
    }

    /// [`JournalStore::get`], with the error.
    pub fn try_get(&self, key: &str) -> Result<Option<JournalRow>, JournalError> {
        let key = key.to_string();
        self.call(move |db| async move { db.get(&key).await })
    }
}

fn unreachable_store(op: &str, e: &JournalError) {
    tracing::warn!("ToronJournal {op}: {e}; answering 'nothing changed'");
}

impl JournalStore for ToronJournal {
    fn record_intent(&self, key: &str, max_attempts: u32) -> JournalRow {
        self.try_record_intent(key, max_attempts)
            .unwrap_or_else(|e| {
                unreachable_store("record_intent", &e);
                JournalRow {
                    key: key.to_string(),
                    state: EffectState::Unknown,
                    owner: None,
                    lease_deadline: None,
                    generation: 0,
                    offset: -1,
                    disposition: Some(JournalDisposition::Reconcile),
                    attempts: 0,
                    max_attempts,
                }
            })
    }

    fn claim(&self, key: &str, owner: &str, lease_deadline: i64) -> Option<JournalRow> {
        self.try_claim(key, owner, lease_deadline)
            .unwrap_or_else(|e| {
                unreachable_store("claim", &e);
                None
            })
    }

    fn settle(&self, key: &str, generation: u64, to: EffectState) -> Option<JournalRow> {
        self.try_settle(key, generation, to).unwrap_or_else(|e| {
            unreachable_store("settle", &e);
            None
        })
    }

    fn attempt(&self, key: &str) -> Option<JournalRow> {
        self.try_attempt(key).unwrap_or_else(|e| {
            unreachable_store("attempt", &e);
            None
        })
    }

    fn read(&self, offset: i64) -> Option<JournalRead> {
        self.try_read(offset).unwrap_or_else(|e| {
            unreachable_store("read", &e);
            None
        })
    }

    fn get(&self, key: &str) -> Option<JournalRow> {
        self.try_get(key).unwrap_or_else(|e| {
            unreachable_store("get", &e);
            None
        })
    }
}
