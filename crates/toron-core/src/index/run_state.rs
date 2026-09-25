// governed-by: ADR-0035
//! Run-state checkpoints: the durable projection of a long-lived pure fold.
//!
//! Implements the storage half of
//! `docs/notes/2026-09-26-run-state-checkpoint-design.md` (flywheel B2/B3).
//! Read that spec's §3 (authority rule), §5 (key shape), §7 (retention) and
//! §8 (fork semantics) before changing anything here — the constraints are not
//! stylistic.
//!
//! ## What this type is NOT
//!
//! It is not a second copy of the model that code may read to reconstruct
//! state on its own initiative. Per the spec's authority rule, the log is
//! authoritative and the model is a pure function of it; a field whose
//! ownership is ambiguous between this table and a projection seeder is a
//! defect. [`RunStateStore::append`] is therefore the only writer and
//! [`RunStateStore::load_for_resume`] is the only sanctioned way to seed a
//! model from disk.
//!
//! Every read shares one [`COLUMNS`] projection, so a column cannot drift
//! between `get`, `latest`, `history` and `load_for_resume`.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::SqlitePool;

const COLUMNS: &str = "thread_id, seq, checkpoint_id, parent_checkpoint_id, \
     model_json, msg_kind, effect_keys, created_at";

/// One checkpoint row: the fold's state at one boundary.
///
/// All-primitive columns, so `FromRow` derives directly. The hand-written
/// `map_*` readers used elsewhere in this module exist for rows carrying typed
/// enums that need parse-and-report; there is none here, so a derive is both
/// correct and less code.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::FromRow)]
pub struct RunStateCheckpoint {
    pub thread_id: String,
    pub seq: i64,
    pub checkpoint_id: String,
    pub parent_checkpoint_id: Option<String>,
    pub model_json: String,
    /// The `Msg` variant that produced this state, as evidence. `None` means
    /// "resume point, no message" — never a default guess.
    pub msg_kind: Option<String>,
    /// Bounded JSON array of effect keys this boundary's commands own: the
    /// pending-write set. On resume, a key here that the effect journal already
    /// reports `settled` replays from `outcome_json` instead of re-invoking the
    /// provider, and one left `in_flight` stays honestly Unknown.
    pub effect_keys: Option<String>,
    pub created_at: i64,
}

/// A checkpoint to append.
#[derive(Debug, Clone)]
pub struct NewRunStateCheckpoint {
    pub thread_id: String,
    pub seq: i64,
    pub parent_checkpoint_id: Option<String>,
    pub model_json: String,
    pub msg_kind: Option<String>,
    pub effect_keys: Option<String>,
    pub created_at: i64,
}

/// Rows removed by one prune call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStatePruneCounts {
    pub checkpoints: i64,
}

/// Durable run-state checkpoint store over the EXISTING index pool.
///
/// One index, one schema — the same rule as `0010_workflow_effects.sql`. This
/// type never opens a database of its own.
#[derive(Debug, Clone)]
pub struct RunStateStore {
    pool: SqlitePool,
}

impl RunStateStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// The durable, thread-independent evidence identity for a boundary.
    ///
    /// Fixed width keeps it lexicographically sortable, so a caller CAN sort by
    /// it across threads. The store itself orders by `seq` instead, because
    /// `seq` is the fold's own counter and does not depend on the clock; see
    /// [`RunStateStore::latest`]. Built from `(created_at_us, seq)` rather than
    /// a ULID crate so no dependency is added for an id this codebase already
    /// fully controls.
    pub fn checkpoint_id(thread_id: &str, seq: i64, created_at_us: i64) -> String {
        format!("{created_at_us:016x}-{seq:012x}-{}", short_hash(thread_id))
    }

    /// Append one boundary. Idempotent: a durable replay re-delivers the same
    /// `(thread_id, seq)`, so a repeat attaches to the existing row rather than
    /// forking it.
    ///
    /// Returns the stored row and whether this call created it. A repeat with
    /// DIFFERENT content for the same key is refused loudly — silently keeping
    /// either version would make the log's authority depend on write order.
    pub async fn append(&self, new: NewRunStateCheckpoint) -> Result<(RunStateCheckpoint, bool)> {
        let checkpoint_id = Self::checkpoint_id(&new.thread_id, new.seq, new.created_at);
        if let Some(row) = self.get(&new.thread_id, new.seq).await? {
            anyhow::ensure!(
                row.model_json == new.model_json
                    && row.checkpoint_id == checkpoint_id
                    && row.parent_checkpoint_id == new.parent_checkpoint_id
                    && row.msg_kind == new.msg_kind
                    && row.effect_keys == new.effect_keys,
                "run-state conflict at {}/{}: an existing checkpoint holds different \
                 content. The log is authoritative, so a divergent rewrite is a \
                 contract violation, not an update.",
                new.thread_id,
                new.seq
            );
            return Ok((row, false));
        }
        sqlx::query(
            "INSERT INTO run_state_checkpoints
                (thread_id, seq, checkpoint_id, parent_checkpoint_id, model_json,
                 msg_kind, effect_keys, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&new.thread_id)
        .bind(new.seq)
        .bind(&checkpoint_id)
        .bind(&new.parent_checkpoint_id)
        .bind(&new.model_json)
        .bind(&new.msg_kind)
        .bind(&new.effect_keys)
        .bind(new.created_at)
        .execute(&self.pool)
        .await?;
        Ok((
            RunStateCheckpoint {
                thread_id: new.thread_id,
                seq: new.seq,
                checkpoint_id,
                parent_checkpoint_id: new.parent_checkpoint_id,
                model_json: new.model_json,
                msg_kind: new.msg_kind,
                effect_keys: new.effect_keys,
                created_at: new.created_at,
            },
            true,
        ))
    }

    /// The row at one boundary, or `None`. Equality on the primary key.
    pub async fn get(&self, thread_id: &str, seq: i64) -> Result<Option<RunStateCheckpoint>> {
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints WHERE thread_id = ?1 AND seq = ?2"
        ))
        .bind(thread_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// The newest row for a thread, or `None` when it has no boundary yet.
    ///
    /// Orders by `seq`, NOT by `checkpoint_id`. `seq` is the fold's own
    /// monotonic counter, so this is independent of the wall clock; ordering by
    /// an id that embeds `created_at_us` would let a backwards clock step make
    /// `latest` return an OLDER boundary than one already handed out. The
    /// `(thread_id, seq)` primary key serves this order directly.
    pub async fn latest(&self, thread_id: &str) -> Result<Option<RunStateCheckpoint>> {
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints WHERE thread_id = ?1 \
             ORDER BY seq DESC LIMIT 1"
        ))
        .bind(thread_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// One row by its durable evidence id, independent of thread.
    pub async fn get_by_checkpoint_id(
        &self,
        checkpoint_id: &str,
    ) -> Result<Option<RunStateCheckpoint>> {
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints WHERE checkpoint_id = ?1"
        ))
        .bind(checkpoint_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// History newest-first, optionally only rows strictly older than
    /// `before_seq`. `limit` of 0 means unbounded — callers that walk history
    /// should pass a real bound, because an unbounded read of a long-lived
    /// thread is a memory bug.
    pub async fn history(
        &self,
        thread_id: &str,
        before_seq: Option<i64>,
        limit: i64,
    ) -> Result<Vec<RunStateCheckpoint>> {
        let cap = if limit > 0 { limit } else { i64::MAX };
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints \
             WHERE thread_id = ?1 AND (?2 IS NULL OR seq < ?2) \
             ORDER BY seq DESC LIMIT ?3"
        ))
        .bind(thread_id)
        .bind(before_seq)
        .bind(cap)
        .fetch_all(&self.pool)
        .await?)
    }

    /// The sanctioned resume seed: the row at `seq`, else the newest row at or
    /// before it, else `None`.
    ///
    /// "At or before" is deliberate. A resume demanding an exact boundary and
    /// getting nothing is a worse failure than one resuming from the last
    /// boundary at or before the request: the caller then knows the true state
    /// it got and the gap stays visible instead of being papered over.
    pub async fn load_for_resume(
        &self,
        thread_id: &str,
        seq: i64,
    ) -> Result<Option<RunStateCheckpoint>> {
        if let Some(exact) = self.get(thread_id, seq).await? {
            return Ok(Some(exact));
        }
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints \
             WHERE thread_id = ?1 AND seq <= ?2 ORDER BY seq DESC LIMIT 1"
        ))
        .bind(thread_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// Delete rows strictly older than `protect_from_seq` and return the count.
    ///
    /// `protect_from_seq` is REQUIRED, not defaulted. The store cannot know
    /// which boundary a live resume still needs, so it refuses to guess: making
    /// the caller state the floor turns "prune must not drop a row a live
    /// resume needs" from a convention into something the API forces. Pass the
    /// resume point; 0 prunes everything, legitimate only when nothing can
    /// resume.
    pub async fn prune_before(
        &self,
        thread_id: &str,
        protect_from_seq: i64,
    ) -> Result<RunStatePruneCounts> {
        let res =
            sqlx::query("DELETE FROM run_state_checkpoints WHERE thread_id = ?1 AND seq < ?2")
                .bind(thread_id)
                .bind(protect_from_seq)
                .execute(&self.pool)
                .await?;
        Ok(RunStatePruneCounts {
            checkpoints: res.rows_affected() as i64,
        })
    }
}

/// A short, stable, dependency-free thread discriminator for the checkpoint id.
///
/// Only needs to disambiguate two threads sharing a `(created_at_us, seq)`
/// pair; the primary key remains `(thread_id, seq)`.
fn short_hash(thread_id: &str) -> String {
    let mut acc: u64 = 0xcbf2_9ce4_8422_2325;
    for b in thread_id.as_bytes() {
        acc ^= u64::from(*b);
        acc = acc.wrapping_mul(0x100_0000_01b3);
    }
    format!("{acc:016x}")
}

/// Parse a stored `model_json`, refusing a corrupt row loudly.
///
/// A checkpoint whose payload will not parse is not "empty state" — resuming
/// from it would silently discard the model. Fail closed.
pub fn parse_model(row: &RunStateCheckpoint) -> Result<Value> {
    Ok(serde_json::from_str(&row.model_json)?)
}
