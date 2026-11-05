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

use anyhow::{Context, Result};
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
    /// `before_seq`. `before_seq` is exclusive, so a walk resumes without
    /// re-reading its own cursor row.
    ///
    /// `limit` is REQUIRED and must be positive — there is deliberately no
    /// "0 means everything" escape. An unbounded read of a long-lived thread is
    /// a memory bug, and a default that silently means `i64::MAX` is exactly
    /// the kind of permissive default this codebase rejects elsewhere. The
    /// same reasoning that makes [`RunStateStore::prune_before`]'s protected
    /// floor a required argument applies here: a bound the caller must state
    /// beats a bound a convention provides.
    pub async fn history(
        &self,
        thread_id: &str,
        before_seq: Option<i64>,
        limit: i64,
    ) -> Result<Vec<RunStateCheckpoint>> {
        anyhow::ensure!(
            limit > 0,
            "run-state history needs a positive limit ({limit}); an unbounded read of a \
             long-lived thread is a memory bug, not a convenience"
        );
        Ok(sqlx::query_as::<_, RunStateCheckpoint>(&format!(
            "SELECT {COLUMNS} FROM run_state_checkpoints \
             WHERE thread_id = ?1 AND (?2 IS NULL OR seq < ?2) \
             ORDER BY seq DESC LIMIT ?3"
        ))
        .bind(thread_id)
        .bind(before_seq)
        .bind(limit)
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

    /// Read one boundary for a reader: the model plus its pending-write set.
    ///
    /// The ONE effect in the read side. A read, so it takes no clock and no
    /// network; `created_at` comes back off the row rather than from
    /// `SystemTime`, which is what lets a test pin it.
    ///
    /// EXACT, unlike [`RunStateStore::load_for_resume`]. A resume may fall back
    /// to the boundary at-or-before the request, because the alternative is
    /// refusing to resume over a gap; a READER that fell back would silently
    /// show a different boundary than the one asked for, and the operator
    /// would believe they are looking at step N. So a miss here is a typed
    /// failure naming the boundary, never a nearby row.
    pub async fn replay(&self, thread_id: &str, seq: i64) -> Result<RunStateReplay> {
        let row = self.get(thread_id, seq).await?.ok_or_else(|| {
            anyhow::anyhow!(
                "no run-state boundary at {thread_id}/{seq}. `replay` is exact on purpose: \
                 falling back to a nearby boundary would show a step the caller did not \
                 ask for. List the thread's boundaries, or use the resume seed, which is \
                 the one read allowed to fall back at-or-before."
            )
        })?;
        RunStateReplay::from_checkpoint(&row)
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

/// One boundary, rendered for a reader: the model plus the pending-write set.
///
/// The pending-write set is the half a bare `model_json` cannot give you. A
/// boundary that owned effects may have already dispatched them to a real
/// provider, so a reader deciding whether it is safe to re-enter that boundary
/// needs the KEYS, not just the state. `effect_keys` is empty for a boundary
/// that owned nothing, which is a fact about the boundary rather than a
/// missing read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunStateReplay {
    pub thread_id: String,
    pub seq: i64,
    pub checkpoint_id: String,
    pub parent_checkpoint_id: Option<String>,
    /// The `Msg` variant that produced this state, as evidence. `None` means
    /// "resume point, no message" — never a default guess.
    pub msg_kind: Option<String>,
    pub created_at: i64,
    /// The fold's state at this boundary, already parsed. A payload that will
    /// not parse is a typed failure, never an empty model.
    pub model: Value,
    /// Effect keys this boundary owned: the pending-write set.
    pub effect_keys: Vec<String>,
}

impl RunStateReplay {
    /// Build a reader view from a stored row. Pure: no clock, no I/O.
    pub fn from_checkpoint(row: &RunStateCheckpoint) -> Result<Self> {
        Ok(Self {
            thread_id: row.thread_id.clone(),
            seq: row.seq,
            checkpoint_id: row.checkpoint_id.clone(),
            parent_checkpoint_id: row.parent_checkpoint_id.clone(),
            msg_kind: row.msg_kind.clone(),
            created_at: row.created_at,
            model: parse_model(row)
                .with_context(|| format!("run-state boundary {}/{}", row.thread_id, row.seq))?,
            effect_keys: parse_effect_keys(row)?,
        })
    }

    /// Refuse to install this boundary as a live model. Always.
    ///
    /// Spec §3 makes the log authoritative and the model a pure function of
    /// it, and that is a statement about which side is the RECORD. It is not a
    /// grant to any component to SEED the live model from the log: seeding is
    /// an authority call with exactly one legitimate owner, the live host
    /// beside its existing resume path. A reader that grew an installer would
    /// put a second seeding path beside it, which §3 forbids and which the
    /// bead that owns the authority call has to make.
    ///
    /// This exists as a method rather than as the absence of one on purpose:
    /// a caller who reaches for it gets the refusal AND the name of who to
    /// ask, at the call site, instead of discovering the rule by reading a
    /// design note.
    pub fn install(self) -> Result<RunStateCheckpoint, InstallRefused> {
        Err(InstallRefused {
            thread_id: self.thread_id,
            seq: self.seq,
        })
    }
}

/// A typed refusal to seed a live model from the log.
///
/// Not a defect and not a cancellation: the caller asked a legal question and
/// the answer is that this component does not own the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallRefused {
    pub thread_id: String,
    pub seq: i64,
}

impl std::fmt::Display for InstallRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "refusing to seed a live model from run-state boundary {}/{}: the log is a \
             durable projection, not a seeding source (spec S3). Seeding is the live \
             host's authority call beside its own resume path, so a reader with an \
             installer would be a second seeding path. Ask the run's authority owner \
             to wire it, do not add one here.",
            self.thread_id, self.seq
        )
    }
}

impl std::error::Error for InstallRefused {}

/// Parse the stored pending-write set. Pure; fails closed.
///
/// `NULL` is a boundary that owned no effects, which is a real state and not a
/// missing read. Malformed JSON is NOT that: it is an unreadable row, and
/// reporting it as "owned nothing" would tell a reader it is safe to re-enter
/// a boundary whose effects nobody can name.
fn parse_effect_keys(row: &RunStateCheckpoint) -> Result<Vec<String>> {
    let Some(raw) = row.effect_keys.as_deref() else {
        return Ok(Vec::new());
    };
    serde_json::from_str(raw).with_context(|| {
        format!(
            "run-state boundary {}/{} carries an unreadable pending-write set; refusing to \
             report it as empty, because a reader would then believe re-entering this \
             boundary is safe when its effects cannot be named",
            row.thread_id, row.seq
        )
    })
}

/// What a caller is asking a fork to do.
///
/// `Dry` is the default and the only variant a plain `toron run-state fork`
/// ever builds. `Perform` is not a boolean you flip; it carries the
/// acknowledgement, because the hazard it names is real: a fork point may
/// ALREADY have dispatched effects to a provider, so re-entering it can
/// duplicate work that already left the machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ForkIntent {
    #[default]
    Dry,
    Perform(AcknowledgeDispatchedEffects),
}

/// The caller's acknowledgement that this boundary may already have dispatched
/// real external effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcknowledgeDispatchedEffects;

impl AcknowledgeDispatchedEffects {
    /// Spelled as a call rather than a bare literal so the acknowledgement
    /// reads in a diff as an event, not as a field someone set.
    pub const fn accept() -> Self {
        Self
    }
}

/// What a fork WOULD do, rendered for a reader. Pure data.
///
/// `installable` is carried as a field rather than left implied so the refusal
/// is something an operator can read in the output instead of something they
/// have to know to go looking for in a design note.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForkPlan {
    pub thread_id: String,
    pub seq: i64,
    pub checkpoint_id: String,
    /// Effect keys the boundary already owned. Re-entering this boundary may
    /// replay work that already reached a provider.
    pub inherited_effect_keys: Vec<String>,
    /// Whether this plan would dispatch anything. True only for an
    /// acknowledged `Perform` intent.
    pub would_perform: bool,
    /// Always false in this build: nothing here seeds a live model from the
    /// log (spec §3). See [`RunStateReplay::install`].
    pub installable: bool,
    /// Operator-facing refusals, ordered so the blocking one reads first.
    pub refusals: Vec<String>,
}

/// Decide a fork. PURE: same boundary + same intent ⇒ same plan.
///
/// Total rather than fallible — a fork is never an error, it is a report. The
/// refusals it can carry are the answer, not a failure to produce one.
pub fn plan_fork(boundary: &RunStateReplay, intent: &ForkIntent) -> ForkPlan {
    let (would_perform, mut refusals) = match intent {
        ForkIntent::Dry => (
            false,
            vec![
                "dry fork: nothing is dispatched. A performing fork is an explicit \
                 opt-in, because this boundary may already have dispatched effects."
                    .to_string(),
            ],
        ),
        ForkIntent::Perform(_) => (true, Vec::new()),
    };

    if !boundary.effect_keys.is_empty() {
        refusals.insert(
            0,
            format!(
                "this boundary owned {} effect(s) ({}), so re-entering it may replay work \
                 that already reached a provider; a settled effect replays from its \
                 recorded outcome and one left in_flight stays honestly unknown",
                boundary.effect_keys.len(),
                boundary.effect_keys.join(", ")
            ),
        );
    }

    refusals.push(
        "no installer in this build: seeding a live model from the log is the live \
         host's authority call, so this plan is a report and not a resume."
            .to_string(),
    );

    ForkPlan {
        thread_id: boundary.thread_id.clone(),
        seq: boundary.seq,
        checkpoint_id: boundary.checkpoint_id.clone(),
        inherited_effect_keys: boundary.effect_keys.clone(),
        would_perform,
        installable: false,
        refusals,
    }
}
