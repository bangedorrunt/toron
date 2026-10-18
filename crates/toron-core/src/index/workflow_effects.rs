// governed-by: ADR-0032
#![forbid(unsafe_code)]

//! Workflow-effect journal storage (ADR-0032 Spec 5.1, bd-v139.1).
//!
//! Durable records for workflow runs, logical effects, dispatch attempts,
//! parked pending work, and terminal publications in the existing SQLite
//! persistence boundary (migration `0010_workflow_effects.sql`). This is a
//! storage primitive only: no provider calls, no sink draining, no
//! reconciliation policy, no pruning (retention enforcement is bd-v139.3).
//!
//! The mailbox `receipts` table is untouched and unreferenced here:
//! `owed → acked → resulted` describes message lifecycle only and can never
//! substitute for workflow-effect state (ADR-0032 §31 correction).

use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::BTreeMap;

/// Maximum bytes kept in any single evidence projection. Longer payloads
/// are truncated with a marker — diagnostics stay bounded and secret-safe
/// by construction (redacted projections and hashes in, raw renders out).
pub const MAX_EVIDENCE_BYTES: usize = 4096;

/// Truncation marker appended to over-bound evidence.
pub const TRUNCATION_MARKER: &str = "…[truncated]";

/// Bound one evidence projection to [`MAX_EVIDENCE_BYTES`] bytes.
fn bound_evidence(value: &serde_json::Value) -> String {
    let mut s = serde_json::to_string(value).unwrap_or_default();
    if s.len() > MAX_EVIDENCE_BYTES {
        let mut end = MAX_EVIDENCE_BYTES;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push_str(TRUNCATION_MARKER);
    }
    s
}

/// Microsecond clock (i64-µs, ADR G7).
///
/// Host-side stamp (ADR-0035 wave 1): the single `SystemTime` read lives in
/// `index::writer::SystemClock`; this delegates so no clock is read in this
/// file. Per-method host-clock threading lands with wave-4 storage truth.
pub fn now_us() -> i64 {
    crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
}

/// Per-process event distinguisher: two mutations of one key in one µs
/// still land distinct log rows (never a silent dedup loss).
static WFX_EVENT_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Append one `wfx.row` event inside the caller's transaction (dual-write
/// half of the v20 log for workflow effects). The payload is the full
/// post-write row; replay folds last-write-wins per (table, key).
/// Generation contention retries 3x, then fails loud (fail-closed).
async fn log_wfx_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    key: &str,
    row_json: &serde_json::Value,
    ts_us: i64,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(&serde_json::json!({
        "table": table,
        "key": key,
        "row": row_json,
        "ts_us": ts_us,
    }))
    .unwrap_or_default();
    append_wfx_event_in_tx(
        tx,
        "wfx.row",
        &format!("wfx:{table}:{key}"),
        &payload,
        ts_us,
    )
    .await
    .map_err(|e| anyhow::anyhow!("wfx dual-write contention on {table}:{key}: {e}"))
}

/// Tombstone twin of [`log_wfx_event_in_tx`]: rebuild must not resurrect
/// pruned/deleted rows.
async fn log_wfx_tombstone_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    key: &str,
    ts_us: i64,
) -> anyhow::Result<()> {
    let payload = serde_json::to_string(&serde_json::json!({
        "table": table,
        "key": key,
        "tombstone": true,
    }))
    .unwrap_or_default();
    append_wfx_event_in_tx(
        tx,
        "wfx.tombstone",
        &format!("wfx-tomb:{table}:{key}"),
        &payload,
        ts_us,
    )
    .await
    .map_err(|e| anyhow::anyhow!("wfx tombstone contention on {table}:{key}: {e}"))
}

/// Single generation-assigning append behind both log helpers: one retry
/// loop, one manifest advance. Event ids suffix pid+seq so two mutations
/// of one key in one µs never collide into a silent dedup loss.
async fn append_wfx_event_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    kind: &str,
    id_prefix: &str,
    payload: &str,
    ts_us: i64,
) -> anyhow::Result<()> {
    for _ in 0..3 {
        let mrow: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM generation_manifest WHERE singleton = 1")
                .fetch_optional(&mut **tx)
                .await?;
        let next = mrow.map_or(0, |(g,)| g) + 1;
        let seq = WFX_EVENT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let event_id = format!("{id_prefix}:{ts_us}:{pid}:{seq}", pid = std::process::id());
        let inserted = sqlx::query(
            "INSERT INTO events (generation, event_id, kind, payload, ts_us) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(next)
        .bind(&event_id)
        .bind(kind)
        .bind(payload)
        .bind(ts_us)
        .execute(&mut **tx)
        .await;
        let inserted = match inserted {
            Ok(r) => r,
            Err(_) => continue,
        };
        if inserted.rows_affected() == 0 {
            continue;
        }
        sqlx::query(
            "INSERT INTO generation_manifest (singleton, generation, updated_ts_us) \
             VALUES (1, ?1, ?2) ON CONFLICT(singleton) DO UPDATE SET \
             generation = excluded.generation, updated_ts_us = excluded.updated_ts_us",
        )
        .bind(next)
        .bind(ts_us)
        .execute(&mut **tx)
        .await?;
        return Ok(());
    }
    anyhow::bail!("retry budget spent")
}

/// Lifecycle state of one logical external effect (ADR-0032 Spec 5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Prepared,
    InFlight,
    Accepted,
    Settled,
    Failed,
    Unknown,
    ReconcilePending,
    Cancelled,
    /// bd-ynef: parked until a wake time, re-driven at T under the same key.
    /// Its own state rather than a reinterpretation of `Failed` or `Settled`:
    /// a park is INTERRUPTION, not a typed failure and not a completed step.
    /// Non-terminal, so a crash mid-park leaves a visible row to resume from
    /// rather than a wait that vanished.
    Parked,
}

impl EffectState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::InFlight => "in_flight",
            Self::Accepted => "accepted",
            Self::Settled => "settled",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
            Self::ReconcilePending => "reconcile_pending",
            Self::Cancelled => "cancelled",
            Self::Parked => "parked",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "prepared" => Ok(Self::Prepared),
            "in_flight" => Ok(Self::InFlight),
            "accepted" => Ok(Self::Accepted),
            "settled" => Ok(Self::Settled),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            "reconcile_pending" => Ok(Self::ReconcilePending),
            "cancelled" => Ok(Self::Cancelled),
            "parked" => Ok(Self::Parked),
            _ => Err(format!("unknown effect state {s:?}")),
        }
    }

    /// True for terminal effect claims (settled, proven failed, cancelled).
    /// `unknown` and `reconcile_pending` are explicitly nonterminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Settled | Self::Failed | Self::Cancelled)
    }

    /// bd-ynef: a parked effect is the one non-terminal state a re-drive may
    /// resume. It is NOT terminal (the step has not finished) and NOT a
    /// failure (nothing went wrong), which is what keeps park, success, and
    /// typed failure three distinct outcomes.
    pub const fn is_resumable(self) -> bool {
        matches!(self, Self::Parked)
    }
}

impl std::str::FromStr for EffectState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl std::fmt::Display for EffectState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether one effect-state transition is legal (ADR-0032 §5.2). In
/// particular: `unknown` enters only from in-flight/accepted uncertainty
/// and leaves only via reconciliation; terminal states never move; a
/// timeout never becomes absence (there is no transition to "nothing").
pub const fn can_transition(from: EffectState, to: EffectState) -> bool {
    use EffectState as S;
    matches!(
        (from, to),
        (S::Prepared, S::InFlight)
            | (S::Prepared, S::Failed)
            | (S::Prepared, S::Cancelled)
            | (S::InFlight, S::Accepted)
            | (S::InFlight, S::Settled)
            | (S::InFlight, S::Failed)
            | (S::InFlight, S::Unknown)
            | (S::InFlight, S::Cancelled)
            | (S::Accepted, S::Settled)
            | (S::Accepted, S::Failed)
            | (S::Accepted, S::Unknown)
            | (S::Accepted, S::Cancelled)
            | (S::Unknown, S::ReconcilePending)
            | (S::Unknown, S::Cancelled)
            | (S::ReconcilePending, S::Settled)
            | (S::ReconcilePending, S::Failed)
            | (S::ReconcilePending, S::Unknown)
            | (S::ReconcilePending, S::Cancelled)
    )
}

/// Workflow run status (ADR-0032 Spec 5.1 `workflow_runs` record).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Started,
    Running,
    Suspended,
    ReconcilePending,
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
    Unknown,
}

impl RunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::ReconcilePending => "reconcile_pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "started" => Ok(Self::Started),
            "running" => Ok(Self::Running),
            "suspended" => Ok(Self::Suspended),
            "reconcile_pending" => Ok(Self::ReconcilePending),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            "cancelled" => Ok(Self::Cancelled),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown run status {s:?}")),
        }
    }

    /// Terminal run claims. `reconcile_pending` is explicitly nonterminal.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Blocked | Self::Cancelled | Self::Unknown
        )
    }
}

impl std::str::FromStr for RunStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Terminal-publication state for one semantic terminal digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationState {
    Noted,
    Published,
    Failed,
}

impl PublicationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Noted => "noted",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "noted" => Ok(Self::Noted),
            "published" => Ok(Self::Published),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("unknown publication state {s:?}")),
        }
    }
}

/// Parked-promise kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingKind {
    Approval,
    Spawn,
    /// A time-wait park (`park_until`): resumes at its wake time under the
    /// same re-drive key. Neither an approval nor a spawn.
    Park,
}

impl PendingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::Spawn => "spawn",
            Self::Park => "park",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "approval" => Ok(Self::Approval),
            "spawn" => Ok(Self::Spawn),
            "park" => Ok(Self::Park),
            _ => Err(format!("unknown pending kind {s:?}")),
        }
    }
}

/// A logical workflow effect (current-state row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEffect {
    pub effect_key: String,
    pub run_id: String,
    pub project: Option<String>,
    pub workflow_digest: String,
    pub node_path: String,
    pub iteration_key: Option<String>,
    pub action_type: String,
    pub action_digest: String,
    pub redacted_action: Option<String>,
    pub state: EffectState,
    pub attempt: i64,
    pub attempt_id: Option<String>,
    pub claim_token: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub transport_corr: Option<String>,
    pub external_id: Option<String>,
    pub outcome_json: Option<String>,
    pub failure_json: Option<String>,
    pub reconcile_after: Option<i64>,
    pub retention_until: Option<i64>,
}

/// A new logical effect to prepare (intent before dispatch).
#[derive(Debug, Clone)]
pub struct NewEffect {
    pub effect_key: String,
    pub run_id: String,
    pub project: Option<String>,
    pub workflow_digest: String,
    pub node_path: String,
    pub iteration_key: Option<String>,
    pub action_type: String,
    pub action_digest: String,
    pub redacted_action: Option<serde_json::Value>,
    pub retention_until: Option<i64>,
}

/// Outcome of [`WorkflowEffectStore::prepare_effect`].
#[derive(Debug, Clone)]
pub struct PrepareOutcome {
    /// True when this call inserted the intent; false when the key already
    /// existed (duplicate prepare attaches to the existing row).
    pub created: bool,
    pub effect: WorkflowEffect,
}

/// A dispatch-attempt claim with fencing token.
#[derive(Debug, Clone)]
pub struct ClaimAttempt {
    pub attempt: i64,
    pub attempt_id: String,
    pub claim_token: String,
    pub lease_expires_at: i64,
    /// The token the claimant believes owns the effect. `None` claims only
    /// an unclaimed effect; a live lease under another token refuses.
    pub expected_claim_token: Option<String>,
}

/// One append-only dispatch-attempt evidence row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectAttempt {
    pub id: Option<i64>,
    pub effect_key: String,
    pub attempt: i64,
    pub attempt_id: String,
    pub claim_token: Option<String>,
    pub state: EffectState,
    pub evidence_json: Option<String>,
}

/// A new attempt row to record.
#[derive(Debug, Clone)]
pub struct NewAttempt {
    pub effect_key: String,
    pub attempt: i64,
    pub attempt_id: String,
    pub claim_token: Option<String>,
    pub state: EffectState,
    pub evidence: Option<serde_json::Value>,
}

/// A workflow run to create.
#[derive(Debug, Clone)]
pub struct NewRun {
    pub run_id: String,
    pub project: Option<String>,
    pub workflow: String,
    pub workflow_digest: String,
}

/// A workflow run (lifecycle row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRun {
    pub run_id: String,
    pub project: Option<String>,
    pub workflow: String,
    pub workflow_digest: String,
    pub status: RunStatus,
    pub started_at: i64,
    pub suspended_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub current_node: Option<String>,
    pub failed_node: Option<String>,
    pub terminal_digest: Option<String>,
}

/// A park to persist: one `Parked` effect row plus its pending resume row.
///
/// `workflow` is the pending table's workflow label; `workflow_digest` is
/// the effect row's digest. They differ the way they do on the two tables.
#[derive(Debug, Clone)]
pub struct NewPark {
    /// The re-drive key (`park:{run_id}:{node}:{until_ms}`): parking the
    /// same step to the same T twice lands on the same row.
    pub effect_key: String,
    pub run_id: String,
    pub promise_id: String,
    pub park_generation: i64,
    pub project: Option<String>,
    pub workflow: String,
    pub workflow_digest: String,
    pub node_path: String,
    /// What the wait is for (approval answer, spawn completion, time).
    /// The host knows this; the leaf-side intent does not.
    pub kind: PendingKind,
    /// The effective wake time when the parker knows T; `None` when the
    /// host indexes a promise whose T lives in the leaf.
    pub until_ms: Option<u64>,
    pub reason: Option<String>,
}

/// A parked pending entry to upsert.
#[derive(Debug, Clone)]
pub struct NewPending {
    pub run_id: String,
    pub promise_id: String,
    pub park_generation: i64,
    pub project: Option<String>,
    pub workflow: String,
    pub node_path: String,
    pub effect_key: Option<String>,
    pub kind: PendingKind,
    pub pending_payload: serde_json::Value,
    pub deadline_secs: Option<i64>,
}

/// A parked pending row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pending {
    pub run_id: String,
    pub promise_id: String,
    pub park_generation: i64,
    pub project: Option<String>,
    pub workflow: String,
    pub node_path: String,
    pub effect_key: Option<String>,
    pub kind: PendingKind,
    pub pending_payload: String,
    pub publication_state: Option<String>,
}

/// A terminal publication to note.
#[derive(Debug, Clone)]
pub struct TerminalPub {
    pub terminal_digest: String,
    pub run_id: String,
    pub status: String,
    pub node_path: Option<String>,
    pub evidence: Option<serde_json::Value>,
}

impl TerminalPub {
    pub fn new(
        run_id: impl Into<String>,
        digest: impl Into<String>,
        status: impl Into<String>,
        node: impl Into<String>,
    ) -> Self {
        Self {
            terminal_digest: digest.into(),
            run_id: run_id.into(),
            status: status.into(),
            node_path: Some(node.into()),
            evidence: None,
        }
    }
}

/// One terminal-event row (projection shape for the log fold).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TerminalRow {
    pub terminal_digest: String,
    pub run_id: String,
    pub status: String,
    pub node_path: Option<String>,
    pub evidence_json: Option<String>,
    pub publication_state: PublicationState,
    pub created_at: i64,
    pub updated_at: i64,
}

/// How [`WorkflowEffectStore::note_terminal_pub`] classified the attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TerminalOutcome {
    /// First publication of this digest — publish it.
    New,
    /// Same digest already recorded — no second semantic terminal.
    Duplicate,
    /// A different digest already terminal for this run — conflict
    /// requiring evidence, recorded alongside (never silently merged).
    Conflict { existing_digest: String },
}

/// How many rows one [`WorkflowEffectStore::prune_expired`] run removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PruneCounts {
    pub effects: i64,
    pub attempts: i64,
    pub terminals: i64,
    pub runs: i64,
}

/// Durable workflow-effect journal over the `workflow_*` SQLite tables.
#[derive(Debug, Clone)]
pub struct WorkflowEffectStore {
    pool: SqlitePool,
}

/// Folded workflow projection: one map per table plus skip count.
/// Tuples carry the winning event's clock for timestamp restoration.
#[derive(Debug, Default)]
pub struct WfxFold {
    pub effects: BTreeMap<String, (WorkflowEffect, i64)>,
    pub attempts: BTreeMap<i64, (EffectAttempt, i64)>,
    pub runs: BTreeMap<String, (WorkflowRun, i64)>,
    pub pending: BTreeMap<String, (Pending, i64)>,
    pub terminals: BTreeMap<String, (TerminalRow, i64)>,
    pub skipped: u64,
}

impl WfxFold {
    fn remove(&mut self, table: &str, key: &str) {
        match table {
            "workflow_effects" => {
                self.effects.remove(key);
            }
            "workflow_effect_attempts" => {
                if let Ok(id) = key.parse::<i64>() {
                    self.attempts.remove(&id);
                }
            }
            "workflow_runs" => {
                self.runs.remove(key);
            }
            "workflow_pending" => {
                self.pending.remove(key);
            }
            "workflow_terminal_events" => {
                self.terminals.remove(key);
            }
            _ => {}
        }
    }

    /// Insert one row payload; `false` when table/row won't parse.
    fn insert(&mut self, table: &str, key: &str, row: &serde_json::Value, ts_us: i64) -> bool {
        match table {
            "workflow_effects" => match serde_json::from_value::<WorkflowEffect>(row.clone()) {
                Ok(r) => {
                    self.effects.insert(key.to_string(), (r, ts_us));
                    true
                }
                Err(_) => false,
            },
            "workflow_effect_attempts" => {
                match serde_json::from_value::<EffectAttempt>(row.clone()) {
                    Ok(r) => {
                        // Logged attempts always carry their row id (post-insert
                        // read-back); 0 is unreachable from our writer.
                        self.attempts.insert(r.id.unwrap_or(0), (r, ts_us));
                        true
                    }
                    Err(_) => false,
                }
            }
            "workflow_runs" => match serde_json::from_value::<WorkflowRun>(row.clone()) {
                Ok(r) => {
                    self.runs.insert(key.to_string(), (r, ts_us));
                    true
                }
                Err(_) => false,
            },
            "workflow_pending" => match serde_json::from_value::<Pending>(row.clone()) {
                Ok(r) => {
                    self.pending.insert(key.to_string(), (r, ts_us));
                    true
                }
                Err(_) => false,
            },
            "workflow_terminal_events" => {
                match serde_json::from_value::<TerminalRow>(row.clone()) {
                    Ok(r) => {
                        self.terminals.insert(key.to_string(), (r, ts_us));
                        true
                    }
                    Err(_) => false,
                }
            }
            _ => false,
        }
    }
}
impl WorkflowEffectStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Save one session delivery-cursor snapshot (bd-aqvg): upsert by
    /// session. in_flight never reaches here (snapshot drops it); ids
    /// unconfirmed at crash stay retryable instead of restored-delivered.
    pub async fn save_delivery_cursor(
        &self,
        session: &str,
        delivered_json: &str,
        cursor: i64,
    ) -> anyhow::Result<()> {
        let now = now_us();
        sqlx::query(
            "INSERT INTO delivery_cursors (session, delivered_json, cursor, updated_us) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(session) DO UPDATE SET \
             delivered_json = excluded.delivered_json, cursor = excluded.cursor, \
             updated_us = excluded.updated_us",
        )
        .bind(session)
        .bind(delivered_json)
        .bind(cursor)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load all session cursors for boot restore (bd-aqvg).
    pub async fn load_delivery_cursors(&self) -> anyhow::Result<Vec<(String, String, i64)>> {
        let rows = sqlx::query_as::<_, (String, String, i64)>(
            "SELECT session, delivered_json, cursor FROM delivery_cursors",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Prune sessions idle longer than the bound (bd-aqvg): whole-session
    /// rows only. A pruned session re-delivers on next sighting — a bounded
    /// duplicate, chosen over unbounded table growth. Callers pass 90d in us.
    pub async fn prune_delivery_cursors(&self, older_than_us: i64) -> anyhow::Result<u64> {
        let r = sqlx::query("DELETE FROM delivery_cursors WHERE updated_us < ?")
            .bind(older_than_us)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected())
    }

    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Re-read one effect row inside the committing tx and append its
    /// `wfx.row` event (dual-write read-back; keeps payloads exact).
    async fn log_effect_row_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        effect_key: &str,
        ts_us: i64,
    ) -> anyhow::Result<()> {
        let row = sqlx::query(
            "SELECT effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
                    action_type, action_digest, redacted_action, state, attempt, attempt_id, \
                    claim_token, lease_expires_at, transport_corr, external_id, outcome_json, \
                    failure_json, reconcile_after, retention_until \
             FROM workflow_effects WHERE effect_key = ?1",
        )
        .bind(effect_key)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("effect vanished mid-write"))?;
        let payload = serde_json::to_value(self.map_effect(&row)?)?;
        log_wfx_event_in_tx(tx, "workflow_effects", effect_key, &payload, ts_us).await
    }

    /// Re-read one run row inside the committing tx and append its event.
    async fn log_run_row_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run_id: &str,
        ts_us: i64,
    ) -> anyhow::Result<()> {
        let row = sqlx::query(
            "SELECT run_id, project, workflow, workflow_digest, status, started_at, \
                    suspended_at, finished_at, current_node, failed_node, terminal_digest \
             FROM workflow_runs WHERE run_id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("run vanished mid-write"))?;
        let payload = serde_json::to_value(self.map_run(&row)?)?;
        log_wfx_event_in_tx(tx, "workflow_runs", run_id, &payload, ts_us).await
    }

    /// Re-read one pending row inside the committing tx and append its event.
    async fn log_pending_row_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        run_id: &str,
        promise_id: &str,
        park_generation: i64,
        ts_us: i64,
    ) -> anyhow::Result<()> {
        let row = sqlx::query(
            "SELECT run_id, promise_id, park_generation, project, workflow, node_path, \
                    effect_key, kind, pending_payload, publication_state \
             FROM workflow_pending WHERE run_id = ?1 AND promise_id = ?2 AND park_generation = ?3",
        )
        .bind(run_id)
        .bind(promise_id)
        .bind(park_generation)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("pending vanished mid-write"))?;
        let payload = serde_json::to_value(self.map_pending(&row)?)?;
        let key = format!("{run_id}:{promise_id}:{park_generation}");
        log_wfx_event_in_tx(tx, "workflow_pending", &key, &payload, ts_us).await
    }

    /// Re-read one terminal row inside the committing tx and append its event.
    async fn log_terminal_row_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        terminal_digest: &str,
        ts_us: i64,
    ) -> anyhow::Result<()> {
        let row = sqlx::query(
            "SELECT terminal_digest, run_id, status, node_path, evidence_json, publication_state, \
                    created_at, updated_at \
             FROM workflow_terminal_events WHERE terminal_digest = ?1",
        )
        .bind(terminal_digest)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("terminal vanished mid-write"))?;
        let payload = serde_json::to_value(self.map_terminal(&row)?)?;
        log_wfx_event_in_tx(
            tx,
            "workflow_terminal_events",
            terminal_digest,
            &payload,
            ts_us,
        )
        .await
    }

    /// True when a table exists (migration witness).
    pub async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")
                .bind(table)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.is_some())
    }

    /// Prepare one logical-effect intent. Idempotent on `effect_key`: a
    /// duplicate prepare returns the existing row with `created: false` and
    /// never starts a second logical effect.
    pub async fn prepare_effect(&self, effect: NewEffect) -> anyhow::Result<PrepareOutcome> {
        let now = now_us();
        let redacted = effect.redacted_action.map(|v| bound_evidence(&v));
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "INSERT INTO workflow_effects \
             (effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
              action_type, action_digest, redacted_action, state, attempt, \
              retention_until, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'prepared', 1, ?10, ?11, ?12) \
             ON CONFLICT (effect_key) DO NOTHING",
        )
        .bind(&effect.effect_key)
        .bind(&effect.run_id)
        .bind(&effect.project)
        .bind(&effect.workflow_digest)
        .bind(&effect.node_path)
        .bind(&effect.iteration_key)
        .bind(&effect.action_type)
        .bind(&effect.action_digest)
        .bind(&redacted)
        .bind(effect.retention_until)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let created = res.rows_affected() > 0;
        let row = sqlx::query(
            "SELECT effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
                    action_type, action_digest, redacted_action, state, attempt, attempt_id, \
                    claim_token, lease_expires_at, transport_corr, external_id, outcome_json, \
                    failure_json, reconcile_after, retention_until \
             FROM workflow_effects WHERE effect_key = ?1",
        )
        .bind(&effect.effect_key)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("effect vanished after prepare"))?;
        let row = self.map_effect(&row)?;
        let payload = serde_json::to_value(&row)?;
        log_wfx_event_in_tx(
            &mut tx,
            "workflow_effects",
            &effect.effect_key,
            &payload,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(PrepareOutcome {
            created,
            effect: row,
        })
    }

    /// Persist a park: the `Parked` effect row and its `workflow_pending
    /// resume row, atomically. The effect row is INSERTED, never
    /// transitioned: `can_transition` has no edge into `Parked` on either
    /// side of the kernel port (framework law), so `transition_effect`
    /// can never write this row. A second park under the same effect key
    /// attaches to the existing row (`created: false`), which is what
    /// makes re-parking idempotent rather than a duplicate wait.
    pub async fn park_effect(&self, park: NewPark) -> anyhow::Result<PrepareOutcome> {
        let now = now_us();
        let reason = park
            .reason
            .clone()
            .map(|r| bound_evidence(&serde_json::json!(r)));
        let pending_payload = bound_evidence(&serde_json::json!({
            "until_ms": park.until_ms,
            "reason": park.reason,
            "effect_key": park.effect_key,
        }));
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "INSERT INTO workflow_effects \
             (effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
              action_type, action_digest, redacted_action, state, attempt, \
              retention_until, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, 'park', ?6, ?7, 'parked', 1, NULL, ?8, ?9) \
             ON CONFLICT (effect_key) DO NOTHING",
        )
        .bind(&park.effect_key)
        .bind(&park.run_id)
        .bind(&park.project)
        .bind(&park.workflow_digest)
        .bind(&park.node_path)
        .bind(
            park.until_ms
                .map(|t| t.to_string())
                .unwrap_or_else(|| "unknown".into()),
        )
        .bind(&reason)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        let created = res.rows_affected() > 0;
        let row = sqlx::query(
            "SELECT effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
                    action_type, action_digest, redacted_action, state, attempt, attempt_id, \
                    claim_token, lease_expires_at, transport_corr, external_id, outcome_json, \
                    failure_json, reconcile_after, retention_until \
             FROM workflow_effects WHERE effect_key = ?1",
        )
        .bind(&park.effect_key)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("effect vanished after park"))?;
        let row = self.map_effect(&row)?;
        let row_json = serde_json::to_value(&row)?;
        log_wfx_event_in_tx(
            &mut tx,
            "workflow_effects",
            &park.effect_key,
            &row_json,
            now,
        )
        .await?;
        // The resume index commits with the effect row: a crash between
        // them would leave a Parked row no supervisor lists.
        let presid = sqlx::query(
            "INSERT INTO workflow_pending \
             (run_id, promise_id, park_generation, project, workflow, node_path, effect_key, \
              kind, pending_payload, deadline, publication_state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'noted', ?11, ?12) \
             ON CONFLICT (run_id, promise_id, park_generation) DO NOTHING",
        )
        .bind(&park.run_id)
        .bind(&park.promise_id)
        .bind(park.park_generation)
        .bind(&park.project)
        .bind(&park.workflow)
        .bind(&park.node_path)
        .bind(&park.effect_key)
        .bind(park.kind.as_str())
        .bind(&pending_payload)
        .bind(park.until_ms.map(|t| (t / 1000) as i64))
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if presid.rows_affected() > 0 {
            self.log_pending_row_in_tx(
                &mut tx,
                &park.run_id,
                &park.promise_id,
                park.park_generation,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(PrepareOutcome {
            created,
            effect: row,
        })
    }

    /// Fold key for one pending row (unit separators: unambiguous even
    /// when ids contain colons).
    pub fn pending_fold_key(run_id: &str, promise_id: &str, park_generation: i64) -> String {
        format!("{run_id}\u{1f}{promise_id}\u{1f}{park_generation}")
    }

    /// Wave-4 projection fold (ADR-0035 Spec 4): the workflow tables as the
    /// event log sees them. `wfx.row` last-write-wins per (table, key),
    /// `wfx.tombstone` removes. Unparseable rows are skipped (rebuild and
    /// verify surface the counts, never silent).
    /// ponytail: full fold per read; effect tables are run-scoped (hundreds
    /// of rows). A per-table index if this ever serves a hot path.
    pub async fn fold_projected(&self) -> anyhow::Result<WfxFold> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let mut fold = WfxFold::default();
        for event in log.read_since(0).await? {
            if event.kind != "wfx.row" && event.kind != "wfx.tombstone" {
                continue;
            }
            let payload: serde_json::Value = match serde_json::from_str(&event.payload) {
                Ok(v) => v,
                Err(_) => {
                    fold.skipped += 1;
                    continue;
                }
            };
            let Some(table) = payload.get("table").and_then(|t| t.as_str()) else {
                fold.skipped += 1;
                continue;
            };
            let Some(key) = payload.get("key").and_then(|k| k.as_str()) else {
                fold.skipped += 1;
                continue;
            };
            if event.kind == "wfx.tombstone" {
                fold.remove(table, key);
                continue;
            }
            let Some(row) = payload.get("row") else {
                fold.skipped += 1;
                continue;
            };
            let ts_us = payload
                .get("ts_us")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            if !fold.insert(table, key, row, ts_us) {
                fold.skipped += 1;
            }
        }
        // Attempts pruned as orphans carry no tombstone: drop attempts whose
        // effect is absent from the fold (matches prune semantics exactly).
        fold.attempts
            .retain(|_, (a, _)| fold.effects.contains_key(&a.effect_key));
        Ok(fold)
    }

    /// Table-only row restores (rebuild path: no events, no policy).
    /// `event_ts_us` stamps created/updated from the winning event's clock
    /// (the same clock the live write used, so rebuilds reproduce stamps).
    pub async fn restore_attempt_row(
        &self,
        row: &EffectAttempt,
        event_ts_us: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO workflow_effect_attempts \
             (id, effect_key, attempt, attempt_id, claim_token, state, evidence_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT (id) DO UPDATE SET \
               effect_key = excluded.effect_key, attempt = excluded.attempt, \
               attempt_id = excluded.attempt_id, claim_token = excluded.claim_token, \
               state = excluded.state, evidence_json = excluded.evidence_json",
        )
        .bind(row.id)
        .bind(&row.effect_key)
        .bind(row.attempt)
        .bind(&row.attempt_id)
        .bind(&row.claim_token)
        .bind(row.state.as_str())
        .bind(&row.evidence_json)
        .bind(event_ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn restore_run_row(&self, row: &WorkflowRun, event_ts_us: i64) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO workflow_runs \
             (run_id, project, workflow, workflow_digest, status, started_at, \
              suspended_at, finished_at, current_node, failed_node, terminal_digest, \
              created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
             ON CONFLICT (run_id) DO UPDATE SET \
               project = excluded.project, workflow = excluded.workflow, \
               workflow_digest = excluded.workflow_digest, status = excluded.status, \
               started_at = excluded.started_at, suspended_at = excluded.suspended_at, \
               finished_at = excluded.finished_at, current_node = excluded.current_node, \
               failed_node = excluded.failed_node, terminal_digest = excluded.terminal_digest, \
               updated_at = excluded.updated_at",
        )
        .bind(&row.run_id)
        .bind(&row.project)
        .bind(&row.workflow)
        .bind(&row.workflow_digest)
        .bind(row.status.as_str())
        .bind(row.started_at)
        .bind(row.suspended_at)
        .bind(row.finished_at)
        .bind(&row.current_node)
        .bind(&row.failed_node)
        .bind(&row.terminal_digest)
        .bind(event_ts_us)
        .bind(event_ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn restore_pending_row(&self, row: &Pending, event_ts_us: i64) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO workflow_pending \
             (run_id, promise_id, park_generation, project, workflow, node_path, effect_key, \
              kind, pending_payload, deadline, publication_state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
             ON CONFLICT (run_id, promise_id, park_generation) DO UPDATE SET \
               project = excluded.project, workflow = excluded.workflow, \
               node_path = excluded.node_path, effect_key = excluded.effect_key, \
               kind = excluded.kind, pending_payload = excluded.pending_payload, \
               deadline = excluded.deadline, publication_state = excluded.publication_state, \
               updated_at = excluded.updated_at",
        )
        .bind(&row.run_id)
        .bind(&row.promise_id)
        .bind(row.park_generation)
        .bind(&row.project)
        .bind(&row.workflow)
        .bind(&row.node_path)
        .bind(&row.effect_key)
        .bind(row.kind.as_str())
        .bind(&row.pending_payload)
        .bind(None::<i64>)
        .bind(&row.publication_state)
        .bind(event_ts_us)
        .bind(event_ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn restore_terminal_row(
        &self,
        row: &TerminalRow,
        event_ts_us: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO workflow_terminal_events \
             (terminal_digest, run_id, status, node_path, evidence_json, publication_state, \
              created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT (terminal_digest) DO UPDATE SET \
               run_id = excluded.run_id, status = excluded.status, \
               node_path = excluded.node_path, evidence_json = excluded.evidence_json, \
               publication_state = excluded.publication_state, \
               updated_at = excluded.updated_at",
        )
        .bind(&row.terminal_digest)
        .bind(&row.run_id)
        .bind(&row.status)
        .bind(&row.node_path)
        .bind(&row.evidence_json)
        .bind(row.publication_state.as_str())
        .bind(event_ts_us)
        .bind(event_ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Table-only row restore (rebuild path: no events, no policy).
    /// `event_ts_us` stamps created/updated (the mutation clock the event
    /// carries); on conflict the stored created_at is preserved.
    pub async fn restore_effect_row(
        &self,
        row: &WorkflowEffect,
        event_ts_us: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO workflow_effects \
             (effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
              action_type, action_digest, redacted_action, state, attempt, attempt_id, \
              claim_token, lease_expires_at, transport_corr, external_id, outcome_json, \
              failure_json, reconcile_after, retention_until, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, \
                     ?16, ?17, ?18, ?19, ?20, ?21, ?22) \
             ON CONFLICT (effect_key) DO UPDATE SET \
               run_id = excluded.run_id, project = excluded.project, \
               workflow_digest = excluded.workflow_digest, node_path = excluded.node_path, \
               iteration_key = excluded.iteration_key, action_type = excluded.action_type, \
               action_digest = excluded.action_digest, redacted_action = excluded.redacted_action, \
               state = excluded.state, attempt = excluded.attempt, attempt_id = excluded.attempt_id, \
               claim_token = excluded.claim_token, lease_expires_at = excluded.lease_expires_at, \
               transport_corr = excluded.transport_corr, external_id = excluded.external_id, \
               outcome_json = excluded.outcome_json, failure_json = excluded.failure_json, \
               reconcile_after = excluded.reconcile_after, retention_until = excluded.retention_until, \
               created_at = excluded.created_at, updated_at = excluded.updated_at",
        )
        .bind(&row.effect_key)
        .bind(&row.run_id)
        .bind(&row.project)
        .bind(&row.workflow_digest)
        .bind(&row.node_path)
        .bind(&row.iteration_key)
        .bind(&row.action_type)
        .bind(&row.action_digest)
        .bind(&row.redacted_action)
        .bind(row.state.as_str())
        .bind(row.attempt)
        .bind(&row.attempt_id)
        .bind(&row.claim_token)
        .bind(row.lease_expires_at)
        .bind(&row.transport_corr)
        .bind(&row.external_id)
        .bind(&row.outcome_json)
        .bind(&row.failure_json)
        .bind(row.reconcile_after)
        .bind(row.retention_until)
        .bind(event_ts_us)
        .bind(event_ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    /// Load one effect by key — served from the event projection
    /// (wave-4 reader switch). The table stays dual-written as the tripwire.
    pub async fn get_effect(&self, effect_key: &str) -> anyhow::Result<Option<WorkflowEffect>> {
        Ok(self
            .fold_projected()
            .await?
            .effects
            .remove(effect_key)
            .map(|(row, _)| row))
    }

    fn map_run(&self, r: &sqlx::sqlite::SqliteRow) -> anyhow::Result<WorkflowRun> {
        let status: String = r.get("status");
        Ok(WorkflowRun {
            run_id: r.get("run_id"),
            project: r.get("project"),
            workflow: r.get("workflow"),
            workflow_digest: r.get("workflow_digest"),
            status: RunStatus::parse(&status).map_err(|e| anyhow::anyhow!("{e}"))?,
            started_at: r.get("started_at"),
            suspended_at: r.get("suspended_at"),
            finished_at: r.get("finished_at"),
            current_node: r.get("current_node"),
            failed_node: r.get("failed_node"),
            terminal_digest: r.get("terminal_digest"),
        })
    }

    fn map_pending(&self, r: &sqlx::sqlite::SqliteRow) -> anyhow::Result<Pending> {
        let kind: String = r.get("kind");
        Ok(Pending {
            run_id: r.get("run_id"),
            promise_id: r.get("promise_id"),
            park_generation: r.get("park_generation"),
            project: r.get("project"),
            workflow: r.get("workflow"),
            node_path: r.get("node_path"),
            effect_key: r.get("effect_key"),
            kind: PendingKind::parse(&kind).map_err(|e| anyhow::anyhow!("{e}"))?,
            pending_payload: r.get("pending_payload"),
            publication_state: r.get("publication_state"),
        })
    }

    fn map_terminal(&self, r: &sqlx::sqlite::SqliteRow) -> anyhow::Result<TerminalRow> {
        let publication_state: String = r.get("publication_state");
        Ok(TerminalRow {
            terminal_digest: r.get("terminal_digest"),
            run_id: r.get("run_id"),
            status: r.get("status"),
            node_path: r.get("node_path"),
            evidence_json: r.get("evidence_json"),
            publication_state: PublicationState::parse(&publication_state)
                .map_err(|e| anyhow::anyhow!("{e}"))?,
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
    }

    fn map_effect(&self, r: &sqlx::sqlite::SqliteRow) -> anyhow::Result<WorkflowEffect> {
        let state: String = r.get("state");
        Ok(WorkflowEffect {
            effect_key: r.get("effect_key"),
            run_id: r.get("run_id"),
            project: r.get("project"),
            workflow_digest: r.get("workflow_digest"),
            node_path: r.get("node_path"),
            iteration_key: r.get("iteration_key"),
            action_type: r.get("action_type"),
            action_digest: r.get("action_digest"),
            redacted_action: r.get("redacted_action"),
            state: EffectState::parse(&state).map_err(|e| anyhow::anyhow!("{e}"))?,
            attempt: r.get("attempt"),
            attempt_id: r.get("attempt_id"),
            claim_token: r.get("claim_token"),
            lease_expires_at: r.get("lease_expires_at"),
            transport_corr: r.get("transport_corr"),
            external_id: r.get("external_id"),
            outcome_json: r.get("outcome_json"),
            failure_json: r.get("failure_json"),
            reconcile_after: r.get("reconcile_after"),
            retention_until: r.get("retention_until"),
        })
    }

    /// Claim one dispatch attempt with fencing. Succeeds (and moves the
    /// effect to `in_flight`) only when the effect is unclaimed, the
    /// expected token matches the live owner, or the live lease expired
    /// (crash recovery). A stale token against a live lease returns `false`
    /// — a stale owner can never settle or steal the attempt.
    pub async fn claim_attempt(
        &self,
        effect_key: &str,
        claim: ClaimAttempt,
    ) -> anyhow::Result<bool> {
        let Some(current) = self.get_effect(effect_key).await? else {
            anyhow::bail!("unknown effect {effect_key:?}");
        };
        if current.state.is_terminal() {
            return Ok(false);
        }
        let now = now_us();
        let live = match (&current.claim_token, current.lease_expires_at) {
            (Some(_), Some(expires)) => expires > now,
            (Some(_), None) => true,
            (None, _) => false,
        };
        if live && claim.expected_claim_token.as_deref() != current.claim_token.as_deref() {
            return Ok(false);
        }
        let updated = if let Some(owned) = &current.claim_token {
            let mut tx = self.pool.begin().await?;
            let updated = sqlx::query(
                "UPDATE workflow_effects SET attempt = ?1, attempt_id = ?2, claim_token = ?3, \
                        lease_expires_at = ?4, state = 'in_flight', updated_at = ?5 \
                 WHERE effect_key = ?6 AND claim_token = ?7",
            )
            .bind(claim.attempt)
            .bind(&claim.attempt_id)
            .bind(&claim.claim_token)
            .bind(claim.lease_expires_at)
            .bind(now)
            .bind(effect_key)
            .bind(owned)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if updated > 0 {
                self.log_effect_row_in_tx(&mut tx, effect_key, now).await?;
            }
            tx.commit().await?;
            updated
        } else {
            let mut tx = self.pool.begin().await?;
            let updated = sqlx::query(
                "UPDATE workflow_effects SET attempt = ?1, attempt_id = ?2, claim_token = ?3, \
                        lease_expires_at = ?4, state = 'in_flight', updated_at = ?5 \
                 WHERE effect_key = ?6 AND claim_token IS NULL",
            )
            .bind(claim.attempt)
            .bind(&claim.attempt_id)
            .bind(&claim.claim_token)
            .bind(claim.lease_expires_at)
            .bind(now)
            .bind(effect_key)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if updated > 0 {
                self.log_effect_row_in_tx(&mut tx, effect_key, now).await?;
            }
            tx.commit().await?;
            updated
        };
        Ok(updated > 0)
    }

    /// Transition one effect's state with bounded evidence. Illegal
    /// transitions (see [`can_transition`]) and moves out of terminal
    /// states return `false` and change nothing.
    pub async fn transition_effect(
        &self,
        effect_key: &str,
        to: EffectState,
        failure: Option<serde_json::Value>,
        outcome: Option<serde_json::Value>,
    ) -> anyhow::Result<bool> {
        let Some(current) = self.get_effect(effect_key).await? else {
            anyhow::bail!("unknown effect {effect_key:?}");
        };
        if !can_transition(current.state, to) {
            return Ok(false);
        }
        let failure_json = failure.as_ref().map(bound_evidence);
        let outcome_json = outcome.as_ref().map(bound_evidence);
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE workflow_effects SET state = ?1, failure_json = COALESCE(?2, failure_json), \
                    outcome_json = COALESCE(?3, outcome_json), lease_expires_at = NULL, \
                    updated_at = ?4 \
             WHERE effect_key = ?5 AND state = ?6",
        )
        .bind(to.as_str())
        .bind(&failure_json)
        .bind(&outcome_json)
        .bind(now)
        .bind(effect_key)
        .bind(current.state.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated > 0 {
            self.log_effect_row_in_tx(&mut tx, effect_key, now).await?;
        }
        tx.commit().await?;
        Ok(updated > 0)
    }

    /// Link one effect to its mailbox transport correlation (ADR-0032
    /// Spec 5.4): for a workflow-originated mailbox effect the sender sets
    /// `corr = effect_key` as a deliberate projection. The link is one
    /// column, never a second journal — the receipts table stays untouched.
    /// Returns `false` when the effect is unknown.
    pub async fn link_transport_corr(
        &self,
        effect_key: &str,
        transport_corr: &str,
    ) -> anyhow::Result<bool> {
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE workflow_effects SET transport_corr = ?1, updated_at = ?2 \
             WHERE effect_key = ?3",
        )
        .bind(transport_corr)
        .bind(now)
        .bind(effect_key)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            self.log_effect_row_in_tx(&mut tx, effect_key, now).await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    /// Settle (or fail) one effect under fencing: the presented token must
    /// equal the live claim token, and the transition must be legal (see
    /// [`can_transition`]). A stale owner fails closed with `false` and
    /// changes nothing — uncertainty is preserved, never overwritten.
    pub async fn settle_with_fence(
        &self,
        effect_key: &str,
        presented_token: &str,
        to: EffectState,
        failure: Option<serde_json::Value>,
        outcome: Option<serde_json::Value>,
    ) -> anyhow::Result<bool> {
        let Some(current) = self.get_effect(effect_key).await? else {
            anyhow::bail!("unknown effect {effect_key:?}");
        };
        if presented_token.is_empty() || current.claim_token.as_deref() != Some(presented_token) {
            return Ok(false);
        }
        if !can_transition(current.state, to) {
            return Ok(false);
        }
        let failure_json = failure.as_ref().map(bound_evidence);
        let outcome_json = outcome.as_ref().map(bound_evidence);
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE workflow_effects SET state = ?1, failure_json = COALESCE(?2, failure_json), \
                    outcome_json = COALESCE(?3, outcome_json), lease_expires_at = NULL, \
                    updated_at = ?4 \
             WHERE effect_key = ?5 AND claim_token = ?6 AND state = ?7",
        )
        .bind(to.as_str())
        .bind(&failure_json)
        .bind(&outcome_json)
        .bind(now)
        .bind(effect_key)
        .bind(presented_token)
        .bind(current.state.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated > 0 {
            self.log_effect_row_in_tx(&mut tx, effect_key, now).await?;
        }
        tx.commit().await?;
        Ok(updated > 0)
    }

    /// Record one append-only attempt-evidence row.
    pub async fn record_attempt(&self, attempt: NewAttempt) -> anyhow::Result<i64> {
        let evidence = attempt.evidence.as_ref().map(bound_evidence);
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let rowid = sqlx::query(
            "INSERT INTO workflow_effect_attempts \
             (effect_key, attempt, attempt_id, claim_token, state, evidence_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(&attempt.effect_key)
        .bind(attempt.attempt)
        .bind(&attempt.attempt_id)
        .bind(&attempt.claim_token)
        .bind(attempt.state.as_str())
        .bind(&evidence)
        .bind(now)
        .execute(&mut *tx)
        .await?
        .last_insert_rowid();
        let row = sqlx::query(
            "SELECT id, effect_key, attempt, attempt_id, claim_token, state, evidence_json \
             FROM workflow_effect_attempts WHERE id = ?1",
        )
        .bind(rowid)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| anyhow::anyhow!("attempt vanished after record"))?;
        let state: String = row.get("state");
        let payload = serde_json::to_value(EffectAttempt {
            id: row.get("id"),
            effect_key: row.get("effect_key"),
            attempt: row.get("attempt"),
            attempt_id: row.get("attempt_id"),
            claim_token: row.get("claim_token"),
            state: EffectState::parse(&state).map_err(|e| anyhow::anyhow!("{e}"))?,
            evidence_json: row.get("evidence_json"),
        })?;
        log_wfx_event_in_tx(
            &mut tx,
            "workflow_effect_attempts",
            &format!("attempt:{rowid}"),
            &payload,
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(rowid)
    }

    /// Attempt evidence for one effect, oldest first — from the projection.
    pub async fn list_attempts(&self, effect_key: &str) -> anyhow::Result<Vec<EffectAttempt>> {
        let mut out: Vec<EffectAttempt> = self
            .fold_projected()
            .await?
            .attempts
            .into_values()
            .filter(|(row, _)| row.effect_key == effect_key)
            .map(|(row, _)| row)
            .collect();
        out.sort_by_key(|a| a.id);
        Ok(out)
    }

    /// Create one run. Idempotent on `run_id`: duplicates return `false`.
    pub async fn create_run(&self, run: NewRun) -> anyhow::Result<bool> {
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "INSERT INTO workflow_runs \
             (run_id, project, workflow, workflow_digest, status, started_at, \
              created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, 'started', ?5, ?6, ?7) \
             ON CONFLICT (run_id) DO NOTHING",
        )
        .bind(&run.run_id)
        .bind(&run.project)
        .bind(&run.workflow)
        .bind(&run.workflow_digest)
        .bind(now)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            self.log_run_row_in_tx(&mut tx, &run.run_id, now).await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    /// Load one run by id — served from the event projection.
    pub async fn get_run(&self, run_id: &str) -> anyhow::Result<Option<WorkflowRun>> {
        Ok(self
            .fold_projected()
            .await?
            .runs
            .remove(run_id)
            .map(|(row, _)| row))
    }

    /// Compare-and-swap one run's status: transitions only from the
    /// expected current status (terminal rows never move — the `WHERE`
    /// also refuses to leave a terminal status). Returns `false` when the
    /// current status differs.
    pub async fn cas_run_status(
        &self,
        run_id: &str,
        expected: RunStatus,
        to: RunStatus,
    ) -> anyhow::Result<bool> {
        if expected.is_terminal() {
            return Ok(false);
        }
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let updated = sqlx::query(
            "UPDATE workflow_runs SET status = ?1, \
                    suspended_at = CASE WHEN ?1 = 'suspended' THEN ?2 ELSE suspended_at END, \
                    finished_at = CASE WHEN ?1 IN \
                        ('succeeded','failed','blocked','cancelled','unknown') \
                        THEN ?2 ELSE finished_at END, \
                    updated_at = ?2 \
             WHERE run_id = ?3 AND status = ?4",
        )
        .bind(to.as_str())
        .bind(now)
        .bind(run_id)
        .bind(expected.as_str())
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if updated > 0 {
            self.log_run_row_in_tx(&mut tx, run_id, now).await?;
        }
        tx.commit().await?;
        Ok(updated > 0)
    }

    /// Upsert one parked pending entry. Idempotent on
    /// `(run_id, promise_id, park_generation)`: duplicates return `false`.
    pub async fn upsert_pending(&self, pending: NewPending) -> anyhow::Result<bool> {
        let now = now_us();
        let payload = bound_evidence(&pending.pending_payload);
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "INSERT INTO workflow_pending \
             (run_id, promise_id, park_generation, project, workflow, node_path, effect_key, \
              kind, pending_payload, deadline, publication_state, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13) \
             ON CONFLICT (run_id, promise_id, park_generation) DO NOTHING",
        )
        .bind(&pending.run_id)
        .bind(&pending.promise_id)
        .bind(pending.park_generation)
        .bind(&pending.project)
        .bind(&pending.workflow)
        .bind(&pending.node_path)
        .bind(&pending.effect_key)
        .bind(pending.kind.as_str())
        .bind(&payload)
        .bind(pending.deadline_secs)
        .bind(PublicationState::Noted.as_str())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            self.log_pending_row_in_tx(
                &mut tx,
                &pending.run_id,
                &pending.promise_id,
                pending.park_generation,
                now,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    /// Load one pending entry by full identity — from the projection.
    pub async fn get_pending(
        &self,
        run_id: &str,
        promise_id: &str,
        park_generation: i64,
    ) -> anyhow::Result<Option<Pending>> {
        Ok(self
            .fold_projected()
            .await?
            .pending
            .remove(&Self::pending_fold_key(run_id, promise_id, park_generation))
            .map(|(row, _)| row))
    }

    /// All pending entries for one run, in park order — from the projection.
    pub async fn list_pending(&self, run_id: &str) -> anyhow::Result<Vec<Pending>> {
        let mut out: Vec<Pending> = self
            .fold_projected()
            .await?
            .pending
            .into_values()
            .filter(|(row, _)| row.run_id == run_id)
            .map(|(row, _)| row)
            .collect();
        out.sort_by_key(|r| r.park_generation);
        Ok(out)
    }

    /// Delete one pending entry (on validated resolution). Returns `false`
    /// when no such entry existed.
    pub async fn delete_pending(
        &self,
        run_id: &str,
        promise_id: &str,
        park_generation: i64,
    ) -> anyhow::Result<bool> {
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "DELETE FROM workflow_pending \
             WHERE run_id = ?1 AND promise_id = ?2 AND park_generation = ?3",
        )
        .bind(run_id)
        .bind(promise_id)
        .bind(park_generation)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            let key = format!("{run_id}:{promise_id}:{park_generation}");
            log_wfx_tombstone_in_tx(&mut tx, "workflow_pending", &key, now).await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    /// Note one terminal publication: first digest for a run is `New`, the
    /// same digest again is `Duplicate` (no second semantic terminal), and
    /// a different digest for an already-terminal run is `Conflict`.
    pub async fn note_terminal_pub(
        &self,
        terminal: TerminalPub,
    ) -> anyhow::Result<TerminalOutcome> {
        let existing: Option<(String,)> = sqlx::query_as(
            "SELECT terminal_digest FROM workflow_terminal_events WHERE terminal_digest = ?1",
        )
        .bind(&terminal.terminal_digest)
        .fetch_optional(&self.pool)
        .await?;
        if existing.is_some() {
            return Ok(TerminalOutcome::Duplicate);
        }
        let rival: Option<(String,)> = sqlx::query_as(
            "SELECT terminal_digest FROM workflow_terminal_events \
             WHERE run_id = ?1 AND terminal_digest != ?2 LIMIT 1",
        )
        .bind(&terminal.run_id)
        .bind(&terminal.terminal_digest)
        .fetch_optional(&self.pool)
        .await?;
        let now = now_us();
        let evidence = terminal.evidence.as_ref().map(bound_evidence);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO workflow_terminal_events \
             (terminal_digest, run_id, status, node_path, evidence_json, publication_state, \
              created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&terminal.terminal_digest)
        .bind(&terminal.run_id)
        .bind(&terminal.status)
        .bind(&terminal.node_path)
        .bind(&evidence)
        .bind(PublicationState::Noted.as_str())
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        self.log_terminal_row_in_tx(&mut tx, &terminal.terminal_digest, now)
            .await?;
        tx.commit().await?;
        match rival {
            Some((other,)) => Ok(TerminalOutcome::Conflict {
                existing_digest: other,
            }),
            None => Ok(TerminalOutcome::New),
        }
    }

    /// Mark one terminal digest published. Returns `false` when unknown.
    pub async fn mark_terminal_published(&self, terminal_digest: &str) -> anyhow::Result<bool> {
        let now = now_us();
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE workflow_terminal_events SET publication_state = ?1, updated_at = ?2 \
             WHERE terminal_digest = ?3",
        )
        .bind(PublicationState::Published.as_str())
        .bind(now)
        .bind(terminal_digest)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() > 0 {
            self.log_terminal_row_in_tx(&mut tx, terminal_digest, now)
                .await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected() > 0)
    }

    /// Enforce retention/compaction bounds (bd-v139.3): delete terminal
    /// effects whose `retention_until` passed (plus their orphaned attempt
    /// evidence), published terminals older than `terminal_max_age_us`, and
    /// terminal runs older than `terminal_max_age_us` with no live effects.
    /// NEVER deletes: non-terminal effects, `unknown`/`reconcile_pending`
    /// rows (uncertainty outlives every evidence window), or parked pending
    /// rows (live work is not evidence). Each table is bounded by `limit`.
    pub async fn prune_expired(
        &self,
        now: i64,
        terminal_max_age_us: i64,
        limit: i64,
    ) -> anyhow::Result<PruneCounts> {
        // NOTE: stock SQLite has no DELETE..LIMIT; bound each pass with a
        // rowid subquery instead (same bound, portable syntax).
        let mut tx = self.pool.begin().await?;
        let doomed_effects: Vec<(String,)> = sqlx::query_as(
            "SELECT effect_key FROM workflow_effects \
             WHERE retention_until IS NOT NULL AND retention_until < ?1 \
                AND state IN ('settled','failed','cancelled') \
              LIMIT ?2",
        )
        .bind(now)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let effects = sqlx::query(
            "DELETE FROM workflow_effects WHERE effect_key IN \
             (SELECT effect_key FROM workflow_effects \
              WHERE retention_until IS NOT NULL AND retention_until < ?1 \
                AND state IN ('settled','failed','cancelled') \
              LIMIT ?2)",
        )
        .bind(now)
        .bind(limit)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        for (key,) in &doomed_effects {
            log_wfx_tombstone_in_tx(&mut tx, "workflow_effects", key, now).await?;
        }
        let attempts = sqlx::query(
            "DELETE FROM workflow_effect_attempts WHERE id IN \
             (SELECT id FROM workflow_effect_attempts \
              WHERE effect_key NOT IN (SELECT effect_key FROM workflow_effects) \
              LIMIT ?1)",
        )
        .bind(limit)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        let cutoff = now.saturating_sub(terminal_max_age_us);
        let doomed_terminals: Vec<(String,)> = sqlx::query_as(
            "SELECT terminal_digest FROM workflow_terminal_events \
             WHERE publication_state = 'published' AND updated_at < ?1 \
              LIMIT ?2",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let terminals = sqlx::query(
            "DELETE FROM workflow_terminal_events WHERE terminal_digest IN \
             (SELECT terminal_digest FROM workflow_terminal_events \
              WHERE publication_state = 'published' AND updated_at < ?1 \
              LIMIT ?2)",
        )
        .bind(cutoff)
        .bind(limit)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        for (key,) in &doomed_terminals {
            log_wfx_tombstone_in_tx(&mut tx, "workflow_terminal_events", key, now).await?;
        }
        let doomed_runs: Vec<(String,)> = sqlx::query_as(
            "SELECT run_id FROM workflow_runs \
             WHERE status IN ('succeeded','failed','blocked','cancelled','unknown') \
                AND updated_at < ?1 \
                AND NOT EXISTS (SELECT 1 FROM workflow_effects \
                                WHERE workflow_effects.run_id = workflow_runs.run_id \
                                  AND state NOT IN ('settled','failed','cancelled')) \
              LIMIT ?2",
        )
        .bind(cutoff)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        let runs = sqlx::query(
            "DELETE FROM workflow_runs WHERE run_id IN \
             (SELECT run_id FROM workflow_runs \
              WHERE status IN ('succeeded','failed','blocked','cancelled','unknown') \
                AND updated_at < ?1 \
                AND NOT EXISTS (SELECT 1 FROM workflow_effects \
                                WHERE workflow_effects.run_id = workflow_runs.run_id \
                                  AND state NOT IN ('settled','failed','cancelled')) \
              LIMIT ?2)",
        )
        .bind(cutoff)
        .bind(limit)
        .execute(&mut *tx)
        .await?
        .rows_affected() as i64;
        for (key,) in &doomed_runs {
            log_wfx_tombstone_in_tx(&mut tx, "workflow_runs", key, now).await?;
        }
        tx.commit().await?;
        Ok(PruneCounts {
            effects,
            attempts,
            terminals,
            runs,
        })
    }
}

#[cfg(test)]
mod workflow_effect_schema_tests {
    use super::*;

    async fn pool(tag: &str) -> (sqlx::SqlitePool, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("toron-wfx-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("wfx.sqlite");
        let p = crate::index::schema::connect(db.to_str().unwrap())
            .await
            .unwrap();
        crate::index::schema::migrate(&p).await.unwrap();
        (p, dir)
    }

    fn effect(key: &str, run: &str) -> NewEffect {
        NewEffect {
            effect_key: key.into(),
            run_id: run.into(),
            project: Some("demo".into()),
            workflow_digest: "digest-1".into(),
            node_path: "gate".into(),
            iteration_key: None,
            action_type: "send_message".into(),
            action_digest: "adigest-1".into(),
            redacted_action: Some(serde_json::json!({"action": "send_message"})),
            retention_until: None,
        }
    }

    /// bd-v139.1: the migration creates the five workflow tables and no
    /// workflow table references `receipts`.
    #[tokio::test]
    async fn workflow_effect_schema_migration_creates_tables() {
        let (p, _dir) = pool("migrate").await;
        let store = WorkflowEffectStore::new(p);
        for table in [
            "workflow_runs",
            "workflow_effects",
            "workflow_effect_attempts",
            "workflow_pending",
            "workflow_terminal_events",
        ] {
            assert!(store.table_exists(table).await.unwrap(), "{table} exists");
        }
        assert!(
            store.table_exists("delivery_cursors").await.unwrap(),
            "delivery_cursors exists (bd-aqvg)"
        );
    }

    /// bd-aqvg: cursor snapshots round-trip through SQLite; upsert overwrites.
    #[tokio::test]
    async fn delivery_cursor_save_load_upserts() {
        let (p, _dir) = pool("cursor").await;
        let store = WorkflowEffectStore::new(p);
        store
            .save_delivery_cursor("s1", r#"["a","b"]"#, 2)
            .await
            .unwrap();
        let rows = store.load_delivery_cursors().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "s1");
        assert_eq!(rows[0].1, r#"["a","b"]"#);
        assert_eq!(rows[0].2, 2);
        // Upsert: same session overwrites, never duplicates.
        store
            .save_delivery_cursor("s1", r#"["a","b","c"]"#, 3)
            .await
            .unwrap();
        let rows = store.load_delivery_cursors().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, r#"["a","b","c"]"#);
        assert_eq!(rows[0].2, 3);
    }

    /// bd-aqvg: prune drops idle sessions only; fresh rows survive. A pruned
    /// session re-delivers on next sighting (bounded duplicate over unbounded
    /// growth).
    #[tokio::test]
    async fn delivery_cursor_prune_keeps_fresh() {
        let (p, _dir) = pool("cursor-prune").await;
        let store = WorkflowEffectStore::new(p);
        // Anchor the idle bound BEFORE the saves: pruning deletes strictly older.
        let bound = now_us();
        store.save_delivery_cursor("fresh", "[]", 0).await.unwrap();
        // Backdate one row past any sane idle bound directly.
        sqlx::query("UPDATE delivery_cursors SET updated_us = 1 WHERE session = 'fresh'")
            .execute(store.pool())
            .await
            .unwrap();
        store.save_delivery_cursor("live", "[]", 0).await.unwrap();
        let dropped = store.prune_delivery_cursors(bound).await.unwrap();
        assert_eq!(dropped, 1);
        let rows = store.load_delivery_cursors().await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "live");
    }

    /// bd-jysu: every mutation dual-writes a wfx.row event in the same
    /// transaction; the generation manifest advances per event.
    #[tokio::test]
    async fn workflow_events_dual_write_per_mutation() {
        let (p, _dir) = pool("dual").await;
        let store = WorkflowEffectStore::new(p.clone());
        store
            .create_run(NewRun {
                run_id: "run-1".into(),
                project: Some("demo".into()),
                workflow: "wf".into(),
                workflow_digest: "d".into(),
            })
            .await
            .unwrap();
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        let log = crate::index::event_log::EventLog::new(p);
        assert_eq!(log.count_kind("wfx.row").await.unwrap(), 2);
        assert_eq!(log.generation().await.unwrap(), 2);
    }

    /// bd-jysu: readers serve the projection — a raw row smuggled behind
    /// the log's back is invisible to get/list.
    #[tokio::test]
    async fn workflow_readers_serve_projection_not_table() {
        let (p, _dir) = pool("projread").await;
        let store = WorkflowEffectStore::new(p.clone());
        store
            .create_run(NewRun {
                run_id: "run-1".into(),
                project: Some("demo".into()),
                workflow: "wf".into(),
                workflow_digest: "d".into(),
            })
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO workflow_runs (run_id, project, workflow, workflow_digest, status, \
             started_at, created_at, updated_at) \
             VALUES ('ghost', 'demo', 'wf', 'd', 'started', 1, 1, 1)",
        )
        .execute(&p)
        .await
        .unwrap();
        assert!(store.get_run("ghost").await.unwrap().is_none());
        assert!(store.get_run("run-1").await.unwrap().is_some());
    }

    /// bd-jysu: wipe all five tables, rebuild purely from the log — full
    /// state returns, tombstoned rows stay deleted.
    #[tokio::test]
    async fn workflow_rebuild_from_log_restores_all_tables() {
        use crate::index::rebuild::rebuild_workflow_effects_from_log;
        let (p, _dir) = pool("rebuild").await;
        let store = WorkflowEffectStore::new(p.clone());
        store
            .create_run(NewRun {
                run_id: "run-1".into(),
                project: Some("demo".into()),
                workflow: "wf".into(),
                workflow_digest: "d".into(),
            })
            .await
            .unwrap();
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        store
            .record_attempt(NewAttempt {
                effect_key: "ek-1".into(),
                attempt: 1,
                attempt_id: "att-1".into(),
                claim_token: None,
                state: EffectState::Prepared,
                evidence: None,
            })
            .await
            .unwrap();
        store
            .upsert_pending(NewPending {
                run_id: "run-1".into(),
                promise_id: "p1".into(),
                park_generation: 0,
                project: Some("demo".into()),
                workflow: "wf".into(),
                node_path: "gate".into(),
                effect_key: Some("ek-1".into()),
                kind: PendingKind::Approval,
                pending_payload: serde_json::json!({}),
                deadline_secs: None,
            })
            .await
            .unwrap();
        store
            .note_terminal_pub(TerminalPub::new("dig-1", "run-1", "succeeded", "gate"))
            .await
            .unwrap();
        // Tombstone one pending entry before the wipe.
        assert!(store.delete_pending("run-1", "p1", 0).await.unwrap());
        wipe_workflow_tables(&p).await;
        let (applied, deleted, skipped) = rebuild_workflow_effects_from_log(&store).await.unwrap();
        assert_eq!(skipped, 0);
        // 4 live rows (run, effect, attempt, terminal); the pending entry
        // was tombstoned pre-wipe and stays deleted.
        assert_eq!(applied, 4, "live rows restored, got {applied}");
        assert_eq!(deleted, 0);
        assert!(store.get_effect("ek-1").await.unwrap().is_some());
        assert_eq!(store.list_attempts("ek-1").await.unwrap().len(), 1);
        assert!(store.get_run("run-1").await.unwrap().is_some());
        assert!(store.list_pending("run-1").await.unwrap().is_empty());
        // Second rebuild is idempotent (same rows, no dupes).
        let (applied2, _, _) = rebuild_workflow_effects_from_log(&store).await.unwrap();
        assert_eq!(applied2, applied);
    }

    /// Wipe all five workflow tables (rebuild-test helper).
    async fn wipe_workflow_tables(pool: &sqlx::SqlitePool) {
        for table in [
            "workflow_effects",
            "workflow_effect_attempts",
            "workflow_runs",
            "workflow_pending",
            "workflow_terminal_events",
        ] {
            sqlx::query(&format!("DELETE FROM {table} WHERE 1 = 1"))
                .execute(pool)
                .await
                .unwrap();
        }
    }

    /// bd-v139.1: duplicate `prepared` writes are idempotent — same key
    /// twice yields one row; the second call reports the existing row.
    #[tokio::test]
    async fn workflow_effect_schema_duplicate_prepare_is_idempotent() {
        let (p, _dir) = pool("prepare").await;
        let store = WorkflowEffectStore::new(p);
        let first = store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        assert!(first.created);
        assert_eq!(first.effect.state, EffectState::Prepared);
        let second = store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        assert!(
            !second.created,
            "duplicate prepare attaches, never duplicates"
        );
        assert_eq!(second.effect.attempt, 1);
    }

    /// bd-v139.1: fencing — a stale claim token cannot settle or steal an
    /// attempt while the live lease holds; the live owner re-claims and an
    /// expired lease is recoverable.
    #[tokio::test]
    async fn workflow_effect_schema_stale_claim_cannot_settle() {
        let (p, _dir) = pool("fence").await;
        let store = WorkflowEffectStore::new(p);
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        let now = now_us();
        assert!(
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 1,
                        attempt_id: "att-1".into(),
                        claim_token: "token-a".into(),
                        lease_expires_at: now + 60_000_000,
                        expected_claim_token: None,
                    },
                )
                .await
                .unwrap(),
            "first claim wins"
        );
        assert!(
            !store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 2,
                        attempt_id: "att-2".into(),
                        claim_token: "token-b".into(),
                        lease_expires_at: now + 60_000_000,
                        expected_claim_token: Some("token-stale".into()),
                    },
                )
                .await
                .unwrap(),
            "stale token cannot steal a live lease"
        );
        assert!(
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 1,
                        attempt_id: "att-1b".into(),
                        claim_token: "token-a".into(),
                        lease_expires_at: now + 60_000_000,
                        expected_claim_token: Some("token-a".into()),
                    },
                )
                .await
                .unwrap(),
            "live owner re-claims"
        );
        assert!(
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 2,
                        attempt_id: "att-2".into(),
                        claim_token: "token-b".into(),
                        lease_expires_at: now + 60_000_000,
                        expected_claim_token: Some("token-a".into()),
                    },
                )
                .await
                .unwrap(),
            "correct expected token advances the attempt"
        );
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(row.attempt, 2);
        assert_eq!(row.attempt_id.as_deref(), Some("att-2"));
        assert!(
            store
                .transition_effect("ek-1", EffectState::Settled, None, None)
                .await
                .unwrap(),
            "in_flight settles"
        );
        assert!(
            !store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 3,
                        attempt_id: "att-4".into(),
                        claim_token: "token-z".into(),
                        lease_expires_at: now + 60_000_000,
                        expected_claim_token: Some("token-b".into()),
                    },
                )
                .await
                .unwrap(),
            "terminal effects never re-open for claim"
        );
    }

    /// bd-v139.1: run-status compare-and-swap — only the expected current
    /// status transitions; terminal states never move.
    #[tokio::test]
    async fn workflow_effect_schema_run_status_cas() {
        let (p, _dir) = pool("runs").await;
        let store = WorkflowEffectStore::new(p);
        let run = NewRun {
            run_id: "run-1".into(),
            project: None,
            workflow: "ship".into(),
            workflow_digest: "digest-1".into(),
        };
        assert!(store.create_run(run.clone()).await.unwrap());
        assert!(!store.create_run(run).await.unwrap());
        assert!(
            store
                .cas_run_status("run-1", RunStatus::Started, RunStatus::Running)
                .await
                .unwrap()
        );
        assert!(
            !store
                .cas_run_status("run-1", RunStatus::Started, RunStatus::Failed)
                .await
                .unwrap(),
            "wrong expected status cannot transition"
        );
        assert!(
            store
                .cas_run_status("run-1", RunStatus::Running, RunStatus::Succeeded)
                .await
                .unwrap()
        );
        assert!(
            !store
                .cas_run_status("run-1", RunStatus::Succeeded, RunStatus::Failed)
                .await
                .unwrap(),
            "terminal states never move"
        );
    }

    /// bd-v139.1: pending upsert is idempotent on
    /// (run_id, promise_id, park_generation); generations are distinct rows.
    #[tokio::test]
    async fn workflow_effect_schema_pending_upsert_idempotent() {
        let (p, _dir) = pool("pending").await;
        let store = WorkflowEffectStore::new(p);
        let pend = NewPending {
            run_id: "run-1".into(),
            promise_id: "p-1".into(),
            park_generation: 1,
            project: None,
            workflow: "ship".into(),
            node_path: "gate".into(),
            effect_key: None,
            kind: PendingKind::Approval,
            pending_payload: serde_json::json!({"question": "Release?"}),
            deadline_secs: None,
        };
        assert!(store.upsert_pending(pend.clone()).await.unwrap());
        assert!(!store.upsert_pending(pend.clone()).await.unwrap());
        let gen2 = NewPending {
            park_generation: 2,
            ..pend.clone()
        };
        assert!(store.upsert_pending(gen2).await.unwrap());
        assert_eq!(store.list_pending("run-1").await.unwrap().len(), 2);
        store.delete_pending("run-1", "p-1", 1).await.unwrap();
        assert_eq!(store.list_pending("run-1").await.unwrap().len(), 1);
    }

    /// bd-lm27: a park persists as a `Parked` effect row plus its pending
    /// resume row, keyed `(run_id, promise_id, park_generation)`. The row
    /// is INSERTED, never transitioned: `can_transition` has no edge into
    /// `Parked` in either direction, and a re-park under the same key
    /// attaches to the existing row. A `Parked` row is resumable, never a
    /// failure — the re-drive plan (`ResumeParked`) is proven at the plan
    /// table, this test proves the row it reads.
    #[tokio::test]
    async fn workflow_effect_schema_park_persists_and_redrives() {
        let (p, _dir) = pool("park").await;
        let store = WorkflowEffectStore::new(p);
        let park = NewPark {
            effect_key: "park:run-9:n1:2000".into(),
            run_id: "run-9".into(),
            promise_id: "p-9".into(),
            park_generation: 0,
            project: None,
            workflow: "ship".into(),
            workflow_digest: "dig".into(),
            node_path: "n1".into(),
            kind: PendingKind::Park,
            until_ms: Some(2000),
            reason: Some("busy owner".into()),
        };
        let out = store.park_effect(park.clone()).await.unwrap();
        assert!(out.created, "first park inserts");
        assert_eq!(out.effect.state, EffectState::Parked);
        assert!(out.effect.state.is_resumable());
        assert!(!out.effect.state.is_terminal());
        assert_ne!(out.effect.state, EffectState::Failed);
        // The resume index lists it under the same key triple.
        let pending = store.list_pending("run-9").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].park_generation, 0);
        assert_eq!(pending[0].kind, PendingKind::Park);
        assert_eq!(pending[0].effect_key.as_deref(), Some("park:run-9:n1:2000"));
        // Re-parking the same key is idempotent, not a duplicate wait.
        let again = store.park_effect(park).await.unwrap();
        assert!(!again.created);
        assert_eq!(store.list_pending("run-9").await.unwrap().len(), 1);
        // A host-side promise park carries no T (the leaf waits; the host
        // indexes the promise): digest reads "unknown", deadline is null.
        let host_park = NewPark {
            effect_key: "park:run-9:n2:prom-1".into(),
            run_id: "run-9".into(),
            promise_id: "prom-1".into(),
            park_generation: 1,
            project: None,
            workflow: "ship".into(),
            workflow_digest: "dig".into(),
            node_path: "n2".into(),
            kind: PendingKind::Approval,
            until_ms: None,
            reason: None,
        };
        let hosted = store.park_effect(host_park).await.unwrap();
        assert!(hosted.created);
        assert_eq!(hosted.effect.action_digest, "unknown");
        let rows = store.list_pending("run-9").await.unwrap();
        assert_eq!(rows.len(), 2);
        // No edge into or out of Parked through the transition table, on
        // any state — the row arrives by insert and leaves by resume.
        for state in [
            EffectState::Prepared,
            EffectState::InFlight,
            EffectState::Accepted,
            EffectState::Settled,
            EffectState::Failed,
            EffectState::Unknown,
            EffectState::ReconcilePending,
            EffectState::Cancelled,
            EffectState::Parked,
        ] {
            assert!(
                !can_transition(state, EffectState::Parked),
                "{state:?} must not transition into Parked"
            );
            assert!(
                !can_transition(EffectState::Parked, state),
                "Parked must not transition into {state:?} (not even itself)"
            );
        }
    }

    /// bd-v139.1: `unknown` and `reconcile_pending` are first-class states —
    /// every state round-trips and no read path auto-transitions (a timeout
    /// is never absence).
    #[tokio::test]
    async fn workflow_effect_schema_unknown_is_not_absence() {
        for state in [
            EffectState::Prepared,
            EffectState::InFlight,
            EffectState::Accepted,
            EffectState::Settled,
            EffectState::Failed,
            EffectState::Unknown,
            EffectState::ReconcilePending,
            EffectState::Cancelled,
        ] {
            assert_eq!(state, state.as_str().parse().unwrap());
        }
        assert!("bogus".parse::<EffectState>().is_err());
        // Legal lattice (ADR-0032 §5.2): unknown enters only from
        // in-flight/accepted uncertainty and leaves only via reconciliation.
        assert!(can_transition(EffectState::InFlight, EffectState::Unknown));
        assert!(can_transition(
            EffectState::Unknown,
            EffectState::ReconcilePending
        ));
        assert!(can_transition(
            EffectState::ReconcilePending,
            EffectState::Settled
        ));
        assert!(!can_transition(EffectState::Settled, EffectState::Unknown));
        assert!(!can_transition(EffectState::Failed, EffectState::Settled));
        assert!(!can_transition(EffectState::Prepared, EffectState::Settled));
    }

    /// bd-v139.1: terminal publication evidence — same digest republishes
    /// idempotently; a different digest for the same run is a conflict.
    #[tokio::test]
    async fn workflow_effect_schema_terminal_digest_idempotent_and_conflicting() {
        let (p, _dir) = pool("terminal").await;
        let store = WorkflowEffectStore::new(p);
        let pub1 = TerminalPub::new("run-1", "digest-a", "succeeded", "gate");
        assert!(matches!(
            store.note_terminal_pub(pub1.clone()).await.unwrap(),
            TerminalOutcome::New
        ));
        assert!(matches!(
            store.note_terminal_pub(pub1).await.unwrap(),
            TerminalOutcome::Duplicate
        ));
        assert!(store.mark_terminal_published("digest-a").await.unwrap());
        let conflict = TerminalPub::new("run-1", "digest-b", "failed", "gate");
        assert!(matches!(
            store.note_terminal_pub(conflict).await.unwrap(),
            TerminalOutcome::Conflict { .. }
        ));
    }

    /// bd-v139.1: evidence is bounded — oversize outcome/failure projections
    /// are truncated with a marker, never stored raw.
    #[tokio::test]
    async fn workflow_effect_schema_evidence_bounded() {
        let (p, _dir) = pool("bounded").await;
        let store = WorkflowEffectStore::new(p);
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        let big = "x".repeat(MAX_EVIDENCE_BYTES + 1000);
        store
            .transition_effect(
                "ek-1",
                EffectState::InFlight,
                None,
                Some(serde_json::json!({"blob": big})),
            )
            .await
            .unwrap();
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        let outcome = row.outcome_json.unwrap();
        assert!(outcome.len() <= MAX_EVIDENCE_BYTES + 32);
        assert!(outcome.ends_with("…[truncated]"));
    }

    /// bd-v139.1: restart recovery — rows survive pool drop/reopen; an
    /// expired lease is reclaimable by a new owner after restart.
    #[tokio::test]
    async fn workflow_effect_schema_restart_recovers_rows() {
        let dir = std::env::temp_dir().join(format!("toron-wfx-restart-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("wfx.sqlite");
        let path = db.to_str().unwrap().to_string();
        let now = now_us();
        {
            let p = crate::index::schema::connect(&path).await.unwrap();
            crate::index::schema::migrate(&p).await.unwrap();
            let store = WorkflowEffectStore::new(p);
            store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 1,
                        attempt_id: "att-1".into(),
                        claim_token: "token-old".into(),
                        lease_expires_at: now - 1_000_000,
                        expected_claim_token: None,
                    },
                )
                .await
                .unwrap();
        }
        let p = crate::index::schema::connect(&path).await.unwrap();
        crate::index::schema::migrate(&p).await.unwrap();
        let store = WorkflowEffectStore::new(p);
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(row.state, EffectState::InFlight);
        assert!(
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 2,
                        attempt_id: "att-2".into(),
                        claim_token: "token-new".into(),
                        lease_expires_at: now_us() + 60_000_000,
                        expected_claim_token: Some("token-wrong".into()),
                    },
                )
                .await
                .unwrap(),
            "expired lease is reclaimable even with a wrong expected token"
        );
    }

    /// bd-v139.1: attempt evidence is append-only — every claim and
    /// transition leaves a bounded evidence row; replays read history,
    /// never rewrite it.
    #[tokio::test]
    async fn workflow_effect_schema_attempts_recorded() {
        let (p, _dir) = pool("attempts").await;
        let store = WorkflowEffectStore::new(p);
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        let now = now_us();
        store
            .claim_attempt(
                "ek-1",
                ClaimAttempt {
                    attempt: 1,
                    attempt_id: "att-1".into(),
                    claim_token: "token-a".into(),
                    lease_expires_at: now + 60_000_000,
                    expected_claim_token: None,
                },
            )
            .await
            .unwrap();
        for (attempt_id, state) in [
            ("att-1", EffectState::InFlight),
            ("att-1", EffectState::Unknown),
        ] {
            store
                .record_attempt(NewAttempt {
                    effect_key: "ek-1".into(),
                    attempt: 1,
                    attempt_id: attempt_id.into(),
                    claim_token: Some("token-a".into()),
                    state,
                    evidence: Some(serde_json::json!({"at": attempt_id})),
                })
                .await
                .unwrap();
        }
        let attempts = store.list_attempts("ek-1").await.unwrap();
        assert_eq!(attempts.len(), 2, "attempt evidence appends");
        assert_eq!(attempts[0].state, EffectState::InFlight);
        assert_eq!(attempts[1].state, EffectState::Unknown);
        assert!(store.list_attempts("ek-9").await.unwrap().is_empty());
    }

    /// bd-v139.1: mailbox receipts stay mailbox receipts — a receipt row and
    /// an effect row coexist with no foreign key between them, and the
    /// ReceiptStore path is untouched by workflow writes.
    #[tokio::test]
    async fn workflow_effect_schema_receipts_stay_separate() {
        let (p, _dir) = pool("separate").await;
        let store = WorkflowEffectStore::new(p.clone());
        store.prepare_effect(effect("ek-1", "run-1")).await.unwrap();
        let receipts = crate::mailbox::receipts::ReceiptStore::new(p.clone());
        receipts
            .upsert(&crate::mailbox::receipts::Receipt {
                corr: "ek-1".into(),
                state: crate::mailbox::receipts::ReceiptState::Owed,
                wake: None,
                due_at: None,
                result_path: None,
                updated_at: now_us(),
            })
            .await
            .unwrap();
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(row.transport_corr, None, "no implied corr link on prepare");
        let got = receipts.get("ek-1").await.unwrap().unwrap();
        assert_eq!(got.state, crate::mailbox::receipts::ReceiptState::Owed);
    }
}
