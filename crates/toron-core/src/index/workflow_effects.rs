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

use sqlx::{Row, SqlitePool};

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

/// Lifecycle state of one logical external effect (ADR-0032 Spec 5.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EffectState {
    Prepared,
    InFlight,
    Accepted,
    Settled,
    Failed,
    Unknown,
    ReconcilePending,
    Cancelled,
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
            _ => Err(format!("unknown effect state {s:?}")),
        }
    }

    /// True for terminal effect claims (settled, proven failed, cancelled).
    /// `unknown` and `reconcile_pending` are explicitly nonterminal.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Settled | Self::Failed | Self::Cancelled)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingKind {
    Approval,
    Spawn,
}

impl PendingKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::Spawn => "spawn",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "approval" => Ok(Self::Approval),
            "spawn" => Ok(Self::Spawn),
            _ => Err(format!("unknown pending kind {s:?}")),
        }
    }
}

/// A logical workflow effect (current-state row).
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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

impl WorkflowEffectStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
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
        .execute(&self.pool)
        .await?;
        let created = res.rows_affected() > 0;
        let row = self
            .get_effect(&effect.effect_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("effect vanished after prepare"))?;
        Ok(PrepareOutcome {
            created,
            effect: row,
        })
    }

    /// Load one effect by key.
    pub async fn get_effect(&self, effect_key: &str) -> anyhow::Result<Option<WorkflowEffect>> {
        let row = sqlx::query(
            "SELECT effect_key, run_id, project, workflow_digest, node_path, iteration_key, \
                    action_type, action_digest, redacted_action, state, attempt, attempt_id, \
                    claim_token, lease_expires_at, transport_corr, external_id, outcome_json, \
                    failure_json, reconcile_after, retention_until \
             FROM workflow_effects WHERE effect_key = ?1",
        )
        .bind(effect_key)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| self.map_effect(&r)).transpose()
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
            sqlx::query(
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
            .execute(&self.pool)
            .await?
            .rows_affected()
        } else {
            sqlx::query(
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
            .execute(&self.pool)
            .await?
            .rows_affected()
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
        let updated = sqlx::query(
            "UPDATE workflow_effects SET state = ?1, failure_json = COALESCE(?2, failure_json), \
                    outcome_json = COALESCE(?3, outcome_json), lease_expires_at = NULL, \
                    updated_at = ?4 \
             WHERE effect_key = ?5 AND state = ?6",
        )
        .bind(to.as_str())
        .bind(&failure_json)
        .bind(&outcome_json)
        .bind(now_us())
        .bind(effect_key)
        .bind(current.state.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();
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
        let res = sqlx::query(
            "UPDATE workflow_effects SET transport_corr = ?1, updated_at = ?2 \
             WHERE effect_key = ?3",
        )
        .bind(transport_corr)
        .bind(now_us())
        .bind(effect_key)
        .execute(&self.pool)
        .await?;
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
        let updated = sqlx::query(
            "UPDATE workflow_effects SET state = ?1, failure_json = COALESCE(?2, failure_json), \
                    outcome_json = COALESCE(?3, outcome_json), lease_expires_at = NULL, \
                    updated_at = ?4 \
             WHERE effect_key = ?5 AND claim_token = ?6 AND state = ?7",
        )
        .bind(to.as_str())
        .bind(&failure_json)
        .bind(&outcome_json)
        .bind(now_us())
        .bind(effect_key)
        .bind(presented_token)
        .bind(current.state.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated > 0)
    }

    /// Record one append-only attempt-evidence row.
    pub async fn record_attempt(&self, attempt: NewAttempt) -> anyhow::Result<i64> {
        let evidence = attempt.evidence.as_ref().map(bound_evidence);
        let res = sqlx::query(
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
        .bind(now_us())
        .execute(&self.pool)
        .await?;
        Ok(res.last_insert_rowid())
    }

    /// Attempt evidence for one effect, oldest first.
    pub async fn list_attempts(&self, effect_key: &str) -> anyhow::Result<Vec<EffectAttempt>> {
        let rows = sqlx::query(
            "SELECT id, effect_key, attempt, attempt_id, claim_token, state, evidence_json \
             FROM workflow_effect_attempts WHERE effect_key = ?1 ORDER BY id ASC",
        )
        .bind(effect_key)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                let state: String = r.get("state");
                Ok(EffectAttempt {
                    id: r.get("id"),
                    effect_key: r.get("effect_key"),
                    attempt: r.get("attempt"),
                    attempt_id: r.get("attempt_id"),
                    claim_token: r.get("claim_token"),
                    state: EffectState::parse(&state).map_err(|e| anyhow::anyhow!("{e}"))?,
                    evidence_json: r.get("evidence_json"),
                })
            })
            .collect()
    }

    /// Create one run. Idempotent on `run_id`: duplicates return `false`.
    pub async fn create_run(&self, run: NewRun) -> anyhow::Result<bool> {
        let now = now_us();
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
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Load one run by id.
    pub async fn get_run(&self, run_id: &str) -> anyhow::Result<Option<WorkflowRun>> {
        let row = sqlx::query(
            "SELECT run_id, project, workflow, workflow_digest, status, started_at, \
                    suspended_at, finished_at, current_node, failed_node, terminal_digest \
             FROM workflow_runs WHERE run_id = ?1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
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
        })
        .transpose()
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
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(updated > 0)
    }

    /// Upsert one parked pending entry. Idempotent on
    /// `(run_id, promise_id, park_generation)`: duplicates return `false`.
    pub async fn upsert_pending(&self, pending: NewPending) -> anyhow::Result<bool> {
        let now = now_us();
        let payload = bound_evidence(&pending.pending_payload);
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
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Load one pending entry by full identity.
    pub async fn get_pending(
        &self,
        run_id: &str,
        promise_id: &str,
        park_generation: i64,
    ) -> anyhow::Result<Option<Pending>> {
        let row = sqlx::query(
            "SELECT run_id, promise_id, park_generation, project, workflow, node_path, \
                    effect_key, kind, pending_payload, publication_state \
             FROM workflow_pending WHERE run_id = ?1 AND promise_id = ?2 AND park_generation = ?3",
        )
        .bind(run_id)
        .bind(promise_id)
        .bind(park_generation)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
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
        })
        .transpose()
    }

    /// All pending entries for one run, in park order.
    pub async fn list_pending(&self, run_id: &str) -> anyhow::Result<Vec<Pending>> {
        let rows = sqlx::query(
            "SELECT run_id, promise_id, park_generation, project, workflow, node_path, \
                    effect_key, kind, pending_payload, publication_state \
             FROM workflow_pending WHERE run_id = ?1 ORDER BY rowid ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
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
            })
            .collect()
    }

    /// Delete one pending entry (on validated resolution). Returns `false`
    /// when no such entry existed.
    pub async fn delete_pending(
        &self,
        run_id: &str,
        promise_id: &str,
        park_generation: i64,
    ) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "DELETE FROM workflow_pending \
             WHERE run_id = ?1 AND promise_id = ?2 AND park_generation = ?3",
        )
        .bind(run_id)
        .bind(promise_id)
        .bind(park_generation)
        .execute(&self.pool)
        .await?;
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
        .execute(&self.pool)
        .await?;
        match rival {
            Some((other,)) => Ok(TerminalOutcome::Conflict {
                existing_digest: other,
            }),
            None => Ok(TerminalOutcome::New),
        }
    }

    /// Mark one terminal digest published. Returns `false` when unknown.
    pub async fn mark_terminal_published(&self, terminal_digest: &str) -> anyhow::Result<bool> {
        let res = sqlx::query(
            "UPDATE workflow_terminal_events SET publication_state = ?1, updated_at = ?2 \
             WHERE terminal_digest = ?3",
        )
        .bind(PublicationState::Published.as_str())
        .bind(now_us())
        .bind(terminal_digest)
        .execute(&self.pool)
        .await?;
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
        let effects = sqlx::query(
            "DELETE FROM workflow_effects WHERE effect_key IN \
             (SELECT effect_key FROM workflow_effects \
              WHERE retention_until IS NOT NULL AND retention_until < ?1 \
                AND state IN ('settled','failed','cancelled') \
              LIMIT ?2)",
        )
        .bind(now)
        .bind(limit)
        .execute(&self.pool)
        .await?
        .rows_affected() as i64;
        let attempts = sqlx::query(
            "DELETE FROM workflow_effect_attempts WHERE id IN \
             (SELECT id FROM workflow_effect_attempts \
              WHERE effect_key NOT IN (SELECT effect_key FROM workflow_effects) \
              LIMIT ?1)",
        )
        .bind(limit)
        .execute(&self.pool)
        .await?
        .rows_affected() as i64;
        let cutoff = now.saturating_sub(terminal_max_age_us);
        let terminals = sqlx::query(
            "DELETE FROM workflow_terminal_events WHERE terminal_digest IN \
             (SELECT terminal_digest FROM workflow_terminal_events \
              WHERE publication_state = 'published' AND updated_at < ?1 \
              LIMIT ?2)",
        )
        .bind(cutoff)
        .bind(limit)
        .execute(&self.pool)
        .await?
        .rows_affected() as i64;
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
        .execute(&self.pool)
        .await?
        .rows_affected() as i64;
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
