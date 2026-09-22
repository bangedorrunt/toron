// governed-by: ADR-0027
#![forbid(unsafe_code)]

//! Durable receipts store (ADR-0027 E2).
//!
//! Table `receipts(corr TEXT PRIMARY KEY, state TEXT NOT NULL, wake TEXT,
//! due_at INTEGER, result_path TEXT, updated_at INTEGER NOT NULL)` via
//! mailbox migrations next to `message_reads` / `inbox_cursors`.
//! The sqlite table is the durable truth; NIP-17 wrap idempotency is
//! `Mailbox.wrap_idempotency` (not SLA). `WorkspaceModel.assignment` is a
//! process cache rebuilt from this table on startup. Chat journal is NOT
//! the cursor.

use sqlx::SqlitePool;
use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::ids::Corr;
use super::receipts_core::{self, ReceiptEffect, ReceiptsModel, ReceiptsMsg, State as CoreState};
use crate::mailbox::send_tags::Wake;

/// Receipt state (E2/E3 lifecycle, extended by flywheel ADR-0010 D8).
///
/// `Degraded` means acked-unknown: the message was durably enqueued but the
/// receipt bookkeeping failed, so whether it was acked can never be proven
/// from the store. It is terminal-downward — never dragged back to `owed`
/// (a re-owed corr invites a resend, and a resend would duplicate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReceiptState {
    Owed,
    Acked,
    Resulted,
    /// ADR-0010 D8: bookkeeping failure after durable enqueue. The wrap
    /// landed; the receipt could not be maintained. Never resend-inviting.
    Degraded,
}

impl From<ReceiptState> for CoreState {
    fn from(s: ReceiptState) -> Self {
        match s {
            ReceiptState::Owed => Self::Owed,
            ReceiptState::Acked => Self::Acked,
            ReceiptState::Resulted => Self::Resulted,
            ReceiptState::Degraded => Self::Degraded,
        }
    }
}

impl From<CoreState> for ReceiptState {
    fn from(s: CoreState) -> Self {
        match s {
            CoreState::Owed => Self::Owed,
            CoreState::Acked => Self::Acked,
            CoreState::Resulted => Self::Resulted,
            CoreState::Degraded => Self::Degraded,
        }
    }
}

impl ReceiptState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Owed => "owed",
            Self::Acked => "acked",
            Self::Resulted => "resulted",
            Self::Degraded => "degraded",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "owed" => Ok(Self::Owed),
            "acked" => Ok(Self::Acked),
            "resulted" => Ok(Self::Resulted),
            "degraded" => Ok(Self::Degraded),
            _ => Err(format!(
                "unknown receipt state {s:?} — expected owed|acked|resulted|degraded"
            )),
        }
    }
}

impl std::fmt::Display for ReceiptState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for ReceiptState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Printed on transport-unknown (ADR-0010 D8.1): the bus did not confirm
/// storage, so the wrap may or may not have landed. The only safe recovery is
/// the corr-preserving resend; a plain re-run of the send mints a NEW corr and
/// duplicates. With no corr there is nothing to preserve — the guidance
/// degrades to the duplicate warning.
pub fn resend_guidance(corr: Option<&str>) -> String {
    corr.map_or_else(
        || "toron: transport-unknown — the wrap may not have landed and no corr was stamped; \
             any re-run mints a new corr (a plain re-run DUPLICATES at the recipient only if it landed)."
            .to_string(),
        |c| format!(
            "toron: transport-unknown — the wrap may not have landed. \
             corr-preserving resend: toron mail send --corr {c} (re-supply to/subject/body). \
             A plain re-run of the send mints a NEW corr and DUPLICATES."
        ),
    )
}

/// Printed whenever a corr receipt is recorded degraded (ADR-0010 D8.2):
/// the message was durably enqueued, only the bookkeeping failed — a resend
/// would duplicate.
pub const DEGRADED_NOTE: &str = "toron: receipt degraded (acked-unknown): message enqueued but receipt bookkeeping failed — do NOT resend (a resend would duplicate).";

/// Durable receipt (projection of assignment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub corr: String,
    pub state: ReceiptState,
    pub wake: Option<Wake>,
    pub due_at: Option<i64>,
    pub result_path: Option<String>,
    pub updated_at: i64,
}

/// Wave-4 receipt event (ADR-0035 Spec 4): the log form of one stamp or
/// prune. The event log folds these last-write-wins per corr; the table
/// is that fold's projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptEvent {
    pub corr: String,
    pub state: String,
    pub wake: Option<String>,
    pub due_at: Option<i64>,
    pub result_path: Option<String>,
    pub updated_at: i64,
}

impl ReceiptEvent {
    /// One table stamp committed.
    pub const STAMPED: &'static str = "receipt.stamped";
    /// One prune tombstone: rebuild must NOT resurrect this corr.
    pub const PRUNED: &'static str = "receipt.pruned";

    pub fn stamped(r: &Receipt) -> Self {
        Self {
            corr: r.corr.clone(),
            state: r.state.as_str().to_string(),
            wake: r.wake.map(|w| w.as_str().to_string()),
            due_at: r.due_at,
            result_path: r.result_path.clone(),
            updated_at: r.updated_at,
        }
    }

    /// Unique per stamp (corr + stamp clock): idempotent re-stamps of the
    /// same row dedupe commutatively.
    pub fn event_id(&self) -> String {
        format!("receipt:{}:{}", self.corr, self.updated_at)
    }

    /// Tombstone id for a prune of `corr` at host time `now_ms`.
    pub fn prune_id(corr: &str, now_ms: i64) -> String {
        format!("receipt-prune:{corr}:{now_ms}")
    }
}

/// One place where the receipts table disagrees with its event fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptDivergence {
    pub corr: String,
    pub kind: DivergenceKind,
}

/// How table and log disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceKind {
    /// The log stamps a corr the table lacks.
    Missing,
    /// Same corr, different stamped field (names it).
    DivergedField(&'static str),
    /// A pruned corr is back in the table (resurrected by a raw write).
    Resurrected,
    /// A table row predates the log (pre-wave-4 write, informational).
    Ghost,
}

/// Store over the `receipts` sqlite table (E2).
#[derive(Debug, Clone)]
pub struct ReceiptStore {
    pool: SqlitePool,
}

/// Raw receipts table row (backfill + tripwire comparisons).
type TableRow = (
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    i64,
);

impl ReceiptStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Load one corr row as a core model plus the raw row (for the host to
    /// carry wake/due_at across the stamp). One loader for both transitions.
    async fn load_model(&self, corr: &Corr) -> anyhow::Result<(ReceiptsModel, Option<Receipt>)> {
        let current = self.get(corr.as_str()).await?;
        let model = current.as_ref().map_or_else(ReceiptsModel::new, |r| {
            ReceiptsModel::with_row(
                corr.as_str(),
                CoreState::from(r.state),
                r.result_path.clone(),
                r.updated_at,
            )
        });
        Ok((model, current))
    }

    /// Idempotent upsert (INSERT OR REPLACE via ON CONFLICT). Validates
    /// state/wake at the boundary (typed, not free strings).
    ///
    /// ADR-0028 E3 receipt ownership: sqlite owns `owed→acked→resulted`;
    /// this upsert is monotonic — it never downgrades `resulted` (not to
    /// `acked`, not to `owed`) and never drags `acked` back to `owed`
    /// (e.g. a late owed-on-send re-fire). Forward transitions apply;
    /// a `resulted` row keeps its stored `result_path` unless the incoming
    /// row carries a new one.
    pub async fn upsert(&self, receipt: &Receipt) -> anyhow::Result<()> {
        // Boundary parse (ADR-0035 Spec 6): corr is a `Corr`, non-empty.
        if Corr::from_str(&receipt.corr).is_err() {
            anyhow::bail!("corr must be non-empty");
        }
        // Validate wake if present (ensures only standing|tasked ever lands)
        if let Some(w) = receipt.wake {
            // Wake is already typed, so valid by construction; this is a no-op but
            // documents the invariant.
            let _ = w.as_str();
        }
        // Monotonic guard: never downgrade resulted, never drag acked to owed.
        // bd-aku0: tombstones refuse first — a pruned corr stays dead no
        // matter what the projection fold returns (get() hides pruned rows
        // as None, which the old code read as "free to insert").
        if self.is_tombstoned(&receipt.corr).await? {
            anyhow::bail!(
                "corr {} was pruned (tombstoned); refusing resurrection",
                receipt.corr
            );
        }
        if let Some(existing) = self.get(&receipt.corr).await? {
            match (existing.state, receipt.state) {
                (ReceiptState::Resulted, ReceiptState::Resulted) => {
                    // Idempotent: keep stored row unless the incoming row
                    // carries a new result_path.
                    if receipt.result_path.is_some() && receipt.result_path != existing.result_path
                    {
                        let mut merged = existing;
                        merged.result_path = receipt.result_path.clone();
                        merged.updated_at = receipt.updated_at;
                        return Box::pin(self.upsert_row(&merged)).await.map(|_| ());
                    }
                    return Ok(());
                }
                (ReceiptState::Resulted, _) => return Ok(()),
                (ReceiptState::Acked, ReceiptState::Owed) => return Ok(()),
                // ADR-0010 D8: degraded is acked-unknown — never re-owed, and
                // an ack is strictly more information than degraded, so an
                // acked row must not be dragged down either.
                (ReceiptState::Degraded, ReceiptState::Owed) => return Ok(()),
                (ReceiptState::Acked, ReceiptState::Degraded) => return Ok(()),
                _ => {}
            }
        }
        Box::pin(self.upsert_row(receipt)).await.map(|_| ())
    }

    /// Tombstone probe (bd-aku0): true when the event log carries a
    /// `receipt.pruned` for `corr` with no later stamp. A tombstoned corr
    /// must never be re-inserted — `get()` reports it as None, which is
    /// exactly what the old guard misread as "free to insert".
    async fn is_tombstoned(&self, corr: &str) -> anyhow::Result<bool> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let events = log.read_since(0).await?;
        Ok(matches!(Self::fold_events(&events).get(corr), Some(None)))
    }

    /// Authority for the monotonic lifecycle (bd-aku0): the same
    /// never-downgrade rules the outer `upsert` applies, re-checked on a
    /// fresh read every write attempt and enforced atomically by the
    /// conditional write's WHERE clause. False = skip (no write).
    const fn transition_allowed(current: ReceiptState, incoming: &Receipt) -> bool {
        match (current, incoming.state) {
            // Idempotent re-stamp carries nothing new unless it brings a path.
            (ReceiptState::Resulted, ReceiptState::Resulted) => incoming.result_path.is_some(),
            (ReceiptState::Resulted, _) => false,
            (ReceiptState::Acked, ReceiptState::Owed) => false,
            // ADR-0010 D8: degraded is acked-unknown — never re-owed, and
            // an ack is strictly more information than degraded.
            (ReceiptState::Degraded, ReceiptState::Owed) => false,
            (ReceiptState::Acked, ReceiptState::Degraded) => false,
            _ => true,
        }
    }

    /// Wave-4 dual-write (ADR-0035 Spec 4): the table row and its
    /// `receipt.stamped` event commit in ONE transaction. The log is the
    /// emerging source of truth; the table is its projection. A log failure
    /// fails the write (fail-closed) — never row-without-event.
    ///
    /// Returns true when the write landed, false when the guard skipped a
    /// stale write (bd-aku0: check-then-act with busy-retry re-validation —
    /// every attempt re-reads before writing, and the write itself is
    /// conditional so a concurrent commit between read and write matches
    /// zero rows instead of landing last). A tombstoned corr fails loud
    /// (never resurrected).
    ///
    /// Lock discipline: no SELECT ever runs inside the write tx before the
    /// write statement — the tx goes straight to a conditional write (which
    /// takes RESERVED up-front, so contenders wait on the busy timeout)
    /// instead of holding SHARED across an upgrade (which deadlocks).
    async fn upsert_row(&self, receipt: &Receipt) -> anyhow::Result<bool> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        for _ in 0..3 {
            // Pre-tx read (no locks held): fast skip + expected-state
            // snapshot. Re-read every attempt — a retry re-validates.
            if self.is_tombstoned(&receipt.corr).await? {
                anyhow::bail!(
                    "corr {} was pruned (tombstoned); refusing resurrection",
                    receipt.corr
                );
            }
            let current: Option<ReceiptState> =
                sqlx::query_scalar("SELECT state FROM receipts WHERE corr = ?1")
                    .bind(&receipt.corr)
                    .fetch_optional(&self.pool)
                    .await?
                    .map(|s: String| ReceiptState::parse(&s))
                    .transpose()
                    .map_err(|e| anyhow::anyhow!(e))?;
            if let Some(s) = current
                && !Self::transition_allowed(s, receipt)
            {
                return Ok(false);
            }
            let mut tx = self.pool.begin().await?;
            let wake_s = receipt.wake.map(|w| w.as_str().to_string());
            // Conditional act: the WHERE clause is the atomic guard.
            let wrote = match current {
                None => sqlx::query(
                    "INSERT OR IGNORE INTO receipts (corr, state, wake, due_at, result_path, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .bind(&receipt.corr)
                .bind(receipt.state.as_str())
                .bind(wake_s)
                .bind(receipt.due_at)
                .bind(&receipt.result_path)
                .bind(receipt.updated_at)
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
                Some(expected) => sqlx::query(
                    "UPDATE receipts SET state = ?1, wake = ?2, due_at = ?3, \
                     result_path = ?4, updated_at = ?5 \
                     WHERE corr = ?6 AND state = ?7",
                )
                .bind(receipt.state.as_str())
                .bind(wake_s)
                .bind(receipt.due_at)
                .bind(&receipt.result_path)
                .bind(receipt.updated_at)
                .bind(&receipt.corr)
                .bind(expected.as_str())
                .execute(&mut *tx)
                .await?
                .rows_affected()
                    == 1,
            };
            if !wrote {
                // Lost the race: another writer moved the row first.
                // Roll back and re-read — the next attempt re-validates.
                tx.rollback().await?;
                continue;
            }
            // Re-check the tombstone AFTER holding the write lock: a prune
            // that committed between our pre-read and this tx must refuse
            // us (otherwise the insert above resurrected a pruned corr).
            if self.is_tombstoned(&receipt.corr).await? {
                tx.rollback().await?;
                anyhow::bail!(
                    "corr {} was pruned (tombstoned); refusing resurrection",
                    receipt.corr
                );
            }
            let event = ReceiptEvent::stamped(receipt);
            let payload = serde_json::to_string(&event).unwrap_or_default();
            match log
                .append_in(
                    &mut tx,
                    &event.event_id(),
                    ReceiptEvent::STAMPED,
                    &payload,
                    receipt.updated_at.saturating_mul(1000),
                )
                .await
            {
                Ok(_) => {
                    tx.commit().await?;
                    return Ok(true);
                }
                Err(_) => {
                    tx.rollback().await?;
                }
            }
        }
        anyhow::bail!("receipt dual-write contention on {:?}", receipt.corr)
    }

    /// Wave-4 projection fold (ADR-0035 Spec 4): `receipt.stamped`
    /// last-write-wins per corr, `receipt.pruned` tombstones applied.
    /// Shared by readers, rebuild, and verify — one fold, never three.
    fn fold_events(
        events: &[crate::index::event_log::StoredEvent],
    ) -> BTreeMap<String, Option<ReceiptEvent>> {
        let mut folded = BTreeMap::new();
        for e in events {
            if e.kind == ReceiptEvent::STAMPED {
                if let Ok(ev) = serde_json::from_str::<ReceiptEvent>(&e.payload) {
                    folded.insert(ev.corr.clone(), Some(ev));
                }
            } else if e.kind == ReceiptEvent::PRUNED
                && let Ok(v) = serde_json::from_str::<serde_json::Value>(&e.payload)
                && let Some(corr) = v.get("corr").and_then(|c| c.as_str())
            {
                folded.insert(corr.to_string(), None);
            }
        }
        folded
    }

    /// Wave-4 projection fold (ADR-0035 Spec 4): the receipts table as the
    /// event log sees it — `receipt.stamped` last-write-wins per corr,
    /// `receipt.pruned` tombstones applied. Unparseable payloads are skipped
    /// (verify_against_log surfaces them as Ghost/Missing, never silent).
    /// ponytail: full fold per read; receipts are low-volume (hundreds of
    /// rows). Per-corr index if this ever serves a hot path.
    async fn fold_projected(&self) -> anyhow::Result<BTreeMap<String, Receipt>> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let events = log.read_since(0).await?;
        let mut out = BTreeMap::new();
        for (corr, slot) in Self::fold_events(&events) {
            let Some(ev) = slot else { continue };
            let Ok(state) = ReceiptState::parse(&ev.state) else {
                continue;
            };
            let wake = match ev.wake {
                Some(s) => match Wake::parse(&s) {
                    Ok(w) => Some(w),
                    Err(_) => continue,
                },
                None => None,
            };
            out.insert(
                corr.clone(),
                Receipt {
                    corr,
                    state,
                    wake,
                    due_at: ev.due_at,
                    result_path: ev.result_path,
                    updated_at: ev.updated_at,
                },
            );
        }
        Ok(out)
    }

    /// Get one receipt by corr — served from the event projection (wave-4
    /// reader switch). The table stays dual-written as the tripwire.
    pub async fn get(&self, corr: &str) -> anyhow::Result<Option<Receipt>> {
        Ok(self.fold_projected().await?.remove(corr))
    }

    /// List receipts, optionally filtered by state — from the projection.
    /// `None` = all states, newest first (table order preserved).
    pub async fn list(&self, state: Option<ReceiptState>) -> anyhow::Result<Vec<Receipt>> {
        let mut out: Vec<Receipt> = self
            .fold_projected()
            .await?
            .into_values()
            .filter(|r| state.is_none_or(|st| r.state == st))
            .collect();
        out.sort_by_key(|r| std::cmp::Reverse(r.updated_at));
        Ok(out)
    }

    /// List receipts for a single state (spec shape: `list(state: ReceiptState)`).
    pub async fn list_by_state(&self, state: ReceiptState) -> anyhow::Result<Vec<Receipt>> {
        self.list(Some(state)).await
    }

    /// Transition owed -> acked for `corr`. Idempotent: acked->acked no-op;
    /// resulted never downgrades. `now_ms` is host-supplied (ADR-0035 Spec 1:
    /// the clock enters as a Msg payload). Returns `true` if the row changed.
    /// The decision lives in `receipts_core::tick`; this method interprets.
    pub async fn transition_acked(&self, corr: &Corr, now_ms: i64) -> anyhow::Result<bool> {
        let (model, current) = self.load_model(corr).await?;
        let (_, fx) = receipts_core::tick(
            &model,
            &ReceiptsMsg::Ack {
                corr: corr.as_str().to_string(),
                now_ms,
            },
        );
        match fx.first() {
            Some(ReceiptEffect::Stamp {
                state,
                result_path,
                updated_at_ms,
                ..
            }) => {
                let base = current.unwrap_or_else(|| Receipt {
                    corr: corr.as_str().to_string(),
                    state: ReceiptState::Owed,
                    wake: None,
                    due_at: None,
                    result_path: None,
                    updated_at: 0,
                });
                self.upsert_row(&Receipt {
                    corr: corr.as_str().to_string(),
                    state: ReceiptState::from(*state),
                    wake: base.wake,
                    due_at: base.due_at,
                    result_path: result_path.clone(),
                    updated_at: *updated_at_ms,
                })
                .await
            }
            _ => Ok(false),
        }
    }

    /// Explicitly set `corr` to resulted (ADR-0027 E2.3). Never downgrades
    /// resulted: the update is idempotent. Optionally updates `result_path`.
    /// Creates the row if no prior receipt exists (explicit verb). `now_ms`
    /// is host-supplied; the decision lives in `receipts_core::tick`.
    pub async fn set_resulted(
        &self,
        corr: &Corr,
        result_path: Option<String>,
        now_ms: i64,
    ) -> anyhow::Result<Receipt> {
        let (model, current) = self.load_model(corr).await?;
        let (_, fx) = receipts_core::tick(
            &model,
            &ReceiptsMsg::SetResulted {
                corr: corr.as_str().to_string(),
                result_path: result_path.clone(),
                now_ms,
            },
        );
        match fx.first() {
            Some(ReceiptEffect::Stamp {
                state,
                result_path: stamped_path,
                updated_at_ms,
                ..
            }) => {
                let base = current.unwrap_or_else(|| Receipt {
                    corr: corr.as_str().to_string(),
                    state: ReceiptState::Owed,
                    wake: None,
                    due_at: None,
                    result_path: None,
                    updated_at: 0,
                });
                let stamped = Receipt {
                    corr: corr.as_str().to_string(),
                    state: ReceiptState::from(*state),
                    wake: base.wake,
                    due_at: base.due_at,
                    result_path: stamped_path.clone(),
                    updated_at: *updated_at_ms,
                };
                // bd-aku0: the in-tx guard is the authority — a stale Stamp
                // decision that lost its race skips instead of landing last.
                if self.upsert_row(&stamped).await? {
                    Ok(stamped)
                } else {
                    self.get(corr.as_str())
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("receipt vanished mid-transition"))
                }
            }
            _ => self
                .get(corr.as_str())
                .await?
                .ok_or_else(|| anyhow::anyhow!("receipt vanished mid-transition")),
        }
    }
    /// Prune receipts older than `retention_ms` of host time `now_ms` (both
    /// host-supplied; ADR-0035 Spec 1). Tombstones each pruned corr in the
    /// log so a rebuild never resurrects it. Best-effort on empty store.
    pub async fn prune_expired(&self, now_ms: i64, retention_ms: i64) -> anyhow::Result<u64> {
        let (_, fx) = receipts_core::tick(
            &ReceiptsModel::new(),
            &ReceiptsMsg::Prune {
                now_ms,
                retention_ms,
            },
        );
        let cutoff = match fx.first() {
            Some(ReceiptEffect::PruneBefore { cutoff_ms }) => *cutoff_ms,
            _ => now_ms.saturating_sub(retention_ms),
        };
        let doomed: Vec<(String,)> =
            sqlx::query_as("SELECT corr FROM receipts WHERE updated_at < ?1")
                .bind(cutoff)
                .fetch_all(&self.pool)
                .await?;
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query("DELETE FROM receipts WHERE updated_at < ?1")
            .bind(cutoff)
            .execute(&mut *tx)
            .await?;
        for (corr,) in &doomed {
            let payload =
                serde_json::to_string(&serde_json::json!({ "corr": corr })).unwrap_or_default();
            log.append_in(
                &mut tx,
                &ReceiptEvent::prune_id(corr, now_ms),
                ReceiptEvent::PRUNED,
                &payload,
                now_ms.saturating_mul(1000),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected())
    }

    /// Keep at most `keep` newest receipts (by updated_at), tombstoning the
    /// evicted corrs in the log (same anti-resurrection contract as prune).
    pub async fn prune_to_cap(&self, keep: i64, now_ms: i64) -> anyhow::Result<u64> {
        let doomed: Vec<(String,)> = sqlx::query_as(
            "SELECT corr FROM receipts WHERE corr NOT IN \
             (SELECT corr FROM receipts ORDER BY updated_at DESC LIMIT ?1)",
        )
        .bind(keep)
        .fetch_all(&self.pool)
        .await?;
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "DELETE FROM receipts WHERE corr NOT IN \
             (SELECT corr FROM receipts ORDER BY updated_at DESC LIMIT ?1)",
        )
        .bind(keep)
        .execute(&mut *tx)
        .await?;
        for (corr,) in &doomed {
            let payload =
                serde_json::to_string(&serde_json::json!({ "corr": corr })).unwrap_or_default();
            log.append_in(
                &mut tx,
                &ReceiptEvent::prune_id(corr, now_ms),
                ReceiptEvent::PRUNED,
                &payload,
                now_ms.saturating_mul(1000),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(res.rows_affected())
    }

    /// One-time backfill (wave-4 cutover): stamp every table row into the
    /// log under its deterministic id. Idempotent — reruns dedupe and
    /// append nothing. Returns rows newly stamped.
    pub async fn backfill_from_table(&self) -> anyhow::Result<u64> {
        let rows = self.list_table_rows().await?;
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let mut stamped = 0u64;
        for r in &rows {
            let event = ReceiptEvent::stamped(r);
            if log.existing(&event.event_id()).await?.is_some() {
                continue;
            }
            let payload = serde_json::to_string(&event).unwrap_or_default();
            log.append(
                &event.event_id(),
                ReceiptEvent::STAMPED,
                &payload,
                r.updated_at.saturating_mul(1000),
            )
            .await?;
            stamped += 1;
        }
        Ok(stamped)
    }

    /// Rebuild the table from the log fold (export+import target, disaster
    /// recovery). Table-only writes — no new events. Tombstones delete.
    /// Returns (rows_written, rows_deleted).
    pub async fn rebuild_from_log(&self) -> anyhow::Result<(u64, u64)> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let events = log.read_since(0).await?;
        let folded = Self::fold_events(&events);
        let mut written = 0u64;
        let mut deleted = 0u64;
        for (corr, slot) in &folded {
            match slot {
                Some(ev) => {
                    sqlx::query(
                        "INSERT INTO receipts (corr, state, wake, due_at, result_path, updated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
                         ON CONFLICT(corr) DO UPDATE SET \
                           state = excluded.state, wake = excluded.wake, \
                           due_at = excluded.due_at, result_path = excluded.result_path, \
                           updated_at = excluded.updated_at",
                    )
                    .bind(corr)
                    .bind(&ev.state)
                    .bind(&ev.wake)
                    .bind(ev.due_at)
                    .bind(&ev.result_path)
                    .bind(ev.updated_at)
                    .execute(&self.pool)
                    .await?;
                    written += 1;
                }
                None => {
                    let res = sqlx::query("DELETE FROM receipts WHERE corr = ?1")
                        .bind(corr)
                        .execute(&self.pool)
                        .await?;
                    deleted += res.rows_affected();
                }
            }
        }
        Ok((written, deleted))
    }

    /// Raw table rows (backfill source). Not a reader — readers serve the
    /// projection.
    async fn list_table_rows(&self) -> anyhow::Result<Vec<Receipt>> {
        let rows: Vec<TableRow> = sqlx::query_as(
            "SELECT corr, state, wake, due_at, result_path, updated_at FROM receipts",
        )
        .fetch_all(&self.pool)
        .await?;
        let mut out = Vec::with_capacity(rows.len());
        for (corr, state_s, wake_s, due_at, result_path, updated_at) in rows {
            let state = ReceiptState::parse(&state_s).map_err(|e| anyhow::anyhow!(e))?;
            let wake = match wake_s {
                Some(s) => Some(Wake::parse(&s).map_err(|e| anyhow::anyhow!(e.message))?),
                None => None,
            };
            out.push(Receipt {
                corr,
                state,
                wake,
                due_at,
                result_path,
                updated_at,
            });
        }
        Ok(out)
    }

    /// Rebuild-verify (ADR-0035 wave 4): fold the `receipt.*` events
    /// last-write-wins per corr and diff against the RAW table rows (never
    /// the projection — comparing fold-to-fold would always agree).
    /// Pre-log rows report as `Ghost` (informational, never a failure).
    pub async fn verify_against_log(&self) -> anyhow::Result<Vec<ReceiptDivergence>> {
        let log = crate::index::event_log::EventLog::new(self.pool.clone());
        let events = log.read_since(0).await?;
        let expected = Self::fold_events(&events);
        let rows = self.list_table_rows().await?;
        let table: BTreeMap<&str, &Receipt> = rows.iter().map(|r| (r.corr.as_str(), r)).collect();
        let mut out = Vec::new();
        for (corr, exp) in &expected {
            match (exp, table.get(corr.as_str())) {
                (Some(_), None) => out.push(ReceiptDivergence {
                    corr: corr.clone(),
                    kind: DivergenceKind::Missing,
                }),
                (Some(ev), Some(row)) => {
                    for (field, same) in [
                        ("state", ev.state == row.state.as_str()),
                        ("updated_at", ev.updated_at == row.updated_at),
                        ("result_path", ev.result_path == row.result_path),
                    ] {
                        if !same {
                            out.push(ReceiptDivergence {
                                corr: corr.clone(),
                                kind: DivergenceKind::DivergedField(field),
                            });
                            break;
                        }
                    }
                }
                (None, Some(_)) => out.push(ReceiptDivergence {
                    corr: corr.clone(),
                    kind: DivergenceKind::Resurrected,
                }),
                (None, None) => {}
            }
        }
        for corr in table.keys() {
            if !expected.contains_key(*corr) {
                out.push(ReceiptDivergence {
                    corr: (*corr).to_string(),
                    kind: DivergenceKind::Ghost,
                });
            }
        }
        Ok(out)
    }
}

impl crate::mailbox::Mailbox {
    /// ReceiptStore accessor (requires index pool). Returns an error when no
    /// pool is attached — the CLI surfaces it as a usage error.
    pub fn receipt_store(&self) -> anyhow::Result<ReceiptStore> {
        let pool = self
            .index_pool()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("receipts store requires index pool (no --index)"))?;
        Ok(ReceiptStore::new(pool))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn test_pool(tag: &str) -> (SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-receipts-{}-{}.sqlite",
            tag,
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    /// bd-d8e4 reverify: the printed resend pointer must name a REAL command.
    /// `flywheel mail resend` never existed; the corr-preserving path is
    /// `toron mail send --corr` (verified present on the CLI).
    #[test]
    fn resend_guidance_names_live_command() {
        let g = resend_guidance(Some("c1"));
        assert!(
            g.contains("toron mail send --corr c1"),
            "pointer names the live corr resend path: {g}"
        );
        assert!(!g.contains("mail resend"), "no dead resend pointer: {g}");
    }
    #[tokio::test]
    async fn receipt_prune_tombstone_blocks_resurrection() {
        // bd-aku0: a pruned corr is tombstoned in the log. A later upsert
        // for the same corr must NOT resurrect it — the guard refuses.
        let (pool, path) = test_pool("tombstone").await;
        let store = ReceiptStore::new(pool);
        let r = Receipt {
            corr: "doomed-1".into(),
            state: ReceiptState::Owed,
            wake: None,
            due_at: None,
            result_path: None,
            updated_at: 100,
        };
        store.upsert(&r).await.expect("upsert");
        let pruned = store.prune_expired(10_000, 1_000).await.expect("prune");
        assert_eq!(pruned, 1, "one row pruned");
        assert!(store.get("doomed-1").await.expect("get").is_none());
        let ghost = Receipt {
            updated_at: 20_000,
            ..r
        };
        let err = store
            .upsert(&ghost)
            .await
            .expect_err("resurrection refused");
        assert!(
            err.to_string().contains("pruned"),
            "refusal names the tombstone: {err}"
        );
        assert!(store.get("doomed-1").await.expect("get").is_none());
        assert!(
            store
                .verify_against_log()
                .await
                .expect("verify")
                .iter()
                .all(|d| d.kind != DivergenceKind::Resurrected),
            "no resurrection in the tripwire"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipt_guard_in_tx_no_lost_update() {
        // bd-aku0: the monotonic guard must live INSIDE the write tx.
        // A stale owed write applied directly to the row writer must not
        // drag a committed resulted row back — even when the outer
        // check-then-act was bypassed.
        let (pool, path) = test_pool("intx").await;
        let store = ReceiptStore::new(pool);
        store
            .upsert(&Receipt {
                corr: "race-1".into(),
                state: ReceiptState::Resulted,
                wake: None,
                due_at: None,
                result_path: Some("/done.json".into()),
                updated_at: 300,
            })
            .await
            .expect("resulted");
        store
            .upsert_row(&Receipt {
                corr: "race-1".into(),
                state: ReceiptState::Owed,
                wake: None,
                due_at: None,
                result_path: None,
                updated_at: 200,
            })
            .await
            .expect("stale write skipped, not failed");
        let got = store.get("race-1").await.expect("get").expect("found");
        assert_eq!(got.state, ReceiptState::Resulted, "no lost update");
        assert_eq!(got.result_path.as_deref(), Some("/done.json"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_upsert_and_get_roundtrip() {
        let (pool, path) = test_pool("roundtrip").await;
        let store = ReceiptStore::new(pool);
        let r = Receipt {
            corr: "FM-GS-012".into(),
            state: ReceiptState::Owed,
            wake: Some(Wake::Tasked),
            due_at: Some(1_700_000_000_000_000),
            result_path: Some("/tmp/out.json".into()),
            updated_at: 1_700_000_000_000_000,
        };
        store.upsert(&r).await.expect("upsert");
        let got = store.get("FM-GS-012").await.expect("get").expect("found");
        assert_eq!(got, r);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_upsert_is_idempotent() {
        let (pool, path) = test_pool("idempotent").await;
        let store = ReceiptStore::new(pool);
        let r1 = Receipt {
            corr: "corr-1".into(),
            state: ReceiptState::Owed,
            wake: None,
            due_at: None,
            result_path: None,
            updated_at: 100,
        };
        store.upsert(&r1).await.expect("upsert 1");
        let r2 = Receipt {
            corr: "corr-1".into(),
            state: ReceiptState::Acked,
            wake: Some(Wake::Standing),
            due_at: Some(200),
            result_path: Some("/tmp/a".into()),
            updated_at: 200,
        };
        store.upsert(&r2).await.expect("upsert 2 overwrites");
        let got = store.get("corr-1").await.expect("get").expect("found");
        assert_eq!(got.state, ReceiptState::Acked);
        assert_eq!(got.wake, Some(Wake::Standing));
        assert_eq!(got.updated_at, 200);
        let all = store.list(None).await.expect("list");
        assert_eq!(all.len(), 1, "idempotent: still one row");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_list_filters_by_state() {
        let (pool, path) = test_pool("list").await;
        let store = ReceiptStore::new(pool);
        for (corr, state) in [
            ("c1", ReceiptState::Owed),
            ("c2", ReceiptState::Acked),
            ("c3", ReceiptState::Owed),
            ("c4", ReceiptState::Resulted),
        ] {
            store
                .upsert(&Receipt {
                    corr: corr.into(),
                    state,
                    wake: None,
                    due_at: None,
                    result_path: None,
                    updated_at: 1,
                })
                .await
                .expect("upsert");
        }
        let owed = store
            .list(Some(ReceiptState::Owed))
            .await
            .expect("list owed");
        assert_eq!(owed.len(), 2);
        assert!(owed.iter().all(|r| r.state == ReceiptState::Owed));
        let all = store.list(None).await.expect("list all");
        assert_eq!(all.len(), 4);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_get_missing_is_none() {
        let (pool, path) = test_pool("missing").await;
        let store = ReceiptStore::new(pool);
        let got = store.get("nope").await.expect("get");
        assert!(got.is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_state_parse_rejects_unknown() {
        assert!(ReceiptState::parse("bogus").is_err());
        assert_eq!(ReceiptState::parse("owed").unwrap(), ReceiptState::Owed);
    }

    #[tokio::test]
    async fn receipts_transition_acked_owed_to_acked_idempotent_and_no_downgrade() {
        use super::super::ids::Corr;
        fn c(s: &str) -> Corr {
            s.parse().expect("test corr")
        }
        let (pool, path) = test_pool("trans-acked").await;
        let store = ReceiptStore::new(pool);
        // missing -> no-op
        assert!(
            !store
                .transition_acked(&c("missing"), 1000)
                .await
                .expect("transition missing")
        );
        // owed -> acked
        store
            .upsert(&Receipt {
                corr: "c-acked".into(),
                state: ReceiptState::Owed,
                wake: Some(Wake::Tasked),
                due_at: None,
                result_path: None,
                updated_at: 100,
            })
            .await
            .expect("seed owed");
        assert!(
            store
                .transition_acked(&c("c-acked"), 1001)
                .await
                .expect("transition")
        );
        let got = store.get("c-acked").await.expect("get").expect("found");
        assert_eq!(got.state, ReceiptState::Acked);
        assert_eq!(got.updated_at, 1001, "host clock stamps the row");
        // idempotent: acked -> acked no-op
        assert!(
            !store
                .transition_acked(&c("c-acked"), 1002)
                .await
                .expect("second ack")
        );
        let got2 = store.get("c-acked").await.expect("get2").expect("found2");
        assert_eq!(got2.state, ReceiptState::Acked);
        // resulted never downgrades
        store
            .upsert(&Receipt {
                corr: "c-resulted".into(),
                state: ReceiptState::Resulted,
                wake: None,
                due_at: None,
                result_path: Some("/tmp/x".into()),
                updated_at: 300,
            })
            .await
            .expect("seed resulted");
        assert!(
            !store
                .transition_acked(&c("c-resulted"), 1003)
                .await
                .expect("ack on resulted")
        );
        let got3 = store
            .get("c-resulted")
            .await
            .expect("get3")
            .expect("found3");
        assert_eq!(got3.state, ReceiptState::Resulted);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_dual_write_and_rebuild_verify_clean() {
        use super::super::ids::Corr;
        fn c(s: &str) -> Corr {
            s.parse().expect("test corr")
        }
        let (pool, path) = test_pool("dual-write").await;
        let store = ReceiptStore::new(pool.clone());
        let log = crate::index::event_log::EventLog::new(pool);
        // Every stamp lands an event; the fold matches the table exactly.
        store
            .set_resulted(&c("w1"), Some("/w.json".into()), 5000)
            .await
            .expect("w1");
        store
            .transition_acked(&c("w1"), 5001)
            .await
            .expect("no downgrade of proven");
        assert_eq!(
            log.count_kind(ReceiptEvent::STAMPED).await.expect("count"),
            1
        );
        assert_eq!(log.generation().await.expect("gen"), 1);
        let div = store.verify_against_log().await.expect("verify");
        assert!(div.is_empty(), "clean fold, no divergence: {div:?}");
        // Prune tombstones: rebuild must not resurrect.
        store.prune_expired(9000, 1000).await.expect("prune");
        assert!(store.get("w1").await.expect("get").is_none(), "row pruned");
        let div = store.verify_against_log().await.expect("verify2");
        assert!(div.is_empty(), "tombstoned fold stays clean: {div:?}");
        assert_eq!(
            log.count_kind(ReceiptEvent::PRUNED).await.expect("pruned"),
            1
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reader_switch_backfill_rebuild_parallel_close() {
        use super::super::ids::Corr;
        fn c(s: &str) -> Corr {
            s.parse().expect("test corr")
        }
        let (pool, path) = test_pool("switch").await;
        let store = ReceiptStore::new(pool.clone());
        // Two lanes stamp concurrently: both survive (no lost writes).
        let (s1, s2) = (store.clone(), store.clone());
        let (r1, r2) = tokio::join!(
            async move {
                for i in 0..10 {
                    s1.set_resulted(&c(&format!("lane-a-{i}")), None, 7000 + i)
                        .await
                        .expect("a");
                }
            },
            async move {
                for i in 0..10 {
                    s2.set_resulted(&c(&format!("lane-b-{i}")), None, 8000 + i)
                        .await
                        .expect("b");
                }
            }
        );
        let _ = (r1, r2);
        // Readers serve the projection: all 20 visible via get/list.
        assert_eq!(store.list(None).await.expect("list").len(), 20);
        assert!(store.get("lane-a-3").await.expect("get").is_some());
        assert_eq!(
            store
                .list_by_state(ReceiptState::Resulted)
                .await
                .expect("filtered")
                .len(),
            20
        );
        // Backfill is a no-op on dual-written rows (idempotent).
        assert_eq!(store.backfill_from_table().await.expect("backfill"), 0);
        // Export + import into a fresh pool: both lanes survive.
        let log = crate::index::event_log::EventLog::new(pool);
        let exported = log.read_since(0).await.expect("export");
        assert_eq!(exported.len(), 20);
        let (pool2, path2) = test_pool("switch-import").await;
        let log2 = crate::index::event_log::EventLog::new(pool2.clone());
        for e in &exported {
            log2.append(&e.event_id, &e.kind, &e.payload, e.ts_us)
                .await
                .expect("import");
        }
        let store2 = ReceiptStore::new(pool2);
        let (wrote, deleted) = store2.rebuild_from_log().await.expect("rebuild");
        assert_eq!((wrote, deleted), (20, 0));
        assert_eq!(store2.list(None).await.expect("list2").len(), 20);
        assert!(store2.verify_against_log().await.expect("v2").is_empty());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&path2);
    }

    #[tokio::test]
    async fn rebuild_verify_catches_raw_table_write() {
        use super::super::ids::Corr;
        fn c(s: &str) -> Corr {
            s.parse().expect("test corr")
        }
        let (pool, path) = test_pool("diverged").await;
        let store = ReceiptStore::new(pool.clone());
        store.set_resulted(&c("v1"), None, 6000).await.expect("v1");
        // Bypass the store: raw write the table behind the log's back.
        sqlx::query("UPDATE receipts SET state = 'owed' WHERE corr = 'v1'")
            .execute(&pool)
            .await
            .expect("raw write");
        let div = store.verify_against_log().await.expect("verify");
        assert_eq!(div.len(), 1);
        assert_eq!(div[0].corr, "v1");
        assert_eq!(div[0].kind, DivergenceKind::DivergedField("state"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn receipts_set_resulted_creates_and_transitions_and_idempotent() {
        use super::super::ids::Corr;
        fn c(s: &str) -> Corr {
            s.parse().expect("test corr")
        }
        let (pool, path) = test_pool("set-resulted").await;
        let store = ReceiptStore::new(pool);
        // create when missing
        let r = store
            .set_resulted(&c("new-corr"), Some("/tmp/a.json".into()), 2000)
            .await
            .expect("set new");
        assert_eq!(r.state, ReceiptState::Resulted);
        assert_eq!(r.result_path.as_deref(), Some("/tmp/a.json"));
        // owed -> resulted
        store
            .upsert(&Receipt {
                corr: "owed-1".into(),
                state: ReceiptState::Owed,
                wake: Some(Wake::Standing),
                due_at: None,
                result_path: None,
                updated_at: 100,
            })
            .await
            .expect("seed owed");
        let r2 = store
            .set_resulted(&c("owed-1"), Some("/tmp/b.json".into()), 2001)
            .await
            .expect("set owed->resulted");
        assert_eq!(r2.state, ReceiptState::Resulted);
        assert_eq!(r2.wake, Some(Wake::Standing));
        assert_eq!(r2.result_path.as_deref(), Some("/tmp/b.json"));
        // acked -> resulted
        store
            .upsert(&Receipt {
                corr: "acked-1".into(),
                state: ReceiptState::Acked,
                wake: None,
                due_at: None,
                result_path: None,
                updated_at: 200,
            })
            .await
            .expect("seed acked");
        let r3 = store
            .set_resulted(&c("acked-1"), None, 2002)
            .await
            .expect("acked->resulted");
        assert_eq!(r3.state, ReceiptState::Resulted);
        // idempotent: resulted -> resulted (no downgrade, optional path update)
        let r4 = store
            .set_resulted(&c("acked-1"), Some("/tmp/c.json".into()), 2003)
            .await
            .expect("second resulted");
        assert_eq!(r4.state, ReceiptState::Resulted);
        assert_eq!(r4.result_path.as_deref(), Some("/tmp/c.json"));
        let r5 = store
            .set_resulted(&c("acked-1"), None, 2004)
            .await
            .expect("third resulted no path");
        assert_eq!(r5.state, ReceiptState::Resulted);
        assert_eq!(r5.result_path.as_deref(), Some("/tmp/c.json"));
        let _ = std::fs::remove_file(&path);
    }
}
