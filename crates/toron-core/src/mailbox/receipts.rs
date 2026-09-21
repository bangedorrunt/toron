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
use std::str::FromStr;

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

/// Store over the `receipts` sqlite table (E2).
#[derive(Debug, Clone)]
pub struct ReceiptStore {
    pool: SqlitePool,
}

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
                        return Box::pin(self.upsert_row(&merged)).await;
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
        Box::pin(self.upsert_row(receipt)).await
    }

    async fn upsert_row(&self, receipt: &Receipt) -> anyhow::Result<()> {
        let wake_s = receipt.wake.map(|w| w.as_str().to_string());
        sqlx::query(
            "INSERT INTO receipts (corr, state, wake, due_at, result_path, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(corr) DO UPDATE SET \
               state = excluded.state, wake = excluded.wake, due_at = excluded.due_at, \
               result_path = excluded.result_path, updated_at = excluded.updated_at",
        )
        .bind(&receipt.corr)
        .bind(receipt.state.as_str())
        .bind(wake_s)
        .bind(receipt.due_at)
        .bind(&receipt.result_path)
        .bind(receipt.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get one receipt by corr.
    #[allow(clippy::type_complexity)]
    pub async fn get(&self, corr: &str) -> anyhow::Result<Option<Receipt>> {
        let row: Option<(String, String, Option<String>, Option<i64>, Option<String>, i64)> =
            sqlx::query_as(
                "SELECT corr, state, wake, due_at, result_path, updated_at FROM receipts WHERE corr = ?1",
            )
            .bind(corr)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some((corr, state_s, wake_s, due_at, result_path, updated_at)) => {
                let state = ReceiptState::parse(&state_s).map_err(|e| anyhow::anyhow!(e))?;
                let wake = match wake_s {
                    Some(s) => Some(Wake::parse(&s).map_err(|e| anyhow::anyhow!(e.message))?),
                    None => None,
                };
                Ok(Some(Receipt {
                    corr,
                    state,
                    wake,
                    due_at,
                    result_path,
                    updated_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// List receipts, optionally filtered by state. `None` = all states.
    #[allow(clippy::type_complexity)]
    pub async fn list(&self, state: Option<ReceiptState>) -> anyhow::Result<Vec<Receipt>> {
        let rows: Vec<(
            String,
            String,
            Option<String>,
            Option<i64>,
            Option<String>,
            i64,
        )> = if let Some(st) = state {
            sqlx::query_as(
                    "SELECT corr, state, wake, due_at, result_path, updated_at FROM receipts WHERE state = ?1 ORDER BY updated_at DESC",
                )
                .bind(st.as_str())
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query_as(
                    "SELECT corr, state, wake, due_at, result_path, updated_at FROM receipts ORDER BY updated_at DESC",
                )
                .fetch_all(&self.pool)
                .await?
        };
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
                .await?;
                Ok(true)
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
                self.upsert_row(&stamped).await?;
                Ok(stamped)
            }
            _ => self
                .get(corr.as_str())
                .await?
                .ok_or_else(|| anyhow::anyhow!("receipt vanished mid-transition")),
        }
    }
    /// Prune receipts older than `retention_ms` of host time `now_ms` (both
    /// host-supplied; ADR-0035 Spec 1). Keeps the table bounded — callers
    /// should invoke periodically (e.g. on mailbox open and daily).
    /// Best-effort: never errors on empty store.
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
        let res = sqlx::query("DELETE FROM receipts WHERE updated_at < ?1")
            .bind(cutoff)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    /// Keep at most `keep` newest receipts (by updated_at). Deletes oldest
    /// beyond the cap — second bound for high-volume projects.
    pub async fn prune_to_cap(&self, keep: i64) -> anyhow::Result<u64> {
        let res = sqlx::query(
            "DELETE FROM receipts WHERE corr NOT IN \
             (SELECT corr FROM receipts ORDER BY updated_at DESC LIMIT ?1)",
        )
        .bind(keep)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
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
