// governed-by: ADR-0027
#![forbid(unsafe_code)]

//! Durable receipts store (ADR-0027 E2).
//!
//! Table `receipts(corr TEXT PRIMARY KEY, state TEXT NOT NULL, wake TEXT,
//! due_at INTEGER, result_path TEXT, updated_at INTEGER NOT NULL)` via
//! mailbox migrations next to `message_reads` / `inbox_cursors`.
//! The sqlite table is the durable truth; `receipt_cache` HashMap and
//! `WorkspaceModel.assignment` are process caches only (rebuilt from table on
//! startup, not folded from journal). Chat journal is NOT the cursor.

use sqlx::SqlitePool;

use crate::mailbox::send_tags::Wake;

/// Receipt state (E2/E3 lifecycle).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReceiptState {
    Owed,
    Acked,
    Resulted,
}

impl ReceiptState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Owed => "owed",
            Self::Acked => "acked",
            Self::Resulted => "resulted",
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "owed" => Ok(Self::Owed),
            "acked" => Ok(Self::Acked),
            "resulted" => Ok(Self::Resulted),
            _ => Err(format!(
                "unknown receipt state {s:?} — expected owed|acked|resulted"
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
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Idempotent upsert (INSERT OR REPLACE via ON CONFLICT). Validates
    /// state/wake at the boundary (typed, not free strings).
    pub async fn upsert(&self, receipt: &Receipt) -> anyhow::Result<()> {
        if receipt.corr.trim().is_empty() {
            anyhow::bail!("corr must be non-empty");
        }
        // Validate wake if present (ensures only standing|tasked ever lands)
        if let Some(w) = receipt.wake {
            // Wake is already typed, so valid by construction; this is a no-op but
            // documents the invariant.
            let _ = w.as_str();
        }
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
}
