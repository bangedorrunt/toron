// governed-by: ADR-0028
#![forbid(unsafe_code)]

//! Corr-keyed idempotent send (ADR-0028 slice D follow-up, zfnx.12).
//!
//! Same `(project, sender, corr)` within [`IDEMPOTENT_SEND_TTL_MS`]
//! converges on one rumor no matter how often the send retries
//! (`make-operations-idempotent`). The send claims
//! `(project, sender, corr) -> event_id` in the `idempotent_sends` sqlite
//! table BEFORE the bus publish (INSERT OR IGNORE), so a crash between the
//! publish and the receipt upsert — or an executor-level retry of Publish —
//! returns the existing event id instead of emitting a second rumor.
//!
//! Residual risk (documented, accepted in the bead): a crash between the
//! claim insert and the publish leaves a claim row for a rumor that never
//! hit the bus; retries within the TTL suppress instead of resending. The
//! TTL (24h) bounds the suppression; a resend with a fresh corr recovers.
//! Demo mode (no index pool) skips dedupe entirely.

use sqlx::SqlitePool;

/// TTL for a claimed send key (24h in ms). A retry with the same
/// `(project, sender, corr)` inside this window returns the first rumor's
/// event id; after it, the send goes out fresh.
pub const IDEMPOTENT_SEND_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// Claim key: project, sender, and sender-minted corr. `\x1f` (unit
/// separator) joins the parts — it cannot appear in slugs, pins, or corrs
/// without failing their own validation first.
pub fn send_key(project: &str, sender: &str, corr: &str) -> String {
    format!("{project}\x1f{sender}\x1f{corr}")
}

/// Outcome of claiming a send key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendClaim {
    /// First claim (or the prior claim expired): caller must publish.
    /// The row already records this send's event id + deliveries.
    New,
    /// Fresh prior claim: caller must NOT publish; replay these.
    Duplicate {
        event_id: String,
        /// JSON array of `[recipient, event_id]` pairs.
        deliveries_json: String,
    },
}

/// Store over the `idempotent_sends` sqlite table.
#[derive(Debug, Clone)]
pub struct IdempotentSendStore {
    pool: SqlitePool,
}

impl IdempotentSendStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Claim `key` for this send, or return the fresh prior claim.
    ///
    /// Atomic convergence: INSERT OR IGNORE decides the winner; losers read
    /// the winner's row. An expired row is refreshed in place and treated
    /// as new.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_or_duplicate(
        &self,
        key: &str,
        event_id: &str,
        project: &str,
        sender: &str,
        corr: &str,
        deliveries_json: &str,
        now_ms: i64,
    ) -> anyhow::Result<SendClaim> {
        if key.trim().is_empty() {
            anyhow::bail!("send key must be non-empty");
        }
        let inserted = sqlx::query(
            "INSERT INTO idempotent_sends \
             (send_key, event_id, project_slug, sender_name, corr, recipients_json, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
             ON CONFLICT(send_key) DO NOTHING",
        )
        .bind(key)
        .bind(event_id)
        .bind(project)
        .bind(sender)
        .bind(corr)
        .bind(deliveries_json)
        .bind(now_ms)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(SendClaim::New);
        }
        let row: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT event_id, recipients_json, created_at FROM idempotent_sends \
             WHERE send_key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((stored_id, stored_deliveries, created_at)) => {
                if now_ms - created_at > IDEMPOTENT_SEND_TTL_MS {
                    sqlx::query(
                        "UPDATE idempotent_sends \
                         SET event_id = ?2, recipients_json = ?3, created_at = ?4 \
                         WHERE send_key = ?1",
                    )
                    .bind(key)
                    .bind(event_id)
                    .bind(deliveries_json)
                    .bind(now_ms)
                    .execute(&self.pool)
                    .await?;
                    Ok(SendClaim::New)
                } else {
                    Ok(SendClaim::Duplicate {
                        event_id: stored_id,
                        deliveries_json: stored_deliveries,
                    })
                }
            }
            // Lost race against a concurrent delete (prune): reclaim.
            None => Ok(SendClaim::New),
        }
    }

    /// Delete claims older than the TTL. Best-effort bounded retention —
    /// callers invoke after a successful new claim; never blocks the send.
    pub async fn prune_expired(&self, now_ms: i64) -> anyhow::Result<u64> {
        let res = sqlx::query("DELETE FROM idempotent_sends WHERE created_at < ?1")
            .bind(now_ms - IDEMPOTENT_SEND_TTL_MS)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }
}

impl crate::mailbox::Mailbox {
    /// Idempotent-send store accessor (requires index pool). `Err` when no
    /// pool is attached — `send_message` skips dedupe in that mode.
    pub fn idempotent_store(&self) -> anyhow::Result<IdempotentSendStore> {
        let pool = self
            .index_pool()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("idempotent sends require index pool (no --index)"))?;
        Ok(IdempotentSendStore::new(pool))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn test_pool(tag: &str) -> (SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-idempotent-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    #[test]
    fn idempotent_send_key_joins_three_parts() {
        assert_eq!(
            send_key("demo", "GreenCastle", "C-1"),
            "demo\x1fGreenCastle\x1fC-1"
        );
        assert_ne!(
            send_key("demo", "GreenCastle", "C-1"),
            send_key("demo", "GreenCastle", "C-2")
        );
    }

    #[tokio::test]
    async fn idempotent_send_claim_first_wins_then_duplicate() {
        let (pool, path) = test_pool("claim").await;
        let store = IdempotentSendStore::new(pool);
        let now = 1_700_000_000_000_000;
        let key = send_key("demo", "GreenCastle", "C-1");
        let first = store
            .claim_or_duplicate(&key, "ev-1", "demo", "GreenCastle", "C-1", "[]", now)
            .await
            .expect("claim");
        assert_eq!(first, SendClaim::New);
        let second = store
            .claim_or_duplicate(&key, "ev-2", "demo", "GreenCastle", "C-1", "[]", now + 1)
            .await
            .expect("reclaim");
        match second {
            SendClaim::Duplicate { event_id, .. } => assert_eq!(event_id, "ev-1"),
            SendClaim::New => panic!("second claim must be a duplicate"),
        }
        let _ = std::fs::remove_file(&path);
    }
}
