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
//! bd-ff23: the claim row IS the durable command. It carries the rumor's
//! `created_at` (part of the event id, so a retry rebuilds the SAME event)
//! and whether the publish COMPLETED (`published`). A claim whose publish
//! never finished is re-published on the next retry — the mail is not lost —
//! and a claim whose publish finished replays its deliveries without
//! publishing again. The rumor's id is the command's identity: the key IS
//! the event id.
//!
//! Rows written before this change carry no `rumor_created_at`; the retry
//! cannot rebuild their event, so they keep the old suppress behaviour
//! (bounded by the TTL).
//!
//! Demo mode (no index pool) skips dedupe entirely.

use sha2::Digest;
use sqlx::SqlitePool;

/// TTL for a claimed send key (24h in ms). A retry with the same
/// `(project, sender, corr)` inside this window returns the first rumor's
/// event id; after it, the send goes out fresh.
pub const IDEMPOTENT_SEND_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// Claim key: project, sender, sender-minted corr, and a SHA-256 digest
/// of the content (`subject\x1fbody`). `\x1f` (unit separator) joins the
/// parts — it cannot appear in slugs, pins, or corrs without failing their
/// own validation first.
///
/// ADR-0010 F6: the key covers the content, so re-using a corr for a
/// DIFFERENT body/subject mints a new event instead of silently inheriting
/// the first event id. An identical retry still converges (same key).
/// Callers that seed claims directly (tests) must pass the send's actual
/// subject/body — a seed with mismatched content is a different send.
pub fn send_key(project: &str, sender: &str, corr: &str, subject: &str, body: &str) -> String {
    let digest = sha2::Sha256::digest(format!("{subject}\x1f{body}").as_bytes());
    format!("{project}\x1f{sender}\x1f{corr}\x1f{digest:x}")
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
        /// The prior attempt's publish completed. `false` means the crash
        /// happened between the claim and the publish: the mail never left,
        /// so the caller re-publishes it (bd-ff23).
        published: bool,
        /// The rumor's `created_at` (unix seconds) of the prior attempt.
        /// `None` on a row written before bd-ff23 — the caller cannot
        /// rebuild that event, so it keeps the old suppress behaviour.
        rumor_created_at: Option<i64>,
    },
}

/// Store over the `idempotent_sends` sqlite table.
#[derive(Debug, Clone)]
pub struct IdempotentSendStore {
    pool: SqlitePool,
}

impl IdempotentSendStore {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub const fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Claim `key` for this send, or return the fresh prior claim.
    ///
    /// Atomic convergence: INSERT OR IGNORE decides the winner; losers read
    /// the winner's row. An expired row is refreshed in place and treated
    /// as new.
    ///
    /// `rumor_created_at` is the rumor's `created_at` (unix seconds): it is
    /// part of the event id, so a retry that re-publishes an unfinished
    /// claim rebuilds the SAME event (bd-ff23).
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_or_duplicate(
        &self,
        key: &str,
        event_id: &str,
        project: &str,
        sender: &str,
        corr: &str,
        deliveries_json: &str,
        rumor_created_at: i64,
        now_ms: i64,
    ) -> anyhow::Result<SendClaim> {
        if key.trim().is_empty() {
            anyhow::bail!("send key must be non-empty");
        }
        let inserted = sqlx::query(
            "INSERT INTO idempotent_sends \
             (send_key, event_id, project_slug, sender_name, corr, recipients_json, created_at, \
              rumor_created_at, published) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0) \
             ON CONFLICT(send_key) DO NOTHING",
        )
        .bind(key)
        .bind(event_id)
        .bind(project)
        .bind(sender)
        .bind(corr)
        .bind(deliveries_json)
        .bind(now_ms)
        .bind(rumor_created_at)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 1 {
            return Ok(SendClaim::New);
        }
        let row: Option<(String, String, i64, i64, Option<i64>)> = sqlx::query_as(
            "SELECT event_id, recipients_json, created_at, published, rumor_created_at \
             FROM idempotent_sends WHERE send_key = ?1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((stored_id, stored_deliveries, created_at, published, rumor_ts)) => {
                if now_ms - created_at > IDEMPOTENT_SEND_TTL_MS {
                    sqlx::query(
                        "UPDATE idempotent_sends \
                         SET event_id = ?2, recipients_json = ?3, created_at = ?4, \
                             rumor_created_at = ?5, published = 0 \
                         WHERE send_key = ?1",
                    )
                    .bind(key)
                    .bind(event_id)
                    .bind(deliveries_json)
                    .bind(now_ms)
                    .bind(rumor_created_at)
                    .execute(&self.pool)
                    .await?;
                    Ok(SendClaim::New)
                } else {
                    Ok(SendClaim::Duplicate {
                        event_id: stored_id,
                        deliveries_json: stored_deliveries,
                        published: published != 0,
                        rumor_created_at: rumor_ts,
                    })
                }
            }
            // Lost race against a concurrent delete (prune): reclaim.
            None => Ok(SendClaim::New),
        }
    }

    /// The publish completed: the command is SETTLED. `event_id` and
    /// `deliveries_json` are re-stated because a re-published claim may
    /// carry a rebuilt event (the recipients changed between attempts).
    /// Best-effort: a failure here leaves the claim unpublished, and the
    /// next retry re-publishes the SAME event — never a second event.
    pub async fn mark_published(
        &self,
        key: &str,
        event_id: &str,
        deliveries_json: &str,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE idempotent_sends \
             SET published = 1, event_id = ?2, recipients_json = ?3 \
             WHERE send_key = ?1",
        )
        .bind(key)
        .bind(event_id)
        .bind(deliveries_json)
        .execute(&self.pool)
        .await?;
        Ok(())
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
    fn idempotent_send_key_covers_corr_and_content() {
        let base = send_key("demo", "GreenCastle", "C-1", "s", "b");
        assert_eq!(
            base,
            send_key("demo", "GreenCastle", "C-1", "s", "b"),
            "identical retry converges on one key"
        );
        assert_ne!(
            base,
            send_key("demo", "GreenCastle", "C-1", "s", "DIFFERENT"),
            "same corr with different body is a different send"
        );
        assert_ne!(
            base,
            send_key("demo", "GreenCastle", "C-2", "s", "b"),
            "different corr is a different send"
        );
    }

    #[tokio::test]
    async fn idempotent_send_claim_first_wins_then_duplicate() {
        let (pool, path) = test_pool("claim").await;
        let store = IdempotentSendStore::new(pool);
        let now = 1_700_000_000_000_000;
        let key = send_key("demo", "GreenCastle", "C-1", "s", "b");
        let first = store
            .claim_or_duplicate(
                &key,
                "ev-1",
                "demo",
                "GreenCastle",
                "C-1",
                "[]",
                1_700_000_000,
                now,
            )
            .await
            .expect("claim");
        assert_eq!(first, SendClaim::New);
        let second = store
            .claim_or_duplicate(
                &key,
                "ev-2",
                "demo",
                "GreenCastle",
                "C-1",
                "[]",
                1_700_000_001,
                now + 1,
            )
            .await
            .expect("reclaim");
        match second {
            SendClaim::Duplicate {
                event_id,
                published,
                rumor_created_at,
                ..
            } => {
                assert_eq!(event_id, "ev-1");
                assert!(!published, "a claim whose publish never finished");
                assert_eq!(
                    rumor_created_at,
                    Some(1_700_000_000),
                    "the rumor's timestamp rides the claim: a retry rebuilds the same event"
                );
            }
            SendClaim::New => panic!("second claim must be a duplicate"),
        }
        // The publish completes → the claim is settled.
        store
            .mark_published(&key, "ev-1", "[]")
            .await
            .expect("mark published");
        let third = store
            .claim_or_duplicate(
                &key,
                "ev-3",
                "demo",
                "GreenCastle",
                "C-1",
                "[]",
                1_700_000_002,
                now + 2,
            )
            .await
            .expect("reclaim");
        match third {
            SendClaim::Duplicate { published, .. } => assert!(published, "settled claim"),
            SendClaim::New => panic!("third claim must be a duplicate"),
        }
        let _ = std::fs::remove_file(&path);
    }
}
