// governed-by: ADR-0035
#![forbid(unsafe_code)]

//! Wave-4 event log (ADR-0035 Spec 4, bd-lnit): `events` + the generation
//! manifest is the source of truth; index, receipts, workflow effects, and
//! exports are projections.
//!
//! Rules: flush appends events (never rewrites); generations are monotonic
//! (`UNIQUE`, retry on contention); duplicate `event_id` appends are
//! commutative no-ops returning the stored generation. Payloads are JSON;
//! secret-bearing projections stay out (hashes/digests in, raw renders out).

use sqlx::SqlitePool;

/// One stored event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub generation: i64,
    pub event_id: String,
    pub kind: String,
    pub payload: String,
    pub ts_us: i64,
}

/// Append-only event log over the `events` table.
#[derive(Debug, Clone)]
pub struct EventLog {
    pool: SqlitePool,
}

impl EventLog {
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Current high-water generation from the manifest (0 on a fresh log).
    pub async fn generation(&self) -> anyhow::Result<i64> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM generation_manifest WHERE singleton = 1")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map_or(0, |(g,)| g))
    }

    /// Append one event, assigning the next generation. A duplicate
    /// `event_id` is a commutative no-op: the stored generation returns and
    /// the manifest does not advance.
    pub async fn append(
        &self,
        event_id: &str,
        kind: &str,
        payload: &str,
        ts_us: i64,
    ) -> anyhow::Result<i64> {
        // Single retry on generation contention (UNIQUE): re-read max and
        // re-insert. Single-writer discipline makes this a rarity, never
        // the path.
        for _ in 0..3 {
            let mut tx = self.pool.begin().await?;
            match self
                .append_in(&mut tx, event_id, kind, payload, ts_us)
                .await
            {
                Ok(g) => {
                    tx.commit().await?;
                    return Ok(g);
                }
                Err(_) => {
                    tx.rollback().await?;
                }
            }
        }
        anyhow::bail!("event_log append contention on {event_id:?}: retry budget spent")
    }

    /// Append inside the caller's transaction (dual-write: the table row
    /// and its event commit atomically). Same dedup contract as [`append`].
    /// Generation contention surfaces as an error for the caller to retry.
    pub async fn append_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event_id: &str,
        kind: &str,
        payload: &str,
        ts_us: i64,
    ) -> anyhow::Result<i64> {
        if event_id.trim().is_empty() {
            anyhow::bail!("event_id must be non-empty");
        }
        if kind.trim().is_empty() {
            anyhow::bail!("event kind must be non-empty");
        }
        if let Some(g) = self.existing_in(tx, event_id).await? {
            return Ok(g);
        }
        let mrow: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM generation_manifest WHERE singleton = 1")
                .fetch_optional(&mut **tx)
                .await?;
        let next = mrow.map_or(0, |(g,)| g) + 1;
        let inserted = sqlx::query(
            "INSERT INTO events (generation, event_id, kind, payload, ts_us) \
             VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(event_id) DO NOTHING",
        )
        .bind(next)
        .bind(event_id)
        .bind(kind)
        .bind(payload)
        .bind(ts_us)
        .execute(&mut **tx)
        .await?;
        if inserted.rows_affected() == 0 {
            // Lost the event_id race after the pre-check: read the winner.
            return self
                .existing_in(tx, event_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("event_log lost both insert and read"));
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
        Ok(next)
    }

    /// Generation already stored for `event_id`, if any (caller's tx).
    async fn existing_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event_id: &str,
    ) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM events WHERE event_id = ?1")
                .bind(event_id)
                .fetch_optional(&mut **tx)
                .await?;
        Ok(row.map(|(g,)| g))
    }

    /// Generation already stored for `event_id`, if any (idempotent
    /// backfill checks).
    pub async fn existing(&self, event_id: &str) -> anyhow::Result<Option<i64>> {
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT generation FROM events WHERE event_id = ?1")
                .bind(event_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(g,)| g))
    }

    /// Every event above `generation`, in generation order (rebuild input).
    pub async fn read_since(&self, generation: i64) -> anyhow::Result<Vec<StoredEvent>> {
        let rows: Vec<(i64, String, String, String, i64)> = sqlx::query_as(
            "SELECT generation, event_id, kind, payload, ts_us FROM events \
             WHERE generation > ?1 ORDER BY generation ASC",
        )
        .bind(generation)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(generation, event_id, kind, payload, ts_us)| StoredEvent {
                generation,
                event_id,
                kind,
                payload,
                ts_us,
            })
            .collect())
    }

    /// Count events of one kind (rebuild progress / divergence triage).
    /// Read only the events belonging to ONE `corr`, in generation order.
    ///
    /// bd-v51i: [`read_since`](Self::read_since) from 0 returns the entire log,
    /// which is unbounded and unindexed, so every call to it grew with the
    /// system's lifetime. Callers that only care about one corr's history must
    /// use this instead. The `json_extract` predicate is served by the
    /// `events_corr` expression index (migration 0014) — without it this is a
    /// scan that merely returns fewer rows, so the index is load-bearing, not
    /// a nicety.
    ///
    /// Callers pass the `corr` as a bind parameter. `json_extract` returns NULL
    /// for a non-JSON payload and NULL `= NULL` is never true, so a malformed
    /// row is simply absent rather than an error.
    pub async fn read_for_corr(&self, corr: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let rows: Vec<(i64, String, String, String, i64)> = sqlx::query_as(
            "SELECT generation, event_id, kind, payload, ts_us FROM events \
             WHERE json_extract(payload, '$.corr') = ?1 ORDER BY generation ASC",
        )
        .bind(corr)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(generation, event_id, kind, payload, ts_us)| StoredEvent {
                generation,
                event_id,
                kind,
                payload,
                ts_us,
            })
            .collect())
    }

    pub async fn count_kind(&self, kind: &str) -> anyhow::Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE kind = ?1")
            .bind(kind)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn log(tag: &str) -> (EventLog, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-eventlog-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        (EventLog::new(pool), path)
    }

    #[tokio::test]
    async fn generations_advance_monotonically() {
        let (log, _p) = log("mono").await;
        assert_eq!(log.generation().await.expect("gen"), 0);
        assert_eq!(log.append("e1", "k", "{}", 10).await.expect("a1"), 1);
        assert_eq!(log.append("e2", "k", "{}", 11).await.expect("a2"), 2);
        assert_eq!(log.generation().await.expect("gen2"), 2);
    }

    #[tokio::test]
    async fn duplicate_append_is_commutative_noop() {
        let (log, _p) = log("dedupe").await;
        let g = log.append("e1", "k", "{\"v\":1}", 10).await.expect("a1");
        // Same id, different payload: stored row wins, manifest unmoved.
        assert_eq!(log.append("e1", "k", "{\"v\":2}", 11).await.expect("a2"), g);
        assert_eq!(log.generation().await.expect("gen"), g);
        let rows = log.read_since(0).await.expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload, "{\"v\":1}");
    }

    #[tokio::test]
    async fn read_since_orders_by_generation() {
        let (log, _p) = log("order").await;
        for (id, ts) in [("a", 3), ("b", 1), ("c", 2)] {
            log.append(id, "k", "{}", ts).await.expect("append");
        }
        let rows = log.read_since(1).await.expect("read");
        assert_eq!(rows.len(), 2);
        assert!(rows[0].generation < rows[1].generation, "generation order");
        assert_eq!(log.count_kind("k").await.expect("count"), 3);
        assert_eq!(log.count_kind("nope").await.expect("count0"), 0);
    }

    #[tokio::test]
    async fn rejects_empty_identity() {
        let (log, _p) = log("empty").await;
        assert!(log.append("", "k", "{}", 1).await.is_err());
        assert!(log.append("e", "", "{}", 1).await.is_err());
    }
}
