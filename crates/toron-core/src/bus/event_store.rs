// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SQLite-backed event store for the in-process bus.
//!
//! Dedups by event id (UNIQUE), stores the wire-identical event JSON, and
//! provides monotonic replay for durable subscribers. The table is created
//! idempotently (`IF NOT EXISTS`) so it coexists with the index schema
//! (bd-led.4.1) in the same SQLite file without touching oc-b's migrations.
//!
//! This is the *bus* event store (transient envelope, TTL 90d per ADR-0003
//! G13): current state + replay cursor. The permanent Git archive and the
//! FTS5 search index are separate projections.

use sqlx::Row;
use sqlx::SqlitePool;
use std::time::Duration;

use nostr_sdk::nostr::{Event, JsonUtil, Timestamp};

/// Max rows kept in the bus store. Beyond this, the oldest envelopes are
/// pruned (bounded bus, permanent Git content — ADR-0003 superiority row).
pub const MAX_STORED_EVENTS: i64 = 100_000;

/// The bus event table DDL (idempotent).
pub const EVENT_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS bus_events (
    event_id   TEXT PRIMARY KEY,
    raw_json   TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    kind       INTEGER NOT NULL,
    author     TEXT NOT NULL,
    expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS bus_events_created_at ON bus_events (created_at);
CREATE INDEX IF NOT EXISTS bus_events_kind ON bus_events (kind);
";

/// SQLite event store for the in-process bus.
#[derive(Clone)]
pub struct BusEventStore {
    pool: SqlitePool,
}

impl BusEventStore {
    /// Open the store against an existing pool (table created idempotently).
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        sqlx::raw_sql(EVENT_TABLE_DDL).execute(&pool).await?;
        Ok(Self { pool })
    }

    /// Insert an event, deduping by event id.
    ///
    /// Returns `true` if the event was newly stored, `false` if it already
    /// existed (duplicate — do not fan out).
    pub async fn insert(&self, event: &Event) -> anyhow::Result<bool> {
        let expires = event.tags.expiration().map(|t| t.as_secs() as i64);
        let result = sqlx::query(
            "INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.id.to_hex())
        .bind(event.as_json())
        .bind(event.created_at.as_secs() as i64)
        .bind(event.kind.as_u16() as i64)
        .bind(event.pubkey.to_hex())
        .bind(expires)
        .execute(&self.pool)
        .await?;

        // ON CONFLICT DO NOTHING reports 0 affected rows for duplicates.
        Ok(result.rows_affected() == 1)
    }

    /// Replay stored events ordered by `(created_at, event_id)` ascending
    /// (ADR-0003 G7 envelope order), filtering expired envelopes and
    /// respecting the filter's `limit` when set.
    pub async fn replay(&self, filter: &crate::bus::Filter) -> anyhow::Result<Vec<Event>> {
        let limit = filter.limit.unwrap_or(1000).min(MAX_STORED_EVENTS as usize);
        let rows = sqlx::query(
            "SELECT raw_json FROM bus_events
             WHERE (expires_at IS NULL OR expires_at > ?1)
             ORDER BY created_at ASC, event_id ASC
             LIMIT ?2",
        )
        .bind(Timestamp::now().as_secs() as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let raw: String = Row::get(&row, 0);
            if let Ok(ev) = Event::from_json(&raw) {
                if filter.match_event(&ev, Default::default()) {
                    out.push(ev);
                }
            }
        }
        Ok(out)
    }

    /// Count of stored events (diagnostics / health).
    pub async fn count(&self) -> anyhow::Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) FROM bus_events")
            .fetch_one(&self.pool)
            .await?;
        Ok(Row::get(&row, 0))
    }

    /// Prune expired envelopes + the oldest beyond the cap. Best-effort.
    pub async fn prune(&self) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM bus_events WHERE expires_at IS NOT NULL AND expires_at <= ?1")
            .bind(Timestamp::now().as_secs() as i64)
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "DELETE FROM bus_events WHERE event_id IN (
                SELECT event_id FROM bus_events
                ORDER BY created_at ASC, event_id ASC
                LIMIT MAX(0, (SELECT COUNT(*) FROM bus_events) - ?1)
            )",
        )
        .bind(MAX_STORED_EVENTS)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Keep-alive interval for the prune loop.
    pub const PRUNE_INTERVAL: Duration = Duration::from_secs(60);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::PROJECT_MANIFEST;
    use nostr_sdk::nostr::{Keys, Tag};

    async fn temp_store() -> (BusEventStore, sqlx::SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("toron-bus-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let store = BusEventStore::new(pool.clone()).await.expect("store");
        (store, pool, path)
    }

    fn sample_event(keys: &Keys, content: &str) -> Event {
        build_event(
            keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            content,
            vec![Tag::parse(["t", "demo"]).expect("t")],
            Timestamp::now(),
        )
        .expect("build")
    }

    #[tokio::test]
    async fn test_insert_dedup_by_event_id() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let ev = sample_event(&keys, "one");

        let first = store.insert(&ev).await.expect("insert");
        assert!(first, "first insert stores");
        let dup = store.insert(&ev).await.expect("insert");
        assert!(!dup, "duplicate id is deduped");
        assert_eq!(store.count().await.expect("count"), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_replay_returns_matching_events() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let ev = sample_event(&keys, "hello");
        store.insert(&ev).await.expect("insert");

        let filter = crate::bus::project_filter("demo");
        let replayed = store.replay(&filter).await.expect("replay");
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].id, ev.id);

        let wrong = crate::bus::project_filter("other");
        assert!(store.replay(&wrong).await.expect("replay").is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_replay_skips_expired() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "expired",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(Timestamp::from(1_500_000_000_u64)),
            ],
            Timestamp::from(1_400_000_000_u64),
        )
        .expect("build");
        store.insert(&ev).await.expect("insert");

        let filter = crate::bus::project_filter("demo");
        assert!(store.replay(&filter).await.expect("replay").is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_replay_respects_limit() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        for i in 0..5 {
            let ev = sample_event(&keys, &format!("msg {i}"));
            store.insert(&ev).await.expect("insert");
        }
        let filter = crate::bus::project_filter("demo").limit(2);
        assert_eq!(store.replay(&filter).await.expect("replay").len(), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_wire_identical_roundtrip() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let ev = sample_event(&keys, "wire");
        store.insert(&ev).await.expect("insert");
        let filter = crate::bus::project_filter("demo");
        let replayed = store.replay(&filter).await.expect("replay");
        assert_eq!(replayed[0].as_json(), ev.as_json());
        let _ = std::fs::remove_file(&path);
    }
}
