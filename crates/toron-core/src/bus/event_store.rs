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
    d_tag      TEXT,
    expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS bus_events_created_at ON bus_events (created_at);
CREATE INDEX IF NOT EXISTS bus_events_kind ON bus_events (kind);
CREATE INDEX IF NOT EXISTS bus_events_addressable ON bus_events (kind, author, d_tag);
";

/// SQLite event store for the in-process bus.
#[derive(Clone)]
pub struct BusEventStore {
    pub(crate) pool: SqlitePool,
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
        let d_tag = crate::bus::AddressableKey::from_event(event).map(|k| k.d);
        let result = sqlx::query(
            "INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event.id.to_hex())
        .bind(event.as_json())
        .bind(event.created_at.as_secs() as i64)
        .bind(event.kind.as_u16() as i64)
        .bind(event.pubkey.to_hex())
        .bind(d_tag)
        .bind(expires)
        .execute(&self.pool)
        .await?;

        // ON CONFLICT DO NOTHING reports 0 affected rows for duplicates.
        Ok(result.rows_affected() == 1)
    }

    /// Insert an addressable (parameterized-replaceable, NIP-33) event with
    /// author isolation: the same author's prior event with the same
    /// `(kind, d)` is superseded (the bus keeps current state per author,
    /// ADR-0003 G13). A different author's event with the same `d` is kept —
    /// NIP-33 never supersedes across authors.
    ///
    /// Returns `true` when the event was stored (new or superseding).
    pub async fn insert_addressable(&self, event: &Event) -> anyhow::Result<bool> {
        use crate::bus::AddressableKey;
        if let Some(key) = AddressableKey::from_event(event) {
            // Delete the author's prior event at the same coordinate.
            sqlx::query("DELETE FROM bus_events WHERE kind = ?1 AND author = ?2 AND d_tag = ?3")
                .bind(key.kind as i64)
                .bind(&key.pubkey)
                .bind(&key.d)
                .execute(&self.pool)
                .await?;
        }
        self.insert(event).await
    }

    /// Replay stored events ordered by `(created_at, event_id)` ascending
    /// (ADR-0003 G7 envelope order), filtering expired envelopes and
    /// respecting the filter's `limit` when set.
    ///
    /// The kind filter is pushed into SQL (`WHERE kind IN (…)`) — the
    /// scan never truncates BEFORE matching. This is the P0 fix (bd-1pzd):
    /// with the old `LIMIT ?` applied to the raw scan, the NEWEST events
    /// (recent reservations/intents) fell outside the replay window on a
    /// busy bus, so the conflict check saw no holders and granted the same
    /// exclusive path to two agents. The `limit` is now applied to the
    /// MATCHED output, so a mailbox query always sees the events it asks
    /// for — up to the filter limit.
    pub async fn replay(&self, filter: &crate::bus::Filter) -> anyhow::Result<Vec<Event>> {
        let limit = filter.limit.unwrap_or(1000).min(MAX_STORED_EVENTS as usize);
        // SQL kind pushdown: `filter.kinds` is a public field on the nostr
        // Filter; when set, only those kinds are scanned (cheap + correct).
        let (sql, binds): (String, Vec<i64>) = match &filter.kinds {
            Some(kinds) if !kinds.is_empty() => {
                let kinds_vec: Vec<i64> = kinds.iter().map(|k| k.as_u16() as i64).collect();
                let placeholders: Vec<String> = (2..=kinds_vec.len() + 1)
                    .map(|i| format!("?{i}"))
                    .collect();
                (
                    format!(
                        "SELECT raw_json FROM bus_events
                         WHERE (expires_at IS NULL OR expires_at > ?1)
                           AND kind IN ({})
                         ORDER BY created_at ASC, event_id ASC",
                        placeholders.join(",")
                    ),
                    kinds_vec,
                )
            }
            _ => (
                "SELECT raw_json FROM bus_events
                 WHERE (expires_at IS NULL OR expires_at > ?1)
                 ORDER BY created_at ASC, event_id ASC"
                    .to_string(),
                Vec::new(),
            ),
        };
        let mut q = sqlx::query(&sql).bind(Timestamp::now().as_secs() as i64);
        for b in binds {
            q = q.bind(b);
        }
        let rows = q.fetch_all(&self.pool).await?;

        let mut out: Vec<Event> = Vec::with_capacity(rows.len());
        for row in rows {
            let raw: String = Row::get(&row, 0);
            if let Ok(ev) = Event::from_json(&raw)
                && filter.match_event(&ev, Default::default())
            {
                out.push(ev);
            }
        }
        // bd-cumj: the SQL yields ASC (envelope) order; when the matched set
        // exceeds the limit, keep the NEWEST `limit` (the tail) — dropping the
        // OLDEST superseded events, NEVER the latest state. The prior
        // oldest-first break silently dropped the newest events when a
        // coordinate accumulated >1000 events (a renewal storm), making a
        // fresh operator revocation invisible to projection resolution and
        // leaving stale locks reported as live.
        if out.len() > limit {
            out.drain(0..out.len() - limit);
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

    /// bd-cumj regression: when the matched set exceeds the limit, replay
    /// must keep the NEWEST events (the tail in ASC order), never the oldest.
    /// A renewal storm of >limit events on one coordinate previously made the
    /// newest revocation/state invisible to projection resolution.
    #[tokio::test]
    async fn test_replay_keeps_newest_when_over_limit() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        // Distinct ascending created_at so the newest are well-defined.
        for i in 0..5u64 {
            let ev = build_event(
                &keys,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                &format!("msg {i}"),
                vec![Tag::parse(["t", "demo"]).expect("t")],
                Timestamp::from(1_700_000_000_u64 + i),
            )
            .expect("build");
            store.insert(&ev).await.expect("insert");
        }
        let replayed = store
            .replay(&crate::bus::project_filter("demo").limit(2))
            .await
            .expect("replay");
        assert_eq!(replayed.len(), 2, "limit respected");
        // The two newest (i=3, i=4) are kept; the oldest three are dropped.
        let contents: Vec<String> = replayed
            .iter()
            .map(|e| e.content.clone())
            .collect();
        assert_eq!(
            contents,
            vec!["msg 3".to_string(), "msg 4".to_string()],
            "replay keeps the NEWEST events (ASC tail), not the oldest: {contents:?}"
        );
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

    /// GC: prune removes expired envelopes but keeps live ones (bounded bus,
    /// NIP-40 TTL — ADR-0003 superiority row).
    #[tokio::test]
    async fn test_prune_gc_removes_expired_keeps_live() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();

        // Expired envelope (past expiration) + live envelope (no expiration).
        let expired = build_event(
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
        store.insert(&expired).await.expect("insert expired");

        let live = sample_event(&keys, "live");
        store.insert(&live).await.expect("insert live");
        assert_eq!(store.count().await.expect("count"), 2);

        store.prune().await.expect("prune");

        assert_eq!(store.count().await.expect("count after prune"), 1);
        let all = store.all_events().await.expect("all");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, live.id, "only the live envelope survives GC");
        let _ = std::fs::remove_file(&path);
    }

    /// bd-1pzd regression: the replay window must NOT truncate before the
    /// filter matches. Two DISTINCT authors' 30031s on the same path both
    /// replay (the corruption-prevention floor: the conflict check must see
    /// every holder). The old SQL `LIMIT` applied to the raw scan dropped
    /// the newest events on a busy bus.
    #[tokio::test]
    async fn test_replay_kind_pushdown_returns_all_30031() {
        use crate::nostr::kinds::FILE_RESERVATION;
        let (store, _pool, path) = temp_store().await;
        let a = Keys::generate();
        let b = Keys::generate();
        for (keys, d) in [(a.clone(), "proj:src/shared/*"), (b.clone(), "proj:src/shared/*")] {
            let ev = build_event(
                &keys,
                crate::nostr::kinds::toron_kind(FILE_RESERVATION),
                r#"{"exclusive":true}"#,
                vec![Tag::parse(["d", d]).expect("d"), Tag::parse(["t", "proj"]).expect("t")],
                Timestamp::now(),
            )
            .expect("build");
            assert!(store.insert(&ev).await.expect("insert"));
        }
        let filter = crate::bus::filter::kind_project_filter(
            crate::nostr::kinds::toron_kind(FILE_RESERVATION),
            "proj",
        );
        let replayed = store.replay(&filter).await.expect("replay");
        assert_eq!(replayed.len(), 2, "both authors' 30031s replayed");
        assert_ne!(replayed[0].pubkey, replayed[1].pubkey, "distinct authors");
        let _ = std::fs::remove_file(&path);
    }

    /// NIP-33 author isolation at the store: the same author's replaceable
    /// event supersedes its own prior one; a different author's event with
    /// the same d survives (kept for the projection resolver).
    #[tokio::test]
    async fn test_addressable_supersede_by_author() {
        use crate::bus::{is_revocation, resolve_coordinate};
        let (store, _pool, path) = temp_store().await;
        let agent = Keys::generate();
        let operator = Keys::generate();

        let lease_v1 = build_event(
            &agent,
            crate::nostr::kinds::toron_kind(crate::nostr::kinds::FILE_RESERVATION),
            "lease v1",
            vec![Tag::parse(["d", "proj:src/**"]).expect("d")],
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build");
        let lease_v2 = build_event(
            &agent,
            crate::nostr::kinds::toron_kind(crate::nostr::kinds::FILE_RESERVATION),
            "lease v2",
            vec![Tag::parse(["d", "proj:src/**"]).expect("d")],
            Timestamp::from(1_700_000_001_u64),
        )
        .expect("build");
        // Operator revocation for the same coordinate.
        let revoke = build_event(
            &operator,
            crate::nostr::kinds::toron_kind(crate::nostr::kinds::FILE_RESERVATION),
            r#"{"status":"revoked"}"#,
            vec![Tag::parse(["d", "proj:src/**"]).expect("d")],
            Timestamp::from(1_700_000_002_u64),
        )
        .expect("build");

        assert!(store.insert_addressable(&lease_v1).await.expect("v1"));
        assert!(
            store
                .insert_addressable(&lease_v2)
                .await
                .expect("v2 supersedes")
        );
        // Same author's v1 is gone (author isolation); v2 remains.
        assert_eq!(store.count().await.expect("count"), 1);
        assert_eq!(store.all_events().await.expect("all")[0].id, lease_v2.id);

        // Operator revoke lands (different author) — both co-exist in the
        // store (raw relay view), projection resolves operator>agent.
        assert!(store.insert_addressable(&revoke).await.expect("revoke"));
        assert_eq!(store.count().await.expect("count after revoke"), 2);

        let all = store.all_events().await.expect("all");
        assert!(all.iter().any(|e| e.id == lease_v2.id));
        assert!(all.iter().any(|e| e.id == revoke.id && is_revocation(e)));

        // Projection: operator revocation wins over the agent lease (A1).
        let resolved =
            resolve_coordinate(all.iter(), Some(&operator.public_key())).expect("resolved");
        assert_eq!(resolved.id, revoke.id);
        let _ = std::fs::remove_file(&path);
    }
}
