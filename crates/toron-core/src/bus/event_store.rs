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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nostr_sdk::nostr::{Alphabet, Event, JsonUtil, SingleLetterTag, TagKind, Timestamp};

/// Max rows kept in the bus store. Beyond this, the oldest envelopes are
/// pruned (bounded bus, permanent Git content — ADR-0003 superiority row).
pub const MAX_STORED_EVENTS: i64 = 100_000;

/// bd-py9o: entry cap for the hot parse memo. The store caps at
/// MAX_STORED_EVENTS rows; when the memo exceeds this it clears wholesale —
/// a memo, never the source of truth, so the worst case after a clear is one
/// cold re-parse per row on the next replay.
const HOT_CACHE_CAP: usize = 10_000;

/// bd-replay-tag-pushdown-lurm: rows parsed in `replay()` (test-only
/// instrumented counter; the acceptance gate asserts the SQL prefilter
/// shrinks the parsed set to the matched rows).
#[cfg(test)]
pub(crate) static PARSED_ROWS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// bd-py9o: rows hydrated from the hot memo in `replay()` (test-only; the
/// incremental-replay gate asserts a second identical replay parses ZERO
/// rows and hits the memo for every row).
#[cfg(test)]
pub(crate) static HOT_HITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

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

/// p/e tag index DDL (bd-replay-tag-pushdown-lurm). The `p`/`e` tag values
/// are persisted as rows (EAV), not first-tag columns: an event can carry
/// several `p` or `e` tags, and a first-tag column would silently miss
/// filters on the non-first values. Exact-match rows are index-served and
/// sound for the hot shapes (p-recipient, e-thread).
pub const EVENT_TAG_TABLE_DDL: &str = "
CREATE TABLE IF NOT EXISTS bus_event_tags (
    event_id   TEXT NOT NULL,
    tag_kind   TEXT NOT NULL,
    tag_value  TEXT NOT NULL,
    PRIMARY KEY (event_id, tag_kind, tag_value)
);
CREATE INDEX IF NOT EXISTS bus_event_tags_kind_value ON bus_event_tags (tag_kind, tag_value);
";

/// SQLite event store for the in-process bus.
#[derive(Clone)]
pub struct BusEventStore {
    pub(crate) pool: SqlitePool,
    /// bd-py9o: hot memo of parsed events (`event_id -> Event`), shared
    /// across clones. `replay()` hydrates from it instead of re-running
    /// serde_json over every matching row on every subscribe — fleet polling
    /// replays identical filters every few seconds, and re-parsing the whole
    /// history each time pinned the daemon's cores. A memo only: SQL remains
    /// the source of truth (only ids returned by SQL are ever surfaced, and
    /// `match_event` still gates every hydrated row).
    hot: Arc<parking_lot::Mutex<HashMap<String, Arc<Event>>>>,
}

impl BusEventStore {
    /// Open the store against an existing pool (table created idempotently).
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        sqlx::raw_sql(EVENT_TABLE_DDL).execute(&pool).await?;
        sqlx::raw_sql(EVENT_TAG_TABLE_DDL).execute(&pool).await?;
        Self::backfill_tag_index(&pool).await?;
        Ok(Self {
            pool,
            hot: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        })
    }

    /// Memoize a freshly stored event (bd-py9o). Clears wholesale at the cap.
    fn hot_put(&self, event: &Event) {
        let mut hot = self.hot.lock();
        if hot.len() >= HOT_CACHE_CAP {
            hot.clear();
        }
        hot.insert(event.id.to_hex(), Arc::new(event.clone()));
    }

    /// One-time backfill of the p/e tag index from stored raw_json. Guarded
    /// by "tag table empty" so a store that already indexes tags (new
    /// inserts populate as they arrive) is never re-scanned.
    async fn backfill_tag_index(pool: &SqlitePool) -> anyhow::Result<()> {
        let row = sqlx::query("SELECT COUNT(*) FROM bus_event_tags")
            .fetch_one(pool)
            .await?;
        let existing: i64 = Row::get(&row, 0);
        if existing > 0 {
            return Ok(());
        }
        let events: Vec<(String, String)> =
            sqlx::query_as("SELECT event_id, raw_json FROM bus_events")
                .fetch_all(pool)
                .await?;
        let mut tx = pool.begin().await?;
        for (id, raw) in events {
            if let Ok(ev) = Event::from_json(&raw) {
                Self::index_tag_values(&mut tx, &id, &ev).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Index the p/e tag values of `event` as rows in `bus_event_tags`
    /// (exact-match EAV; see [`EVENT_TAG_TABLE_DDL`]).
    async fn index_tag_values(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        event_id: &str,
        event: &Event,
    ) -> anyhow::Result<()> {
        for tag in event.tags.iter() {
            let (kind, value) = match tag.kind() {
                TagKind::SingleLetter(s) if s == SingleLetterTag::lowercase(Alphabet::P) => {
                    ("p", tag.content())
                }
                TagKind::SingleLetter(s) if s == SingleLetterTag::lowercase(Alphabet::E) => {
                    ("e", tag.content())
                }
                _ => continue,
            };
            let Some(value) = value else { continue };
            sqlx::query(
                "INSERT INTO bus_event_tags (event_id, tag_kind, tag_value) VALUES (?1, ?2, ?3)
                 ON CONFLICT (event_id, tag_kind, tag_value) DO NOTHING",
            )
            .bind(event_id)
            .bind(kind)
            .bind(value)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
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

        // p/e tag index (bd-replay-tag-pushdown-lurm): written unconditionally
        // so a duplicate insert still converges (ON CONFLICT DO NOTHING).
        let mut tx = self.pool.begin().await?;
        Self::index_tag_values(&mut tx, &event.id.to_hex(), event).await?;
        tx.commit().await?;

        // ON CONFLICT DO NOTHING reports 0 affected rows for duplicates.
        if result.rows_affected() == 1 {
            self.hot_put(event);
        }
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
            // Evict the superseded events from the hot memo BEFORE the delete
            // (bd-py9o: stale memo entries must never outlive their rows).
            let superseded: Vec<(String,)> = sqlx::query_as(
                "SELECT event_id FROM bus_events WHERE kind = ?1 AND author = ?2 AND d_tag = ?3",
            )
            .bind(key.kind as i64)
            .bind(&key.pubkey)
            .bind(&key.d)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();
            if !superseded.is_empty() {
                let mut hot = self.hot.lock();
                for (id,) in &superseded {
                    hot.remove(id);
                }
            }
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
    ///
    /// bd-replay-tag-pushdown-lurm: the `#p`/`#e` tag filters are also
    /// pushed into SQL via the `bus_event_tags` index, and the limit is a
    /// SQL `LIMIT` on the DESCENDING matched set (the bd-cumj newest tail),
    /// re-emitted in ASC envelope order — rows are parsed only when they can
    /// match (the SQL prefilter is exact for p/e; `match_event` still runs
    /// on the surviving candidates for the non-pushed predicates).
    pub async fn replay(&self, filter: &crate::bus::Filter) -> anyhow::Result<Vec<Event>> {
        let limit = filter.limit.unwrap_or(1000).min(MAX_STORED_EVENTS as usize);

        // Predicates, in bind order: ?1 expiry, then kinds, then p values,
        // then e values, then the trailing limit.
        let mut preds: Vec<String> = vec!["(expires_at IS NULL OR expires_at > ?1)".to_string()];
        let mut kinds_binds: Vec<i64> = Vec::new();
        let mut p_binds: Vec<String> = Vec::new();
        let mut e_binds: Vec<String> = Vec::new();
        let mut next = 2usize;

        if let Some(kinds) = &filter.kinds
            && !kinds.is_empty()
        {
            let placeholders: Vec<String> = (next..next + kinds.len())
                .map(|i| format!("?{i}"))
                .collect();
            next += kinds.len();
            kinds_binds = kinds.iter().map(|k| k.as_u16() as i64).collect();
            preds.push(format!("kind IN ({})", placeholders.join(",")));
        }

        let p_vals = filter
            .generic_tags
            .get(&SingleLetterTag::lowercase(Alphabet::P));
        if let Some(vals) = p_vals
            && !vals.is_empty()
        {
            let placeholders: Vec<String> =
                (next..next + vals.len()).map(|i| format!("?{i}")).collect();
            next += vals.len();
            p_binds = vals.iter().cloned().collect();
            preds.push(format!(
                "event_id IN (SELECT event_id FROM bus_event_tags \
                 WHERE tag_kind = 'p' AND tag_value IN ({}))",
                placeholders.join(",")
            ));
        }

        let e_vals = filter
            .generic_tags
            .get(&SingleLetterTag::lowercase(Alphabet::E));
        if let Some(vals) = e_vals
            && !vals.is_empty()
        {
            let placeholders: Vec<String> =
                (next..next + vals.len()).map(|i| format!("?{i}")).collect();
            next += vals.len();
            e_binds = vals.iter().cloned().collect();
            preds.push(format!(
                "event_id IN (SELECT event_id FROM bus_event_tags \
                 WHERE tag_kind = 'e' AND tag_value IN ({}))",
                placeholders.join(",")
            ));
        }

        // bd-cumj newest-tail semantics in SQL: the DESC subquery takes the
        // NEWEST `limit` of the matched set (never truncating the latest
        // state), and the outer query re-emits it in ASC envelope order.
        // The matched set is never materialized beyond `limit` rows.
        //
        // LIMIT is pushed only when the SQL predicate set FULLY decides the
        // match (expiry + kinds + #p/#e). Filters carrying non-pushed
        // predicates (#t project tags, since/until, authors, ids, search)
        // must NOT push the limit: SQL would truncate CANDIDATES before
        // match_event runs, silently dropping older matched rows (a project
        // replay whose newest same-kind rows belong to other projects).
        // Those filters keep the post-parse drain over the prefiltered
        // candidate set (still shrunk by kinds/#p/#e, per the bead).
        let sql_exact = filter.ids.as_ref().is_none_or(|i| i.is_empty())
            && filter.authors.as_ref().is_none_or(|a| a.is_empty())
            && filter.search.is_none()
            && filter.since.is_none()
            && filter.until.is_none()
            && filter.generic_tags.iter().all(|(tag, vals)| {
                !vals.is_empty()
                    && (tag == &SingleLetterTag::lowercase(Alphabet::P)
                        || tag == &SingleLetterTag::lowercase(Alphabet::E))
            });
        let limit_clause = if sql_exact {
            format!(" LIMIT ?{next}")
        } else {
            String::new()
        };
        let inner = format!(
            "SELECT event_id, created_at FROM bus_events
             WHERE {}
             ORDER BY created_at DESC, event_id DESC{limit_clause}",
            preds.join("\n               AND ")
        );
        let sql = format!(
            "SELECT event_id FROM ({inner})
             ORDER BY created_at ASC, event_id ASC"
        );

        let mut q = sqlx::query_as::<_, (String,)>(&sql).bind(Timestamp::now().as_secs() as i64);
        for b in kinds_binds {
            q = q.bind(b);
        }
        for b in &p_binds {
            q = q.bind(b.as_str());
        }
        for b in &e_binds {
            q = q.bind(b.as_str());
        }
        if sql_exact {
            q = q.bind(limit as i64);
        }
        // bd-py9o: phase 1 selects ids only. Phase 2 hydrates — hot memo hits
        // skip the raw_json transfer AND the serde parse entirely; only rows
        // this process has never parsed are fetched and parsed (once). SQL
        // stays the source of truth; `match_event` still gates every row.
        let id_rows: Vec<(String,)> = q.fetch_all(&self.pool).await?;
        let ids: Vec<String> = id_rows.into_iter().map(|(id,)| id).collect();

        let mut resolved: HashMap<String, Event> = HashMap::with_capacity(ids.len());
        let mut cold_ids: Vec<String> = Vec::new();
        {
            let hot = self.hot.lock();
            for id in &ids {
                match hot.get(id) {
                    Some(ev) => {
                        #[cfg(test)]
                        HOT_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        resolved.insert(id.clone(), ev.as_ref().clone());
                    }
                    None => cold_ids.push(id.clone()),
                }
            }
        }
        for chunk in cold_ids.chunks(500) {
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("?{i}")).collect();
            let sql = format!(
                "SELECT event_id, raw_json FROM bus_events WHERE event_id IN ({})",
                placeholders.join(",")
            );
            let mut q = sqlx::query_as::<_, (String, String)>(&sql);
            for id in chunk {
                q = q.bind(id);
            }
            let rows = q.fetch_all(&self.pool).await?;
            if rows.is_empty() {
                continue;
            }
            let mut hot = self.hot.lock();
            for (id, raw) in rows {
                #[cfg(test)]
                PARSED_ROWS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Ok(ev) = Event::from_json(&raw) {
                    if hot.len() >= HOT_CACHE_CAP {
                        hot.clear();
                    }
                    hot.insert(id.clone(), Arc::new(ev.clone()));
                    resolved.insert(id, ev);
                }
            }
        }

        let mut out: Vec<Event> = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(ev) = resolved.get(id)
                && filter.match_event(ev, Default::default())
            {
                out.push(ev.clone());
            }
        }
        if !sql_exact && out.len() > limit {
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
        // Orphaned p/e tag rows of pruned events (they could never match a
        // bus_events join again; keep the index store bounded).
        sqlx::query(
            "DELETE FROM bus_event_tags WHERE event_id NOT IN (SELECT event_id FROM bus_events)",
        )
        .execute(&self.pool)
        .await?;
        // bd-py9o: drop memo entries wholesale — SQL is the source of truth,
        // and this keeps hot memory from drifting past the live row set.
        // Next replay repopulates incrementally (cold parse per new id only).
        self.hot.lock().clear();
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

    /// bd-py9o acceptance gate: a SECOND identical replay must parse zero
    /// rows — every row hydrates from the hot memo (and a warm instance,
    /// whose inserts memoized as they arrived, parses nothing at all).
    /// Only genuinely unseen rows may ever be parsed.
    #[tokio::test]
    async fn test_replay_second_identical_call_parses_zero_rows_bd_py9o() {
        use std::sync::atomic::Ordering;
        let (store, pool, path) = temp_store().await;
        for i in 0..5 {
            let ev = sample_event(&Keys::generate(), &format!("e{i}"));
            store.insert(&ev).await.expect("insert");
        }
        let filter = crate::bus::project_filter("demo");

        // Warm instance: inserts memoized each event on arrival.
        let w1 = store.replay(&filter).await.expect("warm replay 1");
        assert_eq!(w1.len(), 5);
        let p_warm = PARSED_ROWS.load(Ordering::Relaxed);
        let h_warm = HOT_HITS.load(Ordering::Relaxed);
        let w2 = store.replay(&filter).await.expect("warm replay 2");
        assert_eq!(w2.len(), 5);
        assert_eq!(
            PARSED_ROWS.load(Ordering::Relaxed),
            p_warm,
            "a warm replay parses NOTHING"
        );
        assert_eq!(
            HOT_HITS.load(Ordering::Relaxed) - h_warm,
            5,
            "the second warm replay served every row from the memo"
        );

        // Cold instance over the same pool (the daemon-restart shape): the
        // first replay pays one parse per row; the second pays ZERO.
        let cold = BusEventStore::new(pool.clone()).await.expect("cold store");
        let c1 = cold.replay(&filter).await.expect("cold replay 1");
        assert_eq!(c1.len(), 5);
        let p_cold = PARSED_ROWS.load(Ordering::Relaxed);
        assert!(
            p_cold >= p_warm + 5,
            "cold first replay parses every row from SQL"
        );
        let c2 = cold.replay(&filter).await.expect("cold replay 2");
        assert_eq!(c2.len(), 5);
        assert_eq!(
            c2.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>(),
            c1.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>(),
            "identical envelope order across replays"
        );
        assert_eq!(
            PARSED_ROWS.load(Ordering::Relaxed),
            p_cold,
            "SECOND identical replay must parse ZERO rows (bd-py9o)"
        );

        // A row inserted by ANOTHER process instance (daemon-restart shape)
        // is cold here: parsed exactly once, then memoized.
        let other = BusEventStore::new(pool.clone()).await.expect("other store");
        other
            .insert(&sample_event(&Keys::generate(), "fresh"))
            .await
            .expect("insert fresh");
        let c3 = cold.replay(&filter).await.expect("cold replay 3");
        assert_eq!(c3.len(), 6);
        assert_eq!(
            PARSED_ROWS.load(Ordering::Relaxed),
            p_cold + 1,
            "only the new row may be parsed"
        );
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
                format!("msg {i}"),
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
        let contents: Vec<String> = replayed.iter().map(|e| e.content.clone()).collect();
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
        for (keys, d) in [
            (a.clone(), "proj:src/shared/*"),
            (b.clone(), "proj:src/shared/*"),
        ] {
            let ev = build_event(
                &keys,
                crate::nostr::kinds::toron_kind(FILE_RESERVATION),
                r#"{"exclusive":true}"#,
                vec![
                    Tag::parse(["d", d]).expect("d"),
                    Tag::parse(["t", "proj"]).expect("t"),
                ],
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

    /// bd-replay-tag-pushdown-lurm acceptance #1: a p-tag-filtered replay on
    /// a 10k-event store parses only matched rows. The instrumented counter
    /// must see exactly the 5 matching rows (SQL prefilter via
    /// bus_event_tags), and the query plan must use the tag index.
    #[tokio::test]
    async fn test_replay_p_tag_pushdown_parses_only_matched() {
        use nostr_sdk::nostr::Filter;
        let (store, pool, path) = temp_store().await;
        let author = Keys::generate();
        let target = Keys::generate();
        let other = Keys::generate();
        let target_hex = target.public_key().to_hex();
        let other_hex = other.public_key().to_hex();
        const N: usize = 10_000;

        let mut tx = pool.begin().await.expect("tx");
        for i in 0..N {
            let p = if i < N - 5 { &other_hex } else { &target_hex };
            let ev = build_event(
                &author,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                format!("target {i}"),
                vec![
                    Tag::parse(["p", p]).expect("p"),
                    Tag::parse(["t", "demo"]).expect("t"),
                ],
                Timestamp::from(1_700_000_000u64 + i as u64),
            )
            .expect("build");
            sqlx::query(
                "INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)",
            )
            .bind(ev.id.to_hex())
            .bind(ev.as_json())
            .bind(ev.created_at.as_secs() as i64)
            .bind(ev.kind.as_u16() as i64)
            .bind(ev.pubkey.to_hex())
            .execute(&mut *tx)
            .await
            .expect("bulk insert");
            for tag in ev.tags.iter() {
                if let Some(v) = tag.content() {
                    sqlx::query(
                        "INSERT INTO bus_event_tags (event_id, tag_kind, tag_value) VALUES (?1, 'p', ?2)",
                    )
                    .bind(ev.id.to_hex())
                    .bind(v)
                    .execute(&mut *tx)
                    .await
                    .expect("tag insert");
                }
            }
        }
        tx.commit().await.expect("commit");
        assert_eq!(store.count().await.expect("count"), N as i64);

        // EXPLAIN: the p predicate is served by the tag index.
        let plan_rows = sqlx::query(
            "EXPLAIN QUERY PLAN SELECT raw_json FROM (
                 SELECT raw_json, created_at, event_id FROM bus_events
                 WHERE (expires_at IS NULL OR expires_at > ?1)
                   AND event_id IN (SELECT event_id FROM bus_event_tags
                                    WHERE tag_kind = 'p' AND tag_value IN (?2))
                 ORDER BY created_at DESC, event_id DESC LIMIT ?3
             ) ORDER BY created_at ASC, event_id ASC",
        )
        .bind(1_800_000_000i64)
        .bind(&target_hex)
        .bind(10i64)
        .fetch_all(&pool)
        .await
        .expect("explain");
        let details: Vec<String> = plan_rows
            .iter()
            .map(|r| Row::get::<String, _>(r, 3))
            .collect();
        assert!(
            details
                .iter()
                .any(|d| d.contains("bus_event_tags_kind_value")),
            "p predicate must be served by the tag index, plan:\n{}",
            details.join("\n")
        );

        PARSED_ROWS.store(0, std::sync::atomic::Ordering::Relaxed);
        let replayed = store
            .replay(&Filter::new().pubkey(target.public_key()).limit(10))
            .await
            .expect("replay");
        assert_eq!(replayed.len(), 5, "only the 5 target events match");
        assert_eq!(
            PARSED_ROWS.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "SQL prefilter must shrink the parsed set to the 5 matched rows"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm acceptance #2: limit=5 over 10k matching
    /// rows returns the NEWEST 5 without materializing 10k Events (the
    /// parse counter sees exactly 5 rows — SQL LIMIT on the DESC matched set).
    #[tokio::test]
    async fn test_replay_limit_pushdown_newest_5_of_10k() {
        use nostr_sdk::nostr::Filter;
        let (store, pool, path) = temp_store().await;
        let author = Keys::generate();
        let target = Keys::generate();
        let target_hex = target.public_key().to_hex();
        const N: usize = 10_000;

        let mut tx = pool.begin().await.expect("tx");
        for i in 0..N {
            let ev = build_event(
                &author,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                format!("target {i}"),
                vec![
                    Tag::parse(["p", &target_hex]).expect("p"),
                    Tag::parse(["t", "demo"]).expect("t"),
                ],
                Timestamp::from(1_700_000_000u64 + i as u64),
            )
            .expect("build");
            sqlx::query(
                "INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)",
            )
            .bind(ev.id.to_hex())
            .bind(ev.as_json())
            .bind(ev.created_at.as_secs() as i64)
            .bind(ev.kind.as_u16() as i64)
            .bind(ev.pubkey.to_hex())
            .execute(&mut *tx)
            .await
            .expect("bulk insert");
            sqlx::query(
                "INSERT INTO bus_event_tags (event_id, tag_kind, tag_value) VALUES (?1, 'p', ?2)",
            )
            .bind(ev.id.to_hex())
            .bind(&target_hex)
            .execute(&mut *tx)
            .await
            .expect("tag insert");
        }
        tx.commit().await.expect("commit");

        PARSED_ROWS.store(0, std::sync::atomic::Ordering::Relaxed);
        let replayed = store
            .replay(&Filter::new().pubkey(target.public_key()).limit(5))
            .await
            .expect("replay");
        assert_eq!(replayed.len(), 5);
        let contents: Vec<String> = replayed.iter().map(|e| e.content.clone()).collect();
        assert_eq!(
            contents,
            vec![
                "target 9995".to_string(),
                "target 9996".to_string(),
                "target 9997".to_string(),
                "target 9998".to_string(),
                "target 9999".to_string(),
            ],
            "limit=5 returns the NEWEST 5 in envelope order"
        );
        assert_eq!(
            PARSED_ROWS.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "only 5 rows parsed — no materialization of the 10k matched set"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm soundness: an event with SEVERAL p tags
    /// matches a filter on the non-first value (a first-p-tag column would
    /// miss it — the EAV index matches any position).
    #[tokio::test]
    async fn test_replay_p_tag_any_position_sound() {
        use nostr_sdk::nostr::Filter;
        let (store, _pool, path) = temp_store().await;
        let me = Keys::generate();
        let first = Keys::generate();
        let sender = Keys::generate();
        let decoy_author = Keys::generate();
        let other2 = Keys::generate();

        let multi = build_event(
            &sender,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "multi",
            vec![
                Tag::parse(["p", &first.public_key().to_hex()]).expect("p1"),
                Tag::parse(["p", &me.public_key().to_hex()]).expect("p2"),
            ],
            Timestamp::from(1_700_000_000u64),
        )
        .expect("build");
        let other_ev = build_event(
            &decoy_author,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "decoy",
            vec![Tag::parse(["p", &other2.public_key().to_hex()]).expect("p")],
            Timestamp::from(1_700_000_001u64),
        )
        .expect("build");
        assert!(store.insert(&multi).await.expect("insert multi"));
        assert!(store.insert(&other_ev).await.expect("insert decoy"));

        let mine = store
            .replay(&Filter::new().pubkey(me.public_key()))
            .await
            .expect("replay");
        assert_eq!(mine.len(), 1, "me matches only via the 2nd p tag");
        assert_eq!(mine[0].id, multi.id);

        let theirs = store
            .replay(&Filter::new().pubkey(first.public_key()))
            .await
            .expect("replay");
        assert_eq!(theirs.len(), 1, "the 1st p tag also matches");
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm soundness: e-tag filters match any e tag
    /// position (root of a reply chain is not necessarily the first e tag).
    #[tokio::test]
    async fn test_replay_e_tag_any_position_sound() {
        use nostr_sdk::nostr::Filter;
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let root: nostr_sdk::nostr::EventId = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "root",
            vec![],
            Timestamp::from(1_700_000_000u64),
        )
        .expect("root build")
        .id;
        let reply_id: nostr_sdk::nostr::EventId = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "reply",
            vec![],
            Timestamp::from(1_700_000_001u64),
        )
        .expect("reply build")
        .id;

        let reply = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "threaded",
            vec![
                Tag::parse(["e", &reply_id.to_hex()]).expect("e1"),
                Tag::parse(["e", &root.to_hex()]).expect("e2"),
            ],
            Timestamp::from(1_700_000_002u64),
        )
        .expect("build");
        let direct = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "direct",
            vec![Tag::parse(["e", &root.to_hex()]).expect("e1")],
            Timestamp::from(1_700_000_003u64),
        )
        .expect("build");
        assert!(store.insert(&reply).await.expect("insert reply"));
        assert!(store.insert(&direct).await.expect("insert direct"));

        let by_root = store
            .replay(&Filter::new().event(root))
            .await
            .expect("replay");
        assert_eq!(by_root.len(), 2, "both events reference the root");
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm backfill: a store opened before the tag
    /// index (no bus_event_tags rows) gets its existing events indexed once
    /// at open, so p-filtered replay works without re-inserting.
    #[tokio::test]
    async fn test_backfill_indexes_existing_events_on_open() {
        let (store, pool, path) = temp_store().await;
        let k1 = Keys::generate();
        let k2 = Keys::generate();
        let target = Keys::generate();
        let other = Keys::generate();
        let target_hex = target.public_key().to_hex();

        for (keys, content, p_hex) in [
            (&k1, "wanted", &target_hex),
            (&k2, "unwanted", &other.public_key().to_hex()),
            (&k1, "wanted2", &target_hex),
        ] {
            let ev = build_event(
                keys,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                content,
                vec![Tag::parse(["p", p_hex]).expect("p")],
                Timestamp::now(),
            )
            .expect("build");
            assert!(store.insert(&ev).await.expect("insert"));
        }

        // Simulate a pre-index store: drop the tag table (and its index).
        sqlx::raw_sql("DROP TABLE bus_event_tags")
            .execute(&pool)
            .await
            .expect("drop");

        // Reopen: the table is recreated and backfilled from raw_json.
        let reopened = BusEventStore::new(pool.clone()).await.expect("reopen");
        use nostr_sdk::nostr::Filter;
        let replayed = reopened
            .replay(&Filter::new().pubkey(target.public_key()))
            .await
            .expect("replay");
        assert_eq!(replayed.len(), 2, "backfilled rows serve the p filter");
        assert!(
            replayed.iter().all(|e| e.content.starts_with("wanted")),
            "only the target's events replay: {:?}",
            replayed
                .iter()
                .map(|e| e.content.clone())
                .collect::<Vec<_>>()
        );
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm: a filter with a NON-pushed predicate
    /// (#t project tag) must not let the SQL LIMIT truncate candidates
    /// before matching. The newest same-kind rows belong to another project;
    /// an unconditional LIMIT pushdown would drop the project's OLDER matched
    /// rows and replay empty. The drain over the prefiltered candidates
    /// preserves the bd-cumj matched-set limit.
    #[tokio::test]
    async fn test_replay_non_pushed_t_tag_keeps_older_matched_rows() {
        let (store, _pool, path) = temp_store().await;
        let author = Keys::generate();
        let kind = crate::nostr::kinds::toron_kind(PROJECT_MANIFEST);
        // The project's events are OLDER than 10 other-project events; with
        // limit=5 the SQL pushdown would keep only the newest 5 (all other).
        for i in 0..2 {
            let ev = build_event(
                &author,
                kind,
                format!("mine {i}"),
                vec![Tag::parse(["t", "demo"]).expect("t")],
                Timestamp::from(1_699_999_990u64 + i as u64),
            )
            .expect("build");
            assert!(store.insert(&ev).await.expect("insert"));
        }
        for i in 0..10 {
            let ev = build_event(
                &author,
                kind,
                format!("other {i}"),
                vec![Tag::parse(["t", "other"]).expect("t")],
                Timestamp::from(1_700_000_000u64 + i as u64),
            )
            .expect("build");
            assert!(store.insert(&ev).await.expect("insert"));
        }

        let filter = crate::bus::kind_project_filter(kind, "demo").limit(5);
        let replayed = store.replay(&filter).await.expect("replay");
        let contents: Vec<String> = replayed.iter().map(|e| e.content.clone()).collect();
        assert_eq!(
            contents,
            vec!["mine 0".to_string(), "mine 1".to_string()],
            "older #t-matched rows must survive a non-pushed-predicate filter"
        );
        let _ = std::fs::remove_file(&path);
    }
}
