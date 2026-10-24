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
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use crate::nostr::{Event, MatchEventOptions, SingleLetterTag, Timestamp};

/// Max rows kept in the bus store. Beyond this, the oldest envelopes are
/// pruned (bounded bus, permanent Git content — ADR-0003 superiority row).
pub const MAX_STORED_EVENTS: i64 = 100_000;

/// Durable coordination state (bd-fxzv): channel room (39000/39002),
/// project manifest (30010), agent profile (30013) and file reservation
/// (30031) rows are the OLDEST arrivals on a kind-9-flood-pinned bus, so
/// FIFO cap eviction ate them first (channel list empty, reads not-found,
/// agent rows flapped). These kinds are exempt from CAP eviction; the
/// expiry-GC delete above still applies to every kind.
const DURABLE_KINDS: [i64; 5] = [39000, 39002, 30010, 30013, 30031];

/// bd-py9o: entry cap for the hot parse memo. The store caps at
/// MAX_STORED_EVENTS rows; when the memo exceeds this it clears wholesale —
/// a memo, never the source of truth, so the worst case after a clear is one
/// cold re-parse per row on the next replay.
const HOT_CACHE_CAP: usize = 10_000;

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
-- Hot replay path: WHERE kind IN (?) ORDER BY created_at DESC, event_id DESC
-- needs a composite index to avoid TEMP B-TREE sort (1-5s → ms on 10k rows).
CREATE INDEX IF NOT EXISTS bus_events_kind_created ON bus_events (kind, created_at DESC, event_id DESC);
-- Expiry scan: WHERE expires_at IS NOT NULL / prune + snapshot-memo bypass
CREATE INDEX IF NOT EXISTS bus_events_expires ON bus_events (expires_at);
CREATE TABLE IF NOT EXISTS bus_meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
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

/// Arrival log (bd-wake-live-burst-loss-j86v, bd-wake-replay-cap-qk8n): one
/// row per stored event, numbered in commit order. `AUTOINCREMENT` makes the
/// sequence strictly monotonic and never reused, unlike `rowid`, which an
/// addressable supersede (delete-then-insert of the newest row) can recycle.
/// The wake socket's resume cursor is this sequence: `created_at` is protocol
/// metadata (second resolution, randomized for gift wraps) and cannot order
/// arrivals.
pub const EVENT_ARRIVAL_DDL: &str = "
CREATE TABLE IF NOT EXISTS bus_arrivals (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE
);
";

/// `bus_meta` key: the highest arrival seq the cap has evicted. A wake cursor
/// below it may have lost rows, and is told so with an `events_lost` frame.
pub const ARRIVALS_EVICTED_KEY: &str = "arrivals_evicted_hwm";

/// bd-cgyh: how many store mutations may pass between ANALYZE passes. The bus
/// turns over completely (the cap evicts while arrivals land), so statistics
/// written once stop describing the store and the poll's plan is what pays.
const STATS_MUTATIONS_PER_ANALYZE: i64 = 10_000;

/// `bus_meta` key: the highest `bus_events.rowid` already given an arrival
/// seq (bd-dtua).
///
/// The marker is DURABLE because the work it guards is not: the arrival log
/// persists, so a daemon that restarts must not re-insert every event into it.
/// In memory the marker started at 0 on every open, so each restart ran a
/// 100,000-row INSERT - and with 230 live subscribers every one of them
/// triggered it before the in-memory marker advanced, so the pool was buried
/// under 230 copies of the same insert while the MCP endpoint timed out.
pub const ARRIVALS_HEALED_ROWID_KEY: &str = "arrivals_healed_rowid";

/// `bus_meta` key: the `mutations` value the last ANALYZE of the bus tables ran
/// at. Absent = this store has never refreshed its planner statistics.
const STATS_ANALYZED_AT_KEY: &str = "stats_analyzed_at";

/// A recorded `bus_events` row count this many times away from the live one
/// means the statistics describe an older, smaller store. The live index was
/// 10x under (9,630 recorded, 100,000 held), so the factor is deliberately
/// loose: the point is to catch drift, not to re-analyze on every open.
const STATS_DRIFT_FACTOR: i64 = 2;

/// bd-3gwy: entry cap for the per-filter snapshot memo. Fleet polling
/// rotates dozens of distinct inbox filters; 64 slots keeps every active
/// filter's matched-id list warm. Overflow evicts the oldest entry.
const SNAPSHOT_MEMO_CAP: usize = 64;

/// NIP-40: an event whose `expiration` tag is at or before now must never
/// surface from replay, even when a snapshot memo still lists its id
/// (the row sits until prune, ≤ PRUNE_INTERVAL).
fn event_is_expired(event: &Event) -> bool {
    event
        .tags
        .expiration()
        .is_some_and(|exp| exp.as_secs() <= Timestamp::now().as_secs())
}

/// Exact-compare key for the snapshot memo: everything that decides the
/// phase-1 SQL (and therefore the matched id set). Hashed for lookup,
/// compared for safety — a hash collision can never serve another filter's
/// ids. Non-pushed predicates (#t, since/until, authors, ids, search) ride
/// in `extra` so distinct filters never share an entry.
#[derive(Clone, PartialEq, Eq, Hash)]
struct FilterKey {
    kinds: Vec<i64>,
    p: Vec<String>,
    e: Vec<String>,
    limit: usize,
    sql_exact: bool,
    extra: String,
}

/// One memoized snapshot: the ordered matched-id list plus the store
/// version it was computed at. The version lives IN SQLite (`bus_meta`,
/// bd-3gwy) — every writer instance bumps it through SQL and every reader
/// compares against the same authoritative number, so instances over one
/// pool/file invalidate each other correctly with no shared registry.
struct SnapMemo {
    key: FilterKey,
    version: i64,
    ids: Arc<Vec<String>>,
}

#[derive(Default)]
struct SnapMemos {
    map: HashMap<u64, SnapMemo>,
    order: std::collections::VecDeque<u64>,
}

impl SnapMemos {
    fn get(&self, hash: u64) -> Option<&SnapMemo> {
        self.map.get(&hash)
    }

    fn put(&mut self, hash: u64, memo: SnapMemo) {
        if !self.map.contains_key(&hash)
            && self.map.len() >= SNAPSHOT_MEMO_CAP
            && let Some(oldest) = self.order.pop_front()
        {
            self.map.remove(&oldest);
        }
        self.map.insert(hash, memo);
        self.order.retain(|h| *h != hash);
        self.order.push_back(hash);
    }
}

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
    /// bd-3gwy: per-filter snapshot memo. Validity is checked against the
    /// authoritative mutation counter in `bus_meta` (SQL): any insert /
    /// supersede / prune bumps it, so every instance invalidates. Expired
    /// rows do not bypass the memo (bd-741i); hydrate skips them.
    snaps: Arc<parking_lot::Mutex<SnapMemos>>,
    /// bd-py9o/bd-3gwy measurement counters (per instance — globals would
    /// cross-contaminate under parallel tests). Rows parsed from raw_json,
    /// rows hydrated from the hot memo, phase-1 id-scans executed.
    parsed_rows: Arc<AtomicU64>,
    hot_hits: Arc<AtomicU64>,
    sql_scans: Arc<AtomicU64>,
    /// Highest `bus_events.rowid` already given an arrival seq. Rows past it
    /// were written by a binary that predates the arrival log and are healed
    /// in on the next arrival read (see [`Self::heal_arrivals`]).
    healed_rowid: Arc<std::sync::atomic::AtomicI64>,
    /// bd-dtua: how many times this store RAN the arrival insert. The insert
    /// is OR IGNORE, so a re-insert changes no row and only the counter
    /// shows the work was done again.
    heal_inserts: Arc<AtomicU64>,
    /// bd-dtua: the heal runs ONCE at a time. Every subscriber drains the
    /// arrival log, and every drain heals it first, so without this lock a
    /// fleet of subscribers all read the same marker and all run the same
    /// insert before the marker advances.
    heal_lock: Arc<tokio::sync::Mutex<()>>,
}

impl BusEventStore {
    /// Open the store against an existing pool (table created idempotently).
    pub async fn new(pool: SqlitePool) -> anyhow::Result<Self> {
        sqlx::raw_sql(EVENT_TABLE_DDL).execute(&pool).await?;
        sqlx::raw_sql(EVENT_TAG_TABLE_DDL).execute(&pool).await?;
        Self::backfill_tag_index(&pool).await?;
        sqlx::raw_sql(EVENT_ARRIVAL_DDL).execute(&pool).await?;
        let store = Self {
            pool,
            hot: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            snaps: Arc::new(parking_lot::Mutex::new(SnapMemos::default())),
            parsed_rows: Arc::new(AtomicU64::new(0)),
            hot_hits: Arc::new(AtomicU64::new(0)),
            sql_scans: Arc::new(AtomicU64::new(0)),
            healed_rowid: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            heal_lock: Arc::new(tokio::sync::Mutex::new(())),
            heal_inserts: Arc::new(AtomicU64::new(0)),
        };
        // bd-dtua: the arrival log persists, so the marker does too. Seeding
        // it from the store is what stops a restart from re-inserting every
        // event (100,000 rows on the live bus) - and with a fleet of
        // subscribers each triggering the heal, that insert buried the pool
        // and the daemon stopped answering.
        let durable = store.durable_healed_rowid().await;
        store
            .healed_rowid
            .store(durable, std::sync::atomic::Ordering::Release);
        // Number every row that predates the arrival log, in arrival
        // (rowid) order, so a resume cursor can reach all of them.
        store.heal_arrivals().await?;
        // bd-cgyh: an open is the one moment a drifted store is guaranteed to
        // be seen after it may have run unwatched (a daemon restart), and the
        // check is one count plus one stat read. Loud on failure, never a
        // refused boot: a store that cannot analyze is not a store to die on.
        if store.planner_stats_are_stale().await? {
            let mutations = store.current_mutations().await;
            if let Err(e) = store.analyze_bus_tables(mutations).await {
                tracing::warn!("bus planner stats refresh at open failed: {e}");
            }
        }
        Ok(store)
    }

    /// Give an arrival seq to every stored event that lacks one, in rowid
    /// order. Rows written by this binary already have one (same
    /// transaction as the insert); this covers rows from an older binary on
    /// the same file. Cost is proportional to rows past the last heal.
    pub(crate) async fn heal_arrivals(&self) -> anyhow::Result<()> {
        use std::sync::atomic::Ordering;
        // bd-dtua: one heal at a time. The drain is called by every
        // subscriber, and each drain heals the log first, so without this
        // lock a fleet of subscribers reads the same marker and all runs the
        // same insert before the marker advances - 230 copies of a 100,000-row
        // insert, with the pool buried under them while the MCP endpoint
        // times out.
        let _guard = self.heal_lock.lock().await;
        let max_rowid: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(rowid), 0) FROM bus_events")
            .fetch_one(&self.pool)
            .await?;
        let healed = self.healed_rowid.load(Ordering::Acquire);
        if max_rowid <= healed {
            return Ok(());
        }
        sqlx::query(
            "INSERT OR IGNORE INTO bus_arrivals (event_id)
             SELECT event_id FROM bus_events WHERE rowid > ?1 ORDER BY rowid ASC",
        )
        .bind(healed)
        .execute(&self.pool)
        .await?;
        self.heal_inserts.fetch_add(1, Ordering::AcqRel);
        self.healed_rowid.fetch_max(max_rowid, Ordering::AcqRel);
        // The marker is durable: the arrival log persists, so the next open
        // must not insert the same rows again.
        self.persist_healed_rowid(max_rowid).await?;
        Ok(())
    }

    /// The durable heal marker (bd-dtua): the highest `bus_events.rowid` the
    /// arrival log already covers.
    async fn persist_healed_rowid(&self, rowid: i64) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO bus_meta (key, value) VALUES (?1, ?2)
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        )
        .bind(ARRIVALS_HEALED_ROWID_KEY)
        .bind(rowid)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The durable heal marker as the store reads it at open (bd-dtua).
    async fn durable_healed_rowid(&self) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE((SELECT value FROM bus_meta WHERE key = ?1), 0)",
        )
        .bind(ARRIVALS_HEALED_ROWID_KEY)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0)
    }

    /// Stored events with an arrival seq strictly after `after`, oldest
    /// first, at most `limit` rows, restricted to `kinds` (empty = all) and
    /// to unexpired rows. Paging with the last returned seq walks the whole
    /// history; no newest-N window applies.
    pub async fn arrivals_after(
        &self,
        kinds: &[u16],
        after: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<(u64, Event)>> {
        self.heal_arrivals().await?;
        let sql = Self::arrivals_after_sql(kinds.len());
        let mut q = sqlx::query_as::<_, (i64, String)>(&sql)
            .bind(i64::try_from(after).unwrap_or(i64::MAX))
            .bind(Timestamp::now().as_secs() as i64)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX));
        for k in kinds {
            q = q.bind(i64::from(*k));
        }
        let rows = q.fetch_all(&self.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for (seq, raw) in rows {
            match Event::from_json(&raw) {
                Ok(ev) => out.push((seq as u64, ev)),
                // A row we cannot parse is still an arrival: surface it in
                // the log rather than pretend it never came.
                Err(e) => tracing::warn!(seq, "unparseable stored event skipped: {e}"),
            }
        }
        Ok(out)
    }

    /// The poll's SQL (bd-cgyh). One source for the store and for the plan
    /// witness that pins it: the arrival cursor IS the order, so the shape here
    /// is what keeps the seq-ordered walk (and its early stop at the LIMIT)
    /// available — a witness against a copy of the string would drift from the
    /// query the daemon actually runs.
    fn arrivals_after_sql(n_kinds: usize) -> String {
        let kind_pred = if n_kinds == 0 {
            String::new()
        } else {
            let ph: Vec<String> = (0..n_kinds).map(|i| format!("?{}", i + 4)).collect();
            format!(" AND e.kind IN ({})", ph.join(","))
        };
        format!(
            "SELECT a.seq, e.raw_json FROM bus_arrivals a
             JOIN bus_events e ON e.event_id = a.event_id
             WHERE a.seq > ?1 AND (e.expires_at IS NULL OR e.expires_at > ?2){kind_pred}
             ORDER BY a.seq ASC LIMIT ?3"
        )
    }

    /// The arrival seq of a stored event id, if it has one.
    pub async fn arrival_seq_of(&self, event_id: &str) -> anyhow::Result<Option<u64>> {
        self.heal_arrivals().await?;
        let seq: Option<i64> =
            sqlx::query_scalar("SELECT seq FROM bus_arrivals WHERE event_id = ?1")
                .bind(event_id.to_ascii_lowercase())
                .fetch_optional(&self.pool)
                .await?;
        Ok(seq.map(|s| s as u64))
    }

    /// Highest arrival seq the cap has evicted (0 = none).
    pub async fn arrivals_evicted_through(&self) -> anyhow::Result<u64> {
        let v: i64 =
            sqlx::query_scalar("SELECT COALESCE((SELECT value FROM bus_meta WHERE key = ?1), 0)")
                .bind(ARRIVALS_EVICTED_KEY)
                .fetch_one(&self.pool)
                .await?;
        Ok(v.max(0) as u64)
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
                "p" => ("p", tag.content()),
                "e" => ("e", tag.content()),
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
        // One transaction: the row, its arrival seq and its tag index commit
        // together, so a reader never sees a stored event without its place
        // in arrival order, and seq order is commit order (the write lock is
        // held from the first statement to commit).
        let mut tx = self.pool.begin().await?;
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
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() == 1 {
            // A re-insert after a delete is a new arrival: drop any stale
            // seq so the event gets a fresh one past every live cursor.
            sqlx::query("DELETE FROM bus_arrivals WHERE event_id = ?1")
                .bind(event.id.to_hex())
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO bus_arrivals (event_id) VALUES (?1)")
                .bind(event.id.to_hex())
                .execute(&mut *tx)
                .await?;
        }

        // p/e tag index (bd-replay-tag-pushdown-lurm): written unconditionally
        // so a duplicate insert still converges (ON CONFLICT DO NOTHING).
        Self::index_tag_values(&mut tx, &event.id.to_hex(), event).await?;
        tx.commit().await?;

        // ON CONFLICT DO NOTHING reports 0 affected rows for duplicates.
        if result.rows_affected() == 1 {
            self.hot_put(event);
            self.bump_mutations().await;
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
            let incumbents: Vec<(String, String)> = sqlx::query_as(
                "SELECT event_id, raw_json FROM bus_events WHERE kind = ?1 AND author = ?2 AND d_tag = ?3",
            )
            .bind(key.kind as i64)
            .bind(&key.pubkey)
            .bind(&key.d)
            .fetch_all(&self.pool)
            .await
            .unwrap_or_default();
            for (_id, raw) in &incumbents {
                if let Ok(existing) = Event::from_json(raw)
                    && event.created_at.as_secs() < existing.created_at.as_secs()
                {
                    // Older replay must not overwrite a newer coordinate.
                    // Same-second upserts last-write-win: `Timestamp::now()` is
                    // 1s granularity and 30013/30031 renewals land in-process
                    // in the same second. Lowest-id ties live in projection
                    // (`nip01_replaceable_cmp`), not the write path.
                    return Ok(false);
                }
            }
            // Evict the superseded events from the hot memo BEFORE the delete
            // (bd-py9o: stale memo entries must never outlive their rows).
            if !incumbents.is_empty() {
                let mut hot = self.hot.lock();
                for (id, _) in &incumbents {
                    hot.remove(id);
                }
            }
            sqlx::query("DELETE FROM bus_events WHERE kind = ?1 AND author = ?2 AND d_tag = ?3")
                .bind(key.kind as i64)
                .bind(&key.pubkey)
                .bind(&key.d)
                .execute(&self.pool)
                .await?;
            if !incumbents.is_empty() {
                self.bump_mutations().await;
            }
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

        // Predicate pieces, composed inside scan_ids (bd-bus-scan): ?1 expiry,
        // then a kind list (`kind IN (…)`, single combined query) OR a
        // per-kind split (`kind = ?2`, index-served), then p values, then e
        // values, then the trailing limit. Placeholders are renumbered inside
        // scan_ids per branch, so only the tag VALUES are passed here.
        let expiry_pred = "(expires_at IS NULL OR expires_at > ?1)".to_string();
        let mut kinds_binds: Vec<i64> = Vec::new();

        let kind_in_pred: Option<String> = if let Some(kinds) = &filter.kinds
            && !kinds.is_empty()
        {
            let placeholders: Vec<String> = (2..2 + kinds.len()).map(|i| format!("?{i}")).collect();
            kinds_binds = kinds.iter().map(|k| k.as_u16() as i64).collect();
            Some(format!("kind IN ({})", placeholders.join(",")))
        } else {
            None
        };

        let p_vals: Vec<String> = filter
            .generic_tags
            .get(&SingleLetterTag::LOWERCASE_P)
            .map(|vals| vals.iter().cloned().collect())
            .unwrap_or_default();
        let e_vals: Vec<String> = filter
            .generic_tags
            .get(&SingleLetterTag::LOWERCASE_E)
            .map(|vals| vals.iter().cloned().collect())
            .unwrap_or_default();

        // bd-cumj newest-tail semantics: the scan takes the NEWEST `limit` of
        // the matched set (never truncating the latest state), and re-emits
        // it in ASC envelope order. The matched set is never materialized
        // beyond `limit` rows.
        //
        // LIMIT is pushed only when the SQL predicate set FULLY decides the
        // match (expiry + kinds + #p/#e). Filters carrying non-pushed
        // predicates (#t project tags, since/until, authors, ids, search)
        // must NOT push the limit: SQL would truncate CANDIDATES before
        // match_event runs, silently dropping older matched rows (a project
        // replay whose newest same-kind rows belong to other projects).
        // Those filters keep the post-parse drain over the prefiltered
        // candidate set (still shrunk by kinds/#p/#e, per the bead).
        let sql_exact = filter.ids.as_ref().is_none_or(BTreeSet::is_empty)
            && filter.authors.as_ref().is_none_or(BTreeSet::is_empty)
            && filter.search.is_none()
            && filter.since.is_none()
            && filter.until.is_none()
            && filter.generic_tags.iter().all(|(tag, vals)| {
                !vals.is_empty()
                    && (tag == &SingleLetterTag::LOWERCASE_P
                        || tag == &SingleLetterTag::LOWERCASE_E)
            });
        // bd-3gwy: snapshot memo key — every input that decides the phase-1
        // result. Non-pushed predicates ride in `extra` (they change which
        // rows `match_event` accepts, so distinct values must never share a
        // memo entry).
        let mut extra_tags: Vec<(String, Vec<String>)> = filter
            .generic_tags
            .iter()
            .filter(|(tag, vals)| {
                !vals.is_empty()
                    && *tag != &SingleLetterTag::LOWERCASE_P
                    && *tag != &SingleLetterTag::LOWERCASE_E
            })
            .map(|(tag, vals)| (format!("{tag:?}"), vals.iter().cloned().collect()))
            .collect();
        extra_tags.sort();
        let fkey = FilterKey {
            kinds: kinds_binds.clone(),
            p: p_vals.clone(),
            e: e_vals.clone(),
            limit,
            sql_exact,
            extra: format!(
                "ids={:?}|authors={:?}|search={:?}|since={:?}|until={:?}|tags={extra_tags:?}",
                filter.ids, filter.authors, filter.search, filter.since, filter.until
            ),
        };
        let fhash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            fkey.hash(&mut h);
            h.finish()
        };

        // bd-3gwy / bd-741i: snapshot memo — identical filter + unchanged
        // store version ⇒ reuse the matched-id list and skip phase-1 SQL.
        // Expired rows are NOT a memo-bypass: scan_ids already drops them
        // (`expires_at IS NULL OR expires_at > now`) and hydrate skips
        // `expiration() <= now`. Prune (60s) bumps the version. Bypassing
        // the memo for any stale TTL row forced every fleet poll / `toron
        // watch` onto a full scan and spilled 128GB RSS overnight.
        let current = self.current_mutations().await;
        let memo_ids = self
            .snaps
            .lock()
            .get(fhash)
            .and_then(|m| (m.version == current && m.key == fkey).then(|| m.ids.clone()));
        let ids: Arc<Vec<String>> = match memo_ids {
            Some(ids) => ids,
            None => {
                self.sql_scans
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let v0 = self.current_mutations().await;
                let scanned = self
                    .scan_ids(
                        &expiry_pred,
                        kind_in_pred.as_deref(),
                        &kinds_binds,
                        &p_vals,
                        &e_vals,
                        sql_exact,
                        limit,
                    )
                    .await?;
                let v1 = self.current_mutations().await;
                if v0 == v1 {
                    self.snaps.lock().put(
                        fhash,
                        SnapMemo {
                            key: fkey,
                            version: v1,
                            ids: Arc::new(scanned.clone()),
                        },
                    );
                }
                Arc::new(scanned)
            }
        };

        let mut resolved: HashMap<String, Event> = HashMap::with_capacity(ids.len());
        let mut cold_ids: Vec<String> = Vec::new();
        {
            let hot = self.hot.lock();
            for id in ids.iter() {
                match hot.get(id) {
                    Some(ev) => {
                        self.hot_hits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            for (id, raw) in rows {
                self.parsed_rows
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let Ok(ev) = Event::from_json(&raw) {
                    // Tight per-row guard (significant-drop): the JSON parse
                    // above runs lock-free; only the cap check + insert hold
                    // the map, so readers never wait out 500 parses.
                    {
                        let mut hot = self.hot.lock();
                        if hot.len() >= HOT_CACHE_CAP {
                            hot.clear();
                        }
                        hot.insert(id.clone(), Arc::new(ev.clone()));
                    }
                    resolved.insert(id, ev);
                }
            }
        }

        let mut out: Vec<Event> = Vec::with_capacity(ids.len());
        for id in ids.iter() {
            if let Some(ev) = resolved.get(id)
                && !event_is_expired(ev)
                && filter.match_event(ev, MatchEventOptions::default())
            {
                out.push(ev.clone());
            }
        }
        if !sql_exact && out.len() > limit {
            out.drain(0..out.len() - limit);
        }
        Ok(out)
    }

    /// bd-py9o/bd-3gwy measurement counters (per instance).
    pub fn parsed_rows(&self) -> u64 {
        self.parsed_rows.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn hot_hits(&self) -> u64 {
        self.hot_hits.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn sql_scans(&self) -> u64 {
        self.sql_scans.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Count of stored events (diagnostics / health).
    pub async fn count(&self) -> anyhow::Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) FROM bus_events")
            .fetch_one(&self.pool)
            .await?;
        Ok(Row::get(&row, 0))
    }

    /// Refresh the planner's picture of the bus when it has drifted (bd-cgyh).
    ///
    /// The bus DDL is `CREATE TABLE IF NOT EXISTS` and nothing re-runs ANALYZE
    /// as the store turns over, so `sqlite_stat1` can end up describing a much
    /// smaller, older store. Measured 2026-10-18 on the live 297 MB index: the
    /// stats said 9,630 rows while the store held 100,000, and the poll's plan
    /// became `MULTI-INDEX OR (bus_events_expires)` + `TEMP B-TREE FOR ORDER
    /// BY` — every call materialised ~99k rows (1.0-3.4 s of CPU each, daemon
    /// pinned at 400%) instead of walking `bus_arrivals.seq` and stopping at
    /// the LIMIT. Fresh stats restore the walk (ms).
    ///
    /// Rate-limited on purpose: ANALYZE of the two bus tables is ~46 ms on that
    /// store, so it rides turnover every [`STATS_MUTATIONS_PER_ANALYZE`]
    /// mutations, never the poll.
    pub async fn refresh_planner_stats_if_stale(&self) -> anyhow::Result<bool> {
        let mutations = self.current_mutations().await;
        let due = match self.stats_analyzed_at().await? {
            None => true,
            Some(at) => mutations.saturating_sub(at) >= STATS_MUTATIONS_PER_ANALYZE,
        };
        if !due {
            return Ok(false);
        }
        self.analyze_bus_tables(mutations).await?;
        Ok(true)
    }

    /// ANALYZE exactly the two tables the poll joins — never the whole store:
    /// `messages` and the FTS indexes belong to other readers, and analyzing
    /// them costs seconds for nothing here. The marker is written last, so a
    /// crash mid-analyze simply re-runs next tick.
    async fn analyze_bus_tables(&self, mutations: i64) -> anyhow::Result<()> {
        sqlx::raw_sql("ANALYZE bus_events; ANALYZE bus_arrivals;")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "INSERT INTO bus_meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(STATS_ANALYZED_AT_KEY)
        .bind(mutations)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The `mutations` value the last ANALYZE recorded (`None` = never).
    async fn stats_analyzed_at(&self) -> anyhow::Result<Option<i64>> {
        let v: Option<i64> = sqlx::query_scalar("SELECT value FROM bus_meta WHERE key = ?1")
            .bind(STATS_ANALYZED_AT_KEY)
            .fetch_optional(&self.pool)
            .await?;
        Ok(v)
    }

    /// True when `sqlite_stat1` cannot be trusted about `bus_events`: no
    /// recorded row count at all, or one the live count has outgrown by
    /// [`STATS_DRIFT_FACTOR`]. This is the open-time plausibility check — a
    /// restart is when a store that drifted while nothing looked is first seen.
    /// Cost is one index-only count plus one `sqlite_stat1` read (~2 ms on the
    /// live store), and a missing `sqlite_stat1` is itself the stale answer.
    async fn planner_stats_are_stale(&self) -> anyhow::Result<bool> {
        let live = self.count().await?;
        Ok(match self.recorded_stat_rows().await {
            None => true,
            Some(recorded) => {
                live > 0
                    && (recorded * STATS_DRIFT_FACTOR < live
                        || recorded > live * STATS_DRIFT_FACTOR)
            }
        })
    }

    /// The `bus_events` row count `sqlite_stat1` describes, or `None` when the
    /// table has never been analyzed. Every stat row carries the table's row
    /// count as its first number, and there may be no `idx IS NULL` row at all:
    /// ANALYZE wrote none on the live store or in the hermetic witness, so the
    /// first number of ANY row is the one to read (a predicate on the null-index
    /// row reports "never analyzed" forever and re-analyzes on every open).
    async fn recorded_stat_rows(&self) -> Option<i64> {
        sqlx::query_scalar(
            "SELECT MAX(CAST(substr(stat, 1, instr(stat, ' ') - 1) AS INTEGER))
             FROM sqlite_stat1 WHERE tbl = 'bus_events'",
        )
        .fetch_one(&self.pool)
        .await
        .ok()
    }

    /// Cheap SQLite connectivity probe (`SELECT 1`) for presence fail-closed
    /// (GS-062 / Buzz #7532 pattern — never Redis). `Ok(())` = store reachable.
    pub async fn ping(&self) -> anyhow::Result<()> {
        let _: i64 = sqlx::query_scalar("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    /// True when [`Self::ping`] succeeds.
    pub async fn is_healthy(&self) -> bool {
        self.ping().await.is_ok()
    }

    /// Operator-grade local removal (bd-channel-close-purge-bsqb): delete the
    /// stored events with these hex ids, their tag rows, their hot-memo
    /// entries, and bump the authoritative mutation version so every
    /// instance's snapshot memo invalidates. Returns rows removed. The bus is
    /// local-first SQLite — this is the same authority `prune` uses, scoped
    /// to explicit ids instead of age.
    pub async fn purge_ids(&self, ids: &[String]) -> anyhow::Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }
        let mut total: u64 = 0;
        // Chunk to bound SQL bind params; channel lifecycles are small but a
        // purge must not blow the parameter limit on a busy bus.
        for chunk in ids.chunks(500) {
            let ph = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let mut tx = self.pool.begin().await?;
            let del_sql = format!("DELETE FROM bus_events WHERE event_id IN ({ph})");
            let mut q = sqlx::query(&del_sql);
            for id in chunk {
                q = q.bind(id);
            }
            total += q.execute(&mut *tx).await?.rows_affected();
            let del_tag_sql = format!("DELETE FROM bus_event_tags WHERE event_id IN ({ph})");
            let mut qt = sqlx::query(&del_tag_sql);
            for id in chunk {
                qt = qt.bind(id);
            }
            qt.execute(&mut *tx).await?;
            tx.commit().await?;
        }
        self.bump_mutations().await;
        self.hot.lock().retain(|id, _| !ids.contains(id));
        Ok(total)
    }

    /// Prune expired envelopes + the oldest beyond the cap. Best-effort.
    pub async fn prune(&self) -> anyhow::Result<()> {
        // bd-cgyh: prune is the store's own periodic tick, so the planner's
        // picture is refreshed here rather than on the poll path. It declines
        // on two small reads when the mutation count has not moved far.
        if let Err(e) = self.refresh_planner_stats_if_stale().await {
            tracing::warn!("bus planner stats refresh failed: {e}");
        }
        sqlx::query("DELETE FROM bus_events WHERE expires_at IS NOT NULL AND expires_at <= ?1")
            .bind(Timestamp::now().as_secs() as i64)
            .execute(&self.pool)
            .await?;
        // bd-eifs: evict FIFO BY ARRIVAL (rowid), never by `created_at`.
        // NIP-17/NIP-59 gift wraps carry a randomized created_at up to 2 days
        // in the past for metadata privacy — timestamp-ordered eviction made
        // a just-arrived mail wrap the OLDEST row in a full store and pruned
        // it before any reader saw it, while send had already reported
        // success. Storage age is arrival order; created_at is protocol
        // metadata only.
        // bd-fxzv: cap victims are the oldest NON-DURABLE arrivals (rowid).
        // Durable coordination state (39000/39002 rooms, 30010 manifests,
        // 30013 profiles, 30031 reservations) never dates the cap — a
        // kind-9 flood would otherwise evict the rooms first. If the store
        // is over cap with nothing but durable rows, it stays over cap:
        // losing the rooms is worse than breaching the bound.
        //
        // bd-wake-live-burst-loss-j86v: cap eviction is the one way a stored
        // event leaves before every reader saw it. Record the highest evicted
        // arrival seq in the SAME transaction as the delete, so a wake cursor
        // behind it is told `events_lost` instead of silently skipping rows.
        self.heal_arrivals().await?;
        const VICTIMS: &str = "SELECT event_id FROM bus_events
                WHERE kind NOT IN (?2, ?3, ?4, ?5, ?6)
                ORDER BY rowid ASC
                LIMIT MAX(0, (SELECT COUNT(*) FROM bus_events) - ?1)";
        let mut tx = self.pool.begin().await?;
        sqlx::query(&format!(
            "INSERT INTO bus_meta (key, value)
             SELECT ?7, MAX(a.seq) FROM bus_arrivals a WHERE a.event_id IN ({VICTIMS})
             AND a.seq IS NOT NULL
             GROUP BY 1 HAVING MAX(a.seq) IS NOT NULL
             ON CONFLICT(key) DO UPDATE SET value = MAX(value, excluded.value)"
        ))
        .bind(MAX_STORED_EVENTS)
        .bind(DURABLE_KINDS[0])
        .bind(DURABLE_KINDS[1])
        .bind(DURABLE_KINDS[2])
        .bind(DURABLE_KINDS[3])
        .bind(DURABLE_KINDS[4])
        .bind(ARRIVALS_EVICTED_KEY)
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "DELETE FROM bus_events WHERE event_id IN ({VICTIMS})"
        ))
        .bind(MAX_STORED_EVENTS)
        .bind(DURABLE_KINDS[0])
        .bind(DURABLE_KINDS[1])
        .bind(DURABLE_KINDS[2])
        .bind(DURABLE_KINDS[3])
        .bind(DURABLE_KINDS[4])
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        // Arrival rows of events no longer stored (evicted, expired,
        // superseded, purged) can never be served again. AUTOINCREMENT keeps
        // their numbers retired.
        sqlx::query(
            "DELETE FROM bus_arrivals WHERE event_id NOT IN (SELECT event_id FROM bus_events)",
        )
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
        // bd-3gwy: pruned rows must never be served from a stale snapshot.
        self.bump_mutations().await;
        // Storage-leak guard: checkpoint WAL after prune so the -wal file
        // doesn't grow unbounded (prior 4 MB wal survived until reboot).
        let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.pool)
            .await;
        Ok(())
    }

    /// Bump the authoritative store mutation version in `bus_meta`,
    /// invalidating every snapshot memo entry of EVERY instance over this
    /// database at once (bd-3gwy). Best-effort: a failed bump only costs a
    /// missed invalidation window that the next successful write closes.
    async fn bump_mutations(&self) {
        let _ = sqlx::query(
            "INSERT INTO bus_meta (key, value) VALUES ('mutations', 1) \
             ON CONFLICT(key) DO UPDATE SET value = value + 1",
        )
        .execute(&self.pool)
        .await;
    }

    /// Authoritative mutation version from `bus_meta` (bd-3gwy).
    ///
    /// Read by [`crate::bus::Bus::store_version`] so a poll-driven consumer
    /// can tell a quiet store from a changed one in one indexed read.
    pub(crate) async fn current_mutations(&self) -> i64 {
        sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE((SELECT value FROM bus_meta WHERE key = 'mutations'), 0)",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0)
    }

    /// Phase-1 id-scan — the only query the snapshot memo skips (bd-3gwy).
    /// Newest-tail `limit` when the predicate set fully decides the match
    /// (bd-cumj), re-emitted in ASC envelope order.
    ///
    /// bd-bus-scan: SQLite will not merge a `kind IN (…)` range with
    /// `ORDER BY created_at DESC` through the composite index — the plan
    /// degrades to a MULTI-INDEX OR walk of EVERY row (via
    /// `bus_events_expires`) plus a TEMP B-TREE sort, which on a large store
    /// spilled 48–128 GB RSS overnight (bd-741i) and produced the 1–2 s
    /// replay queries. Splitting into one query per kind turns each into an
    /// index-served walk of `bus_events_kind_created` (single-kind shape
    /// verified via EXPLAIN QUERY PLAN); the per-kind DESC lists are merged
    /// in Rust with the identical `(created_at, event_id)` comparator,
    /// truncated to `limit` when pushdown is exact, and re-emitted ASC.
    /// (8 bind-related params; clippy allowance matches mailbox convention.)
    #[allow(clippy::too_many_arguments)]
    async fn scan_ids(
        &self,
        expiry_pred: &str,
        kind_in_pred: Option<&str>,
        kinds: &[i64],
        p_vals: &[String],
        e_vals: &[String],
        sql_exact: bool,
        limit: usize,
    ) -> anyhow::Result<Vec<String>> {
        // p/e subquery parts, placeholder-numbered per branch (bind order:
        // ?1 expiry, then [?2 kind], then p values, then e values, then limit).
        let p_part = |start: usize| -> (String, usize) {
            if p_vals.is_empty() {
                return (String::new(), start);
            }
            let ph: Vec<String> = (start..start + p_vals.len())
                .map(|i| format!("?{i}"))
                .collect();
            (
                format!(
                    " AND event_id IN (SELECT event_id FROM bus_event_tags \
                     WHERE tag_kind = 'p' AND tag_value IN ({}))",
                    ph.join(",")
                ),
                start + p_vals.len(),
            )
        };
        let e_part = |start: usize| -> (String, usize) {
            if e_vals.is_empty() {
                return (String::new(), start);
            }
            let ph: Vec<String> = (start..start + e_vals.len())
                .map(|i| format!("?{i}"))
                .collect();
            (
                format!(
                    " AND event_id IN (SELECT event_id FROM bus_event_tags \
                     WHERE tag_kind = 'e' AND tag_value IN ({}))",
                    ph.join(",")
                ),
                start + e_vals.len(),
            )
        };
        let limit_clause = |next: usize| {
            if sql_exact {
                format!(" LIMIT ?{next}")
            } else {
                String::new()
            }
        };
        let now = Timestamp::now().as_secs() as i64;

        // (event_id, created_at) rows in DESC order (the merge input).
        let mut rows: Vec<(String, i64)> = Vec::new();
        match kind_in_pred {
            None => {
                // No kind filter: single combined query. Kinds-empty filters
                // are rare, so the un-splittable plan is acceptable here.
                let (p_part, after_p) = p_part(2);
                let (e_part, after_e) = e_part(after_p);
                let next = after_e;
                let sql = format!(
                    "SELECT event_id, created_at FROM bus_events
                     WHERE {expiry_pred}{p_part}{e_part}
                     ORDER BY created_at DESC, event_id DESC{}",
                    limit_clause(next)
                );
                let mut q = sqlx::query_as::<_, (String, i64)>(&sql).bind(now);
                for b in p_vals {
                    q = q.bind(b.as_str());
                }
                for b in e_vals {
                    q = q.bind(b.as_str());
                }
                if sql_exact {
                    q = q.bind(limit as i64);
                }
                rows = q.fetch_all(&self.pool).await?;
            }
            Some(_) => {
                // Per-kind split: each kind gets its own index-served query
                // (`kind = ?2`, still filtered by expiry + p/e), so no row of
                // a non-requested kind is ever walked and ORDER BY is served
                // by bus_events_kind_created without a temp sort. Per-kind
                // LIMIT n is safe for exact pushdown: a row outside its own
                // kind's top-n is beaten by n rows of that kind, so it can
                // never be in the global top-n.
                for kind in kinds {
                    let (p_part, after_p) = p_part(3);
                    let (e_part, after_e) = e_part(after_p);
                    let next = after_e;
                    let sql = format!(
                        "SELECT event_id, created_at FROM bus_events
                         WHERE {expiry_pred} AND kind = ?2{p_part}{e_part}
                         ORDER BY created_at DESC, event_id DESC{}",
                        limit_clause(next)
                    );
                    let mut q = sqlx::query_as::<_, (String, i64)>(&sql)
                        .bind(now)
                        .bind(kind);
                    for b in p_vals {
                        q = q.bind(b.as_str());
                    }
                    for b in e_vals {
                        q = q.bind(b.as_str());
                    }
                    if sql_exact {
                        q = q.bind(limit as i64);
                    }
                    rows.extend(q.fetch_all(&self.pool).await?);
                }
            }
        }

        // Merge with the exact comparator SQLite used (ORDER BY created_at
        // DESC, event_id DESC), keep the newest `limit` when exact, then
        // re-emit ASC — the order the caller's drain relies on.
        rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
        if sql_exact && rows.len() > limit {
            rows.truncate(limit);
        }
        rows.reverse();
        Ok(rows.into_iter().map(|(id, _)| id).collect())
    }

    /// Keep-alive interval for the prune loop.
    pub const PRUNE_INTERVAL: Duration = Duration::from_secs(60);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::PROJECT_MANIFEST;
    use crate::nostr::{Keys, Tag};

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
    async fn ping_succeeds_on_reachable_store() {
        let (store, _pool, path) = temp_store().await;
        store.ping().await.expect("SELECT 1 must succeed");
        assert!(store.is_healthy().await);
        let _ = std::fs::remove_file(&path);
    }

    /// bd-dtua: the heal is DURABLE and runs ONCE.
    ///
    /// The arrival log persists, so a restart must not re-insert every event
    /// into it: on the live bus that was a 100,000-row INSERT per restart,
    /// and because every subscriber's drain heals first, a fleet of
    /// subscribers each ran that insert before the in-memory marker
    /// advanced. The pool was buried under the copies and the daemon stopped
    /// answering (every MCP request timed out, so the pre-commit guard could
    /// not reach it and every commit was blocked).
    #[tokio::test]
    async fn the_heal_marker_survives_a_restart_and_the_heal_runs_once() {
        let (store, pool, path) = temp_store().await;
        let keys = Keys::generate();
        for i in 0..50 {
            store
                .insert(&sample_event(&keys, &format!("e{i}")))
                .await
                .expect("insert");
        }
        store.heal_arrivals().await.expect("heal");
        let arrivals: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_arrivals")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert!(arrivals > 0, "the events are in the arrival log");
        let marker: i64 = sqlx::query_scalar(
            "SELECT value FROM bus_meta WHERE key = ?1",
        )
        .bind(ARRIVALS_HEALED_ROWID_KEY)
        .fetch_one(&pool)
        .await
        .expect("the marker is durable");
        assert!(marker > 0, "the marker is written to the store, not only to memory");

        // A restart: the marker comes back from the store, so the heal does
        // not insert the same rows again. The marker is the witness - the
        // insert is OR IGNORE, so a re-insert changes no row and only the
        // marker shows that the work was done again.
        let reopened = BusEventStore::new(pool.clone()).await.expect("reopen");
        assert_eq!(
            reopened.heal_inserts.load(std::sync::atomic::Ordering::Acquire),
            0,
            "a restart must take the heal marker from the store, so the insert does not run \
             again: the insert is OR IGNORE, so a re-insert changes no row and only this \
             counter shows the work was done again"
        );
        let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_arrivals")
            .fetch_one(&pool)
            .await
            .expect("count");
        reopened.heal_arrivals().await.expect("heal");
        let after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_arrivals")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(
            before, after,
            "a restart must not re-insert the events into the arrival log"
        );

        // Concurrent drains: the heal runs once, not once per caller.
        for i in 50..80 {
            store
                .insert(&sample_event(&keys, &format!("e{i}")))
                .await
                .expect("insert");
        }
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = pool.clone();
            tasks.push(tokio::spawn(async move {
                let s = BusEventStore::new(pool).await.expect("store");
                s.arrivals_after(&[], 0, 512).await.map(|r| r.len())
            }));
        }
        for t in tasks {
            t.await.expect("join").expect("drain");
        }
        let distinct: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bus_arrivals",
        )
        .fetch_one(&pool)
        .await
        .expect("count");
        assert!(
            distinct <= 80,
            "concurrent drains must not duplicate arrivals: {distinct}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// The poll's plan as the planner would run the store's own SQL (bd-cgyh).
    /// Built from [`BusEventStore::arrivals_after_sql`] so it cannot drift from
    /// the query the daemon runs; the binds are inlined because SQLite chooses
    /// a plan at prepare time, before it sees the values.
    async fn poll_plan(pool: &sqlx::SqlitePool) -> String {
        let mut sql = BusEventStore::arrivals_after_sql(7);
        // ?1 cursor, ?2 now, ?3 limit, then one kind per ?4..
        let binds = [
            "1", "0", "512", "9", "14", "1", "30010", "30013", "30023", "30031",
        ];
        for (i, lit) in binds.iter().enumerate() {
            sql = sql.replace(&format!("?{}", i + 1), lit);
        }
        let rows = sqlx::query(&format!("EXPLAIN QUERY PLAN {sql}"))
            .fetch_all(pool)
            .await
            .expect("plan");
        rows.iter()
            .map(|r| Row::get::<String, _>(r, 3))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// bd-cgyh: the planner's picture of the bus must follow the store.
    ///
    /// Measured on the live 297 MB index on 2026-10-18: `sqlite_stat1` still
    /// described 9,630 rows while the store held 100,000, so the poll planned
    /// `MULTI-INDEX OR (bus_events_expires)` + `TEMP B-TREE FOR ORDER BY` and
    /// spent 1.0-3.4 s per call materialising the whole bus. After the refresh
    /// the same statement walks `bus_arrivals.seq` and stops at the LIMIT:
    /// 1.63 s → 0.32 ms on a copy of that store.
    ///
    /// That exact plan choice is not reproducible hermetically (it needs the
    /// live data distribution), so the proof here is the statistics rewrite,
    /// which is what the fix does. `PRAGMA optimize` is NOT enough and the test
    /// pins why: on the live copy it analyzed `bus_arrivals` (no stats at all)
    /// and left `bus_events` at 9,630, so the poll kept sorting. Both tables
    /// are analyzed on purpose.
    ///
    /// The plan check is a guard, not the proof: the store's own SQL with the
    /// real filter shape must still give the planner the option of walking
    /// arrivals. It is asserted with the store's SQL text (so it cannot drift
    /// from the query the daemon runs) and literals (a parameterized
    /// statement is a different plan, and the daemon's parameterized plan on
    /// the live store was measured separately: stale MULTI-INDEX OR + sort,
    /// healed seq walk).
    #[tokio::test]
    async fn planner_stats_follow_the_store_and_the_poll_never_sorts_the_bus() {
        let (store, pool, path) = temp_store().await;
        // Seed both tables directly: the witness is about the planner's
        // statistics, and thousands of signed events would only slow it down.
        // 20k is the size where a healed planner walks arrivals at a LIMIT.
        sqlx::raw_sql(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n < 20000)
             INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, expires_at)
             SELECT 'e'||n, '{}', n,
                    CASE WHEN n%500=0 THEN 30031 WHEN n%20=0 THEN 30010
                         WHEN n%7=0 THEN 14 ELSE 9 END,
                    'a', NULL FROM c;
             INSERT INTO bus_arrivals (event_id) SELECT event_id FROM bus_events;",
        )
        .execute(&pool)
        .await
        .expect("seed");
        sqlx::raw_sql("ANALYZE;")
            .execute(&pool)
            .await
            .expect("analyze");
        // Poison the statistics the way turnover does: index rows describing
        // an older, smaller store, and no `bus_arrivals` row at all — the live
        // shape, verbatim.
        sqlx::raw_sql(
            "DELETE FROM sqlite_stat1;
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','bus_events_expires','9630 11');
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','bus_events_kind_created','9630 741 2 1');
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','bus_events_addressable','9630 741 4 3');
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','bus_events_kind','9630 741');
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','bus_events_created_at','9630 2');
             INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('bus_events','sqlite_autoindex_bus_events_1','9630 1');",
        )
        .execute(&pool)
        .await
        .expect("poison");

        // Premise: the store's own check reads that as drifted (9,630 vs
        // 20,000). Without this the rewrite below could pass for the wrong
        // reason, e.g. by analyzing on every open.
        assert!(
            store
                .planner_stats_are_stale()
                .await
                .expect("staleness check"),
            "poisoned statistics must read as stale"
        );

        // Opening the store is when a drifted store is first seen (a daemon
        // restart): the plausibility check heals it. Before the fix the
        // recorded 9,630 was never questioned and the poll kept its plan.
        let reopened = BusEventStore::new(pool.clone()).await.expect("reopen");
        let recorded: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(CAST(substr(stat, 1, instr(stat, ' ') - 1) AS INTEGER))
             FROM sqlite_stat1 WHERE tbl = 'bus_events'",
        )
        .fetch_optional(&pool)
        .await
        .expect("stat1");
        assert_eq!(
            recorded,
            Some(20_000),
            "the refresh records the live row count"
        );
        let arrivals_stats: Option<i64> =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'bus_arrivals'")
                .fetch_optional(&pool)
                .await
                .expect("stat1");
        assert_eq!(
            arrivals_stats,
            Some(1),
            "bus_arrivals is analyzed too: PRAGMA optimize alone left it writable but the poll unplanned"
        );
        // The widened filter the daemon's subscriber runs (seven kinds): a
        // two-kind filter legitimately sorts a small bus, which is neither the
        // live shape nor the pathology.
        let healed = poll_plan(&pool).await;
        assert!(
            healed.contains("SEARCH a USING INTEGER PRIMARY KEY")
                && !healed.contains("TEMP B-TREE"),
            "the poll walks arrivals in seq order and stops at the LIMIT: {healed}"
        );

        // The urgent path is rate-limited: right after a refresh there is
        // nothing due, and turnover is what makes it due again.
        assert!(
            !reopened
                .refresh_planner_stats_if_stale()
                .await
                .expect("refresh"),
            "not due right after a refresh"
        );
        sqlx::raw_sql(
            "INSERT INTO bus_meta (key, value) VALUES ('mutations', 100000)
             ON CONFLICT(key) DO UPDATE SET value = value + 100000",
        )
        .execute(&pool)
        .await
        .expect("advance turnover");
        assert!(
            reopened
                .refresh_planner_stats_if_stale()
                .await
                .expect("refresh"),
            "turnover makes the refresh due"
        );
        let _ = std::fs::remove_file(&path);
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

    /// bd-channel-close-purge-bsqb: `purge_ids` removes exactly the named
    /// rows (+ their tag rows), bumps the mutation version, and leaves no
    /// hot-memo residue — replay must not resurrect purged events.
    #[tokio::test]
    async fn channel_close_purge_removes_bus_events() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let keep = tagged_event(&keys, "keep");
        let gone = tagged_event(&keys, "gone");
        let also_gone = tagged_event(&keys, "also gone");
        store.insert(&keep).await.expect("insert keep");
        store.insert(&gone).await.expect("insert gone");
        store.insert(&also_gone).await.expect("insert also_gone");

        /// A p-tagged event: p/e values are the only tag rows the store indexes,
        /// so the purge's tag-row cleanup is observable on these.
        fn tagged_event(keys: &Keys, content: &str) -> Event {
            build_event(
                keys,
                crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
                content,
                vec![
                    Tag::parse(["t", "demo"]).expect("t"),
                    Tag::public_key(Keys::generate().public_key()),
                ],
                Timestamp::now(),
            )
            .expect("build")
        }
        // Hydrate the hot memo so the purge must also clear memo entries.
        let all = crate::bus::Filter::new();
        let replayed = store.replay(&all).await.expect("replay");
        assert_eq!(replayed.len(), 3);

        let ids = vec![gone.id.to_hex(), also_gone.id.to_hex()];
        let removed = store.purge_ids(&ids).await.expect("purge");
        assert_eq!(removed, 2, "exactly the named rows are removed");

        let after = store.replay(&all).await.expect("replay after purge");
        assert_eq!(after.len(), 1, "only the survivor remains");
        assert_eq!(after[0].id, keep.id);

        // Tag rows went with them.
        let tag_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM bus_event_tags")
            .fetch_one(&store.pool)
            .await
            .expect("tag count");
        assert_eq!(tag_rows, 1, "purged events carry no tag residue");

        // Re-publishing a purged id is NOT blocked (no dedup residue).
        assert!(
            store.insert(&gone).await.expect("re-insert"),
            "a purged id may be stored again"
        );
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

    /// bd-bus-scan gate: the per-kind split in `scan_ids` preserves the exact
    /// contract the single combined query produced — newest-tail `limit`
    /// ACROSS kinds (DESC by created_at, event_id), expiry exclusion, p-tag
    /// narrowing, and ASC envelope order.
    #[tokio::test]
    async fn test_replay_per_kind_split_newest_tail_expiry_and_p() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let k1 = crate::nostr::Kind::TextNote;
        let k2 = crate::nostr::kinds::toron_kind(crate::nostr::kinds::AGENT_PROFILE);
        let t = |secs: u64| Timestamp::from(secs);

        // Interleaved created_at across two kinds, inserted out of order.
        let mut evs = Vec::new();
        let mut push = |kind: crate::nostr::Kind, secs: u64, content: &str| {
            let ev = build_event(
                &keys,
                kind,
                content,
                vec![Tag::parse(["t", "demo"]).expect("t")],
                t(secs),
            )
            .expect("build");
            evs.push(ev.clone());
            ev
        };
        push(k1, 100, "a"); // kind 1 @100
        push(k2, 150, "b"); // kind 2 @150
        push(k1, 200, "c"); // kind 1 @200
        push(k2, 250, "d"); // kind 2 @250
        push(k1, 300, "e"); // kind 1 @300
        push(k2, 350, "f"); // kind 2 @350
        push(k1, 400, "g"); // kind 1 @400
        push(k2, 450, "h"); // kind 2 @450
        for ev in &evs {
            store.insert(ev).await.expect("insert");
        }

        // A NIP-40-expired row must never surface (prune may not have run).
        let expired = build_event(
            &keys,
            k1,
            "expired",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(t(420)),
            ],
            t(500),
        )
        .expect("build");
        store.insert(&expired).await.expect("insert expired");

        // Newest-tail across kinds: top-3 = @450 (k2), @400 (k1), @350 (k2),
        // re-emitted in ASC envelope order.
        let filter = crate::bus::Filter::new().kinds([k1, k2]).limit(3);
        let replayed = store.replay(&filter).await.expect("replay");
        let ids: Vec<u64> = replayed.iter().map(|e| e.created_at.as_secs()).collect();
        assert_eq!(
            ids,
            vec![350, 400, 450],
            "newest-tail across kinds, ASC order"
        );

        // No expiry leakage with a limit that would otherwise include it.
        let all = crate::bus::Filter::new().kinds([k1, k2]).limit(100);
        let replayed = store.replay(&all).await.expect("replay all");
        let secs: Vec<u64> = replayed.iter().map(|e| e.created_at.as_secs()).collect();
        assert_eq!(secs, vec![100, 150, 200, 250, 300, 350, 400, 450]);
        assert!(
            replayed.iter().all(|e| e.content != "expired"),
            "expired event must never surface"
        );

        // #p narrowing still applies inside the split scan.
        let target = Keys::generate();
        let p_ev = build_event(
            &keys,
            k2,
            "p-tagged",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::public_key(target.public_key()),
            ],
            t(600),
        )
        .expect("build");
        store.insert(&p_ev).await.expect("insert p");
        let p_filter = crate::bus::Filter::new()
            .kinds([k1, k2])
            .pubkey(target.public_key());
        let replayed = store.replay(&p_filter).await.expect("replay p");
        assert_eq!(replayed.len(), 1, "only the p-tagged event matches");
        assert_eq!(replayed[0].id, p_ev.id);

        // Same-created_at tie-break: DESC by event_id hex, then ASC re-emit.
        let tie_a = build_event(
            &keys,
            k1,
            "tie-a",
            vec![Tag::parse(["t", "demo"]).expect("t")],
            t(700),
        )
        .expect("build");
        let tie_b = build_event(
            &keys,
            k1,
            "tie-b",
            vec![Tag::parse(["t", "demo"]).expect("t")],
            t(700),
        )
        .expect("build");
        store.insert(&tie_a).await.expect("insert tie_a");
        store.insert(&tie_b).await.expect("insert tie_b");
        let tie = crate::bus::Filter::new().kinds([k1]).limit(2);
        let replayed = store.replay(&tie).await.expect("replay tie");
        let ids: Vec<String> = replayed.iter().map(|e| e.id.to_hex()).collect();
        assert_eq!(ids.len(), 2);
        let mut sorted = ids.clone();
        sorted.sort(); // ASC envelope = smaller event_id first on the tie
        assert_eq!(ids, sorted);
        assert!(ids.contains(&tie_a.id.to_hex()) && ids.contains(&tie_b.id.to_hex()));

        let _ = std::fs::remove_file(&path);
    }

    /// bd-py9o acceptance gate: a SECOND identical replay must parse zero
    /// rows — every row hydrates from the hot memo (and a warm instance,
    /// whose inserts memoized as they arrived, parses nothing at all).
    /// Only genuinely unseen rows may ever be parsed.
    #[tokio::test]
    async fn test_replay_second_identical_call_parses_zero_rows_bd_py9o() {
        let (store, pool, path) = temp_store().await;
        for i in 0..5 {
            let ev = sample_event(&Keys::generate(), &format!("e{i}"));
            store.insert(&ev).await.expect("insert");
        }
        let filter = crate::bus::project_filter("demo");

        // Warm instance: inserts memoized each event on arrival.
        let w1 = store.replay(&filter).await.expect("warm replay 1");
        assert_eq!(w1.len(), 5);
        let p_warm = store.parsed_rows();
        let h_warm = store.hot_hits();
        let w2 = store.replay(&filter).await.expect("warm replay 2");
        assert_eq!(w2.len(), 5);
        assert_eq!(store.parsed_rows(), p_warm, "a warm replay parses NOTHING");
        assert_eq!(
            store.hot_hits() - h_warm,
            5,
            "the second warm replay served every row from the memo"
        );

        // Cold instance over the same pool (the daemon-restart shape): the
        // first replay pays one parse per row; the second pays ZERO.
        let cold = BusEventStore::new(pool.clone()).await.expect("cold store");
        let c1 = cold.replay(&filter).await.expect("cold replay 1");
        assert_eq!(c1.len(), 5);
        let p_cold = cold.parsed_rows();
        assert!(p_cold >= 5, "cold first replay parses every row from SQL");
        let c2 = cold.replay(&filter).await.expect("cold replay 2");
        assert_eq!(c2.len(), 5);
        assert_eq!(
            c2.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>(),
            c1.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>(),
            "identical envelope order across replays"
        );
        assert_eq!(
            cold.parsed_rows(),
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
            cold.parsed_rows(),
            p_cold + 1,
            "only the new row may be parsed"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// bd-3gwy / bd-741i acceptance gate: a replay on an UNCHANGED store
    /// skips the phase-1 SQL id-scan entirely (snapshot memo); any insert
    /// bumps the version and forces exactly one fresh scan. Expired rows
    /// must not disable the memo (hydrate skips them).
    #[tokio::test]
    async fn test_snapshot_memo_second_replay_skips_sql_bd_3gwy() {
        let (store, pool, path) = temp_store().await;
        for i in 0..4 {
            let ev = sample_event(&Keys::generate(), &format!("e{i}"));
            store.insert(&ev).await.expect("insert");
        }
        let filter = crate::bus::project_filter("demo");

        let r1 = store.replay(&filter).await.expect("replay 1");
        assert_eq!(r1.len(), 4);
        let s1 = store.sql_scans();
        let r2 = store.replay(&filter).await.expect("replay 2");
        assert_eq!(r2.len(), 4);
        assert_eq!(
            store.sql_scans(),
            s1,
            "second identical replay must NOT touch SQL (memo hit)"
        );
        assert_eq!(
            r2.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>(),
            r1.iter().map(|e| e.id.to_hex()).collect::<Vec<_>>()
        );

        // A mutation invalidates: next replay scans again, sees the new row.
        let other = BusEventStore::new(pool.clone()).await.expect("other");
        other
            .insert(&sample_event(&Keys::generate(), "new"))
            .await
            .expect("insert new");
        let r3 = store.replay(&filter).await.expect("replay 3");
        assert_eq!(r3.len(), 5);
        assert_eq!(
            store.sql_scans(),
            s1 + 1,
            "mutation forces exactly one fresh scan"
        );
        // And the post-mutation state is memoized too.
        let r4 = store.replay(&filter).await.expect("replay 4");
        assert_eq!(r4.len(), 5);
        assert_eq!(
            store.sql_scans(),
            s1 + 1,
            "post-mutation replays are memo hits again"
        );

        // TTL safety: a future TTL must NOT disable the memo (90-day
        // reservation TTLs used to pin every fleet poll to a TEMP B-TREE
        // scan). Already-expired rows must also keep the memo: scan_ids
        // already drops them, hydrate skips expiration(), and prune (60s)
        // bumps the version. Bypassing the memo while any stale row existed
        // is what turned overnight fleet polls + `toron watch` into a 128GB
        // RSS / 84-swapfile watchdog panic (bd-741i).
        let still_valid = build_event(
            &Keys::generate(),
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "ttl-future",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(Timestamp::now() + crate::nostr::Timestamp::from(3600)),
            ],
            Timestamp::now(),
        )
        .expect("build ttl event");
        other.insert(&still_valid).await.expect("insert ttl future");
        let before_future = store.sql_scans();
        let _ = store.replay(&filter).await.expect("replay future 1");
        let _ = store.replay(&filter).await.expect("replay future 2");
        assert_eq!(
            store.sql_scans(),
            before_future + 1,
            "future TTL must NOT disable memo — second replay is a hit"
        );

        // Already-expired row: insert is a mutation (one scan) then memo hits.
        let expiring = build_event(
            &Keys::generate(),
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "ttl-expired",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(Timestamp::from(1_500_000_000_u64)),
            ],
            Timestamp::from(1_400_000_000_u64),
        )
        .expect("build expired ttl");
        other.insert(&expiring).await.expect("insert ttl expired");
        let before_ttl = store.sql_scans();
        let ttl1 = store.replay(&filter).await.expect("replay ttl 1");
        let ttl2 = store.replay(&filter).await.expect("replay ttl 2");
        assert_eq!(
            store.sql_scans(),
            before_ttl + 1,
            "already-expired rows must NOT disable the memo (bd-741i)"
        );
        assert!(
            ttl1.iter().all(|e| e.content != "ttl-expired")
                && ttl2.iter().all(|e| e.content != "ttl-expired"),
            "expired rows never surface even when the memo stays hot"
        );

        // Correctness under the memo: an EXPIRED event is never returned.
        let expired = build_event(
            &Keys::generate(),
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "expired-long-ago",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(Timestamp::from(1_500_000_000_u64)),
            ],
            Timestamp::from(1_400_000_000_u64),
        )
        .expect("build expired");
        other.insert(&expired).await.expect("insert expired");
        let after = store.replay(&filter).await.expect("replay expired");
        assert!(
            after.iter().all(|e| e.content != "expired-long-ago"),
            "expired rows never surface"
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

    /// bd-eifs regression: cap eviction is FIFO BY ARRIVAL (rowid), never by
    /// NIP-17 randomized `created_at`. Gift wraps (and NIP-59 seals) are born
    /// with a created_at up to 2 days in the past for metadata privacy — under
    /// `created_at ASC` eviction a freshly arrived mail wrap looks like the
    /// oldest row in a full store and is pruned BEFORE ANY READER SEES IT,
    /// while send already reported success. Live reproduction: the doorbell's
    /// kind-9 wake flood pins the store at MAX_STORED_EVENTS continuously, so
    /// the prune tick runs constantly and the eviction window is seconds.
    #[tokio::test]
    async fn test_cap_eviction_is_arrival_fifo_not_created_at() {
        let (store, pool, path) = temp_store().await;
        let keys = Keys::generate();

        // Pin the store at the cap with filler rows whose created_at is
        // NEWER than the backdated arrival below — under created_at-ordering
        // these all outrank it for survival.
        sqlx::query(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100000) \
             INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at) \
             SELECT 'filler-' || x, '{}', 1789630000 + x, 9, 'filler-author', NULL, NULL FROM cnt",
        )
        .execute(&pool)
        .await
        .expect("seed fillers");

        // First arrival: punctual timestamp. Second arrival: BACKDATED
        // created_at (what gift_wrap_with_urgency produces — NIP-17 jitter
        // up to 2 days back). Arrival order: early → backdated.
        let early = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "early arrival",
            vec![Tag::parse(["t", "demo"]).expect("t")],
            Timestamp::now(),
        )
        .expect("build early");
        store.insert(&early).await.expect("insert early");

        let backdated_created_at = Timestamp::now().as_secs() - 2 * 24 * 60 * 60;
        let backdated = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "backdated arrival",
            vec![Tag::parse(["t", "demo"]).expect("t")],
            Timestamp::from(backdated_created_at),
        )
        .expect("build backdated");
        store.insert(&backdated).await.expect("insert backdated");

        assert_eq!(store.count().await.expect("count"), 100_002);
        store.prune().await.expect("prune");

        // Cap holds (100_000), and BOTH arrivals survive: the eviction
        // victims are the earliest-ARRIVED filler rows, not the row whose
        // protocol timestamp happens to look oldest.
        let n = store.count().await.expect("count after prune");
        assert!(n <= super::MAX_STORED_EVENTS, "cap holds: {n}");
        for (name, ev) in [("backdated", &backdated), ("early", &early)] {
            let hit: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM bus_events WHERE event_id = ?1")
                    .bind(ev.id.to_hex())
                    .fetch_optional(&pool)
                    .await
                    .expect("survivor lookup");
            assert!(
                hit.is_some(),
                "{name} arrival must survive cap prune — created_at is \
                 protocol metadata, not storage age"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// bd-fxzv: cap prune must not evict durable coordination state. A
    /// kind-9 flood pins the store at MAX_STORED_EVENTS; the prune tick
    /// then eats the OLDEST arrivals first — which are the channel room
    /// (39000/39002), manifest (30010), profile (30013) and reservation
    /// (30031) rows. Those kinds are exempt from CAP eviction (expiry GC
    /// still applies); the victims are the flood rows.
    #[tokio::test]
    async fn test_cap_prune_exempts_durable_coordination_state() {
        use crate::nostr::kinds::{
            AGENT_PROFILE, CHANNEL_GROUP, CHANNEL_MEMBERS, FILE_RESERVATION,
        };
        let (store, pool, path) = temp_store().await;
        let keys = Keys::generate();
        // Durable rows arrive FIRST (oldest rowid — today's victims).
        let mut durable_ids = Vec::new();
        for kind in [
            CHANNEL_GROUP,
            CHANNEL_MEMBERS,
            PROJECT_MANIFEST,
            AGENT_PROFILE,
            FILE_RESERVATION,
        ] {
            let ev = build_event(
                &keys,
                crate::nostr::kinds::toron_kind(kind),
                "durable",
                vec![Tag::parse(["t", "demo"]).expect("t")],
                Timestamp::now(),
            )
            .expect("build durable");
            durable_ids.push(ev.id.to_hex());
            store.insert(&ev).await.expect("insert durable");
        }
        // Flood past the cap with kind-9 rows (same CTE as the FIFO test).
        sqlx::query(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100000) \
             INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at) \
             SELECT 'flood-' || x, '{}', 1789630000 + x, 9, 'filler-author', NULL, NULL FROM cnt",
        )
        .execute(&pool)
        .await
        .expect("seed flood");
        assert_eq!(store.count().await.expect("count"), 100_005);
        store.prune().await.expect("prune");
        for id in &durable_ids {
            let hit: Option<i64> =
                sqlx::query_scalar("SELECT 1 FROM bus_events WHERE event_id = ?1")
                    .bind(id)
                    .fetch_optional(&pool)
                    .await
                    .expect("durable lookup");
            assert!(
                hit.is_some(),
                "durable row {id} must survive cap prune — flood rows are the victims"
            );
        }
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

    /// NIP-01 parameterized-replaceable: an older event arriving after a
    /// newer one on the same `(kind, author, d)` must not overwrite it.
    #[tokio::test]
    async fn test_addressable_older_replay_does_not_overwrite_newer() {
        let (store, _pool, path) = temp_store().await;
        let agent = Keys::generate();
        let newer = build_event(
            &agent,
            crate::nostr::kinds::toron_kind(crate::nostr::kinds::FILE_RESERVATION),
            "lease v2",
            vec![Tag::parse(["d", "proj:src/**"]).expect("d")],
            Timestamp::from(1_700_000_001_u64),
        )
        .expect("build newer");
        let older = build_event(
            &agent,
            crate::nostr::kinds::toron_kind(crate::nostr::kinds::FILE_RESERVATION),
            "lease v1",
            vec![Tag::parse(["d", "proj:src/**"]).expect("d")],
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build older");

        assert!(store.insert_addressable(&newer).await.expect("newer"));
        let kept = store
            .insert_addressable(&older)
            .await
            .expect("older replay");
        assert!(
            !kept,
            "older event must not replace a newer one on the same coordinate"
        );
        assert_eq!(store.count().await.expect("count"), 1);
        assert_eq!(
            store.all_events().await.expect("all")[0].id,
            newer.id,
            "newer lease survives an older replay"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// bd-replay-tag-pushdown-lurm acceptance #1: a p-tag-filtered replay on
    /// a 10k-event store parses only matched rows. The instrumented counter
    /// must see exactly the 5 matching rows (SQL prefilter via
    /// bus_event_tags), and the query plan must use the tag index.
    #[tokio::test]
    async fn test_replay_p_tag_pushdown_parses_only_matched() {
        use crate::nostr::Filter;
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

        // bd-py9o: inserts warm the writer's hot memo, so the parse counter
        // is measured through a COLD reader over the same pool — exactly the
        // daemon-restart shape the prefilter property protects.
        let reader = BusEventStore::new(pool.clone()).await.expect("cold reader");
        let replayed = reader
            .replay(&Filter::new().pubkey(target.public_key()).limit(10))
            .await
            .expect("replay");
        assert_eq!(replayed.len(), 5, "only the 5 target events match");
        assert_eq!(
            reader.parsed_rows(),
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
        use crate::nostr::Filter;
        let (store, pool, path) = temp_store().await;
        let _ = &store;
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

        // bd-py9o: cold reader (see gate #1) so parses measure SQL work.
        let reader = BusEventStore::new(pool.clone()).await.expect("cold reader");
        let replayed = reader
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
            reader.parsed_rows(),
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
        use crate::nostr::Filter;
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
        use crate::nostr::Filter;
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let root: crate::nostr::EventId = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "root",
            vec![],
            Timestamp::from(1_700_000_000u64),
        )
        .expect("root build")
        .id;
        let reply_id: crate::nostr::EventId = build_event(
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
        use crate::nostr::Filter;
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

    // ---- arrival log (bd-wake-live-burst-loss-j86v, bd-wake-replay-cap-qk8n)

    fn profile(keys: &Keys, d: &str, at: u64) -> Event {
        build_event(
            keys,
            crate::nostr::Kind::from(crate::nostr::kinds::AGENT_PROFILE),
            format!("profile {d} @{at}"),
            vec![Tag::parse(["d", d]).expect("d")],
            Timestamp::from(at),
        )
        .expect("build profile")
    }

    /// A supersede deletes the newest row and inserts its replacement. With
    /// `rowid` as the cursor that replacement would reuse the deleted number
    /// and every cursor sitting on it would skip it; the arrival seq must
    /// move strictly forward instead.
    #[tokio::test]
    async fn arrival_seq_moves_forward_through_a_supersede_and_ignores_duplicates() {
        let (store, _pool, path) = temp_store().await;
        let keys = Keys::generate();
        let first = profile(&keys, "me", 1_800_000_000);
        assert!(store.insert_addressable(&first).await.expect("first"));
        let s1 = store
            .arrival_seq_of(&first.id.to_hex())
            .await
            .expect("lookup")
            .expect("first has a seq");

        let second = profile(&keys, "me", 1_800_000_001);
        assert!(store.insert_addressable(&second).await.expect("supersede"));
        let s2 = store
            .arrival_seq_of(&second.id.to_hex())
            .await
            .expect("lookup")
            .expect("replacement has a seq");
        assert!(s2 > s1, "supersede got seq {s2}, not past {s1}");

        // Control: a duplicate publish is not a new arrival.
        assert!(!store.insert(&second).await.expect("dup"));
        assert_eq!(
            store
                .arrival_seq_of(&second.id.to_hex())
                .await
                .expect("lookup"),
            Some(s2)
        );
        let page = store.arrivals_after(&[], 0, 10).await.expect("page");
        assert_eq!(
            page.iter().map(|(s, e)| (*s, e.id)).collect::<Vec<_>>(),
            vec![(s2, second.id)],
            "the superseded row is gone; the live one is listed once"
        );

        // The reuse trap proper: the NEWEST arrival leaves the store
        // (purge, then prune drops its arrival row) and a new event lands.
        // A plain INTEGER PRIMARY KEY would hand the newcomer the retired
        // number, and a cursor parked on it would skip the newcomer.
        let newest = sample_event(&keys, "newest");
        store.insert(&newest).await.expect("newest");
        let s_newest = store
            .arrival_seq_of(&newest.id.to_hex())
            .await
            .expect("lookup")
            .expect("newest has a seq");
        store
            .purge_ids(&[newest.id.to_hex()])
            .await
            .expect("purge newest");
        store.prune().await.expect("prune drops its arrival row");
        let after = sample_event(&keys, "after purge");
        store.insert(&after).await.expect("after");
        let s_after = store
            .arrival_seq_of(&after.id.to_hex())
            .await
            .expect("lookup")
            .expect("newcomer has a seq");
        assert!(
            s_after > s_newest,
            "retired seq {s_newest} was reused as {s_after}"
        );
        let _ = std::fs::remove_file(&path);
    }

    /// Rows an older binary wrote (no arrival row) are numbered in rowid
    /// order on the next read, after everything already numbered.
    #[tokio::test]
    async fn rows_from_an_older_binary_are_healed_into_the_arrival_log() {
        let (store, pool, path) = temp_store().await;
        let keys = Keys::generate();
        let native = sample_event(&keys, "native");
        store.insert(&native).await.expect("native insert");
        let legacy: Vec<Event> = (0..3)
            .map(|i| sample_event(&keys, &format!("legacy {i}")))
            .collect();
        for ev in &legacy {
            sqlx::query(
                "INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL)",
            )
            .bind(ev.id.to_hex())
            .bind(ev.as_json())
            .bind(ev.created_at.as_secs() as i64)
            .bind(ev.kind.as_u16() as i64)
            .bind(ev.pubkey.to_hex())
            .execute(&pool)
            .await
            .expect("legacy write");
        }
        let page = store.arrivals_after(&[], 0, 10).await.expect("page");
        let got: Vec<_> = page.iter().map(|(_, e)| e.id).collect();
        let mut want = vec![native.id];
        want.extend(legacy.iter().map(|e| e.id));
        assert_eq!(got, want, "native first, then legacy rows in write order");
        let seqs: Vec<u64> = page.iter().map(|(s, _)| *s).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// Cap eviction records the highest evicted arrival in the same
    /// transaction as the delete; a prune that evicts nothing leaves it.
    #[tokio::test]
    async fn cap_eviction_records_the_highest_evicted_arrival() {
        let (store, pool, path) = temp_store().await;
        sqlx::query(
            "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 100000) \
             INSERT INTO bus_events (event_id, raw_json, created_at, kind, author, d_tag, expires_at) \
             SELECT 'filler-' || x, '{}', 1789630000 + x, 9, 'filler-author', NULL, NULL FROM cnt",
        )
        .execute(&pool)
        .await
        .expect("seed fillers");
        assert_eq!(store.arrivals_evicted_through().await.expect("mark"), 0);
        let keys = Keys::generate();
        let a = sample_event(&keys, "a");
        let b = sample_event(&keys, "b");
        store.insert(&a).await.expect("a");
        store.insert(&b).await.expect("b");
        // Heal numbers the fillers 1..=100000 in rowid order; a and b follow.
        let second_filler = store
            .arrival_seq_of("filler-2")
            .await
            .expect("lookup")
            .expect("healed");

        store.prune().await.expect("prune");
        assert_eq!(
            store.arrivals_evicted_through().await.expect("mark"),
            second_filler,
            "the two oldest fillers were evicted"
        );
        assert!(
            store
                .arrival_seq_of(&a.id.to_hex())
                .await
                .expect("a")
                .is_some()
        );

        // Control: at the cap, nothing is evicted and the mark stays.
        store.prune().await.expect("prune again");
        assert_eq!(
            store.arrivals_evicted_through().await.expect("mark"),
            second_filler
        );
        let _ = std::fs::remove_file(&path);
    }
}
