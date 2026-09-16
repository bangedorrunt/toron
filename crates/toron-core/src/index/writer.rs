// governed-by: ADR-0003
#![forbid(unsafe_code)]

// bd-led.4.2: single-writer SQLite discipline (ADR G15, risk 8).
// - Dedicated write pool sized to one connection; reads on a separate pool.
// - An async write gate serializes all mutations (no SQLITE_BUSY storms).
// - `DeferredTouch` coalesces "last-active"-style writes (30s, AM-Rust parity)
//   behind an injectable clock so hot-path updates batch instead of spamming.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use parking_lot::Mutex;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;
use tokio::sync::Mutex as AsyncMutex;

/// Monotone-ish wall clock in i64 microseconds (ADR G7: i64-µs timestamps).
pub trait Clock: Send + Sync {
    fn now_us(&self) -> i64;
}

/// Real wall clock.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_us(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }
}

/// Deterministic clock for tests: explicitly advanced.
#[derive(Debug, Clone)]
pub struct FakeClock(Arc<AtomicI64>);

impl FakeClock {
    pub fn new(start_us: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start_us)))
    }

    pub fn set(&self, us: i64) {
        self.0.store(us, Ordering::Relaxed);
    }

    pub fn advance(&self, us: i64) {
        self.0.fetch_add(us, Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn now_us(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Coalescing touch buffer. `touch` is O(1); `drain_due` returns writes whose
/// entry is older than `interval_us` (and clears them). Multiple touches within
/// the interval collapse to one write — the 30s deferred-touch discipline.
pub struct DeferredTouch<K, V, C: Clock> {
    clock: C,
    interval_us: i64,
    pending: Mutex<HashMap<K, (V, i64)>>,
}

impl<K, V, C: Clock> DeferredTouch<K, V, C>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(clock: C, interval_us: i64) -> Self {
        Self {
            clock,
            interval_us,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Buffer a touch for `key`. Returns true if it is the first buffered
    /// touch for that key (callers can use this to schedule the next flush).
    pub fn touch(&self, key: K, value: V) -> bool {
        let now = self.clock.now_us();
        let mut p = self.pending.lock();
        let first = !p.contains_key(&key);
        p.insert(key, (value, now));
        first
    }

    /// Return every buffered entry older than `interval_us`, removing them.
    pub fn drain_due(&self) -> Vec<(K, V)> {
        let now = self.clock.now_us();
        let mut p = self.pending.lock();
        let due: Vec<(K, V)> = p
            .iter()
            .filter(|(_, (_, ts))| now - *ts >= self.interval_us)
            .map(|(k, (v, _))| (k.clone(), v.clone()))
            .collect();
        for (k, _) in &due {
            p.remove(k);
        }
        due
    }

    /// Drain everything regardless of age (shutdown / force flush).
    pub fn drain_all(&self) -> Vec<(K, V)> {
        let mut p = self.pending.lock();
        let all: Vec<(K, V)> = p.iter().map(|(k, (v, _))| (k.clone(), v.clone())).collect();
        p.clear();
        all
    }

    pub fn pending_len(&self) -> usize {
        self.pending.lock().len()
    }
}

/// Single-writer index. All mutations go through the dedicated write pool
/// (max one connection) under an async gate; reads use the read pool.
#[derive(Debug, Clone)]
pub struct IndexWriter {
    write_pool: SqlitePool,
    read_pool: SqlitePool,
    gate: Arc<AsyncMutex<()>>,
}

impl IndexWriter {
    /// `write_pool` MUST be sized to one connection (dedicated writer).
    /// `read_pool` is the concurrent read path (WAL lets readers proceed
    /// while a writer holds the single write conn).
    pub fn new(write_pool: SqlitePool, read_pool: SqlitePool) -> Self {
        Self {
            write_pool,
            read_pool,
            gate: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Execute one SQL write inside the gate on the dedicated writer.
    pub async fn execute(&self, sql: &str) -> anyhow::Result<u64> {
        let _guard = self.gate.lock().await;
        let rows = sqlx::query(sql).execute(&self.write_pool).await?;
        Ok(rows.rows_affected())
    }

    /// Execute multiple SQL writes atomically in one transaction on the writer.
    pub async fn execute_many(&self, queries: &[String]) -> anyhow::Result<()> {
        let _guard = self.gate.lock().await;
        let mut tx = self.write_pool.begin().await?;
        for sql in queries {
            sqlx::query(sql).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Run a scalar read on the read pool (never blocks the writer).
    pub async fn query_scalar<T>(&self, sql: &str) -> anyhow::Result<Option<T>>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
    {
        let row = sqlx::query(sql).fetch_one(&self.read_pool).await?;
        let value: T = row.get(0);
        Ok(Some(value))
    }

    /// Run a full-row read on the read pool (never blocks the writer).
    pub async fn fetch_rows(&self, sql: &str) -> anyhow::Result<Vec<sqlx::sqlite::SqliteRow>> {
        let rows = sqlx::query(sql).fetch_all(&self.read_pool).await?;
        Ok(rows)
    }

    /// The dedicated write pool (callers that need raw access, e.g.
    /// migrations, must also run on the writer path).
    pub const fn write_pool(&self) -> &SqlitePool {
        &self.write_pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn pools() -> (SqlitePool, SqlitePool, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("toron-writer-{}.sqlite", uuid::Uuid::new_v4()));
        let p = path.to_str().unwrap();
        let w = schema::connect(p).await.expect("write pool");
        schema::migrate(&w).await.expect("migrate");
        let r = schema::connect(p).await.expect("read pool");
        (w, r, path)
    }

    #[tokio::test]
    async fn deferred_touch_coalesces_within_interval() {
        let clock = FakeClock::new(1_000_000);
        let touch = DeferredTouch::<String, i64, FakeClock>::new(clock.clone(), 30_000_000);

        assert!(touch.touch("pi".into(), 1));
        assert!(!touch.touch("pi".into(), 2), "second touch coalesces");
        assert_eq!(touch.pending_len(), 1);

        // Within 30s: nothing drains.
        clock.advance(10_000_000);
        assert!(touch.drain_due().is_empty(), "not yet due");

        // Past 30s: drains once with the latest value.
        clock.advance(25_000_000);
        let due = touch.drain_due();
        assert_eq!(due, vec![("pi".to_string(), 2)]);
        assert_eq!(touch.pending_len(), 0);
    }

    #[tokio::test]
    async fn deferred_touch_drain_all_flushes_shutdown() {
        let clock = FakeClock::new(0);
        let touch = DeferredTouch::<&str, u32, FakeClock>::new(clock, 30_000_000);
        touch.touch("a", 1);
        touch.touch("b", 2);
        let all = touch.drain_all();
        assert_eq!(all.len(), 2);
        assert_eq!(touch.pending_len(), 0);
    }

    #[tokio::test]
    async fn concurrent_reads_during_write_no_busy_storms() {
        let (w, r, path) = pools().await;
        let writer = IndexWriter::new(w.clone(), r.clone());

        writer
            .execute(
                "INSERT INTO agents \
                 (project_slug, name, pubkey, created_ts_us) \
                 VALUES ('demo', 'PiAlpha', 'aaaa', 1000000)",
            )
            .await
            .expect("seed");

        // Concurrent: many writes + many reads. WAL + single-writer gate must
        // serialize writes so no SQLITE_BUSY surfaces.
        let mut writes = Vec::new();
        for i in 0..20i64 {
            let w2 = writer.clone();
            writes.push(tokio::spawn(async move {
                w2.execute(&format!(
                    "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
                     VALUES ('demo', 'agent{i}', 'bbbb', 2000000)"
                ))
                .await
            }));
        }
        let mut reads = Vec::new();
        for _ in 0..20 {
            let w2 = writer.clone();
            reads.push(tokio::spawn(async move {
                let n: i64 = w2
                    .query_scalar("SELECT count(*) FROM agents")
                    .await
                    .expect("read ok")
                    .unwrap();
                assert!(n >= 0);
            }));
        }

        for h in writes {
            h.await.expect("task").expect("no busy storm");
        }
        for h in reads {
            h.await.expect("task");
        }

        // All writes landed.
        let n: i64 = writer
            .query_scalar("SELECT count(*) FROM agents")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 21, "seed + 20 writes");

        drop((w, r, writer));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn execute_many_is_atomic() {
        let (w, r, path) = pools().await;
        let writer = IndexWriter::new(w.clone(), r.clone());

        writer
            .execute_many(&[
                "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
                 VALUES ('demo','A','x',1)"
                    .to_string(),
                "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
                 VALUES ('demo','B','y',2)"
                    .to_string(),
            ])
            .await
            .expect("batch write");

        let n: i64 = writer
            .query_scalar("SELECT count(*) FROM agents")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 2);

        drop((w, r, writer));
        let _ = std::fs::remove_file(path);
    }

    /// P1 (bd-p1-await-while-locked-9a2v): the writer `gate` tokio-Mutex guard
    /// is held across sqlx awaits BY DESIGN (single dedicated write conn;
    /// reads ride the read pool). Concurrent writers must serialize through
    /// the gate — never deadlock — and every write must land. Bounded: a gate
    /// deadlock trips the 30s timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_serialize_no_deadlock() {
        use std::sync::Arc;

        let (w, r, path) = pools().await;
        let writer = Arc::new(IndexWriter::new(w, r));

        writer
            .execute("CREATE TABLE IF NOT EXISTS writer_pin (n INTEGER)")
            .await
            .expect("ddl");

        let mut handles = Vec::new();
        for i in 0..8 {
            let w = writer.clone();
            handles.push(tokio::spawn(async move {
                for k in 0..50 {
                    w.execute(&format!("INSERT INTO writer_pin VALUES ({})", i * 1000 + k))
                        .await
                        .expect("write");
                }
            }));
        }
        let all = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for h in handles {
                h.await.expect("writer task");
            }
        })
        .await;
        assert!(
            all.is_ok(),
            "concurrent gate writers must not deadlock (guard held across await)"
        );

        let count: Option<i64> = writer
            .query_scalar("SELECT COUNT(*) FROM writer_pin")
            .await
            .expect("count");
        assert_eq!(count, Some(400), "all 8x50 writes landed");

        let _ = std::fs::remove_file(&path);
    }
}
