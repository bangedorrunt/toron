// governed-by: ADR-0003
// bd-led.4.1: AM-Rust-compatible SQLite index schema + FTS5 tokenizer contract.
// Tokenizer contract (AM schema.rs): porter unicode61 remove_diacritics 2, prefix='2 3'.

use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Connection, SqliteConnection, SqlitePool};

/// AM-Rust FTS5 tokenizer string (schema.rs parity).
pub const FTS5_TOKENIZER: &str = "porter unicode61 remove_diacritics 2";
/// AM-Rust FTS5 prefix indexes (schema.rs parity).
pub const FTS5_PREFIX: &str = "2 3";

/// Embedded migrations, relative to toron-core CARGO_MANIFEST_DIR.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// The `created_ts_us` unit contract (S3 / bd-ts-unit-audit-pmn5): every
/// `*_ts_us` column is int-microseconds (ADR G7). Values below 1e12 µs
/// (≈ 2001-09-09) are implausible for post-2001 mail and are treated as
/// seconds-scale — the audit normalizes them to µs.
///
/// Mirrors migration 0005; kept as a runtime hook so the audit test can
/// exercise the scale fix without re-running the embedded migration.
pub const TS_US_SCALE_SECONDS_THRESHOLD: i64 = 1_000_000_000_000; // 1e12 µs

/// Normalize any `messages.created_ts_us` rows whose scale is implausible
/// (seconds in a µs field, per [`TS_US_SCALE_SECONDS_THRESHOLD`]). Idempotent.
/// Returns the number of rows scaled.
pub async fn normalize_created_ts_scale(pool: &SqlitePool) -> anyhow::Result<u64> {
    let res = sqlx::query(
        "UPDATE messages \
         SET created_ts_us = created_ts_us * 1000000 \
         WHERE created_ts_us > 0 AND created_ts_us < ?",
    )
    .bind(TS_US_SCALE_SECONDS_THRESHOLD)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Open a SQLite pool with WAL + busy timeout (ADR: dedicated write conn,
/// WAL pragmas; small read pool).
pub async fn connect(db_path: &str) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5))
        .statement_cache_capacity(32);
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await?;
    // Storage-leak guard: WAL autocheckpoint + size caps. Without this an
    // idle writer can leave a multi-MB WAL that never checkpoints (the prior
    // 4 MB index.sqlite-wal that survived until reboot). 1k pages ≈ 4 MB.
    //
    // bd-741i: do NOT set temp_store=MEMORY. Nested ORDER BY scans that miss
    // the index put TEMP B-TREEs in RAM; overnight that was 48–128GB RSS
    // instead of a bounded spill file. Default FILE temp_store is the cap.
    for pragma in [
        "PRAGMA journal_size_limit = 67108864", // 64 MB hard cap
        "PRAGMA wal_autocheckpoint = 1000",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -16000", // 16 MB page cache (was 64 — 5 conns)
        "PRAGMA mmap_size = 67108864", // 64 MB mmap (file is ~20 MB)
    ] {
        let _ = sqlx::query(pragma).execute(&pool).await;
    }
    Ok(pool)
}

/// Open a SINGLE SQLite connection (WAL + busy timeout). Used by the
/// disaster-recovery seal path (`build_fresh_index`) where exactly one
/// connection must exist so `wal_checkpoint(TRUNCATE)` is guaranteed
/// busy-free (a pool's idle connections could otherwise hold it busy).
pub async fn connect_single(db_path: &str) -> anyhow::Result<SqliteConnection> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let mut conn = SqliteConnection::connect_with(&opts).await?;
    for pragma in [
        "PRAGMA journal_size_limit = 67108864",
        "PRAGMA wal_autocheckpoint = 1000",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA cache_size = -16000",
    ] {
        let _ = sqlx::query(pragma).execute(&mut conn).await;
    }
    Ok(conn)
}

/// Apply all embedded migrations, serialized against concurrent migrators
/// of the SAME database file (GS-018): sqlx inserts one row per version
/// into `_sqlx_migrations`, so two processes/threads migrating a fresh
/// shared file (e.g. the durable root under parallel test binaries) race
/// the INSERT and die with `UNIQUE constraint failed:
/// _sqlx_migrations.version`. The lock is an atomic `create_new` file
/// beside the DB (O_CREAT|O_EXCL — no new deps, POSIX-atomic); stale
/// holders (crashed mid-migrate) expire after 120s by mtime. Memory DBs
/// skip the lock. Lock is always removed, success or failure.
pub async fn migrate(pool: &SqlitePool) -> anyhow::Result<()> {
    let lock_path: Option<std::path::PathBuf> = pool
        .connect_options()
        .get_filename()
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.join(".migrate.lock"));
    if let Some(lock) = lock_path.clone() {
        let start = std::time::Instant::now();
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock)
            {
                Ok(f) => {
                    drop(f);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let stale = std::fs::metadata(&lock)
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|t| t.elapsed().ok())
                        .map(|d| d.as_secs() > 120)
                        .unwrap_or(false);
                    if stale {
                        let _ = std::fs::remove_file(&lock);
                        continue;
                    }
                    if start.elapsed().as_secs() > 300 {
                        anyhow::bail!(
                            "migrate lock held over 300s at {} — holder crashed without cleanup?",
                            lock.display()
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => anyhow::bail!("migrate lock at {}: {e:#}", lock.display()),
            }
        }
    }
    let out = MIGRATOR.run(pool).await;
    if let Some(lock) = lock_path {
        let _ = std::fs::remove_file(&lock);
    }
    out?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    /// Temp-file DB so WAL is exercisable (memory DBs ignore journal_mode).
    async fn temp_pool(tag: &str) -> (SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-index-{}-{}.sqlite",
            tag,
            uuid::Uuid::new_v4()
        ));
        let pool = connect(path.to_str().unwrap()).await.expect("connect");
        (pool, path)
    }

    async fn insert_msg(
        pool: &SqlitePool,
        event_id: &str,
        subject: &str,
        body: &str,
        ts_us: i64,
        local_seq: i64,
    ) {
        sqlx::query(
            "INSERT INTO messages \
             (event_id, project_slug, sender, sender_pubkey, subject, body, \
              importance, ack_required, to_list, cc_list, created_ts_us, local_seq) \
             VALUES (?, ?, 'PiAlpha', 'aaaa', ?, ?, 'normal', 0, '[]', '[]', ?, ?)",
        )
        .bind(event_id)
        .bind("demo")
        .bind(subject)
        .bind(body)
        .bind(ts_us)
        .bind(local_seq)
        .execute(pool)
        .await
        .expect("insert message");
    }

    #[tokio::test]
    async fn migration_up_on_empty_db() {
        let (pool, path) = temp_pool("migrate").await;
        migrate(&pool).await.expect("migrations apply on empty DB");

        let journal: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("journal_mode readable");
        assert_eq!(journal, "wal", "WAL pragma persists");

        // Every base table present.
        for tbl in [
            "agents",
            "messages",
            "threads",
            "contacts",
            "file_reservations",
            "build_slots",
            "products",
            "product_projects",
            "proof_nonces",
            "message_reads",
        ] {
            let n: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            )
            .bind(tbl)
            .fetch_one(&pool)
            .await
            .expect("count works");
            assert_eq!(n, 1, "table {tbl} exists");
        }

        // FTS5 virtual table present.
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='messages_fts'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 1, "messages_fts exists");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn fts5_matches_stemmed_diacritics_and_prefix() {
        let (pool, path) = temp_pool("fts").await;
        migrate(&pool).await.expect("migrations apply");

        insert_msg(
            &pool,
            "e1",
            "Running builds",
            "quick brown foxes jump fence",
            1700000000000000,
            1,
        )
        .await;
        insert_msg(
            &pool,
            "e2",
            "Lunch",
            "rendezvous café résumé",
            1700000000000001,
            2,
        )
        .await;

        // Porter stem: "Running" -> "run", "builds" -> "build".
        for (term, expect, note) in [
            ("run", 1, "porter stem running->run"),
            ("build", 1, "porter stem builds->build"),
        ] {
            let n: i64 =
                sqlx::query_scalar("SELECT count(*) FROM messages_fts WHERE messages_fts MATCH ?")
                    .bind(term)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(n, expect, "{note}: MATCH {term}");
        }

        // Case folding: unicode61 folds query + doc.
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH 'QUICK'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(n, 1, "unicode61 case-fold");

        // remove_diacritics 2: 'café' indexed as 'cafe', 'résumé' as 'resume'.
        for (term, expect) in [("cafe", 1), ("resume", 1)] {
            let n: i64 =
                sqlx::query_scalar("SELECT count(*) FROM messages_fts WHERE messages_fts MATCH ?")
                    .bind(term)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(n, expect, "remove_diacritics: MATCH {term}");
        }

        // prefix='2 3': 2- and 3-char prefix queries via `term*`.
        for (term, expect, note) in [
            ("qu*", 1, "2-char prefix qu* -> quick"),
            ("jum*", 1, "3-char prefix jum* -> jump"),
            ("br*", 1, "2-char prefix br* -> brown"),
            ("zzz*", 0, "no match"),
        ] {
            let n: i64 =
                sqlx::query_scalar("SELECT count(*) FROM messages_fts WHERE messages_fts MATCH ?")
                    .bind(term)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(n, expect, "{note}: MATCH {term}");
        }

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// Golden tokenizer contract, behavioral (documented golden tokens per AM
    /// contract; `fts5_tokenize` TVF is not built into the system SQLite, so
    /// the contract is asserted via MATCH semantics which are equivalent).
    #[tokio::test]
    async fn fts5_tokenizer_golden_behavior() {
        let (pool, path) = temp_pool("golden").await;
        migrate(&pool).await.expect("migrations apply");

        // The AM golden fixture corpus: each string + the terms that MUST hit it.
        let cases: &[(&str, &[&str])] = &[
            // porter stems "quickly" -> "quickli" (NOT "quick").
            (
                "Running builds café quickly",
                &["run", "build", "cafe", "quickli"],
            ),
            ("The 2024 Q3 report", &["2024", "report"]),
            ("rendezvous résumé", &["resume"]),
            ("NOSQL is not SQL", &["nosql", "sql"]),
        ];
        for (i, (doc, expected)) in cases.iter().enumerate() {
            insert_msg(
                &pool,
                &format!("g{i}"),
                doc,
                "",
                1_700_000_000_000_000 + i as i64,
                i as i64 + 1,
            )
            .await;
            for term in *expected {
                let n: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM messages_fts WHERE messages_fts MATCH ?",
                )
                .bind(term)
                .fetch_one(&pool)
                .await
                .unwrap();
                assert_eq!(n, 1, "golden doc {doc:?} must MATCH {term}");
            }
        }

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// i64-µs + local_seq columns exist (ADR G7 ordering substrate).
    #[tokio::test]
    async fn microsecond_and_local_seq_columns() {
        let (pool, path) = temp_pool("us").await;
        migrate(&pool).await.expect("migrations apply");

        let cols = sqlx::query(
            "SELECT name FROM pragma_table_info('messages') WHERE name IN ('created_ts_us','local_seq')",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        let names: Vec<String> = cols.iter().map(|r| r.get::<String, _>("name")).collect();
        assert_eq!(names, ["created_ts_us", "local_seq"]);

        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// S3 audit (bd-ts-unit-audit-pmn5): old-era rows may carry SECONDS in
    /// the µs `created_ts_us` field (e.g. 1786508238 instead of
    /// 1786508238000000). The audit normalizes any value below 1e12 µs
    /// (≈ 2001-09-09 — implausible for post-2001 mail) to µs scale, and the
    /// migration does the same at upgrade time.
    #[tokio::test]
    async fn created_ts_us_scale_audit_normalizes_seconds_rows() {
        let (pool, path) = temp_pool("scale").await;
        migrate(&pool).await.expect("migrations apply");

        // Seconds-scale row (the reported 2026-08 era value).
        insert_msg(&pool, "e_sec", "old", "seconds scale", 1_786_508_238, 1).await;
        // Already-correct µs row must be untouched.
        insert_msg(&pool, "e_us", "new", "us scale", 1_786_509_672_000_000, 2).await;

        // Audit query: flag rows whose scale is implausible (< 1e12).
        let bad: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM messages WHERE created_ts_us > 0 AND created_ts_us < ?",
        )
        .bind(TS_US_SCALE_SECONDS_THRESHOLD)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(bad, 1, "exactly the seconds-scale row is flagged");

        // Migration path (also exercised at upgrade): normalize in place.
        let scaled = normalize_created_ts_scale(&pool).await.expect("normalize");
        assert_eq!(scaled, 1, "one row scaled");

        let ts: i64 = sqlx::query_scalar("SELECT created_ts_us FROM messages WHERE event_id = ?")
            .bind("e_sec")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(ts, 1_786_508_238_000_000, "seconds scaled to µs");
        let ts2: i64 = sqlx::query_scalar("SELECT created_ts_us FROM messages WHERE event_id = ?")
            .bind("e_us")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(ts2, 1_786_509_672_000_000, "µs row untouched");

        // Idempotent: second run scales nothing.
        let again = normalize_created_ts_scale(&pool)
            .await
            .expect("normalize again");
        assert_eq!(again, 0, "idempotent");

        drop(pool);
        let _ = std::fs::remove_file(path);
    }
}
