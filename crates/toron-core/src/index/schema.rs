// governed-by: ADR-0003
// bd-led.4.1: AM-Rust-compatible SQLite index schema + FTS5 tokenizer contract.
// Tokenizer contract (AM schema.rs): porter unicode61 remove_diacritics 2, prefix='2 3'.

use std::time::Duration;

use anyhow::Context;
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

/// Writer wait budget for a contended index database (bd-0lyz). WAL has
/// one write lock; parallel test binaries, their shelled subprocesses, and
/// a live daemon all queue on it. The old 5s budget expired under
/// workspace-parallel CPU saturation and leaked SQLITE_BUSY out of
/// otherwise-idempotent construction (`demo mailbox: database is
/// locked`). 30s waits out realistic starvation; a genuine pathology
/// still fails loud (and `.migrate.lock` bails at 300s).
///
/// Crate-private on purpose: no caller outside this module configures the
/// wait, and a public const would make the budget part of toron-core's API.
const BUSY_BUDGET: Duration = Duration::from_secs(30);

/// Open a SQLite pool with WAL + busy timeout (ADR: dedicated write conn,
/// WAL pragmas; small read pool).
pub async fn connect(db_path: &str) -> anyhow::Result<SqlitePool> {
    connect_with_budget(db_path, BUSY_BUDGET).await
}

/// [`connect`] with an explicit writer-wait budget (test seam for
/// [`BUSY_BUDGET`]: narrow budgets must still fail loud).
async fn connect_with_budget(db_path: &str, busy: Duration) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(busy)
        .statement_cache_capacity(32);
    // bd-2y36: sqlx runs `PRAGMA journal_mode=WAL` while establishing each
    // connection, and sqlite answers a journal-mode CHANGE with an immediate
    // SQLITE_BUSY — the busy handler (this budget) is never consulted — so
    // processes racing the first conversion of a shared fresh file failed
    // instantly under workspace load (pin: establish_journal_mode_busy_beats_any_budget).
    // Retry the establish inside the SAME bounded budget: once any holder
    // converts the file the pragma is a no-op and the pool lands. A non-BUSY
    // error or an expired budget fails loud immediately.
    let deadline = std::time::Instant::now() + busy;
    let pool = loop {
        match SqlitePoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(opts.clone())
            .await
        {
            Ok(pool) => break pool,
            Err(e) if is_locked(&e) && std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("establish pool at {db_path} (busy budget {busy:?})")
                });
            }
        }
    };
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

/// SQLITE_BUSY as sqlx surfaces it (primary code 5, or the locked messages)
/// — the only error class worth retrying at establish; every other error
/// fails loud on first sight.
fn is_locked(e: &sqlx::Error) -> bool {
    let Some(db) = e.as_database_error() else {
        return false;
    };
    db.code().as_deref().is_some_and(|c| c == "5") || {
        let m = db.message();
        m.contains("database is locked") || m.contains("database table is locked")
    }
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
        .busy_timeout(BUSY_BUDGET);
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

/// Highest migration version this binary resolves. Compared against the
/// store's applied set when a skew refuses the open (bd-jivw).
fn resolved_max_version() -> i64 {
    MIGRATOR.iter().map(|m| m.version).max().unwrap_or(0)
}

/// Stable machine marker for a store-skew refusal (bd-6as4).
///
/// This refusal crosses a process boundary: the doorbell shells this binary for
/// every inbox fetch and classifies the failure so `/health` can name it. Until
/// this token existed, the only way to tell a skew apart from an ordinary fetch
/// failure was to match the English sentence — so reflowing the prose would
/// silently downgrade a total outage to a generic fetch error. The token is the
/// contract; the sentence around it is free to change.
///
/// Consumer: flywheel `doorbell-rt` `file_host::inbox_fail_kind`. It cannot
/// import this constant across repos, so it matches the literal and its own
/// test pins the spelling.
pub const STORE_SKEW_MARKER: &str = "toron-error=store_skew";

/// bd-jivw: a store can be AHEAD of the binary that opens it. The shared XDG
/// store is written by whatever toron touched it last (dev builds and
/// installed binaries share the root, ADR-0007 D3), so an older binary dies
/// on sqlx's bare `VersionMissing` and every verb reads as broken. Name the
/// skew, the store, and the fix.
fn migration_skew_message(store: &str, missing: i64, resolved_max: i64) -> String {
    format!(
        "migration {missing} is applied in {store} but this toron build resolves at most \
         {resolved_max}, so this binary is older than the store ({STORE_SKEW_MARKER}). Fix: \
         rebuild or upgrade toron from the source that wrote the store (cargo install --path \
         crates/toron-cli), or point --data-dir at a store this binary can open.",
    )
}

/// Same skew, different shape: the version exists on both sides with
/// different contents (the migration file changed after it was applied).
/// Carries the same marker, because to a consumer the meaning is identical:
/// this binary cannot open this store.
fn migration_content_mismatch_message(store: &str, version: i64) -> String {
    format!(
        "migration {version} is applied in {store} but this toron build ships different \
         contents for it, so the store was written by a different build ({STORE_SKEW_MARKER}). \
         Fix: rebuild or upgrade toron from the source that wrote the store, or point \
         --data-dir at a store this binary can open.",
    )
}

/// Map sqlx's migration failures into errors that carry the store, the skew,
/// and the fix. Every other failure keeps its own message.
fn map_migrate_error(store: &str, e: sqlx::migrate::MigrateError) -> anyhow::Error {
    match e {
        sqlx::migrate::MigrateError::VersionMissing(v) => {
            anyhow::anyhow!(migration_skew_message(store, v, resolved_max_version()))
        }
        sqlx::migrate::MigrateError::VersionMismatch(v) => {
            anyhow::anyhow!(migration_content_mismatch_message(store, v))
        }
        other => other.into(),
    }
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
    let store_label = pool.connect_options().get_filename().display().to_string();
    let out = MIGRATOR.run(pool).await;
    if let Some(lock) = lock_path {
        let _ = std::fs::remove_file(&lock);
    }
    out.map_err(|e| map_migrate_error(&store_label, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    /// bd-6as4: the skew refusal crosses a process boundary — the doorbell
    /// shells this binary for every inbox fetch — so a consumer must be able
    /// to tell it apart from an ordinary fetch failure WITHOUT matching the
    /// English. The prose is free to change; this token is not.
    #[test]
    fn store_skew_messages_carry_the_machine_marker() {
        let ahead = migration_skew_message("/tmp/store.sqlite", 14, 13);
        assert!(
            ahead.contains(STORE_SKEW_MARKER),
            "the store-ahead refusal must carry the declared marker: {ahead}"
        );
        assert!(
            ahead.contains("resolves at most"),
            "the human half stays too — an operator reads this line: {ahead}"
        );
        let mismatch = migration_content_mismatch_message("/tmp/store.sqlite", 14);
        assert!(
            mismatch.contains(STORE_SKEW_MARKER),
            "same-version-different-contents means the same thing to a consumer, \
             so it carries the same marker: {mismatch}"
        );
    }

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

    /// bd-0lyz: a queued writer must outlive a multi-second hold of the
    /// WAL write lock. Under workspace-parallel load (36 cli_robot tests
    /// + subprocesses on one PID-shared dir) construction hits held locks
    /// longer than the old 5s busy budget and SQLITE_BUSY leaked out of
    /// `demo_mailbox` as `database is locked`. The hold below is longer
    /// than 5s on purpose: this test is red whenever the budget can no
    /// longer wait out realistic contention.
    #[tokio::test]
    async fn queued_writer_outlives_a_multi_second_hold() {
        let path = std::env::temp_dir().join(format!("toron-busy-{}.sqlite", uuid::Uuid::new_v4()));
        let db = path.to_str().unwrap().to_string();
        let boot = connect(&db).await.expect("create db");
        sqlx::query("CREATE TABLE t (x INTEGER)")
            .execute(&boot)
            .await
            .expect("create table");
        drop(boot);

        // Hold the WAL write lock from a dedicated connection.
        let mut holder = connect_single(&db).await.expect("holder conn");
        let mut tx = holder.begin().await.expect("begin");
        sqlx::query("INSERT INTO t VALUES (1)")
            .execute(&mut *tx)
            .await
            .expect("hold write");

        // A writer queues for 6s while the lock is held, then must succeed
        // the instant the holder commits.
        let db2 = db.clone();
        let attempt = tokio::spawn(async move {
            let pool = connect(&db2).await.expect("attempt pool");
            sqlx::query("INSERT INTO t VALUES (2)").execute(&pool).await
        });
        tokio::time::sleep(Duration::from_millis(6000)).await;
        tx.commit().await.expect("release hold");
        attempt
            .await
            .expect("join")
            .expect("writer waited out the hold instead of surfacing SQLITE_BUSY");
    }

    /// bd-0lyz: the budget is still a real timeout — a narrow budget
    /// expires into a loud SQLITE_BUSY, so widening the default never
    /// masks a genuine lock pathology.
    #[tokio::test]
    async fn narrow_busy_budget_fails_loud() {
        let path =
            std::env::temp_dir().join(format!("toron-busy-narrow-{}.sqlite", uuid::Uuid::new_v4()));
        let db = path.to_str().unwrap().to_string();
        let boot = connect(&db).await.expect("create db");
        sqlx::query("CREATE TABLE t (x INTEGER)")
            .execute(&boot)
            .await
            .expect("create table");
        drop(boot);

        let mut holder = connect_single(&db).await.expect("holder conn");
        let mut tx = holder.begin().await.expect("begin");
        sqlx::query("INSERT INTO t VALUES (1)")
            .execute(&mut *tx)
            .await
            .expect("hold write");

        let pool = connect_with_budget(&db, Duration::from_millis(50))
            .await
            .expect("budget pool");
        let err = sqlx::query("INSERT INTO t VALUES (3)")
            .execute(&pool)
            .await
            .expect_err("a narrow budget must surface the busy, not hang");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("locked") || msg.contains("busy"),
            "loud SQLITE_BUSY expected, got: {msg}"
        );
        tx.commit().await.expect("release hold");
    }

    /// bd-2y36 PIN: sqlx applies `journal_mode(Wal)` while ESTABLISHING each
    /// pool connection, and sqlite answers a journal-mode CHANGE with an
    /// immediate SQLITE_BUSY — the busy handler (`busy_timeout`) is never
    /// consulted, so no budget can cover it. Deterministic repro: one
    /// connection holds a write transaction on a rollback-journal file; a
    /// RAW pool establish (bypassing our retry layer, the exact call frame
    /// the workspace-load failure blamed) with a 3s budget must fail in
    /// well under a second — a budget-respecting wait would take ~3s.
    #[tokio::test]
    async fn establish_journal_mode_busy_beats_any_budget() {
        let path = std::env::temp_dir().join(format!(
            "toron-journal-race-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let db = path.to_str().unwrap().to_string();

        // Keep the file in rollback-journal mode with a held write txn.
        let mut holder = SqliteConnection::connect_with(
            &SqliteConnectOptions::new()
                .filename(&db)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Delete),
        )
        .await
        .expect("holder conn");
        sqlx::query("CREATE TABLE t (x INTEGER)")
            .execute(&mut holder)
            .await
            .expect("schema");
        let mut tx = holder.begin().await.expect("begin");
        sqlx::query("INSERT INTO t VALUES (1)")
            .execute(&mut *tx)
            .await
            .expect("hold write");

        let started = std::time::Instant::now();
        let err = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&db)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(3)),
            )
            .await
            .expect_err("mode change under a held write txn must BUSY");
        let elapsed = started.elapsed();
        let msg = format!("{err:#}");
        assert!(msg.contains("locked"), "SQLITE_BUSY expected, got: {msg}");
        assert!(
            elapsed < Duration::from_millis(500),
            "BUSY must be IMMEDIATE (busy handler bypassed) — waited {elapsed:?}; a \
             budget-respecting path would wait the full 3s"
        );
        tx.rollback().await.ok();
        drop(holder);
        let _ = std::fs::remove_file(&path);
    }

    /// bd-2y36: `connect_with_budget` must out-wait an establish-time BUSY
    /// (the workspace-load failure): retry the establish inside the same
    /// bounded budget until the holder releases — then it lands.
    #[tokio::test]
    async fn connect_waits_out_establish_busy_within_budget() {
        let path = std::env::temp_dir().join(format!(
            "toron-establish-retry-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let db = path.to_str().unwrap().to_string();

        // Holder: rollback-journal file, write txn open, releases after 400ms.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let db_holder = db.clone();
        let releaser = tokio::spawn(async move {
            let mut holder = SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .filename(&db_holder)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Delete),
            )
            .await
            .expect("holder conn");
            sqlx::query("CREATE TABLE t (x INTEGER)")
                .execute(&mut holder)
                .await
                .expect("schema");
            let mut tx = holder.begin().await.expect("begin");
            sqlx::query("INSERT INTO t VALUES (1)")
                .execute(&mut *tx)
                .await
                .expect("hold write");
            let _ = ready_tx.send(());
            tokio::time::sleep(Duration::from_millis(400)).await;
            tx.commit().await.expect("release");
            drop(holder);
        });
        ready_rx.await.expect("holder ready");

        let pool = connect_with_budget(&db, Duration::from_secs(30))
            .await
            .expect("establish retries until the holder releases (bd-2y36)");
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("mode readable");
        assert_eq!(mode, "wal", "conversion completed after the retry");
        releaser.await.expect("releaser");
        drop(pool);
        let _ = std::fs::remove_file(&path);
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

    /// bd-jivw: an older binary meeting a newer store must name the skew and
    /// the fix. The repro substitutes the older binary with one fabricated
    /// applied row (version 99) that the resolved set can never contain.
    #[tokio::test]
    async fn migrate_error_names_the_missing_version_and_the_fix() {
        let (pool, path) = temp_pool("version-skew").await;
        migrate(&pool).await.expect("baseline migrate");
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, installed_on, success, checksum, execution_time) \
             VALUES (99, 'written by a newer toron', CURRENT_TIMESTAMP, TRUE, X'00', 0)",
        )
        .execute(&pool)
        .await
        .expect("fabricate a migration only a newer binary could have applied");

        let err = migrate(&pool)
            .await
            .expect_err("a store ahead of the binary must refuse to open");
        let msg = format!("{err:#}");
        assert!(msg.contains("99"), "must name the missing version: {msg}");
        let resolved_max = MIGRATOR.iter().map(|m| m.version).max().unwrap_or(0);
        assert!(
            msg.contains(&resolved_max.to_string()),
            "must name what this build resolves ({resolved_max}): {msg}"
        );
        assert!(
            msg.contains("rebuild") || msg.contains("upgrade"),
            "must name the fix: {msg}"
        );
        assert!(
            msg.contains(path.to_str().unwrap()),
            "must name the store: {msg}"
        );
        drop(pool);
        let _ = std::fs::remove_file(path);
    }

    /// bd-jivw, second arm: a migration file edited after it was applied must
    /// read as a content mismatch, not as sqlx's bare VersionMismatch.
    #[tokio::test]
    async fn migrate_error_names_a_content_mismatch() {
        let (pool, path) = temp_pool("content-skew").await;
        migrate(&pool).await.expect("baseline migrate");
        sqlx::query("UPDATE _sqlx_migrations SET checksum = X'ff' WHERE version = 13")
            .execute(&pool)
            .await
            .expect("alter an applied migration's checksum");

        let err = migrate(&pool)
            .await
            .expect_err("a modified applied migration must refuse the open");
        let msg = format!("{err:#}");
        assert!(msg.contains("13"), "must name the version: {msg}");
        assert!(
            msg.contains("different"),
            "must say the contents differ: {msg}"
        );
        assert!(
            msg.contains("rebuild") || msg.contains("upgrade"),
            "must name the fix: {msg}"
        );
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
            1_700_000_000_000_000,
            1,
        )
        .await;
        insert_msg(
            &pool,
            "e2",
            "Lunch",
            "rendezvous café résumé",
            1_700_000_000_000_001,
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
