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

/// Open a SQLite pool with WAL + busy timeout (ADR: dedicated write conn,
/// WAL pragmas; small read pool).
pub async fn connect(db_path: &str) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await?;
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
    Ok(SqliteConnection::connect_with(&opts).await?)
}

/// Apply all embedded migrations.
pub async fn migrate(pool: &SqlitePool) -> anyhow::Result<()> {
    MIGRATOR.run(pool).await?;
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
}
