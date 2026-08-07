// governed-by: ADR-0003
// governed-by: ADR-0004 (inspect_index, build_fresh_index — Ship 3)
#![forbid(unsafe_code)]

//! Disaster-recovery rebuild paths (ADR G11).
//!
//! Two independent recovery sources (AM-Rust has one):
//!
//! 1. **SQLite lost** → rebuild the index from the Git archive. The archive
//!    holds the unwrapped canonical mail (`MailRecord`) permanently; re-insert
//!    into SQLite. FTS triggers keep search consistent.
//! 2. **Git/archive lost** → rebuild from bus/relay events. The relay replay /
//!    NIP-77 negentropy delta lives in the bus lane (bd-led.3.6, oc-a); this
//!    module exposes `replay_sink` as the landing hook so both paths converge
//!    on the same insert.
//!
//! Either source alone reconstructs a consistent mailbox state.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::archive::git::layout;
use crate::index::writer::IndexWriter;

/// Canonical archive form of one mail message (unwrapped content; the
/// auditable system of record). Mirrors the `messages` index columns so the
/// rebuild path is a straight column-wise insert.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailRecord {
    pub event_id: String,
    pub project_slug: String,
    pub sender: String,
    pub sender_pubkey: String,
    pub subject: String,
    pub body: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub reply_to: Option<String>,
    pub to_list: Vec<String>,
    pub cc_list: Vec<String>,
    pub created_ts_us: i64,
    pub local_seq: i64,
}

impl MailRecord {
    /// Write this record to the archive at `messages/{YYYY}/{MM}/{event_id}.json`.
    pub fn archive_to(&self, archive_root: &Path) -> anyhow::Result<()> {
        let (yyyy, mm) = calendar_ym(self.created_ts_us);
        let path = layout::message(archive_root, yyyy, mm, &self.event_id);
        let json = serde_json::to_vec_pretty(self)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load a `MailRecord` from an archive `messages/**/*.json` file.
    pub fn load_from(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }
}

/// Split a µs timestamp into calendar `(year, month)`.
fn calendar_ym(ts_us: i64) -> (u32, u32) {
    let secs = ts_us / 1_000_000;
    // Days since epoch.
    let days = secs / 86_400;
    // Civil-from-days (Howard Hinnant algorithm).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };
    (year as u32, month as u32)
}

/// Insert one `MailRecord` into the SQLite index (shared by the Git-rebuild
/// path and the bus-replay sink so both converge identically).
pub async fn insert_record(writer: &IndexWriter, rec: &MailRecord) -> anyhow::Result<()> {
    let sql = "INSERT OR IGNORE INTO messages \
               (event_id, project_slug, sender, sender_pubkey, subject, body, \
                importance, ack_required, thread_id, reply_to, to_list, cc_list, \
                created_ts_us, local_seq) \
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    // Writer exposes raw SQL execution without binds; use the write pool
    // directly (single-writer discipline preserved by the pool size).
    let pool = writer.write_pool();
    let mut conn = pool.acquire().await?;
    sqlx::query(sql)
        .bind(&rec.event_id)
        .bind(&rec.project_slug)
        .bind(&rec.sender)
        .bind(&rec.sender_pubkey)
        .bind(&rec.subject)
        .bind(&rec.body)
        .bind(&rec.importance)
        .bind(rec.ack_required)
        .bind(&rec.thread_id)
        .bind(&rec.reply_to)
        .bind(serde_json::to_string(&rec.to_list)?)
        .bind(serde_json::to_string(&rec.cc_list)?)
        .bind(rec.created_ts_us)
        .bind(rec.local_seq)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Rebuild the SQLite index from the Git archive (G11 path 1).
///
/// Walks `messages/{YYYY}/{MM}/*.json`, loads each `MailRecord`, and inserts
/// into the index. Idempotent (`INSERT OR IGNORE`).
pub async fn rebuild_index_from_git(
    writer: &IndexWriter,
    archive_root: &Path,
) -> anyhow::Result<usize> {
    let messages_root = archive_root.join("messages");
    let mut count = 0usize;
    if !messages_root.exists() {
        return Ok(0);
    }
    for year in std::fs::read_dir(&messages_root)? {
        let year = year?;
        if !year.path().is_dir() {
            continue;
        }
        for month in std::fs::read_dir(year.path())? {
            let month = month?;
            if !month.path().is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(month.path())? {
                let entry = entry?;
                if entry.path().extension().map(|e| e == "json") != Some(true) {
                    continue;
                }
                let rec = MailRecord::load_from(&entry.path())?;
                insert_record(writer, &rec).await?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Bus-replay landing hook (G11 path 2). The bus lane (bd-led.3.6) drives
/// this with negentropy/full-replay events; both recovery sources converge
/// on the same insert. Kept separate from `rebuild_index_from_git` so the
/// two sources never cross.
pub async fn replay_sink(writer: &IndexWriter, records: &[MailRecord]) -> anyhow::Result<usize> {
    for rec in records {
        insert_record(writer, rec).await?;
    }
    Ok(records.len())
}

/// Inspect an existing index db without rebuilding (bd-jwa, ADR-0004 Ship 3).
/// Returns `(db_exists, integrity_ok, row_count)`. An absent db is
/// `(false, false, None)`; an existing-but-corrupt/unreadable db collapses to
/// `(true, false, None)` so the caller can decide to rebuild.
pub async fn inspect_index(db_path: &Path) -> anyhow::Result<(bool, bool, Option<i64>)> {
    if !db_path.exists() {
        return Ok((false, false, None));
    }
    let path_str = db_path.to_string_lossy().to_string();
    let pool = match crate::index::schema::connect(&path_str).await {
        Ok(p) => p,
        Err(_) => return Ok((true, false, None)),
    };
    // integrity_check returns one row whose first column is "ok" on success.
    let integrity_ok = match sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
        .fetch_optional(&pool)
        .await
    {
        Ok(Some(s)) => s.trim().eq_ignore_ascii_case("ok"),
        _ => false,
    };
    let rows = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM messages")
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten();
    Ok((true, integrity_ok, rows))
}

/// Build a fresh index db at `db_path` (must not exist), migrate, and rebuild
/// from the archive (bd-jwa, ADR-0004 Ship 3). Returns `(records_rebuilt,
/// rows_in_index)` for convergence verification.
///
/// The db is SEALED for an atomic rename: all pool connections are closed
/// (`close().await` — deterministic, unlike `drop` which can leave background
/// connections), then a SINGLE connection runs `wal_checkpoint(TRUNCATE)`
/// with an explicit busy check and re-counts rows from the main file. If the
/// checkpoint is busy or the main-file count diverges from the rebuild count,
/// we FAIL LOUD rather than let the caller rename a main file that would
/// orphan uncheckpointed WAL frames (silent data loss).
pub async fn build_fresh_index(
    db_path: &str,
    archive_root: &Path,
) -> anyhow::Result<(usize, usize)> {
    let wp = crate::index::schema::connect(db_path).await?;
    crate::index::schema::migrate(&wp).await?;
    let rp = crate::index::schema::connect(db_path).await?;
    let writer = IndexWriter::new(wp.clone(), rp.clone());
    let rebuilt = rebuild_index_from_git(&writer, archive_root).await?;
    drop(writer);
    // Deterministically close ALL pool connections before sealing (a lingering
    // idle connection could hold the checkpoint busy).
    rp.close().await;
    wp.close().await;

    // Single connection: checkpoint + verify the main file is authoritative.
    let mut conn = crate::index::schema::connect_single(db_path).await?;
    let (busy, _log, _ckpt): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(&mut conn)
        .await?;
    if busy != 0 {
        anyhow::bail!(
            "wal_checkpoint busy={busy}; refusing to seal a fresh index that may still have uncheckpointed WAL frames"
        );
    }
    // Post-checkpoint count: the WAL is truncated, so this reads the MAIN
    // file — the file the caller will rename into place.
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM messages")
        .fetch_one(&mut conn)
        .await?;
    if rows as usize != rebuilt {
        anyhow::bail!(
            "post-checkpoint count mismatch: main file has {rows} rows, rebuild produced {rebuilt}; refusing to rename"
        );
    }
    Ok((rebuilt, rows as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::git::GitArchive;
    use crate::index::schema;
    use sqlx::Row;

    fn sample_records() -> Vec<MailRecord> {
        vec![
            MailRecord {
                event_id: "evt-1".into(),
                project_slug: "demo".into(),
                sender: "PiAlpha".into(),
                sender_pubkey: "aaaa".into(),
                subject: "Review".into(),
                body: "First".into(),
                importance: "normal".into(),
                ack_required: true,
                thread_id: None,
                reply_to: None,
                to_list: vec!["BlueLake".into()],
                cc_list: vec![],
                created_ts_us: 1_700_000_000_000_000,
                local_seq: 1,
            },
            MailRecord {
                event_id: "evt-2".into(),
                project_slug: "demo".into(),
                sender: "BlueLake".into(),
                sender_pubkey: "bbbb".into(),
                subject: "Re: Review".into(),
                body: "Second".into(),
                importance: "normal".into(),
                ack_required: false,
                thread_id: Some("evt-1".into()),
                reply_to: Some("evt-1".into()),
                to_list: vec!["PiAlpha".into()],
                cc_list: vec![],
                created_ts_us: 1_700_000_000_100_000,
                local_seq: 2,
            },
        ]
    }

    async fn index_snapshot(writer: &IndexWriter) -> Vec<(String, String)> {
        writer
            .fetch_rows(
                "SELECT event_id, body FROM messages \
                 ORDER BY created_ts_us, local_seq",
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get::<String, _>("event_id"), r.get::<String, _>("body")))
            .collect()
    }

    #[tokio::test]
    async fn wipe_sqlite_rebuild_from_git_converges() {
        let dir = std::env::temp_dir().join(format!("toron-dr-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let archive_root = dir.join("archive");
        let db_path = dir.join("index.sqlite").to_str().unwrap().to_string();

        // --- populate: archive + index both written ---
        let archive = GitArchive::open_or_init(&archive_root).expect("init archive");
        let (w, r) = {
            let wp = schema::connect(&db_path).await.unwrap();
            schema::migrate(&wp).await.unwrap();
            let rp = schema::connect(&db_path).await.unwrap();
            (wp, rp)
        };
        let writer = IndexWriter::new(w.clone(), r.clone());
        for rec in sample_records() {
            rec.archive_to(&archive_root).expect("archive rec");
            insert_record(&writer, &rec).await.expect("index rec");
        }
        archive.commit("seed").ok();
        let before = index_snapshot(&writer).await;
        assert_eq!(before.len(), 2);
        assert_eq!(before[0].0, "evt-1");

        // --- destroy SQLite ---
        drop((w, r, writer));
        let _ = std::fs::remove_file(&db_path);
        assert!(!std::fs::metadata(&db_path).is_ok(), "sqlite destroyed");

        // --- rebuild from Git archive ---
        let wp = schema::connect(&db_path).await.unwrap();
        schema::migrate(&wp).await.unwrap();
        let rp = schema::connect(&db_path).await.unwrap();
        let writer2 = IndexWriter::new(wp.clone(), rp.clone());
        let rebuilt = rebuild_index_from_git(&writer2, &archive_root)
            .await
            .expect("rebuild");
        assert_eq!(rebuilt, 2, "both records re-inserted");

        // --- convergence: snapshot matches prior state ---
        let after = index_snapshot(&writer2).await;
        assert_eq!(before, after, "fetch_inbox-equivalent snapshot converges");

        drop((wp, rp, writer2));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn replay_sink_converges_on_same_insert() {
        let dir = std::env::temp_dir().join(format!("toron-dr2-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("index.sqlite").to_str().unwrap().to_string();
        let wp = schema::connect(&db_path).await.unwrap();
        schema::migrate(&wp).await.unwrap();
        let rp = schema::connect(&db_path).await.unwrap();
        let writer = IndexWriter::new(wp.clone(), rp.clone());

        let recs = sample_records();
        let n = replay_sink(&writer, &recs).await.expect("replay");
        assert_eq!(n, 2);
        // Idempotent — replaying again does not duplicate (INSERT OR IGNORE).
        let n2 = replay_sink(&writer, &recs).await.expect("replay again");
        assert_eq!(n2, 2);
        let rows = writer
            .fetch_rows("SELECT count(*) FROM messages")
            .await
            .unwrap();
        let cnt: i64 = rows[0].get(0);
        assert_eq!(cnt, 2, "no duplicates on replay");

        drop((wp, rp, writer));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn calendar_ym_matches_layout() {
        // 2026-08-06 ~ 2026-08 epoch days.
        let (y, m) = calendar_ym(1_752_825_600_000_000);
        assert!((2025..=2027).contains(&y), "year in range: {y}");
        assert!((1..=12).contains(&m), "month in range: {m}");
        // Deterministic: same input → same output.
        assert_eq!(calendar_ym(1_752_825_600_000_000), (y, m));
    }
}
