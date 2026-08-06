// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-14a — Disaster recovery: Git rebuild of SQLite (P1 / v1 gate).
//!
//! Contract: delete SQLite → rebuild projection from Git archive → tool
//! queries (`fetch_inbox`-equivalent) match the prior snapshot.
//! No bus/relay required for this path; unwrapped canonical content in Git is
//! sufficient; signed content integrity preserved (INSERT OR IGNORE dedups).
//!
//! Gate: green in `cargo test -p toron-e2e` (feature `p1`).
#![cfg(feature = "p1")]

use sqlx::Row;
use toron_core::archive::git::GitArchive;
use toron_core::index::rebuild::{rebuild_index_from_git, MailRecord};
use toron_core::index::schema;
use toron_core::index::writer::IndexWriter;

fn sample_records() -> Vec<MailRecord> {
    vec![
        MailRecord {
            event_id: "sc14a-evt-1".into(),
            project_slug: "demo".into(),
            sender: "PiAlpha".into(),
            sender_pubkey: "a".repeat(64),
            subject: "Review".into(),
            body: "ship it".into(),
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
            event_id: "sc14a-evt-2".into(),
            project_slug: "demo".into(),
            sender: "BlueLake".into(),
            sender_pubkey: "b".repeat(64),
            subject: "Re: Review".into(),
            body: "approved".into(),
            importance: "normal".into(),
            ack_required: false,
            thread_id: Some("sc14a-evt-1".into()),
            reply_to: Some("sc14a-evt-1".into()),
            to_list: vec!["PiAlpha".into()],
            cc_list: vec![],
            created_ts_us: 1_700_000_000_100_000,
            local_seq: 2,
        },
    ]
}

/// `fetch_inbox`-equivalent snapshot: all messages for the project, ordered
/// by (created_ts_us, local_seq) — the G7 mail timeline.
async fn inbox_snapshot(writer: &IndexWriter) -> Vec<(String, String, i64)> {
    writer
        .fetch_rows(
            "SELECT event_id, body, created_ts_us FROM messages \
             ORDER BY created_ts_us, local_seq",
        )
        .await
        .expect("snapshot query")
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("event_id"),
                r.get::<String, _>("body"),
                r.get::<i64, _>("created_ts_us"),
            )
        })
        .collect()
}

#[tokio::test]
async fn sc14a_git_rebuild_of_sqlite_converges() {
    let dir = std::env::temp_dir().join(format!("toron-sc14a-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let archive_root = dir.join("archive");
    let db_path = dir.join("index.sqlite");
    let db_str = db_path.to_str().unwrap();

    // --- populate: unwrapped canonical content to Git + SQLite index ---
    let archive = GitArchive::open_or_init(&archive_root).expect("init archive");
    let write_pool = schema::connect(db_str).await.expect("write pool");
    schema::migrate(&write_pool).await.expect("migrate");
    let read_pool = schema::connect(db_str).await.expect("read pool");
    let writer = IndexWriter::new(write_pool.clone(), read_pool.clone());

    for rec in &sample_records() {
        rec.archive_to(&archive_root).expect("archive record");
        toron_core::index::rebuild::insert_record(&writer, rec)
            .await
            .expect("index record");
    }
    archive.commit("sc14a seed").ok();
    let before = inbox_snapshot(&writer).await;
    assert_eq!(before.len(), 2, "seeded two messages");
    assert_eq!(before[0].0, "sc14a-evt-1");
    assert!(
        before[0].1.contains("ship it"),
        "canonical content in index"
    );

    // --- destroy SQLite (index lost; Git archive survives) ---
    drop((write_pool, read_pool, writer));
    let _ = std::fs::remove_file(&db_path);
    assert!(!db_path.exists(), "sqlite destroyed");

    // --- rebuild from Git archive (no bus/relay) ---
    let write_pool2 = schema::connect(db_str).await.expect("reconnect write");
    schema::migrate(&write_pool2)
        .await
        .expect("migrate rebuilt");
    let read_pool2 = schema::connect(db_str).await.expect("reconnect read");
    let writer2 = IndexWriter::new(write_pool2.clone(), read_pool2.clone());

    let rebuilt = rebuild_index_from_git(&writer2, &archive_root)
        .await
        .expect("rebuild from Git");
    assert_eq!(rebuilt, 2, "both canonical records re-inserted");

    // --- convergence: tool query matches prior snapshot ---
    let after = inbox_snapshot(&writer2).await;
    assert_eq!(before, after, "fetch_inbox snapshot converges after DR");

    drop((write_pool2, read_pool2, writer2));
    let _ = std::fs::remove_dir_all(dir);
}
