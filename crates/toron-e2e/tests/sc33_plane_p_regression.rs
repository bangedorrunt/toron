// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-33 — Plane P regression (ADR-0020 §6.2).
//!
//! `toron mail send` A→B still gift-wraps (plane P intact); `fetch_inbox` as
//! C does NOT see it (named-recipient privacy); and the SC-27 channel post is
//! visible to members and is NOT a 1059 (plane S is plaintext-to-members).
//!
//! Asserts:
//!   - mail send exit 0; the shared store gains a kind-1059 gift-wrap (plane P).
//!   - B's inbox contains the message; C's inbox does not.
//!   - a channel post (kind 9) is visible to members and is not a 1059; the
//!     gift-wrap count for the channel post is 0.
//!
//! RED contract: the mail half works today (robot CLI exists); the channel
//! half fails until the wave-1 CLI lands, so the test as a whole is RED.
//!
//! Un-gated (ADR-0020 §6.2).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use serde_json::Value;
use sqlx::SqlitePool;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc33";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";
const C: &str = "flywheel-toron-coder3";

mod common;

fn toron_bin() -> PathBuf {
    common::toron_bin()
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    Command::new(toron_bin())
        .args(args)
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        // An ambient TORON_MCP_URL outranks the data-dir pin
        // (`mailbox_source::decide_source` checks the daemon URL first), so
        // the CLI would hit a live daemon instead of this scenario's store
        // and reject the locally registered recipient (bd-5z0l).
        .env_remove("TORON_MCP_URL")
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn stdout_json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout is not JSON (exit {}): {} | stderr: {}",
            exit_code(out),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

async fn setup(data_dir: &Path, project: &str, names: &[&str]) -> SqlitePool {
    std::fs::create_dir_all(data_dir).expect("create data dir");
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let mailbox =
        Mailbox::new(Arc::new(bus), data_dir.to_path_buf(), None).with_index_pool(pool.clone());
    for name in names {
        mailbox
            .register_agent(project, &AgentProfile::new(*name))
            .await
            .expect("register agent");
    }
    pool
}

async fn store_kind_count(pool: &SqlitePool, kind: u16) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM bus_events WHERE kind = ?1")
        .bind(kind as i64)
        .fetch_one(pool)
        .await
        .expect("count")
}

#[tokio::test]
async fn sc33_plane_p_still_gift_wraps_channel_does_not() {
    let dir = std::env::temp_dir().join(format!("toron-sc33-{}", uuid::Uuid::new_v4()));
    let pool = setup(&dir, PROJECT, &[A, B, C]).await;

    // --- Plane P: mail send A→B still gift-wraps. ---
    let subject = "sc33-secret";
    let out = run(
        &dir,
        &[
            "mail",
            "send",
            "--as",
            A,
            "--to",
            B,
            "--subject",
            subject,
            "--body",
            "pw=abc",
            "--project",
            PROJECT,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "mail send must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // B's inbox sees it; C's does not (fetch_inbox privacy).
    let out = run(&dir, &["mail", "inbox", "--as", B, "--project", PROJECT]);
    assert_eq!(
        exit_code(&out),
        0,
        "B inbox: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let b_inbox = stdout_json(&out);
    assert!(
        b_inbox.to_string().contains(subject),
        "B inbox must contain the gift-wrapped message: {b_inbox}"
    );
    let out = run(&dir, &["mail", "inbox", "--as", C, "--project", PROJECT]);
    assert_eq!(
        exit_code(&out),
        0,
        "C inbox: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let c_inbox = stdout_json(&out);
    assert!(
        !c_inbox.to_string().contains(subject),
        "C inbox must NOT see A→B's message (plane P named recipients): {c_inbox}"
    );

    // Plane P is gift-wrapped: kind 1059 on the shared bus.
    let wraps_after_mail = store_kind_count(&pool, 1059).await;
    assert!(
        wraps_after_mail >= 1,
        "mail send must produce a kind-1059 gift-wrap"
    );

    // --- Plane S: a channel post is kind 9 (not 1059) and member-visible. ---
    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc33",
            "--private",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel create: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(
        &dir,
        &["channel", "join", CHANNEL, "--project", PROJECT, "--as", B],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel join: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(
        &dir,
        &[
            "channel",
            "post",
            CHANNEL,
            "--body",
            "plane S is plaintext to members",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel post: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let post = stdout_json(&out);
    assert_eq!(
        post["kind"].as_u64(),
        Some(9),
        "channel post must be kind 9: {post}"
    );
    let post_id = post["event_id"].as_str().expect("event_id").to_string();

    // Member read sees it; the post is NOT a 1059; the channel post added no
    // gift-wrap (plane S never wraps).
    let out = run(
        &dir,
        &["channel", "read", CHANNEL, "--project", PROJECT, "--as", B],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "B channel read: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let read = stdout_json(&out);
    assert!(
        read.to_string().contains("plaintext to members"),
        "channel post must be visible to members: {read}"
    );
    assert_eq!(
        store_kind_count(&pool, 1059).await,
        wraps_after_mail,
        "channel post must NOT add a gift-wrap (plane S); still {wraps_after_mail}"
    );
    let _ = post_id;

    let _ = std::fs::remove_dir_all(&dir);
}
