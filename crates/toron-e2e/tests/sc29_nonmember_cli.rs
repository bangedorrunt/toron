// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-29 — Non-member CLI post fails (ADR-0020 §6.2).
//!
//! A creates a PRIVATE channel and B joins. C (registered but NOT joined):
//!   - `toron channel post` as C → exit 1 (membership gate at the store).
//!   - `toron channel read` as C on the private channel → exit 1.
//!   - History length unchanged: A's read before/after C's attempts is the
//!     same (A's post only; C's failed post never lands on the bus).
//!
//! RED contract: `toron channel` does not exist until the wave-1 CLI lands,
//! so this fails at the first command with exit 2.
//!
//! Un-gated (ADR-0020 §6.2).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use serde_json::Value;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc29";
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
        // (`mailbox_source::decide_source` checks the daemon URL first),
        // so the CLI would hit a live daemon instead of this scenario's
        // store and reject the locally registered recipient (bd-5z0l).
        .env_remove("TORON_MCP_URL")
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn stderr_text(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

async fn setup(data_dir: &Path, project: &str, names: &[&str]) {
    std::fs::create_dir_all(data_dir).expect("create data dir");
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let mailbox = Mailbox::new(Arc::new(bus), data_dir.to_path_buf(), None).with_index_pool(pool);
    for name in names {
        mailbox
            .register_agent(project, &AgentProfile::new(*name))
            .await
            .expect("register agent");
    }
}

#[tokio::test]
async fn sc29_nonmember_cli_post_denied() {
    let dir = std::env::temp_dir().join(format!("toron-sc29-{}", uuid::Uuid::new_v4()));
    setup(&dir, PROJECT, &[A, B, C]).await;

    // A creates the private channel, B joins, A posts once.
    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc29",
            "--private",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(exit_code(&out), 0, "channel create: {}", stderr_text(&out));
    let out = run(
        &dir,
        &["channel", "join", CHANNEL, "--project", PROJECT, "--as", B],
    );
    assert_eq!(exit_code(&out), 0, "channel join: {}", stderr_text(&out));
    let out = run(
        &dir,
        &[
            "channel",
            "post",
            CHANNEL,
            "--body",
            "hello members",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(exit_code(&out), 0, "A post: {}", stderr_text(&out));

    // History length before: A sees exactly one post.
    let read_before = run(
        &dir,
        &["channel", "read", CHANNEL, "--project", PROJECT, "--as", A],
    );
    assert_eq!(
        exit_code(&read_before),
        0,
        "A read: {}",
        stderr_text(&read_before)
    );
    let before = serde_json::from_slice::<Value>(&read_before.stdout).unwrap_or_else(|e| {
        panic!(
            "read stdout JSON: {e}: {}",
            String::from_utf8_lossy(&read_before.stdout)
        )
    });
    let before_len = match &before {
        Value::Array(rows) => rows.len(),
        Value::Object(map) => map
            .values()
            .find_map(|v| v.as_array())
            .map(Vec::len)
            .unwrap_or(0),
        _ => 0,
    };

    // C (not joined): post denied, read denied.
    let out = run(
        &dir,
        &[
            "channel",
            "post",
            CHANNEL,
            "--body",
            "let me in",
            "--project",
            PROJECT,
            "--as",
            C,
        ],
    );
    assert_eq!(
        exit_code(&out),
        1,
        "non-member post must exit 1: {}",
        stderr_text(&out)
    );
    let out = run(
        &dir,
        &["channel", "read", CHANNEL, "--project", PROJECT, "--as", C],
    );
    assert_eq!(
        exit_code(&out),
        1,
        "non-member read on private channel must exit 1: {}",
        stderr_text(&out)
    );

    // History length unchanged after C's attempts.
    let read_after = run(
        &dir,
        &["channel", "read", CHANNEL, "--project", PROJECT, "--as", A],
    );
    assert_eq!(
        exit_code(&read_after),
        0,
        "A read after: {}",
        stderr_text(&read_after)
    );
    let after = serde_json::from_slice::<Value>(&read_after.stdout).unwrap_or_else(|e| {
        panic!(
            "read stdout JSON: {e}: {}",
            String::from_utf8_lossy(&read_after.stdout)
        )
    });
    let after_len = match &after {
        Value::Array(rows) => rows.len(),
        Value::Object(map) => map
            .values()
            .find_map(|v| v.as_array())
            .map(Vec::len)
            .unwrap_or(0),
        _ => 0,
    };
    assert_eq!(
        before_len, after_len,
        "history length unchanged by C's attempts"
    );
    assert_eq!(before_len, 1, "one post (A's own) before and after");

    let _ = std::fs::remove_dir_all(&dir);
}
