// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-32 — Watch --once (ADR-0020 §6.2).
//!
//! Process 1: `toron watch --kinds 9 --channel <id> --once --timeout-ms 15000`.
//! Process 2 (after watch is listening): `toron channel post`.
//! Process 1 exits 0, stdout JSON `kind == 9`.
//!
//! Timeout with no post: exit 1.
//!
//! RED contract: `toron watch` / `toron channel` do not exist until the
//! wave-1 CLI lands, so this fails at the first command with exit 2.
//!
//! Un-gated (ADR-0020 §6.2).

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Output, Stdio};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc32";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";

mod common;

fn toron_bin() -> PathBuf {
    common::toron_bin()
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    let absent = std::env::temp_dir().join(format!("toron-e2e-absent-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&absent);
    toron_core::testkit::toron_command(toron_bin())
        .args(args)
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        .env_remove("TORON_MCP_URL")
        .env("TORON_WAKE_SOCK", &absent)
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

/// Spawn watch in the background (process 1), backed by the daemon socket
/// at `wake_sock` (bd-hs8y harness: `TORON_WAKE_SOCK` points the child's
/// socket-first tail at the test-held [`WakeServer`]). Returns the child +
/// a reader that captures the child's stderr once it exits.
fn spawn_watch(
    data_dir: &Path,
    timeout_ms: &str,
    wake_sock: &Path,
) -> (Child, std::thread::JoinHandle<String>) {
    let mut child = toron_core::testkit::toron_command(toron_bin())
        .args([
            "watch",
            "--project",
            PROJECT,
            "--as",
            B,
            "--channel",
            CHANNEL,
            "--kinds",
            "9",
            "--once",
            "--timeout-ms",
            timeout_ms,
        ])
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        .env("TORON_WAKE_SOCK", wake_sock)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn watch");
    let mut stderr = child.stderr.take().expect("watch stderr");
    let reader = std::thread::spawn(move || {
        let mut buf = String::new();
        let _ = stderr.read_to_string(&mut buf);
        buf
    });
    (child, reader)
}

async fn setup(data_dir: &Path, project: &str, names: &[&str]) -> Mailbox {
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
    mailbox
}

#[tokio::test]
async fn sc32_watch_once_sees_cross_process_post() {
    use toron_core::bus::wake::{SystemClock, WakeConfig, WakeServer};

    let dir = std::env::temp_dir().join(format!("toron-sc32-{}", uuid::Uuid::new_v4()));
    let mailbox = setup(&dir, PROJECT, &[A, B]).await;
    let sock = std::env::temp_dir().join(format!("toron-sc32-{}.sock", std::process::id()));
    let _daemon = WakeServer::new(
        WakeConfig::new(&sock),
        mailbox.bus().clone(),
        Arc::new(SystemClock),
    )
    .start()
    .await
    .expect("daemon wake.sock");

    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc32",
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

    let (mut child, stderr_reader) = spawn_watch(&dir, "15000", &sock);
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let keys = toron_core::keys::generate();
    let ev = toron_core::nostr::event::build_event(
        &keys,
        toron_core::nostr::kinds::toron_kind(9),
        "watch me",
        vec![
            toron_core::nostr::Tag::parse(["t", PROJECT]).expect("t tag"),
            toron_core::nostr::Tag::parse(["h", CHANNEL]).expect("h tag"),
        ],
        toron_core::nostr::Timestamp::now(),
    )
    .expect("sign kind-9");
    mailbox.bus().publish(ev).await.expect("daemon publish");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("watch did not exit within deadline");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let stderr = stderr_reader.join().expect("stderr reader");
    assert!(
        status.success(),
        "watch --once must exit 0, stderr: {stderr}"
    );
    assert!(
        stderr.contains(&format!("wake.sock attached at {}", sock.display())),
        "the attach must name the socket it dialed, got stderr: {stderr}"
    );

    let out = run(
        &dir,
        &[
            "watch",
            "--project",
            PROJECT,
            "--as",
            B,
            "--channel",
            CHANNEL,
            "--kinds",
            "9",
            "--once",
            "--timeout-ms",
            "15000",
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "watch --once (replay) must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let watched: Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "watch stdout JSON: {e}: {}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    assert_eq!(
        watched["kind"].as_u64(),
        Some(9),
        "watch --once must emit kind 9: {watched}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sc32_watch_once_timeout_exits_1() {
    let dir = std::env::temp_dir().join(format!("toron-sc32b-{}", uuid::Uuid::new_v4()));
    let _ = setup(&dir, PROJECT, &[A, B]).await;

    let out = run(
        &dir,
        &[
            "watch",
            "--project",
            PROJECT,
            "--as",
            B,
            "--kinds",
            "9",
            "--once",
            "--timeout-ms",
            "500",
        ],
    );
    assert_eq!(
        exit_code(&out),
        1,
        "watch timeout must exit 1: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        stderr.contains("watch: timeout"),
        "timeout stderr must say 'watch: timeout', got: {stderr}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
