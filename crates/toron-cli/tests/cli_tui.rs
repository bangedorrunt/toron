// governed-by: ADR-0003
// (not forbid(unsafe_code): this test sets TORON_MCP_DATA_DIR via
// std::env::set_var, which is unsafe in edition 2024; the unsafe is confined
// to the test harness, never to toron-core/toron-mcp library code.)

//! bd-esh.4 acceptance: `toron tui` TTY gate + data plumbing.
//!
//! The TUI itself cannot run on a pipe (raw-mode dashboards need a real TTY),
//! so the interactive surface is tested at its boundaries: (1) the process
//! must refuse a non-TTY stdout with a fast, clear error instead of hanging;
//! (2) help lists the subcommand; (3) the demo-mailbox plumbing the TUI reads
//! (fetch_inbox / list_agents over the same project+agent pair) works.

use std::process::Command;
use std::sync::{Mutex, OnceLock};

use toron_core::mailbox::{FetchInbox, ListAgentsOptions};
use toron_core::nostr::identity::AgentProfile;

fn toron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_toron"))
}

/// Shared per-test-run data dir so the in-process demo mailbox NEVER touches
/// the durable production root (`~/.local/share/toron` — 500+ agent keys
/// there would re-run the ~1s/key argon2 KDF on every mailbox build).
fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-tui-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        // SAFETY: test process; inherited by every shelled-out subprocess.
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

/// Serialize TestAgent key writes across tests (idempotent; same pattern as
/// cli_robot.rs) so parallel tests never race the first key file.
static REG: OnceLock<Mutex<()>> = OnceLock::new();

fn register_guard() -> std::sync::MutexGuard<'static, ()> {
    REG.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// `toron tui` on a piped stdout must exit 1 with a TTY explanation on stderr
/// — and must NOT hang (`.output()` blocks until the process exits on its own).
#[test]
fn tui_on_pipe_exits_1_with_tty_error_without_hanging() {
    let out = toron().arg("tui").output().expect("run toron tui");
    assert_eq!(
        out.status.code(),
        Some(1),
        "tui on a pipe must exit 1, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("TTY"),
        "stderr must explain the TTY requirement, got: {stderr}"
    );
}

/// `toron --help` lists the `tui` subcommand (safe: help never runs the TUI).
#[test]
fn help_lists_tui_subcommand() {
    let out = toron().arg("--help").output().expect("run toron --help");
    assert!(
        out.status.success(),
        "help must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("tui"),
        "help must list the tui subcommand, got: {text}"
    );
}

/// Data plumbing the TUI reads each tick, without a TTY: register TestAgent,
/// fetch its (empty) inbox, and confirm it appears in list_agents. Mirrors
/// the demo-mailbox pattern from cli_robot.rs on a single mailbox instance.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tui_data_plumbing_demo_mailbox() {
    let _ = shared_data_dir();
    let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
    let _guard = register_guard();
    mailbox
        .register_agent("demo", &AgentProfile::new("TestAgent"))
        .await
        .expect("register TestAgent");
    drop(_guard);

    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "TestAgent".into(),
            include_bodies: false,
            ..Default::default()
        })
        .await
        .expect("fetch_inbox");
    assert!(inbox.is_empty(), "fresh demo inbox must be empty");

    let agents = mailbox
        .list_agents("demo", ListAgentsOptions::default())
        .await
        .expect("list_agents");
    assert!(
        agents.iter().any(|a| a.name == "TestAgent"),
        "TestAgent must be listed, got: {agents:?}"
    );
}
