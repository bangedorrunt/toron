// governed-by: ADR-0003
// governed-by: ADR-0031

//! `toron tui` acceptance: flags, the TTY gate, and the data contract the
//! console reads.
//!
//! The console itself cannot run on a pipe (a raw-mode dashboard needs a real
//! TTY), so the interactive surface is tested at its boundaries:
//!
//! 1. Flags are validated before the TTY gate, so `--tab bogus` reports the
//!    typo instead of pretending the terminal was the problem.
//! 2. A valid invocation on a pipe refuses with a fast, clear error and does
//!    not hang.
//! 3. The store plumbing the Mail and Agents tabs read — inbox rows with the
//!    fields the TUI maps, and the presence projection join — works on a real
//!    mailbox.
//!
//! The pure core (fold determinism, keymap, interpreter) is covered by unit
//! tests in `crates/toron-cli/src/tui/tests.rs`; this file covers the process
//! boundary those tests cannot reach.

use std::process::Command;
use std::sync::{Mutex, OnceLock, PoisonError};

use toron_core::mailbox::{FetchInbox, ListAgentsOptions, SendMessage};
use toron_core::nostr::identity::AgentProfile;

/// A `toron` subprocess pinned to this test run's data dir. An ambient
/// `TORON_MCP_URL` outranks that pin (`mailbox_source::decide_source`), which
/// would send these assertions at a developer's live daemon instead.
fn toron() -> Command {
    let mut c = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL");
    c
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
        .unwrap_or_else(PoisonError::into_inner)
}

/// An unknown `--tab` is a usage error: it is reported before the TTY gate, so
/// a script that passes a typo learns what it did wrong.
#[test]
fn tui_unknown_tab_fails_loud_before_the_tty_gate() {
    let out = toron()
        .args(["tui", "--tab", "bogus"])
        .output()
        .expect("run toron tui --tab bogus");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unknown --tab"),
        "stderr must name the bad flag, got: {stderr}"
    );
    assert!(
        !stderr.contains("TTY"),
        "the flag error must come before the TTY gate, got: {stderr}"
    );
}

/// Same contract for `--folder`.
#[test]
fn tui_unknown_folder_fails_loud() {
    let out = toron()
        .args(["tui", "--folder", "nope"])
        .output()
        .expect("run toron tui --folder nope");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unknown --folder"),
        "stderr must name the bad folder, got: {stderr}"
    );
}

/// A valid invocation on a piped stdout must exit 1 with a TTY explanation —
/// and must NOT hang (`.output()` blocks until the process exits on its own).
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

/// The Mail tab's data contract, without a TTY: register TestAgent, send it a
/// message, and check every field the console maps into a row (id, thread,
/// sender, subject, importance, ack flag, read state).
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tui_mail_pipeline_exposes_the_fields_the_console_maps() {
    let _ = shared_data_dir();
    let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
    let guard = register_guard();
    mailbox
        .register_agent("demo", &AgentProfile::new("TestAgent"))
        .await
        .expect("register TestAgent");
    drop(guard);

    mailbox
        .send_message(&SendMessage {
            project_slug: "demo".into(),
            sender_name: "TestAgent".into(),
            to: vec!["TestAgent".into()],
            subject: "console probe".into(),
            body_md: "the console reads this row".into(),
            importance: Some("high".into()),
            ack_required: true,
            ..Default::default()
        })
        .await
        .expect("send the probe message");

    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "TestAgent".into(),
            include_bodies: false,
            ..Default::default()
        })
        .await
        .expect("fetch_inbox");
    assert_eq!(inbox.len(), 1, "one message, got {inbox:?}");
    let row = &inbox[0];
    assert!(!row.id.is_empty(), "the row carries the message id");
    assert_eq!(row.sender, "TestAgent");
    assert_eq!(row.subject, "console probe");
    assert_eq!(row.importance, "high");
    assert!(row.ack_required, "the ack badge comes from this flag");
    assert!(
        row.read_ts_us.is_none(),
        "a fresh message is unread — the ● badge depends on it"
    );
    assert!(row.created_ts_us > 0, "the ledger row is timestamped");

    assert!(
        row.thread_id.is_none(),
        "a fresh top-level message has no thread tag, got {:?}",
        row.thread_id
    );
    let page = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "TestAgent".into(),
            include_bodies: true,
            ..Default::default()
        })
        .await
        .expect("fetch the body-carrying page");
    assert!(
        page.iter()
            .any(|m| m.body_md.contains("console reads this row")),
        "the reader's fetch must carry bodies, got {page:?}"
    );
    let exact = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "TestAgent".into(),
            thread_id: Some(row.id.clone()),
            ..Default::default()
        })
        .await
        .expect("thread query");
    assert!(
        exact.is_empty(),
        "an exact thread query cannot see an untagged message — the reason the \
         console reads a page instead"
    );
}

/// The Agents tab joins the roster with the presence projection. Assert both
/// halves exist on the same store the console opens: the roster names the pin,
/// and `presence list` answers (an empty list is a valid answer — it is the
/// file the console reads, not a synthesis).
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn tui_agents_pipeline_has_roster_and_presence() {
    let _ = shared_data_dir();
    let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
    let guard = register_guard();
    mailbox
        .register_agent("demo", &AgentProfile::new("TestAgent"))
        .await
        .expect("register TestAgent");
    drop(guard);

    let agents = mailbox
        .list_agents("demo", ListAgentsOptions::default())
        .await
        .expect("list_agents");
    assert!(
        agents.iter().any(|a| a.name == "TestAgent"),
        "TestAgent must be listed, got: {agents:?}"
    );

    let out = toron()
        .args(["presence", "list", "--project", "demo"])
        .output()
        .expect("run toron presence list");
    assert!(
        out.status.success(),
        "presence list must succeed, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let payload: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("presence list emits JSON rows");
    assert!(
        payload.is_array(),
        "the projection is a row array (empty until a heartbeat), got {payload}"
    );
}
