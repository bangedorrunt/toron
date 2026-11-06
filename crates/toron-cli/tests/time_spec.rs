// governed-by: ADR-0003

//! wave5-b acceptance: `--since` accepts RFC3339, relative offsets, and
//! `last` (where a cursor exists), and rejects garbage with the accepted
//! forms named. Shelled-out `toron` against an embedded data dir (no
//! daemon); agent keys live in a shared temp data dir like cli_robot.rs.

use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock, PoisonError};

use serde_json::Value;
use toron_core::nostr::identity::AgentProfile;

fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-timespec-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        // SAFETY: test process; set before any demo_mailbox construction in
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

fn toron() -> Command {
    let mut c = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL");
    c
}

fn run(args: &[&str]) -> Output {
    toron().args(args).output().expect("run toron")
}

fn register_test_agent() {
    let _ = shared_data_dir();
    static REG: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = REG
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        mailbox
            .register_agent("demo", &AgentProfile::new("TestAgent"))
            .await
            .expect("register TestAgent");
    });
}

fn stderr_of(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// `mail inbox --since 6h` exits 0 (relative offsets resolve against now).
#[test]
fn inbox_since_relative_is_accepted() {
    register_test_agent();
    let out = run(&[
        "mail",
        "inbox",
        "--as",
        "TestAgent",
        "--project",
        "demo",
        "--since",
        "6h",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "--since 6h must exit 0, stderr: {}",
        stderr_of(&out)
    );
    let v: Value = serde_json::from_slice(&out.stdout).expect("inbox prints JSON");
    assert!(v.is_array() || v.is_object(), "inbox shape kept: {v}");
}

/// `mail inbox --since 6hours` fails loudly and names the accepted forms.
#[test]
fn inbox_since_garbage_is_rejected_with_the_accepted_forms() {
    register_test_agent();
    let out = run(&[
        "mail",
        "inbox",
        "--as",
        "TestAgent",
        "--project",
        "demo",
        "--since",
        "6hours",
        "--json",
    ]);
    assert!(
        !out.status.success(),
        "--since 6hours must fail, stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let err = stderr_of(&out);
    assert!(err.contains("30s"), "names relative forms: {err}");
    assert!(err.contains("`last`"), "names `last`: {err}");
    assert!(err.contains("RFC3339"), "names RFC3339: {err}");
}

fn ensure_channel() {
    let created = run(&[
        "channel",
        "create",
        "tspec-room",
        "--name",
        "tspec",
        "--project",
        "demo",
        "--as",
        "TestAgent",
    ]);
    assert!(
        created.status.success(),
        "channel create must exit 0, stderr: {}",
        stderr_of(&created)
    );
}

/// `channel read --since 6h` exits 0 on the channel-read edge.
#[test]
fn channel_read_since_relative_is_accepted() {
    register_test_agent();
    ensure_channel();
    let out = run(&[
        "channel",
        "read",
        "tspec-room",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--since",
        "6h",
    ]);
    assert!(
        out.status.success(),
        "channel read --since 6h must exit 0, stderr: {}",
        stderr_of(&out)
    );
}

/// `channel read --since 6hours` fails loudly with the accepted forms;
/// `--since last` fails loudly too (a channel read tracks no high-water
/// cursor — never silent 0).
#[test]
fn channel_read_since_garbage_is_rejected() {
    register_test_agent();
    ensure_channel();
    let out = run(&[
        "channel",
        "read",
        "tspec-room",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--since",
        "6hours",
    ]);
    assert!(
        !out.status.success(),
        "channel read --since 6hours must fail, stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let err = stderr_of(&out);
    assert!(err.contains("30s"), "names relative forms: {err}");
    assert!(err.contains("`last`"), "names `last`: {err}");
    assert!(err.contains("RFC3339"), "names RFC3339: {err}");
}

#[test]
fn channel_read_since_last_is_rejected_for_missing_cursor() {
    register_test_agent();
    ensure_channel();
    let out = run(&[
        "channel",
        "read",
        "tspec-room",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--since",
        "last",
    ]);
    assert!(
        !out.status.success(),
        "channel read --since last must fail, stdout: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let err = stderr_of(&out);
    assert!(
        err.contains("high-water"),
        "last without a cursor names the reason: {err}"
    );
}
