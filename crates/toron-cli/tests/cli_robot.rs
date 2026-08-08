// governed-by: ADR-0003
// (not forbid(unsafe_code): this test sets TORON_MCP_DATA_DIR via
// std::env::set_var, which is unsafe in edition 2024; the unsafe is confined
// to the test harness, never to toron-core/toron-mcp library code.)

//! bd-led.8.3 acceptance: robot CLI ↔ MCP tool JSON parity.
//!
//! Each command shells out to the built `toron` binary and asserts the same
//! JSON shape the matching MCP tool returns. The demo mailbox is fully
//! in-process (no network); agent keys live in a shared temp data dir, so the
//! test registers `TestAgent` once (idempotent) before read commands that need
//! a local key.

use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock};

use serde_json::Value;
use toron_core::nostr::identity::AgentProfile;

/// Shared per-test-run data dir so the in-process `register_test_agent` and
/// the shelled-out `toron` subprocess see the SAME agent keys. `demo_mailbox`
/// honors `TORON_MCP_DATA_DIR` (otherwise it uses a per-invocation temp dir,
/// which would isolate the two processes from each other).
fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-robot-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        // SAFETY: test process; set before any demo_mailbox construction in
        // this process, and inherited by the shelled-out subprocess.
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

fn toron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_toron"))
}

fn run(args: &[&str]) -> Output {
    toron().args(args).output().expect("run toron")
}

/// Register `TestAgent` in project `demo` inside the shared demo mailbox data
/// dir — the same data dir the CLI subprocess builds via
/// `toron_mcp::demo_mailbox`. Idempotent (re-registration upserts the existing
/// key); serialized so parallel tests never race the first key write.
fn register_test_agent() {
    // Ensure the shared data dir env is set BEFORE demo_mailbox constructs
    // (in-process) and before any shelled-out subprocess (inherited env).
    let _ = shared_data_dir();
    static REG: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = REG
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        mailbox
            .register_agent("demo", &AgentProfile::new("TestAgent"))
            .await
            .expect("register TestAgent");
    });
}

#[test]
fn inbox_parity_emits_valid_json() {
    register_test_agent();
    let out = run(&[
        "mail",
        "inbox",
        "--agent",
        "TestAgent",
        "--project",
        "demo",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "inbox must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value =
        serde_json::from_slice(&out.stdout).expect("inbox must emit valid JSON on stdout");
    assert!(
        value.is_array(),
        "inbox parity shape is an array, got {value}"
    );
}

#[test]
fn send_parity_shape_or_clean_failure() {
    register_test_agent();
    let out = run(&[
        "mail",
        "send",
        "--to",
        "TestAgent",
        "--subject",
        "hi",
        "--body",
        "hello",
        "--project",
        "demo",
        "--json",
    ]);
    let code = out.status.code();
    assert!(
        code == Some(0) || code == Some(1),
        "send must succeed or fail cleanly (never panic), code={code:?}, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    match code {
        Some(0) => {
            let value: Value =
                serde_json::from_slice(&out.stdout).expect("send success JSON must parse");
            assert!(
                value.get("count").is_some() || value.get("deliveries").is_some(),
                "send parity shape must carry count/deliveries, got {value}"
            );
        }
        Some(1) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                stderr.contains("register") || stderr.contains("unknown"),
                "send failure must name the missing registration, stderr: {stderr}"
            );
        }
        _ => unreachable!("status code is 0 or 1"),
    }
}

#[test]
fn reserve_conflicts_parity_json() {
    register_test_agent();
    let out = run(&[
        "reserve",
        "conflicts",
        "--paths",
        "src/**",
        "--project",
        "demo",
        "--agent",
        "TestAgent",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "conflicts must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("conflicts must emit valid JSON");
    assert!(
        value.get("conflict_free").is_some() || value.get("conflicts").is_some(),
        "ConflictReport parity shape must carry conflict_free/conflicts, got {value}"
    );
}

#[test]
fn help_lists_robot_subcommands() {
    let out = run(&["--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    for cluster in ["mail", "reserve", "slot", "contact", "guard"] {
        assert!(
            text.contains(cluster),
            "help must list the robot cluster {cluster}, got: {text}"
        );
    }
}

#[test]
fn inbox_missing_required_arg_exits_2() {
    let out = run(&["mail", "inbox"]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "missing required --agent must be a usage error (exit 2)"
    );
}
