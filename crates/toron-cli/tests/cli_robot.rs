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

/// P5 (bd-p5-concurrency-stress-tests-9q8l): MULTI-PROCESS durable root —
/// TWO shelled-out toron binaries on the shared TORON_MCP_DATA_DIR: binary A
/// sends, binary B reads its inbox and sees the message (S3 at the
/// binary-to-binary level, no in-process side at all).
#[test]
fn cross_process_binary_send_binary_receive() {
    register_test_agent();

    let marker = format!("bin2bin-{}", std::process::id());
    let sent = run(&[
        "mail",
        "send",
        "--from",
        "TestAgent",
        "--to",
        "TestAgent",
        "--subject",
        &marker,
        "--body",
        "binary-to-binary send",
        "--project",
        "demo",
        "--json",
    ]);
    assert!(
        sent.status.success(),
        "binary A send must exit 0, stderr: {}",
        String::from_utf8_lossy(&sent.stderr)
    );

    // Binary B polls its inbox until the projection lands (the sender's own
    // unwrap loop projects into the shared root; bounded poll).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
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
            "binary B inbox must exit 0, stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let value: Value = serde_json::from_slice(&out.stdout).expect("inbox must emit valid JSON");
        let found = value
            .as_array()
            .map(|arr| {
                arr.iter()
                    .any(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
            })
            .unwrap_or(false);
        if found {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "binary B never saw binary A's message (multi-process durable root): {value}"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// S3 (ADR-0007 Soundness Ledger) — cross-process MAIL-CONTENT sharing over
/// the durable root (bd-p0-s3-shared-state-reword-mujh).
///
/// The pre-existing cli_robot tests prove IDENTITY sharing only (the shelled
/// binary finds TestAgent's key). This test proves MAIL CONTENT sharing:
/// a message sent in-process must be visible to a shelled-out `toron`
/// subprocess on the same `TORON_MCP_DATA_DIR`.
///
/// RED→GREEN contract: if the subprocess CANNOT see the in-process-sent
/// message, that is a genuine S3 gap (re-opens the "fully implemented"
/// claim for S3) — file as P0 finding. If it passes, the durable root
/// shares projected mail and this test pins the S3 claim.
#[test]
fn cross_process_mail_content_shares_durable_root() {
    register_test_agent();

    // 1. In-process: send a marker message (self-addressed, gift-wrapped).
    let marker = format!("s3-shared-{}", std::process::id());
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        let sent = mailbox
            .send_message(&toron_core::mailbox::messaging::SendMessage {
                project_slug: "demo".into(),
                sender_name: "TestAgent".into(),
                to: vec!["TestAgent".into()],
                subject: marker.clone(),
                body_md: "cross-process mail-content sharing probe".into(),
                cc: vec![],
                bcc: vec![],
                importance: None,
                ack_required: false,
                thread_id: None,
                sender_token: None,
                overseer: false,
                extra_tags: Vec::new(),
            })
            .await
            .expect("in-process send_message");
        assert_eq!(sent.count, 1, "one delivery");

        // 2. Wait for the projection/bus store to catch up (async unwrap
        //    loop) before shelling out — bounded, then fail with context.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let inbox = mailbox
                .fetch_inbox(&toron_core::mailbox::messaging::FetchInbox {
                    project_slug: "demo".into(),
                    agent_name: "TestAgent".into(),
                    unread_only: false,
                    limit: None,
                    include_bodies: true,
                })
                .await
                .expect("in-process inbox");
            if inbox.iter().any(|m| m.subject == marker) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "in-process projection of the marker message timed out"
            );
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });

    // 3. Shell out on the SAME TORON_MCP_DATA_DIR (inherited env).
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
        "subprocess inbox must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value =
        serde_json::from_slice(&out.stdout).expect("subprocess inbox must emit valid JSON");

    // 4. Content assertion: the subprocess inbox CONTAINS the marker.
    let found = value
        .as_array()
        .map(|arr| {
            arr.iter()
                .any(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
        })
        .unwrap_or(false);
    assert!(
        found,
        "subprocess inbox must contain the in-process-sent mail (S3 mail-content sharing), got {value}"
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
