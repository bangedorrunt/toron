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
use std::sync::{Mutex, OnceLock, PoisonError};

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

/// bd-pin-public-plane-s-easy.3: `toron mail inbox --since <RFC3339>` is a
/// REAL flag wired to the shared fetch_inbox since filter — not invented help
/// text. Send via the shelled-out binary, then assert the same message is
/// INCLUDED with a past since and EXCLUDED with a future since. A malformed
/// timestamp is a hard error (exit 1), never a silently-ignored filter.
#[test]
fn inbox_since_rfc3339_filters_through_binary() {
    register_test_agent();

    let marker = format!("since-{}", std::process::id());
    let sent = run(&[
        "mail",
        "send",
        "--as",
        "TestAgent",
        "--to",
        "TestAgent",
        "--subject",
        &marker,
        "--body",
        "inbox --since filter probe",
        "--project",
        "demo",
        "--json",
    ]);
    assert!(
        sent.status.success(),
        "send must exit 0, stderr: {}",
        String::from_utf8_lossy(&sent.stderr)
    );

    // Poll until the projection lands, then read the message's own
    // created_ts_us back out of the inbox (the filter compares against it).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let created_us: i64 = loop {
        let out = run(&[
            "mail",
            "inbox",
            "--as",
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
        let value: Value = serde_json::from_slice(&out.stdout).expect("inbox emits valid JSON");
        let hit = value.as_array().and_then(|arr| {
            arr.iter()
                .find(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
        });
        if let Some(m) = hit {
            break m
                .get("created_ts_us")
                .and_then(serde_json::Value::as_i64)
                .expect("inbox message carries created_ts_us");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "marker message never projected: {value}"
        );
        std::thread::sleep(std::time::Duration::from_millis(200));
    };

    let since_past = chrono::DateTime::from_timestamp_micros(created_us - 1_000_000)
        .expect("created_us is a valid timestamp")
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
    let since_future = chrono::DateTime::from_timestamp_micros(created_us + 1_000_000)
        .expect("created_us is a valid timestamp")
        .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

    // Past since: the message is strictly newer → included.
    let past = run(&[
        "mail",
        "inbox",
        "--as",
        "TestAgent",
        "--project",
        "demo",
        "--since",
        &since_past,
        "--json",
    ]);
    assert!(
        past.status.success(),
        "inbox --since (past) must exit 0, stderr: {}",
        String::from_utf8_lossy(&past.stderr)
    );
    let past_value: Value = serde_json::from_slice(&past.stdout).expect("inbox emits valid JSON");
    assert!(
        past_value.as_array().is_some_and(|arr| {
            arr.iter()
                .any(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
        }),
        "past --since must include the message, got {past_value}"
    );

    // Future since: the message is older → excluded.
    let future = run(&[
        "mail",
        "inbox",
        "--as",
        "TestAgent",
        "--project",
        "demo",
        "--since",
        &since_future,
        "--json",
    ]);
    assert!(
        future.status.success(),
        "inbox --since (future) must exit 0, stderr: {}",
        String::from_utf8_lossy(&future.stderr)
    );
    let future_value: Value =
        serde_json::from_slice(&future.stdout).expect("inbox emits valid JSON");
    assert!(
        future_value.as_array().is_some_and(|arr| {
            !arr.iter()
                .any(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
        }),
        "future --since must exclude the message, got {future_value}"
    );

    // Malformed since: hard error, never a silent pass-through.
    let malformed = run(&[
        "mail",
        "inbox",
        "--as",
        "TestAgent",
        "--project",
        "demo",
        "--since",
        "not-a-timestamp",
        "--json",
    ]);
    assert!(
        !malformed.status.success(),
        "malformed --since must exit non-zero, stdout: {}",
        String::from_utf8_lossy(&malformed.stdout)
    );
    assert!(
        String::from_utf8_lossy(&malformed.stderr).contains("RFC3339"),
        "malformed --since must name the RFC3339 contract, stderr: {}",
        String::from_utf8_lossy(&malformed.stderr)
    );
}

/// bd-pin-public-plane-s-easy.12: `toron reserve request-release` /
/// `reserve respond` wrap the existing Mailbox methods (ADR-0011 Spec 6).
/// Full roundtrip over the shared durable root: A holds src/a.rs (in-process
/// mailbox), B requests release via the shelled-out binary, B answers via
/// the binary. The ask stays on plane P (kind-14 DM); the CLI is the verb.
#[test]
fn request_release_roundtrip_through_binary() {
    let _guard = plane_guard();
    plane_register(&["HolderAgent", "AskerAgent"]);

    // A (HolderAgent) holds src/a.rs exclusively — in-process mailbox on the
    // same durable root the CLI subprocess will use.
    let lease_id = {
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async {
            let mailbox = plane_mailbox().await.expect("plane mailbox");
            let held = mailbox
                .file_reservation_paths(
                    "demo",
                    "HolderAgent",
                    &["src/a.rs".into()],
                    Some(3600),
                    true,
                    None,
                )
                .await
                .expect("HolderAgent holds src/a.rs");
            held.granted[0].id.clone()
        })
    };

    // B (AskerAgent) asks HolderAgent to release it — plane P via CLI.
    let out = plane_run(&[
        "reserve",
        "request-release",
        "--project",
        "demo",
        "--as",
        "AskerAgent",
        "--target",
        "HolderAgent",
        "--reservation-id",
        &lease_id,
        "--ask",
        "release",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "request-release must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value =
        serde_json::from_slice(&out.stdout).expect("request-release emits valid JSON");
    assert_eq!(
        value.get("holder").and_then(|v| v.as_str()),
        Some("HolderAgent"),
        "request-release must name the holder, got {value}"
    );
    assert_eq!(
        value.get("path_pattern").and_then(|v| v.as_str()),
        Some("src/a.rs"),
        "request-release must name the contested path, got {value}"
    );
    assert_eq!(
        value.get("delivered").and_then(serde_json::Value::as_bool),
        Some(true),
        "request-release must deliver, got {value}"
    );
    let request_id = value
        .get("message_id")
        .and_then(|v| v.as_str())
        .expect("request-release carries the message id")
        .to_string();

    // Holder answers via the CLI (agreed) — the response resolves the ask.
    let resp = plane_run(&[
        "reserve",
        "respond",
        "--project",
        "demo",
        "--as",
        "HolderAgent",
        "--request-id",
        &request_id,
        "--decision",
        "agreed",
        "--json",
    ]);
    assert!(
        resp.status.success(),
        "reserve respond must exit 0, stderr: {}",
        String::from_utf8_lossy(&resp.stderr)
    );
    let resp_value: Value = serde_json::from_slice(&resp.stdout).expect("respond emits valid JSON");
    assert!(
        resp_value.get("deliveries").is_some(),
        "respond parity shape carries deliveries, got {resp_value}"
    );

    // Unhappy path: unknown reservation id is a hard error naming it.
    let bad = plane_run(&[
        "reserve",
        "request-release",
        "--project",
        "demo",
        "--as",
        "AskerAgent",
        "--target",
        "HolderAgent",
        "--reservation-id",
        &"f".repeat(64),
        "--ask",
        "release",
        "--json",
    ]);
    assert!(
        !bad.status.success(),
        "unknown reservation id must exit non-zero, stdout: {}",
        String::from_utf8_lossy(&bad.stdout)
    );
    assert!(
        String::from_utf8_lossy(&bad.stderr).contains("no active held reservation"),
        "error names the missing reservation, stderr: {}",
        String::from_utf8_lossy(&bad.stderr)
    );

    // Unhappy path: malformed --ask is a usage error naming the grammar.
    let bad_ask = plane_run(&[
        "reserve",
        "request-release",
        "--project",
        "demo",
        "--as",
        "AskerAgent",
        "--target",
        "HolderAgent",
        "--reservation-id",
        &lease_id,
        "--ask",
        "maybe",
        "--json",
    ]);
    assert!(
        !bad_ask.status.success(),
        "malformed --ask must exit non-zero"
    );
    assert!(
        String::from_utf8_lossy(&bad_ask.stderr).contains("release or defer"),
        "error names the ask grammar, stderr: {}",
        String::from_utf8_lossy(&bad_ask.stderr)
    );
}

#[test]
fn inbox_parity_emits_valid_json() {
    register_test_agent();
    let out = run(&[
        "mail",
        "inbox",
        "--as",
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
        "--as",
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
            "--as",
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
                    urgent_only: false,
                    since_ts: None,
                    thread_id: None,
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
        "--as",
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
        "--as",
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

/// F14 + F11 (docs/learnings/2026-09-12-swarm-stack-friction.md): the lease
/// board is now a CLI noun (`reserve list`), and release-on-bead-close is
/// reachable without MCP (`reserve release-by-reason <bead>`). Own data dir +
/// an explicit operator key so the operator-signed revocation fires.
#[test]
fn reserve_list_board_and_release_by_reason() {
    let data_dir = std::env::temp_dir().join(format!("toron-reserve-list-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("mkdir dedicated data dir");
    let op_key = data_dir.join("operator.ncryptsec");

    // Pin the root (embedded path beats the live-daemon probe) and set the
    // operator key so `release-by-reason` is not "no operator key configured".
    let own_run = |args: &[&str]| {
        toron()
            .env("TORON_MCP_DATA_DIR", &data_dir)
            .env("TORON_OPERATOR_KEY", &op_key)
            .args(args)
            .output()
            .expect("run toron on own data dir")
    };
    let json = |out: &std::process::Output| -> Value {
        serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
            panic!(
                "expected JSON, stderr: {}\nerror: {e}",
                String::from_utf8_lossy(&out.stderr)
            )
        })
    };

    // Operator key + the pin, then one exclusive lease keyed to a bead id.
    let keygen = own_run(&[
        "key",
        "generate",
        "--role",
        "operator",
        "--out",
        op_key.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        keygen.status.success(),
        "key generate: {}",
        String::from_utf8_lossy(&keygen.stderr)
    );
    let boot = own_run(&[
        "agent",
        "bootstrap",
        "--project",
        "demo",
        "--as",
        "BoardAgent",
        "--json",
    ]);
    assert!(
        boot.status.success(),
        "bootstrap: {}",
        String::from_utf8_lossy(&boot.stderr)
    );
    let acq = own_run(&[
        "reserve",
        "acquire",
        "--paths",
        "src/board/**",
        "--reason",
        "bd-f11-test",
        "--exclusive",
        "--project",
        "demo",
        "--as",
        "BoardAgent",
        "--json",
    ]);
    assert!(
        acq.status.success(),
        "acquire: {}",
        String::from_utf8_lossy(&acq.stderr)
    );

    // The board shows the held lease with holder/reason/exclusive.
    let listed = own_run(&["reserve", "list", "--project", "demo", "--json"]);
    assert!(
        listed.status.success(),
        "list: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let board = json(&listed);
    let rows = board["reservations"]
        .as_array()
        .expect("reservations array");
    assert!(
        rows.iter().any(|r| {
            r["reason"] == "bd-f11-test" && r["holder"] == "BoardAgent" && r["exclusive"] == true
        }),
        "board must show the held lease, got {board}"
    );

    // Filters: exact holder hit, and a stranger matches nothing.
    let by_holder = json(&own_run(&[
        "reserve",
        "list",
        "--project",
        "demo",
        "--holder",
        "BoardAgent",
        "--json",
    ]));
    assert!(
        by_holder["count"].as_i64().unwrap_or(0) >= 1,
        "holder filter must keep the lease, got {by_holder}"
    );
    let miss = json(&own_run(&[
        "reserve",
        "list",
        "--project",
        "demo",
        "--holder",
        "NobodyHere",
        "--json",
    ]));
    assert_eq!(
        miss["count"].as_i64().unwrap_or(-1),
        0,
        "holder filter must exclude a stranger, got {miss}"
    );

    // Release-on-bead-close, then the board no longer carries that reason.
    let rel = own_run(&[
        "reserve",
        "release-by-reason",
        "bd-f11-test",
        "--project",
        "demo",
        "--as",
        "BoardAgent",
        "--json",
    ]);
    assert!(
        rel.status.success(),
        "release-by-reason: {}",
        String::from_utf8_lossy(&rel.stderr)
    );
    let rel_v = json(&rel);
    assert!(
        rel_v["released"].as_i64().unwrap_or(0) >= 1,
        "must release the reason-matching lease, got {rel_v}"
    );
    let after = json(&own_run(&[
        "reserve",
        "list",
        "--project",
        "demo",
        "--json",
    ]));
    assert!(
        !after["reservations"]
            .as_array()
            .map(|rows| rows.iter().any(|r| r["reason"] == "bd-f11-test"))
            .unwrap_or(false),
        "released lease must leave the board, got {after}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// bd-pin-public-plane-s-easy.14: ONE mailbox — a lease written by one
/// `toron` process must be visible to a SECOND `toron guard check` process
/// (the pre-commit hook runs in its own process against the same durable
/// root the daemon/CLI write). A guard that embeds a fresh demo_mailbox and
/// cannot replay the daemon's held lease reports false `Unreserved` — the
/// observed bug (flywheel `mail locks reserve` → daemon; `toron guard
/// check` → a different store).
#[test]
fn guard_check_sees_lease_held_by_another_process() {
    // Dedicated data dir (plane-test pattern): this test shells out multiple
    // `toron` invocations, each opening its own mailbox on the SAME root.
    // The shared cli-robot root races parallel tests' subprocesses (SQLite
    // "database is locked" / migration UNIQUE flake); a private root makes
    // the test hermetic.
    let data_dir = std::env::temp_dir().join(format!("toron-guard-own-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("mkdir dedicated data dir");
    let own_run = |args: &[&str]| {
        toron()
            .env("TORON_MCP_DATA_DIR", &data_dir)
            .args(args)
            .output()
            .expect("run toron on own data dir")
    };

    // 1. Register GuardAgent (in-process, dedicated dir) then Process A
    //    holds an EXCLUSIVE lease via the shelled-out binary.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use std::sync::Arc;
        let db = data_dir.join("index.sqlite");
        let pool = toron_core::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("pool");
        toron_core::index::schema::migrate(&pool)
            .await
            .expect("migrate");
        toron_core::mailbox::aliases::reconcile_aliases(&pool, &data_dir)
            .await
            .expect("reconcile");
        let bus = toron_core::bus::InProcessBus::open(
            pool.clone(),
            toron_core::bus::in_process::InProcessBusConfig::default(),
        )
        .await
        .expect("bus");
        let mailbox = toron_core::mailbox::Mailbox::new(Arc::new(bus), data_dir.clone(), None)
            .with_index_pool(pool);
        mailbox
            .register_agent("demo", &AgentProfile::new("GuardAgent"))
            .await
            .expect("register GuardAgent");
    });
    let acq = own_run(&[
        "reserve",
        "acquire",
        "--paths",
        "src/guard.rs",
        "--project",
        "demo",
        "--as",
        "GuardAgent",
        "--exclusive",
        "--json",
    ]);
    assert!(
        acq.status.success(),
        "acquire must exit 0, stderr: {}",
        String::from_utf8_lossy(&acq.stderr)
    );

    // 2. Process B (fresh invocation, same data dir) runs the STRICT guard
    //    against a repo staging src/guard.rs attributed to GuardAgent. The
    //    held lease MUST be visible — the guard must NOT report Unreserved.
    let repo = std::env::temp_dir().join(format!("toron-guard-repo-{}", std::process::id()));
    let _ = std::fs::create_dir_all(repo.join("src"));
    let git = |args: &[&str]| {
        let mut c = Command::new("git");
        c.current_dir(&repo).args(args);
        c.output().expect("git")
    };
    git(&["init", "-q"]);
    std::fs::write(repo.join("src/guard.rs"), "pub fn g() {}\n").unwrap();
    git(&["add", "src/guard.rs"]);
    git(&[
        "-c",
        "user.email=t@t",
        "-c",
        "user.name=t",
        "commit",
        "-qm",
        "base",
    ]);
    std::fs::write(repo.join("src/guard.rs"), "pub fn g() { // changed\n}\n").unwrap();
    git(&["add", "src/guard.rs"]);

    let out = own_run(&[
        "guard",
        "check",
        "--repo",
        repo.to_str().unwrap(),
        "--project",
        "demo",
        "--as-agent",
        "GuardAgent",
        "--strict",
        "--json",
    ]);
    assert!(
        out.status.success(),
        "guard must exit 0 (own lease visible), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("guard must emit JSON");
    let blocked = value
        .get("blocked")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        blocked.is_empty(),
        "guard must see the OTHER-process held lease as own coverage, got {value}"
    );

    // 3. Release + cleanup.
    let rel = own_run(&[
        "reserve",
        "release",
        "--project",
        "demo",
        "--as",
        "GuardAgent",
        "--json",
    ]);
    assert!(
        rel.status.success(),
        "release: {}",
        String::from_utf8_lossy(&rel.stderr)
    );
    let _ = std::fs::remove_dir_all(&repo);
    let _ = std::fs::remove_dir_all(&data_dir);
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

// ---------------------------------------------------------------------------
// ADR-0020 Spec 2 — two-plane shared surface: channel / react (plane S),
// job (plane J), presence (plane N), watch (plane W).
//
// Cross-process contract: every shelled-out `toron` invocation opens the
// same durable root (its own TORON_MCP_DATA_DIR), so channel lifecycle
// events (kind 39000/39002), posts (kind 9), jobs (43001-43006) and presence
// (projection file) survive across processes — the SC-27 shape.
//
// The plane suite uses a DEDICATED data dir (not the shared cli-robot one)
// so its extra subprocesses never contend with the pre-existing parity
// tests on the same SQLite root. Registration is serialized and the plane
// tests share one mutex: the mailbox builds + KDF key scans are heavy, and
// parallel plane tests would stall each other's unwrap loops.
// ---------------------------------------------------------------------------

/// Dedicated data dir for the two-plane tests.
fn plane_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-plane-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        d
    })
    .clone()
}

/// Build a mailbox directly against the plane data dir (mirrors
/// `demo_mailbox`'s TORON_MCP_DATA_DIR branch, but without touching the
/// process-global env var the pre-existing tests share).
async fn plane_mailbox() -> anyhow::Result<toron_core::mailbox::Mailbox> {
    use std::sync::Arc;
    let d = plane_data_dir();
    let db = d.join("index.sqlite");
    let pool = toron_core::index::schema::connect(db.to_str().unwrap()).await?;
    toron_core::index::schema::migrate(&pool).await?;
    toron_core::mailbox::aliases::reconcile_aliases(&pool, &d).await?;
    let bus = toron_core::bus::InProcessBus::open(
        pool.clone(),
        toron_core::bus::in_process::InProcessBusConfig::default(),
    )
    .await?;
    Ok(toron_core::mailbox::Mailbox::new(Arc::new(bus), d, None).with_index_pool(pool))
}

/// Serialize plane mailbox builds (KDF-heavy) across the plane tests.
fn plane_register(names: &[&str]) {
    static REG: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = REG
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = plane_mailbox().await.expect("plane mailbox");
        for name in names {
            mailbox
                .register_agent("demo", &AgentProfile::new(*name))
                .await
                .expect("register agent");
        }
    });
}

/// Run a `toron` subprocess against the plane data dir.
fn plane_run(args: &[&str]) -> Output {
    toron()
        .env("TORON_MCP_DATA_DIR", plane_data_dir())
        .args(args)
        .output()
        .expect("run toron on plane data dir")
}

/// Serialize the plane tests themselves (they share the plane data dir).
fn plane_guard() -> std::sync::MutexGuard<'static, ()> {
    static GATE: OnceLock<Mutex<()>> = OnceLock::new();
    GATE.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Channel id unique to this test run (parallel-safe).
fn channel_id(tag: &str) -> String {
    format!("steer-{tag}-{}", std::process::id())
}

#[test]
fn channel_create_post_read_react_roundtrip() {
    let _g = plane_guard();
    plane_register(&["TestAgent"]);

    // create (process 1) → kind-39000 group metadata persisted.
    let id = channel_id("rt");
    let created = plane_run(&[
        "channel",
        "create",
        &id,
        "--name",
        "Roundtrip",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        created.status.success(),
        "channel create must exit 0, stderr: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let created_json: Value = serde_json::from_slice(&created.stdout).expect("create emits JSON");
    assert_eq!(created_json["channel_id"], serde_json::json!(id));

    // post (process 2) → kind 9, binding shape {event_id, kind:9, channel, mentions}.
    let posted = plane_run(&[
        "channel",
        "post",
        &id,
        "--body",
        "hello plane S",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        posted.status.success(),
        "channel post must exit 0, stderr: {}",
        String::from_utf8_lossy(&posted.stderr)
    );
    let post: Value = serde_json::from_slice(&posted.stdout).expect("post emits JSON");
    assert_eq!(post["kind"], serde_json::json!(9), "post kind is 9");
    assert_eq!(post["channel"], serde_json::json!(id));
    assert_eq!(post["mentions"], serde_json::json!([]));
    let post_id = post["event_id"]
        .as_str()
        .expect("post event_id")
        .to_string();

    // read (process 3) → the post is visible cross-process.
    let read = plane_run(&[
        "channel",
        "read",
        &id,
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        read.status.success(),
        "channel read must exit 0, stderr: {}",
        String::from_utf8_lossy(&read.stderr)
    );
    let rows: Value = serde_json::from_slice(&read.stdout).expect("read emits JSON array");
    let found = rows
        .as_array()
        .map(|a| {
            a.iter()
                .any(|r| r["event_id"] == serde_json::json!(post_id))
        })
        .unwrap_or(false);
    assert!(found, "read must contain the posted event, got {rows}");

    // react (process 4) → kind 7 with e=target.
    let reacted = plane_run(&[
        "react",
        "--to",
        &post_id,
        "--emoji",
        ":+1:",
        "--channel",
        &id,
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        reacted.status.success(),
        "react must exit 0, stderr: {}",
        String::from_utf8_lossy(&reacted.stderr)
    );
    let reaction: Value = serde_json::from_slice(&reacted.stdout).expect("react emits JSON");
    assert_eq!(reaction["kind"], serde_json::json!(7), "reaction is kind 7");
    assert_eq!(reaction["react_to"], serde_json::json!(post_id));
}

#[test]
fn channel_post_mention_dispatches_one_job() {
    let _g = plane_guard();
    plane_register(&["TestAgent", "TestAgent2"]);

    let id = channel_id("mention");
    let created = plane_run(&[
        "channel",
        "create",
        &id,
        "--name",
        "Mention",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        created.status.success(),
        "create: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    // TestAgent2 must join so the mention can target a member of the room.
    let joined = plane_run(&[
        "channel",
        "join",
        &id,
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        joined.status.success(),
        "join: {}",
        String::from_utf8_lossy(&joined.stderr)
    );

    // @mention post → p-tag + one 43001 via the mention dispatcher.
    let posted = plane_run(&[
        "channel",
        "post",
        &id,
        "--body",
        "@TestAgent2 take the repro",
        "--mention",
        "TestAgent2",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        posted.status.success(),
        "mention post must exit 0, stderr: {}",
        String::from_utf8_lossy(&posted.stderr)
    );
    let post: Value = serde_json::from_slice(&posted.stdout).expect("post emits JSON");
    let mentions = post["mentions"].as_array().expect("mentions array");
    assert_eq!(mentions.len(), 1, "one mention p-tag");

    // job list (new process) sees exactly one 43001 for the mention.
    let listed = plane_run(&[
        "job",
        "list",
        "--project",
        "demo",
        "--assignee",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        listed.status.success(),
        "job list must exit 0, stderr: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let jobs: Value = serde_json::from_slice(&listed.stdout).expect("job list emits JSON array");
    let open_for_b: Vec<&Value> = jobs
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|j| j["state"] == serde_json::json!("open"))
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        open_for_b.len(),
        1,
        "mention must dispatch exactly ONE open 43001 for TestAgent2 (SC-27: one 43001, not N), got {jobs}"
    );
}

#[test]
fn channel_unknown_as_exits_2() {
    let _g = plane_guard();
    let out = plane_run(&[
        "channel",
        "create",
        "steer-x",
        "--name",
        "X",
        "--project",
        "demo",
        "--as",
        "NoSuchAgent",
        "--json",
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "unknown --as is a usage error (exit 2), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn channel_nonmember_post_exits_1() {
    let _g = plane_guard();
    plane_register(&["TestAgent", "TestAgent2"]);

    let id = channel_id("gate");
    let created = plane_run(&[
        "channel",
        "create",
        &id,
        "--name",
        "Gate",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        created.status.success(),
        "create: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    // TestAgent2 never joined → membership gate rejects the post (exit 1).
    let out = plane_run(&[
        "channel",
        "post",
        &id,
        "--body",
        "intrude",
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "non-member post must fail (exit 1), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn help_lists_two_plane_commands() {
    let out = plane_run(&["channel", "--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    for sub in ["create", "join", "leave", "list", "members", "post", "read"] {
        assert!(
            text.contains(sub),
            "channel --help must list {sub}, got: {text}"
        );
    }
    let job_help = plane_run(&["job", "--help"]);
    assert!(job_help.status.success());
    let jt = String::from_utf8_lossy(&job_help.stdout);
    for sub in [
        "list", "show", "claim", "progress", "result", "error", "cancel",
    ] {
        assert!(jt.contains(sub), "job --help must list {sub}, got: {jt}");
    }
}

// ---------------------------------------------------------------------------
// plane J — jobs 43001-43006 (bd-36qf.4)
// ---------------------------------------------------------------------------

/// A job in project `demo` ready to claim: a mention post dispatches one
/// 43001 for TestAgent2. Returns the job id from `job list`.
fn make_open_job(tag: &str) -> String {
    plane_register(&["TestAgent", "TestAgent2"]);
    let id = channel_id(tag);
    let created = plane_run(&[
        "channel",
        "create",
        &id,
        "--name",
        "Job",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        created.status.success(),
        "create: {}",
        String::from_utf8_lossy(&created.stderr)
    );
    let joined = plane_run(&[
        "channel",
        "join",
        &id,
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        joined.status.success(),
        "join: {}",
        String::from_utf8_lossy(&joined.stderr)
    );
    let posted = plane_run(&[
        "channel",
        "post",
        &id,
        "--body",
        "@TestAgent2 do the work",
        "--mention",
        "TestAgent2",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        posted.status.success(),
        "post: {}",
        String::from_utf8_lossy(&posted.stderr)
    );

    let listed = plane_run(&[
        "job",
        "list",
        "--project",
        "demo",
        "--assignee",
        "TestAgent2",
        "--state",
        "open",
        "--json",
    ]);
    assert!(
        listed.status.success(),
        "list: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let jobs: Value = serde_json::from_slice(&listed.stdout).expect("job list JSON");
    jobs.as_array()
        .and_then(|a| a.first())
        .and_then(|j| j["job_id"].as_str())
        .expect("at least one open job")
        .to_string()
}

#[test]
fn job_claim_first_wins_second_exits_1() {
    let _g = plane_guard();
    let job_id = make_open_job("claim");

    // First claimer (TestAgent2) → won:true.
    let first = plane_run(&[
        "job",
        "claim",
        &job_id,
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        first.status.success(),
        "first claim must exit 0, stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: Value = serde_json::from_slice(&first.stdout).expect("claim JSON");
    assert_eq!(first_json["kind"], serde_json::json!(43002));
    assert_eq!(
        first_json["won"],
        serde_json::json!(true),
        "first claimer wins"
    );
    assert_eq!(first_json["claimer"], serde_json::json!("TestAgent2"));

    // Second claimer (TestAgent) → won:false, exit 1.
    let second = plane_run(&[
        "job",
        "claim",
        &job_id,
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert_eq!(
        second.status.code(),
        Some(1),
        "second claimer must exit 1, stderr: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: Value = serde_json::from_slice(&second.stdout).expect("second claim JSON");
    assert_eq!(
        second_json["won"],
        serde_json::json!(false),
        "second claimer loses"
    );
}

#[test]
fn job_result_sets_done_visible_to_show() {
    let _g = plane_guard();
    let job_id = make_open_job("result");

    let claimed = plane_run(&[
        "job",
        "claim",
        &job_id,
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        claimed.status.success(),
        "claim: {}",
        String::from_utf8_lossy(&claimed.stderr)
    );

    let result = plane_run(&[
        "job",
        "result",
        &job_id,
        "--status",
        "ok",
        "--body",
        "receipt=abc123",
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert!(
        result.status.success(),
        "result must exit 0, stderr: {}",
        String::from_utf8_lossy(&result.stderr)
    );

    // job show (new process) reads the terminal state.
    let shown = plane_run(&["job", "show", &job_id, "--project", "demo", "--json"]);
    assert!(
        shown.status.success(),
        "show: {}",
        String::from_utf8_lossy(&shown.stderr)
    );
    let row: Value = serde_json::from_slice(&shown.stdout).expect("show JSON");
    assert_eq!(
        row["state"],
        serde_json::json!("done"),
        "result closes the job"
    );
}

#[test]
fn job_cancel_requester_only() {
    let _g = plane_guard();
    let job_id = make_open_job("cancel");

    // Non-requester (TestAgent2) cannot cancel → exit 1.
    let denied = plane_run(&[
        "job",
        "cancel",
        &job_id,
        "--project",
        "demo",
        "--as",
        "TestAgent2",
        "--json",
    ]);
    assert_eq!(
        denied.status.code(),
        Some(1),
        "non-requester cancel must exit 1, stderr: {}",
        String::from_utf8_lossy(&denied.stderr)
    );

    // Requester (TestAgent, who posted the mention) can cancel.
    let ok = plane_run(&[
        "job",
        "cancel",
        &job_id,
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        ok.status.success(),
        "requester cancel: {}",
        String::from_utf8_lossy(&ok.stderr)
    );
    let shown = plane_run(&["job", "show", &job_id, "--project", "demo", "--json"]);
    let row: Value = serde_json::from_slice(&shown.stdout).expect("show JSON");
    assert_eq!(row["state"], serde_json::json!("cancelled"));
}

#[test]
fn job_show_unknown_job_exits_1() {
    let out = plane_run(&[
        "job",
        "show",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "--project",
        "demo",
        "--json",
    ]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "unknown job must exit 1, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ---------------------------------------------------------------------------
// plane N + W — presence 20001, watch (bd-36qf.5)
// ---------------------------------------------------------------------------

#[test]
fn presence_publish_then_list_cross_process() {
    let _g = plane_guard();
    plane_register(&["TestAgent"]);

    let published = plane_run(&[
        "presence",
        "publish",
        "--state",
        "working",
        "--status",
        "reviewing",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        published.status.success(),
        "presence publish must exit 0, stderr: {}",
        String::from_utf8_lossy(&published.stderr)
    );
    let p: Value = serde_json::from_slice(&published.stdout).expect("publish JSON");
    assert_eq!(p["state"], serde_json::json!("working"));
    assert_eq!(p["name"], serde_json::json!("TestAgent"));

    // A separate process lists the heartbeat from the projection file.
    let listed = plane_run(&["presence", "list", "--project", "demo", "--json"]);
    assert!(
        listed.status.success(),
        "list: {}",
        String::from_utf8_lossy(&listed.stderr)
    );
    let rows: Value = serde_json::from_slice(&listed.stdout).expect("list JSON");
    let found = rows
        .as_array()
        .map(|a| {
            a.iter().any(|r| {
                r["name"] == serde_json::json!("TestAgent")
                    && r["state"] == serde_json::json!("working")
            })
        })
        .unwrap_or(false);
    assert!(
        found,
        "presence list must include TestAgent working, got {rows}"
    );
}

#[test]
fn watch_once_catches_cross_process_post() {
    let _g = plane_guard();
    plane_register(&["TestAgent"]);
    let id = channel_id("watch");
    let created = plane_run(&[
        "channel",
        "create",
        &id,
        "--name",
        "Watch",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        created.status.success(),
        "create: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    // Process 1: watch --once (listening on the shared durable root).
    let watch = toron()
        .env("TORON_MCP_DATA_DIR", plane_data_dir())
        .args([
            "watch",
            "--project",
            "demo",
            "--as",
            "TestAgent",
            "--kinds",
            "9",
            "--channel",
            &id,
            "--once",
            "--timeout-ms",
            "15000",
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn watch");

    // Give the watcher time to open its bus + subscription.
    std::thread::sleep(std::time::Duration::from_millis(800));

    // Process 2: post on the same root.
    let posted = plane_run(&[
        "channel",
        "post",
        &id,
        "--body",
        "seen by watch",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(
        posted.status.success(),
        "post: {}",
        String::from_utf8_lossy(&posted.stderr)
    );

    let out = watch.wait_with_output().expect("watch completes");
    assert!(
        out.status.success(),
        "watch --once must exit 0 on a match, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let ev: Value = serde_json::from_slice(&out.stdout).expect("watch emits one event JSON");
    assert_eq!(ev["kind"], serde_json::json!(9), "watched kind 9");
    assert_eq!(
        ev["content"],
        serde_json::json!("seen by watch"),
        "watched the posted content"
    );
}

#[test]
fn watch_once_timeout_exits_1() {
    let _g = plane_guard();
    plane_register(&["TestAgent"]);

    let out = plane_run(&[
        "watch",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--kinds",
        "9",
        "--channel",
        "no-such-channel",
        "--once",
        "--timeout-ms",
        "1500",
    ]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "watch timeout must exit 1, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("timeout"),
        "stderr names the timeout: {stderr}"
    );
}

#[test]
fn watch_once_without_timeout_is_usage_error() {
    let out = plane_run(&["watch", "--project", "demo", "--once", "--kinds", "9"]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "--once without --timeout-ms is a usage error (exit 2), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn watch_1059_requires_self_identity() {
    let out = plane_run(&[
        "watch",
        "--project",
        "demo",
        "--kinds",
        "1059",
        "--once",
        "--timeout-ms",
        "1000",
    ]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "watching 1059 without --as is a usage error (p-gated to self), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn presence_help_lists_commands() {
    let out = plane_run(&["presence", "--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("publish"), "presence --help lists publish");
    assert!(text.contains("list"), "presence --help lists list");
}

// ---------------------------------------------------------------------------
// bd-pin-public-plane-s-easy.14: one mailbox — the guard reads reservations
// from the daemon when a URL is set, never a second in-process demo_mailbox.
// ---------------------------------------------------------------------------

/// Boot a real `toron serve --mcp-http` daemon on a free port with its own
/// data dir. Returns (child, port). The caller must kill the child.
fn boot_daemon(data_dir: &std::path::Path) -> (std::process::Child, u16) {
    // Pick a free port: bind, note it, drop the listener.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("free port");
    let child = Command::new(env!("CARGO_BIN_EXE_toron"))
        .args(["serve", "--mcp-http", &format!("127.0.0.1:{port}")])
        .env("TORON_MCP_DATA_DIR", data_dir)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn toron serve");
    // Wait for the port to accept (the daemon mounts /mcp at bind time).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "daemon did not start listening on :{port}"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    (child, port)
}

/// Acquire an exclusive lease through the daemon's MCP tools (the HTTP/MCP
/// path — the writer's view). Uses rmcp directly, mirroring am_parity_e2e.
fn acquire_lease_via_daemon(port: u16, project: &str, agent: &str, path: &str) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use rmcp::ClientServiceExt;
        use rmcp::model::{ClientCapabilities, ClientConfig, Implementation, ProtocolVersion};
        use rmcp::service::ClientLifecycleMode;
        use rmcp::transport::streamable_http_client::{
            StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
        };
        use serde_json::json;

        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://127.0.0.1:{port}/mcp")),
        );
        let client_info = ClientConfig::new(
            ClientCapabilities::default(),
            Implementation::from_build_env(),
        );
        let client = client_info
            .serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![
                        ProtocolVersion::V_2026_07_28,
                        ProtocolVersion::V_2025_11_25,
                    ],
                },
            )
            .await
            .expect("daemon connect");

        // Register the agent on the daemon (its keys live in the daemon root).
        let reg = client
            .call_tool(
                rmcp::model::CallToolRequestParams::new("register_agent".to_string())
                    .with_arguments(
                        json!({
                            "project_key": project,
                            "name": agent,
                            "program": "test",
                        })
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                    ),
            )
            .await
            .expect("register_agent over daemon");
        assert!(!reg.is_error.unwrap_or(false), "register failed: {reg:?}");

        // Acquire the exclusive lease through the daemon.
        let acq = client
            .call_tool(
                rmcp::model::CallToolRequestParams::new("file_reservation_paths".to_string())
                    .with_arguments(
                        json!({
                            "project_key": project,
                            "agent_name": agent,
                            "paths": vec![path],
                            "exclusive": true,
                            "reason": "bd-pin-public-plane-s-easy.14",
                        })
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                    ),
            )
            .await
            .expect("file_reservation_paths over daemon");
        assert!(!acq.is_error.unwrap_or(false), "acquire failed: {acq:?}");
    });
}

/// Init a git repo with `src/a.rs` staged, return the repo path.
fn staged_git_repo(tag: &str) -> std::path::PathBuf {
    let root =
        std::env::temp_dir().join(format!("toron-guard-daemon-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("src")).expect("mkdir src");
    std::fs::write(root.join("src/a.rs"), "pub fn a() {}\n").expect("write a.rs");
    let init = Command::new("git")
        .arg("init")
        .arg("-q")
        .arg(&root)
        .output()
        .expect("git init");
    assert!(
        init.status.success(),
        "git init: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    let add = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["add", "src/a.rs"])
        .output()
        .expect("git add");
    assert!(
        add.status.success(),
        "git add: {}",
        String::from_utf8_lossy(&add.stderr)
    );
    root
}

/// The acceptance: a lease written through the HTTP/MCP path is visible to
/// `toron guard check` when TORON_MCP_URL is set — even when TORON_MCP_DATA_DIR
/// points at an EMPTY dir (a second in-process demo_mailbox() would see no
/// reservations and false-block every path as Unreserved; the daemon view
/// sees the holder).
#[test]
fn guard_check_reads_reservations_from_daemon_when_url_set() {
    let daemon_dir =
        std::env::temp_dir().join(format!("toron-guard-daemon-data-{}", std::process::id()));
    let empty_dir =
        std::env::temp_dir().join(format!("toron-guard-daemon-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    std::fs::create_dir_all(&empty_dir).expect("empty dir");

    let (mut daemon, port) = boot_daemon(&daemon_dir);
    let url = format!("http://127.0.0.1:{port}/mcp");
    let repo = staged_git_repo("a");

    acquire_lease_via_daemon(port, "demo", "TestAgent", "src/a.rs");

    // Guard check AS the holder with the daemon URL set + an EMPTY data dir:
    // must pass (own reservation visible from the daemon). If the guard used
    // demo_mailbox (empty dir) it would see no reservations → Unreserved → 1.
    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "TestAgent",
            "--strict",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("guard check via daemon");
    assert_eq!(
        out.status.code(),
        Some(0),
        "guard must see the daemon lease (own path passes), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("guard JSON");
    assert_eq!(
        value["blocked"].as_array().map(Vec::len).unwrap_or(99),
        0,
        "no blocked paths for the holder: {value}"
    );

    // The SAME check WITHOUT the URL (embedded on the empty dir) must FAIL —
    // proving the URL is what made the lease visible (demo_mailbox is not the
    // guard's source when a URL is set).
    let out_embedded = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "TestAgent",
            "--strict",
        ])
        .env_remove("TORON_MCP_URL")
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("guard check embedded on empty dir");
    assert_eq!(
        out_embedded.status.code(),
        Some(1),
        "embedded empty-dir guard must NOT see the daemon lease (control)"
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// Live daemon is the writer. `reserve request-release` / `respond` /
/// `transfer` must hit that store (compose existing MCP tools — no new
/// tool names). A bail that says "CLI-only" is a split-brain: the verbs
/// exist on the CLI and the lease lives on the daemon.
#[test]
fn request_release_respond_transfer_via_daemon_url() {
    let daemon_dir =
        std::env::temp_dir().join(format!("toron-reserve-daemon-data-{}", std::process::id()));
    let empty_dir =
        std::env::temp_dir().join(format!("toron-reserve-daemon-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    std::fs::create_dir_all(&empty_dir).expect("empty dir");

    let (mut daemon, port) = boot_daemon(&daemon_dir);
    let url = format!("http://127.0.0.1:{port}/mcp");

    let lease_id = acquire_lease_via_daemon_returning_id(port, "demo", "HolderPin", "src/a.rs");
    register_agent_via_daemon(port, "demo", "AskerPin");

    let req = toron()
        .args([
            "reserve",
            "request-release",
            "--project",
            "demo",
            "--as",
            "AskerPin",
            "--target",
            "HolderPin",
            "--reservation-id",
            &lease_id,
            "--ask",
            "release",
            "--json",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("request-release via daemon");
    let req_err = String::from_utf8_lossy(&req.stderr);
    assert!(
        req.status.success(),
        "request-release via daemon must exit 0 (not CLI-only bail), stderr: {req_err}"
    );
    assert!(
        !req_err.contains("CLI-only"),
        "daemon path must not bail CLI-only, stderr: {req_err}"
    );
    let req_value: Value = serde_json::from_slice(&req.stdout).expect("request-release JSON");
    assert_eq!(
        req_value.get("holder").and_then(|v| v.as_str()),
        Some("HolderPin"),
        "request-release names the holder, got {req_value}"
    );
    let request_id = req_value
        .get("message_id")
        .and_then(|v| v.as_str())
        .expect("message_id")
        .to_string();

    let resp = toron()
        .args([
            "reserve",
            "respond",
            "--project",
            "demo",
            "--as",
            "HolderPin",
            "--request-id",
            &request_id,
            "--decision",
            "agreed",
            "--json",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("respond via daemon");
    assert!(
        resp.status.success(),
        "respond via daemon must exit 0, stderr: {}",
        String::from_utf8_lossy(&resp.stderr)
    );

    let xfer = toron()
        .args([
            "reserve",
            "transfer",
            "--project",
            "demo",
            "--as",
            "HolderPin",
            "--to",
            "AskerPin",
            "--path",
            "src/a.rs",
            "--json",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("transfer via daemon");
    assert!(
        xfer.status.success(),
        "transfer via daemon must exit 0, stderr: {}",
        String::from_utf8_lossy(&xfer.stderr)
    );
    let xfer_value: Value = serde_json::from_slice(&xfer.stdout).expect("transfer JSON");
    assert_eq!(
        xfer_value.get("to_agent").and_then(|v| v.as_str()),
        Some("AskerPin"),
        "transfer names the new holder, got {xfer_value}"
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
}

/// Register `agent` on the live daemon (keys live in the daemon root).
fn register_agent_via_daemon(port: u16, project: &str, agent: &str) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let client = daemon_mcp_client(port).await;
        let reg = client
            .call_tool(
                rmcp::model::CallToolRequestParams::new("register_agent".to_string())
                    .with_arguments(
                        serde_json::json!({
                            "project_key": project,
                            "name": agent,
                            "program": "test",
                        })
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                    ),
            )
            .await
            .expect("register_agent over daemon");
        assert!(!reg.is_error.unwrap_or(false), "register failed: {reg:?}");
    });
}

/// Acquire an exclusive lease through the daemon and return the granted id.
fn acquire_lease_via_daemon_returning_id(
    port: u16,
    project: &str,
    agent: &str,
    path: &str,
) -> String {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let client = daemon_mcp_client(port).await;
        let reg = client
            .call_tool(
                rmcp::model::CallToolRequestParams::new("register_agent".to_string())
                    .with_arguments(
                        serde_json::json!({
                            "project_key": project,
                            "name": agent,
                            "program": "test",
                        })
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                    ),
            )
            .await
            .expect("register_agent over daemon");
        assert!(!reg.is_error.unwrap_or(false), "register failed: {reg:?}");

        let acq = client
            .call_tool(
                rmcp::model::CallToolRequestParams::new("file_reservation_paths".to_string())
                    .with_arguments(
                        serde_json::json!({
                            "project_key": project,
                            "agent_name": agent,
                            "paths": vec![path],
                            "exclusive": true,
                            "reason": "bd-pin-public-plane-s-easy.7",
                        })
                        .as_object()
                        .cloned()
                        .unwrap_or_default(),
                    ),
            )
            .await
            .expect("file_reservation_paths over daemon");
        assert!(!acq.is_error.unwrap_or(false), "acquire failed: {acq:?}");
        let text = acq
            .content
            .iter()
            .find_map(|c| c.as_text().map(|t| t.text.clone()))
            .expect("acquire text");
        let value: Value = serde_json::from_str(&text).expect("acquire JSON");
        value["granted"][0]["id"]
            .as_str()
            .expect("granted id")
            .to_string()
    })
}

async fn daemon_mcp_client(
    port: u16,
) -> rmcp::service::RunningService<rmcp::service::RoleClient, rmcp::model::ClientConfig> {
    use rmcp::ClientServiceExt;
    use rmcp::model::{ClientCapabilities, ClientConfig, Implementation, ProtocolVersion};
    use rmcp::service::ClientLifecycleMode;
    use rmcp::transport::streamable_http_client::{
        StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
    };

    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(format!("http://127.0.0.1:{port}/mcp")),
    );
    let client_info = ClientConfig::new(
        ClientCapabilities::default(),
        Implementation::from_build_env(),
    );
    client_info
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![
                    ProtocolVersion::V_2026_07_28,
                    ProtocolVersion::V_2025_11_25,
                ],
            },
        )
        .await
        .expect("daemon connect")
}

// ---------------------------------------------------------------------------
// bd-sdzw: guard hatch + shared self-holds + committer npub derivation.
// ---------------------------------------------------------------------------

/// TDD red (bd-sdzw defect 1): TORON_GUARD_STRICT=0 must exit 0 even when
/// staged paths are unreserved — the hatch is the operator escape, it must
/// not fall through to the blocking path.
#[test]
fn strict_hatch_exits_zero_when_disabled() {
    // A repo with an unreserved staged path.
    let repo = staged_git_repo("hatch");
    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "HatchAgent",
        ])
        .env("TORON_GUARD_STRICT", "0")
        .output()
        .expect("guard check with hatch");
    assert_eq!(
        out.status.code(),
        Some(0),
        "TORON_GUARD_STRICT=0 must exit 0 (hatch), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let _ = std::fs::remove_dir_all(&repo);
}

/// TDD red (bd-sdzw defect 3): the guard's committer identity must derive
/// from the SAME key source the daemon signs reservations with. A lease
/// held by CoralFinch over the daemon, checked with --as-agent CoralFinch,
/// must be seen as OWN coverage (not HeldBy npub) — the guard resolves the
/// agent name to its registered npub and compares npub-to-npub.
#[test]
fn guard_daemon_resolves_committer_to_registered_npub() {
    let daemon_dir = std::env::temp_dir().join(format!("toron-sdzw-data-{}", std::process::id()));
    let empty_dir = std::env::temp_dir().join(format!("toron-sdzw-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    std::fs::create_dir_all(&empty_dir).expect("empty dir");

    let (mut daemon, port) = boot_daemon(&daemon_dir);
    let url = format!("http://127.0.0.1:{port}/mcp");
    let repo = staged_git_repo("npub");

    // Hold an exclusive lease as CoralFinch via the daemon.
    acquire_lease_via_daemon(port, "demo", "CoralFinch", "src/a.rs");

    // Guard check AS CoralFinch with the daemon URL: own npub must match.
    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "CoralFinch",
            "--strict",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .output()
        .expect("guard check via daemon as CoralFinch");
    assert_eq!(
        out.status.code(),
        Some(0),
        "CoralFinch's own daemon lease must pass, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("guard JSON");
    assert_eq!(
        value["blocked"].as_array().map(Vec::len).unwrap_or(99),
        0,
        "no blocked paths for CoralFinch (npub resolves): {value}"
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// TDD red (bd-sdzw defect 1, held-by variant): TORON_GUARD_STRICT=0 must
/// exit 0 even when a staged path is held by ANOTHER agent — the hatch
/// disables the WHOLE sweep (strict AND non-strict blocking), it is the
/// operator escape for emergencies. The legacy non-strict guard must not
/// still block after the hatch.
#[test]
fn strict_hatch_exits_zero_when_held_by_other() {
    let daemon_dir = std::env::temp_dir().join(format!("toron-hatch-data-{}", std::process::id()));
    let empty_dir = std::env::temp_dir().join(format!("toron-hatch-empty-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    std::fs::create_dir_all(&empty_dir).expect("empty dir");

    let (mut daemon, port) = boot_daemon(&daemon_dir);
    let url = format!("http://127.0.0.1:{port}/mcp");
    let repo = staged_git_repo("hatch-other");

    // Another agent (CoralFinch) holds the path — non-strict would block.
    acquire_lease_via_daemon(port, "demo", "CoralFinch", "src/a.rs");

    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "SomeOtherAgent",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &empty_dir)
        .env("TORON_GUARD_STRICT", "0")
        .output()
        .expect("guard check with hatch, held-by-other");
    assert_eq!(
        out.status.code(),
        Some(0),
        "TORON_GUARD_STRICT=0 must exit 0 even when a path is held by another agent (hatch), stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&empty_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// TDD red (bd-sdzw defect 3, embedded witness): the guard runs from a repo
/// whose LOCAL project root holds `agents/CoralFinch/key.ncryptsec` — but the
/// reservation was written by the DAEMON under ITS root, so CoralFinch's npub
/// differs across stores. With the daemon URL set the guard MUST resolve the
/// committer from the DAEMON's agents table (npub-to-npub), never from the
/// repo-local key.
#[test]
fn guard_embedded_committer_resolves_via_daemon_npub() {
    let daemon_dir = std::env::temp_dir().join(format!("toron-sdzw2-data-{}", std::process::id()));
    let local_dir = std::env::temp_dir().join(format!("toron-sdzw2-local-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&local_dir);

    let (mut daemon, port) = boot_daemon(&daemon_dir);
    let url = format!("http://127.0.0.1:{port}/mcp");

    // Local repo root with a DIFFERENT CoralFinch key (the witness: repo-local
    // passphrase-encrypted key cannot match the daemon's).
    std::fs::create_dir_all(local_dir.join("agents/CoralFinch")).expect("local agents dir");
    // A repo whose project root is local_dir (staged under local_dir/src).
    std::fs::create_dir_all(local_dir.join("src")).expect("local src");
    std::fs::write(local_dir.join("src/a.rs"), "pub fn a() {}\n").expect("write a.rs");
    let init = Command::new("git")
        .arg("init")
        .arg("-q")
        .arg(&local_dir)
        .output()
        .expect("git init");
    assert!(init.status.success());
    let add = Command::new("git")
        .arg("-C")
        .arg(&local_dir)
        .args(["add", "src/a.rs"])
        .output()
        .expect("git add");
    assert!(add.status.success());

    // Daemon holds the lease for CoralFinch (its OWN npub).
    acquire_lease_via_daemon(port, "demo", "CoralFinch", "src/a.rs");

    // Guard from the local repo AS CoralFinch with the daemon URL: the
    // committer must resolve to the DAEMON's CoralFinch npub, so the
    // daemon-held lease is own coverage.
    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            local_dir.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "CoralFinch",
            "--strict",
        ])
        .env("TORON_MCP_URL", &url)
        .env("TORON_MCP_DATA_DIR", &daemon_dir)
        .output()
        .expect("guard check local repo as CoralFinch");
    assert_eq!(
        out.status.code(),
        Some(0),
        "CoralFinch's daemon lease must pass even with a divergent local key, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("guard JSON");
    assert_eq!(
        value["blocked"].as_array().map(Vec::len).unwrap_or(99),
        0,
        "committer resolves to daemon npub (own lease visible): {value}"
    );

    let _ = daemon.kill();
    let _ = daemon.wait();
    let _ = std::fs::remove_dir_all(&daemon_dir);
    let _ = std::fs::remove_dir_all(&local_dir);
}

/// TDD red (bd-sdzw defect 2): strict mode must see the committer's OWN
/// SHARED (non-exclusive) reservations as coverage — a shared self-lease is
/// still the agent's own reservation; only cross-agent exclusive holds block.
/// Embedded path (shared data dir, no daemon).
#[test]
fn strict_shared_self_hold_covers_path_embedded() {
    let data_dir = std::env::temp_dir().join(format!("toron-sdzw-shared-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let own_run = |args: &[&str]| {
        toron()
            .env("TORON_MCP_DATA_DIR", &data_dir)
            .args(args)
            .output()
            .expect("run toron on own data dir")
    };

    // Register + hold a SHARED (non-exclusive) lease.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use std::sync::Arc;
        let db = data_dir.join("index.sqlite");
        let pool = toron_core::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("pool");
        toron_core::index::schema::migrate(&pool)
            .await
            .expect("migrate");
        toron_core::mailbox::aliases::reconcile_aliases(&pool, &data_dir)
            .await
            .expect("reconcile");
        let bus = toron_core::bus::InProcessBus::open(
            pool.clone(),
            toron_core::bus::in_process::InProcessBusConfig::default(),
        )
        .await
        .expect("bus");
        let mailbox = toron_core::mailbox::Mailbox::new(Arc::new(bus), data_dir.clone(), None)
            .with_index_pool(pool);
        mailbox
            .register_agent("demo", &AgentProfile::new("SharedAgent"))
            .await
            .expect("register");
    });
    let acq = own_run(&[
        "reserve",
        "acquire",
        "--paths",
        "src/a.rs",
        "--project",
        "demo",
        "--as",
        "SharedAgent",
        "--json", // NO --exclusive → shared
    ]);
    assert!(
        acq.status.success(),
        "shared acquire: {}",
        String::from_utf8_lossy(&acq.stderr)
    );

    // Staged repo with src/a.rs.
    let repo = staged_git_repo("shared");
    let out = toron()
        .args([
            "guard",
            "check",
            "--repo",
            repo.to_str().unwrap(),
            "--project",
            "demo",
            "--as-agent",
            "SharedAgent",
            "--strict",
        ])
        .env("TORON_MCP_DATA_DIR", &data_dir)
        .output()
        .expect("guard check embedded shared");
    assert_eq!(
        out.status.code(),
        Some(0),
        "own SHARED lease must cover the staged path, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: Value = serde_json::from_slice(&out.stdout).expect("guard JSON");
    assert_eq!(
        value["blocked"].as_array().map(Vec::len).unwrap_or(99),
        0,
        "own shared lease is coverage: {value}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// bd-9mb8: a commit whose staged diff ADDS bd-* tokens absent from the
/// commit message is rejected by the installed commit-msg hook; naming the
/// tokens in the message passes; the TORON_GUARD_STRICT=0 hatch warns and
/// passes. Full-process e2e: real git, real hooks, second toron process.
#[test]
fn guard_commit_msg_blocks_foreign_bead_tokens() {
    let data_dir = std::env::temp_dir().join(format!("toron-guard-bd9mb8-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("mkdir dedicated data dir");

    // Register the committing agent on a private root (own-process pattern).
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        use std::sync::Arc;
        let db = data_dir.join("index.sqlite");
        let pool = toron_core::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("pool");
        toron_core::index::schema::migrate(&pool)
            .await
            .expect("migrate");
        toron_core::mailbox::aliases::reconcile_aliases(&pool, &data_dir)
            .await
            .expect("reconcile");
        let bus = toron_core::bus::InProcessBus::open(
            pool.clone(),
            toron_core::bus::in_process::InProcessBusConfig::default(),
        )
        .await
        .expect("bus");
        let mailbox = toron_core::mailbox::Mailbox::new(Arc::new(bus), data_dir.clone(), None)
            .with_index_pool(pool);
        mailbox
            .register_agent("demo", &AgentProfile::new("HunkAgent"))
            .await
            .expect("register HunkAgent");
    });

    let repo = std::env::temp_dir().join(format!("toron-hunk-repo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&repo);
    std::fs::create_dir_all(repo.join("src")).expect("src dir");

    let bin_dir = std::path::Path::new(env!("CARGO_BIN_EXE_toron"))
        .parent()
        .expect("bin dir")
        .to_path_buf();
    let path_var = std::env::var("PATH").unwrap_or_default();
    let with_env = |c: &mut Command| {
        c.current_dir(&repo)
            .env("TORON_MCP_DATA_DIR", &data_dir)
            .env("TORON_AGENT", "HunkAgent")
            .env(
                "PATH",
                std::env::join_paths(
                    std::iter::once(bin_dir.clone()).chain(std::env::split_paths(&path_var)),
                )
                .expect("path"),
            );
    };
    let git = |args: &[&str]| {
        let mut c = Command::new("git");
        with_env(&mut c);
        c.args(args).output().expect("git")
    };

    assert!(git(&["init", "-q"]).status.success(), "git init");
    // CI runners have no global git identity — the fixture must carry its
    // own or every `git commit` below fails with "tell me who you are"
    // (GS-018: build-job red on toron-only runners).
    assert!(
        git(&["config", "user.email", "ci@toron.invalid"])
            .status
            .success()
    );
    assert!(git(&["config", "user.name", "toron-ci"]).status.success());
    // Lease covers the file so the pre-commit sweep passes; only the
    // bead-hunk check may block.
    let mut acq = Command::new(env!("CARGO_BIN_EXE_toron"));
    acq.env("TORON_MCP_DATA_DIR", &data_dir)
        .args([
            "reserve",
            "acquire",
            "--paths",
            "src/hunk.rs",
            "--project",
            "demo",
            "--as",
            "HunkAgent",
            "--exclusive",
            "--json",
        ])
        .output()
        .expect("acquire lease");
    // Install BOTH guard hooks into the temp repo.
    let mut inst = Command::new(env!("CARGO_BIN_EXE_toron"));
    inst.env("TORON_MCP_DATA_DIR", &data_dir)
        .args(["guard", "install", "--repo"])
        .arg(&repo)
        .args(["--project", "demo"])
        .output()
        .expect("install guard");
    assert!(
        repo.join(".git/hooks/commit-msg").exists(),
        "install must write the commit-msg hook"
    );

    // 1. Foreign bead token in the diff, absent from the message -> blocked.
    std::fs::write(
        repo.join("src/hunk.rs"),
        "// governed-by: bd-swept-foreign\n",
    )
    .unwrap();
    assert!(git(&["add", "src/hunk.rs"]).status.success());
    let blocked = git(&["commit", "-m", "feat: no attribution here"]);
    assert!(
        !blocked.status.success(),
        "commit with unattributed bd-token must be rejected — stdout: {} stderr: {}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        err.contains("bd-swept-foreign"),
        "rejection must name the offending token, got: {err}"
    );

    // 2. Same change, token named in the message -> passes.
    let ok = git(&[
        "commit",
        "-m",
        "feat: hunk work attributed to bd-swept-foreign",
    ]);
    assert!(
        ok.status.success(),
        "message carrying the token must pass, stderr: {}",
        String::from_utf8_lossy(&ok.stderr)
    );

    // 3. Hatch open (TORON_GUARD_STRICT=0): violation warns, commit passes.
    std::fs::write(
        repo.join("src/hunk.rs"),
        "// governed-by: bd-swept-foreign\n// more bd-hatch-token\n",
    )
    .unwrap();
    assert!(git(&["add", "src/hunk.rs"]).status.success());
    let mut hatched = Command::new("git");
    with_env(&mut hatched);
    hatched.env("TORON_GUARD_STRICT", "0");
    let hatched = hatched
        .args(["commit", "-m", "feat: operator bypass"])
        .output()
        .expect("hatched commit");
    assert!(
        hatched.status.success(),
        "strict hatch must let the commit through, stderr: {}",
        String::from_utf8_lossy(&hatched.stderr)
    );
    let hatch_err = String::from_utf8_lossy(&hatched.stderr);
    assert!(
        hatch_err.contains("TORON_GUARD_STRICT=0"),
        "hatch must log the operator bypass loudly, stderr: {hatch_err}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
    let _ = std::fs::remove_dir_all(&repo);
}

/// bd-4a0u: `toron agent prune` absorbs the folded-slug leftovers `doctor`
/// names, stays preview-only under the global `--dry-run`, and is idempotent
/// on a second sweep. Exercises the real binary against the shared durable
/// root, with the leftover seeded in-process (the same store the subprocess
/// reads).
#[test]
fn agent_prune_absorbs_folded_slug_leftovers_through_binary() {
    let _ = shared_data_dir();
    let project = format!("prune-{}", std::process::id());
    let slug = "flywheel-prune-cli-oc1";
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        // No `ensure_project`: the demo mailbox has no operator key to sign a
        // project manifest, and registration + folds only need the index pool.
        mailbox
            .register_agent(&project, &AgentProfile::new(slug))
            .await
            .expect("legacy slug");
        mailbox
            .register_agent(&project, &AgentProfile::new("CliPrunePin"))
            .await
            .expect("canonical pin");
        // Record the fold WITHOUT absorbing — the legacy leftover shape.
        mailbox
            .fold_agent_name_alias_no_absorb(&project, slug, "CliPrunePin")
            .await;
    });

    // Global --dry-run previews; nothing is absorbed.
    let preview = run(&[
        "agent",
        "prune",
        "--project",
        &project,
        "--json",
        "--dry-run",
    ]);
    assert!(
        preview.status.success(),
        "preview must exit 0, stderr: {}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let preview_json: Value = serde_json::from_slice(&preview.stdout).expect("preview JSON");
    assert_eq!(
        preview_json["count"], 1,
        "one leftover previewed: {preview_json}"
    );

    let pruned = run(&["agent", "prune", "--project", &project, "--json"]);
    assert!(
        pruned.status.success(),
        "prune must exit 0, stderr: {}",
        String::from_utf8_lossy(&pruned.stderr)
    );
    let pruned_json: Value = serde_json::from_slice(&pruned.stdout).expect("prune JSON");
    assert_eq!(pruned_json["removed_count"], 1, "absorbed: {pruned_json}");
    assert_eq!(
        pruned_json["kept_count"], 0,
        "nothing stranded: {pruned_json}"
    );
    assert_eq!(
        pruned_json["removed"][0]["slug"], slug,
        "report names the absorbed slug"
    );

    // Idempotent: a second sweep finds nothing left to do.
    let again = run(&["agent", "prune", "--project", &project, "--json"]);
    assert!(again.status.success(), "re-prune must exit 0");
    let again_json: Value = serde_json::from_slice(&again.stdout).expect("re-prune JSON");
    assert_eq!(again_json["removed_count"], 0, "idempotent: {again_json}");
    assert_eq!(again_json["kept_count"], 0, "clean: {again_json}");
}
