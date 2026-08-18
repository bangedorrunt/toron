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
        "--from",
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
        let value: Value = serde_json::from_slice(&out.stdout).expect("inbox emits valid JSON");
        let hit = value.as_array().and_then(|arr| {
            arr.iter().find(|m| {
                m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str())
            })
        });
        if let Some(m) = hit {
            break m
                .get("created_ts_us")
                .and_then(|v| v.as_i64())
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
        "--agent",
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
        "--agent",
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
            !arr
                .iter()
                .any(|m| m.get("subject").and_then(|s| s.as_str()) == Some(marker.as_str()))
        }),
        "future --since must exclude the message, got {future_value}"
    );

    // Malformed since: hard error, never a silent pass-through.
    let malformed = run(&[
        "mail",
        "inbox",
        "--agent",
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

// ---------------------------------------------------------------------------
// ADR-0020 Spec 2 — two-plane shared surface: channel / react (plane S),
// job (plane J), presence (plane N), watch (plane W).
//
// Cross-process contract: every shelled-out `toron` invocation opens the
// same durable root (its own TORON_MCP_DATA_DIR), so channel lifecycle
// events (kind 40/41), posts (kind 9), jobs (43001-43006) and presence
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
    let bus = toron_core::bus::InProcessBus::open(pool.clone(), Default::default()).await?;
    Ok(toron_core::mailbox::Mailbox::new(Arc::new(bus), d, None).with_index_pool(pool))
}

/// Serialize plane mailbox builds (KDF-heavy) across the plane tests.
fn plane_register(names: &[&str]) {
    static REG: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = REG
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());

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
        .unwrap_or_else(|e| e.into_inner())
}

/// Channel id unique to this test run (parallel-safe).
fn channel_id(tag: &str) -> String {
    format!("steer-{tag}-{}", std::process::id())
}

#[test]
fn channel_create_post_read_react_roundtrip() {
    let _g = plane_guard();
    plane_register(&["TestAgent"]);

    // create (process 1) → kind-40 lifecycle event persisted.
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
