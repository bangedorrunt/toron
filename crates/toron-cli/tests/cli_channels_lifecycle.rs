// governed-by: ADR-0020
// governed-by: ADR-0023

//! bd-channel-close-purge-bsqb + bd-delivery-visibility-g96y +
//! bd-verify-after-send-fxtv: channel lifecycle + delivery visibility +
//! verify-after-send, through the real binary against the shared demo
//! mailbox (each `run` is a FRESH process — cross-process rebuild from the
//! bus is the contract under test).

use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock, PoisonError};

use serde_json::Value;
use toron_core::nostr::identity::AgentProfile;

fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-channels-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

/// A `toron` subprocess pinned to this test run's data dir. An ambient
/// `TORON_MCP_URL` would otherwise select the developer's daemon — and with
/// both selectors set the CLI now refuses outright, so the pin is the only
/// honest environment for these cross-process assertions.
fn toron() -> Command {
    let mut c = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL");
    c
}

fn run(args: &[&str]) -> Output {
    toron().args(args).output().expect("run toron")
}

/// Register the creator + reporter agents once (idempotent, serialized).
fn register_agents() {
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
            .register_agent("demo", &AgentProfile::new("Closer"))
            .await
            .expect("register Closer");
        mailbox
            .register_agent("demo", &AgentProfile::new("Scout"))
            .await
            .expect("register Scout");
    });
}

fn out_json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).expect("valid JSON on stdout")
}

/// Unique-per-run channel id so parallel test binaries never collide.
fn channel() -> String {
    format!("probe-{}", std::process::id())
}

/// bd-channel-close-purge-bsqb: the open -> closed -> purged lifecycle,
/// observed across processes (close in one process, gates in the next).
#[test]
fn channel_close_purge_lifecycle_through_binary() {
    register_agents();
    let ch = channel();

    assert!(
        run(&[
            "channel",
            "create",
            &ch,
            "--name",
            "Probe",
            "--project",
            "demo",
            "--as",
            "Closer",
        ])
        .status
        .success()
    );
    assert!(
        run(&["channel", "join", &ch, "--project", "demo", "--as", "Scout"])
            .status
            .success()
    );
    assert!(
        run(&[
            "channel",
            "post",
            &ch,
            "--body",
            "scout report",
            "--project",
            "demo",
            "--as",
            "Scout",
        ])
        .status
        .success()
    );

    let refused = run(&[
        "channel",
        "close",
        &ch,
        "--project",
        "demo",
        "--as",
        "Scout",
    ]);
    assert!(!refused.status.success(), "non-creator close must fail");
    assert!(String::from_utf8_lossy(&refused.stderr).contains("creator"));
    let closed = run(&[
        "channel",
        "close",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
    ]);
    assert!(
        closed.status.success(),
        "creator close must succeed: {}",
        String::from_utf8_lossy(&closed.stderr)
    );
    assert_eq!(out_json(&closed)["closed"], serde_json::json!(true));

    let post = run(&[
        "channel",
        "post",
        &ch,
        "--body",
        "after close",
        "--project",
        "demo",
        "--as",
        "Closer",
    ]);
    assert!(!post.status.success(), "post to closed channel must fail");
    assert!(String::from_utf8_lossy(&post.stderr).contains("closed"));

    let join = run(&["channel", "join", &ch, "--project", "demo", "--as", "Scout"]);
    assert!(!join.status.success(), "join on closed channel must fail");

    let read = run(&[
        "channel",
        "read",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
    ]);
    assert!(!read.status.success(), "read on closed channel must fail");

    let list = out_json(&run(&[
        "channel",
        "list",
        "--project",
        "demo",
        "--as",
        "Closer",
    ]));
    let hidden = list
        .as_array()
        .expect("list is an array")
        .iter()
        .any(|c| c["id"].as_str() == Some(ch.as_str()));
    assert!(!hidden, "closed channel is hidden from list by default");

    let list_all = out_json(&run(&[
        "channel",
        "list",
        "--project",
        "demo",
        "--as",
        "Closer",
        "--all",
    ]));
    let flagged = list_all
        .as_array()
        .expect("list --all is an array")
        .iter()
        .find(|c| c["id"].as_str() == Some(ch.as_str()))
        .expect("closed channel visible with --all");
    assert_eq!(flagged["closed"], serde_json::json!(true));

    let stranger = run(&[
        "channel",
        "purge",
        &ch,
        "--project",
        "demo",
        "--as",
        "Scout",
    ]);
    assert!(!stranger.status.success(), "non-creator purge must fail");

    let purged = run(&[
        "channel",
        "purge",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
    ]);
    assert!(
        purged.status.success(),
        "creator purge must succeed: {}",
        String::from_utf8_lossy(&purged.stderr)
    );
    assert!(
        out_json(&purged)["purged"].as_u64().expect("purged count") >= 3,
        "close 39000 supersedes create (NIP-33); join 39002 + post + close remain"
    );

    let post_after = run(&[
        "channel",
        "post",
        &ch,
        "--body",
        "ghost",
        "--project",
        "demo",
        "--as",
        "Closer",
    ]);
    assert!(!post_after.status.success(), "channel no longer exists");
}

/// bd-delivery-visibility-g96y: `channel read --author` answers "did the
/// scout report?" in one query.
#[test]
fn channel_read_author_filter_delivery_visibility() {
    register_agents();
    let ch = format!("vis-{}", std::process::id());

    assert!(
        run(&[
            "channel",
            "create",
            &ch,
            "--name",
            "Vis",
            "--project",
            "demo",
            "--as",
            "Closer",
        ])
        .status
        .success()
    );
    assert!(
        run(&["channel", "join", &ch, "--project", "demo", "--as", "Scout"])
            .status
            .success()
    );
    assert!(
        run(&[
            "channel",
            "post",
            &ch,
            "--body",
            "scout only report",
            "--project",
            "demo",
            "--as",
            "Scout",
        ])
        .status
        .success()
    );

    let delivery = out_json(&run(&[
        "channel",
        "read",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
        "--author",
        "Scout",
    ]));
    assert_eq!(
        delivery["author_name"].as_str(),
        Some("Scout"),
        "delivery summary names the author: {delivery}"
    );
    assert!(
        delivery["delivered_posts"].as_u64().expect("posts") >= 1,
        "scout's post is visible in one query: {delivery}"
    );
    assert!(
        delivery["newest_post_at"].is_u64(),
        "newest post ts present: {delivery}"
    );
    let events = delivery["events"].as_array().expect("events");
    assert!(!events.is_empty());

    let none = out_json(&run(&[
        "channel",
        "read",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
        "--author",
        "Closer",
    ]));
    assert_eq!(
        none["delivered_posts"].as_u64(),
        Some(0),
        "creator posted nothing here: {none}"
    );
}

/// bd-verify-after-send-fxtv: `channel post --wait-ack` confirms the post's
/// own event id reads back; the JSON carries verified: true.
#[test]
fn channel_post_wait_ack_verifies_readback() {
    register_agents();
    let ch = format!("ack-{}", std::process::id());

    assert!(
        run(&[
            "channel",
            "create",
            &ch,
            "--name",
            "Ack",
            "--project",
            "demo",
            "--as",
            "Closer",
        ])
        .status
        .success()
    );

    let posted = run(&[
        "channel",
        "post",
        &ch,
        "--body",
        "load-bearing post",
        "--project",
        "demo",
        "--as",
        "Closer",
        "--wait-ack",
        "5",
    ]);
    assert!(
        posted.status.success(),
        "post --wait-ack must succeed: {}",
        String::from_utf8_lossy(&posted.stderr)
    );
    let v = out_json(&posted);
    assert_eq!(v["verified"], serde_json::json!(true), "verified: {v}");
    let event_id = v["event_id"].as_str().expect("event id").to_string();

    let read_back = out_json(&run(&[
        "channel",
        "read",
        &ch,
        "--project",
        "demo",
        "--as",
        "Closer",
    ]));
    let seen = read_back
        .as_array()
        .expect("read array")
        .iter()
        .any(|e| e["event_id"].as_str() == Some(event_id.as_str()));
    assert!(seen, "the verified event id reads back: {read_back}");
}
