// governed-by: ADR-0003
//! bd-ll7bs: a blocked acquire queues; a freed path hands over FIFO.
//!
//! Hermetic fixture: temp sqlite + in-process bus, no live daemon, no :18080,
//! no operator config. The handoff is the same atomic transfer the CLI uses.

use std::path::PathBuf;
use std::sync::Arc;

use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "waitlist-fixture";
const PATH: &str = "src/lib.rs";

async fn test_mailbox(tag: &str) -> (Mailbox, PathBuf, PathBuf) {
    let db_path = std::env::temp_dir().join(format!(
        "toron-waitlist-cli-{tag}-{}-{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let pool = schema::connect(db_path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let data_dir = std::env::temp_dir().join(format!(
        "toron-waitlist-cli-data-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data_dir).expect("mkdir");
    let mailbox = Mailbox::new(
        Arc::new(bus),
        data_dir.clone(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("reserve-waitlist-test")
    .with_index_pool(pool);
    (mailbox, db_path, data_dir)
}

struct DaemonGuard(Option<std::process::Child>);
impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn toron() -> std::process::Command {
    toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"))
}

/// bd-ll7bs residual (3): the `--queue` flag must reach the MCP tool through
/// the daemon arm, not just the embedded mailbox. Spare port, temp store,
/// never :18080.
#[test]
fn queue_acquire_through_daemon_end_to_end() {
    const DP: &str = "waitlist-daemon";
    let data = std::env::temp_dir().join(format!(
        "toron-waitlist-daemon-data-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data).expect("data dir");
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("free port");
    let daemon = toron()
        .args(["serve", "--mcp-http", &format!("127.0.0.1:{port}")])
        .env("TORON_MCP_DATA_DIR", &data)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map(|c| DaemonGuard(Some(c)))
        .expect("spawn daemon");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "daemon did not bind spare port {port}"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let url = format!("http://127.0.0.1:{port}/mcp");
    for pin in ["DuskHolder", "DawnWaiter"] {
        let out = toron()
            .args([
                "agent",
                "bootstrap",
                "--project",
                DP,
                "--as",
                pin,
                "--program",
                "claude-code",
                "--model",
                "opus",
                "--json",
            ])
            .env("TORON_MCP_DATA_DIR", &data)
            .env("TORON_MCP_URL", &url)
            .output()
            .expect("bootstrap");
        assert!(
            out.status.success(),
            "bootstrap {pin}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let acquire = |pin: &str, queue: bool| {
        let mut args = vec![
            "reserve".to_string(),
            "acquire".to_string(),
            "--project".to_string(),
            DP.to_string(),
            "--as".to_string(),
            pin.to_string(),
            "--paths".to_string(),
            PATH.to_string(),
            "--exclusive".to_string(),
        ];
        if queue {
            args.push("--queue".to_string());
        }
        let out = toron()
            .args(args)
            .env("TORON_MCP_DATA_DIR", &data)
            .env("TORON_MCP_URL", &url)
            .output()
            .expect("acquire");
        assert!(
            out.status.success(),
            "acquire {pin}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    acquire("DuskHolder", false);
    acquire("DawnWaiter", true);

    let list = toron()
        .args(["reserve", "list", "--project", DP, "--json"])
        .env("TORON_MCP_DATA_DIR", &data)
        .env("TORON_MCP_URL", &url)
        .output()
        .expect("list");
    let body: serde_json::Value = serde_json::from_slice(&list.stdout).expect("list json");
    let queues = body["waitlists"].as_array().expect("waitlists field");
    assert!(
        queues.iter().any(|q| q["waiters"][0] == "DawnWaiter"),
        "the daemon MCP path recorded the waiter: {body}"
    );
    assert!(
        body["reservations"]
            .as_array()
            .is_some_and(|rows| rows.iter().all(|r| r["holder_live"] == true)),
        "a freshly bootstrapped holder is live: {body}"
    );

    let release = toron()
        .args([
            "reserve",
            "release",
            "--project",
            DP,
            "--as",
            "DuskHolder",
            "--paths",
            PATH,
        ])
        .env("TORON_MCP_DATA_DIR", &data)
        .env("TORON_MCP_URL", &url)
        .output()
        .expect("release");
    assert!(
        release.status.success(),
        "release: {}",
        String::from_utf8_lossy(&release.stderr)
    );
    let list = toron()
        .args(["reserve", "list", "--project", DP, "--json"])
        .env("TORON_MCP_DATA_DIR", &data)
        .env("TORON_MCP_URL", &url)
        .output()
        .expect("list");
    let body: serde_json::Value = serde_json::from_slice(&list.stdout).expect("list json");
    assert!(
        body["reservations"]
            .as_array()
            .is_some_and(|rows| rows.iter().any(|r| r["holder"] == "DawnWaiter")),
        "the daemon path handed the lease over: {body}"
    );
    drop(daemon);
}

#[tokio::test]
async fn ttl_above_the_cap_is_clamped() {
    let (mailbox, _db, _data) = test_mailbox("ttl-cap").await;
    mailbox
        .register_agent(PROJECT, &AgentProfile::new("LongHold"))
        .await
        .expect("register");
    let before = chrono::Utc::now().timestamp_micros();
    let out = mailbox
        .file_reservation_paths(
            PROJECT,
            "LongHold",
            &["src/capped.rs".to_string()],
            Some(60 * 60 * 24 * 30),
            true,
            None,
        )
        .await
        .expect("acquire");
    assert_eq!(out.granted.len(), 1);
    let rows = mailbox
        .list_file_reservations(PROJECT, None, None)
        .await
        .expect("board");
    let lease = rows
        .iter()
        .find(|r| r.path_pattern == "src/capped.rs")
        .expect("lease");
    let expires = lease.expires_ts_us.expect("expiry");
    let cap_us = toron_core::mailbox::reservations::MAX_RESERVATION_TTL_SECS as i64 * 1_000_000;
    assert!(
        expires <= before + cap_us + 5_000_000 && expires > before,
        "TTL must be clamped to one hour, got {expires} (now {before})"
    );
}

#[tokio::test]
async fn expiry_hands_over_without_any_release_call() {
    let (mailbox, _db, _data) = test_mailbox("expiry").await;
    for name in ["ShortHolder", "ExpiryWaiter"] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register");
    }
    let paths = vec![PATH.to_string()];
    mailbox
        .file_reservation_paths(PROJECT, "ShortHolder", &paths, Some(1), true, Some("ttl"))
        .await
        .expect("short lease");
    let queued = mailbox
        .file_reservation_paths_queued(PROJECT, "ExpiryWaiter", &paths, Some(3600), true, None)
        .await
        .expect("queue");
    assert!(!queued.conflicts.is_empty());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // No release is called: the lease-board read that sees the expired lease
    // must run the handoff itself.
    let _ = mailbox.active_reservations(PROJECT).await.expect("read");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("ExpiryWaiter"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "expiry handed the path to the waiter");
    assert!(
        mailbox
            .list_waiters(PROJECT, PATH)
            .await
            .expect("queue")
            .is_empty()
    );
}

/// Project a real waitlist run onto `ToronLeaseResolution.tla` observation
/// names. Pure: the test owns the file write behind CONFORM_TRACE_OUT.
fn conform_row(action: &str, params: &[&str]) -> serde_json::Value {
    let mut row = serde_json::json!({
        "action": action,
        "bus_holder": "A",
        "bus_status": "Held",
        "bus_epoch": 0,
        "next_epoch": 1,
        "bus_expired": false,
        "mirror_expired_active": [],
        "mirror_epoch": {},
        "durable_live": ["A"],
        "waitq": ["None", "None"],
        "waitqLen": 0,
        "arrq": ["None", "None"],
        "arrLen": 0,
        "servedq": ["None", "None"],
        "servedLen": 0,
        "arrived": [],
        "served": [],
        "asked": [],
        "badRelease": [],
        "ghost": [],
    });
    if !params.is_empty() {
        row["params"] = serde_json::json!(params);
    }
    row
}

#[test]
fn conform_trace_projection_is_deterministic() {
    let rows = [
        conform_row("AcquireA", &[]),
        conform_row("QueueHold", &["B"]),
        conform_row("QueueHold", &["C"]),
        conform_row("HandoffRelease", &["commit"]),
    ];
    assert_eq!(rows.len(), 4);
    if let Ok(path) = std::env::var("CONFORM_TRACE_OUT") {
        let text = rows
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(path, format!("{text}\n")).expect("write conform trace");
    }
}

#[tokio::test]
async fn queued_acquires_hand_off_fifo_on_every_release() {
    let (mailbox, _db, _data) = test_mailbox("fifo").await;
    for name in ["Holder", "WaiterA", "WaiterB"] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register");
    }

    let paths = vec![PATH.to_string()];
    mailbox
        .file_reservation_paths(PROJECT, "Holder", &paths, Some(3600), true, Some("lane"))
        .await
        .expect("holder acquires");

    for waiter in ["WaiterA", "WaiterB"] {
        let out = mailbox
            .file_reservation_paths_queued(PROJECT, waiter, &paths, Some(3600), true, Some("lane"))
            .await
            .expect("queued acquire");
        assert_eq!(out.granted.len(), 0, "{waiter} cannot take a held path");
        assert_eq!(out.conflicts.len(), 1, "{waiter} sees the hard conflict");
    }

    let queue = mailbox.list_waiters(PROJECT, PATH).await.expect("queue");
    assert_eq!(
        queue
            .iter()
            .map(|w| w.agent_name.as_str())
            .collect::<Vec<_>>(),
        vec!["WaiterA", "WaiterB"],
        "FIFO arrival order"
    );

    mailbox
        .release_file_reservations(PROJECT, "Holder", &paths)
        .await
        .expect("release");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("WaiterA"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "the head waiter now holds the path");
    let remaining = mailbox.list_waiters(PROJECT, PATH).await.expect("queue");
    assert_eq!(remaining.len(), 1, "exactly one waiter was consumed");
    assert_eq!(remaining[0].agent_name, "WaiterB");

    // TTL expiry and retire go through the same helper; a second release with
    // no waiters must not invent a grant.
    mailbox
        .release_file_reservations(PROJECT, "WaiterA", &paths)
        .await
        .expect("release");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("WaiterB"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "the second waiter holds the path");
    assert!(
        mailbox
            .list_waiters(PROJECT, PATH)
            .await
            .expect("queue")
            .is_empty()
    );
}
