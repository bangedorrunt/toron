// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-31 — Presence list (ADR-0020 §6.2).
//!
//! A `toron presence publish --state working` → B `toron presence list`
//! includes A `working` within 1s. After broker expiry (short test TTL), A
//! disappears or is `unknown`.
//!
//! The presence broker TTL is shortened for the test via the standard
//! `TORON_PRESENCE_TTL_SECS` knob (ADR-0020 Spec 7: "use a short test TTL");
//! publish and list both run as separate `toron` CLI subprocesses against the
//! same data dir.
//!
//! RED contract: `toron presence` does not exist until the wave-1 CLI lands,
//! so this fails at the first command with exit 2.
//!
//! Un-gated (ADR-0020 §6.2).

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";

mod common;

fn toron_bin() -> PathBuf {
    common::toron_bin()
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    toron_core::testkit::toron_command(toron_bin())
        .args(args)
        .arg("--json")
        .env("TORON_MCP_DATA_DIR", data_dir)
        // An ambient TORON_MCP_URL outranks the data-dir pin
        // (`mailbox_source::decide_source` checks the daemon URL first),
        // so the CLI would hit a live daemon instead of this scenario's
        // store and reject the locally registered recipient (bd-5z0l).
        .env_remove("TORON_MCP_URL")
        .env("TORON_PRESENCE_TTL_SECS", "1")
        .output()
        .expect("spawn toron")
}

fn exit_code(out: &Output) -> i32 {
    out.status.code().unwrap_or(-1)
}

fn stdout_json(out: &Output) -> Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout is not JSON (exit {}): {} | stderr: {}",
            exit_code(out),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

async fn setup(data_dir: &Path, project: &str, names: &[&str]) {
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
}

fn find_state(list: &Value, name: &str) -> Option<String> {
    let rows = match list {
        Value::Array(rows) => rows,
        _ => return None,
    };
    rows.iter().find_map(|row| {
        if row.get("name").and_then(|n| n.as_str()) == Some(name) {
            row.get("state")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        } else {
            None
        }
    })
}

#[tokio::test]
async fn sc31_presence_list_sees_working_then_expires() {
    let dir = std::env::temp_dir().join(format!("toron-sc31-{}", uuid::Uuid::new_v4()));
    setup(&dir, PROJECT, &[A, B]).await;

    // A publishes working.
    let out = run(
        &dir,
        &[
            "presence",
            "publish",
            "--state",
            "working",
            "--project",
            PROJECT,
            "--as",
            A,
            "--status",
            "reviewing",
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "presence publish must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // B lists within 1s → A working.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let out = run(&dir, &["presence", "list", "--project", PROJECT]);
        assert_eq!(
            exit_code(&out),
            0,
            "presence list must exit 0: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let list = stdout_json(&out);
        if find_state(&list, A).as_deref() == Some("working") {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("presence list never showed A working: {list}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // After broker expiry (TTL=1s), A disappears or is unknown.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let out = run(&dir, &["presence", "list", "--project", PROJECT]);
    assert_eq!(
        exit_code(&out),
        0,
        "presence list (expired) must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let list = stdout_json(&out);
    match find_state(&list, A) {
        None => {} // disappeared
        Some(state) => {
            assert!(
                state == "unknown" || state != "working",
                "A must disappear or be unknown after expiry, got state {state}: {list}"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
