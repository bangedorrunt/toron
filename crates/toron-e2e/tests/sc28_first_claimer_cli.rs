// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-28 — First-claimer via CLI (ADR-0020 §6.2).
//!
//! Two `toron job claim` nearly together (threads). Exactly one `won: true`;
//! the other exits 1 with `won: false` (ADR-0020 Spec 2: "won: false is exit
//! 1 (second claimer). Do not pretend success.").
//!
//! Setup mirrors SC-27: A creates a private channel, B joins, A posts a
//! kind-9 @mention → the mention dispatcher stamps ONE kind-43001. Then two
//! concurrent claim subprocesses race on the shared bus; `first_claimer` on
//! the bus total order picks exactly one winner.
//!
//! RED contract: `toron channel|job` subcommands do not exist until the
//! wave-1 CLI lands, so this fails at the first command with exit 2.
//!
//! Un-gated (ADR-0020 §6.2).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc28";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";

fn toron_bin() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_toron") {
        return PathBuf::from(p);
    }
    let exe = std::env::current_exe().expect("current_exe");
    let deps = exe.parent().expect("deps dir");
    let debug = deps.parent().expect("debug dir");
    let bin = debug.join("toron");
    assert!(
        bin.exists(),
        "toron binary not found at {} — build toron-cli first (cargo build -p toron-cli)",
        bin.display()
    );
    bin
}

fn run(data_dir: &Path, args: &[&str]) -> Output {
    Command::new(toron_bin())
        .args(args)
        .arg("--json")
        .arg("--no-input")
        .env("TORON_MCP_DATA_DIR", data_dir)
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

async fn setup(data_dir: &Path, project: &str, names: &[&str]) -> Vec<(String, String)> {
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
    let mut out = Vec::new();
    for name in names {
        let reg = mailbox
            .register_agent(project, &AgentProfile::new(*name))
            .await
            .expect("register agent");
        out.push((reg.name, reg.npub));
    }
    out
}

async fn store_job_id(data_dir: &Path) -> String {
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    sqlx::query_scalar("SELECT event_id FROM bus_events WHERE kind = 43001 LIMIT 1")
        .fetch_one(&pool)
        .await
        .expect("43001 event id")
}

/// Drive one claim to completion (helper used from both threads).
fn claim(data_dir: &Path, job_id: &str, who: &str) -> Output {
    run(
        data_dir,
        &["job", "claim", job_id, "--project", PROJECT, "--as", who],
    )
}

#[tokio::test]
async fn sc28_first_claimer_cli_exactly_one_wins() {
    let dir = std::env::temp_dir().join(format!("toron-sc28-{}", uuid::Uuid::new_v4()));
    let agents = setup(&dir, PROJECT, &[A, B]).await;
    let b_npub = agents
        .iter()
        .find(|(n, _)| n == B)
        .map(|(_, npub)| npub.clone())
        .unwrap();
    let _a_npub = agents
        .iter()
        .find(|(n, _)| n == A)
        .map(|(_, npub)| npub.clone())
        .unwrap();

    // Channel + kind-9 mention → one 43001 (mirrors SC-27 setup).
    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc28",
            "--private",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel create: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(
        &dir,
        &["channel", "join", CHANNEL, "--project", PROJECT, "--as", B],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel join: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(
        &dir,
        &[
            "channel",
            "post",
            CHANNEL,
            "--body",
            "@flywheel-toron-coder2 take the repro",
            "--mention",
            B,
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    assert_eq!(
        exit_code(&out),
        0,
        "channel post: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Wait for the single 43001 to be stamped by the dispatcher.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let job_id = loop {
        let out = run(
            &dir,
            &[
                "job",
                "list",
                "--project",
                PROJECT,
                "--as",
                B,
                "--state",
                "open",
            ],
        );
        if exit_code(&out) == 0 {
            let v = stdout_json(&out);
            let rows: Vec<&Value> = match &v {
                Value::Array(rows) => rows.iter().collect(),
                Value::Object(map) => map
                    .values()
                    .find_map(|x| x.as_array())
                    .map(|rows| rows.iter().collect())
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            if !rows.is_empty() {
                break store_job_id(&dir).await;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("job never appeared for B");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };

    // Two claims nearly together (threads). Exactly one won:true.
    let dir_b = dir.clone();
    let job_b = job_id.clone();
    let t_b = std::thread::spawn(move || claim(&dir_b, &job_b, B));
    let dir_a = dir.clone();
    let job_a = job_id;
    let t_a = std::thread::spawn(move || claim(&dir_a, &job_a, A));
    let out_b = t_b.join().expect("thread B");
    let out_a = t_a.join().expect("thread A");

    let mut winners = 0;
    let mut losers = 0;
    for out in [&out_b, &out_a] {
        let v = stdout_json(out);
        match (exit_code(out), v["won"].as_bool()) {
            (0, Some(true)) => winners += 1,
            (1, Some(false)) => losers += 1,
            other => panic!("unexpected claim outcome {other:?}: {v}"),
        }
    }
    assert_eq!(winners, 1, "exactly one won:true (first-claimer)");
    assert_eq!(losers, 1, "exactly one exit 1 + won:false");
    let _ = b_npub;

    let _ = std::fs::remove_dir_all(&dir);
}
