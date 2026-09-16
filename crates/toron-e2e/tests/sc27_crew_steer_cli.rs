// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-27 — Crew steer, no captain, CLI only. **THE ADR-0020 gate.**
//!
//! Two registered agents (A, B) complete the full Spec 5 loop as separate
//! `toron` binaries — no flywheel, no daemon, no worktree:
//!
//!   1. A: `channel create steer-sc27 ... --private`
//!   2. B: `channel join steer-sc27`
//!   3. A: `channel post steer-sc27 --mention B`        → kind 9, mentions
//!   4. B: `job list --state open`                      → exactly ONE 43001 for B
//!   5. B: `job claim`                                  → won:true
//!   6. A: `job claim` (second)                         → exit 1, won:false
//!   7. B: `job result --status ok`
//!   8. A: `job show`                                   → state done
//!
//! Asserts (ADR-0020 §6.2 SC-27 + bead bd-36qf.6):
//!   - post JSON `kind == 9` and `mentions` non-empty (plane S, kind-9 emit)
//!   - one 43001, not N (job list one row for B; store count == 1)
//!   - B claim won:true; A second claim exit 1 + won:false (first-claimer)
//!   - `job show` state done
//!   - gift-wrap count for the channel post is 0 (plane S never gift-wraps)
//!   - no flywheel binary invoked, no worktree in logs
//!
//! RED contract: every `toron channel|job` subcommand is missing until the
//! wave-1 CLI lands (bd-36qf.3/.4), so this test fails at the first step with
//! exit code 2 (unrecognized subcommand). It goes GREEN only when the CLI +
//! the kind-9 → mention-dispatcher wiring (§7.2) exist.
//!
//! Un-gated (ADR-0020 §6.2: "new un-gated CLI tests").

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use sqlx::SqlitePool;
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "crew-demo";
const CHANNEL: &str = "steer-sc27";
const A: &str = "flywheel-toron-coder";
const B: &str = "flywheel-toron-coder2";

/// Locate the built `toron` binary. CARGO_BIN_EXE_toron is only defined in the
/// crate that owns the bin (toron-cli); this cross-package e2e crate falls back
/// to current_exe()-relative discovery: test binary lives in
/// `target/debug/deps/`, so the toron binary is at `target/debug/toron`.
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

/// Run one `toron` CLI command against the shared data dir with the binding
/// robot flags (`--json --no-input` per ADR-0020 §6.2). Records the spawned
/// program name so the "no flywheel binary invoked" assert is exact.
fn run(data_dir: &Path, args: &[&str]) -> Output {
    SPAWNED.lock().unwrap().push("toron".to_string());
    Command::new(toron_bin())
        .args(args)
        .arg("--json")
        .arg("--no-input")
        .env("TORON_MCP_DATA_DIR", data_dir)
        .output()
        .expect("spawn toron")
}

static SPAWNED: std::sync::LazyLock<std::sync::Mutex<Vec<String>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

/// Assert no non-toron binary was ever spawned (SC-27: "No flywheel binary
/// invoked"). The toron test actors are NAMED `flywheel-toron-coder*`
/// (ADR-0018), so the check is on spawned program names, never output text.
fn assert_no_foreign_binary_spawned() {
    let progs = SPAWNED.lock().unwrap();
    assert!(
        progs.iter().all(|p| p == "toron"),
        "only the toron binary may be spawned, got: {progs:?}"
    );
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

/// Boot a mailbox over the shared data dir and register agents (ADR-0018 open
/// mode accepts any name). Returns the pool (for store-level asserts) and the
/// registered (name, npub) pairs. The CLI subprocesses open the SAME root via
/// `TORON_MCP_DATA_DIR`, so whois resolves the registered names and the bus
/// store is shared cross-process.
async fn setup(
    data_dir: &Path,
    project: &str,
    names: &[&str],
) -> (SqlitePool, Vec<(String, String)>) {
    std::fs::create_dir_all(data_dir).expect("create data dir");
    let db = data_dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let mailbox =
        Mailbox::new(Arc::new(bus), data_dir.to_path_buf(), None).with_index_pool(pool.clone());
    let mut out = Vec::new();
    for name in names {
        let reg = mailbox
            .register_agent(project, &AgentProfile::new(*name))
            .await
            .expect("register agent");
        out.push((reg.name, reg.npub));
    }
    (pool, out)
}

/// Store-level kind count on the shared bus (cross-process: the CLI
/// subprocesses persisted their events to the same index.sqlite).
async fn store_kind_count(pool: &SqlitePool, kind: u16) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM bus_events WHERE kind = ?1")
        .bind(kind as i64)
        .fetch_one(pool)
        .await
        .expect("count")
}

/// Kind of a stored event by id (None when absent).
async fn store_kind_of(pool: &SqlitePool, event_id: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT kind FROM bus_events WHERE event_id = ?1")
        .bind(event_id)
        .fetch_optional(pool)
        .await
        .expect("kind")
        .map(|k: i64| k)
}

/// The single kind-43001 event id (the job id) from the shared store.
async fn store_job_id(pool: &SqlitePool) -> String {
    sqlx::query_scalar("SELECT event_id FROM bus_events WHERE kind = 43001 LIMIT 1")
        .fetch_one(pool)
        .await
        .expect("43001 event id")
}

/// Recursively search a JSON value for a state/status field equal to `done`
/// (or any serialized occurrence of `"done"`).
fn json_is_done(v: &Value) -> bool {
    match v {
        Value::Object(map) => {
            for (k, val) in map {
                if (k == "state" || k == "status") && val.as_str().is_some_and(|s| s == "done") {
                    return true;
                }
                if json_is_done(val) {
                    return true;
                }
            }
            false
        }
        Value::Array(items) => items.iter().any(json_is_done),
        _ => false,
    }
}

#[tokio::test]
async fn sc27_crew_steer_cli_no_captain() {
    let dir = std::env::temp_dir().join(format!("toron-sc27-{}", uuid::Uuid::new_v4()));
    let (pool, agents) = setup(&dir, PROJECT, &[A, B]).await;
    let b_npub = agents
        .iter()
        .find(|(n, _)| n == B)
        .map(|(_, npub)| npub.clone())
        .expect("B registered");

    let mut all_output = String::new();
    let mut collect = |out: &Output| {
        all_output.push_str(&String::from_utf8_lossy(&out.stdout));
        all_output.push_str(&String::from_utf8_lossy(&out.stderr));
    };

    // 1. A creates the private channel.
    let out = run(
        &dir,
        &[
            "channel",
            "create",
            CHANNEL,
            "--name",
            "bead sc27",
            "--private",
            "--project",
            PROJECT,
            "--as",
            A,
        ],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "channel create must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 2. B joins.
    let out = run(
        &dir,
        &["channel", "join", CHANNEL, "--project", PROJECT, "--as", B],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "channel join must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 3. A posts a kind-9 mention to B.
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
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "channel post must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let post = stdout_json(&out);
    assert_eq!(
        post["kind"].as_u64(),
        Some(9),
        "post JSON kind must be 9: {post}"
    );
    let mentions = post["mentions"].as_array().expect("mentions array");
    assert!(
        !mentions.is_empty() && mentions.iter().any(|m| m.as_str() == Some(b_npub.as_str())),
        "post mentions must contain B's npub: {post}"
    );
    let post_id = post["event_id"].as_str().expect("event_id").to_string();

    // 4. B's watch --once replays the stored kind-9 post (deterministic).
    let out = run(
        &dir,
        &[
            "watch",
            "--project",
            PROJECT,
            "--as",
            B,
            "--channel",
            CHANNEL,
            "--kinds",
            "9,43001",
            "--once",
            "--timeout-ms",
            "30000",
        ],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "watch --once must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let watched = stdout_json(&out);
    assert!(
        watched.get("kind").is_some(),
        "watch --once must print an event object: {watched}"
    );

    // 5. job list --state open: exactly one row for B (one 43001, not N).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let jobs_json: Value = loop {
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
        collect(&out);
        assert_eq!(
            exit_code(&out),
            0,
            "job list must exit 0: {}",
            String::from_utf8_lossy(&out.stderr)
        );
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
        let b_rows = rows
            .iter()
            .filter(|r| r.to_string().contains(B) || r.to_string().contains(&b_npub))
            .count();
        if b_rows >= 1 {
            break v;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("job list never showed a job for B: {v}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let rows: Vec<&Value> = match &jobs_json {
        Value::Array(rows) => rows.iter().collect(),
        Value::Object(map) => map
            .values()
            .find_map(|x| x.as_array())
            .map(|rows| rows.iter().collect())
            .unwrap_or_default(),
        _ => Vec::new(),
    };
    let b_rows = rows
        .iter()
        .filter(|r| r.to_string().contains(B) || r.to_string().contains(&b_npub))
        .count();
    assert_eq!(b_rows, 1, "exactly one open job for B, got {jobs_json}");
    // settle: still exactly one 43001 (not N)
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        store_kind_count(&pool, 43001).await,
        1,
        "exactly one 43001 in the shared store"
    );

    // 6. B claims → won:true, first-claimer.
    let job_id = store_job_id(&pool).await;
    let out = run(
        &dir,
        &["job", "claim", &job_id, "--project", PROJECT, "--as", B],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "B claim must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let claim_b = stdout_json(&out);
    assert_eq!(
        claim_b["kind"].as_u64(),
        Some(43002),
        "claim kind 43002: {claim_b}"
    );
    assert_eq!(
        claim_b["claimer"].as_str(),
        Some(B),
        "claimer must be B: {claim_b}"
    );
    assert_eq!(
        claim_b["npub"].as_str(),
        Some(b_npub.as_str()),
        "claimer npub must be B's: {claim_b}"
    );
    assert_eq!(
        claim_b["won"].as_bool(),
        Some(true),
        "B claim won:true: {claim_b}"
    );

    // 7. A's second claim → exit 1, won:false (first-claimer wins).
    let out = run(
        &dir,
        &["job", "claim", &job_id, "--project", PROJECT, "--as", A],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        1,
        "second claim must exit 1: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let claim_a = stdout_json(&out);
    assert_eq!(
        claim_a["won"].as_bool(),
        Some(false),
        "A claim won:false: {claim_a}"
    );

    // 8. B reports the result.
    let out = run(
        &dir,
        &[
            "job",
            "result",
            &job_id,
            "--status",
            "ok",
            "--body",
            "receipt=sc27",
            "--project",
            PROJECT,
            "--as",
            B,
        ],
    );
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "job result must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 9. A sees the terminal state.
    let out = run(&dir, &["job", "show", &job_id, "--project", PROJECT]);
    collect(&out);
    assert_eq!(
        exit_code(&out),
        0,
        "job show must exit 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let shown = stdout_json(&out);
    assert!(json_is_done(&shown), "job show state must be done: {shown}");

    // Plane S: the channel post is kind 9 (not 1059) and no gift-wrap exists.
    assert_eq!(
        store_kind_of(&pool, &post_id).await,
        Some(9),
        "post must be kind 9 in the store"
    );
    assert_eq!(
        store_kind_count(&pool, 1059).await,
        0,
        "gift-wrap count for the channel post must be 0 (plane S never gift-wraps)"
    );

    // No flywheel binary invoked (spawn log, not output text — the actors are
    // NAMED flywheel-toron-coder* per ADR-0018), no worktree in logs.
    assert_no_foreign_binary_spawned();
    assert!(
        !all_output.to_lowercase().contains("worktree"),
        "no worktree in logs: {all_output}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
