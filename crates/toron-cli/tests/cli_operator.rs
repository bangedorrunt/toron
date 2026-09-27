// governed-by: ADR-0003
// (not forbid(unsafe_code): this test sets TORON_MCP_DATA_DIR via
// std::env::set_var, which is unsafe in edition 2024; the unsafe is confined
// to the test harness, never to toron-core/toron-mcp library code.)

//! bd-led.8.2 acceptance: operator commands (doctor/metrics/key/project/relay/backend).
//!
//! Shells out to the built `toron` binary. No network: herdr probes are local
//! socket connects, NIP-11 is explicitly unwired, and key ops use the env
//! passphrase default ("toron"). Metrics tests isolate their counter file via
//! `TORON_METRICS_FILE` so parallel tests never race a shared file.

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, PoisonError};

use serde_json::Value;

fn toron() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_toron"));
    // These tests pin their own mailbox with TORON_MCP_DATA_DIR. An ambient
    // TORON_MCP_URL outranks that pin — `mailbox_source::decide_source`
    // checks the daemon URL first and returns before reading the data dir —
    // so the shelled-out CLI would talk to a developer's live daemon and
    // never see the keys registered into the temp dir, failing as
    // "unknown agent name" / "has no local key". Drop it by default so the
    // suite is hermetic on any machine with the daemon running (bd-5z0l).
    c.env_remove("TORON_MCP_URL");
    c
}

/// Shared per-test-run data dir so the shelled-out `toron` subprocess and the
/// in-process `register_probe_agent` mailbox NEVER touch the durable
/// production root (`~/.local/share/toron` — 500+ agent keys there would
/// re-run the ~1s/key argon2 KDF on every mailbox build).
fn shared_data_dir() -> PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-operator-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        // SAFETY: test process; inherited by every shelled-out subprocess.
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

fn run(args: &[&str]) -> Output {
    let _ = shared_data_dir();
    toron().args(args).output().expect("run toron")
}

fn run_with_env(args: &[&str], envs: &[(&str, &str)]) -> Output {
    let _ = shared_data_dir();
    let mut cmd = toron();
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.args(args).output().expect("run toron with env")
}

fn parse(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).expect("stdout must be valid JSON")
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_dir(tag: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("toron-op-{}-{}-{tag}", std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// ---------------------------------------------------------------------------
// key
// ---------------------------------------------------------------------------

#[test]
fn key_generate_writes_0600_ncryptsec_without_nsec() {
    let dir = unique_dir("keygen");
    let key_path = dir.join("agent.ncryptsec");
    let out = run(&[
        "key",
        "generate",
        "--role",
        "agent",
        "--out",
        key_path.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        out.status.success(),
        "key generate must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    assert!(
        value.get("npub").and_then(Value::as_str).is_some(),
        "JSON must carry npub, got {value}"
    );
    assert!(
        value.get("path").is_some(),
        "JSON must carry path, got {value}"
    );
    assert!(key_path.is_file(), "key file must exist");

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let mode = std::fs::metadata(&key_path).unwrap().mode();
        assert_eq!(mode & 0o777, 0o600, "file mode must be 0600, got {mode:o}");
    }

    let content = std::fs::read_to_string(&key_path).unwrap();
    assert!(
        !content.contains("nsec1"),
        "key file must never contain the nsec"
    );
    assert!(
        content.starts_with("ncryptsec1"),
        "must be NIP-49 ncryptsec"
    );
}

#[test]
fn key_inspect_prints_npub_not_nsec() {
    let dir = unique_dir("keyinspect");
    let key_path = dir.join("agent.ncryptsec");
    let generated = run(&[
        "key",
        "generate",
        "--role",
        "agent",
        "--out",
        key_path.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        generated.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let npub = parse(&generated.stdout)["npub"]
        .as_str()
        .unwrap()
        .to_string();

    let out = run(&["key", "inspect", key_path.to_str().unwrap(), "--json"]);
    assert!(
        out.status.success(),
        "inspect must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    assert_eq!(
        value["npub"].as_str(),
        Some(npub.as_str()),
        "inspect npub must match the generated public key"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("nsec1"),
        "inspect must never print the nsec"
    );
}

#[test]
fn key_inspect_missing_file_exits_1() {
    let dir = unique_dir("keymissing");
    let missing = dir.join("does-not-exist.ncryptsec");
    let out = run(&["key", "inspect", missing.to_str().unwrap(), "--json"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "inspect of a missing file must exit 1, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn key_generate_default_path_ok_or_clean_error() {
    // No --out: default to operator_key or ~/.config/toron/operator_nsec.
    // Isolate HOME so the default path lands in temp, never the real home.
    let dir = unique_dir("keydefault");
    let out = run_with_env(&["key", "generate"], &[("HOME", dir.to_str().unwrap())]);
    let code = out.status.code();
    assert!(
        code == Some(0) || code == Some(1),
        "key generate without --out must exit 0 or 1, got {code:?}, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

#[test]
fn doctor_json_parses_with_checks() {
    let out = run(&["doctor", "--json"]);
    assert!(
        out.status.success(),
        "doctor must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    let checks = value["checks"]
        .as_array()
        .expect("doctor JSON must carry a checks array");
    assert!(!checks.is_empty(), "checks must not be empty");
    for check in checks {
        assert!(check.get("name").is_some(), "each check has a name");
        assert!(check.get("ok").is_some(), "each check has an ok flag");
        assert!(check.get("detail").is_some(), "each check has detail");
    }
}

#[test]
fn doctor_check_exits_0_or_3_with_parseable_json() {
    let out = run(&["doctor", "--check", "--json"]);
    let code = out.status.code();
    assert!(
        code == Some(0) || code == Some(3),
        "doctor --check must exit 0 (all pass) or 3 (any fail), got {code:?}, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    let checks = value["checks"].as_array().expect("checks array");
    assert!(!checks.is_empty());
    if code == Some(3) {
        assert!(
            checks.iter().any(|c| c["ok"] == serde_json::json!(false)),
            "exit 3 requires at least one failed check, got {value}"
        );
    }
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

#[test]
fn metrics_json_has_counters() {
    let out = run(&["metrics", "--json"]);
    assert!(
        out.status.success(),
        "metrics must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    assert!(
        value["total_calls"].is_number(),
        "total_calls is a number, got {value}"
    );
    assert!(
        value["errors"].is_number(),
        "errors is a number, got {value}"
    );
    assert!(
        value["tool_calls"].is_object(),
        "tool_calls is an object, got {value}"
    );
}

#[test]
fn metrics_increment_after_robot_call() {
    let dir = unique_dir("metrics");
    let mfile = dir.join("metrics.json");
    let env = [("TORON_METRICS_FILE", mfile.to_str().unwrap())];

    let before = parse(&run_with_env(&["metrics", "--json"], &env).stdout);
    let before_total = before["total_calls"].as_u64().unwrap();

    // A robot tool call: record the metrics-probe agent so inbox resolves.
    register_probe_agent();
    let tool = run_with_env(
        &[
            "mail",
            "inbox",
            "--as",
            "metrics-probe",
            "--project",
            "demo",
            "--json",
        ],
        &env,
    );

    let after = parse(&run_with_env(&["metrics", "--json"], &env).stdout);
    let after_total = after["total_calls"].as_u64().unwrap();
    assert!(
        after_total > before_total,
        "total_calls must increase after a tool call \
         (before={before_total}, after={after_total}, mail stderr: {})",
        String::from_utf8_lossy(&tool.stderr)
    );
}

/// Register `metrics-probe` in the shared demo mailbox (idempotent), so the
/// robot call in [`metrics_increment_after_robot_call`] succeeds cleanly.
fn register_probe_agent() {
    // Ensure the shared data dir env is set BEFORE demo_mailbox constructs
    // in-process (a durable-root mailbox would re-KDF 500+ agent keys).
    let _ = shared_data_dir();
    static REG: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _guard = REG
        .get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("metrics-probe"),
            )
            .await
            .expect("register metrics-probe");
    });
}

// ---------------------------------------------------------------------------
// project / relay / backend
// ---------------------------------------------------------------------------

#[test]
fn project_list_is_object_with_projects_array() {
    let out = run(&["project", "list", "--json"]);
    assert!(
        out.status.success(),
        "project list must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    assert!(
        value["projects"].is_array(),
        "project list must carry a projects array, got {value}"
    );
}

#[test]
fn relay_list_defaults_empty() {
    let out = run(&["relay", "list", "--json"]);
    assert!(out.status.success());
    let value = parse(&out.stdout);
    assert_eq!(
        value["relays"],
        serde_json::json!([]),
        "relays must default empty (zero egress)"
    );
}

#[test]
fn agent_bootstrap_registers_and_reserves() {
    let args = [
        "agent",
        "bootstrap",
        "--project",
        "bootstrap-probe",
        "--as",
        "QuietBadger",
        "--paths",
        "src/**",
        "--reason",
        "probe",
        "--json",
    ];
    let out = run(&args);
    assert!(
        out.status.success(),
        "bootstrap must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    assert_eq!(value["agent"], serde_json::json!("QuietBadger"));
    assert!(
        value["created"].is_boolean(),
        "bootstrap reports created flag, got {value}"
    );
    // Idempotent re-run: same pin upserts, reserves again.
    let again = run(&args);
    assert!(again.status.success());
    let second = parse(&again.stdout);
    assert_eq!(
        second["created"],
        serde_json::json!(false),
        "second bootstrap upserts, got {second}"
    );
}

#[test]
fn relay_add_returns_proposed_list() {
    let out = run(&["relay", "add", "wss://relay.example.com", "--json"]);
    assert!(out.status.success());
    let value = parse(&out.stdout);
    assert_eq!(
        value["relays"],
        serde_json::json!(["wss://relay.example.com"]),
        "relay add must propose the appended list, got {value}"
    );
}

#[test]
fn backend_list_is_json_array_with_herdr() {
    let out = run(&["backend", "list", "--json"]);
    assert!(
        out.status.success(),
        "backend list must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value = parse(&out.stdout);
    let arr = value.as_array().expect("backend list must be an array");
    assert!(
        arr.iter().any(|b| b["name"] == serde_json::json!("herdr")),
        "array must include herdr, got {value}"
    );
    assert!(
        arr.iter().any(|b| {
            b["name"] == serde_json::json!("tmux") && b["available"] == serde_json::json!(false)
        }),
        "tmux must be reported unavailable, got {value}"
    );
}

#[test]
fn backend_status_reports_effective() {
    let out = run(&["backend", "status", "--json"]);
    assert!(out.status.success());
    let value = parse(&out.stdout);
    assert_eq!(value["backend"], serde_json::json!("herdr"));
}
