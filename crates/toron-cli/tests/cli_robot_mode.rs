// governed-by: ADR-0003
// (not forbid(unsafe_code): this test sets TORON_MCP_DATA_DIR via
// std::env::set_var, which is unsafe in edition 2024; the unsafe is confined
// to the test harness, never to toron-core/toron-mcp library code.)

//! bd-led.8.4 acceptance: robot-mode global flags + bench.
//!
//! Shells out to the built `toron` binary. `--robot-*` flags must emit valid
//! JSON and short-circuit without a subcommand; `bench <case>` must emit a
//! percentile report.

use std::process::{Command, Output};
use std::sync::OnceLock;

use serde_json::Value;

fn toron() -> Command {
    toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"))
}

/// Shared per-test-run data dir so the shelled-out `toron` subprocess never
/// touches the durable production root (`~/.local/share/toron` — 500+ agent
/// keys there would re-run the ~1s/key argon2 KDF on every mailbox build).
fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-robot-mode-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
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

fn parse(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).expect("stdout must be valid JSON")
}

#[test]
fn robot_status_emits_valid_json() {
    let out = run(&["--robot-status"]);
    assert!(out.status.success(), "robot-status must exit 0");
    let v = parse(&out.stdout);
    assert_eq!(v["ok"], true, "status.ok present");
    assert!(v["tools"].is_number());
    assert!(v["calls"].is_number());
    assert!(v["errors"].is_number());
}

#[test]
fn robot_capabilities_emits_valid_json() {
    let out = run(&["--robot-capabilities"]);
    assert!(out.status.success());
    let v = parse(&out.stdout);
    let clusters = v["tool_clusters"].as_object().expect("tool_clusters");
    assert_eq!(clusters.len(), 9, "9 AM-Rust clusters");
    assert!(clusters["messaging"].is_array());
    assert!(v["json"].as_bool().unwrap(), "json capability advertised");
    // ADR-0020 Spec 3: the CLI Nostr plane nouns are shell verbs, not MCP
    // tools. They MUST be advertised under the `cli` cluster — and the MCP
    // tool cluster count stays 9 (39 tools frozen).
    let cli = v["cli"].as_object().expect("cli cluster");
    let flat: Vec<&str> = cli
        .values()
        .flat_map(|arr| {
            arr.as_array()
                .map(|a| a.iter().filter_map(|s| s.as_str()).collect::<Vec<_>>())
                .unwrap_or_default()
        })
        .collect();
    for noun in ["channel", "job", "react", "presence", "watch"] {
        assert!(
            flat.contains(&noun),
            "cli cluster must list {noun}: {cli:?}"
        );
    }
}

#[test]
fn robot_snapshot_emits_valid_json() {
    let out = run(&["--robot-snapshot"]);
    assert!(out.status.success());
    let v = parse(&out.stdout);
    assert!(v["status"].is_object());
    assert!(v["capabilities"].is_object());
    assert!(v["metrics"].is_object());
    assert!(v["runtime"]["envelope_ttl_days"].is_number());
}

#[test]
fn robot_flag_works_without_subcommand() {
    // --robot-* must short-circuit; a bare `toron --robot-status` (no
    // subcommand) must NOT hit the "requires a subcommand" error.
    let out = run(&["--robot-status"]);
    assert!(out.status.success(), "robot flag needs no subcommand");
    parse(&out.stdout);
}

#[test]
#[ignore = "bench probe: run explicitly with --ignored (20× mailbox builds; slow by design)"]
fn bench_startup_emits_report() {
    let out = run(&["bench", "startup", "--json"]);
    assert!(out.status.success(), "bench startup exits 0");
    let v = parse(&out.stdout);
    let cases = v["scenarios"].as_array().expect("scenarios");
    assert_eq!(cases.len(), 1);
    assert_eq!(cases[0]["name"], "startup");
    assert!(cases[0]["iterations"].as_u64().unwrap() >= 1);
    assert!(cases[0]["avg_us"].as_u64().unwrap() > 0);
    assert!(cases[0]["p50_us"].as_u64().unwrap() > 0);
    assert!(cases[0]["p95_us"].as_u64().unwrap() >= cases[0]["p50_us"].as_u64().unwrap());
}

#[test]
#[ignore = "bench probe: run explicitly with --ignored (5 cases × 20 mailbox ops)"]
fn bench_all_cases_exit_zero() {
    for case in ["startup", "inbox", "send", "search", "threads"] {
        let out = run(&["bench", case, "--json"]);
        assert!(out.status.success(), "bench {case} exits 0");
        let v = parse(&out.stdout);
        assert_eq!(v["scenarios"][0]["name"], case, "case name matches");
    }
}
