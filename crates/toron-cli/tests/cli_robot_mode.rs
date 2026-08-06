// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-led.8.4 acceptance: robot-mode global flags + bench.
//!
//! Shells out to the built `toron` binary. `--robot-*` flags must emit valid
//! JSON and short-circuit without a subcommand; `bench <case>` must emit a
//! percentile report.

use std::process::{Command, Output};

use serde_json::Value;

fn toron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_toron"))
}

fn run(args: &[&str]) -> Output {
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
fn bench_all_cases_exit_zero() {
    for case in ["startup", "inbox", "send", "search", "threads"] {
        let out = run(&["bench", case, "--json"]);
        assert!(out.status.success(), "bench {case} exits 0");
        let v = parse(&out.stdout);
        assert_eq!(v["scenarios"][0]["name"], case, "case name matches");
    }
}
