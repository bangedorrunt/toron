// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-led.8.1 acceptance: clap framework, exit codes, --json vs human.

use std::process::{Command, Output};

fn toron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_toron"))
}

fn run(args: &[&str]) -> Output {
    toron().args(args).output().expect("run toron")
}

#[test]
fn help_is_examples_first() {
    let out = run(&["--help"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    // clig.dev / ADR-0003: help leads with examples, not just a usage line.
    assert!(
        text.contains("Examples") || text.contains("example"),
        "help must surface examples, got: {text}"
    );
    assert!(text.contains("serve"));
    assert!(text.contains("status"));
}

#[test]
fn usage_error_exits_2() {
    // Unknown subcommand -> clap usage error -> exit 2.
    let out = run(&["definitely-not-a-command"]);
    assert_eq!(out.status.code(), Some(2), "usage errors must exit 2");
}

#[test]
fn missing_required_arg_exits_2() {
    // `toron serve` without --mcp-stdio/--mcp-http is a runtime error (1),
    // but a missing subcommand entirely is a usage error (2).
    let out = run(&[]);
    assert_eq!(out.status.code(), Some(2), "no command must exit 2");
}

#[test]
fn status_json_parses() {
    let out = run(&["status", "--json"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("status --json must emit valid JSON");
    assert!(value.is_object());
    // Federated relays must default to empty (opt-in).
    assert_eq!(value["relays"], serde_json::json!([]));
}

#[test]
fn status_human_is_not_json() {
    let out = run(&["status"]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    // Human form is a labeled table, not bare JSON.
    assert!(text.contains("toron status") || text.contains("data_dir"));
    let first_line = text.lines().next().unwrap_or("");
    assert!(
        !first_line.starts_with('{'),
        "human status must not start with a JSON object"
    );
}

#[test]
fn env_config_overrides_project_file() {
    // TORON_BACKEND=herdr must win over a project .toron.toml saying tmux.
    let mut cmd = toron();
    cmd.current_dir("tests")
        .env("TORON_BACKEND", "herdr")
        .args(["status", "--json"]);
    let out = cmd.output().expect("run toron with env");
    assert!(out.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("status --json must parse");
    assert_eq!(value["backend"], serde_json::json!("herdr"));
}

#[test]
fn config_show_emits_effective_defaults() {
    let out = run(&["config", "--json"]);
    assert!(out.status.success());
    let value: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("config --json must parse");
    assert_eq!(
        value["relays"],
        serde_json::json!([]),
        "zero egress by default"
    );
    assert_eq!(value["envelope_ttl_days"], serde_json::json!(90));
    assert_eq!(value["backend"], serde_json::json!("herdr"));
    assert_eq!(value["federation_auth"], serde_json::json!(false));
    assert_eq!(value["proof_scheme"], serde_json::json!("schnorr"));
}
