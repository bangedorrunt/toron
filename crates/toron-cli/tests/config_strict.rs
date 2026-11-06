// governed-by: ADR-0003

//! wave7-c parse-or-refuse acceptance: every migrated `TORON_*` knob rejects
//! garbage with one actionable message (exit nonzero, accepted forms named),
//! and valid values behave as before.
//!
//! Shells out to the built `toron` binary through
//! `toron_core::testkit::toron_command_clean` (cleared env + sandbox pins),
//! so no test can touch the operator's live store.

use std::process::{Command, Output};

fn toron() -> Command {
    toron_core::testkit::toron_command_clean(env!("CARGO_BIN_EXE_toron"))
}

fn run_with_env(envs: &[(&str, &str)]) -> Output {
    let mut cmd = toron();
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.args(["doctor", "--json"]).output().expect("run toron")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

/// Garbage exits nonzero and names the accepted forms.
fn assert_refuses(knob: &str, garbage: &str, accepted: &str) {
    let out = run_with_env(&[(knob, garbage)]);
    assert!(
        !out.status.success(),
        "{knob}={garbage:?} must fail, stderr: {}",
        stderr(&out)
    );
    let err = stderr(&out);
    assert!(
        err.contains(knob) && err.contains(accepted),
        "{knob}={garbage:?} must name the knob and the accepted forms ({accepted}), stderr: {err}"
    );
}

/// A valid value behaves as before: the command succeeds and no refusal is
/// named.
fn assert_accepts(knob: &str, valid: &str) {
    let out = run_with_env(&[(knob, valid)]);
    assert!(
        out.status.success(),
        "{knob}={valid:?} must succeed, stderr: {}",
        stderr(&out)
    );
    assert!(
        !stderr(&out).contains("is not valid"),
        "{knob}={valid:?} must not refuse, stderr: {}",
        stderr(&out)
    );
}

#[test]
fn startup_knobs_refuse_garbage_and_accept_valid_values() {
    // (knob, garbage, accepted-forms substring, valid value)
    let table: &[(&str, &str, &str, &str)] = &[
        ("TORON_BACKEND", "bogus", "auto|rex|superlogical|hc", "hc"),
        (
            "TORON_ENVELOPE_TTL_DAYS",
            "lots",
            "a non-negative integer",
            "90",
        ),
        (
            "TORON_PROOF_GATE_ENABLED",
            "maybe",
            "1|true|on|0|false|off",
            "true",
        ),
        ("TORON_PROOF_SCHEME", "rsa", "schnorr|ed25519", "ed25519"),
        (
            "TORON_GUARD_TIMEOUT_MS",
            "soon",
            "a non-negative integer",
            "5000",
        ),
        (
            "TORON_COALESCE_WINDOW_MS",
            "wide",
            "a non-negative integer",
            "2000",
        ),
        (
            "TORON_COALESCE_SUBWINDOW_MS",
            "wide",
            "a non-negative integer",
            "500",
        ),
        (
            "TORON_TOOL_TIMEOUT_MS",
            "long",
            "a non-negative integer",
            "30000",
        ),
        (
            "TORON_TUI_TIMEOUT_MS",
            "slow",
            "a non-negative integer",
            "10000",
        ),
        (
            "TORON_PRESENCE_TTL_SECS",
            "eternal",
            "a non-negative integer",
            "60",
        ),
        (
            "TORON_WAKE_SOCK_HANDSHAKE_MS",
            "firm",
            "a non-negative integer",
            "5000",
        ),
        (
            "TORON_WAKE_SOCK_IDLE_MS",
            "lazy",
            "a non-negative integer",
            "60000",
        ),
        (
            "TORON_MCP_URL",
            "not-a-url",
            "an http:// or https:// URL",
            "http://127.0.0.1:9",
        ),
        ("TORON_COMMS_INBOX", "perhaps", "1|true|on|0|false|off", "0"),
        (
            "TORON_GUARD_STRICT",
            "perhaps",
            "1|true|on|0|false|off",
            "0",
        ),
    ];
    for (knob, garbage, accepted, valid) in table {
        assert_refuses(knob, garbage, accepted);
        assert_accepts(knob, valid);
    }
}

#[test]
fn backend_accepts_each_valid_choice() {
    for valid in ["auto", "rex", "superlogical", "hc", " HC "] {
        assert_accepts("TORON_BACKEND", valid);
    }
}

#[test]
fn dead_operator_nsec_name_has_no_effect() {
    // `TORON_OPERATOR_NSEC` is read by nothing (ADR-0004 rejected the
    // raw-secret-in-env form); setting it must not change behavior.
    let out = run_with_env(&[("TORON_OPERATOR_NSEC", "nsec1garbage")]);
    assert!(
        out.status.success(),
        "dead name must be ignored, stderr: {}",
        stderr(&out)
    );
    assert!(
        !stderr(&out).contains("OPERATOR_NSEC"),
        "dead name must not be named, stderr: {}",
        stderr(&out)
    );
}

#[test]
fn doctor_names_the_freshness_triple() {
    let out = run_with_env(&[]);
    assert!(out.status.success(), "stderr: {}", stderr(&out));
    let value: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("doctor stdout must be JSON");
    let freshness = value["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["name"] == "local_binary")
        .expect("doctor must carry a local_binary check");
    assert_eq!(freshness["ok"], true);
    let detail = freshness["detail"].as_str().expect("detail string");
    for part in ["head=", "binary=", "store="] {
        assert!(
            detail.contains(part),
            "triple must name {part}, detail: {detail}"
        );
    }
}

#[test]
fn doctor_names_the_remedy_for_a_store_newer_than_the_binary() {
    let dir = std::env::temp_dir().join(format!("toron-config-strict-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("index.sqlite");
    let known = toron_core::index::schema::resolved_max_version();
    let script = format!(
        "import sqlite3; c=sqlite3.connect({:?}); \
         c.execute('CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, success BOOLEAN)'); \
         c.execute('INSERT INTO _sqlx_migrations VALUES ({}, 1)'); c.commit()",
        db.to_string_lossy().into_owned(),
        known + 1000
    );
    let status = Command::new("python3")
        .args(["-c", &script])
        .status()
        .expect("python3 builds the stale store");
    assert!(status.success());

    let out = run_with_env(&[("TORON_DATA_DIR", dir.to_str().unwrap())]);
    let value: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("doctor stdout must be JSON");
    let freshness = value["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["name"] == "local_binary")
        .expect("local_binary check");
    assert_eq!(freshness["ok"], false);
    let detail = freshness["detail"].as_str().expect("detail string");
    for part in ["head=", "binary=", "store=", "remedy"] {
        assert!(
            detail.contains(part),
            "stale store must name {part}, detail: {detail}"
        );
    }
    assert!(
        detail.contains(&known.to_string()),
        "detail must name what this binary resolves, detail: {detail}"
    );
}
