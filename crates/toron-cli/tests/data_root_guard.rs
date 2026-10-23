// governed-by: ADR-0007
//! The real `toron` binary, built in a cargo target dir and spawned by a test,
//! never resolves an operator's data root: an inherited `HOME` and
//! `XDG_DATA_HOME` that point outside scratch space send it to the spawning
//! test's sandbox instead.
//!
//! This is the 2026-10-24 store-skew incident, replayed on a fake operator.
//! The operator's shell exports `XDG_DATA_HOME=$HOME/.local/share`. A test
//! spawned `toron guard check` with that environment and without a data-dir
//! pin, the child opened the live index and migrated it, and every installed
//! binary then refused the store.
//!
//! The fake operator's home is `/nonexistent-toron-guard-<pid>`. It does not
//! exist and cannot be created, so the replay can never reach a real store: a
//! regressed guard fails here on `create_dir_all`, nowhere else. The
//! environment is cleared (no `TORON_MCP_URL`, no daemon, no port), and every
//! store this test does create is under the temp root and removed afterwards.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn fake_operator_home() -> PathBuf {
    PathBuf::from(format!("/nonexistent-toron-guard-{}", std::process::id()))
}

fn scratch(tag: &str) -> PathBuf {
    static SEQ: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!("toron-guard-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// The sandbox the spawned dev build must land in: this test's own.
fn this_tests_sandbox() -> PathBuf {
    std::env::temp_dir().join(format!("toron-test-root-{}", std::process::id()))
}

/// `toron receipt list` opens the default mailbox: it resolves the data root,
/// creates its layout, opens `index.sqlite` and migrates it. That is the
/// whole path the incident took, with nothing written beyond the schema.
fn receipt_list(home: &Path, envs: &[(&str, &Path)]) -> Output {
    let config = scratch("config");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_toron"));
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("XDG_DATA_HOME", home.join(".local").join("share"))
        // The operator key is minted on first open; keep it in scratch.
        .env("XDG_CONFIG_HOME", &config)
        .env("XDG_STATE_HOME", config.join("state"));
    // The child must agree with this process on the temp root.
    if let Some(tmp) = std::env::var_os("TMPDIR") {
        cmd.env("TMPDIR", tmp);
    }
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd
        .args(["receipt", "list", "--json"])
        .output()
        .expect("run toron receipt list");
    let _ = std::fs::remove_dir_all(&config);
    out
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn a_spawned_dev_build_with_the_operators_xdg_lands_in_the_tests_sandbox() {
    let home = fake_operator_home();
    let sandbox = this_tests_sandbox();
    let _ = std::fs::remove_dir_all(&sandbox);

    let out = receipt_list(&home, &[]);
    assert!(
        out.status.success(),
        "the dev build must open its sandbox, not the operator's root: {}",
        stderr(&out)
    );
    assert!(
        sandbox.join("index.sqlite").is_file(),
        "the store must be in the spawning test's sandbox {}",
        sandbox.display()
    );
    assert!(
        !home.exists(),
        "nothing may be created under the operator's home"
    );
    assert!(
        stderr(&out).contains("dev build") && stderr(&out).contains(&sandbox.display().to_string()),
        "the redirect must be said out loud, naming the sandbox: {}",
        stderr(&out)
    );
    let _ = std::fs::remove_dir_all(&sandbox);
}

#[test]
fn an_explicit_data_dir_pin_still_wins_for_a_dev_build() {
    let home = fake_operator_home();
    let pinned = scratch("pinned");
    let out = receipt_list(&home, &[("TORON_DATA_DIR", &pinned)]);
    assert!(out.status.success(), "pinned open: {}", stderr(&out));
    assert!(pinned.join("index.sqlite").is_file(), "the pin is the root");
    assert!(
        !stderr(&out).contains("dev build"),
        "an explicit pin is not a redirect: {}",
        stderr(&out)
    );
    assert!(!home.exists());
    let _ = std::fs::remove_dir_all(&pinned);
}

#[test]
fn a_scratch_fixture_home_is_still_honoured_by_a_dev_build() {
    let fixture = scratch("fixture-home");
    let out = receipt_list(&fixture, &[]);
    assert!(out.status.success(), "fixture open: {}", stderr(&out));
    assert!(
        fixture
            .join(".local")
            .join("share")
            .join("toron")
            .join("index.sqlite")
            .is_file(),
        "a fixture XDG_DATA_HOME under the temp root is the root"
    );
    let _ = std::fs::remove_dir_all(&fixture);
}
