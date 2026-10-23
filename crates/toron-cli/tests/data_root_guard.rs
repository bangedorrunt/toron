// governed-by: ADR-0007
//! A `toron` spawned under the test harness never resolves an operator's data
//! root or config home, whatever `HOME` / `XDG_*` it inherits, under any build
//! tool. It knows it is under test from the explicit marker the harness sets
//! (`TORON_UNDER_TEST`), not from where the build put it.
//!
//! This is the 2026-10-24 store-skew incident, replayed on a fake operator.
//! The operator's shell exports `XDG_DATA_HOME=$HOME/.local/share`. A test
//! spawned `toron guard check` with that environment and no data-dir pin; the
//! child opened the live index and migrated it.
//!
//! The fake operator's home is `/nonexistent-toron-guard-<pid>`, and every
//! `XDG_*` points inside it. It does not exist and cannot be created, so the
//! replay never reaches a real store or key: a broken guard fails here on
//! `create_dir_all`. The environment is cleared apart from PATH and TMPDIR (no
//! `TORON_MCP_URL`, no daemon, no port). Everything the child writes lands in
//! this test's harness sandbox under the temp root, which is removed after.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard, PoisonError};

use toron_core::testkit::{harness_root, toron_command_clean};

/// The tests share one harness sandbox (it is per process), so they run one
/// at a time and each starts from an empty one.
fn serial() -> MutexGuard<'static, ()> {
    static GATE: Mutex<()> = Mutex::new(());
    let guard = GATE.lock().unwrap_or_else(PoisonError::into_inner);
    let _ = std::fs::remove_dir_all(harness_root());
    guard
}

fn fake_operator_home() -> PathBuf {
    PathBuf::from(format!("/nonexistent-toron-guard-{}", std::process::id()))
}

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("toron-guard-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A harness command whose every home-shaped variable is the fake operator's,
/// and whose explicit pins are removed: the incident's environment, with only
/// the marker left to protect it.
fn as_the_operator(home: &Path) -> Command {
    let mut cmd = toron_command_clean(env!("CARGO_BIN_EXE_toron"));
    cmd.env_remove("TORON_DATA_DIR")
        .env("HOME", home)
        .env("XDG_DATA_HOME", home.join(".local").join("share"))
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local").join("state"));
    cmd
}

/// `toron receipt list` opens the default mailbox: it resolves the data root,
/// creates its layout, loads or mints the operator key in the config home,
/// opens `index.sqlite` and migrates it. That is the incident's whole path.
fn receipt_list(mut cmd: Command) -> Output {
    cmd.args(["receipt", "list", "--json"])
        .output()
        .expect("run toron receipt list")
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn a_spawned_toron_with_the_operators_home_lands_in_the_harness_sandbox() {
    let _gate = serial();
    let home = fake_operator_home();
    let sandbox = harness_root();

    let out = receipt_list(as_the_operator(&home));
    assert!(
        out.status.success(),
        "under test the child must open its sandbox, not the operator's root: {}",
        stderr(&out)
    );
    assert!(
        sandbox.join("index.sqlite").is_file(),
        "the store must be in the harness sandbox {}",
        sandbox.display()
    );
    let key = sandbox
        .join("home")
        .join(".config")
        .join("toron")
        .join("operator_nsec");
    assert!(
        key.is_file(),
        "the operator key must be minted in the sandboxed config home {}",
        key.display()
    );
    assert!(
        !home.exists(),
        "nothing may be created under the operator's home"
    );
    assert!(
        stderr(&out).contains("TORON_UNDER_TEST")
            && stderr(&out).contains(&sandbox.display().to_string()),
        "the redirect must be said out loud, naming the sandbox: {}",
        stderr(&out)
    );
    let _ = std::fs::remove_dir_all(&sandbox);
}

#[test]
fn an_explicit_data_dir_pin_still_wins_under_test() {
    let _gate = serial();
    let home = fake_operator_home();
    let pinned = scratch("pinned");
    let mut cmd = as_the_operator(&home);
    cmd.env("TORON_DATA_DIR", &pinned);
    let out = receipt_list(cmd);
    assert!(out.status.success(), "pinned open: {}", stderr(&out));
    assert!(pinned.join("index.sqlite").is_file(), "the pin is the root");
    assert!(
        !harness_root().join("index.sqlite").exists(),
        "a pinned child does not fall back to the sandbox"
    );
    assert!(!home.exists());
    let _ = std::fs::remove_dir_all(&pinned);
    let _ = std::fs::remove_dir_all(harness_root());
}

#[test]
fn a_scratch_fixture_home_is_still_honoured_under_test() {
    let _gate = serial();
    let fixture = scratch("fixture-home");
    let out = receipt_list(as_the_operator(&fixture));
    assert!(out.status.success(), "fixture open: {}", stderr(&out));
    assert!(
        fixture
            .join(".local")
            .join("share")
            .join("toron")
            .join("index.sqlite")
            .is_file(),
        "a fixture XDG_DATA_HOME under the temp root is the data root"
    );
    assert!(
        fixture
            .join(".config")
            .join("toron")
            .join("operator_nsec")
            .is_file(),
        "a fixture XDG_CONFIG_HOME under the temp root is the config home"
    );
    let _ = std::fs::remove_dir_all(&fixture);
    let _ = std::fs::remove_dir_all(harness_root());
}

/// The harness itself: a plain `toron_command` carries the marker and an
/// explicit data dir, config home and state home inside its sandbox, and
/// drops any daemon URL.
#[test]
fn the_harness_pins_every_root_inside_its_sandbox() {
    let cmd = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    let envs: std::collections::HashMap<_, _> = cmd
        .get_envs()
        .map(|(k, v)| (k.to_string_lossy().into_owned(), v.map(PathBuf::from)))
        .collect();
    let root = harness_root();
    assert_eq!(envs["TORON_UNDER_TEST"].as_deref(), Some(root.as_path()));
    assert_eq!(envs["TORON_DATA_DIR"].as_deref(), Some(root.as_path()));
    for var in ["XDG_CONFIG_HOME", "XDG_STATE_HOME"] {
        let dir = envs[var].clone().expect(var);
        assert!(
            dir.starts_with(&root),
            "{var}={} is outside the sandbox",
            dir.display()
        );
    }
    assert_eq!(envs["TORON_MCP_URL"], None, "the daemon URL is removed");
}
