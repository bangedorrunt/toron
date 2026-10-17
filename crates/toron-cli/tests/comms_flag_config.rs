// governed-by: ADR-0041
//! `[comms] inbox_log` reaches the verbs (bd-652a), through the real binary.
//!
//! Hermetic by construction: the environment is cleared, so `TORON_MCP_URL`
//! (the daemon on :18080) is NOT set and cannot be used; HOME, XDG and the
//! data dir all point at a throwaway directory. Every case uses an unknown
//! project, so nothing is ever written even when the gate is open: the verb
//! gets past the flag, then fails to resolve `--as`.

use std::path::Path;
use std::process::{Command, Output};

fn run(dir: &Path, config: Option<&str>, env_flag: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_toron"));
    cmd.env_clear()
        .env("HOME", dir)
        .env("XDG_DATA_HOME", dir.join("xdg-data"))
        .env("XDG_STATE_HOME", dir.join("xdg-state"))
        .env("XDG_CONFIG_HOME", dir.join("xdg-config"))
        .env("TORON_DATA_DIR", dir.join("data"))
        .current_dir(dir);
    if let Some(flag) = env_flag {
        cmd.env("TORON_COMMS_INBOX", flag);
    }
    if let Some(body) = config {
        let path = dir.join("cfg.toml");
        std::fs::write(&path, body).unwrap();
        cmd.arg("--config").arg(path);
    }
    cmd.args(["next", "--project", "nosuchproject", "--as", "nobody"]);
    assert!(
        cmd.get_envs().all(|(k, _)| k != "TORON_MCP_URL"),
        "the daemon URL must not reach the child"
    );
    cmd.output().expect("run toron")
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("toron-comms-flag-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn refused_as_off(o: &Output) -> bool {
    o.status.code() == Some(2) && String::from_utf8_lossy(&o.stderr).contains("comms inbox is off")
}

/// REPRO: the config key alone opens the gate. Before the key existed the file
/// was rejected as invalid and the verb stayed off.
#[test]
fn inbox_log_true_in_config_opens_the_gate() {
    let d = scratch("on");
    let o = run(&d, Some("[comms]\ninbox_log = true\n"), None);
    assert!(
        !refused_as_off(&o),
        "gate still closed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
}

/// REPRO: the env var still overrides a true config key, off.
#[test]
fn env_off_overrides_inbox_log_true() {
    let d = scratch("envoff");
    let o = run(&d, Some("[comms]\ninbox_log = true\n"), Some("0"));
    assert!(refused_as_off(&o), "{}", String::from_utf8_lossy(&o.stderr));
}

// --- negative controls ------------------------------------------------------

/// CONTROL: no config and no env var opens the gate. The off switches are
/// `TORON_COMMS_INBOX=0` and `inbox_log = false`.
#[test]
fn control_nothing_set_opens_the_gate() {
    let d = scratch("none");
    let o = run(&d, None, None);
    assert!(
        !refused_as_off(&o),
        "default must be on: {}",
        String::from_utf8_lossy(&o.stderr)
    );
}

/// CONTROL: `inbox_log = false` stays refused.
#[test]
fn control_inbox_log_false_is_refused() {
    let d = scratch("false");
    let o = run(&d, Some("[comms]\ninbox_log = false\n"), None);
    assert!(refused_as_off(&o));
}

/// CONTROL: the env var alone still opens the gate.
#[test]
fn control_env_on_alone_opens_the_gate() {
    let d = scratch("envon");
    let o = run(&d, None, Some("1"));
    assert!(
        !refused_as_off(&o),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}
