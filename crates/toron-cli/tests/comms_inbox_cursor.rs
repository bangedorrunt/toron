// governed-by: ADR-0041
//! `toron inbox cursor` through the real binary (bd-yc6jf).
//!
//! Hermetic: the environment is cleared and HOME, XDG and the data dir are a
//! throwaway directory per test. A fresh reader's committed pull cursor is 0,
//! served with a stable JSON shape the doorbell ring guard can take as the
//! one notion of consumed.

use std::path::{Path, PathBuf};
use std::process::Output;

fn scratch(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "toron-inbox-cursor-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn toron(dir: &Path, args: &[&str]) -> Output {
    let mut cmd = toron_core::testkit::toron_command_clean(env!("CARGO_BIN_EXE_toron"));
    cmd.env("HOME", dir)
        .env("XDG_DATA_HOME", dir.join("xdg-data"))
        .env("XDG_STATE_HOME", dir.join("xdg-state"))
        .env("XDG_CONFIG_HOME", dir.join("xdg-config"))
        .env("TORON_DATA_DIR", dir.join("data"))
        .env("TORON_COMMS_INBOX", "1")
        .current_dir(dir)
        .args(args)
        .arg("--json");
    cmd.output().expect("run toron")
}

fn json(o: &Output) -> serde_json::Value {
    serde_json::from_slice(&o.stdout).unwrap_or_else(|_| {
        panic!(
            "not JSON (exit {:?}): {} | {}",
            o.status.code(),
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        )
    })
}

#[test]
fn a_fresh_reader_cursor_is_zero() {
    let dir = scratch("cold");
    let out = toron(
        &dir,
        &["inbox", "cursor", "--project", "demo", "--as", "BlueLake"],
    );
    assert!(
        out.status.success(),
        "cursor is read-only: {} | {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    assert_eq!(v["project"], "demo");
    assert_eq!(v["agent"], "BlueLake");
    assert_eq!(v["cursor_us"], 0, "cold start reads zero: {v}");
    std::fs::remove_dir_all(&dir).ok();
}
