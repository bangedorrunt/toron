// governed-by: ADR-0003

//! wave9-c: `doctor --check` judges the LIVE daemon, not the local target.
//!
//! The stale/current verdicts live as pure unit tests on
//! `toron_core::serve_build::daemon_freshness_verdict` (no clock, no
//! filesystem, no daemon). This file covers the integration edge that
//! shelling out CAN fake honestly: no boot record in the sandbox keeps the
//! existing not-running result (never a false failure), and the two legs
//! stay separate (`local_binary` vs `daemon_freshness`). A live stale daemon
//! cannot be faked without holding the daemon port, so that path is proven
//! by the unit tests, not here.

use std::process::Command;

fn toron() -> Command {
    toron_core::testkit::toron_command_clean(env!("CARGO_BIN_EXE_toron"))
}

fn doctor_json(envs: &[(&str, &str)]) -> serde_json::Value {
    let mut cmd = toron();
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd
        .args(["doctor", "--json"])
        .output()
        .expect("run toron doctor");
    assert!(
        out.status.success(),
        "plain doctor must exit 0, stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("doctor stdout must be JSON")
}

fn check<'a>(value: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    value["checks"]
        .as_array()
        .expect("checks array")
        .iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("doctor must carry a {name} check"))
}

#[test]
fn no_boot_record_keeps_the_not_running_result() {
    // Sandboxed data root: no serve.build.json can exist there.
    let value = doctor_json(&[]);
    let leg = check(&value, "daemon_freshness");
    assert_eq!(leg["ok"], true, "no daemon must not fail");
    let detail = leg["detail"].as_str().expect("detail string");
    assert!(
        detail.contains("no boot record"),
        "detail must name the absence, got: {detail}"
    );
}

#[test]
fn local_binary_leg_stays_separate_from_the_daemon_leg() {
    let value = doctor_json(&[]);
    let local = check(&value, "local_binary");
    let daemon = check(&value, "daemon_freshness");
    assert_ne!(
        local["detail"], daemon["detail"],
        "the two legs must report different subjects"
    );
    for leg in [local, daemon] {
        assert!(leg.get("name").is_some());
        assert!(leg.get("ok").is_some());
        assert!(leg.get("detail").is_some());
    }
}
