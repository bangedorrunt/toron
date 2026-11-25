// governed-by: ADR-0003
// governed-by: ADR-0031
//! `toron tui` (`dashboard` / `ui`) acceptance tests: CLI aliases, TTY gate,
//! and non-TTY `--json` / `--check` AX door.

use std::process::Command;

fn toron() -> Command {
    let mut c = toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL");
    c.env("TORON_TUI_NO_FLYWHEEL", "1");
    c.env("TORON_TUI_NO_HTTP_PROBE", "1");
    c
}

#[test]
fn tui_on_pipe_exits_1_with_tty_error_without_hanging() {
    for subcmd in ["tui", "dashboard", "ui"] {
        let out = toron().arg(subcmd).output().expect("run toron tui");
        assert_eq!(
            out.status.code(),
            Some(1),
            "{subcmd} on a pipe must exit 1, stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("TTY"),
            "stderr must explain the TTY requirement, got: {stderr}"
        );
    }
}

#[test]
fn help_lists_tui_and_aliases_dashboard_ui() {
    let out = toron().arg("--help").output().expect("run toron --help");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("tui"));
    assert!(text.contains("dashboard"));
    assert!(text.contains("ui"));
}

#[test]
fn dashboard_json_and_check_work_on_non_tty_pipe() {
    let dir = std::env::temp_dir().join(format!("toron-cli-tui-ax-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let out = toron()
        .env("TORON_MCP_DATA_DIR", &dir)
        .args(["dashboard", "--project", "axproj", "--json", "--check"])
        .output()
        .expect("run toron dashboard --json --check");
    assert!(
        out.status.success(),
        "dashboard --json --check failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("stdout must be valid JSON");
    assert_eq!(v["schema"], "toron-board.v1");
    assert_eq!(v["project"], "axproj");
    assert!(v["diagnostics"].is_array());
    assert!(v["posts"].is_array());
    assert!(v["roster"].is_array());
}
