// governed-by: ADR-0035
//! wave8-b acceptance: one endpoint resolver — env > config > healthy
//! default > embedded. Shelled-out `toron` with per-child env (never the
//! parent's): a dead loopback URL for the failure arms (refused instantly,
//! no daemon), a scrubbed HOME for the config arm, and the operator-shaped
//! `reserve list` for the no-refusal arm (exit 0 with a live daemon or an
//! embedded store — both are success; the refusal was the bug).
//!
//! The arms a shell cannot force deterministically (dead default probe,
//! pin-beats-live) live as `resolve_with` unit tests in `mailbox_source.rs`
//! — same seam, no live :18080 anywhere.

use std::process::{Command, Output};

const DEAD_URL: &str = "http://127.0.0.1:9/mcp";

fn toron() -> Command {
    let mut c = toron_core::testkit::toron_command_clean(env!("CARGO_BIN_EXE_toron"));
    c.env_remove("TORON_MCP_URL")
        .env_remove("TORON_MCP_DATA_DIR")
        .env("HOME", std::env::temp_dir().join("toron-cli-no-home"));
    c
}

fn run(mut cmd: Command) -> Output {
    cmd.output().expect("spawn toron under test")
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn env_url_wins_and_a_dead_one_fails_typed() {
    let mut cmd = toron();
    cmd.env("TORON_MCP_URL", DEAD_URL)
        .args(["reserve", "list", "--project", "flywheel"]);
    let out = run(cmd);
    assert!(!out.status.success(), "a dead env daemon must fail loud");
    let err = stderr(&out);
    assert!(
        err.contains("127.0.0.1:9") && err.contains("toron serve --mcp-http"),
        "typed refusal names the endpoint + remedy, got: {err}"
    );
}

#[test]
fn config_url_wins_and_a_dead_one_fails_typed() {
    let home = std::env::temp_dir().join(format!("toron-cli-ddisc-{}", std::process::id()));
    let cfg_dir = home.join(".config").join("flywheel");
    std::fs::create_dir_all(&cfg_dir).expect("sandbox config dir");
    std::fs::write(
        cfg_dir.join("config.toml"),
        "[mail.toron]\nurl = \"http://127.0.0.1:9/mcp\"\n",
    )
    .expect("sandbox flywheel config");
    let mut cmd = toron();
    cmd.env("HOME", &home)
        .args(["reserve", "list", "--project", "flywheel"]);
    let out = run(cmd);
    assert!(!out.status.success(), "a dead config daemon must fail loud");
    let err = stderr(&out);
    assert!(
        err.contains("127.0.0.1:9") && err.contains("toron serve --mcp-http"),
        "typed refusal names the config endpoint + remedy, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&home);
}

#[test]
fn contradiction_warns_but_the_daemon_still_wins() {
    let data_dir = std::env::temp_dir().join(format!("toron-cli-ddisc-pin-{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).expect("sandbox data dir");
    let mut cmd = toron();
    cmd.env("TORON_MCP_URL", DEAD_URL)
        .env("TORON_MCP_DATA_DIR", &data_dir)
        .args(["reserve", "list", "--project", "flywheel"]);
    let out = run(cmd);
    assert!(!out.status.success(), "dead env daemon fails even pinned");
    let err = stderr(&out);
    assert!(
        err.contains("TORON_MCP_URL") && err.contains("TORON_MCP_DATA_DIR"),
        "the contradiction warning names both variables, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&data_dir);
}

#[test]
fn no_config_means_no_refusal() {
    // Operator-shaped probe: with a live daemon this discovers it; with a
    // quiet port it embeds. Both exit 0 — the refusal was the bug.
    let mut cmd = toron();
    cmd.args(["reserve", "list", "--project", "flywheel"]);
    let out = run(cmd);
    assert!(
        out.status.success(),
        "no configured URL must never refuse, got: {}",
        stderr(&out)
    );
    let _ = stdout(&out);
}
