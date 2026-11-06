// governed-by: ADR-0035
//! wave8-b acceptance: one endpoint resolver — env > config > healthy
//! default > embedded. Shelled-out `toron` with per-child env (never the
//! parent's): a dead loopback URL for the failure arms (refused instantly,
//! no daemon), a scrubbed HOME for the config arm, and the operator-shaped
//! `reserve list` for the no-refusal arm (exit 0 with a live daemon or an
//! embedded store — both are success; the refusal was the bug).
//!
//! The arms a shell cannot force deterministically (dead default probe,
//! matching-root discovery) live as `resolve_with`/`decide_source` unit
//! tests in `mailbox_source.rs` — same seam, no live :18080 anywhere.
//! Matching-root discovery is additionally covered by the live operator
//! probe (same store either way, so exit 0 is the whole contract).

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
    let home = std::env::temp_dir().join(format!("toron-cli-ddisc-cfg-{}", std::process::id()));
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

/// wave13-b: a temp store root never discovers the live daemon, even
/// with a healthy default probe. The proof is on disk: the embedded
/// mailbox opens `{tmp}/index.sqlite`; the daemon path never touches the
/// temp dir. Deterministic with or without a live daemon.
#[test]
fn temp_store_root_never_discovers_the_live_daemon() {
    let dir = std::env::temp_dir().join(format!("toron-cli-ddisc-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("sandbox data dir");
    let mut cmd = toron();
    cmd.env("TORON_MCP_DATA_DIR", &dir).args([
        "agent",
        "bootstrap",
        "--project",
        "demo",
        "--as",
        "IsoRidge",
        "--json",
    ]);
    let out = run(cmd);
    assert!(
        out.status.success(),
        "bootstrap on the temp store must succeed, got: {}",
        stderr(&out)
    );
    assert!(
        dir.join("index.sqlite").is_file(),
        "the temp store was opened embedded (daemon discovery would leave no index.sqlite)"
    );
    assert!(
        stderr(&out).contains("never cross-store"),
        "the isolation note names the rule, got: {}",
        stderr(&out)
    );
    // And the short pin name is honored too: same expectation through
    // TORON_DATA_DIR (the harness's own isolation variable).
    let dir2 = std::env::temp_dir().join(format!("toron-cli-ddisc2-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir2);
    std::fs::create_dir_all(&dir2).expect("sandbox data dir 2");
    let mut cmd = toron();
    cmd.env("TORON_DATA_DIR", &dir2).args([
        "agent",
        "bootstrap",
        "--project",
        "demo",
        "--as",
        "IsoOtter",
        "--json",
    ]);
    let out = run(cmd);
    assert!(
        out.status.success(),
        "bootstrap on the short-pinned store must succeed, got: {}",
        stderr(&out)
    );
    assert!(
        dir2.join("index.sqlite").is_file(),
        "the short-pinned store was opened embedded"
    );
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

/// Explicit env still wins over an isolated store: the operator asked for
/// that daemon, so the dead URL fails typed (connect attempted), never
/// silently embedded.
#[test]
fn explicit_env_wins_over_an_isolated_store() {
    let dir = std::env::temp_dir().join(format!("toron-cli-ddisc3-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("sandbox data dir 3");
    let mut cmd = toron();
    cmd.env("TORON_MCP_DATA_DIR", &dir)
        .env("TORON_MCP_URL", DEAD_URL)
        .args(["reserve", "list", "--project", "flywheel"]);
    let out = run(cmd);
    assert!(
        !out.status.success(),
        "dead env daemon fails even when isolated"
    );
    let err = stderr(&out);
    assert!(
        err.contains("127.0.0.1:9") && err.contains("toron serve --mcp-http"),
        "typed refusal names the env endpoint + remedy, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&dir);
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
