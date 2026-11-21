// governed-by: ADR-0040
//! bd-ys3jp: a send attempted during a daemon outage is queued locally with
//! its corr and reported as `queued`, instead of vanishing with an error.

use std::path::PathBuf;
use std::process::Command;

#[test]
fn a_daemon_outage_queues_the_send_with_its_corr() {
    let root = std::env::temp_dir().join(format!("toron-outage-queue-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    for rel in [
        "home",
        "xdg/config",
        "xdg/data",
        "xdg/state",
        "xdg/run",
        "data",
    ] {
        std::fs::create_dir_all(root.join(rel)).unwrap();
    }

    let out = Command::new(env!("CARGO_BIN_EXE_toron"))
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("xdg/config"))
        .env("XDG_DATA_HOME", root.join("xdg/data"))
        .env("XDG_STATE_HOME", root.join("xdg/state"))
        .env("XDG_RUNTIME_DIR", root.join("xdg/run"))
        .env("TORON_DATA_DIR", root.join("data"))
        .env("TORON_MCP_URL", "http://127.0.0.1:18999/mcp")
        .args([
            "mail",
            "send",
            "--project",
            "p",
            "--as",
            "Alpha",
            "--to",
            "Beta",
            "--subject",
            "outage",
            "--body",
            "queued",
        ])
        .output()
        .expect("run toron mail send");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "queued send must exit 0: {stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        stdout.contains("\"delivery\":\"queued\""),
        "send must state queued: {stdout}"
    );
    assert!(
        stdout.contains("\"corr\":\"q-"),
        "send must mint a corr: {stdout}"
    );

    let queue: PathBuf = root.join("xdg/state/toron/queued-sends.jsonl");
    let rows = std::fs::read_to_string(&queue).unwrap_or_default();
    let first = rows.lines().next().unwrap_or_default();
    assert!(
        !first.is_empty(),
        "the queued send must be durable: {queue:?}"
    );
    let row: serde_json::Value = serde_json::from_str(first).unwrap();
    let corr = row.get("corr").and_then(|v| v.as_str()).unwrap_or_default();
    assert!(
        stdout.contains(corr),
        "the queue and the output must share corr: {stdout} vs {row}"
    );
    assert_eq!(row.get("subject").and_then(|v| v.as_str()), Some("outage"));
    std::fs::remove_dir_all(&root).ok();
}
