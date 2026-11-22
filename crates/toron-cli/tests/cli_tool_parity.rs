//! bd-5jeyj: every live MCP tool has a CLI command.
//!
//! The gap list comes from this test, not from a hand-kept audit: it reads
//! the live router (`tools/list` source of truth) and fails naming any tool
//! with no parity-table row, or whose CLI path does not exist.

use std::process::Command;
use std::sync::PoisonError;
use std::sync::{Mutex, OnceLock};

fn toron() -> Command {
    toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"))
}

/// Every live tool names its CLI command in the parity table.
#[test]
fn every_live_tool_has_a_parity_row() {
    let live = toron_mcp::tools::ToolServer::tool_router().list_all();
    assert!(!live.is_empty(), "router must enumerate the tools");
    let missing: Vec<String> = live
        .iter()
        .filter(|t| {
            !toron_mcp::catalog::TOOL_CLI_PARITY
                .iter()
                .any(|(name, _)| t.name.as_ref() == *name)
        })
        .map(|t| t.name.to_string())
        .collect();
    assert!(
        missing.is_empty(),
        "tools with no CLI mapping ({}): {}",
        missing.len(),
        missing.join(", ")
    );
}

/// Every mapped CLI path exists: `<words...> --help` must succeed, so a
/// future tool cannot ship mapped to a command that was never built.
#[test]
fn every_mapped_cli_command_exists() {
    let mut missing = Vec::new();
    for (tool, cli) in toron_mcp::catalog::TOOL_CLI_PARITY {
        let mut args: Vec<&str> = cli.split_whitespace().collect();
        args.push("--help");
        let out = toron().args(&args).output().expect("run toron");
        if !out.status.success() {
            missing.push(format!("{tool} -> `{cli}`"));
        }
    }
    assert!(
        missing.is_empty(),
        "tools mapped to missing CLI commands ({}): {}",
        missing.len(),
        missing.join(", ")
    );
}

fn shared_data_dir() -> std::path::PathBuf {
    static DIR: OnceLock<std::path::PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let d = std::env::temp_dir().join(format!("toron-cli-parity-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&d);
        unsafe {
            std::env::set_var("TORON_MCP_DATA_DIR", &d);
        }
        d
    })
    .clone()
}

fn shared_json(args: &[&str]) -> serde_json::Value {
    let out = toron().args(args).output().expect("run toron");
    assert!(
        out.status.success(),
        "{} must exit 0, stderr: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("command emits valid JSON")
}

fn register_test_agent() {
    use toron_core::nostr::identity::AgentProfile;
    let _dir = shared_data_dir();
    static REG: Mutex<()> = Mutex::new(());
    let _guard = REG.lock().unwrap_or_else(PoisonError::into_inner);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let mailbox = toron_mcp::demo_mailbox().await.expect("demo mailbox");
        mailbox
            .register_agent("demo", &AgentProfile::new("TestAgent"))
            .await
            .expect("register TestAgent");
    });
}

/// `agent list` reads the registry (not presence): the registered pin shows
/// with its npub.
#[test]
fn agent_list_lists_registry_identities() {
    register_test_agent();
    let v = shared_json(&["agent", "list", "--project", "demo", "--json"]);
    let names: Vec<&str> = v
        .as_array()
        .expect("agent list emits an array")
        .iter()
        .filter_map(|a| a.get("agent_name").or_else(|| a.get("name")))
        .filter_map(|n| n.as_str())
        .collect();
    assert!(
        names.contains(&"TestAgent"),
        "registry lists TestAgent: {v}"
    );
}

/// `agent mint` mints a fresh keypair and fails closed on collision.
#[test]
fn agent_mint_mints_fresh_key() {
    register_test_agent();
    let pin = format!("Minted{}{}", std::process::id(), "Cove");
    let v = shared_json(&["agent", "mint", "--project", "demo", &pin, "--json"]);
    assert_eq!(v.get("name").and_then(|n| n.as_str()), Some(pin.as_str()));
    assert!(
        v.get("npub").and_then(|n| n.as_str()).is_some(),
        "mint returns npub: {v}"
    );
    let again = toron()
        .args(["agent", "mint", "--project", "demo", &pin, "--json"])
        .output()
        .expect("run toron");
    assert!(
        !again.status.success(),
        "a second mint for the same name must fail closed"
    );
}

/// `mail prepare-thread` returns the tool's shape (thread summary + inbox
/// context) through the CLI.
#[test]
fn mail_prepare_thread_shape() {
    register_test_agent();
    let v = shared_json(&[
        "mail",
        "prepare-thread",
        "--project",
        "demo",
        "--thread",
        "t-shape",
        "--as",
        "TestAgent",
        "--program",
        "pi",
        "--model",
        "m",
        "--json",
    ]);
    assert_eq!(v.get("thread_id").and_then(|t| t.as_str()), Some("t-shape"));
    assert!(
        v.get("summary").is_some(),
        "prepare-thread carries summary: {v}"
    );
    assert!(
        v.get("inbox_count").is_some(),
        "prepare-thread carries inbox_count: {v}"
    );
}

/// `contact handshake` returns the tool's shape (request + welcome).
#[test]
fn contact_handshake_shape() {
    register_test_agent();
    let v = shared_json(&[
        "contact",
        "handshake",
        "--project",
        "demo",
        "--requester",
        "TestAgent",
        "--target",
        "TestAgent",
        "--auto-accept",
        "--json",
    ]);
    assert_eq!(
        v.get("requester").and_then(|r| r.as_str()),
        Some("TestAgent")
    );
    assert!(
        v.get("welcome_sent").is_some(),
        "handshake carries welcome_sent: {v}"
    );
}

/// `reserve cycle` reserves and auto-releases through the tool's shape.
#[test]
fn reserve_cycle_shape() {
    register_test_agent();
    let v = shared_json(&[
        "reserve",
        "cycle",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--paths",
        "cycle/x.rs",
        "--reason",
        "bd-5jeyj",
        "--auto-release",
        "--json",
    ]);
    assert!(v.get("granted").is_some(), "cycle reports granted: {v}");
    assert!(v.get("released").is_some(), "cycle reports released: {v}");
}

/// Pane commands emit identity shapes through the CLI, live or degraded:
/// a pane id plus either the resolved pin or the reason it is unknown.
#[test]
fn pane_commands_shape() {
    for args in [
        vec!["pane", "resolve", "--project", "demo", "pane-9", "--json"],
        vec!["pane", "cleanup", "--project", "demo", "--json"],
    ] {
        let out = toron().args(&args).output().expect("run toron");
        assert!(
            out.status.success(),
            "{} must exit 0, stderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
        let v: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("pane command emits JSON");
        assert!(
            v.get("pane_id").is_some() || v.get("removed").is_some() || v.get("note").is_some(),
            "pane shape names the pane or the outcome: {v}"
        );
    }
}

/// Product commands round-trip through the CLI: ensure + link (operator
/// key), then fan-out reads over the member project.
#[test]
fn product_commands_shape() {
    let data_dir =
        std::env::temp_dir().join(format!("toron-product-parity-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("mkdir dedicated data dir");
    let op_key = data_dir.join("operator.ncryptsec");
    let own = |args: &[&str]| {
        toron()
            .env("TORON_MCP_DATA_DIR", &data_dir)
            .env("TORON_OPERATOR_KEY", &op_key)
            .args(args)
            .output()
            .expect("run toron on own data dir")
    };
    let as_json = |out: &std::process::Output| -> serde_json::Value {
        assert!(
            out.status.success(),
            "command must exit 0, stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).expect("command emits valid JSON")
    };
    let keygen = own(&[
        "key",
        "generate",
        "--role",
        "operator",
        "--out",
        op_key.to_str().unwrap(),
        "--json",
    ]);
    assert!(keygen.status.success(), "key generate");
    let boot = own(&[
        "agent",
        "bootstrap",
        "--project",
        "demo",
        "--as",
        "TestAgent",
        "--json",
    ]);
    assert!(boot.status.success(), "bootstrap");
    let ensured = as_json(&own(&[
        "product",
        "ensure",
        "pp1",
        "Parity Product",
        "--json",
    ]));
    assert_eq!(
        ensured.get("product_key").and_then(|k| k.as_str()),
        Some("pp1")
    );
    let linked = as_json(&own(&["product", "link", "pp1", "demo", "--json"]));
    assert!(
        linked.get("member_projects").is_some(),
        "link lists members: {linked}"
    );
    let searched = as_json(&own(&[
        "product",
        "search",
        "pp1",
        "nothing-matches-this",
        "--json",
    ]));
    assert!(
        searched.as_array().is_some(),
        "search emits an array: {searched}"
    );
    let inbox = as_json(&own(&[
        "product",
        "inbox",
        "pp1",
        "--as",
        "TestAgent",
        "--json",
    ]));
    assert!(inbox.as_array().is_some(), "inbox emits an array: {inbox}");
    let summarized = as_json(&own(&["product", "summarize", "pp1", "t-none", "--json"]));
    assert!(
        summarized.get("thread_id").is_some(),
        "summarize names the thread: {summarized}"
    );
}

/// `judgment ask` evaluates the route-triage pack offline and returns the
/// tool's shape (record, replayed on the second identical call).
#[test]
fn judgment_ask_shape() {
    register_test_agent();
    let state = r#"{"primitive":"none","risk_destructive":false,"risk_security":false}"#;
    let args = [
        "judgment",
        "ask",
        "--project",
        "demo",
        "--effect-key",
        "ek-parity-1",
        "--pack",
        "route-triage",
        "--state",
        state,
        "--transport",
        "offline",
        "--json",
    ];
    let first = shared_json(&args);
    assert_eq!(
        first.get("effect_key").and_then(|k| k.as_str()),
        Some("ek-parity-1")
    );
    assert!(
        first.get("answers").is_some(),
        "ask returns answers: {first}"
    );
    let second = shared_json(&args);
    assert_eq!(
        second.get("replayed"),
        Some(&serde_json::Value::Bool(true)),
        "the identical call replays the journaled row: {second}"
    );
}

/// `toron status --json` carries the health_check tool's shape (bus +
/// archive liveness), degraded rather than fatal with no store.
#[test]
fn status_json_carries_health_shape() {
    let out = toron()
        .args(["status", "--json"])
        .output()
        .expect("run toron");
    assert!(out.status.success(), "status must exit 0");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("status emits JSON");
    let health = v.get("health").expect("status carries health");
    assert!(
        health.get("bus").and_then(|b| b.get("ok")).is_some()
            && health.get("archive").and_then(|a| a.get("ok")).is_some(),
        "health matches the health_check shape (bus/archive ok): {health}"
    );
}
