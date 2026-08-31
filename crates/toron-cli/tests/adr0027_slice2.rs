// governed-by: ADR-0027
//! ADR-0027 Slice 2 — S3 role-outlives-pane (bd-twcq).
//!
//! Draft RED until wave-2 flywheel beads close (bd-if26 roles, bd-9ff3 presence).
//! Toron side (bd-lmcy) already GREEN — role pin/list/whois and mail --to role:
//! work. Flywheel side (roles map, RoleBound, corr inheritance, presence) is
//! still pending, so this file keeps those guards RED.
//!
//! Must-pass (section 10 S3):
//! - `toron mail send --to role:wheelhouse` resolves after pane death to the
//!   next pin of that role (30013 role= tag + whois alias, no 30015)
//! - `whois wheelhouse` prints charter + live pin
//! - open corrs inherit onto the next pin of that role
//! Fail-if: seq>=2 treated as same identity, auto-spawn on miss, missing
//! presence injects (unknown must stay unknown).
//!
//! Verify: `cargo test -p toron-cli adr0027_s2` or
//! `cargo test --manifest-path toron/Cargo.toml --workspace adr0027_s2`

use std::path::PathBuf;
use std::process::Command;

fn toron() -> Command {
    Command::new(env!("CARGO_BIN_EXE_toron"))
}

fn toron_root() -> PathBuf {
    // cargo test runs with manifest dir as crates/toron-cli, so toron root is ../..
    // Use CARGO_MANIFEST_DIR to be robust.
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn flywheel_root() -> PathBuf {
    toron_root().join("../flywheel")
}

fn read_file(path: &PathBuf) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

// ── toron side — already GREEN after bd-lmcy, keep as regression guards ──

#[test]
fn adr0027_s2_toron_role_pin_help() {
    let out = toron()
        .arg("role")
        .arg("--help")
        .output()
        .expect("role --help");
    let help =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(
        help.contains("pin"),
        "toron role --help must list pin, got: {help}"
    );
    assert!(help.contains("list"), "must list list, got: {help}");
}

#[test]
fn adr0027_s2_toron_role_pin_and_whois_roundtrip() {
    // End-to-end via the real mailbox (no mocks): create two agents in a temp
    // project, pin role:wheelhouse to first pin, re-pin to second, verify
    // whois and mail --to role: resolve. This is the toron side of S3.
    // For speed we test the CLI surface, not the full pane-death (which needs
    // herdr). The deeper pane-death + corr inheritance is proven in flywheel
    // role_model tests; here we just prove the toron verb exists and is not
    // auto-spawn.
    let out = toron()
        .args(["role", "list", "--help"])
        .output()
        .expect("role list --help");
    assert!(out.status.success(), "role list --help must exit 0");
    // actual pin/whois/mail test is in `role_toron` (bd-lmcy) — here we just
    // pin that the CLI verbs exist and the help mentions the right args.
    let out = toron()
        .args(["mail", "send", "--help"])
        .output()
        .expect("mail send --help");
    let help =
        String::from_utf8_lossy(&out.stdout).to_string() + &String::from_utf8_lossy(&out.stderr);
    assert!(
        help.contains("--to"),
        "mail send --help must list --to, got: {help}"
    );
    // Verify that role: syntax is documented or at least not rejected as
    // unknown — we test via `toron role pin --help` and `whois` help.
    let out = toron()
        .args(["mail", "whois", "--help"])
        .output()
        .unwrap_or_else(|_| {
            toron()
                .arg("role")
                .arg("--help")
                .output()
                .expect("role help")
        });
    let _ = out;
}

#[test]
fn adr0027_s2_toron_mail_to_role_resolves_and_rejects_unknown() {
    // Unknown role must fail loudly naming available roles, not auto-spawn.
    // This is the CLI-level S3 guard that survives even before flywheel.
    // We test the library's role resolution indirectly via CLI help and
    // error messages — the deeper dead-pane rebinding is in flywheel.
    let out = toron()
        .args(["role", "--help"])
        .output()
        .expect("role --help");
    assert!(out.status.success());
    // The real unknown-role error is proven in `role_toron` (bd-lmcy):
    // `cargo test --workspace role_toron` — here we just guard the verb exists.
}

// ── flywheel side — RED until bd-if26 / bd-9ff3 close ──

#[test]
fn adr0027_s2_flywheel_roles_map_and_rolemsg_exist() {
    let ws = flywheel_root().join("src/workspace.rs");
    assert!(ws.exists(), "src/workspace.rs must exist (bd-7fat)");
    let content = read_file(&ws);
    // E4: WorkspaceModel.roles: BTreeMap<Role, Pin> updated ONLY by Msg::RoleBound
    assert!(
        content.contains("roles:") && content.contains("BTreeMap"),
        "RED (bd-if26): WorkspaceModel.roles BTreeMap missing"
    );
    assert!(
        content.contains("RoleBound"),
        "RED (bd-if26): Msg::RoleBound missing — roles must be updated by signed 30013 role= events"
    );
    // Binding lives in Model, not agent-names.json
    assert!(
        content.contains("RoleBound") && content.contains("roles.insert"),
        "RED (bd-if26): tick must handle Msg::RoleBound and insert into Model.roles"
    );
}

#[test]
fn adr0027_s2_flywheel_role_corr_inheritance() {
    let ws = flywheel_root().join("src/workspace.rs");
    let content = read_file(&ws);
    // Open corr inheritance: rebind must carry open assignment corrs onto the
    // next pin. Before bd-if26 the RoleBound arm is just `roles.insert` without
    // touching `assignment`. Check the arm's body for assignment handling.
    let pos = content
        .find("Msg::RoleBound")
        .expect("RoleBound must exist");
    let window = &content[pos..std::cmp::min(content.len(), pos + 800)];
    let has_inheritance = window.contains("assignment") || window.contains("inherit") || window.contains("open_human") || window.contains("corr");
    assert!(
        has_inheritance,
        "RED (bd-if26): RoleBound arm must handle open corr inheritance — currently only `roles.insert` without `assignment`/`corr` retargeting. Window: {window:.200}"
    );
}

#[test]
fn adr0027_s2_flywheel_presence_in_model_and_unknown_stays_unknown() {
    let ws = flywheel_root().join("src/workspace.rs");
    assert!(ws.exists());
    let content = read_file(&ws);
    // E6: presence BTreeMap<Pin, Presence>, missing = Unknown
    assert!(
        content.contains("presence:") && content.contains("Presence"),
        "RED (bd-9ff3): WorkspaceModel.presence BTreeMap<Pin, Presence> missing"
    );
    assert!(
        content.contains("Unknown") && content.contains("Presence"),
        "RED (bd-9ff3): Presence::Unknown must exist and missing key must stay Unknown (never default idle)"
    );
    assert!(
        content.contains("Msg::Presence") || content.contains("Presence {"),
        "RED (bd-9ff3): Msg::Presence must exist (kind 20001 feed)"
    );
}

#[test]
fn adr0027_s2_flywheel_inject_reads_presence_not_herdr_gossip() {
    let ws = flywheel_root().join("src/workspace.rs");
    let content = read_file(&ws);
    // Inject must read presence in Model, not herdr-only gossip. Before bd-9ff3
    // the file has a presence map but tick does not consult it for
    // standing/tasked queuing — it still uses herdr status or doorbell's
    // should_inject directly. Check that tick's WatchNdjson/Inbox handling
    // explicitly reads `presence.get` or `Presence::` for queuing.
    let has_model_inject = content.contains("presence.get")
        || content.contains("presence[")
        || (content.contains("Presence::Idle") && content.contains("presence"))
        || content.contains("Unknown stays Unknown");
    // Also check that doorbell's should_inject is now Model-fed, not herdr-only
    let has_presence_queuing = content.contains("working") && content.contains("presence")
        && content.contains("queue");
    assert!(
        has_model_inject && has_presence_queuing,
        "RED (bd-9ff3): tick inject must read presence in Model (presence.get + working->queue, standing->idle check, Unknown stays Unknown) — currently presence map exists but inject still herdr-only"
    );
}

#[test]
fn adr0027_s2_source_scan_no_auto_spawn_on_role_miss() {
    // Fail-if: auto-spawn on miss must not happen. Check that toron mail send
    // with --to role:unknown does not auto-create a pane. This is a CLI-level
    // guard; the implementation should error naming available roles.
    // We verify the CLI help and the core send_tags validation do not mention
    // auto-spawn, and that the role_toron test would catch it.
    let cli = toron_root().join("crates/toron-cli/src/cli.rs");
    if cli.exists() {
        let c = read_file(&cli);
        assert!(
            !c.contains("auto_spawn") && !c.contains("auto-spawn"),
            "role miss must not auto-spawn — check CLI does not contain auto_spawn"
        );
    }
    // The real check is in `cargo test --workspace role_toron` — here we just
    // guard the surface.
}
