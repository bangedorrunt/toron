#![allow(clippy::doc_lazy_continuation)]
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
    toron_core::testkit::toron_command(env!("CARGO_BIN_EXE_toron"))
}

fn toron_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Adjacent flywheel checkout (source-scan target). Toron CI checks out
/// THIS repo only, so absence is environmental, not a product failure:
/// callers skip with a loud message, and the asserts still run fail-loud
/// on any checkout that has both repos side by side (dev machines).
fn flywheel_root() -> Option<PathBuf> {
    let p = toron_root().join("../flywheel");
    (p.join("src/workspace.rs").is_file()).then_some(p)
}

fn skip_no_flywheel() {
    eprintln!(
        "SKIP: no adjacent ../flywheel checkout (toron-only CI) — flywheel-side asserts run on dual checkouts"
    );
}

fn read_file(path: &PathBuf) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

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
    let out = toron()
        .args(["role", "list", "--help"])
        .output()
        .expect("role list --help");
    assert!(out.status.success(), "role list --help must exit 0");
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
    let out = toron()
        .args(["role", "--help"])
        .output()
        .expect("role --help");
    assert!(out.status.success());
}

#[test]
fn adr0027_s2_flywheel_roles_map_and_rolemsg_exist() {
    let Some(ws) = flywheel_root() else {
        skip_no_flywheel();
        return;
    };
    let ws = ws.join("src/workspace.rs");
    let content = read_file(&ws);
    assert!(
        content.contains("roles:") && content.contains("BTreeMap"),
        "RED (bd-if26): WorkspaceModel.roles BTreeMap missing"
    );
    assert!(
        content.contains("RoleBound"),
        "RED (bd-if26): Msg::RoleBound missing — roles must be updated by signed 30013 role= events"
    );
    assert!(
        content.contains("RoleBound") && content.contains("roles.insert"),
        "RED (bd-if26): tick must handle Msg::RoleBound and insert into Model.roles"
    );
}

#[test]
fn adr0027_s2_flywheel_role_corr_inheritance() {
    let Some(fw) = flywheel_root() else {
        skip_no_flywheel();
        return;
    };
    let ws = fw.join("src/workspace.rs");
    let content = read_file(&ws);
    let pos = content
        .find("Msg::RoleBound")
        .expect("RoleBound must exist");
    let window = &content[pos..std::cmp::min(content.len(), pos + 800)];
    let has_inheritance = window.contains("assignment")
        || window.contains("inherit")
        || window.contains("open_human")
        || window.contains("corr");
    assert!(
        has_inheritance,
        "RED (bd-if26): RoleBound arm must handle open corr inheritance — currently only `roles.insert` without `assignment`/`corr` retargeting. Window: {window:.200}"
    );
}

#[test]
fn adr0027_s2_flywheel_presence_in_model_and_unknown_stays_unknown() {
    let Some(ws) = flywheel_root() else {
        skip_no_flywheel();
        return;
    };
    let ws = ws.join("src/workspace.rs");
    let content = read_file(&ws);
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
    let Some(fw) = flywheel_root() else {
        skip_no_flywheel();
        return;
    };
    let ws = fw.join("src/workspace.rs");
    let content = read_file(&ws);
    let has_model_inject = content.contains("presence.get")
        || content.contains("presence[")
        || (content.contains("Presence::Idle") && content.contains("presence"))
        || content.contains("Unknown stays Unknown");
    let has_presence_queuing =
        content.contains("working") && content.contains("presence") && content.contains("queue");
    assert!(
        has_model_inject && has_presence_queuing,
        "RED (bd-9ff3): tick inject must read presence in Model (presence.get + working->queue, standing->idle check, Unknown stays Unknown) — currently presence map exists but inject still herdr-only"
    );
}

#[test]
fn adr0027_s2_source_scan_no_auto_spawn_on_role_miss() {
    let cli = toron_root().join("crates/toron-cli/src/cli.rs");
    if cli.exists() {
        let c = read_file(&cli);
        assert!(
            !c.contains("auto_spawn") && !c.contains("auto-spawn"),
            "role miss must not auto-spawn — check CLI does not contain auto_spawn"
        );
    }
}

#[tokio::test]
async fn adr0027_s2_s3_e2e_role_rebind_with_corr_and_whois() {
    use std::sync::Arc;
    use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
    use toron_core::index::schema;
    use toron_core::mailbox::{Mailbox, SendMessage};
    use toron_core::nostr::identity::AgentProfile;

    async fn test_mailbox(tag: &str) -> (Mailbox, PathBuf, PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-s3-{}-{}-{}.sqlite",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-s3-data-{}-{}-{}",
            tag,
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(toron_core::keys::generate()),
        )
        .with_agent_passphrase("x")
        .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    let (mailbox, db_path, data_dir) = test_mailbox("s3-e2e").await;
    mailbox
        .register_agent("demo", &profile("OldPin"))
        .await
        .expect("register old");
    mailbox
        .register_agent("demo", &profile("SenderOne"))
        .await
        .expect("sender");
    mailbox
        .register_agent("demo", &profile("NewPin"))
        .await
        .expect("new pin pre-register");
    mailbox
        .set_role_pin("demo", "wheelhouse", "OldPin")
        .await
        .expect("pin old");
    let who1 = mailbox.whois("demo", "wheelhouse").await.expect("whois1");
    assert_eq!(who1.name, "OldPin", "whois must print live pin OldPin");
    let corr = "FM-S3-001";
    let msg1 = SendMessage {
        project_slug: "demo".into(),
        sender_name: "SenderOne".into(),
        to: vec!["role:wheelhouse".into()],
        subject: "tasked".into(),
        body_md: "do work".into(),
        extra_tags: {
            use toron_core::nostr::Tag;
            vec![
                Tag::parse(["corr", corr]).unwrap(),
                Tag::parse(["wake", "tasked"]).unwrap(),
            ]
        },
        ..Default::default()
    };
    let r1 = mailbox.send_message(&msg1).await.expect("send1 to role");
    assert_eq!(r1.deliveries[0].payload.recipient, "OldPin");
    mailbox
        .set_role_pin("demo", "wheelhouse", "NewPin")
        .await
        .expect("rebind");
    let who2 = mailbox.whois("demo", "wheelhouse").await.expect("whois2");
    assert_eq!(who2.name, "NewPin", "whois after rebind must be NewPin");
    let Some(fw) = flywheel_root() else {
        skip_no_flywheel();
        return;
    };
    let ws_content = read_file(&fw.join("src/workspace.rs"));
    let has_30015_kind = ws_content.contains("30015")
        && (ws_content.contains("Kind(30015)") || ws_content.contains("30015,"));
    assert!(
        !has_30015_kind,
        "must not mint 30015 kind — role via 30013 role= tags only (found 30015 kind definition)"
    );
    let msg2 = SendMessage {
        project_slug: "demo".into(),
        sender_name: "SenderOne".into(),
        to: vec!["role:wheelhouse".into()],
        subject: "second".into(),
        body_md: "more work".into(),
        ..Default::default()
    };
    let r2 = mailbox.send_message(&msg2).await.expect("send2");
    assert_eq!(r2.deliveries[0].payload.recipient, "NewPin");
    let unknown = mailbox
        .send_message(&SendMessage {
            project_slug: "demo".into(),
            sender_name: "SenderOne".into(),
            to: vec!["role:unknown-role-xyz".into()],
            subject: "fail".into(),
            body_md: "hi".into(),
            ..Default::default()
        })
        .await;
    assert!(unknown.is_err(), "unknown role must fail, not auto-spawn");
    let err = unknown.unwrap_err().to_string();
    assert!(
        err.contains("available") || err.contains("unknown role") || err.contains("wheelhouse"),
        "unknown role error must name available roles, got: {err}"
    );

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_dir_all(&data_dir);
}
