// governed-by: ADR-0018
#![forbid(unsafe_code)]

//! SC-26 — Agent identity naming grammar + routing rules (ADR-0018).
//!
//! Acceptance (FIX-SPECS-2026-08-15 S8 + bd-adr-naming-grammar-h8nj):
//! - swarm project: whois + send by canonical name (`flywheel-<slug>-<role>
//!   <seq>`) and npub work; non-canonical names fail with errors naming the
//!   grammar; registration of a non-canonical name is rejected.
//! - a folded legacy alias (`agent_name_aliases`) resolves through to the
//!   canonical npub on whois AND the send-path.
//! - open project (no identity_mode): legacy names still route (AM-Rust
//!   parity preserved).
//! - live reconciliation: `project_identity_mode` reflects the persisted
//!   policy; legacy identities stay listed as display metadata.
//!
//! Un-gated (runs in the default workspace test matrix).

use std::sync::Arc;

use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::messaging::{FetchInbox, SendMessage};
use toron_core::nostr::identity::AgentProfile;

async fn boot(dir: &std::path::Path) -> (Mailbox, InProcessBus, sqlx::SqlitePool) {
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(
        pool.clone(),
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus");
    let mailbox = Mailbox::new(
        Arc::new(bus.clone()),
        dir.to_path_buf(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("toron-dev")
    .with_index_pool(pool.clone());
    (mailbox, bus, pool)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) -> String {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register")
        .npub
}

fn send(project: &str, sender: &str, to: &str, body: &str) -> SendMessage {
    SendMessage {
        project_slug: project.into(),
        sender_name: sender.into(),
        to: vec![to.into()],
        subject: "canonical naming".into(),
        body_md: body.into(),
        cc: vec![],
        bcc: vec![],
        importance: None,
        ack_required: false,
        thread_id: None,
        sender_token: None,
        overseer: false,
        extra_tags: vec![],
    }
}

#[tokio::test]
async fn sc26_swarm_project_routes_canonical_names_and_npubs_only() {
    let dir = std::env::temp_dir().join(format!("toron-sc26-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, _pool) = boot(&dir).await;

    // Pre-swarm state: the legacy identity exists before the flip (the
    // one-time reconciliation scenario — legacy names demote, never delete).
    mailbox.ensure_project("toron").await.expect("ensure");
    let oc_npub = register(&mailbox, "toron", "flywheel-toron-oc").await;
    let oc2_npub = register(&mailbox, "toron", "flywheel-toron-oc2").await;
    register(&mailbox, "toron", "lane-1-bootstrap").await;
    register(&mailbox, "toron", "DustySparrow").await;
    mailbox.set_identity_mode("toron", "swarm").await;

    // --- whois: canonical + npub resolve; legacy fails with grammar error ---
    let w = mailbox
        .whois("toron", "flywheel-toron-oc2")
        .await
        .expect("canonical whois");
    assert_eq!(w.npub, oc2_npub);

    let by_npub = mailbox.whois("toron", &oc_npub).await.expect("npub whois");
    assert_eq!(by_npub.name, "flywheel-toron-oc");

    let err = mailbox
        .whois("toron", "lane-1-bootstrap")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("flywheel-toron-<role><seq>"),
        "grammar in error: {msg}"
    );
    assert!(msg.contains("not routable"), "demotion in error: {msg}");

    // --- registration: canonical accepted, legacy rejected ---
    mailbox
        .register_agent("toron", &AgentProfile::new("flywheel-toron-oc3"))
        .await
        .expect("canonical registration");
    let err = mailbox
        .register_agent("toron", &AgentProfile::new("assistant"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("flywheel-toron-<role><seq>"),
        "register rejects legacy names: {err}"
    );

    // --- send: canonical + npub deliver; legacy fails the whole send ---
    mailbox
        .send_message(&send(
            "toron",
            "flywheel-toron-oc",
            "flywheel-toron-oc2",
            "canonical ping",
        ))
        .await
        .expect("canonical send");
    mailbox
        .send_message(&send("toron", "flywheel-toron-oc", &oc2_npub, "npub ping"))
        .await
        .expect("npub send");
    let err = mailbox
        .send_message(&send(
            "toron",
            "flywheel-toron-oc",
            "lane-1-bootstrap",
            "legacy ping",
        ))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("non-routable"),
        "send rejects legacy recipients: {err}"
    );

    // --- inbox shows the two deliveries (both landed) ---
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "toron".into(),
            agent_name: "flywheel-toron-oc2".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
        })
        .await
        .expect("fetch");
    assert_eq!(inbox.len(), 2, "canonical + npub sends both delivered");

    // --- legacy identities stay listed as display metadata ---
    let agents = mailbox
        .list_agents("toron", Default::default())
        .await
        .expect("list");
    assert!(
        agents.iter().any(|a| a.name == "lane-1-bootstrap"),
        "legacy identity remains visible as metadata"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sc26_folded_alias_resolves_through_to_canonical_on_whois_and_send() {
    let dir = std::env::temp_dir().join(format!("toron-sc26b-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, _pool) = boot(&dir).await;

    mailbox.ensure_project("toron").await.expect("ensure");
    register(&mailbox, "toron", "flywheel-toron-oc").await;
    let oc2_npub = register(&mailbox, "toron", "flywheel-toron-oc2").await;
    register(&mailbox, "toron", "DustySparrow").await;
    mailbox.set_identity_mode("toron", "swarm").await;

    // Fold: DustySparrow → flywheel-toron-oc2 (pane binding discoverable).
    mailbox
        .fold_agent_name_alias("toron", "DustySparrow", "flywheel-toron-oc2")
        .await;

    // whois on the folded alias resolves to the canonical npub.
    let w = mailbox
        .whois("toron", "DustySparrow")
        .await
        .expect("alias whois");
    assert_eq!(w.npub, oc2_npub, "folded alias → canonical npub");

    // send to the folded alias delivers to the canonical identity.
    mailbox
        .send_message(&send(
            "toron",
            "flywheel-toron-oc",
            "DustySparrow",
            "folded ping",
        ))
        .await
        .expect("folded alias send");
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "toron".into(),
            agent_name: "flywheel-toron-oc2".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
        })
        .await
        .expect("fetch");
    assert_eq!(
        inbox.len(),
        1,
        "folded-alias send landed on the canonical identity"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sc26_open_project_keeps_legacy_routing_parity() {
    let dir = std::env::temp_dir().join(format!("toron-sc26c-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, _pool) = boot(&dir).await;

    // No identity_mode row → open. Legacy names keep routing (AM-Rust
    // parity, documented in known-divergences.md).
    mailbox.ensure_project("demo").await.expect("ensure");
    let blue_npub = register(&mailbox, "demo", "BlueLake").await;
    register(&mailbox, "demo", "GreenCastle").await;

    let w = mailbox
        .whois("demo", "BlueLake")
        .await
        .expect("whois legacy");
    assert_eq!(
        w.npub, blue_npub,
        "open mode whois resolves any registered name"
    );
    assert_eq!(
        mailbox.project_identity_mode("demo").await,
        toron_core::mailbox::naming::IdentityMode::Open,
        "default mode is open"
    );

    mailbox
        .send_message(&send("demo", "GreenCastle", "BlueLake", "parity ping"))
        .await
        .expect("open mode send routes legacy names");
    mailbox
        .register_agent("demo", &AgentProfile::new("RoseDune"))
        .await
        .expect("open mode accepts any registration");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sc26_identity_mode_persists_across_mailbox_instances() {
    let dir = std::env::temp_dir().join(format!("toron-sc26d-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, pool) = boot(&dir).await;

    mailbox
        .ensure_project_with_mode("toron", Some("swarm"))
        .await
        .expect("ensure swarm");

    // A fresh mailbox over the same archive sees the persisted policy.
    let bus2 = InProcessBus::open(
        pool.clone(),
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus2");
    let mailbox2 = Mailbox::new(
        Arc::new(bus2),
        dir.clone(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("toron-dev")
    .with_index_pool(pool.clone());
    assert_eq!(
        mailbox2.project_identity_mode("toron").await,
        toron_core::mailbox::naming::IdentityMode::Swarm,
        "identity mode survives daemon restart (archive-backed)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
