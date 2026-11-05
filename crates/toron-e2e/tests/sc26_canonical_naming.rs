// governed-by: ADR-0018
#![forbid(unsafe_code)]

//! SC-26 — Agent identity naming grammar + routing rules (ADR-0018).
//!
//! Acceptance (FIX-SPECS-2026-08-15 S8 + bd-adr-naming-grammar-h8nj,
//! amended by bd-pin-public-plane-s-easy.4 for D1a):
//! - ADR-0018 D1a: the **pin** (adjective+noun, e.g. `BlueRidge`) is the
//!   public, routable identity. The D1 slug (`flywheel-<slug>-<role><seq>`)
//!   is the internal terminal name — a fold alias of the pin, never a second
//!   registered identity.
//! - swarm project: whois + send by registered pin, folded slug, or npub
//!   work; anything else fails with errors naming the grammar; registering a
//!   second identity for a folded slug is refused.
//! - a folded alias (`agent_name_aliases`) resolves through to the holder's
//!   npub on whois AND the send-path; send as the slug loads the pin key.
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
use toron_core::mailbox::ListAgentsOptions;
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

    mailbox.ensure_project("toron").await.expect("ensure");
    let oc_npub = register(&mailbox, "toron", "flywheel-toron-oc").await;
    let oc2_npub = register(&mailbox, "toron", "flywheel-toron-oc2").await;
    register(&mailbox, "toron", "lane-1-bootstrap").await;
    register(&mailbox, "toron", "DustySparrow").await;
    mailbox.set_identity_mode("toron", "swarm").await;

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
    assert!(
        msg.contains("non-routable"),
        "demotion named in error: {msg}"
    );

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

    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "toron".into(),
            agent_name: "flywheel-toron-oc2".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
            thread_id: None,
            ack_overdue_only: false,
        })
        .await
        .expect("fetch");
    assert_eq!(inbox.len(), 2, "canonical + npub sends both delivered");

    let agents = mailbox
        .list_agents("toron", ListAgentsOptions::default())
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

    mailbox
        .fold_agent_name_alias("toron", "DustySparrow", "flywheel-toron-oc2")
        .await;

    let w = mailbox
        .whois("toron", "DustySparrow")
        .await
        .expect("alias whois");
    assert_eq!(w.npub, oc2_npub, "folded alias → canonical npub");

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
            thread_id: None,
            ack_overdue_only: false,
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

#[tokio::test]
async fn sc26_slug_folds_to_pin_same_npub_no_second_key() {
    let dir = std::env::temp_dir().join(format!("toron-sc26e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, _pool) = boot(&dir).await;

    mailbox.ensure_project("toron").await.expect("ensure");
    register(&mailbox, "toron", "flywheel-toron-oc").await;
    let pin_npub = register(&mailbox, "toron", "BlueRidge").await;
    mailbox.set_identity_mode("toron", "swarm").await;

    mailbox
        .fold_agent_name_alias("toron", "flywheel-toron-coder", "BlueRidge")
        .await;

    let w_slug = mailbox
        .whois("toron", "flywheel-toron-coder")
        .await
        .expect("whois slug via fold");
    let w_pin = mailbox
        .whois("toron", "BlueRidge")
        .await
        .expect("whois pin");
    assert_eq!(
        w_slug.npub, pin_npub,
        "whois(slug) resolves to the pin's npub"
    );
    assert_eq!(w_pin.npub, pin_npub, "whois(pin) is the pin");

    mailbox
        .send_message(&send(
            "toron",
            "flywheel-toron-oc",
            "flywheel-toron-coder",
            "slug ping",
        ))
        .await
        .expect("send to slug");
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "toron".into(),
            agent_name: "BlueRidge".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
            thread_id: None,
            ack_overdue_only: false,
        })
        .await
        .expect("fetch");
    assert_eq!(
        inbox.len(),
        1,
        "send to slug landed on the pin identity's inbox"
    );

    let err = mailbox
        .register_agent("toron", &AgentProfile::new("flywheel-toron-coder"))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("fold"),
        "register refuses the folded slug as a second identity: {err}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
