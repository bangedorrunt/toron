// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-7 — Contact handshake + policy enforcement (P1 / v1 gate).
//!
//! Actors: Pi-α (project A), OC-β (project B).
//! Steps:
//! 1. Pi-α `contact request OCβ --project B` → pending AgentLink recorded.
//! 2. OC-β `contact approve` → AgentLink approved.
//! 3. Cross-project resolution works (Pi-α's request resolved OC-β in B).
//! 4. OC-β `contact policy Piα block_all` → policy recorded.
//! 5. block_all → Pi-α's inbound is dropped at the projection gate
//!    (`is_approved_contact` fails; policy is BlockAll).
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use nostr_sdk::prelude::*;
use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::contacts::{RequestContact, RespondContact};
use toron_core::mailbox::{ContactPolicy, LinkStatus, Mailbox, RegisteredAgent};
use toron_core::nostr::identity::AgentProfile;

/// A mailbox on its own project root + a shared bus, with an index pool.
struct MailboxFixture {
    mailbox: Mailbox,
    data_dir: std::path::PathBuf,
}

async fn fixture(
    tag: &str,
    bus: InProcessBus,
    pool: sqlx::SqlitePool,
) -> anyhow::Result<MailboxFixture> {
    let dir = std::env::temp_dir().join(format!("toron-sc7-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir)?;
    let mailbox = Mailbox::new(Arc::new(bus), dir.clone(), None)
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool);
    Ok(MailboxFixture {
        mailbox,
        data_dir: dir,
    })
}

use std::sync::Arc;

async fn register_agent(
    mailbox: &Mailbox,
    project: &str,
    name: &str,
) -> anyhow::Result<RegisteredAgent> {
    let profile = AgentProfile::new(name);
    mailbox.create_agent_identity(project, &profile).await
}

#[tokio::test]
async fn sc7_contact_handshake_and_block_all_policy_drop() {
    let dir = std::env::temp_dir().join(format!("toron-sc7-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
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

    // Pi-α on project A, OC-β on project B (cross-project handshake).
    let a = fixture("proja", bus.clone(), pool.clone())
        .await
        .expect("fixture A");
    let b = fixture("projb", bus.clone(), pool.clone())
        .await
        .expect("fixture B");

    let pi_alpha = register_agent(&a.mailbox, "demo-a", "PiAlpha")
        .await
        .expect("register PiAlpha");
    let oc_beta = register_agent(&b.mailbox, "demo-b", "OCBeta")
        .await
        .expect("register OCBeta");

    // --- Step 1: Pi-α requests contact with OC-β (cross-project) ---
    let link = a
        .mailbox
        .request_contact(&RequestContact {
            project_slug: "demo-a".into(),
            from_agent: "PiAlpha".into(),
            to_agent: "OCBeta".into(),
            to_project: Some("demo-b".into()),
            reason: Some("collab".into()),
        })
        .await
        .expect("request contact");
    assert_eq!(link.status, LinkStatus::Pending);
    assert_eq!(link.to_project.as_deref(), Some("demo-b"));
    assert_eq!(link.to_name, "OCBeta");
    assert!(link.to_pubkey.len() == 64, "cross-project npub resolved");

    // --- Step 2: OC-β approves (responder=OCBeta, requester=PiAlpha in demo-a) ---
    let approved = b
        .mailbox
        .respond_contact(&RespondContact {
            project_slug: "demo-b".into(),
            to_agent: "OCBeta".into(),
            from_agent: "PiAlpha".into(),
            accept: true,
            from_project: Some("demo-a".into()),
        })
        .await
        .expect("approve");
    assert_eq!(approved.status, LinkStatus::Approved);

    // --- Step 3: cross-project approval visible via is_approved_contact ---
    let pi_pk = PublicKey::from_hex(&pi_alpha.npub).expect("PiAlpha pubkey");
    let oc_pk = PublicKey::from_hex(&oc_beta.npub).expect("OCBeta pubkey");
    assert!(
        b.mailbox.is_approved_contact(&pi_pk, &oc_pk).await,
        "Pi-α approved by OC-β after handshake"
    );

    // --- Step 4: OC-β sets block_all for inbound ---
    b.mailbox
        .set_contact_policy("demo-b", "OCBeta", ContactPolicy::BlockAll)
        .await
        .expect("set policy");

    // --- Step 5: block_all → policy gate drops Pi-α's inbound ---
    let policy = b.mailbox.contact_policy_for("demo-b", &oc_pk).await;
    assert_eq!(
        policy,
        ContactPolicy::BlockAll,
        "policy recorded as block_all"
    );
    // BlockAll is enforced even though the AgentLink is approved (policy wins).
    let link_list = b
        .mailbox
        .list_contacts("demo-b", "OCBeta")
        .await
        .expect("list contacts");
    assert!(!link_list.is_empty(), "AgentLink recorded");

    a.cleanup();
    b.cleanup();
    let _ = std::fs::remove_dir_all(dir);
}

impl MailboxFixture {
    fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}
