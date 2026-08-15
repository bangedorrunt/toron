// governed-by: ADR-0017
#![forbid(unsafe_code)]

//! SC-25 — Project-root canonicalization + identity resolution (ADR-0017).
//!
//! Acceptance (FIX-SPECS-2026-08-15 S2): spawning two roots — short slug vs
//! path form — for the SAME project:
//! - a message sent addressing an agent via one root is visible + ackable
//!   via the other;
//! - registration state spans both (same npub either way);
//! - no duplicate agent rows in the index;
//! - ensure_project via either spelling returns the SAME canonical slug.
//!
//! Un-gated (runs in the default workspace test matrix).

use std::sync::Arc;

use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::messaging::{FetchInbox, SendMessage};
use toron_core::nostr::identity::AgentProfile;

/// The two spellings of the same project (the 2026-08-14 split-root repro:
/// MCP short key vs CLI absolute-path slug).
const SHORT: &str = "toron";
const PATH: &str = "/Users/bangedorrunt/workspace/toron";

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
    // Daemon-open migration: fold any pre-existing path-slug roots.
    mailbox
        .reconcile_project_aliases()
        .await
        .expect("reconcile");
    (mailbox, bus, pool)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) -> String {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register")
        .npub
}

async fn agent_rows(pool: &sqlx::SqlitePool, project: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE project_slug = ?1")
        .bind(project)
        .fetch_one(pool)
        .await
        .expect("count")
}

fn send(project: &str, sender: &str, to: &str, body: &str) -> SendMessage {
    SendMessage {
        project_slug: project.into(),
        sender_name: sender.into(),
        to: vec![to.into()],
        subject: "canonical roots".into(),
        body_md: body.into(),
        cc: vec![],
        bcc: vec![],
        importance: Some("high".into()),
        ack_required: true,
        thread_id: None,
        sender_token: None,
        overseer: false,
        extra_tags: vec![],
    }
}

#[tokio::test]
async fn sc25_canonical_roots_slug_and_path_form_are_one_project() {
    let dir = std::env::temp_dir().join(format!("toron-sc25-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, pool) = boot(&dir).await;

    // --- ensure_project via both spellings → ONE canonical root ---
    let first = mailbox.ensure_project(SHORT).await.expect("ensure short");
    assert_eq!(first.slug, SHORT, "short slug is canonical");
    let second = mailbox.ensure_project(PATH).await.expect("ensure path");
    assert_eq!(
        second.slug, SHORT,
        "path form resolves to the canonical slug"
    );
    assert!(!second.created, "second ensure is a no-op on the same root");

    // --- registration state spans both, no duplicate rows ---
    let path_npub = register(&mailbox, PATH, "PiAlpha").await;
    let short_npub = register(&mailbox, SHORT, "PiAlpha").await;
    assert_eq!(path_npub, short_npub, "same identity via either root");
    assert_eq!(
        agent_rows(&pool, SHORT).await,
        1,
        "one agent row under the canonical slug"
    );
    assert_eq!(agent_rows(&pool, PATH).await, 0, "no path-slug rows");

    // whois resolves through either spelling.
    let whois_short = mailbox.whois(SHORT, "PiAlpha").await.expect("whois short");
    let whois_path = mailbox.whois(PATH, "PiAlpha").await.expect("whois path");
    assert_eq!(whois_short.npub, whois_path.npub, "whois spans both roots");

    // Register the recipient via the SHORT slug — cross-root addressing.
    let oc_npub = register(&mailbox, SHORT, "OCBeta").await;

    // The unwrap loop projects inbound wraps into the index (search reads
    // the projection). Spawn it after registration so the npub→project map
    // sees every agent.
    let _loop = mailbox.spawn_unwrap_loop();

    // --- send via PATH root, fetch + ack via SHORT root ---
    let sent = mailbox
        .send_message(&send(PATH, "PiAlpha", "OCBeta", "cross-root ping"))
        .await
        .expect("send via path root");
    assert_eq!(sent.count, 1, "one delivery");
    let message_id = sent.deliveries[0].payload.event_id.clone();

    // Wait for the projection to land (poll, not a fixed sleep).
    let pool2 = pool.clone();
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(&pool2)
                .await
                .expect("count");
            if n >= 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await
    .expect("message projected within 15s");

    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: SHORT.into(),
            agent_name: "OCBeta".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
        })
        .await
        .expect("fetch via short root");
    assert_eq!(inbox.len(), 1, "message visible via the short root");
    assert_eq!(inbox[0].id, message_id, "same rumor id");

    let ack = mailbox
        .acknowledge_message(PATH, "OCBeta", &message_id)
        .await
        .expect("ack via path root");
    assert!(ack.acknowledged, "ack delivered");
    let ack2 = mailbox
        .acknowledge_message(SHORT, "OCBeta", &message_id)
        .await
        .expect("ack via short root");
    assert!(ack2.acknowledged, "ack idempotent across roots");
    assert_eq!(
        ack.acknowledged_at_us, ack2.acknowledged_at_us,
        "same stored receipt"
    );

    // --- search spans both roots ---
    let hits_short = mailbox
        .search_messages(&toron_core::mailbox::search::SearchMessages {
            project_key: SHORT.into(),
            query: "cross-root".into(),
            ..Default::default()
        })
        .await
        .expect("search via short root");
    assert_eq!(hits_short.len(), 1, "search finds the message");
    assert_eq!(hits_short[0].id, message_id);

    let hits_path = mailbox
        .search_messages(&toron_core::mailbox::search::SearchMessages {
            project_key: PATH.into(),
            query: "cross-root".into(),
            ..Default::default()
        })
        .await
        .expect("search via path root");
    assert_eq!(hits_path.len(), 1, "search via path root finds it too");
    assert_eq!(hits_path[0].id, message_id);

    // --- registration state spans both after the fact (upsert) ---
    let npub_after = register(&mailbox, PATH, "OCBeta").await;
    assert_eq!(npub_after, oc_npub, "upsert keeps the one identity");

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn sc25_migration_folds_preexisting_path_slug_rows_read_only() {
    let dir = std::env::temp_dir().join(format!("toron-sc25b-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, pool) = boot(&dir).await;

    // Reconstruct the pre-ADR-0017 state by hand: a project root + identity
    // living under the PATH-slug spelling — legacy key file at the nested
    // tree, legacy profile event tagged `#t = <path slug>`, legacy agents
    // row — exactly what the 2026-08-14 repro left on disk.
    let legacy_slug = "/srv/legacy/toron";
    let profile = AgentProfile::new("RoseDune");
    let result = toron_core::nostr::identity::create_agent_identity(
        legacy_slug,
        &profile,
        nostr_sdk::nostr::Timestamp::now(),
    )
    .expect("legacy identity");
    let legacy_key = mailbox
        .data_dir()
        .join("srv/legacy/toron/agents/RoseDune/key.ncryptsec");
    toron_core::keys::write_encrypted_key(
        &legacy_key,
        &result.keys,
        "toron-dev",
        toron_core::keys::DEFAULT_LOG_N,
    )
    .expect("legacy key");
    mailbox
        .bus()
        .publish(result.event)
        .await
        .expect("legacy profile");
    let now_us = nostr_sdk::nostr::Timestamp::now().as_secs() as i64 * 1_000_000;
    sqlx::query(
        "INSERT INTO agents (project_slug, name, pubkey, program, model, \
         task_description, created_ts_us) VALUES (?1, ?2, ?3, '', '', '', ?4)",
    )
    .bind(legacy_slug)
    .bind("RoseDune")
    .bind(result.keys.public_key().to_hex())
    .bind(now_us)
    .execute(&pool)
    .await
    .expect("legacy agents row");

    // Daemon-open migration folds the legacy root into the alias table.
    mailbox
        .reconcile_project_aliases()
        .await
        .expect("reconcile");
    let set = mailbox.project_read_set(SHORT).await;
    assert!(
        set.contains(&legacy_slug.to_string()),
        "legacy path-slug root is aliased read-only: {set:?}"
    );

    // The legacy identity is reachable through the canonical slug: whois via
    // alias-spelling resolution.
    let whois = mailbox
        .whois(SHORT, "RoseDune")
        .await
        .expect("legacy whois via canonical slug");
    assert_eq!(
        whois.npub,
        result.keys.public_key().to_hex(),
        "legacy registration visible read-only"
    );

    // Recovery path (ADR-0017 D3): re-registering under the canonical slug
    // ADOPTS the legacy key (alt-root fallback) — same npub, no second
    // identity; the legacy row stays archived.
    let npub = register(&mailbox, SHORT, "RoseDune").await;
    assert_eq!(
        npub, whois.npub,
        "upsert lands on the legacy identity via key adoption"
    );
    assert_eq!(agent_rows(&pool, SHORT).await, 1, "one canonical row");
    assert_eq!(
        agent_rows(&pool, legacy_slug).await,
        1,
        "legacy row stays read-only, never deleted"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
