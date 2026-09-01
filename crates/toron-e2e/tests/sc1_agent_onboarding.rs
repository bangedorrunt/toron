// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-1 — Agent onboarding (macro_start_session parity) [P1 / v1 gate].
//!
//! Actors: operator, Pi-α, project `demo`.
//! Steps:
//! 1. `project init demo` (idempotent project root + bus).
//! 2. `agent register PiAlpha --program pi --project demo` → kind-30013 profile.
//! 3. Pi-α reserves `src/**` → kind-30031.
//! 4. `mail inbox` empty (nothing sent yet).
//!
//! Assert:
//! - kind-30013 signed profile published (whois resolves name → npub).
//! - kind-30031 live for the reservation.
//! - `list_agents` shows Pi-α.
//! - re-register is idempotent (same npub, no duplicate).
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use std::sync::Arc;

use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::{FetchInbox, ListAgentsOptions, Mailbox, WhoIs};
use toron_core::nostr::identity::AgentProfile;

async fn boot_bus(pool: sqlx::SqlitePool) -> InProcessBus {
    InProcessBus::open(
        pool.clone(),
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus")
}

#[tokio::test]
async fn sc1_agent_onboarding() {
    let dir = std::env::temp_dir().join(format!("toron-sc1-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");
    let bus = boot_bus(pool.clone()).await;

    let mailbox = Mailbox::new(Arc::new(bus), dir.clone(), None)
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool);

    // --- Step 2: agent register PiAlpha --program pi --project demo ---
    let profile = AgentProfile {
        name: "PiAlpha".into(),
        program: Some("pi".into()),
        model: None,
        task_description: None,
        persona: None,
    };
    let first = mailbox
        .register_agent("demo", &profile)
        .await
        .expect("register PiAlpha");

    // --- Assert: kind-30013 profile published; name → npub resolves ---
    let whois: WhoIs = mailbox.whois("demo", "PiAlpha").await.expect("whois");
    assert_eq!(whois.npub, first.npub, "whois resolves registered npub");
    assert_eq!(whois.program.as_deref(), Some("pi"));

    // --- Assert: list_agents shows Pi-α ---
    let agents = mailbox
        .list_agents(
            "demo",
            ListAgentsOptions {
                active_within_days: None,
            },
        )
        .await
        .expect("list agents");
    assert!(
        agents
            .iter()
            .any(|a| a.name == "PiAlpha" && a.npub == first.npub),
        "PiAlpha listed"
    );

    // --- Step 3: Pi-α reserves src/** (kind-30031) ---
    let res = mailbox
        .file_reservation_paths(
            "demo",
            "PiAlpha",
            &["src/**".into()],
            Some(3600),
            true,
            Some("onboarding"),
        )
        .await
        .expect("reserve src/**");
    assert_eq!(res.conflicts.len(), 0, "no conflicts on first reserve");
    assert_eq!(res.granted.len(), 1);
    assert!(res.granted[0].expires_ts_us.is_some(), "TTL set (NIP-40)");

    // --- Step 4: mail inbox is empty (nothing sent) ---
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: "demo".into(),
            agent_name: "PiAlpha".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
            urgent_only: false,
            since_ts: None,
            thread_id: None,
        })
        .await
        .expect("fetch inbox");
    assert!(inbox.is_empty(), "no mail yet");

    // --- Assert: re-register is idempotent (same npub, no duplicate) ---
    let again = mailbox
        .register_agent("demo", &profile)
        .await
        .expect("re-register PiAlpha");
    assert_eq!(again.npub, first.npub, "idempotent: same npub");
    let agents2 = mailbox
        .list_agents(
            "demo",
            ListAgentsOptions {
                active_within_days: None,
            },
        )
        .await
        .expect("list agents again");
    let pi_entries: Vec<&WhoIs> = agents2.iter().filter(|a| a.name == "PiAlpha").collect();
    assert_eq!(pi_entries.len(), 1, "no duplicate profile entries");

    let _ = std::fs::remove_dir_all(dir);
}
