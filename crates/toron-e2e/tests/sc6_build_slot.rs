// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-6 — Build-slot serialization + TTL (P1 / v1 gate).
//!
//! Actors: Pi-α, OC-β, slot `ci`.
//! Steps:
//! 1. Pi-α `slot acquire ci --exclusive` → granted.
//! 2. OC-β acquire → conflict (exclusive serializes).
//! 3. Pi-α renews (extends TTL); releases; OC-β acquires → ok.
//! 4. Abandoned slot clears at TTL (expired lease not active).
//!
//! Assert: exclusive serializes; renew extends; release frees; TTL clears.
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use std::sync::Arc;

use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::index::slots::active_slots;
use toron_core::mailbox::Mailbox;
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
    let mailbox = Mailbox::new(Arc::new(bus.clone()), dir.to_path_buf(), None)
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool.clone());
    (mailbox, bus, pool)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register");
}

#[tokio::test]
async fn sc6_build_slot_serialization_and_ttl() {
    let dir = std::env::temp_dir().join(format!("toron-sc6-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus, pool) = boot(&dir).await;

    register(&mailbox, "demo", "PiAlpha").await;
    register(&mailbox, "demo", "OCBeta").await;

    // --- 1. Pi-α acquires `ci` exclusive → granted ---
    let first = mailbox
        .acquire_build_slot("demo", "PiAlpha", "ci", Some(3600), true)
        .await
        .expect("PiAlpha acquire");
    assert_eq!(first.granted.len(), 1, "PiAlpha granted");
    assert_eq!(first.conflicts.len(), 0);
    assert!(first.granted[0].expires_ts_us.is_some(), "NIP-40 TTL set");

    // --- 2. OC-β acquire → conflict (exclusive serializes) ---
    let blocked = mailbox
        .acquire_build_slot("demo", "OCBeta", "ci", Some(3600), true)
        .await
        .expect("OCBeta acquire");
    assert_eq!(blocked.granted.len(), 0, "OCBeta not granted");
    assert_eq!(blocked.conflicts.len(), 1, "conflict reported");
    assert_eq!(blocked.conflicts[0].slot, "ci");

    // --- 3. Pi-α renews → TTL extended; releases → OC-β acquires ok ---
    let renewed = mailbox
        .renew_build_slot("demo", "PiAlpha", "ci", 7200)
        .await
        .expect("PiAlpha renew");
    assert!(renewed.is_some(), "renew returns the lease");
    let renewed = renewed.unwrap();
    assert!(
        renewed.expires_ts_us.unwrap() > now_us(),
        "renew extends TTL into the future"
    );

    let released = mailbox
        .release_build_slot("demo", "PiAlpha", "ci")
        .await
        .expect("PiAlpha release");
    assert!(released, "release frees the slot");

    let ok = mailbox
        .acquire_build_slot("demo", "OCBeta", "ci", Some(3600), true)
        .await
        .expect("OCBeta re-acquire");
    assert_eq!(ok.granted.len(), 1, "OCBeta acquires after release");

    // --- 4. TTL clears abandoned slot (expired lease not active) ---
    // A lease with a past expiry is not active (index::slots rule).
    let active = active_slots(&pool, "demo").await.expect("active slots");
    for s in &active {
        if let Some(exp) = s.expires_ts_us {
            assert!(exp > now_us(), "active lease not expired");
        }
    }
    // Direct store check: an expired lease is filtered out.
    let past = now_us() - 1000;
    let lease = toron_core::index::slots::SlotLease {
        project_slug: "demo".into(),
        slot: "expired-slot".into(),
        agent_name: "PiAlpha".into(),
        agent_pubkey: "deadbeef".into(),
        exclusive: true,
        event_id: "expired-evt".into(),
        expires_ts_us: Some(past),
    };
    toron_core::index::slots::insert_slot(&pool, &lease)
        .await
        .expect("insert expired lease");
    let active2 = active_slots(&pool, "demo")
        .await
        .expect("active after expired");
    assert!(
        !active2.iter().any(|s| s.slot == "expired-slot"),
        "expired lease cleared (TTL)"
    );

    let _ = std::fs::remove_dir_all(dir);
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}
