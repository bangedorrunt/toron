// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-4 — Reservation conflict + pre-commit guard (P1 / v1 gate).
//!
//! Actors: Pi-α, OC-β.
//! Steps: Pi-α reserves `src/**` exclusive; OC-β checks `src/lib.rs` →
//! conflict (symmetric fnmatch both directions); OC-β's staged `src/lib.rs`
//! is guard-blocked; Pi-α's OWN staged files never blocked; Pi-α releases →
//! OC-β clear.
//!
//! Assert: symmetric both ways; cross-agent blocked; own never blocked;
//! release clears.
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use std::sync::Arc;

use toron_core::bus::InProcessBus;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::guard::{Reservation, guard_blocked_paths};
use toron_core::nostr::identity::AgentProfile;

async fn boot(dir: &std::path::Path) -> (Mailbox, InProcessBus) {
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
        .with_index_pool(pool);
    (mailbox, bus)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register");
}

#[tokio::test]
async fn sc4_reservation_conflict_and_guard() {
    let dir = std::env::temp_dir().join(format!("toron-sc4-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, _bus) = boot(&dir).await;

    register(&mailbox, "demo", "PiAlpha").await;
    register(&mailbox, "demo", "OCBeta").await;

    let acquire = mailbox
        .file_reservation_paths(
            "demo",
            "PiAlpha",
            &["src/**".into()],
            Some(3600),
            true,
            None,
        )
        .await
        .expect("PiAlpha reserve src/**");
    assert_eq!(acquire.conflicts.len(), 0, "first reserve granted");

    let report = mailbox
        .check_file_reservation_conflicts("demo", "OCBeta", &["src/lib.rs".into()])
        .await
        .expect("OCBeta conflict check");
    assert!(!report.conflict_free, "OCBeta blocked by PiAlpha's src/**");
    assert_eq!(report.conflicts.len(), 1);
    assert!(!report.conflicts[0].holders.is_empty());
    assert_eq!(
        report.conflicts[0].reservation_ids.len(),
        acquire.granted.len(),
        "conflict ids parallel the holder list"
    );
    assert_eq!(
        report.conflicts[0].reservation_ids[0], acquire.granted[0].id,
        "conflict id equals PiAlpha's lease event id"
    );

    let other = Reservation {
        holder: "PiAlpha".into(),
        pattern: "src/**".into(),
        exclusive: true,
    };
    let blocked = guard_blocked_paths(
        "OCBeta",
        &["src/lib.rs".into()],
        std::slice::from_ref(&other),
    );
    assert_eq!(
        blocked,
        vec!["src/lib.rs".to_string()],
        "cross-agent blocked"
    );

    let own = guard_blocked_paths(
        "PiAlpha",
        &["src/lib.rs".into()],
        std::slice::from_ref(&other),
    );
    assert!(own.is_empty(), "own reservation never blocks");

    let reversed = guard_blocked_paths("PiAlpha", &["src/lib.rs".into()], &[]);
    assert!(reversed.is_empty());

    let released = mailbox
        .release_file_reservations("demo", "PiAlpha", &[])
        .await
        .expect("PiAlpha release");
    assert!(released.released >= 1, "lease released");

    let clear = mailbox
        .check_file_reservation_conflicts("demo", "OCBeta", &["src/lib.rs".into()])
        .await
        .expect("clear check");
    assert!(clear.conflict_free, "OCBeta clear after release");

    let _ = std::fs::remove_dir_all(dir);
}
