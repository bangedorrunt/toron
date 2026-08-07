// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-15 — Ephemeral presence 20001 (Phase-2.3 polish, NOT v1).
//!
//! Assert:
//! 1. Agents broadcast kind-20001 working-state heartbeats on the bus.
//! 2. Presence is NEVER stored in Git/SQLite (ephemeral class 20000-29999).
//! 3. Presence expires when the heartbeat stops (TTL staleness).
//!
//! Gate: `cargo test -p toron-e2e --features p2`.
#![cfg(feature = "p2")]

use std::sync::Arc;
use std::time::Duration;

use nostr_sdk::prelude::*;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::presence::PresenceBroker;
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;
use toron_core::nostr::presence::{PresencePayload, PresenceState, build_presence_event};

async fn boot(dir: &std::path::Path) -> (InProcessBus, sqlx::SqlitePool) {
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
    (bus, pool)
}

#[tokio::test]
async fn sc15_ephemeral_presence_not_archived() {
    let dir = std::env::temp_dir().join(format!("toron-sc15-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (bus, _pool) = boot(&dir).await;

    let ttl = Duration::from_secs(30);
    let broker = PresenceBroker::start(Arc::new(bus.clone()), ttl);

    // Let the broker's subscription register before publishing (replay→EOSE→
    // live delivery needs the sub to be active).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- 1. Agents broadcast kind-20001 heartbeats ---
    let pi = Keys::generate();
    let oc = Keys::generate();
    let t0 = 1_700_000_000u64; // controlled heartbeat ts

    for (keys, state, status) in [
        (&pi, PresenceState::Working, Some("reviewing".to_string())),
        (
            &oc,
            PresenceState::Blocked,
            Some("awaiting approval".to_string()),
        ),
    ] {
        let evt = build_presence_event(
            keys,
            &PresencePayload {
                state,
                status,
                ts: t0,
            },
            Timestamp::from(t0),
        )
        .expect("build presence event");
        assert_eq!(evt.kind.as_u16(), 20001, "kind-20001");
        let ok = bus.publish(evt).await.expect("publish presence");
        // --- 2. Ephemeral: NOT stored (no SQLite projection) ---
        assert!(!ok.stored, "ephemeral presence must NOT be stored (SC-15)");
    }

    // Broker picks up both heartbeats (drain replay/EOSE → live).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let live = broker.live_agents(t0);
    assert_eq!(live.len(), 2, "both agents present");
    assert!(
        broker.current(&pi.public_key(), t0).is_some(),
        "PiAlpha present"
    );
    assert!(
        broker.current(&oc.public_key(), t0).is_some(),
        "OCBeta present"
    );

    // Presence state carried through the broker.
    let pi_state = broker.current(&pi.public_key(), t0).unwrap();
    assert_eq!(pi_state.state, PresenceState::Working);
    assert_eq!(pi_state.status.as_deref(), Some("reviewing"));

    // --- 3. Expires when heartbeat stops: past TTL → stale/excluded ---
    let after_ttl = t0 + ttl.as_secs() + 1;
    assert!(
        broker.current(&pi.public_key(), after_ttl).is_none(),
        "stale excluded"
    );
    let live_after = broker.live_agents(after_ttl);
    assert!(live_after.is_empty(), "no live agents after TTL");

    // GC: expire_stale removes them from the map.
    broker.expire_stale(after_ttl);
    assert!(broker.live_agents(after_ttl).is_empty(), "GC'd");

    broker.shutdown();
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn sc15_presence_not_in_sqlite_index() {
    // Presence events never write to the messages/index tables.
    let dir = std::env::temp_dir().join(format!("toron-sc15b-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (bus, pool) = boot(&dir).await;

    let agent = Keys::generate();
    let evt = build_presence_event(
        &agent,
        &PresencePayload {
            state: PresenceState::Idle,
            status: None,
            ts: 1_700_000_000,
        },
        Timestamp::from(1_700_000_000),
    )
    .expect("build presence");
    bus.publish(evt).await.expect("publish");

    // No rows in any indexed table (messages, bus_events store skips ephemeral).
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM messages")
        .fetch_one(&pool)
        .await
        .expect("messages count");
    assert_eq!(n, 0, "no presence in messages table");

    let _ = std::fs::remove_dir_all(dir);
}
