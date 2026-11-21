// governed-by: ADR-0003
//! bd-ll7bs: a blocked acquire queues; a freed path hands over FIFO.
//!
//! Hermetic fixture: temp sqlite + in-process bus, no live daemon, no :18080,
//! no operator config. The handoff is the same atomic transfer the CLI uses.

use std::path::PathBuf;
use std::sync::Arc;

use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;

const PROJECT: &str = "waitlist-fixture";
const PATH: &str = "src/lib.rs";

async fn test_mailbox(tag: &str) -> (Mailbox, PathBuf, PathBuf) {
    let db_path = std::env::temp_dir().join(format!(
        "toron-waitlist-cli-{tag}-{}-{}.sqlite",
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
        "toron-waitlist-cli-data-{tag}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir_all(&data_dir).expect("mkdir");
    let mailbox = Mailbox::new(
        Arc::new(bus),
        data_dir.clone(),
        Some(toron_core::keys::generate()),
    )
    .with_agent_passphrase("reserve-waitlist-test")
    .with_index_pool(pool);
    (mailbox, db_path, data_dir)
}

#[tokio::test]
async fn expiry_hands_over_without_any_release_call() {
    let (mailbox, _db, _data) = test_mailbox("expiry").await;
    for name in ["ShortHolder", "ExpiryWaiter"] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register");
    }
    let paths = vec![PATH.to_string()];
    mailbox
        .file_reservation_paths(PROJECT, "ShortHolder", &paths, Some(1), true, Some("ttl"))
        .await
        .expect("short lease");
    let queued = mailbox
        .file_reservation_paths_queued(PROJECT, "ExpiryWaiter", &paths, Some(3600), true, None)
        .await
        .expect("queue");
    assert!(!queued.conflicts.is_empty());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    // No release is called: the lease-board read that sees the expired lease
    // must run the handoff itself.
    let _ = mailbox.active_reservations(PROJECT).await.expect("read");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("ExpiryWaiter"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "expiry handed the path to the waiter");
    assert!(
        mailbox
            .list_waiters(PROJECT, PATH)
            .await
            .expect("queue")
            .is_empty()
    );
}

#[tokio::test]
async fn queued_acquires_hand_off_fifo_on_every_release() {
    let (mailbox, _db, _data) = test_mailbox("fifo").await;
    for name in ["Holder", "WaiterA", "WaiterB"] {
        mailbox
            .register_agent(PROJECT, &AgentProfile::new(name))
            .await
            .expect("register");
    }

    let paths = vec![PATH.to_string()];
    mailbox
        .file_reservation_paths(PROJECT, "Holder", &paths, Some(3600), true, Some("lane"))
        .await
        .expect("holder acquires");

    for waiter in ["WaiterA", "WaiterB"] {
        let out = mailbox
            .file_reservation_paths_queued(PROJECT, waiter, &paths, Some(3600), true, Some("lane"))
            .await
            .expect("queued acquire");
        assert_eq!(out.granted.len(), 0, "{waiter} cannot take a held path");
        assert_eq!(out.conflicts.len(), 1, "{waiter} sees the hard conflict");
    }

    let queue = mailbox.list_waiters(PROJECT, PATH).await.expect("queue");
    assert_eq!(
        queue
            .iter()
            .map(|w| w.agent_name.as_str())
            .collect::<Vec<_>>(),
        vec!["WaiterA", "WaiterB"],
        "FIFO arrival order"
    );

    mailbox
        .release_file_reservations(PROJECT, "Holder", &paths)
        .await
        .expect("release");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("WaiterA"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "the head waiter now holds the path");
    let remaining = mailbox.list_waiters(PROJECT, PATH).await.expect("queue");
    assert_eq!(remaining.len(), 1, "exactly one waiter was consumed");
    assert_eq!(remaining[0].agent_name, "WaiterB");

    // TTL expiry and retire go through the same helper; a second release with
    // no waiters must not invent a grant.
    mailbox
        .release_file_reservations(PROJECT, "WaiterA", &paths)
        .await
        .expect("release");
    let holders = mailbox
        .list_file_reservations(PROJECT, Some("WaiterB"), None)
        .await
        .expect("board");
    assert_eq!(holders.len(), 1, "the second waiter holds the path");
    assert!(
        mailbox
            .list_waiters(PROJECT, PATH)
            .await
            .expect("queue")
            .is_empty()
    );
}
