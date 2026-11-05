// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-14b — Negentropy delta rejoin (federation DR) [P1/P2].
//!
//! Contract: lose bus state → rejoin via NIP-77 negentropy from an in-process
//! second store → only missing events transferred → consistent. Signed events
//! re-verified on rejoin. No real network (two in-process `BusEventStore`s
//! sharing the same event universe).
//!
//! Gate: `cargo test -p toron-e2e --features p2` (or `federation`).
#![cfg(feature = "p2")]

use nostr_sdk::prelude::*;
use toron_core::bus::event_store::BusEventStore;
use toron_core::index::schema;

async fn temp_store(tag: &str) -> (BusEventStore, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("toron-sc14b-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("store.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    let store = BusEventStore::new(pool).await.expect("store");
    (store, dir)
}

#[tokio::test]
async fn sc14b_negentropy_rejoin_transfers_only_missing() {
    let (relay_store, _relay_dir) = temp_store("relay").await;
    let (node_store, node_dir) = temp_store("node").await;

    let key = Keys::generate();
    let mut events = Vec::new();
    for i in 0..5 {
        let evt = EventBuilder::new(Kind::TextNote, format!("msg-{i}"))
            .finalize(&key)
            .expect("sign");
        assert!(relay_store.insert(&evt).await.expect("relay insert"));
        events.push(evt);
    }

    for evt in &events[..2] {
        assert!(node_store.insert(evt).await.expect("node insert"));
    }

    let stats = node_store
        .sync_delta(&relay_store)
        .await
        .expect("delta sync");
    assert_eq!(stats.synced, 3, "only the missing 3 transferred");
    assert!(
        stats.used_negentropy,
        "negentropy path used (not full replay)"
    );

    assert_eq!(node_store.count().await.expect("count"), 5);

    let replayed = relay_store
        .replay(&toron_core::bus::Filter::new().kinds([Kind::TextNote]))
        .await
        .expect("relay replay");
    assert_eq!(replayed.len(), 5, "relay universe intact");
    for evt in replayed {
        toron_core::nostr::event::verify_event(&evt).expect("re-verified signature");
        assert!(evt.content.starts_with("msg-"), "content intact");
    }

    let _ = std::fs::remove_dir_all(node_dir);
}

#[tokio::test]
async fn sc14b_no_delta_when_consistent() {
    let (relay_store, _relay_dir) = temp_store("relay2").await;
    let (node_store, node_dir) = temp_store("node2").await;

    let key = Keys::generate();
    for i in 0..4 {
        let evt = EventBuilder::new(Kind::TextNote, format!("n-{i}"))
            .finalize(&key)
            .expect("sign");
        relay_store.insert(&evt).await.expect("relay");
        node_store.insert(&evt).await.expect("node");
    }

    let stats = node_store
        .sync_delta(&relay_store)
        .await
        .expect("delta sync");
    assert_eq!(stats.synced, 0, "no missing events");
    assert_eq!(node_store.count().await.expect("count"), 4);

    let _ = std::fs::remove_dir_all(node_dir);
}
