// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Smoke test: daemon boots + `health_check` equivalent + send/subscribe
//! round-trip. Acceptance gate for the harness skeleton.

use nostr_sdk::prelude::*;
use toron_core::bus::{Filter, SubscriptionEvent};
use toron_e2e::harness::TestDaemon;

#[tokio::test]
async fn daemon_boots_and_health_is_ok() {
    let daemon = TestDaemon::boot().await.expect("boot daemon");
    assert!(daemon.health().await.expect("health"), "bus healthy");
    assert!(daemon.clock.now_us() > 0, "virtual clock initialized");
    daemon.cleanup();
}

#[tokio::test]
async fn publish_subscribe_roundtrip_via_bus() {
    let daemon = TestDaemon::boot().await.expect("boot daemon");
    let key = Keys::generate();

    // Kind-14 project event; send on the bus, receive via subscription.
    let event = EventBuilder::new(Kind::TextNote, "hello e2e")
        .tags([Tag::parse(["t", "demo"]).expect("tag")])
        .finalize(&key)
        .expect("sign");

    daemon.send_event(&event).await.expect("publish");

    let sub = daemon
        .subscribe(
            Filter::new()
                .authors([key.public_key()])
                .kinds([Kind::TextNote])
                .hashtag("demo"),
        )
        .await
        .expect("subscribe");
    let events = daemon.collect_until_eose(sub, 5).await;
    assert_eq!(events.len(), 1, "one event delivered");
    assert_eq!(events[0].content, "hello e2e");

    daemon.cleanup();
}

#[tokio::test]
async fn closed_subscription_terminates_collect() {
    let daemon = TestDaemon::boot().await.expect("boot daemon");
    // A subscription for a kind that never arrives; EOSE/closed terminates
    // collect_until_eose without hanging.
    let sub = daemon
        .subscribe(Filter::new().kinds([Kind::from(30010)]))
        .await
        .expect("subscribe");
    let events = daemon.collect_until_eose(sub, 5).await;
    assert!(events.is_empty(), "no events for empty filter");
    let _ = SubscriptionEvent::Eose; // silence unused if shape changes
    daemon.cleanup();
}
