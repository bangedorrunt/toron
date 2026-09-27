// governed-by: ADR-0003
//! Leak proof: a dropped `Subscription` must release its bus registration.
//!
//! `InProcessBus::subs` is the only live-subscriber registry. An entry is
//! inserted in `subscribe_with_gate` and the ONLY removal path is
//! `fanout_to`, which fires on `TrySendError::Closed` — i.e. it needs a
//! publish to reach that subscriber. But `fanout` gates delivery on
//! `access_gate`, and a GLOBAL subscription (filter with no `#t`/`#l`, not
//! trusted) is refused every project-scoped event. Since toron stamps `#t` on
//! essentially every stored event, a global subscriber is never fanned to, so
//! its closed channel is never observed and the entry is never removed.
//!
//! Consequence: every `collect_replay` / MCP resource read that subscribes
//! with an unscoped filter leaks one `Arc<Subscriber>` plus its mpsc block
//! (capacity 1024) for the life of the daemon. Observed live at ~62/sec.
//!
//! This test asserts the correct behavior (count returns to zero). It is
//! expected to FAIL against the current code; that failure is the evidence.

use toron_core::bus::filter::Filter;
use toron_core::bus::in_process::InProcessBusConfig;
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::{FILE_RESERVATION, toron_kind};

fn scoped_lease(
    keys: &toron_core::nostr::Keys,
    slug: &str,
    body: &str,
) -> toron_core::nostr::Event {
    let tags = vec![
        toron_core::nostr::Tag::parse(["d", body]).expect("d"),
        toron_core::nostr::Tag::parse(["t", slug]).expect("t"),
    ];
    build_event(
        keys,
        toron_kind(FILE_RESERVATION),
        body,
        tags,
        toron_core::nostr::Timestamp::now(),
    )
    .expect("build")
}

async fn bus() -> InProcessBus {
    let path = std::env::temp_dir().join(format!("toron-subleak-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    InProcessBus::open(pool, InProcessBusConfig::default())
        .await
        .expect("bus")
}

#[tokio::test]
async fn dropped_global_subscription_is_released() {
    let bus = bus().await;
    let keys = toron_core::nostr::Keys::generate();

    // Seed one project-scoped event, so `project_slug(event).is_some()` holds
    // and the access gate is actually exercised.
    bus.publish(scoped_lease(&keys, "alpha", "seed"))
        .await
        .expect("seed publish");

    assert_eq!(bus.subscriber_count(), 0, "clean start");

    // Unscoped filter => global subscriber. This is the shape every
    // `collect_replay` and MCP resource read uses.
    let global = Filter::new().kinds([toron_kind(FILE_RESERVATION)]);
    for _ in 0..10 {
        let sub = bus.subscribe(global.clone()).await.expect("subscribe");
        drop(sub); // caller is done with it; receiver is closed
    }

    assert_eq!(
        bus.subscriber_count(),
        0,
        "dropped subscriptions must not stay registered (10 subscribed+dropped)"
    );
}

#[tokio::test]
async fn dropped_scoped_subscription_is_released() {
    let bus = bus().await;
    let keys = toron_core::nostr::Keys::generate();

    // Scoped filter => NOT global, so a matching publish reaps it via
    // TrySendError::Closed. This is the control case that shows the removal
    // path works when access_gate lets a publish through.
    let scoped =
        toron_core::bus::filter::kind_project_filter(toron_kind(FILE_RESERVATION), "alpha");
    for _ in 0..10 {
        let sub = bus.subscribe(scoped.clone()).await.expect("subscribe");
        drop(sub);
        bus.publish(scoped_lease(&keys, "alpha", "reap"))
            .await
            .expect("reap publish");
    }

    assert_eq!(
        bus.subscriber_count(),
        0,
        "scoped dropped subscriptions are reaped by the next matching publish"
    );
}
