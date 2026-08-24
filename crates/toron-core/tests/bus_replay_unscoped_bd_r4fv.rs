// governed-by: ADR-0003
//! bd-r4fv fence: pin the two-layer replay contract that a cross-project
//! consumer MUST understand.
//!
//! Layer 1 — `BusEventStore::replay`: raw storage read. A kinds-only
//! UNSCOPED filter returns every stored row of that kind regardless of
//! project scoping tags.
//!
//! Layer 2 — `InProcessBus::subscribe` (what `collect_replay` uses): an
//! access-gated view. A GLOBAL subscription (no `#t`/`#l` in the filter,
//! not trusted) never receives project-scoped history — events carrying a
//! project slug are skipped during replay drain (ADR-0003 G6/A3). Only
//! genuinely unscoped rows pass.
//!
//! Consequence: internal consumers that legitimately need cross-project
//! history must either replay per-slug through the scoped path or use
//! `subscribe_trusted`. `Mailbox::active_reservations_escaping_into`
//! (bd-guard-crossproject-lease-er38) does the former.

use toron_core::bus::event_store::BusEventStore;
use toron_core::bus::filter::{Filter, kind_project_filter};
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;
use toron_core::mailbox::collect_replay;
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::{FILE_RESERVATION, toron_kind};

fn lease_event(
    keys: &nostr_sdk::nostr::Keys,
    slug: Option<&str>,
    body: &str,
) -> nostr_sdk::nostr::Event {
    let mut tags = vec![nostr_sdk::nostr::Tag::parse(["d", body]).expect("d")];
    if let Some(slug) = slug {
        tags.push(nostr_sdk::nostr::Tag::parse(["t", slug]).expect("t"));
    }
    build_event(
        keys,
        toron_kind(FILE_RESERVATION),
        body,
        tags,
        nostr_sdk::nostr::Timestamp::now(),
    )
    .expect("build")
}

#[tokio::test]
async fn replay_contract_store_raw_vs_bus_gated_bd_r4fv() {
    let path = std::env::temp_dir().join(format!("toron-r4fv-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    let store = BusEventStore::new(pool.clone()).await.expect("store");
    let keys = nostr_sdk::nostr::Keys::generate();

    for (slug, body) in [
        (Some("alpha"), "lease-a1"),
        (Some("beta"), "lease-b1"),
        // A lease with NO project scoping tags: visible to global subs.
        (None, "lease-orphan"),
    ] {
        let ev = lease_event(&keys, slug, body);
        assert!(store.insert(&ev).await.expect("insert"), "seed {body}");
    }

    let unscoped: Filter = Filter::new().kinds([toron_kind(FILE_RESERVATION)]);

    // Layer 1: raw storage read sees every row.
    assert_eq!(
        store.replay(&unscoped).await.expect("raw replay").len(),
        3,
        "store-level kinds-only replay is unfiltered by project"
    );

    // Layer 2: a GLOBAL subscription receives only the unscoped row.
    let bus = InProcessBus::open(pool.clone(), Default::default())
        .await
        .expect("bus");
    let gated = collect_replay(&bus, unscoped.clone()).await.expect("gated");
    assert_eq!(gated.len(), 1, "global subs get only non-project rows");
    assert_eq!(gated[0].content, "lease-orphan");

    // The scoped path still sees its own subset through the same API.
    let alpha = collect_replay(
        &bus,
        kind_project_filter(toron_kind(FILE_RESERVATION), "alpha"),
    )
    .await
    .expect("alpha");
    assert_eq!(alpha.len(), 1);
    assert_eq!(alpha[0].content, "lease-a1");

    // Trusted ingress bypasses the global gate (system consumers).
    let trusted = bus
        .subscribe_trusted(unscoped.clone())
        .await
        .expect("trusted");
    let trusted_n = collect_trusted_count(trusted, 3).await;
    assert_eq!(trusted_n, 3, "trusted subscription sees full history");

    let _ = std::fs::remove_file(&path);
}

/// Drain a trusted subscription until EOSE, counting events.
async fn collect_trusted_count(
    mut sub: toron_core::bus::subscription::Subscription,
    cap: usize,
) -> usize {
    let mut n = 0;
    while n < cap {
        match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
            Ok(Some(toron_core::bus::SubscriptionEvent::Event(_))) => n += 1,
            _ => break,
        }
    }
    n
}
