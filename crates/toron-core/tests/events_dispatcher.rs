// governed-by: ADR-0039

//! bd-kz33f U3 integration: an addressed mail on an in-process bus becomes
//! one hint-only mail.addressed row on poll, via one trusted dispatcher.

use std::sync::Arc;

use serde_json::json;
use toron_core::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
use toron_core::bus::Bus;
use toron_core::events::dispatcher::EventDispatcher;
use toron_core::events::store::{SqliteSubscriptionStore, SubscriptionStore};
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::CHANNEL_MESSAGE;
use toron_core::nostr::{Keys, Tag};

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[tokio::test]
async fn addressed_mail_on_bus_becomes_one_hint_on_poll() {
    let path = std::env::temp_dir().join(format!(
        "toron-events-dispatch-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    let bus: Arc<dyn Bus> = Arc::new(
        InProcessBus::open(
            pool.clone(),
            InProcessBusConfig {
                auth: AuthPolicy::AcceptAll,
                allowlist: Vec::new(),
                channel_capacity: 128,
                detailed_stage_logging: false,
            },
        )
        .await
        .unwrap(),
    );
    let store: Arc<dyn SubscriptionStore> =
        Arc::new(SqliteSubscriptionStore::new(pool.clone()));

    let sub = store
        .subscribe("AmberProbe", true, "poll", "mail.addressed", &json!({"project": "flywheel"}), Some(3_600_000_000), now_us())
        .await
        .unwrap();

    let dispatcher = Arc::new(EventDispatcher::new(bus.clone(), store.clone()));
    let _handle = dispatcher.start().await.expect("dispatcher starts");

    // One addressed mail for AmberProbe (kind 9 + p tag), plus one addressed
    // to somebody else — only the first may land in this subscription.
    let keys = Keys::generate();
    for pin in ["AmberProbe", "OtherPin"] {
        let event = build_event(
            &keys,
            toron_core::nostr::kinds::toron_kind(CHANNEL_MESSAGE),
            "hello",
            vec![
                Tag::parse(["p", pin]).unwrap(),
                Tag::parse(["t", "flywheel"]).unwrap(),
            ],
            toron_core::nostr::Timestamp::now(),
        )
        .unwrap();
        bus.publish(event).await.expect("publish");
    }

    // Bounded wait for the dispatcher to classify and append.
    let mut page = None;
    for _ in 0..20 {
        let p = store.poll(&sub.id, 0, 10).await.unwrap();
        if !p.events.is_empty() {
            page = Some(p);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let page = page.expect("one hint delivered");
    assert_eq!(page.events.len(), 1, "exactly one addressed mail hint");
    let hint: serde_json::Value = serde_json::from_str(&page.events[0].payload).unwrap();
    assert_eq!(hint["name"], "mail.addressed");
    assert!(hint.get("body").is_none(), "hint-only payload (R5)");
}
