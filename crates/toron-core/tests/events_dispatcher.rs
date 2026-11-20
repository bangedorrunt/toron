// governed-by: ADR-0039

//! bd-kz33f U3 integration: an addressed mail on an in-process bus becomes
//! one hint-only mail.addressed row on poll, via one trusted dispatcher.

use std::sync::Arc;

use serde_json::json;
use toron_core::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
use toron_core::bus::Bus;
use toron_core::events::dispatcher::EventDispatcher;
use toron_core::events::store::{SubscribeSpec, SqliteSubscriptionStore, SubscriptionStore};
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::CHANNEL_MESSAGE;
use toron_core::nostr::{Keys, Tag};

fn sub_spec(principal: &str, is_pin: bool, name: &str, args: &serde_json::Value, ttl: Option<i64>, now: i64) -> SubscribeSpec {
    SubscribeSpec { principal: principal.into(), is_pin, delivery: "poll".into(), name: name.into(), args: args.clone(), ttl_us: ttl, now_us: now }
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[tokio::test]
async fn expiry_is_executed_through_the_kernel() {
    use toron_core::events::dispatcher::sweep_expired;

    let path = std::env::temp_dir().join(format!(
        "toron-events-kernel-{}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    let store = SqliteSubscriptionStore::new(pool);
    let sub = store
        .subscribe(&sub_spec("P", true, "mail.addressed", &json!({"project": "f"}), Some(100), 1000))
        .await
        .unwrap();

    assert_eq!(sweep_expired(&store, 1099).await.unwrap(), 0, "not yet expired");
    assert_eq!(sweep_expired(&store, 1100).await.unwrap(), 1, "kernel Terminate at expiry");
    let row = store.get(&sub.id).await.unwrap().unwrap();
    assert_eq!(row.state, "terminated");
    assert_eq!(sweep_expired(&store, 1101).await.unwrap(), 0, "Terminate fires once");
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
        .subscribe(&sub_spec("AmberProbe", true, "mail.addressed", &json!({"project": "flywheel"}), Some(3_600_000_000), now_us()))
        .await
        .unwrap();

    let dispatcher = Arc::new(EventDispatcher::new(bus.clone(), store.clone()));
    let _handle = dispatcher.start().await.expect("dispatcher starts");

    // Two addressed mails for AmberProbe (kind 9 + p tag), plus one to
    // somebody else — only the two own-pin mails may land.
    let keys = Keys::generate();
    for (i, pin) in ["AmberProbe", "AmberProbe", "OtherPin"].into_iter().enumerate() {
        let event = build_event(
            &keys,
            toron_core::nostr::kinds::toron_kind(CHANNEL_MESSAGE),
            format!("mail-{i}"),
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
        if p.events.len() >= 2 {
            page = Some(p);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let page = page.expect("one hint delivered");
    assert_eq!(page.events.len(), 2, "two addressed mails -> two hints");
    let first: serde_json::Value = serde_json::from_str(&page.events[0].payload).unwrap();
    let second: serde_json::Value = serde_json::from_str(&page.events[1].payload).unwrap();
    assert_eq!(first["name"], "mail.addressed");
    assert_ne!(first["event_id"], second["event_id"], "the bus event's unique id (D8 dedupe)");
    assert_eq!(first["project"], "flywheel", "the mail's project, not its subject pin");
    assert!(first.get("body").is_none(), "hint-only payload (R5)");
}
