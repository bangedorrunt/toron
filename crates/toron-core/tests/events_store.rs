// governed-by: ADR-0039

//! bd-kz33f U3: durable subscriptions, AE2 identity, TTL caps, cursor poll.

use serde_json::json;
use toron_core::events::store::{
    MAX_PIN_TTL_US, SqliteSubscriptionStore, SubscriptionStore, args_hash, canonical_json,
};

async fn store(tag: &str) -> (SqliteSubscriptionStore, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "toron-events-store-{}-{tag}.sqlite",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    toron_core::index::schema::migrate(&pool).await.expect("migrate");
    (SqliteSubscriptionStore::new(pool), path)
}

#[test]
fn canonical_json_sorts_object_keys() {
    assert_eq!(
        canonical_json(&json!({"b": 1, "a": {"d": 2, "c": 3}})),
        r#"{"a":{"c":3,"d":2},"b":1}"#
    );
    assert_eq!(args_hash(&json!({"a": 1, "b": 2})), args_hash(&json!({"b": 2, "a": 1})));
}

#[tokio::test]
async fn ae2_identical_resubscribe_keeps_id_and_extends_expiry() {
    let (store, _path) = store("ae2").await;
    let args = json!({"project": "flywheel"});
    let first = store
        .subscribe("P", true, "poll", "mail.addressed", &args, Some(1000), 100)
        .await
        .unwrap();
    let second = store
        .subscribe("P", true, "poll", "mail.addressed", &args, Some(1000), 500)
        .await
        .unwrap();
    assert_eq!(first.id, second.id, "identical identity keeps the same id");
    assert_eq!(second.expires_us, Some(1500), "expiry extends from the new now");
    let different = store
        .subscribe("P", true, "poll", "mail.addressed", &json!({"project": "other"}), Some(1000), 100)
        .await
        .unwrap();
    assert_ne!(different.id, first.id, "different args create a distinct subscription");
}

#[tokio::test]
async fn a_pin_cannot_request_no_expiry_and_48h_is_capped() {
    let (store, _path) = store("ttl").await;
    let args = json!({"project": "f"});
    assert!(store
        .subscribe("P", true, "poll", "n", &args, None, 0)
        .await
        .is_err(), "a pin requesting no expiry is denied");
    let capped = store
        .subscribe("P", true, "poll", "n", &args, Some(48 * 3600 * 1_000_000), 0)
        .await
        .unwrap();
    assert_eq!(capped.expires_us, Some(MAX_PIN_TTL_US));
    let operator = store
        .subscribe("op", false, "poll", "n", &args, None, 0)
        .await
        .unwrap();
    assert_eq!(operator.expires_us, None, "the operator may have no expiry");
}

#[tokio::test]
async fn cursor_poll_returns_events_after_the_cursor_and_persists() {
    let (store, path) = store("cursor").await;
    let args = json!({"project": "f"});
    let sub = store
        .subscribe("op", false, "poll", "mail.addressed", &args, None, 0)
        .await
        .unwrap();
    for i in 0..3 {
        store
            .append(&sub.id, &json!({"event_id": format!("e{i}"), "name": "mail.addressed"}), 100)
            .await
            .unwrap();
    }
    let page = store.poll(&sub.id, 0, 2).await.unwrap();
    assert_eq!(page.events.len(), 2);
    assert!(page.has_more);
    let page2 = store.poll(&sub.id, page.cursor, 2).await.unwrap();
    assert_eq!(page2.events.len(), 1);
    assert!(!page2.has_more);

    // Reopen the store from the same file: subscription + cursor persist.
    let pool = toron_core::index::schema::connect(path.to_str().unwrap())
        .await
        .unwrap();
    toron_core::index::schema::migrate(&pool).await.unwrap();
    let reopened = SqliteSubscriptionStore::new(pool);
    let row = reopened.get(&sub.id).await.unwrap().expect("survives restart");
    assert_eq!(row.id, sub.id);
    assert_eq!(reopened.poll(&sub.id, 0, 10).await.unwrap().events.len(), 3);
}

#[tokio::test]
async fn a_cursor_that_fell_off_the_bounded_table_reports_truncated() {
    use toron_core::events::store::{MAX_DELIVERIES_PER_SUB, DELIVERY_TTL_US};

    let (store, _path) = store("gap").await;
    let args = json!({"project": "f"});
    let sub = store
        .subscribe("op", false, "poll", "n", &args, None, 0)
        .await
        .unwrap();
    // Age out the earliest rows so the table's oldest seq advances.
    for i in 0..5 {
        store
            .append(&sub.id, &json!({"i": i}), i * DELIVERY_TTL_US + 1000)
            .await
            .unwrap();
    }
    // Only the newest row survives the age prune.
    let page = store.poll(&sub.id, 0, 10).await.unwrap();
    assert!(page.truncated, "a pruned cursor is reported, not silently skipped");
    assert!(!page.events.is_empty(), "the fresh cursor still serves rows");
    assert!(page.cursor > 0, "a fresh servable cursor comes back");
    let _ = MAX_DELIVERIES_PER_SUB;
}

#[tokio::test]
async fn sweep_terminates_expired_and_unsubscribe_is_owned_and_idempotent() {
    let (store, _path) = store("sweep").await;
    let args = json!({"project": "f"});
    let sub = store
        .subscribe("P", true, "poll", "n", &args, Some(100), 0)
        .await
        .unwrap();
    assert_eq!(store.sweep(99).await.unwrap(), 0);
    assert_eq!(store.sweep(100).await.unwrap(), 1);
    assert_eq!(store.get(&sub.id).await.unwrap().unwrap().state, "terminated");
    assert!(store.unsubscribe("unknown", "P").await.unwrap(), "unknown id idempotent");
    assert!(!store.unsubscribe(&sub.id, "other").await.unwrap(), "another principal denied");
}
