//! bd-6zo01: SEP-3415 Final conformance fixtures. One fixture per spec
//! message, asserting EXACT wire names. No draft names survive the boundary.

use std::sync::Mutex;

use serde_json::{Value, json};
use toron_core::events::principal::Principal;
use toron_core::events::store::{Delivery, Page, SubscribeSpec, Subscription, SubscriptionStore};
use toron_mcp::events::route_events;

#[derive(Default)]
struct FakeStore {
    subs: Mutex<Vec<Subscription>>,
}

#[async_trait::async_trait]
impl SubscriptionStore for FakeStore {
    async fn subscribe(&self, spec: &SubscribeSpec) -> anyhow::Result<Subscription> {
        let sub = Subscription {
            id: "sub_a3f1c8e2b0d49f7e".into(),
            principal: spec.principal.clone(),
            delivery: spec.delivery.clone(),
            name: spec.name.clone(),
            args_hash: String::new(),
            cursor: 7,
            created_us: spec.now_us,
            expires_us: Some(spec.now_us + 3_600_000_000),
            state: "active".into(),
        };
        self.subs.lock().unwrap().push(sub.clone());
        Ok(sub)
    }
    async fn get(&self, id: &str) -> anyhow::Result<Option<Subscription>> {
        if let Some(s) = self
            .subs
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.id == id)
            .cloned()
        {
            return Ok(Some(s));
        }
        Ok(Some(Subscription {
            id: id.to_string(),
            principal: "operator".into(),
            delivery: "poll".into(),
            name: "mail.addressed".into(),
            args_hash: String::new(),
            cursor: 6,
            created_us: 1_700_000_000_000_000,
            expires_us: None,
            state: "active".into(),
        }))
    }
    async fn list_active(&self) -> anyhow::Result<Vec<Subscription>> {
        Ok(self.subs.lock().unwrap().clone())
    }
    async fn append(&self, _id: &str, _payload: &Value, _now_us: i64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn poll(&self, _id: &str, after: u64, _limit: u32) -> anyhow::Result<Page> {
        Ok(Page {
            events: vec![Delivery {
                seq: after + 1,
                payload: json!({
                    "event_id": "evt_001", "name": "mail.addressed",
                    "project": "flywheel", "sender_pin": "BoldCliff",
                    "timestamp": 1_700_000_000_000_000i64,
                })
                .to_string(),
                created_us: 1_700_000_000_000_000,
            }],
            cursor: after + 1,
            has_more: false,
            truncated: false,
        })
    }
    async fn unsubscribe(&self, _id: &str, _principal: &str) -> anyhow::Result<bool> {
        Ok(true)
    }
    async fn terminate(&self, _id: &str) -> anyhow::Result<()> {
        Ok(())
    }
    async fn prune_deliveries(&self, _now_us: i64) -> anyhow::Result<()> {
        Ok(())
    }
    async fn sweep(&self, _now_us: i64) -> anyhow::Result<usize> {
        Ok(0)
    }
}

async fn call(method: &str, params: Value, store: &FakeStore) -> Value {
    let out = route_events(
        method,
        Some(&params),
        &Principal::Operator,
        Some(store),
        1_700_000_000_000_000,
    )
    .await
    .expect("routed")
    .expect("ok");
    out.0
}

#[tokio::test]
async fn fixture_events_list_carries_result_type_and_final_descriptors() {
    let store = FakeStore::default();
    let out = call("events/list", json!({}), &store).await;
    assert_eq!(out["resultType"], "complete");
    let row = &out["events"][0];
    assert!(row.get("name").is_some() && row.get("inputSchema").is_some());
    assert!(row.get("payloadSchema").is_some());
    let modes = row["delivery"].as_array().unwrap();
    assert!(modes.iter().any(|m| m == "webhook"));
    assert!(modes.iter().any(|m| m == "poll"));
    assert!(
        !modes.iter().any(|m| m == "stream"),
        "Final uses webhook, not stream"
    );
}

#[tokio::test]
async fn fixture_events_poll_occurrence_and_result_shape() {
    let store = FakeStore::default();
    let out = call(
        "events/poll",
        json!({"name": "mail.addressed", "arguments": {}, "subscriptionId": "sub_a3f1c8e2b0d49f7e", "cursor": "6", "maxEvents": 10}),
        &store,
    )
    .await;
    assert_eq!(out["resultType"], "complete");
    assert_eq!(out["hasMore"], false);
    assert_eq!(out["truncated"], false);
    assert!(out["nextPollMs"].is_u64());
    let occ = &out["events"][0];
    for key in ["eventId", "name", "timestamp", "data"] {
        assert!(occ.get(key).is_some(), "occurrence needs {key}: {occ}");
    }
    assert!(!occ.to_string().contains("event_id"), "no draft keys");
    assert!(occ["timestamp"].as_str().is_some(), "timestamp is a string");
    assert!(occ["data"].is_object());
}

#[tokio::test]
async fn fixture_events_subscribe_uses_final_result_keys() {
    let store = FakeStore::default();
    let out = call(
        "events/subscribe",
        json!({"name": "mail.addressed", "arguments": {}, "ttlMs": 3_600_000}),
        &store,
    )
    .await;
    assert_eq!(out["resultType"], "complete");
    assert!(out.get("id").is_some());
    assert!(out.get("refreshBefore").is_some());
    assert!(out.get("cursor").is_some() && out.get("truncated").is_some());
    assert!(
        out.get("secret").is_none(),
        "the server never mints a secret"
    );
    assert!(out.get("expiresAt").is_none(), "Final uses refreshBefore");
}

#[tokio::test]
async fn fixture_events_unsubscribe_result_shape() {
    let store = FakeStore::default();
    let out = call(
        "events/unsubscribe",
        json!({"name": "mail.addressed", "arguments": {}, "delivery": {"url": "https://x/h"}}),
        &store,
    )
    .await;
    assert_eq!(out["resultType"], "complete");
    assert_eq!(out["terminated"], true);
}

#[test]
fn fixture_named_error_codes_are_the_final_numbers() {
    assert_eq!(toron_mcp::events::NOT_FOUND.0, -32023);
    assert_eq!(toron_mcp::events::FORBIDDEN.0, -32024);
    assert_eq!(toron_mcp::events::RESOURCE_EXHAUSTED.0, -32025);
    assert_eq!(toron_mcp::events::UNSUPPORTED.0, -32026);
    assert_eq!(toron_mcp::events::CALLBACK_ENDPOINT_ERROR.0, -32027);
}

#[test]
fn fixture_no_gap_notification_survives_the_boundary() {
    // grep-level guard: the non-spec notification is gone from the crate.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = std::fs::read_to_string(root.join("src/events_stream.rs")).unwrap();
    assert!(!src.contains("notifications/events/gap"), "G4: gap removed");
}

#[test]
fn fixture_webhook_header_and_occurrence_shape() {
    let hint =
        json!({"event_id": "evt_789", "name": "mail.addressed", "timestamp": 1739980800000000i64})
            .to_string();
    let occ = toron_core::events::occurrence::occurrence(&hint, 3, 0);
    assert_eq!(occ["eventId"], "evt_789");
    assert_eq!(occ["name"], "mail.addressed");
    assert_eq!(occ["data"], json!({}));
    let hdr = toron_core::events::webhook::Secret::from_client_whsec(
        "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw",
    );
    assert!(hdr.is_ok(), "Final secret shape accepted");
}
