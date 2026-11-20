// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! U3 producer: ONE trusted bus subscription classified for every active
//! subscription, plus the 60s sweep tick (ADR-0039 D3, bd-kz33f). No second
//! bus and no SQLite polling: facts arrive on the bus we already own.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use crate::nostr::{Event, Filter};

use std::sync::atomic::{AtomicU64, Ordering};

use super::catalogue::{Fact, classify};
use super::kernel::{KernelMsg, SubscriptionModel, journal_then_run};
use super::store::SubscriptionStore;
use crate::bus::{Bus, SubscriptionEvent};

static GAP_SEQ: AtomicU64 = AtomicU64::new(0);

/// One gap hint per active subscription when facts were shed or a store error
/// dropped a dispatch. Never silently lose facts (ADR-0039 D5, bd-sptj4).
pub async fn append_gap(store: &dyn SubscriptionStore, now_us: i64) {
    let Ok(subs) = store.list_active().await else {
        return;
    };
    let event_id = format!("gap:{}:{}", now_us, GAP_SEQ.fetch_add(1, Ordering::Relaxed));
    for sub in subs {
        let hint = json!({
            "event_id": event_id,
            "name": "gap",
            "project": "",
            "sender_pin": sub.principal,
            "timestamp": now_us,
        });
        let _ = store.append(&sub.id, &hint, now_us).await;
    }
}

/// Expiry through the pure kernel: tick each subscription, journal Terminate,
/// then execute it against the store (effect law). The clock is read once by
/// the caller and passed in.
pub async fn sweep_expired(store: &dyn SubscriptionStore, now_us: i64) -> anyhow::Result<usize> {
    let mut terminated = 0;
    for sub in store.list_active().await? {
        let model = SubscriptionModel {
            id: sub.id.clone(),
            expires_us: sub.expires_us,
            state: sub.state.clone(),
        };
        let next = journal_then_run(&model, &KernelMsg::Clock { now_us }, |effect| {
            tracing::info!("events effect journal sub={} {effect:?}", model.id);
        });
        if next.state == "terminated" && model.state != "terminated" {
            store.terminate(&sub.id).await?;
            terminated += 1;
        }
    }
    store.prune_deliveries(now_us).await?;
    Ok(terminated)
}

/// Read the fact a catalogue classifier needs from a bus event. `p` carries
/// the subject pin (an npub in production; the classifier compares it to the
/// subscriber's configured pin), `toron-kind` the semantic sub-kind.
pub fn fact_from_event(event: &Event) -> Fact {
    let tag = |name: &str| {
        event.tags.iter().find_map(|t| {
            let parts = t.as_slice();
            if parts.first().map(String::as_str) == Some(name) && parts.len() >= 2 {
                Some(parts[1].clone())
            } else {
                None
            }
        })
    };
    Fact {
        id: event.id.to_hex(),
        kind: event.kind.as_u16(),
        toron_kind: tag("toron-kind"),
        subject_pin: tag("p"),
        project: tag("t"),
        role_change: tag("role").is_some(),
        policy_change: tag("policy").is_some(),
    }
}

/// Classify one fact for every active subscription and append hint-only rows.
/// Pure apart from the injected store; no bus, clock or env reads here.
pub async fn dispatch_fact(
    fact: &Fact,
    store: &dyn SubscriptionStore,
    now_us: i64,
) -> anyhow::Result<usize> {
    let mut delivered = 0;
    for sub in store.list_active().await? {
        // The operator's subscription principal is "operator"; it sees every
        // own-pin fact. Cross-scope is gated at subscribe time.
        let subscriber = if sub.principal == "operator" {
            fact.subject_pin.clone().unwrap_or_default()
        } else {
            sub.principal.clone()
        };
        if let Some(name) = classify(fact, &subscriber) {
            let hint = json!({
                "event_id": fact.id,
                "name": name.as_str(),
                "project": fact.project.clone().unwrap_or_default(),
                "sender_pin": fact.subject_pin,
                "correlation_id": null,
                "timestamp": now_us,
            });
            store.append(&sub.id, &hint, now_us).await?;
            delivered += 1;
        }
    }
    Ok(delivered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use super::super::store::{Page, SubscribeSpec, Subscription};
    use serde_json::Value;
    use std::sync::Mutex;

    struct CountingStore {
        appends: Mutex<usize>,
    }
    #[async_trait]
    impl SubscriptionStore for CountingStore {
        async fn subscribe(&self, _spec: &SubscribeSpec) -> anyhow::Result<Subscription> {
            unreachable!()
        }
        async fn get(&self, _id: &str) -> anyhow::Result<Option<Subscription>> {
            Ok(None)
        }
        async fn list_active(&self) -> anyhow::Result<Vec<Subscription>> {
            Ok(vec![Subscription {
                id: "s".into(),
                principal: "AmberProbe".into(),
                delivery: "poll".into(),
                name: "mail.addressed".into(),
                args_hash: "h".into(),
                cursor: 0,
                created_us: 0,
                expires_us: None,
                state: "active".into(),
            }])
        }
        async fn append(&self, _id: &str, _payload: &Value, _now_us: i64) -> anyhow::Result<()> {
            *self.appends.lock().unwrap() += 1;
            Ok(())
        }
        async fn poll(&self, _id: &str, _after: u64, _limit: u32) -> anyhow::Result<Page> {
            Ok(Page { events: vec![], cursor: 0, has_more: false, truncated: false })
        }
        async fn unsubscribe(&self, _id: &str, _principal: &str) -> anyhow::Result<bool> { Ok(true) }
        async fn terminate(&self, _id: &str) -> anyhow::Result<()> { Ok(()) }
        async fn prune_deliveries(&self, _now_us: i64) -> anyhow::Result<()> { Ok(()) }
        async fn sweep(&self, _now_us: i64) -> anyhow::Result<usize> { Ok(0) }
    }

    #[tokio::test]
    async fn a_lost_fact_appends_one_gap_per_active_subscription() {
        let store = CountingStore { appends: Mutex::new(0) };
        append_gap(&store, 1000).await;
        assert_eq!(*store.appends.lock().unwrap(), 1);
    }
}

pub struct EventDispatcher {
    bus: Arc<dyn Bus>,
    store: Arc<dyn SubscriptionStore>,
}

impl EventDispatcher {
    pub fn new(bus: Arc<dyn Bus>, store: Arc<dyn SubscriptionStore>) -> Self {
        Self { bus, store }
    }

    /// Hold one trusted subscription for the whole daemon. Its JoinHandle is
    /// aborted when the returned handle drops (serve shutdown).
    pub async fn start(self: Arc<Self>) -> anyhow::Result<DispatcherHandle> {
        let filter = Filter::new();
        let mut sub = self.bus.subscribe_trusted(filter).await?;
        let mut sweep = tokio::time::interval(Duration::from_secs(60));
        sweep.tick().await;
        let task = tokio::spawn(async move {
            loop {
                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                tokio::select! {
                    item = sub.next() => match item {
                        Some(SubscriptionEvent::Event(event)) => {
                            let fact = fact_from_event(&event);
                            if let Err(e) = dispatch_fact(&fact, self.store.as_ref(), now_us).await {
                                tracing::warn!("events dispatcher: {e}");
                                append_gap(self.store.as_ref(), now_us).await;
                            }
                        }
                        // EOSE is normal replay end. A closed/shed stream
                        // means facts were lost — one gap hint per active sub.
                        Some(SubscriptionEvent::Eose) => {}
                        Some(SubscriptionEvent::Closed(_)) => {
                            append_gap(self.store.as_ref(), now_us).await
                        }
                        None => break,
                    },
                    _ = sweep.tick() => {
                        if let Err(e) = sweep_expired(self.store.as_ref(), now_us).await {
                            tracing::warn!("events sweep: {e}");
                        }
                    }
                }
            }
        });
        Ok(DispatcherHandle(task))
    }
}

pub struct DispatcherHandle(tokio::task::JoinHandle<()>);
impl Drop for DispatcherHandle {
    fn drop(&mut self) {
        self.0.abort();
    }
}
