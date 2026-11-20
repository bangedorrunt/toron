// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! U3 producer: ONE trusted bus subscription classified for every active
//! subscription, plus the 60s sweep tick (ADR-0039 D3, bd-kz33f). No second
//! bus and no SQLite polling: facts arrive on the bus we already own.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use crate::nostr::{Event, Filter};

use super::catalogue::{Fact, classify};
use super::store::SubscriptionStore;
use crate::bus::{Bus, SubscriptionEvent};

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
        kind: event.kind.as_u16(),
        toron_kind: tag("toron-kind"),
        subject_pin: tag("p"),
        role_change: tag("role") .is_some(),
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
                "event_id": format!("{}", fact.kind),
                "name": name.as_str(),
                "project": fact.subject_pin.clone().unwrap_or_default(),
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
        sweep.tick().await; // skip the immediate first tick
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    item = sub.next() => match item {
                        Some(SubscriptionEvent::Event(event)) => {
                            let fact = fact_from_event(&event);
                            let now_us = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_micros() as i64)
                                .unwrap_or(0);
                            if let Err(e) = dispatch_fact(&fact, self.store.as_ref(), now_us).await {
                                tracing::warn!("events dispatcher: {e}");
                            }
                        }
                        Some(_) => {}
                        None => { break; }
                    },
                    _ = sweep.tick() => {
                        let now_us = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_micros() as i64)
                            .unwrap_or(0);
                        if let Err(e) = self.store.sweep(now_us).await {
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
