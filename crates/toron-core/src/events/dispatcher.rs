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

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

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
        // Since = process start: a restart replays only facts from now on,
        // never re-dispatching stored history as fresh hints.
        let started = crate::nostr::Timestamp::now();
        let mut sub = self
            .bus
            .subscribe_trusted(Filter::new().since(started))
            .await?;
        let mut sweep = tokio::time::interval(Duration::from_secs(60));
        sweep.tick().await;
        let task = tokio::spawn(async move {
            let mut seen_lag = 0u64;
            let mut since = started;
            let mut backoff = Duration::from_millis(200);
            loop {
                let mut closed = false;
                loop {
                    let lag = sub.lag_count();
                    if lag > seen_lag {
                        append_gap(self.store.as_ref(), now_us()).await;
                        seen_lag = lag;
                    }
                    tokio::select! {
                        item = sub.next() => match item {
                            Some(SubscriptionEvent::Event(event)) => {
                                let now = now_us();
                                since = since.max(event.created_at);
                                let fact = fact_from_event(&event);
                                if let Err(e) = dispatch_fact(&fact, self.store.as_ref(), now).await {
                                    tracing::warn!("events dispatcher: {e}");
                                    append_gap(self.store.as_ref(), now).await;
                                }
                            }
                            Some(SubscriptionEvent::Eose) => {}
                            Some(SubscriptionEvent::Closed(_)) | None => {
                                closed = true;
                                break;
                            }
                        },
                        _ = sweep.tick() => {
                            let now = now_us();
                            if let Err(e) = sweep_expired(self.store.as_ref(), now).await {
                                tracing::warn!("events sweep: {e}");
                            }
                        }
                    }
                }
                if closed {
                    // The replay may not cover the hole: record one gap.
                    append_gap(self.store.as_ref(), now_us()).await;
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    match self.bus.subscribe_trusted(Filter::new().since(since)).await {
                        Ok(next) => {
                            sub = next;
                            seen_lag = sub.lag_count();
                            tracing::info!("events dispatcher resubscribed");
                        }
                        Err(e) => tracing::warn!("events dispatcher resubscribe failed: {e}"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use super::super::store::{Page, SubscribeSpec, Subscription};
    use serde_json::Value;
    use std::sync::Mutex;

    struct CountingStore {
        appends: Mutex<usize>,
        payloads: Mutex<Vec<String>>,
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
            self.payloads.lock().unwrap().push(_payload.to_string());
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
        let store = CountingStore { appends: Mutex::new(0), payloads: Mutex::new(Vec::new()) };
        append_gap(&store, 1000).await;
        assert_eq!(*store.appends.lock().unwrap(), 1);
    }

    // A bus whose first trusted subscription closes immediately and whose
    // second one accepts a published fact later. Proves delivery survives a
    // closed stream instead of dying until daemon restart.
    struct ClosingBus {
        calls: AtomicU64,
        txs: Mutex<Vec<tokio::sync::mpsc::Sender<SubscriptionEvent>>>,
    }

    #[async_trait]
    impl crate::bus::Bus for ClosingBus {
        async fn publish(&self, _event: Event) -> Result<crate::bus::PublishOk, crate::bus::BusError> {
            unreachable!()
        }
        async fn subscribe(&self, filter: Filter) -> Result<crate::bus::Subscription, crate::bus::BusError> {
            self.subscribe_trusted(filter).await
        }
        async fn subscribe_trusted(&self, _filter: Filter) -> Result<crate::bus::Subscription, crate::bus::BusError> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            if n == 0 {
                tx.send(SubscriptionEvent::Closed("shed".into())).await.unwrap();
            }
            self.txs.lock().unwrap().push(tx);
            Ok(crate::bus::Subscription::new(
                crate::bus::SubscriptionId::new(format!("fake-{n}")), rx,
            ))
        }
        async fn health(&self) -> Result<crate::bus::Health, crate::bus::BusError> {
            Ok(crate::bus::Health { ok: true, message: String::new() })
        }
    }

    #[tokio::test]
    async fn a_lagging_subscription_exposes_its_drops() {
        use crate::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
        use crate::bus::Bus;

        let path = std::env::temp_dir().join(format!("toron-disp-lag-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let pool = crate::index::schema::connect(path.to_str().unwrap()).await.unwrap();
        crate::index::schema::migrate(&pool).await.unwrap();
        let bus = InProcessBus::open(pool, InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: Vec::new(),
            channel_capacity: 1,
            detailed_stage_logging: false,
        }).await.unwrap();
        let mut sub = bus.subscribe_trusted(Filter::new().since(crate::nostr::Timestamp::now())).await.unwrap();
        let keys = crate::nostr::Keys::generate();
        for i in 0..8 {
            let ev = crate::nostr::event::build_event(
                &keys, crate::nostr::kinds::toron_kind(crate::nostr::kinds::CHANNEL_MESSAGE),
                format!("flood-{i}"), vec![crate::nostr::Tag::parse(["p", "P"]).unwrap()],
                crate::nostr::Timestamp::now(),
            ).unwrap();
            bus.publish(ev).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(sub.lag_count() > 0, "publisher-side lag is observable on the stream");
    }

    #[tokio::test]
    async fn a_since_filter_does_not_replay_stored_history() {
        use crate::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
        use crate::bus::Bus;

        let path = std::env::temp_dir().join(format!("toron-disp-replay-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let pool = crate::index::schema::connect(path.to_str().unwrap()).await.unwrap();
        crate::index::schema::migrate(&pool).await.unwrap();
        let bus = InProcessBus::open(pool, InProcessBusConfig {
            auth: AuthPolicy::AcceptAll, allowlist: Vec::new(),
            channel_capacity: 64, detailed_stage_logging: false,
        }).await.unwrap();
        let keys = crate::nostr::Keys::generate();
        let old = crate::nostr::event::build_event(
            &keys, crate::nostr::kinds::toron_kind(crate::nostr::kinds::CHANNEL_MESSAGE),
            "old", vec![crate::nostr::Tag::parse(["p", "P"]).unwrap()],
            crate::nostr::Timestamp::from_secs(1),
        ).unwrap();
        bus.publish(old).await.unwrap();
        // Fresh dispatcher: since = now, so the old fact is not replayed.
        let mut sub = bus.subscribe_trusted(Filter::new().since(crate::nostr::Timestamp::now())).await.unwrap();
        let mut replayed = 0;
        while let Ok(Some(item)) = tokio::time::timeout(std::time::Duration::from_millis(50), sub.next()).await {
            if matches!(item, SubscriptionEvent::Event(_)) { replayed += 1; }
        }
        assert_eq!(replayed, 0, "restart must not re-dispatch stored history");
    }

    #[tokio::test]
    async fn a_closed_stream_resubscribes_and_later_facts_still_arrive() {
        let bus = Arc::new(ClosingBus {
            calls: AtomicU64::new(0),
            txs: Mutex::new(Vec::new()),
        });
        let store = Arc::new(CountingStore {
            appends: Mutex::new(0),
            payloads: Mutex::new(Vec::new()),
        });
        let dispatcher = Arc::new(EventDispatcher::new(bus.clone(), store.clone()));
        let _handle = dispatcher.start().await.expect("start");

        // Wait for the resubscribe, then deliver a fact on the second stream.
        for _ in 0..40 {
            if bus.calls.load(Ordering::Relaxed) >= 2 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(bus.calls.load(Ordering::Relaxed) >= 2, "dispatcher resubscribed");
        let keys = crate::nostr::Keys::generate();
        let event = crate::nostr::event::build_event(
            &keys, crate::nostr::kinds::toron_kind(crate::nostr::kinds::CHANNEL_MESSAGE),
            "later", vec![crate::nostr::Tag::parse(["p", "AmberProbe"]).unwrap()],
            crate::nostr::Timestamp::now(),
        ).unwrap();
        bus.txs.lock().unwrap()[1]
            .send(SubscriptionEvent::Event(event))
            .await
            .unwrap();
        let mut ok = false;
        for _ in 0..40 {
            if store.payloads.lock().unwrap().iter().any(|p| p.contains("mail.addressed")) { ok = true; break; }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(ok, "the later fact arrived after resubscribe");
    }
}
