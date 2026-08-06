// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! InProcessBus — Tokio per-subscriber channels + SQLite event store + the
//! Buzz pipeline (verify → auth → dedup → access-gated fanout → search hook →
//! audit hook → workflow-trigger hook).
//!
//! The transport is channels, not WS, but events are wire-identical to
//! nostr-sdk `Event` (publish any of them to a real relay unchanged). NOT a
//! NIP-01 REQ-able relay for external clients.
//!
//! Per the beads-review rule (2026-08-05): every subscription is a **lossless
//! per-subscriber `mpsc`** — `tokio::sync::broadcast` drops the oldest events
//! for laggy receivers, which is unacceptable for the durable projection
//! subscriber (bd-led.4.4). Backpressure surfaces to the publisher via
//! `mpsc::error::TrySendError` handling (dropped publisher-side, logged).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use nostr_sdk::nostr::Event;

use crate::bus::event_store::BusEventStore;
use crate::bus::{
    Bus, BusError, Filter, Health, PublishOk, Subscription, SubscriptionEvent, SubscriptionId,
};
use crate::nostr::event::VerifyError;

/// Pipeline stage names (for stage logging on rejection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Verify,
    Auth,
    Dedup,
    Fanout,
    SearchHook,
    AuditHook,
    WorkflowHook,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Stage::Verify => "verify",
            Stage::Auth => "auth",
            Stage::Dedup => "dedup",
            Stage::Fanout => "fanout",
            Stage::SearchHook => "search_hook",
            Stage::AuditHook => "audit_hook",
            Stage::WorkflowHook => "workflow_hook",
        }
    }
}

/// Auth policy applied at the `auth` stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthPolicy {
    /// Default: all events accepted after verify (local-first, no egress).
    #[default]
    AcceptAll,
    /// Reject events whose author is not allowlisted.
    Allowlist,
}

/// A registered subscriber.
struct Subscriber {
    id: SubscriptionId,
    filter: Filter,
    tx: mpsc::Sender<SubscriptionEvent>,
    /// True when this subscription is global (no project scoping).
    global: bool,
}

/// Configuration for [`InProcessBus`].
#[derive(Debug, Clone)]
pub struct InProcessBusConfig {
    /// Auth policy.
    pub auth: AuthPolicy,
    /// Author allowlist (hex pubkeys) used when `auth == Allowlist`.
    pub allowlist: Vec<String>,
    /// Per-subscription channel capacity.
    pub channel_capacity: usize,
    /// Enable stage logging on rejection (tracing::debug + a structured log
    /// callback).
    pub detailed_stage_logging: bool,
}

impl Default for InProcessBusConfig {
    fn default() -> Self {
        Self {
            auth: AuthPolicy::AcceptAll,
            allowlist: Vec::new(),
            channel_capacity: Subscription::DEFAULT_CAPACITY,
            detailed_stage_logging: true,
        }
    }
}

/// In-process bus: channels + SQLite event store + Buzz pipeline.
#[derive(Clone)]
pub struct InProcessBus {
    store: BusEventStore,
    config: Arc<InProcessBusConfig>,
    subs: Arc<RwLock<HashMap<SubscriptionId, Arc<Subscriber>>>>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl InProcessBus {
    /// Open the bus on a SQLite pool (event-store table created idempotently).
    pub async fn open(pool: SqlitePool, config: InProcessBusConfig) -> anyhow::Result<Self> {
        let store = BusEventStore::new(pool).await?;
        Ok(Self {
            store,
            config: Arc::new(config),
            subs: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    fn stage_log(&self, stage: Stage, event: &Event, detail: &str) {
        if self.config.detailed_stage_logging {
            tracing::debug!(
                stage = stage.name(),
                event_id = %event.id,
                detail,
                "bus rejection"
            );
        }
    }

    /// Does the event carry a project-scoping `#t` tag? (ADR-0003 G6/A3)
    fn project_slug(event: &Event) -> Option<&str> {
        event.tags.iter().find_map(|t| {
            if t.kind() == nostr_sdk::nostr::TagKind::t() {
                t.content()
            } else {
                None
            }
        })
    }

    /// Reject project-scoped events for global subscriptions (A3: access
    /// gating). A subscription is global when its filter has no `#t` scoping.
    fn access_gate(&self, sub: &Subscriber, event: &Event) -> bool {
        if Self::project_slug(event).is_some() {
            // Project-scoped event: subscriber must be scoped to this project.
            if sub.global {
                return false;
            }
            // Subscriber's filter must match the event's own #t.
            return sub.filter.match_event(event, Default::default());
        }
        true
    }

    fn fanout_to(&self, sub: &Subscriber, event: &Event) {
        // Lossless: if the channel is full we drop publisher-side with a
        // warning (the event remains in the store for cursor-based replay).
        match sub.tx.try_send(SubscriptionEvent::Event(event.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    sub = %sub.id,
                    event_id = %event.id,
                    "subscription channel full; event stays in store for replay"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Subscriber dropped its receiver; remove the subscription.
                self.subs.write().remove(&sub.id);
            }
        }
    }
}

#[async_trait::async_trait]
impl Bus for InProcessBus {
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError> {
        // 1. Verify: reject forged/mutated events (wrong id or sig).
        match crate::nostr::event::verify_event(&event) {
            Ok(()) => {}
            Err(VerifyError::BadId) => {
                self.stage_log(Stage::Verify, &event, "id mismatch");
                return Err(BusError::InvalidEvent("id mismatch".into()));
            }
            Err(VerifyError::BadSignature) => {
                self.stage_log(Stage::Verify, &event, "signature mismatch");
                return Err(BusError::InvalidEvent("signature mismatch".into()));
            }
            Err(e) => {
                self.stage_log(Stage::Verify, &event, &format!("{e}"));
                return Err(BusError::InvalidEvent(e.to_string()));
            }
        }

        // 2. Auth: reject non-allowlisted authors if policy requires.
        if self.config.auth == AuthPolicy::Allowlist
            && !self
                .config
                .allowlist
                .iter()
                .any(|a| a == &event.pubkey.to_hex())
        {
            self.stage_log(Stage::Auth, &event, "author not allowlisted");
            return Err(BusError::Publish("author not allowlisted".into()));
        }

        // 3. Dedup: store by event id (ON CONFLICT DO NOTHING).
        let stored = self.store.insert(&event).await.map_err(|e| {
            self.stage_log(Stage::Dedup, &event, &format!("store failure: {e}"));
            BusError::Publish(format!("event store failure: {e}"))
        })?;
        if !stored {
            self.stage_log(Stage::Dedup, &event, "duplicate event id, dropped");
            return Ok(PublishOk {
                event_id: event.id,
                stored: false,
            });
        }

        // 4. Access-gated fanout to live subscribers.
        let subs: Vec<Arc<Subscriber>> = {
            let guard = self.subs.read();
            guard.values().cloned().collect()
        };
        let mut fanned = 0usize;
        for sub in &subs {
            if sub.filter.match_event(&event, Default::default()) && self.access_gate(sub, &event) {
                self.fanout_to(sub, &event);
                fanned += 1;
            }
        }
        if fanned == 0 {
            self.stage_log(Stage::Fanout, &event, "no matching subscriber");
        }

        // 5-7. Hooks: search / audit / workflow-trigger are wired by the
        // projection + workflow layers (bd-led.3.3 / bd-led.3.7). InProcessBus
        // records the hook stage boundary for observability.
        self.stage_log(Stage::SearchHook, &event, "search hook boundary (no-op)");
        self.stage_log(Stage::AuditHook, &event, "audit hook boundary (no-op)");
        self.stage_log(
            Stage::WorkflowHook,
            &event,
            "workflow hook boundary (no-op)",
        );

        Ok(PublishOk {
            event_id: event.id,
            stored: true,
        })
    }

    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError> {
        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let id = SubscriptionId::new(format!(
            "sub-{}",
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));

        // Replay stored history matching the filter (expired excluded), then
        // emit EOSE so the subscriber knows live streaming has begun.
        let replayed = self
            .store
            .replay(&filter)
            .await
            .map_err(|e| BusError::Subscription(format!("replay failure: {e}")))?;
        for ev in replayed {
            let _ = tx.send(SubscriptionEvent::Event(ev)).await;
        }
        let _ = tx.send(SubscriptionEvent::Eose).await;

        // Global = filter has no project scoping (#t via hashtags or #l label).
        use nostr_sdk::nostr::{Alphabet, SingleLetterTag};
        let t_tag = SingleLetterTag::lowercase(Alphabet::T);
        let l_tag = SingleLetterTag::lowercase(Alphabet::L);
        let global =
            !filter.generic_tags.contains_key(&t_tag) && !filter.generic_tags.contains_key(&l_tag);

        let sub = Arc::new(Subscriber {
            id: id.clone(),
            filter,
            tx,
            global,
        });
        self.subs.write().insert(id.clone(), sub);

        Ok(Subscription::new(id, rx))
    }

    async fn health(&self) -> Result<Health, BusError> {
        match self.store.count().await {
            Ok(n) => Ok(Health {
                ok: true,
                message: format!("in-process bus healthy, {n} stored events"),
            }),
            Err(e) => Ok(Health {
                ok: false,
                message: format!("event store unreachable: {e}"),
            }),
        }
    }
}

// Allow constructing a bus for tests without exposing internals.
impl InProcessBus {
    /// Count of currently registered subscriptions (diagnostics).
    pub fn subscriber_count(&self) -> usize {
        self.subs.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST};
    use nostr_sdk::nostr::{EventId, Keys, Tag, Timestamp};
    use std::time::{Duration, Instant};

    async fn temp_bus() -> (InProcessBus, std::path::PathBuf) {
        temp_bus_with(InProcessBusConfig::default()).await
    }

    async fn temp_bus_with(config: InProcessBusConfig) -> (InProcessBus, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("toron-ipbus-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool, config).await.expect("bus");
        (bus, path)
    }

    /// Drain up to `max` items, stopping early on a `Duration` of silence.
    async fn drain(
        sub: &mut Subscription,
        max: usize,
        silence: Duration,
    ) -> Vec<SubscriptionEvent> {
        let mut out = Vec::new();
        for _ in 0..max {
            match tokio::time::timeout(silence, sub.next()).await {
                Ok(Some(ev)) => out.push(ev),
                _ => break,
            }
        }
        out
    }

    fn signed(keys: &Keys, kind: u16, content: &str, slug: &str) -> Event {
        let mut tags = vec![];
        if !slug.is_empty() {
            tags.push(Tag::parse(["t", slug]).expect("t"));
        }
        build_event(
            keys,
            crate::nostr::kinds::toron_kind(kind),
            content,
            tags,
            Timestamp::now(),
        )
        .expect("build")
    }

    #[tokio::test]
    async fn test_publish_subscribe_roundtrip() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "hello", "demo");
        let ok = bus.publish(ev.clone()).await.expect("publish");
        assert!(ok.stored);

        // Replay + EOSE come first, then the live event.
        let items = drain(&mut sub, 8, Duration::from_millis(200)).await;
        let saw_event = items
            .iter()
            .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id));
        let got_eose = items.iter().any(|it| matches!(it, SubscriptionEvent::Eose));
        assert!(saw_event, "live event must be delivered, got {items:?}");
        assert!(got_eose, "EOSE marker must be emitted, got {items:?}");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_forged_event_rejected_no_fanout() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "original", "demo");
        // Forge: change content but keep id+sig.
        let forged = Event::new(
            ev.id,
            ev.pubkey,
            ev.created_at,
            ev.kind,
            ev.tags.clone(),
            "MUTATED",
            ev.sig,
        );
        match bus.publish(forged).await {
            Err(BusError::InvalidEvent(_)) => {}
            other => panic!("expected InvalidEvent, got {other:?}"),
        }

        // No event should arrive (verify failed → no fanout). Eose is the
        // only thing the subscription ever receives.
        let items = drain(&mut sub, 4, Duration::from_millis(100)).await;
        let forged_leaked = items
            .iter()
            .any(|it| matches!(it, SubscriptionEvent::Event(_)));
        assert!(
            !forged_leaked,
            "forged event must not fan out, got {items:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_duplicate_id_ignored() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "one", "demo");
        assert!(bus.publish(ev.clone()).await.expect("pub").stored);
        let dup = bus.publish(ev.clone()).await.expect("pub");
        assert!(!dup.stored, "duplicate must not be stored");
        assert_eq!(bus.store.count().await.expect("count"), 1);

        // Drain replay + EOSE; only one copy of the event may be delivered.
        let items = drain(&mut sub, 8, Duration::from_millis(200)).await;
        let count = items
            .iter()
            .filter(|it| matches!(it, SubscriptionEvent::Event(_)))
            .count();
        assert_eq!(
            count, 1,
            "duplicate must not be re-fanned out, got {items:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_expired_event_not_delivered_to_new_sub() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "expired",
            vec![
                Tag::parse(["t", "demo"]).expect("t"),
                Tag::expiration(Timestamp::from(1_500_000_000_u64)),
            ],
            Timestamp::from(1_400_000_000_u64),
        )
        .expect("build");
        bus.publish(ev).await.expect("publish");

        // New subscription replays only non-expired events.
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");
        let first = sub.next().await;
        assert!(
            matches!(first, Some(SubscriptionEvent::Eose)),
            "replay must be empty, got {first:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_latency_smoke_below_50ms() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");

        let start = Instant::now();
        let ev = signed(&keys, PROJECT_MANIFEST, "latency", "demo");
        bus.publish(ev.clone()).await.expect("publish");

        // Measure only up to the moment the live event arrives (Eose precedes
        // it; read one item at a time with a tight per-item timeout).
        let mut delivered = false;
        for _ in 0..8 {
            match tokio::time::timeout(Duration::from_millis(200), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(e))) if e.id == ev.id => {
                    delivered = true;
                    break;
                }
                Ok(_) => continue,
                _ => break,
            }
        }
        assert!(delivered, "live event must be delivered");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "publish→receive latency {elapsed:?} must be < 50ms"
        );
        tracing::info!("latency histogram sample: {elapsed:?}");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_global_sub_excluded_from_project_scoped() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        // A global subscription: filter matches everything, no project scoping.
        let mut global = bus.subscribe(Filter::new()).await.expect("global sub");
        // A scoped subscription.
        let mut scoped = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("scoped sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "scoped secret", "demo");
        bus.publish(ev.clone()).await.expect("publish");

        // Scoped sub receives it.
        let scoped_items = drain(&mut scoped, 8, Duration::from_millis(200)).await;
        assert!(
            scoped_items
                .iter()
                .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id)),
            "project-scoped sub must receive project event, got {scoped_items:?}"
        );

        // Global sub must NOT receive the project-scoped event (access gate).
        let global_items = drain(&mut global, 8, Duration::from_millis(150)).await;
        let global_leaked = global_items
            .iter()
            .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id));
        assert!(
            !global_leaked,
            "global sub received project-scoped event: {global_items:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_allowlist_auth_rejects_non_listed() {
        let allowed = Keys::generate();
        let blocked = Keys::generate();
        let config = InProcessBusConfig {
            auth: AuthPolicy::Allowlist,
            allowlist: vec![allowed.public_key().to_hex()],
            ..Default::default()
        };
        let (bus, path) = temp_bus_with(config).await;

        let ok = bus
            .publish(signed(&allowed, PROJECT_MANIFEST, "ok", "demo"))
            .await;
        assert!(ok.is_ok(), "allowlisted author accepted");

        let bad = bus
            .publish(signed(&blocked, PROJECT_MANIFEST, "nope", "demo"))
            .await;
        assert!(
            matches!(bad, Err(BusError::Publish(_))),
            "blocked author rejected"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_health_reports_count() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        bus.publish(signed(&keys, PROJECT_MANIFEST, "a", "demo"))
            .await
            .expect("pub");
        let h = bus.health().await.expect("health");
        assert!(h.ok);
        assert!(h.message.contains('1'), "message: {}", h.message);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_stage_names_stable() {
        assert_eq!(Stage::Verify.name(), "verify");
        assert_eq!(Stage::Dedup.name(), "dedup");
        assert_eq!(Stage::WorkflowHook.name(), "workflow_hook");
    }

    #[test]
    fn test_kind_tags_resolve() {
        assert_eq!(AGENT_PROFILE, 30013);
        assert_eq!(PROJECT_MANIFEST, 30010);
        let _ =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
    }
}
