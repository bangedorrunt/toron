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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use sqlx::SqlitePool;
use tokio::sync::mpsc;

use crate::nostr::Event;
use crate::nostr::MatchEventOptions;

use crate::bus::event_store::BusEventStore;
use crate::bus::{
    Bus, BusError, Filter, Health, PublishOk, Subscription, SubscriptionEvent, SubscriptionId,
};

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
    const fn name(self) -> &'static str {
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
    /// Event ids captured in the subscribe-time snapshot (replayed by the
    /// background task). Live fanout skips these until `replay_done`.
    snapshot: Arc<HashSet<String>>,
    /// Ids delivered by EITHER path (live fanout or the replay task's
    /// snapshot/delta) while the replay task is still running — the
    /// exactly-once claim check for the overlap window (publishes during
    /// subscribe setup). Cleared once `replay_done` (single-path delivery
    /// needs no dedup; bounded memory).
    delivered: Arc<Mutex<HashSet<String>>>,
    /// Set to true once the replay task has pushed history + EOSE AND every
    /// live event buffered during replay has been delivered. Until then live
    /// events wait in `pending` instead of racing ahead of the EOSE marker.
    replay_done: Arc<std::sync::atomic::AtomicBool>,
    /// Live events published while replay is running, in publish order. The
    /// replay task drains these after EOSE. Bounded by the channel capacity:
    /// a subscriber that cannot keep up during replay is a full-drop, exactly
    /// as it would be after replay.
    pending: Arc<Mutex<Vec<Event>>>,
    /// Serializes the replay seal (EOSE + `replay_done` flip + pending drain)
    /// against live publishers. Held only for synchronous channel operations.
    gate: Arc<Mutex<()>>,
    /// Last channel-full drop report (rate-limits the WARN so a permanently
    /// slow consumer doesn't flood the log; drops stay exactly counted).
    last_full_warn: Arc<Mutex<std::time::Instant>>,
}

impl Subscriber {
    /// WARN rate limit for channel-full drops (see
    /// [`InProcessBus::note_full_drop`]).
    const FULL_WARN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

    /// True when a channel-full WARN should be emitted NOW — at most one per
    /// `FULL_WARN_INTERVAL` per subscriber. The drop itself is counted on the
    /// bus regardless (exact observability); this only throttles the log.
    fn note_full(&self) -> bool {
        let mut last = self.last_full_warn.lock();
        let now = std::time::Instant::now();
        if now.duration_since(*last) < Self::FULL_WARN_INTERVAL {
            return false;
        }
        *last = now;
        true
    }
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
    /// Total channel-full drops (fall-behind subscribers): events not
    /// delivered live, recoverable via store replay. Exact — the
    /// observability half of the lossless-with-replay contract.
    full_drops: Arc<std::sync::atomic::AtomicU64>,
    /// Channel-full WARNs actually emitted (rate-limited per subscriber).
    full_warns: Arc<std::sync::atomic::AtomicU64>,
}

impl InProcessBus {
    /// Measurement accessor for the underlying store (bd-3gwy gates).
    #[cfg(test)]
    pub(crate) const fn event_store(&self) -> &BusEventStore {
        &self.store
    }

    /// Open the bus on a SQLite pool (event-store table created idempotently).
    pub async fn open(pool: SqlitePool, config: InProcessBusConfig) -> anyhow::Result<Self> {
        let store = BusEventStore::new(pool).await?;
        let prune_store = store.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(BusEventStore::PRUNE_INTERVAL).await;
                if let Err(e) = prune_store.prune().await {
                    tracing::warn!("bus prune failed: {e:#}");
                }
            }
        });
        Ok(Self {
            store,
            config: Arc::new(config),
            subs: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            full_drops: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            full_warns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    fn stage_log(&self, stage: Stage, event: &Event, detail: &str) {
        let emitted = super::inspection::InspectionEvent::for_stage(stage.name(), detail);
        if self.config.detailed_stage_logging {
            tracing::debug!(
                stage = stage.name(),
                kind = emitted.kind.as_str(),
                event_id = %event.id,
                detail,
                "bus rejection"
            );
        }
    }

    /// Does the event carry a project-scoping `#t` tag? (ADR-0003 G6/A3)
    ///
    /// Toron-internal markers also ride on `t` (e.g. `toron:workflow-emitted`,
    /// `toron:overseer`). Those are **not** project slugs — treating them as
    /// such would make global subscribers drop stamped workflow jobs (F6/F7).
    fn project_slug(event: &Event) -> Option<&str> {
        event.tags.iter().find_map(|t| {
            if t.kind() == "t" {
                let c = t.content()?;
                if c.starts_with("toron:") {
                    return None;
                }
                Some(c)
            } else {
                None
            }
        })
    }

    /// Reject project-scoped events for global subscriptions (A3: access
    /// gating). A subscription is global when its filter has no `#t` scoping.
    fn access_gate(&self, sub: &Subscriber, event: &Event) -> bool {
        if Self::project_slug(event).is_some() {
            if sub.global {
                return false;
            }
            return sub.filter.match_event(event, MatchEventOptions::default());
        }
        true
    }

    fn fanout_to(&self, sub: &Subscriber, event: &Event) {
        let id = event.id.to_hex();
        let gate = sub.gate.lock();
        if !sub.replay_done.load(std::sync::atomic::Ordering::Acquire) {
            if sub.snapshot.contains(&id) {
                return;
            }
            if !sub.delivered.lock().insert(id.clone()) {
                return;
            }
            let mut pending = sub.pending.lock();
            if pending.len() >= self.config.channel_capacity {
                drop(pending);
                sub.delivered.lock().remove(&id);
                drop(gate);
                self.note_full_drop(sub, event);
                return;
            }
            pending.push(event.clone());
            return;
        }
        match sub.tx.try_send(SubscriptionEvent::Event(event.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                drop(gate);
                self.note_full_drop(sub, event);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                drop(gate);
                self.subs.write().remove(&sub.id);
            }
        }
    }

    /// A fall-behind subscriber: the event is dropped publisher-side and
    /// stays in the store for cursor-based replay (lossless — the drop is
    /// never silent data loss). Count every drop exactly on the bus; emit the
    /// WARN at most once per `Subscriber::FULL_WARN_INTERVAL` per subscriber
    /// so a permanently slow consumer can't flood the log (the Sep-1 stall
    /// pattern, bd-bus-scan).
    fn note_full_drop(&self, sub: &Subscriber, event: &Event) {
        self.full_drops
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if !sub.note_full() {
            return;
        }
        self.full_warns
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            sub = %sub.id,
            event_id = %event.id,
            "subscription channel full; event stays in store for replay"
        );
    }

    /// Test observability: exact channel-full drop count (every dropped
    /// event, whether or not its WARN was rate-limited away).
    #[cfg(test)]
    pub(crate) fn full_drops(&self) -> u64 {
        self.full_drops.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test observability: channel-full WARNs actually emitted.
    #[cfg(test)]
    pub(crate) fn full_warns(&self) -> u64 {
        self.full_warns.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Shared subscription construction (trait helper): `trusted` forces the
    /// subscriber to be treated as project-scoped (A3 gate off) regardless of
    /// the filter's `#t`/`#l` scoping — the mailbox's own internal consumers
    /// see every scoped event (P11.1 reactive ingress).
    async fn subscribe_with_gate(
        &self,
        filter: Filter,
        trusted: bool,
    ) -> Result<Subscription, BusError> {
        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let id = SubscriptionId::new(format!(
            "sub-{}",
            self.next_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));

        use crate::nostr::SingleLetterTag;
        let t_tag = SingleLetterTag::LOWERCASE_T;
        let l_tag = SingleLetterTag::LOWERCASE_L;
        let global = !trusted
            && !filter.generic_tags.contains_key(&t_tag)
            && !filter.generic_tags.contains_key(&l_tag);

        let snapshot = self
            .store
            .replay(&filter)
            .await
            .map_err(|e| BusError::Subscription(format!("replay failure: {e}")))?;
        let snapshot_ids: Arc<HashSet<String>> =
            Arc::new(snapshot.iter().map(|ev| ev.id.to_hex()).collect());
        let delivered: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let replay_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let pending: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
        let gate: Arc<Mutex<()>> = Arc::new(Mutex::new(()));
        let pending_clone = pending.clone();
        let gate_clone = gate.clone();
        let replay_tx = tx.clone();
        let snapshot_clone = snapshot;
        let replay_done_clone = replay_done.clone();
        let global_clone = global;
        let store_clone = self.store.clone();
        let filter_clone = filter.clone();
        let delivered_clone = delivered.clone();
        let snapshot_ids_clone = snapshot_ids.clone();
        tokio::spawn(async move {
            for ev in snapshot_clone {
                if global_clone && Self::project_slug(&ev).is_some() {
                    continue;
                }
                if replay_tx.send(SubscriptionEvent::Event(ev)).await.is_err() {
                    return;
                }
            }
            match store_clone.replay(&filter_clone).await {
                Ok(delta) => {
                    for ev in delta {
                        if global_clone && Self::project_slug(&ev).is_some() {
                            continue;
                        }
                        let id = ev.id.to_hex();
                        if snapshot_ids_clone.contains(&id) {
                            continue;
                        }
                        if !delivered_clone.lock().insert(id) {
                            continue;
                        }
                        if replay_tx.send(SubscriptionEvent::Event(ev)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("subscribe delta re-read failed: {e}");
                }
            }
            // EOSE is the boundary: live events buffered during replay are
            // delivered only after it, and the gate keeps a new publisher from
            // slipping one in front of the marker while we seal.
            if replay_tx.send(SubscriptionEvent::Eose).await.is_err() {
                return;
            }
            loop {
                let gate = gate_clone.lock();
                let batch: Vec<Event> = std::mem::take(&mut *pending_clone.lock());
                if batch.is_empty() {
                    replay_done_clone.store(true, std::sync::atomic::Ordering::Release);
                    drop(gate);
                    delivered_clone.lock().clear();
                    return;
                }
                let mut overflow = Vec::new();
                for ev in batch {
                    match replay_tx.try_send(SubscriptionEvent::Event(ev)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(SubscriptionEvent::Event(ev))) => {
                            overflow.push(ev);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => return,
                    }
                }
                drop(gate);
                // Still pre-seal, so new publishers keep buffering while an
                // overflowed event waits for channel space.
                for ev in overflow {
                    if replay_tx.send(SubscriptionEvent::Event(ev)).await.is_err() {
                        return;
                    }
                }
            }
        });

        let sub = Arc::new(Subscriber {
            id: id.clone(),
            filter,
            tx,
            global,
            snapshot: snapshot_ids,
            delivered,
            replay_done,
            pending,
            gate,
            last_full_warn: Arc::new(Mutex::new(
                std::time::Instant::now()
                    .checked_sub(Subscriber::FULL_WARN_INTERVAL)
                    .unwrap_or_else(std::time::Instant::now),
            )),
        });
        self.subs.write().insert(id.clone(), sub);

        let registry = self.subs.clone();
        let release_id = id.clone();
        Ok(Subscription::new(id, rx).with_release(Box::new(move || {
            registry.write().remove(&release_id);
        })))
    }
}

#[async_trait::async_trait]
impl Bus for InProcessBus {
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError> {
        if let Err(e) = crate::bus::common::verify_publish_event(&event) {
            self.stage_log(Stage::Verify, &event, &e.to_string());
            return Err(e);
        }

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

        if crate::bus::is_ephemeral_kind(event.kind) {
            let store_ok = self.store.is_healthy().await;
            if let Err(reason) = crate::bus::convergence::presence_admit(store_ok) {
                self.stage_log(Stage::Auth, &event, reason);
                return Err(BusError::Publish(reason.into()));
            }
            self.stage_log(Stage::Dedup, &event, "ephemeral kind, not stored");
            return self.fanout(event, false);
        }

        let stored = if crate::bus::is_addressable(event.kind) {
            self.store.insert_addressable(&event).await
        } else {
            self.store.insert(&event).await
        }
        .map_err(|e| {
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

        self.fanout(event, true)
    }

    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError> {
        self.subscribe_with_gate(filter, false).await
    }

    /// Trusted ingress: A3 global-gate bypassed (see [`Bus::subscribe_trusted`]).
    async fn subscribe_trusted(&self, filter: Filter) -> Result<Subscription, BusError> {
        self.subscribe_with_gate(filter, true).await
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

    /// bd-channel-close-purge-bsqb: local SQLite authority — delegate to the
    /// event store (rows + tag rows + hot memo + mutation bump).
    async fn purge_ids(&self, ids: &[String]) -> Result<u64, BusError> {
        self.store
            .purge_ids(ids)
            .await
            .map_err(|e| BusError::Unsupported(format!("purge failed: {e}")))
    }

    /// bd-3gwy/bd-qpm0: the store's authoritative mutation counter. One
    /// indexed `bus_meta` read answers "did anything change?" so the wake
    /// pump can skip a full replay on quiet ticks instead of minting a
    /// subscription per poll.
    async fn store_version(&self) -> Option<i64> {
        Some(self.store.current_mutations().await)
    }

    async fn arrivals_after(
        &self,
        kinds: &[u16],
        after: u64,
        limit: usize,
    ) -> Result<Option<Vec<(u64, Event)>>, BusError> {
        self.store
            .arrivals_after(kinds, after, limit)
            .await
            .map(Some)
            .map_err(|e| BusError::Subscription(format!("arrival read failure: {e}")))
    }

    async fn arrival_seq_of(&self, event_id: &str) -> Result<Option<u64>, BusError> {
        self.store
            .arrival_seq_of(event_id)
            .await
            .map_err(|e| BusError::Subscription(format!("arrival lookup failure: {e}")))
    }

    async fn arrivals_evicted_through(&self) -> Result<u64, BusError> {
        self.store
            .arrivals_evicted_through()
            .await
            .map_err(|e| BusError::Subscription(format!("eviction mark read failure: {e}")))
    }
}

impl InProcessBus {
    /// Count of currently registered subscriptions (diagnostics).
    pub fn subscriber_count(&self) -> usize {
        self.subs.read().len()
    }

    /// Access-gated fanout to live subscribers (shared by stored + ephemeral
    /// publish paths).
    fn fanout(&self, event: Event, stored: bool) -> Result<PublishOk, BusError> {
        let subs: Vec<Arc<Subscriber>> = {
            let guard = self.subs.read();
            guard.values().cloned().collect()
        };
        let mut fanned = 0usize;
        for sub in &subs {
            if sub.filter.match_event(&event, MatchEventOptions::default())
                && self.access_gate(sub, &event)
            {
                self.fanout_to(sub, &event);
                fanned += 1;
            }
        }
        if fanned == 0 {
            self.stage_log(Stage::Fanout, &event, "no matching subscriber");
        }

        self.stage_log(Stage::SearchHook, &event, "search hook boundary (no-op)");
        self.stage_log(Stage::AuditHook, &event, "audit hook boundary (no-op)");
        self.stage_log(
            Stage::WorkflowHook,
            &event,
            "workflow hook boundary (no-op)",
        );

        Ok(PublishOk {
            event_id: event.id,
            stored,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST};
    use crate::nostr::{EventId, Keys, Tag, Timestamp};
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

    /// Failure-detail: dump event id/kind/tags + received stream to the panic
    /// message so a pipeline failure is self-diagnosing.
    #[track_caller]
    fn dump_failure(ev: &Event, stage: &str, received: &[SubscriptionEvent]) -> String {
        format!(
            "pipeline stage [{stage}] failed\n  event id:   {}\n  event kind: {}\n  event tags: {:?}\n  received:   {received:?}",
            ev.id,
            ev.kind.as_u16(),
            ev.tags
                .iter()
                .map(|t| t.as_slice().join(","))
                .collect::<Vec<_>>(),
        )
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

    /// bd-3gwy acceptance gate at the bus level: a SECOND subscribe with the
    /// same filter must skip the phase-1 SQL id-scan entirely (snapshot memo,
    /// mutation-version validated) and still deliver full history + EOSE;
    /// a mutation forces exactly one fresh scan.
    #[tokio::test]
    async fn test_second_subscribe_skips_sql_scan_bd_3gwy() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let n = 8;
        for i in 0..n {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("m{i}"), "demo");
            bus.publish(ev).await.expect("publish");
        }

        let filter = crate::bus::project_filter("demo");
        let mut s1 = bus.subscribe(filter.clone()).await.expect("sub 1");
        let first = drain(&mut s1, 32, Duration::from_millis(400)).await;
        assert_eq!(
            first
                .iter()
                .filter(|e| matches!(e, SubscriptionEvent::Event(_)))
                .count(),
            n
        );
        let mid = bus.event_store().sql_scans();

        let scans_before_second = mid;
        let mut s2 = bus.subscribe(filter.clone()).await.expect("sub 2");
        let second = drain(&mut s2, 32, Duration::from_millis(400)).await;
        let got = second
            .iter()
            .filter_map(|e| match e {
                SubscriptionEvent::Event(ev) => Some(ev.id.to_hex()),
                _ => None,
            })
            .count();
        assert_eq!(got, n, "second subscribe replays full history");
        assert!(
            second.iter().any(|e| matches!(e, SubscriptionEvent::Eose)),
            "EOSE still delivered"
        );
        assert_eq!(
            bus.event_store().sql_scans(),
            scans_before_second,
            "second identical subscribe must NOT run the SQL id-scan (bd-3gwy)"
        );

        let fresh = signed(&keys, PROJECT_MANIFEST, "fresh", "demo");
        bus.publish(fresh).await.expect("publish fresh");
        let mut s3 = bus.subscribe(filter).await.expect("sub 3");
        let third = drain(&mut s3, 32, Duration::from_millis(400)).await;
        let third_events = third
            .iter()
            .filter(|e| matches!(e, SubscriptionEvent::Event(_)))
            .count();
        assert_eq!(third_events, n + 1);
        assert_eq!(
            bus.event_store().sql_scans(),
            scans_before_second + 1,
            "mutation forces exactly one fresh scan at the bus level"
        );
        let _ = std::fs::remove_file(&path);
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
        let items = drain(&mut sub, 8, Duration::from_millis(200)).await;
        let saw_event = items
            .iter()
            .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id));
        let got_eose = items.iter().any(|it| matches!(it, SubscriptionEvent::Eose));
        assert!(saw_event, "live event must be delivered, got {items:?}");
        assert!(got_eose, "EOSE marker must be emitted, got {items:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// A live publish during replay must not be dropped for channel-full
    /// once the channel is saturated by history, and it must never arrive
    /// ahead of EOSE. Before the buffer, `try_send` on the full channel
    /// discarded it; now it waits for the seal and follows the marker.
    #[tokio::test]
    async fn live_events_wait_for_eose_and_survive_a_full_replay_channel() {
        let (bus, path) = temp_bus_with(InProcessBusConfig {
            channel_capacity: 2,
            ..Default::default()
        })
        .await;
        let keys = Keys::generate();
        for i in 0..6 {
            bus.publish(signed(&keys, PROJECT_MANIFEST, &format!("h{i}"), "demo"))
                .await
                .expect("seed");
        }
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");
        // Replay is now blocked on the 2-slot channel, so this publish lands
        // in the pending buffer rather than the channel.
        let live = signed(&keys, PROJECT_MANIFEST, "live", "demo");
        bus.publish(live.clone()).await.expect("live publish");

        let items = drain(&mut sub, 32, Duration::from_millis(300)).await;
        let eose = items
            .iter()
            .position(|it| matches!(it, SubscriptionEvent::Eose))
            .expect("EOSE");
        let live_at = items
            .iter()
            .position(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == live.id))
            .expect("buffered live event must be delivered");
        assert!(
            eose < live_at,
            "history + EOSE must precede a buffered live event: {items:?}"
        );
        let history = items[..eose]
            .iter()
            .filter(|it| matches!(it, SubscriptionEvent::Event(_)))
            .count();
        assert_eq!(history, 6, "all replayed history arrives: {items:?}");
        let _ = std::fs::remove_file(&path);
    }

    /// Regression (bd-subscribe-fanout-gap-race-tgex): events published
    /// while a subscription is being set up — between the history snapshot
    /// read and the subscriber entering the fanout map — were in neither
    /// the snapshot nor live fanout: permanently missed by the new
    /// subscription. The replay task now re-reads the store (delta) after
    /// the snapshot, with an atomic per-subscriber dedup set so delivery
    /// stays exactly-once. Hammer test: seed history, publish a stream
    /// concurrently with subscribe, assert every STORED event arrives
    /// exactly once. (Ephemeral non-stored kinds are best-effort by design.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn subscribe_gap_no_missed_stored_events() {
        let (bus, path) = temp_bus_with(InProcessBusConfig {
            channel_capacity: 4096,
            ..Default::default()
        })
        .await;
        let keys = Keys::generate();

        let mut expected: Vec<String> = Vec::with_capacity(500);
        for i in 0..300 {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("seed {i}"), "demo");
            bus.publish(ev.clone()).await.expect("seed publish");
            expected.push(ev.id.to_hex());
        }

        let pub_bus = bus.clone();
        let pub_keys = keys.clone();
        let publisher = tokio::spawn(async move {
            let mut ids = Vec::with_capacity(200);
            for i in 0..200 {
                let ev = signed(&pub_keys, PROJECT_MANIFEST, &format!("live {i}"), "demo");
                pub_bus.publish(ev.clone()).await.expect("live publish");
                ids.push(ev.id.to_hex());
            }
            ids
        });

        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");
        let live_ids = publisher.await.expect("publisher");
        expected.extend(live_ids);

        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut saw_eose = false;
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(300), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(ev))) => {
                    *counts.entry(ev.id.to_hex()).or_insert(0) += 1;
                }
                Ok(Some(SubscriptionEvent::Eose)) => saw_eose = true,
                Ok(Some(SubscriptionEvent::Closed(_))) | Ok(None) => break,
                Err(_) => break,
            }
        }

        assert!(saw_eose, "EOSE must arrive");
        let mut missing = Vec::new();
        let mut duplicated = Vec::new();
        for id in &expected {
            match counts.get(id).copied().unwrap_or(0) {
                0 => missing.push(id),
                1 => {}
                n => duplicated.push((id, n)),
            }
        }
        assert!(
            missing.is_empty(),
            "{} stored events missed by the subscription (subscribe/fanout gap): {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        );
        assert!(
            duplicated.is_empty(),
            "duplicate deliveries: {duplicated:?}"
        );
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
    async fn test_latency_smoke_below_2s() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");

        let start = Instant::now();
        let ev = signed(&keys, PROJECT_MANIFEST, "latency", "demo");
        bus.publish(ev.clone()).await.expect("publish");

        let mut delivered = false;
        for _ in 0..8 {
            match tokio::time::timeout(Duration::from_millis(200), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(e))) if e.id == ev.id => {
                    delivered = true;
                    break;
                }
                Ok(_) => {}
                _ => break,
            }
        }
        assert!(delivered, "live event must be delivered");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "publish→receive latency {elapsed:?} must be < 2s"
        );
        tracing::info!("latency histogram sample: {elapsed:?}");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_global_sub_excluded_from_project_scoped() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let mut global = bus.subscribe(Filter::new()).await.expect("global sub");
        let mut scoped = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("scoped sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "scoped secret", "demo");
        bus.publish(ev.clone()).await.expect("publish");

        let scoped_items = drain(&mut scoped, 8, Duration::from_millis(200)).await;
        assert!(
            scoped_items
                .iter()
                .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id)),
            "project-scoped sub must receive project event, got {scoped_items:?}"
        );

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

    /// P11.1 (bd-foh8.1): a TRUSTED subscription receives project-scoped
    /// events even with an unscoped filter — the reactive-ingress bypass.
    #[tokio::test]
    async fn test_trusted_sub_receives_project_scoped() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let mut trusted = bus
            .subscribe_trusted(Filter::new())
            .await
            .expect("trusted sub");

        let ev = signed(&keys, PROJECT_MANIFEST, "scoped secret", "demo");
        bus.publish(ev.clone()).await.expect("publish");

        let items = drain(&mut trusted, 8, Duration::from_millis(200)).await;
        assert!(
            items
                .iter()
                .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id)),
            "trusted sub must receive project-scoped event, got {items:?}"
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

    /// Ephemeral kinds (presence 20001) are fanned out live but NEVER stored.
    #[tokio::test]
    async fn test_ephemeral_presence_not_stored() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let mut sub = bus
            .subscribe(
                crate::bus::Filter::new().kinds([crate::nostr::kinds::toron_kind(
                    crate::nostr::kinds::PRESENCE,
                )]),
            )
            .await
            .expect("sub");

        let payload = crate::nostr::presence::PresencePayload {
            state: crate::nostr::presence::PresenceState::Working,
            status: None,
            ts: 1_700_000_000,
        };
        let ev = crate::nostr::presence::build_presence_event(&keys, &payload, Timestamp::now())
            .expect("presence event");
        let ok = bus.publish(ev.clone()).await.expect("publish");
        assert!(!ok.stored, "ephemeral presence must not be stored");
        assert_eq!(
            bus.store.count().await.expect("count"),
            0,
            "store must stay empty"
        );

        let items = drain(&mut sub, 4, Duration::from_millis(200)).await;
        assert!(
            items
                .iter()
                .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id)),
            "ephemeral presence must be delivered live, got {items:?}"
        );
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

    /// Concurrent subscribers: every subscriber that matches receives the
    /// live event (independent per-subscriber channels, no cross-talk).
    #[tokio::test]
    async fn test_concurrent_subscribers_all_receive() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let ev = signed(&keys, PROJECT_MANIFEST, "fanout", "demo");

        let mut subs = Vec::new();
        for _ in 0..3 {
            let sub = bus
                .subscribe(crate::bus::project_filter("demo"))
                .await
                .expect("sub");
            subs.push(sub);
        }

        bus.publish(ev.clone()).await.expect("publish");

        for (i, sub) in subs.iter_mut().enumerate() {
            let items = drain(sub, 8, Duration::from_millis(200)).await;
            let got = items
                .iter()
                .any(|it| matches!(it, SubscriptionEvent::Event(e) if e.id == ev.id));
            assert!(
                got,
                "{}",
                dump_failure(&ev, "concurrent fanout", &items)
                    + &format!(" (subscriber {i} missed the event)")
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    /// Fanout isolation: a full channel never blocks the bus publish path
    /// (per-subscriber try_send, publisher-side drop). The event stays in the
    /// store, so a later subscription replays it (lossless cursor recovery).
    #[tokio::test]
    async fn test_slow_subscriber_does_not_block_others() {
        let (bus, path) = temp_bus_with(InProcessBusConfig {
            channel_capacity: 2,
            ..Default::default()
        })
        .await;
        let keys = Keys::generate();

        let _overflowed_sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("overflow sub");

        let mut ev_ids = Vec::new();
        for i in 0..6 {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("burst {i}"), "demo");
            let ok = bus.publish(ev.clone()).await.expect("publish never blocks");
            assert!(ok.stored, "event {i} stored");
            ev_ids.push(ev.id);
        }

        let mut fresh = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("fresh sub");
        let items = drain(&mut fresh, 12, Duration::from_millis(300)).await;
        let got: Vec<_> = items
            .iter()
            .filter_map(|it| match it {
                SubscriptionEvent::Event(e) => Some(e.id),
                _ => None,
            })
            .collect();
        for id in &ev_ids {
            assert!(
                got.contains(id),
                "event {id} must be recoverable via replay, got {items:?}"
            );
        }
        assert_eq!(got.len(), 6, "all 6 events recovered, got {items:?}");

        let h = bus.health().await.expect("health");
        assert!(h.ok);
        let _ = std::fs::remove_file(&path);
    }

    /// bd-bus-scan: a fall-behind subscriber's drops are counted EXACTLY on
    /// the bus, the WARN is rate-limited (one per 5 s per subscriber), and
    /// the lossless-replay contract holds — a fresh subscriber recovers every
    /// dropped event from the store.
    #[tokio::test]
    async fn test_slow_subscriber_counts_drops_and_rate_limits_warn() {
        let (bus, path) = temp_bus_with(InProcessBusConfig {
            channel_capacity: 2,
            ..Default::default()
        })
        .await;
        let keys = Keys::generate();

        let mut overflowed = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("overflow sub");
        let _ = drain(&mut overflowed, 64, Duration::from_millis(300)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut ev_ids = Vec::new();
        for i in 0..8 {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("burst {i}"), "demo");
            let ok = bus.publish(ev.clone()).await.expect("publish never blocks");
            assert!(ok.stored, "event {i} stored");
            ev_ids.push(ev.id);
        }
        assert_eq!(
            bus.full_drops(),
            6,
            "every overflow drop is counted exactly"
        );
        assert_eq!(
            bus.full_warns(),
            1,
            "the WARN fires once, then is rate-limited per subscriber"
        );

        let mut fresh = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("fresh sub");
        let items = drain(&mut fresh, 16, Duration::from_millis(300)).await;
        let got: Vec<_> = items
            .iter()
            .filter_map(|it| match it {
                SubscriptionEvent::Event(e) => Some(e.id),
                _ => None,
            })
            .collect();
        for id in &ev_ids {
            assert!(
                got.contains(id),
                "event {id} must be recoverable via replay, got {items:?}"
            );
        }
        assert_eq!(got.len(), 8, "all 8 events recovered, got {items:?}");

        let _ = std::fs::remove_file(&path);
    }

    /// Integration: a kind-30013 published through the bus carries the
    /// dual-emit scoping tags (#t + NIP-32) because the caller applied them;
    /// a kind-1059 gift wrap carries NO project tags.
    #[tokio::test]
    async fn test_dual_tag_presence_absence_through_bus() {
        use crate::bus::scoping::{is_metadata_hiding_kind, with_project_scoping};
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let tags = with_project_scoping(
            crate::nostr::kinds::toron_kind(AGENT_PROFILE),
            vec![Tag::parse(["d", "demo:BlueLake"]).expect("d")],
            Some("demo"),
        );
        let profile_ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(AGENT_PROFILE),
            "{}",
            tags,
            Timestamp::now(),
        )
        .expect("build");
        assert!(!is_metadata_hiding_kind(profile_ev.kind));
        bus.publish(profile_ev.clone())
            .await
            .expect("publish 30013");

        let mut sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("sub");
        let items = drain(&mut sub, 8, Duration::from_millis(200)).await;
        let replayed = items
            .iter()
            .find_map(|it| match it {
                SubscriptionEvent::Event(e) if e.id == profile_ev.id => Some(e),
                _ => None,
            })
            .expect("profile event replayed");
        assert!(
            replayed
                .tags
                .iter()
                .any(|t| t.as_slice() == ["t".to_string(), "demo".to_string()]),
            "{}",
            dump_failure(replayed, "dual-tag #t", &items)
        );
        assert!(
            replayed
                .tags
                .iter()
                .any(|t| t.as_slice() == ["l".to_string(), "demo".to_string(), "toron".to_string()]),
            "{}",
            dump_failure(replayed, "dual-tag NIP-32 l", &items)
        );

        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "secret",
        );
        let wrapped = crate::nostr::gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor)
            .expect("wrap");
        assert!(is_metadata_hiding_kind(wrapped.kind));
        let scoped_wrap_tags =
            with_project_scoping(wrapped.kind, wrapped.tags.clone(), Some("demo"));
        let has_t = scoped_wrap_tags.iter().any(|t| t.as_slice()[0] == "t");
        let has_l = scoped_wrap_tags.iter().any(|t| {
            t.as_slice()
                .starts_with(&["l".to_string(), "demo".to_string()])
        });
        assert!(!has_t, "wrap must not gain a #t tag");
        assert!(!has_l, "wrap must not gain a NIP-32 label");

        let visible: Vec<Vec<String>> = wrapped
            .tags
            .iter()
            .map(|t| t.as_slice().iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(
            visible.len(),
            1,
            "wrap envelope carries only the p tag, got {visible:?}"
        );
        assert_eq!(visible[0][0], "p");
        let _ = std::fs::remove_file(&path);
    }

    /// Failure-detail logging: forge rejection reports the stage.
    #[tokio::test]
    async fn test_forge_rejection_dumps_stage() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let ev = signed(&keys, PROJECT_MANIFEST, "original", "demo");
        let forged = Event::new(
            ev.id,
            ev.pubkey,
            ev.created_at,
            ev.kind,
            ev.tags.clone(),
            "MUTATED",
            ev.sig,
        );
        let err = match bus.publish(forged).await {
            Err(e) => e,
            Ok(_) => panic!("forged event must be rejected"),
        };
        assert!(
            err.to_string().contains("verify") || err.to_string().contains("id"),
            "error must identify the failing stage, got: {err}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
