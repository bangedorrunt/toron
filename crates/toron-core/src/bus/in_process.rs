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
    /// Set to true once the replay task has pushed history + EOSE.
    replay_done: Arc<std::sync::atomic::AtomicBool>,
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
        // bd-741i: prune was never scheduled in production — expired rows
        // piled up, WAL lingered, and the snapshot memo stayed cold. One
        // background tick per PRUNE_INTERVAL is the whole contract.
        let prune_store = store.clone();
        tokio::spawn(async move {
            // Sleep first: tokio interval fires immediately, which would
            // bump mutations on every `open()` and bust the snapshot memo
            // before the first real replay (bd-3gwy / bd-741i).
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
        // bd-ii04: writers emit taxonomy kinds by construction — every
        // Stage maps via STAGE_MAP (proven by inspection_events tests).
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
            // Project-scoped event: subscriber must be scoped to this project.
            if sub.global {
                return false;
            }
            // Subscriber's filter must match the event's own #t.
            return sub.filter.match_event(event, MatchEventOptions::default());
        }
        true
    }

    fn fanout_to(&self, sub: &Subscriber, event: &Event) {
        // Before the replay task finishes (snapshot + delta + EOSE), live
        // fanout must neither re-deliver snapshot members nor race the
        // delta: claim the id in the shared `delivered` set — whichever
        // path inserts first delivers, the other steps aside
        // (exactly-once, bd-subscribe-fanout-gap-race-tgex).
        let id = event.id.to_hex();
        if !sub.replay_done.load(std::sync::atomic::Ordering::Acquire) {
            // Snapshot members are (being) delivered by the replay task.
            if sub.snapshot.contains(&id) {
                return;
            }
            if !sub.delivered.lock().insert(id.clone()) {
                return; // the replay task's delta already delivered it
            }
            // Lossless: if the channel is full we drop publisher-side with a
            // warning (the event remains in the store for cursor-based
            // replay) — and release the claim so the replay task may still
            // deliver it through the delta.
            match sub.tx.try_send(SubscriptionEvent::Event(event.clone())) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.delivered.lock().remove(&id);
                    self.note_full_drop(sub, event);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Subscriber dropped its receiver; remove the subscription.
                    self.subs.write().remove(&sub.id);
                }
            }
            return;
        }
        // Post-EOSE: single delivery path, no dedup bookkeeping needed.
        match sub.tx.try_send(SubscriptionEvent::Event(event.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.note_full_drop(sub, event);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Subscriber dropped its receiver; remove the subscription.
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

        // Global = filter has no project scoping (#t via hashtags or #l label)
        // AND not trusted. A trusted subscriber is always project-scoped.
        use crate::nostr::SingleLetterTag;
        let t_tag = SingleLetterTag::LOWERCASE_T;
        let l_tag = SingleLetterTag::LOWERCASE_L;
        let global = !trusted
            && !filter.generic_tags.contains_key(&t_tag)
            && !filter.generic_tags.contains_key(&l_tag);

        // Take the history snapshot synchronously (fast DB read). The
        // snapshot is the exactly-once base; the background task's delta
        // phase below closes the snapshot→map-insert gap; anything published
        // once the subscriber is in the fanout map is delivered live.
        let snapshot = self
            .store
            .replay(&filter)
            .await
            .map_err(|e| BusError::Subscription(format!("replay failure: {e}")))?;
        let snapshot_ids: Arc<HashSet<String>> =
            Arc::new(snapshot.iter().map(|ev| ev.id.to_hex()).collect());
        let delivered: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let replay_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Push the snapshot, then the DELTA, then EOSE in a background task
        // so `subscribe` never blocks on a small channel (lossless
        // backpressure inside the task). The delta (bd-subscribe-fanout-gap-
        // race-tgex): events stored after the snapshot read but published
        // before this subscriber entered the fanout map were missed by both
        // paths. Store writes precede fanout, so a full re-read now contains
        // every gap event; the shared `delivered` set dedups against live
        // fanout (exactly-once preserved).
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
                // Access gate applies to replay too: a global subscription
                // never receives project-scoped history.
                if global_clone && Self::project_slug(&ev).is_some() {
                    continue;
                }
                if replay_tx.send(SubscriptionEvent::Event(ev)).await.is_err() {
                    return;
                }
            }
            // Delta: everything the store now holds beyond the snapshot.
            // Best-effort with a warning on read failure — live fanout and
            // future stores still cover forward traffic.
            match store_clone.replay(&filter_clone).await {
                Ok(delta) => {
                    for ev in delta {
                        if global_clone && Self::project_slug(&ev).is_some() {
                            continue;
                        }
                        let id = ev.id.to_hex();
                        if snapshot_ids_clone.contains(&id) {
                            continue; // replayed in the snapshot phase
                        }
                        if !delivered_clone.lock().insert(id) {
                            continue; // live fanout already delivered it
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
            let _ = replay_tx.send(SubscriptionEvent::Eose).await;
            replay_done_clone.store(true, std::sync::atomic::Ordering::Release);
            // Single-path delivery from here on; the dedup set is dead.
            delivered_clone.lock().clear();
        });

        let sub = Arc::new(Subscriber {
            id: id.clone(),
            filter,
            tx,
            global,
            snapshot: snapshot_ids,
            delivered,
            replay_done,
            // Pre-dated so the FIRST full drop warns immediately.
            last_full_warn: Arc::new(Mutex::new(
                std::time::Instant::now()
                    .checked_sub(Subscriber::FULL_WARN_INTERVAL)
                    .unwrap_or_else(std::time::Instant::now),
            )),
        });
        self.subs.write().insert(id.clone(), sub);

        // Unregister on the caller's scope, not on the next publish. The
        // `Closed` arm in `fanout_to` cannot be the release path for an
        // access-gated subscriber: `access_gate` refuses it every scoped
        // event, so no publish ever reaches it and the entry (plus its
        // capacity-1024 mpsc block) would pin for the life of the daemon.
        // parking_lot locks synchronously, so this is safe from `Drop`
        // outside a runtime context (bd-cqo2).
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
        // 1. Verify: reject forged/mutated events (wrong id or sig).
        //    F23.BUS-DEDUP (batch-fix): shared canonical mapping in bus/common.rs.
        if let Err(e) = crate::bus::common::verify_publish_event(&event) {
            self.stage_log(Stage::Verify, &event, &e.to_string());
            return Err(e);
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

        // 3. Ephemeral kinds (20000–29999, e.g. presence 20001) are broadcast
        //    but NOT stored. GS-062 / Buzz #7532 pattern: fail closed without
        //    Redis — after verify+auth, confirm SQLite store health (`SELECT 1`).
        //    Unreachable store → presence_admit(false) → refuse fanout.
        if crate::bus::is_ephemeral_kind(event.kind) {
            let store_ok = self.store.is_healthy().await;
            if let Err(reason) = crate::bus::convergence::presence_admit(store_ok) {
                self.stage_log(Stage::Auth, &event, reason);
                return Err(BusError::Publish(reason.into()));
            }
            self.stage_log(Stage::Dedup, &event, "ephemeral kind, not stored");
            return self.fanout(event, false);
        }

        // 3. Dedup: store by event id (ON CONFLICT DO NOTHING). Addressable
        //    kinds (NIP-33 parameterized-replaceable, e.g. kind-30031 file
        //    reservations) additionally collapse to the latest event per
        //    (kind, author, d_tag) — without this a renewal storm accumulates
        //    hundreds of superseded events on one coordinate, which overflows
        //    replay's window and hides the newest state (bd-cumj).
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
}

// Allow constructing a bus for tests without exposing internals.
impl InProcessBus {
    /// Count of currently registered subscriptions (diagnostics).
    pub fn subscriber_count(&self) -> usize {
        self.subs.read().len()
    }

    /// Access-gated fanout to live subscribers (shared by stored + ephemeral
    /// publish paths).
    fn fanout(&self, event: Event, stored: bool) -> Result<PublishOk, BusError> {
        // 4. Access-gated fanout to live subscribers.
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
        // First subscribe: publishes already memoized each event (inserts
        // warm the hot cache), so this snapshot may parse nothing — the
        // cold-parse path is covered by the store-level gate.
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

        // Second subscribe: identical filter, no new events.
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

        // A NEW event is parsed exactly once and reaches BOTH paths.
        let fresh = signed(&keys, PROJECT_MANIFEST, "fresh", "demo");
        bus.publish(fresh).await.expect("publish fresh");
        let mut s3 = bus.subscribe(filter).await.expect("sub 3");
        let third = drain(&mut s3, 32, Duration::from_millis(400)).await;
        let third_events = third
            .iter()
            .filter(|e| matches!(e, SubscriptionEvent::Event(_)))
            .count();
        assert_eq!(third_events, n + 1);
        // The publish bumped the mutation version, so the third subscribe
        // runs exactly ONE fresh id-scan; post-mutation replays memoize.
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

        // Seed history so the subscribe-time snapshot read takes real time.
        let mut expected: Vec<String> = Vec::with_capacity(500);
        for i in 0..300 {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("seed {i}"), "demo");
            bus.publish(ev.clone()).await.expect("seed publish");
            expected.push(ev.id.to_hex());
        }

        // Publisher streams 200 more events while subscribe() races through
        // its snapshot-read → map-insert window.
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

        // Drain everything: snapshot + delta + EOSE + live, until quiet.
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

        // Measure only up to the moment the live event arrives (Eose precedes
        // it; read one item at a time with a tight per-item timeout).
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
        // Smoke ceiling, not a latency SLO: under parallel test load a
        // scheduler stall routinely exceeds 50ms (bd-lib-suite-flake-j6jg).
        // 2s still catches pathological transport regressions (relay
        // round-trip, lost wake) while tolerating load jitter.
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

    /// P11.1 (bd-foh8.1): a TRUSTED subscription receives project-scoped
    /// events even with an unscoped filter — the reactive-ingress bypass.
    #[tokio::test]
    async fn test_trusted_sub_receives_project_scoped() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        // Trusted ingress: empty filter, no scoping — must still see scoped
        // events (the workflow-trigger loop's subscription shape).
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

        // Build a presence event (no #t scoping → global sub receives it).
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

        // Live delivery still works.
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

        // Three concurrent subscribers on the same project filter.
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

        // A subscriber that never drains → its channel fills at capacity 2.
        let _overflowed_sub = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("overflow sub");

        // Publish 6 events; publish must NEVER block despite the full channel.
        let mut ev_ids = Vec::new();
        for i in 0..6 {
            let ev = signed(&keys, PROJECT_MANIFEST, &format!("burst {i}"), "demo");
            let ok = bus.publish(ev.clone()).await.expect("publish never blocks");
            assert!(ok.stored, "event {i} stored");
            ev_ids.push(ev.id);
        }

        // A new subscriber replays ALL 6 from the store (nothing lost).
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

        // Bus still healthy.
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

        // A subscriber that never drains → its channel fills at capacity 2.
        let mut overflowed = bus
            .subscribe(crate::bus::project_filter("demo"))
            .await
            .expect("overflow sub");
        // Settle: drain history + EOSE and let the replay task finish, so the
        // bus is on the single-path (post-EOSE) delivery phase and the drop
        // accounting below is deterministic.
        let _ = drain(&mut overflowed, 64, Duration::from_millis(300)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Publish 8 events: the first 2 fit the empty channel, the remaining
        // 6 drop publisher-side — every drop exactly counted on the bus.
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

        // Lossless: a fresh subscriber replays all 8 from the store.
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

        // 30013 with scoping tags applied by the caller (identity flow).
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

        // Recover from store replay and assert dual tags survived the bus.
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

        // Kind-1059 gift wrap: no project tags on the envelope.
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
        // Wrap is metadata-hiding → no scoping tags applied by the helper.
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

        // And the raw envelope (as produced by gift_wrap) has only the p tag.
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
        assert!(visible[0][0] == "p");
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
        // Stage detail must be in the error (self-diagnosing).
        assert!(
            err.to_string().contains("verify") || err.to_string().contains("id"),
            "error must identify the failing stage, got: {err}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
