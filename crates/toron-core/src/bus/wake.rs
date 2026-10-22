// governed-by: ADR-0036
#![forbid(unsafe_code)]

//! Wake bus socket server, slice A (ADR-0036 Specs 6-7).
//!
//! Daemon-held `subscribe_trusted` fan-out over `wake.sock` (JSONL frames).
//! Effect rules: no I/O in constructors (`new` is pure, `start()` performs);
//! clock injected (scripted fakes in tests, zero `tokio::now`/real sleeps);
//! RAII owns sockets/tasks on all paths incl. cancel; every wait bounded
//! (handshake/heartbeat timeouts, queue cap, shed+resync with exact counters);
//! typed errors only.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::bus::{Bus, BusError, Filter};
use crate::nostr::{Event, Kind, MatchEventOptions};

/// Wake kinds held by the daemon (ADR-0036 §6 example + 1059/43004 for
/// `wait --corr` direct-receipt parity with `WAIT_WAKE_KINDS`).
pub const WAKE_KINDS: &[u16] = &[9, 14, 1059, 20001, 30013, 43001, 43004, 30031, 39002];

/// Default per-subscriber queue cap (mirrors `Subscription::DEFAULT_CAPACITY`).
pub const WAKE_QUEUE_CAP: usize = 1024;

/// Default handshake timeout (a subscriber that never sends its subscribe
/// line is dropped, never parked).
pub const WAKE_HANDSHAKE_TIMEOUT_MS: u64 = 5_000;
/// bd-ojw3: how long a single frame write may take before the subscriber is
/// declared dead.
///
/// The write was unbounded, so a subscriber that stopped reading left the
/// daemon's write blocked forever and its fd held for the life of the daemon:
/// one stalled peer became a daemon-wide EMFILE outage. A write that cannot
/// finish is a failure, and a failure is the signal that releases the fd.
pub const WAKE_WRITE_TIMEOUT_MS: u64 = 60_000;

/// Default heartbeat cadence on a quiet bus (quiet-vs-deaf distinction).
pub const WAKE_HEARTBEAT_MS: u64 = 15_000;

/// Default store-delta poll cadence (bd-d41q): the pump re-reads the store
/// this often so writes from other processes (CLI plane commands on the
/// shared store) fan out live. Matches the pre-sidecar 1s snapshot
/// heritage. The re-read itself is gated on the store's `bus_meta` mutation
/// version ([`crate::bus::Bus::store_version`]), so the cadence bounds how
/// quickly a foreign write is noticed, not how often the store is scanned
/// (bd-qpm0).
pub const WAKE_POLL_MS: u64 = 1_000;

/// Bound for the pump's forwarded-id set (bd-d41q): the poll re-reads the
/// whole WAKE_KINDS history each tick, so forwarded ids are remembered to
/// avoid refanout. 10k ids cover any sane tick at ~300B each.
const SEEN_CAP: usize = 10_000;

/// Finished-subscriber detail retained for [`WakeHandle::stats`].
///
/// The aggregate is served from running totals, not from this window, so the
/// cap costs no metric accuracy — it only bounds how far back the per-
/// subscriber breakdown reaches (bd-qpm0).
const STATS_CAP: usize = 1_024;

/// Clock abstraction: wall-clock enters only as `Msg` payload equivalent —
/// here, as an injected `now_ms()`. Unit tests use [`ManualClock`].
pub trait WakeClock: Send + Sync + 'static {
    /// Current time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// Production clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl WakeClock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// Scripted fake clock: tests advance it manually, never sleep.
#[derive(Debug, Default)]
pub struct ManualClock {
    now: AtomicU64,
}

impl ManualClock {
    /// New fake clock at `start_ms`.
    pub const fn new(start_ms: u64) -> Self {
        Self {
            now: AtomicU64::new(start_ms),
        }
    }

    /// Advance the fake clock.
    pub fn advance(&self, delta_ms: u64) {
        self.now.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl WakeClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
}

/// Typed errors for the wake socket boundary. No string-throw.
#[derive(Debug, thiserror::Error)]
pub enum WakeError {
    /// Another live daemon already serves this socket (identity-checked).
    #[error("wake.sock already served (live peer holds {0})")]
    AlreadyServing(String),
    /// Bind/listen failure.
    #[error("wake.sock bind failed: {0}")]
    Bind(String),
    /// Subscriber never sent its subscribe line in time.
    #[error("wake handshake timeout after {0}ms")]
    HandshakeTimeout(u64),
    /// Heartbeat write failure (deaf watcher, counted then dropped).
    #[error("wake heartbeat failed: {0}")]
    Heartbeat(String),
    /// Underlying bus failure.
    #[error("wake bus failure: {0}")]
    Bus(#[from] BusError),
    /// Frame serialization failure.
    #[error("wake frame codec failure: {0}")]
    Codec(String),
    /// Transport I/O failure.
    #[error("wake transport failure: {0}")]
    Transport(String),
}

/// Wire frames, daemon → client, one JSONL line each (ADR-0036 §6).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WakeFrame {
    /// A matched bus event (existing bus event JSON, untouched).
    Event(serde_json::Value),
    /// End of replay: live events follow; carries the resume cursor.
    Eose {
        /// Opaque resume cursor.
        cursor: String,
    },
    /// Quiet-bus heartbeat (same cadence rule as watchbeat).
    Heartbeat {
        /// Clock ms at send.
        ts_ms: u64,
    },
    /// Slow consumer evicted: replay from cursor, then re-subscribe.
    Shed {
        /// Cursor to replay from.
        resync_from: String,
    },
    /// Explicit gap (bd-wake-live-burst-loss-j86v): stored events after the
    /// subscriber's cursor were evicted before it read them, up to and
    /// including `through`. Everything still stored is delivered after this
    /// frame. A subscriber never loses events without one of these.
    #[serde(rename = "events_lost")]
    EventsLost {
        /// The cursor the subscriber resumed from.
        from: String,
        /// Last cursor that may be missing (`unknown` when the resume cursor
        /// itself is no longer known to the store).
        through: String,
    },
}

impl WakeFrame {
    /// Encode one JSONL line (no trailing newline included by caller contract:
    /// use [`WakeFrame::encode_line`]).
    pub fn encode_line(&self) -> Result<String, WakeError> {
        let tag = match self {
            Self::Event(_) => "event",
            Self::Eose { .. } => "eose",
            Self::Heartbeat { .. } => "heartbeat",
            Self::Shed { .. } => "shed",
            Self::EventsLost { .. } => "events_lost",
        };
        let inner = serde_json::to_value(self).map_err(|e| WakeError::Codec(e.to_string()))?;
        let body = inner.get(tag).cloned().unwrap_or(inner);
        serde_json::to_string(&serde_json::json!({ tag: body }))
            .map(|mut s| {
                s.push('\n');
                s
            })
            .map_err(|e| WakeError::Codec(e.to_string()))
    }

    /// Decode one JSONL line.
    pub fn decode_line(line: &str) -> Result<Self, WakeError> {
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| WakeError::Codec(e.to_string()))?;
        if let Some(b) = v.get("event") {
            return Ok(Self::Event(b.clone()));
        }
        if let Some(b) = v.get("eose") {
            let cursor = b
                .get("cursor")
                .and_then(|c| c.as_str())
                .ok_or_else(|| WakeError::Codec("eose without cursor".to_string()))?;
            return Ok(Self::Eose {
                cursor: cursor.to_string(),
            });
        }
        if let Some(b) = v.get("heartbeat") {
            let ts_ms = b
                .get("ts_ms")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            return Ok(Self::Heartbeat { ts_ms });
        }
        if let Some(b) = v.get("shed") {
            let resync_from = b
                .get("resync_from")
                .and_then(|c| c.as_str())
                .ok_or_else(|| WakeError::Codec("shed without resync_from".to_string()))?;
            return Ok(Self::Shed {
                resync_from: resync_from.to_string(),
            });
        }
        if let Some(b) = v.get("events_lost") {
            let field = |name: &str| {
                b.get(name)
                    .and_then(|c| c.as_str())
                    .map(str::to_string)
                    .ok_or_else(|| WakeError::Codec(format!("events_lost without {name}")))
            };
            return Ok(Self::EventsLost {
                from: field("from")?,
                through: field("through")?,
            });
        }
        Err(WakeError::Codec("unknown wake frame".to_string()))
    }
}

/// Subscribe request, client → daemon, one line (ADR-0036 §6).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WakeSubscribe {
    /// Kind filter (defaults to [`WAKE_KINDS`]).
    #[serde(default)]
    pub kinds: Vec<u16>,
    /// Resume cursor (null = replay everything stored, then live). The server
    /// mints `seq:<n>` (arrival order); the older `<created_at>:<event_id>`
    /// form is still accepted. Treat it as opaque.
    #[serde(default)]
    pub since_cursor: Option<String>,
    /// Optional project scope (`#t`).
    #[serde(default)]
    pub project: Option<String>,
}

impl WakeSubscribe {
    /// Parse one subscribe line.
    pub fn parse_line(line: &str) -> Result<Self, WakeError> {
        let v: serde_json::Value =
            serde_json::from_str(line).map_err(|e| WakeError::Codec(e.to_string()))?;
        let sub = v.get("subscribe").ok_or_else(|| {
            WakeError::Codec("first line must be a subscribe request".to_string())
        })?;
        let mut req: Self =
            serde_json::from_value(sub.clone()).map_err(|e| WakeError::Codec(e.to_string()))?;
        if req.kinds.is_empty() {
            req.kinds = WAKE_KINDS.to_vec();
        }
        Ok(req)
    }

    /// Lower into a bus [`Filter`].
    pub fn filter(&self) -> Filter {
        let kinds = self
            .kinds
            .iter()
            .map(|k| Kind::from(*k))
            .collect::<Vec<_>>();
        let f = Filter::new().kinds(kinds);
        match &self.project {
            Some(slug) => f.hashtag(slug.clone()),
            None => f,
        }
    }
}

/// Exact per-subscriber counters (shed honesty: counted, never silent).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SubscriberStats {
    /// Frames delivered.
    pub delivered: u64,
    /// Frames shed (queue full → `Shed` frame sent, subscriber evicted).
    pub shed: u64,
    /// Cursor the shed consumer must replay from (set iff `shed > 0`).
    pub resync_from: Option<String>,
    /// Heartbeats successfully written (quiet bus proof).
    pub heartbeats_ok: u64,
    /// Heartbeat write failures (deaf watcher — counted, then dropped).
    pub heartbeat_deaf: u64,
}

/// Pure server configuration. No I/O here — [`WakeServer::start`] performs.
#[derive(Debug, Clone)]
pub struct WakeConfig {
    /// Socket path (`$root/wake.sock`).
    pub sock_path: PathBuf,
    /// Per-subscriber queue cap.
    pub queue_cap: usize,
    /// Handshake timeout in ms.
    pub handshake_timeout_ms: u64,
    /// Heartbeat interval in ms.
    pub heartbeat_ms: u64,
    /// Store-delta poll interval in ms (bd-d41q): 0 disables the poll
    /// (live fanout only — foreign store writes stay invisible).
    pub poll_interval_ms: u64,
    /// How long a single frame write may take before the subscriber is
    /// declared dead (bd-ojw3).
    pub write_timeout_ms: u64,
}

impl WakeConfig {
    /// Pure constructor: no I/O, no clock read.
    pub fn new(sock_path: impl Into<PathBuf>) -> Self {
        Self {
            sock_path: sock_path.into(),
            queue_cap: WAKE_QUEUE_CAP,
            handshake_timeout_ms: WAKE_HANDSHAKE_TIMEOUT_MS,
            heartbeat_ms: WAKE_HEARTBEAT_MS,
            poll_interval_ms: WAKE_POLL_MS,
            write_timeout_ms: WAKE_WRITE_TIMEOUT_MS,
        }
    }
}

/// Default socket path for a mailbox root (`<root>/wake.sock`, beside
/// `serve.pid`).
pub fn wake_sock_path(root: &Path) -> PathBuf {
    root.join("wake.sock")
}

/// Wake socket server. Construct-then-run: `new` performs no I/O.
pub struct WakeServer {
    config: WakeConfig,
    bus: Arc<dyn Bus>,
    clock: Arc<dyn WakeClock>,
}

impl WakeServer {
    /// Pure constructor: no I/O, no bind, no clock read.
    pub fn new(config: WakeConfig, bus: Arc<dyn Bus>, clock: Arc<dyn WakeClock>) -> Self {
        Self { config, bus, clock }
    }

    /// Config accessor (no I/O).
    pub const fn config(&self) -> &WakeConfig {
        &self.config
    }

    /// Bind + serve. Owns the socket file and all tasks via RAII
    /// ([`WakeHandle`]): drop the handle and the socket file + tasks go away,
    /// including on cancel.
    pub async fn start(self) -> Result<WakeHandle, WakeError> {
        let path = self.config.sock_path.clone();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| WakeError::Bind(e.to_string()))?;
        }
        // ponytail: stale-file probe is a connect check, not a pid read;
        // pid reuse can't fake a live listener.
        if path.exists() {
            match UnixStream::connect(&path).await {
                Ok(_) => {
                    return Err(WakeError::AlreadyServing(path.display().to_string()));
                }
                Err(_) => {
                    let _ = tokio::fs::remove_file(&path).await;
                }
            }
        }
        let listener = UnixListener::bind(&path).map_err(|e| WakeError::Bind(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perm = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&path, perm).map_err(|e| WakeError::Bind(e.to_string()))?;
        }
        // One held trusted subscription: the daemon's single fan-out source.
        let filter = Filter::new().kinds(WAKE_KINDS.iter().map(|k| Kind::from(*k)));
        let sub = self.bus.subscribe_trusted(filter).await?;
        let handle = WakeHandle::serve(
            listener,
            path,
            sub,
            self.bus.clone(),
            self.config.clone(),
            self.clock,
        );
        Ok(handle)
    }
}

/// Exported shed-counter snapshot (wave 4, bd-ud7k): exact sums over
/// finished subscribers plus the live count. Shed honesty leaves the
/// process — this is the shape a `/health` or metrics scrape reads.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WakeStats {
    /// Finished subscriber tasks recorded.
    pub subscribers_finished: usize,
    /// Currently connected subscribers.
    pub subscribers_live: usize,
    /// Sum of frames delivered across finished subscribers.
    pub delivered: u64,
    /// Sum of shed frames across finished subscribers.
    pub shed: u64,
    /// Sum of successful quiet-bus heartbeats.
    pub heartbeats_ok: u64,
    /// Sum of heartbeat write failures (deaf watchers).
    pub heartbeat_deaf: u64,
}

/// RAII serving handle: dropping removes the socket file and aborts tasks.
#[derive(Debug)]
pub struct WakeHandle {
    sock_path: PathBuf,
    tasks: tokio::task::JoinSet<()>,
    /// Bounded window of recent finished-subscriber detail (bd-qpm0).
    recent: Arc<std::sync::Mutex<std::collections::VecDeque<SubscriberStats>>>,
    /// Monotonic running totals. Kept separately from `recent` so the
    /// aggregate stays exact and O(1) no matter how long the sidecar runs.
    totals: Arc<std::sync::Mutex<Totals>>,
    live: Arc<AtomicU64>,
}

/// Running totals folded on every finished subscriber (bd-qpm0).
#[derive(Debug, Default, Clone, Copy)]
struct Totals {
    finished: usize,
    delivered: u64,
    shed: u64,
    heartbeats_ok: u64,
    heartbeat_deaf: u64,
}

impl Totals {
    const fn fold(&mut self, s: &SubscriberStats) {
        self.finished += 1;
        self.delivered = self.delivered.saturating_add(s.delivered);
        self.shed = self.shed.saturating_add(s.shed);
        self.heartbeats_ok = self.heartbeats_ok.saturating_add(s.heartbeats_ok);
        self.heartbeat_deaf = self.heartbeat_deaf.saturating_add(s.heartbeat_deaf);
    }
}

impl WakeHandle {
    fn serve(
        listener: UnixListener,
        sock_path: PathBuf,
        mut bus_sub: crate::bus::Subscription,
        bus: Arc<dyn Bus>,
        config: WakeConfig,
        clock: Arc<dyn WakeClock>,
    ) -> Self {
        let recent = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
        let totals = Arc::new(std::sync::Mutex::new(Totals::default()));
        let live = Arc::new(AtomicU64::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        let bus_tx = tokio::sync::broadcast::channel::<String>(config.queue_cap.max(1)).0;
        let subscriber_bus = bus.clone();
        // Fan-out pump: bus → broadcast (bounded; lagging subscribers shed).
        // bd-d41q: the held subscription only observes THIS process's bus.
        // Writes from other processes on the shared store (CLI plane
        // commands) never traverse it, so every poll_interval the pump
        // re-reads the store through a short subscription and fans out
        // ids not yet forwarded (bounded seen-set). bd-qpm0: that re-read
        // is gated on the store's mutation version, so a quiet tick costs
        // one indexed read instead of a subscription. No poll when nobody
        // listens; 0 interval disables (live fanout only).
        {
            let bus_tx = bus_tx.clone();
            let poll_filter = Filter::new().kinds(WAKE_KINDS.iter().map(|k| Kind::from(*k)));
            let poll_every = std::time::Duration::from_millis(config.poll_interval_ms);
            let poll_on = config.poll_interval_ms > 0;
            tasks.spawn(async move {
                let mut cursor: u64 = 0;
                // Last store version we re-read at (bd-qpm0). `None` = this
                // backend cannot answer cheaply, so every tick polls blind.
                let mut last_store_version: Option<i64> = None;
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                let mut seen_order: std::collections::VecDeque<String> =
                    std::collections::VecDeque::new();
                let mut remember = |id: String| -> bool {
                    if seen.contains(&id) {
                        return false;
                    }
                    seen.insert(id.clone());
                    seen_order.push_back(id);
                    while seen_order.len() > SEEN_CAP {
                        if let Some(old) = seen_order.pop_front() {
                            seen.remove(&old);
                        }
                    }
                    true
                };
                let forward = |payload: serde_json::Value| {
                    let frame = WakeFrame::Event(payload);
                    if let Ok(line) = frame.encode_line() {
                        let _ = bus_tx.send(line);
                    }
                };
                loop {
                    tokio::select! {
                        biased;
                        ev = bus_sub.next() => {
                            match ev {
                                Some(crate::bus::SubscriptionEvent::Event(event)) => {
                                    cursor += 1;
                                    remember(event.id.to_hex());
                                    let payload = serde_json::to_value(&event)
                                        .unwrap_or(serde_json::Value::Null);
                                    forward(payload);
                                }
                                Some(crate::bus::SubscriptionEvent::Eose) => {
                                    let frame = WakeFrame::Eose {
                                        cursor: format!("{cursor}"),
                                    };
                                    if let Ok(line) = frame.encode_line() {
                                        let _ = bus_tx.send(line);
                                    }
                                }
                                _ => break,
                            }
                        }
                        _ = tokio::time::sleep(poll_every), if poll_on => {
                            if bus_tx.receiver_count() == 0 {
                                continue;
                            }
                            // Cheapest possible quiet tick: ask the store
                            // whether anything moved since the last poll.
                            // Minting the subscription below costs two store
                            // scans plus a spawned task, so skipping it while
                            // the version is unchanged is the whole point.
                            let version = bus.store_version().await;
                            if let (Some(now), Some(prev)) = (version, last_store_version)
                                && now == prev
                            {
                                continue;
                            }
                            last_store_version = version;
                            let polled = match bus.subscribe_trusted(poll_filter.clone()).await {
                                Ok(mut tmp) => {
                                    let mut fresh = Vec::new();
                                    while let Some(x) = tmp.next().await {
                                        match x {
                                            crate::bus::SubscriptionEvent::Event(e) => {
                                                fresh.push(e);
                                            }
                                            _ => break,
                                        }
                                    }
                                    fresh
                                }
                                Err(e) => {
                                    tracing::warn!("wake store poll failed: {e}");
                                    Vec::new()
                                }
                            };
                            for event in polled {
                                let id = event.id.to_hex();
                                if !remember(id) {
                                    continue;
                                }
                                cursor += 1;
                                let payload = serde_json::to_value(&event)
                                    .unwrap_or(serde_json::Value::Null);
                                forward(payload);
                            }
                        }
                    }
                }
            });
        }
        // Accept loop: one task per subscriber, each bounded.
        {
            let recent = recent.clone();
            let totals = totals.clone();
            let live_accept = live.clone();
            let bus = subscriber_bus;
            tasks.spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let rx = bus_tx.subscribe();
                    let recent = recent.clone();
                    let totals = totals.clone();
                    let live = live_accept.clone();
                    let clock = clock.clone();
                    let cfg = config.clone();
                    let bus = bus.clone();
                    live.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(async move {
                        let st = serve_subscriber(stream, rx, cfg, clock, bus).await;
                        // Fold into the exact running totals first: the
                        // aggregate must never be capped by the detail
                        // window (bd-qpm0).
                        if let Ok(mut t) = totals.lock() {
                            t.fold(&st);
                        }
                        if let Ok(mut window) = recent.lock() {
                            window.push_back(st);
                            while window.len() > STATS_CAP {
                                window.pop_front();
                            }
                        }
                        live.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            });
        }
        Self {
            sock_path,
            tasks,
            recent,
            totals,
            live,
        }
    }

    /// Snapshot of finished-subscriber stats, most recent last.
    ///
    /// Bounded to the last [`STATS_CAP`] subscribers; older detail is
    /// dropped. The aggregate in [`Self::aggregate_stats`] is unaffected and
    /// stays exact (bd-qpm0).
    pub fn stats(&self) -> Vec<SubscriberStats> {
        self.recent
            .lock()
            .map(|w| w.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Exported aggregate (wave 4): exact sums + live count.
    ///
    /// O(1) in the number of subscribers served, from running totals rather
    /// than a scan of retained detail (bd-qpm0).
    pub fn aggregate_stats(&self) -> WakeStats {
        let t = self.totals.lock().map(|t| *t).unwrap_or_default();
        WakeStats {
            subscribers_finished: t.finished,
            subscribers_live: self.live.load(Ordering::SeqCst) as usize,
            delivered: t.delivered,
            shed: t.shed,
            heartbeats_ok: t.heartbeats_ok,
            heartbeat_deaf: t.heartbeat_deaf,
        }
    }
}

impl Drop for WakeHandle {
    fn drop(&mut self) {
        self.tasks.abort_all();
        let _ = std::fs::remove_file(&self.sock_path);
    }
}

/// Serve one subscriber: handshake (bounded) → replay EOSE → live fan-out
/// with heartbeat (bounded) → shed-and-resync on overflow with exact counts.
///
/// Always returns the accumulated stats, even on error paths (wave 4
/// export honesty: a dropped connection must not discard its counters).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WakeCursor {
    created_at: u64,
    event_id: String,
}

impl WakeCursor {
    const fn origin() -> Self {
        Self {
            created_at: 0,
            event_id: String::new(),
        }
    }

    fn parse(value: &str) -> Result<Self, WakeError> {
        if value == "head" {
            return Ok(Self::origin());
        }
        let (created_at, event_id) = value
            .split_once(':')
            .ok_or_else(|| WakeError::Codec("cursor must be <created_at>:<event_id>".into()))?;
        let created_at = created_at
            .parse::<u64>()
            .map_err(|e| WakeError::Codec(format!("invalid cursor timestamp: {e}")))?;
        if event_id.is_empty() && created_at == 0 {
            return Ok(Self::origin());
        }
        if event_id.len() != 64 || !event_id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(WakeError::Codec("invalid cursor event id".into()));
        }
        Ok(Self {
            created_at,
            event_id: event_id.to_ascii_lowercase(),
        })
    }

    fn for_event(event: &Event) -> Self {
        Self {
            created_at: event.created_at.as_secs(),
            event_id: event.id.to_hex(),
        }
    }
}

impl std::fmt::Display for WakeCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.created_at, self.event_id)
    }
}

async fn write_matching_event(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    event: &Event,
    filter: &Filter,
    cursor: &mut WakeCursor,
    write_timeout_ms: u64,
) -> Result<bool, WakeError> {
    if !filter.match_event(event, MatchEventOptions::default()) {
        return Ok(false);
    }
    let event_cursor = WakeCursor::for_event(event);
    if event_cursor <= *cursor {
        return Ok(false);
    }
    let payload = serde_json::to_value(event).map_err(|e| WakeError::Codec(e.to_string()))?;
    let line = WakeFrame::Event(payload).encode_line()?;
    // bd-ojw3: bounded like every other frame write - an unbounded write to a
    // peer that stopped reading held its fd for the life of the daemon.
    tokio::time::timeout(
        std::time::Duration::from_millis(write_timeout_ms),
        writer.write_all(line.as_bytes()),
    )
    .await
    .map_err(|_| WakeError::Transport(format!("write timed out after {write_timeout_ms}ms")))?
    .map_err(|e| WakeError::Transport(e.to_string()))?;
    *cursor = event_cursor;
    Ok(true)
}

/// Where a subscriber resumes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Resume {
    /// No cursor (or `head`): everything stored, then live.
    Origin,
    /// `seq:<n>`: strictly after arrival `n` (the cursor this server mints).
    Seq(u64),
    /// `<created_at>:<event_id>`: the pre-arrival-log cursor, still accepted
    /// and placed by looking its event up in the arrival log.
    Legacy(WakeCursor),
}

impl Resume {
    fn parse(value: &str) -> Result<Self, WakeError> {
        if let Some(n) = value.strip_prefix("seq:") {
            return n
                .parse::<u64>()
                .map(Self::Seq)
                .map_err(|e| WakeError::Codec(format!("invalid seq cursor: {e}")));
        }
        let cursor = WakeCursor::parse(value)?;
        Ok(if cursor == WakeCursor::origin() {
            Self::Origin
        } else {
            Self::Legacy(cursor)
        })
    }
}

/// Rows per arrival-log page.
const ARRIVAL_PAGE: usize = 512;

async fn write_frame(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &WakeFrame,
) -> Result<(), WakeError> {
    write_frame_within(writer, frame, WAKE_WRITE_TIMEOUT_MS).await
}

/// [`write_frame`] with an explicit bound on how long the write may take.
///
/// bd-ojw3: the write was unbounded. The daemon dropped the subscriber's READ
/// half after the handshake, so it could not see the peer's EOF; the only
/// signal that a subscriber was gone was a failed write. An unbounded write
/// meant a peer that stopped reading left the write blocked forever and its fd
/// held for the life of the daemon - one stalled peer became a daemon-wide
/// EMFILE outage. A write that cannot finish inside the bound is a DEAD
/// subscriber: the write fails, the subscriber returns, and its fd is
/// released.
async fn write_frame_within(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: &WakeFrame,
    timeout_ms: u64,
) -> Result<(), WakeError> {
    let line = frame.encode_line()?;
    tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        writer.write_all(line.as_bytes()),
    )
    .await
    .map_err(|_| WakeError::Transport(format!("write timed out after {timeout_ms}ms")))?
    .map_err(|e| WakeError::Transport(e.to_string()))
}

/// Write every stored arrival after `*after` that matches `filter`, paging
/// until the log is exhausted, and advance `*after` past every row read
/// (matching or not). `floor` skips rows whose `created_at` is below it (the
/// placement fallback for a legacy cursor the store no longer knows).
async fn drain_arrivals(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    bus: &dyn Bus,
    filter: &Filter,
    kinds: &[u16],
    after: &mut u64,
    floor: Option<u64>,
    stats: &mut SubscriberStats,
    write_timeout_ms: u64,
) -> Result<(), WakeError> {
    loop {
        let page = bus
            .arrivals_after(kinds, *after, ARRIVAL_PAGE)
            .await?
            .ok_or_else(|| WakeError::Transport("arrival log unavailable".to_string()))?;
        let full = page.len() == ARRIVAL_PAGE;
        for (seq, event) in page {
            *after = seq;
            if floor.is_some_and(|f| event.created_at.as_secs() < f) {
                continue;
            }
            if !filter.match_event(&event, MatchEventOptions::default()) {
                continue;
            }
            let payload =
                serde_json::to_value(&event).map_err(|e| WakeError::Codec(e.to_string()))?;
            write_frame_within(writer, &WakeFrame::Event(payload), write_timeout_ms).await?;
            stats.delivered += 1;
        }
        if !full {
            return Ok(());
        }
    }
}

/// The lossless subscriber (bd-wake-live-burst-loss-j86v,
/// bd-wake-replay-cap-qk8n): the store is the only source of events. The
/// fan-out broadcast is just a doorbell; on every ring (and on every
/// heartbeat tick, as a safety net) the subscriber pages the arrival log from
/// its own cursor. A slow reader therefore gets backpressure, never a shed,
/// and a burst can never be reordered out of existence by the cursor. The
/// only way to miss a stored event is cap eviction, which is announced with
/// an `events_lost` frame.
#[allow(clippy::too_many_arguments)]
async fn serve_arrivals(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    mut rx: tokio::sync::broadcast::Receiver<String>,
    filter: &Filter,
    kinds: &[u16],
    resume: Resume,
    from_wire: &str,
    config: &WakeConfig,
    clock: &dyn WakeClock,
    bus: &dyn Bus,
    mut stats: SubscriberStats,
) -> SubscriberStats {
    use tokio::sync::broadcast::error::{RecvError, TryRecvError};
    let (mut after, check_eviction, floor) = match resume {
        Resume::Origin => (0, false, None),
        Resume::Seq(n) => (n, true, None),
        Resume::Legacy(c) => match bus.arrival_seq_of(&c.event_id).await {
            Ok(Some(seq)) => (seq, true, None),
            Ok(None) => {
                // The cursor's event is no longer stored, so its place in
                // arrival order is unknown. Say so, then replay from its
                // second: a duplicate is recoverable, a silent skip is not.
                let lost = WakeFrame::EventsLost {
                    from: from_wire.to_string(),
                    through: "unknown".to_string(),
                };
                if write_frame_within(writer, &lost, config.write_timeout_ms)
                    .await
                    .is_err()
                {
                    return stats;
                }
                (0, false, Some(c.created_at))
            }
            Err(_) => return stats,
        },
    };
    if check_eviction {
        match bus.arrivals_evicted_through().await {
            Ok(evicted) if evicted > after => {
                let lost = WakeFrame::EventsLost {
                    from: from_wire.to_string(),
                    through: format!("seq:{evicted}"),
                };
                if write_frame_within(writer, &lost, config.write_timeout_ms)
                    .await
                    .is_err()
                {
                    return stats;
                }
            }
            Ok(_) => {}
            Err(_) => return stats,
        }
    }
    if drain_arrivals(
        writer,
        bus,
        filter,
        kinds,
        &mut after,
        floor,
        &mut stats,
        config.write_timeout_ms,
    )
        .await
        .is_err()
    {
        return stats;
    }
    let eose = WakeFrame::Eose {
        cursor: if after == 0 {
            "head".to_string()
        } else {
            format!("seq:{after}")
        },
    };
    if write_frame_within(writer, &eose, config.write_timeout_ms)
        .await
        .is_err()
    {
        return stats;
    }
    stats.delivered += 1;
    let mut heartbeat_at = clock.now_ms().saturating_add(config.heartbeat_ms);
    loop {
        let wait = std::time::Duration::from_millis(config.heartbeat_ms);
        match tokio::time::timeout(wait, rx.recv()).await {
            Ok(Ok(_) | Err(RecvError::Lagged(_))) => {
                // Coalesce queued rings: one drain covers all of them, and a
                // lagged doorbell loses nothing because the store is the
                // source.
                loop {
                    match rx.try_recv() {
                        Ok(_) | Err(TryRecvError::Lagged(_)) => {}
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Closed) => return stats,
                    }
                }
                if drain_arrivals(
                    writer,
                    bus,
                    filter,
                    kinds,
                    &mut after,
                    None,
                    &mut stats,
                    config.write_timeout_ms,
                )
                .await
                .is_err()
                {
                    return stats;
                }
            }
            Ok(Err(RecvError::Closed)) => return stats,
            Err(_) => {
                // Quiet tick: catch up anything a missed ring left behind,
                // then the quiet-vs-deaf heartbeat.
                if drain_arrivals(
                    writer,
                    bus,
                    filter,
                    kinds,
                    &mut after,
                    None,
                    &mut stats,
                    config.write_timeout_ms,
                )
                .await
                .is_err()
                {
                    return stats;
                }
                if clock.now_ms() >= heartbeat_at {
                    let hb = WakeFrame::Heartbeat {
                        ts_ms: clock.now_ms(),
                    };
                    if write_frame_within(writer, &hb, config.write_timeout_ms)
                        .await
                        .is_err()
                    {
                        stats.heartbeat_deaf += 1;
                        return stats;
                    }
                    stats.heartbeats_ok += 1;
                    stats.delivered += 1;
                    heartbeat_at = clock.now_ms().saturating_add(config.heartbeat_ms);
                }
            }
        }
    }
}

async fn serve_subscriber(
    stream: UnixStream,
    mut rx: tokio::sync::broadcast::Receiver<String>,
    config: WakeConfig,
    clock: Arc<dyn WakeClock>,
    bus: Arc<dyn Bus>,
) -> SubscriberStats {
    let mut stats = SubscriberStats::default();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let raw = match tokio::time::timeout(
        std::time::Duration::from_millis(config.handshake_timeout_ms),
        lines.next_line(),
    )
    .await
    {
        Ok(Ok(line)) => line,
        _ => return stats,
    };
    let Some(line) = raw else { return stats };
    let Ok(req) = WakeSubscribe::parse_line(&line) else {
        return stats;
    };
    let filter = req.filter();
    let resume = match req.since_cursor.as_deref() {
        Some(value) => match Resume::parse(value) {
            Ok(resume) => resume,
            Err(_) => return stats,
        },
        None => Resume::Origin,
    };
    // bd-wake-live-burst-loss-j86v / bd-wake-replay-cap-qk8n: a backend with
    // an arrival log serves the lossless store-tail path. The legacy path
    // below stays only for backends without one (relay buses, test fakes).
    if matches!(
        bus.arrivals_after(&req.kinds, u64::MAX, 0).await,
        Ok(Some(_))
    ) {
        let from_wire = req.since_cursor.clone().unwrap_or_default();
        return serve_arrivals(
            &mut writer,
            rx,
            &filter,
            &req.kinds,
            resume,
            &from_wire,
            &config,
            clock.as_ref(),
            bus.as_ref(),
            stats,
        )
        .await;
    }
    let mut cursor = match resume {
        Resume::Origin => WakeCursor::origin(),
        Resume::Legacy(cursor) => cursor,
        // A `seq:` cursor names an arrival; without an arrival log it cannot
        // be placed, so refuse rather than guess.
        Resume::Seq(_) => return stats,
    };
    let Ok(mut replay) = bus.subscribe_trusted(filter.clone()).await else {
        return stats;
    };
    let replay_complete = loop {
        match replay.next().await {
            Some(crate::bus::SubscriptionEvent::Event(event)) => {
                match write_matching_event(
                    &mut writer,
                    &event,
                    &filter,
                    &mut cursor,
                    config.write_timeout_ms,
                )
                .await
                {
                    Ok(true) => stats.delivered += 1,
                    Ok(false) => {}
                    Err(_) => return stats,
                }
            }
            Some(crate::bus::SubscriptionEvent::Eose) => break true,
            Some(crate::bus::SubscriptionEvent::Closed(_)) | None => break false,
        }
    };
    if !replay_complete {
        return stats;
    }
    let eose = WakeFrame::Eose {
        cursor: if cursor == WakeCursor::origin() {
            "head".to_string()
        } else {
            cursor.to_string()
        },
    };
    let Ok(eose_line) = eose.encode_line() else {
        return stats;
    };
    if writer.write_all(eose_line.as_bytes()).await.is_err() {
        return stats;
    }
    stats.delivered += 1;
    let mut heartbeat_at = clock.now_ms().saturating_add(config.heartbeat_ms);
    loop {
        let wait = config.heartbeat_ms;
        match tokio::time::timeout(std::time::Duration::from_millis(wait), rx.recv()).await {
            Ok(Ok(line)) => {
                let Ok(WakeFrame::Event(payload)) = WakeFrame::decode_line(&line) else {
                    continue;
                };
                let Ok(event) = Event::from_json(payload.to_string()) else {
                    return stats;
                };
                match write_matching_event(
                    &mut writer,
                    &event,
                    &filter,
                    &mut cursor,
                    config.write_timeout_ms,
                )
                .await
                {
                    Ok(true) => stats.delivered += 1,
                    Ok(false) => {}
                    Err(_) => return stats,
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped))) => {
                stats.shed += skipped;
                let resync = cursor.to_string();
                stats.resync_from = Some(resync.clone());
                let shed = WakeFrame::Shed {
                    resync_from: resync,
                };
                let _ = writer
                    .write_all(shed.encode_line().unwrap_or_default().as_bytes())
                    .await;
                return stats;
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => return stats,
            Err(_) => {
                // Heartbeat tick (bounded wait expiry, not a poll of truth).
                if clock.now_ms() >= heartbeat_at {
                    let hb = WakeFrame::Heartbeat {
                        ts_ms: clock.now_ms(),
                    };
                    if writer
                        .write_all(hb.encode_line().unwrap_or_default().as_bytes())
                        .await
                        .is_err()
                    {
                        stats.heartbeat_deaf += 1;
                        return stats;
                    }
                    stats.heartbeats_ok += 1;
                    stats.delivered += 1;
                    heartbeat_at = clock.now_ms().saturating_add(config.heartbeat_ms);
                }
            }
        }
    }
}

#[cfg(test)]
mod wake_bus_tests {
    use super::*;

    struct FakeBus;

    #[async_trait::async_trait]
    impl Bus for FakeBus {
        async fn publish(
            &self,
            _event: crate::nostr::Event,
        ) -> Result<crate::bus::PublishOk, BusError> {
            Err(BusError::Unsupported("fake".to_string()))
        }
        async fn subscribe(&self, _filter: Filter) -> Result<crate::bus::Subscription, BusError> {
            self.subscribe_trusted(_filter).await
        }
        async fn subscribe_trusted(
            &self,
            _filter: Filter,
        ) -> Result<crate::bus::Subscription, BusError> {
            let (tx, rx) = tokio::sync::mpsc::channel(crate::bus::Subscription::DEFAULT_CAPACITY);
            tx.send(crate::bus::SubscriptionEvent::Eose)
                .await
                .map_err(|_| BusError::Shutdown)?;
            Ok(crate::bus::Subscription::new(
                crate::bus::SubscriptionId::new("fake"),
                rx,
            ))
        }
        async fn health(&self) -> Result<crate::bus::Health, BusError> {
            Ok(crate::bus::Health {
                ok: true,
                message: "fake".to_string(),
            })
        }
    }

    fn test_bus() -> Arc<dyn Bus> {
        Arc::new(FakeBus)
    }

    /// bd-ojw3: a write that cannot finish is a DEAD subscriber.
    ///
    /// The daemon dropped the subscriber's read half after the handshake, so
    /// it could not see the peer's EOF - and EOF cannot be used anyway: a
    /// client that half-closes after subscribing (`toron watch --once`) is
    /// alive, not gone. The only sound signal that a subscriber is gone is a
    /// write that cannot complete, so the write is BOUNDED: past the bound the
    /// subscriber is declared dead, the write fails, and its fd is released.
    ///
    /// The bound is a minute on purpose: a slow-but-alive reader must still
    /// receive a burst larger than every internal queue, with no shed and no
    /// loss (the lossless suite allows 30s for that burst). A tighter bound
    /// killed a healthy reader; an unbounded one held the fd forever.
    #[tokio::test]
    async fn a_write_that_cannot_finish_fails_instead_of_blocking_forever() {
        struct NeverCompletes;
        impl tokio::io::AsyncWrite for NeverCompletes {
            fn poll_write(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Pending
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
        }
        let mut writer = NeverCompletes;
        let frame = WakeFrame::Heartbeat { ts_ms: 0 };
        let started = std::time::Instant::now();
        let result = write_frame_within(&mut writer, &frame, 60).await;
        assert!(
            result.is_err(),
            "a write that cannot finish must fail, not block forever: the blocked write held \
             the subscriber's fd for the life of the daemon"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the bound must end the write promptly, took {:?}",
            started.elapsed()
        );
    }

    /// bd-ojw3: a subscriber that stops reading must be DROPPED, so its fd
    /// goes back.
    ///
    /// Measured on the live daemon: 78 held connections, and it died of EMFILE
    /// at launchd's 256 fd cap - which stopped it accepting connections at all,
    /// so the MCP endpoint went unreachable and every commit was blocked
    /// behind it.
    #[tokio::test]
    async fn a_stalled_subscriber_is_dropped_and_its_fd_released() {
        /// A bus whose subscription carries whatever the test publishes, so
        /// the server really writes frames to the subscriber.
        struct FloodBus {
            tx: tokio::sync::broadcast::Sender<crate::bus::SubscriptionEvent>,
        }
        #[async_trait::async_trait]
        impl Bus for FloodBus {
            async fn publish(
                &self,
                _event: crate::nostr::Event,
            ) -> Result<crate::bus::PublishOk, BusError> {
                Err(BusError::Unsupported("flood".to_string()))
            }
            async fn subscribe(&self, filter: Filter) -> Result<crate::bus::Subscription, BusError> {
                self.subscribe_trusted(filter).await
            }
            async fn subscribe_trusted(
                &self,
                _filter: Filter,
            ) -> Result<crate::bus::Subscription, BusError> {
                let (tx, rx) =
                    tokio::sync::mpsc::channel(crate::bus::Subscription::DEFAULT_CAPACITY);
                let mut events = self.tx.subscribe();
                tokio::spawn(async move {
                    while let Ok(ev) = events.recv().await {
                        if tx.send(ev).await.is_err() {
                            break;
                        }
                    }
                });
                Ok(crate::bus::Subscription::new(
                    crate::bus::SubscriptionId::new("flood"),
                    rx,
                ))
            }
            async fn health(&self) -> Result<crate::bus::Health, BusError> {
                Ok(crate::bus::Health {
                    ok: true,
                    message: "flood".to_string(),
                })
            }
        }

        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        let sock = tmp_sock("stalled");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.poll_interval_ms = 0;
        cfg.heartbeat_ms = 20;
        cfg.write_timeout_ms = 100;
        let bus: Arc<dyn Bus> = Arc::new(FloodBus { tx: tx.clone() });
        let h = WakeServer::new(cfg, bus, Arc::new(ManualClock::new(0)))
            .start()
            .await
            .expect("bind");

        // A client that reads NOTHING: its socket buffer fills and every
        // write to it blocks.
        let client = {
            use tokio::io::AsyncWriteExt;
            let mut stream = UnixStream::connect(&sock).await.expect("connect");
            let request = serde_json::json!({
                "subscribe": {"kinds": [9], "project": "demo"}
            })
            .to_string();
            stream
                .write_all(format!("{request}\n").as_bytes())
                .await
                .expect("subscribe");
            stream
        };
        // Hold the client OPEN (so its socket is not closed) but never read
        // from it.
        let _keep_open = client;

        let live = async {
            for _ in 0..100 {
                if h.aggregate_stats().subscribers_live > 0 {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            false
        }
        .await;
        assert!(live, "the subscriber should register as live");

        // A frame far larger than the socket buffer cannot be written to a
        // peer that never reads, so the bounded write fails and the subscriber
        // is dropped.
        let keys = crate::nostr::Keys::generate();
        let big = "x".repeat(512 * 1024);
        let event = crate::nostr::event::build_event(
            &keys,
            Kind::from(9),
            &big,
            vec![crate::nostr::Tag::parse(["t", "demo"]).expect("project tag")],
            crate::nostr::Timestamp::from(1),
        )
        .expect("event");
        let _ = tx.send(crate::bus::SubscriptionEvent::Event(event));

        let released = async {
            for _ in 0..200 {
                if h.aggregate_stats().subscribers_live == 0 {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            false
        }
        .await;
        assert!(
            released,
            "a subscriber that cannot be written to must be dropped: its fd is held otherwise, \
             and one stalled peer is enough to exhaust the daemon's budget"
        );
        let _ = std::fs::remove_file(&sock);
    }

    fn tmp_sock(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "toron-wake-bus-{}-{}-{}.sock",
            name,
            std::process::id(),
            AtomicU64::new(rand_suffix()).load(Ordering::SeqCst)
        ));
        p
    }

    fn rand_suffix() -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::thread::current().id().hash(&mut h);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0)
            .wrapping_add(h.finish())
    }

    #[test]
    fn wake_bus_frames_jsonl_roundtrip() {
        for f in [
            WakeFrame::Event(serde_json::json!({"kind": 9})),
            WakeFrame::Eose {
                cursor: "42".to_string(),
            },
            WakeFrame::Heartbeat { ts_ms: 7 },
            WakeFrame::Shed {
                resync_from: "shed-3".to_string(),
            },
        ] {
            let line = f.encode_line().expect("encode");
            assert!(line.ends_with('\n'), "one frame per line");
            assert_eq!(WakeFrame::decode_line(line.trim()).expect("decode"), f);
        }
        assert!(WakeFrame::decode_line(r#"{"nope":{}}"#).is_err());
    }

    #[test]
    fn wake_bus_subscribe_defaults_and_filter() {
        let req = WakeSubscribe::parse_line(r#"{"subscribe":{}}"#).expect("parse");
        assert_eq!(req.kinds, WAKE_KINDS.to_vec());
        let _ = req.filter();
        let scoped = WakeSubscribe::parse_line(r#"{"subscribe":{"kinds":[9],"project":"toron"}}"#)
            .expect("parse");
        assert_eq!(scoped.kinds, vec![9]);
        assert!(WakeSubscribe::parse_line(r#"{"hello":1}"#).is_err());
    }

    #[test]
    fn wake_bus_constructor_performs_no_io() {
        let sock = tmp_sock("ctor");
        let _ = std::fs::remove_file(&sock);
        let cfg = WakeConfig::new(&sock);
        let _server = WakeServer::new(cfg, test_bus(), Arc::new(ManualClock::new(0)));
        assert!(!sock.exists(), "new() must not touch the filesystem");
        let _ = std::fs::remove_file(&sock);
    }

    #[tokio::test]
    async fn wake_bus_sock_0600_and_stale_takeover_identity() {
        let sock = tmp_sock("perm");
        let _ = std::fs::remove_file(&sock);
        let bus = test_bus();
        let mk = |p: &Path| {
            WakeServer::new(
                WakeConfig::new(p),
                bus.clone(),
                Arc::new(ManualClock::new(0)),
            )
        };
        let h1 = mk(&sock).start().await.expect("first bind");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&sock).expect("meta").permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "wake.sock is 0600");
        }
        // Live peer holds it: second start is a typed refusal, never a steal.
        match mk(&sock).start().await {
            Err(WakeError::AlreadyServing(_)) => {}
            other => panic!("expected AlreadyServing, got {other:?}"),
        }
        drop(h1);
        // RAII: socket file goes away with the handle.
        assert!(!sock.exists(), "drop removes wake.sock");
        // Stale file (no live listener) is taken over, not refused.
        tokio::fs::write(&sock, b"stale")
            .await
            .expect("plant stale");
        let h2 = mk(&sock).start().await.expect("stale takeover");
        assert!(sock.exists());
        drop(h2);
        assert!(!sock.exists());
    }

    #[tokio::test]
    async fn wake_bus_handshake_timeout_bounded() {
        let sock = tmp_sock("hs");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.handshake_timeout_ms = 50;
        let clock: Arc<dyn WakeClock> = Arc::new(ManualClock::new(0));
        let bus = test_bus();
        let _h = WakeServer::new(cfg.clone(), bus, clock)
            .start()
            .await
            .expect("bind");
        // Connect and say nothing: the server must drop us on a bounded wait.
        let stream = UnixStream::connect(&sock).await.expect("connect");
        let (r, _w) = stream.into_split();
        let mut lines = BufReader::new(r).lines();
        let got: Option<String> =
            tokio::time::timeout(std::time::Duration::from_millis(2_000), lines.next_line())
                .await
                .expect("test bound")
                .expect("io");
        // Server closes without our subscribe: EOF, never a frame.
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn wake_bus_replays_matching_store_rows_and_resumes_after_cursor() {
        use crate::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
        use crate::nostr::{Keys, Timestamp};

        let db = std::env::temp_dir().join(format!(
            "toron-wake-replay-{}-{}.sqlite",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let pool = crate::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        let bus = Arc::new(
            InProcessBus::open(
                pool,
                InProcessBusConfig {
                    auth: AuthPolicy::AcceptAll,
                    allowlist: Vec::new(),
                    channel_capacity: 64,
                    detailed_stage_logging: false,
                },
            )
            .await
            .expect("bus"),
        );
        let keys = Keys::generate();
        let mut expected = Vec::new();
        for (kind, project, content, second) in [
            (9, "demo", "first", 10),
            (9, "demo", "second", 11),
            (14, "demo", "wrong kind", 12),
            (9, "other", "wrong project", 13),
        ] {
            let event = crate::nostr::event::build_event(
                &keys,
                Kind::from(kind),
                content,
                vec![crate::nostr::Tag::parse(["t", project]).expect("project tag")],
                Timestamp::from(second),
            )
            .expect("event");
            if content == "first" || content == "second" {
                expected.push((second, event.id.to_hex()));
            }
            bus.publish(event).await.expect("publish historical event");
        }

        let sock = tmp_sock("replay");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.poll_interval_ms = 0;
        let h = WakeServer::new(cfg, bus, Arc::new(ManualClock::new(0)))
            .start()
            .await
            .expect("bind");

        async fn receive_replay(sock: &Path, since_cursor: Option<&str>) -> (Vec<String>, String) {
            use tokio::io::AsyncWriteExt;
            let stream = UnixStream::connect(sock).await.expect("connect");
            let (reader, mut writer) = stream.into_split();
            let mut request = serde_json::json!({
                "subscribe": {"kinds": [9], "project": "demo", "since_cursor": since_cursor}
            })
            .to_string();
            request.push('\n');
            writer
                .write_all(request.as_bytes())
                .await
                .expect("subscribe");
            let mut lines = BufReader::new(reader).lines();
            let mut ids = Vec::new();
            loop {
                let line =
                    tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
                        .await
                        .expect("bounded socket read")
                        .expect("read frame")
                        .expect("frame before EOSE");
                match WakeFrame::decode_line(&line).expect("valid frame") {
                    WakeFrame::Event(event) => {
                        ids.push(event["id"].as_str().expect("event id").to_owned());
                    }
                    WakeFrame::Eose { cursor } => return (ids, cursor),
                    other => panic!("unexpected frame during replay: {other:?}"),
                }
            }
        }

        let (all_ids, cursor) = receive_replay(&sock, None).await;
        assert_eq!(
            all_ids,
            expected
                .iter()
                .map(|(_, id)| id.clone())
                .collect::<Vec<_>>(),
            "initial subscribe replays matching stored events only, in store order"
        );
        assert_ne!(cursor, "head", "EOSE must report a resumable event cursor");

        let resume_cursor = format!("{}:{}", expected[0].0, expected[0].1);
        let (resumed_ids, _) = receive_replay(&sock, Some(&resume_cursor)).await;
        assert_eq!(
            resumed_ids,
            vec![expected[1].1.clone()],
            "resume starts strictly after the supplied event cursor"
        );
        drop(h);
        let _ = std::fs::remove_file(&db);
    }

    #[tokio::test]
    async fn wake_bus_subscribe_eose_then_heartbeat() {
        let sock = tmp_sock("eose");
        let _ = std::fs::remove_file(&sock);

        let mut cfg = WakeConfig::new(&sock);
        cfg.heartbeat_ms = 50;
        let clock: Arc<dyn WakeClock> = Arc::new(ManualClock::new(1_000));
        let bus = test_bus();
        let _h = WakeServer::new(cfg, bus, clock)
            .start()
            .await
            .expect("bind");
        let stream = UnixStream::connect(&sock).await.expect("connect");
        let (r, mut w) = stream.into_split();
        w.write_all(b"{\"subscribe\":{}}\n").await.expect("sub");
        let mut lines = BufReader::new(r).lines();
        let eose = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
            .await
            .expect("test bound")
            .expect("io")
            .expect("eose line");
        assert_eq!(
            WakeFrame::decode_line(&eose).expect("decode"),
            WakeFrame::Eose {
                cursor: "head".to_string(),
            }
        );
    }

    /// The cursor IS the resume contract, so both halves are pinned here at the
    /// write boundary with the writer substituted: a redelivered event is
    /// written ONCE (a duplicate replay/live overlap cannot double-deliver),
    /// and the position after a write round-trips through the wire parse, so a
    /// consumer resuming from it skips what it already has.
    #[tokio::test]
    async fn a_redelivered_event_is_written_once_and_the_cursor_resumes() {
        let keys = crate::nostr::Keys::generate();
        let at = |content: &str, second: u64| {
            crate::nostr::event::build_event(
                &keys,
                Kind::from(9),
                content,
                vec![],
                crate::nostr::Timestamp::from(second),
            )
            .expect("event")
        };
        let ev = at("cursor", 10);
        let older = at("older", 9);
        let filter = Filter::new().kinds([Kind::from(9)]);
        let mut written: Vec<u8> = Vec::new();
        let mut cursor = WakeCursor::origin();

        assert!(
            write_matching_event(&mut written, &ev, &filter, &mut cursor, WAKE_WRITE_TIMEOUT_MS)
                .await
                .expect("first delivery"),
            "a fresh event past the cursor is delivered"
        );
        assert!(
            !write_matching_event(&mut written, &ev, &filter, &mut cursor, WAKE_WRITE_TIMEOUT_MS)
                .await
                .expect("redelivery"),
            "the SAME event again (replay/live overlap) is suppressed, not re-sent"
        );
        assert!(
            !write_matching_event(&mut written, &older, &filter, &mut cursor, WAKE_WRITE_TIMEOUT_MS)
                .await
                .expect("stale"),
            "an event behind the cursor never re-enters the stream"
        );
        let text = String::from_utf8_lossy(&written).into_owned();
        assert_eq!(
            text.matches(&ev.id.to_hex()).count(),
            1,
            "one frame per event, never two: {text}"
        );

        let wire = cursor.to_string();
        assert_eq!(
            WakeCursor::parse(&wire).expect("a cursor we wrote must resume"),
            cursor,
            "the position we advertise must parse back to the same position"
        );
        let mut resumed = WakeCursor::parse(&wire).expect("resumable");
        assert!(
            !write_matching_event(&mut written, &ev, &filter, &mut resumed, WAKE_WRITE_TIMEOUT_MS)
                .await
                .expect("already seen"),
            "a consumer resuming from the advertised cursor does not re-receive it"
        );
    }

    #[test]
    fn wake_bus_shed_counters_exact() {
        // Pure shed accounting: queue 2, 5 arrivals → 3 shed, cursor exact.
        let cap = 2usize;
        let arrivals = 5u64;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(cap);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let mut stats = SubscriberStats::default();
            for i in 0..arrivals {
                let line = format!("{{\"event\":{i}}}\n");
                match tx.try_send(line) {
                    Ok(()) => stats.delivered += 1,
                    Err(_) => {
                        stats.shed += 1;
                        stats.resync_from = Some(format!("shed-{}", stats.shed));
                    }
                }
            }
            // Drain what fits: exactly `cap` delivered, rest shed.
            let mut drained = 0;
            while rx.try_recv().is_ok() {
                drained += 1;
            }
            assert_eq!(drained, cap);
            assert_eq!(stats.delivered, cap as u64);
            assert_eq!(stats.shed, arrivals - cap as u64);
            assert_eq!(
                stats.resync_from,
                Some(format!("shed-{}", arrivals - cap as u64))
            );
        });
    }

    #[test]
    fn wake_bus_typed_errors_not_strings() {
        let e = WakeError::AlreadyServing("x".to_string());
        assert!(e.to_string().contains("already served"));
        let e = WakeError::HandshakeTimeout(5);
        assert!(e.to_string().contains('5'));
        // BusError passthrough is typed (no string-throw across the boundary).
        let b: WakeError = BusError::Shutdown.into();
        assert!(matches!(b, WakeError::Bus(_)));
    }

    /// Wave 4 (bd-ud7k): the exported aggregate is exact — two sequential
    /// subscribers each take one EOSE, then the export sums them.
    #[tokio::test]
    async fn wake_bus_aggregate_shed_stats_exact() {
        let sock = tmp_sock("agg");
        let _ = std::fs::remove_file(&sock);
        // Short heartbeat: disconnects are detected on the next heartbeat
        // write, so finished stats land within one interval of the drop.
        let mut cfg = WakeConfig::new(&sock);
        cfg.heartbeat_ms = 50;
        let clock = Arc::new(ManualClock::new(0));
        let serve_clock: Arc<dyn WakeClock> = clock.clone();
        let h = WakeServer::new(cfg, test_bus(), serve_clock)
            .start()
            .await
            .expect("bind");
        assert_eq!(h.aggregate_stats(), WakeStats::default());
        for _ in 0..2 {
            let stream = UnixStream::connect(&sock).await.expect("connect");
            let (r, mut w) = stream.into_split();
            w.write_all(b"{\"subscribe\":{}}\n").await.expect("sub");
            let mut lines = BufReader::new(r).lines();
            let eose = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
                .await
                .expect("test bound")
                .expect("io")
                .expect("eose line");
            assert!(eose.contains("eose"));
            drop(lines);
            drop(w);
        }
        // Finished-subscriber stats land on task end: bounded spin, never
        // an unbounded wait. The scripted clock advances each tick so the
        // heartbeat fires and the dropped connection is reaped.
        let agg = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let a = h.aggregate_stats();
                if a.subscribers_finished == 2 {
                    break a;
                }
                clock.advance(1_000);
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("stats land on task end");
        assert_eq!(agg.subscribers_live, 0);
        assert_eq!(agg.delivered, 2, "one EOSE per subscriber, exact");
        assert_eq!(agg.shed, 0);
    }

    /// Wave 4 (bd-ud7k): deaf-vs-quiet. A quiet bus delivers a heartbeat
    /// stamped by the injected clock; a dead server reads EOF, never a
    /// frame. The two states are asserted distinct, never conflated.
    #[tokio::test]
    async fn wake_bus_quiet_gets_heartbeat_deaf_gets_eof() {
        let sock = tmp_sock("dq");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.heartbeat_ms = 50;
        let clock = Arc::new(ManualClock::new(1_000));
        let serve_clock: Arc<dyn WakeClock> = clock.clone();
        let h = WakeServer::new(cfg, test_bus(), serve_clock)
            .start()
            .await
            .expect("bind");
        let stream = UnixStream::connect(&sock).await.expect("connect");
        let (r, mut w) = stream.into_split();
        w.write_all(b"{\"subscribe\":{}}\n").await.expect("sub");
        let mut lines = BufReader::new(r).lines();
        let eose = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
            .await
            .expect("test bound")
            .expect("io")
            .expect("eose line");
        assert!(eose.contains("eose"), "live path starts at EOSE");
        // Quiet bus: advance the scripted clock past the heartbeat
        // interval and the next frame must be a stamped heartbeat.
        clock.advance(60_000);
        let hb = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
            .await
            .expect("test bound")
            .expect("io")
            .expect("heartbeat line");
        assert_eq!(
            WakeFrame::decode_line(&hb).expect("decode"),
            WakeFrame::Heartbeat { ts_ms: 61_000 },
            "quiet bus heartbeats with the injected clock, not wall time"
        );
        // Deaf watcher: the server is gone, the client reads EOF.
        drop(h);
        let eof: Option<String> =
            tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
                .await
                .expect("test bound")
                .expect("io");
        assert!(eof.is_none(), "dead server reads EOF, never a frame");
    }

    /// governed-by: bd-d41q — the sidecar must fan out store writes from
    /// a FOREIGN bus instance (the CLI process's embedded mailbox on the
    /// shared store). Two InProcessBus on one pool simulate daemon + CLI:
    /// a kind-9 published via bus B must reach a wake.sock subscriber.
    /// Before the fix the held subscription only observed bus A (live
    /// fanout), so this times out on the event line.
    #[tokio::test]
    async fn wake_pump_fans_out_foreign_store_write() {
        use crate::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
        let db = std::env::temp_dir().join(format!(
            "toron-wake-foreign-{}-{}.sqlite",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let pool = crate::index::schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        let mk_bus = || async {
            InProcessBus::open(
                pool.clone(),
                InProcessBusConfig {
                    auth: AuthPolicy::AcceptAll,
                    allowlist: Vec::new(),
                    channel_capacity: 64,
                    detailed_stage_logging: false,
                },
            )
            .await
            .expect("bus")
        };
        let bus_a = mk_bus().await;
        let bus_b = mk_bus().await;
        let sock = tmp_sock("foreign");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.poll_interval_ms = 50;
        let _h = WakeServer::new(cfg, Arc::new(bus_a), Arc::new(SystemClock))
            .start()
            .await
            .expect("bind");
        let stream = UnixStream::connect(&sock).await.expect("connect");
        let (r, mut w) = stream.into_split();
        w.write_all(b"{\"subscribe\":{\"kinds\":[9]}}\n")
            .await
            .expect("sub");
        let mut lines = BufReader::new(r).lines();
        let eose = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
            .await
            .expect("test bound")
            .expect("io")
            .expect("eose line");
        assert!(eose.contains("eose"), "handshake first, {eose}");
        // Foreign write: bus B publishes (and stores) a kind-9 that bus
        // A's live fanout never traverses.
        let keys = crate::keys::generate();
        let ev = crate::nostr::event::build_event(
            &keys,
            crate::nostr::kinds::toron_kind(9),
            "foreign hello",
            vec![],
            crate::nostr::Timestamp::now(),
        )
        .expect("event");
        let id_hex = ev.id.to_hex();
        bus_b.publish(ev).await.expect("foreign publish");
        let seen = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let line = lines.next_line().await.expect("io").expect("line");
                if line.contains(&id_hex) {
                    break line;
                }
            }
        })
        .await
        .expect("foreign kind-9 must fan out over wake.sock");
        assert!(seen.contains("\"event\""), "event frame, {seen}");
        let _ = std::fs::remove_file(&db);
    }

    /// Counts every `subscribe_trusted` and answers `store_version` from an
    /// atomic the test drives. The churn fix is only observable through the
    /// subscribe count, so that is what this fake records.
    struct CountingBus {
        subscribes: AtomicU64,
        version: std::sync::atomic::AtomicI64,
        /// Held senders: a subscription that ends at EOSE would let the
        /// pump's held `bus_sub` branch resolve `None` and exit the loop
        /// before a single poll tick, which is exactly the bug this fake
        /// would otherwise hide.
        open: std::sync::Mutex<Vec<tokio::sync::mpsc::Sender<crate::bus::SubscriptionEvent>>>,
    }

    impl CountingBus {
        fn new() -> Self {
            Self {
                subscribes: AtomicU64::new(0),
                version: std::sync::atomic::AtomicI64::new(1),
                open: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn subscribes(&self) -> u64 {
            self.subscribes.load(Ordering::SeqCst)
        }

        fn bump(&self) {
            self.version
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Bus for CountingBus {
        async fn publish(
            &self,
            _event: crate::nostr::Event,
        ) -> Result<crate::bus::PublishOk, BusError> {
            Err(BusError::Unsupported("counting fake".to_string()))
        }

        async fn subscribe(&self, filter: Filter) -> Result<crate::bus::Subscription, BusError> {
            self.subscribe_trusted(filter).await
        }

        async fn subscribe_trusted(
            &self,
            _filter: Filter,
        ) -> Result<crate::bus::Subscription, BusError> {
            self.subscribes.fetch_add(1, Ordering::SeqCst);
            let (tx, rx) = tokio::sync::mpsc::channel(crate::bus::Subscription::DEFAULT_CAPACITY);
            tx.send(crate::bus::SubscriptionEvent::Eose)
                .await
                .map_err(|_| BusError::Shutdown)?;
            if let Ok(mut open) = self.open.lock() {
                open.push(tx);
            }
            Ok(crate::bus::Subscription::new(
                crate::bus::SubscriptionId::new("counting"),
                rx,
            ))
        }

        async fn health(&self) -> Result<crate::bus::Health, BusError> {
            Ok(crate::bus::Health {
                ok: true,
                message: "counting".to_string(),
            })
        }

        async fn store_version(&self) -> Option<i64> {
            Some(self.version.load(std::sync::atomic::Ordering::SeqCst))
        }
    }

    /// Churn witness (bd-qpm0): a quiet store must not cost a fresh
    /// subscription on every poll tick. The pump must stop re-reading while
    /// the store version is unchanged, AND a version bump must re-arm it —
    /// the second half is what keeps the first from passing on a poll that
    /// is merely broken.
    #[tokio::test]
    async fn wake_poll_skips_quiet_store_ticks_and_rearms_on_a_write() {
        let bus = Arc::new(CountingBus::new());
        let sock = tmp_sock("qp");
        let _ = std::fs::remove_file(&sock);
        let mut cfg = WakeConfig::new(&sock);
        cfg.poll_interval_ms = 20;
        let _h = WakeServer::new(cfg, bus.clone(), Arc::new(ManualClock::new(0)))
            .start()
            .await
            .expect("bind");
        // A live consumer is required: the pump never polls with nobody
        // listening, which is why the churn scaled with consumers.
        let stream = UnixStream::connect(&sock).await.expect("connect");
        let (r, mut w) = stream.into_split();
        w.write_all(b"{\"subscribe\":{\"kinds\":[9]}}\n")
            .await
            .expect("sub");
        let mut lines = BufReader::new(r).lines();
        let eose = tokio::time::timeout(std::time::Duration::from_secs(2), lines.next_line())
            .await
            .expect("test bound")
            .expect("io")
            .expect("eose line");
        assert!(eose.contains("eose"), "handshake first: {eose}");
        // Let the first tick arm the version, then measure steady state.
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let settled = bus.subscribes();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let quiet_delta = bus.subscribes() - settled;
        assert!(
            quiet_delta <= 1,
            "a quiet store must not re-subscribe per tick: {quiet_delta} polls in ~20 ticks"
        );
        // The skip must be a skip, not a dead poll.
        bus.bump();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(
            bus.subscribes() > settled + quiet_delta,
            "a store write must re-arm the poll"
        );
        let _ = std::fs::remove_file(&sock);
    }
}
