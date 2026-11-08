// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Presence broker — ephemeral kind-20001 working-state, never projected.
//!
//! Per the bd-led.3.2 review carve-out, presence is the **canonical consumer
//! for a dropping channel**: a missed heartbeat just means stale presence
//! until the next one, and presence is never written to SQLite or Git. The
//! broker keeps a **last-heartbeat-wins** map per pubkey with **expiry on
//! stale**, and fans live presence out on a lossy `broadcast`.
//!
//! The projection (bd-led.4.4) must explicitly skip kind-20001 in its filter;
//! that is the other half of the "not stored" guarantee.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::nostr::PublicKey;

use crate::bus::Bus;
use crate::nostr::kinds::PRESENCE;
use crate::nostr::presence::{PresenceState, parse_presence};

/// Default presence TTL: how long a heartbeat is considered live after its
/// `ts` before the agent is marked stale / offline.
pub const DEFAULT_PRESENCE_TTL: Duration = Duration::from_secs(60);

/// A live presence observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Presence {
    /// Agent pubkey.
    pub pubkey: PublicKey,
    /// Latest working state.
    pub state: PresenceState,
    /// Latest status message (if any).
    pub status: Option<String>,
    /// Last heartbeat time (unix seconds).
    pub ts: u64,
}

impl Presence {
    /// True when this observation is stale relative to `now` and the TTL.
    pub const fn is_stale(&self, now_secs: u64, ttl: Duration) -> bool {
        now_secs.saturating_sub(self.ts) > ttl.as_secs()
    }
}

/// One cross-process presence row. The CLI appends these to
/// `<mailbox-root>/presence.jsonl`; the bus broker reads the same file so a
/// heartbeat published by another process is visible without giving up the
/// ephemeral no-store rule (the JSONL is a TTL projection, never the bus).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PresenceSpoolRow {
    pub name: String,
    pub npub: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    pub ts: u64,
    pub project: String,
}

/// Canonical cross-process presence file under a mailbox root.
pub fn spool_path(root: &Path) -> PathBuf {
    root.join("presence.jsonl")
}

/// Compact the spool once it grows past this size. Presence is last-wins
/// and TTL-bounded, so old rows carry no information after a newer beat.
pub const SPOOL_COMPACT_BYTES: u64 = 256 * 1024;

/// Bounded exclusive lock for spool rewrites. A busy writer never blocks the
/// heartbeat path: compaction is opportunistic, and a failed lock falls back
/// to a plain append.
fn lock_spool(path: &Path) -> Option<std::fs::File> {
    const TIMEOUT: Duration = Duration::from_millis(500);
    const POLL: Duration = Duration::from_millis(5);
    let lock_path = path.with_extension("lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .ok()?;
    let deadline = std::time::Instant::now() + TIMEOUT;
    loop {
        match file.try_lock() {
            Ok(()) => return Some(file),
            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                std::thread::sleep(POLL);
            }
            Err(std::fs::TryLockError::WouldBlock | std::fs::TryLockError::Error(_)) => {
                return None;
            }
        }
    }
}

/// Rewrite the TTL projection to one row per npub (`read_spool`'s fold), so
/// an append-only spool cannot grow without bound. Atomic replace under the
/// shared lock: readers see either the old complete file or the new one.
pub fn compact_spool(path: &Path, now_secs: u64, ttl: Duration) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let Some(_lock) = lock_spool(path) else {
        return Ok(());
    };
    let rows = read_spool(path, None, now_secs, ttl);
    let mut body = String::new();
    for row in rows {
        body.push_str(&serde_json::to_string(&row)?);
        body.push('\n');
    }
    let tmp = path.with_extension("jsonl.tmp");
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Append one heartbeat to the shared TTL projection.
pub fn append_spool(path: &Path, row: &PresenceSpoolRow) -> anyhow::Result<()> {
    if path
        .metadata()
        .is_ok_and(|metadata| metadata.len() > SPOOL_COMPACT_BYTES)
    {
        let _ = compact_spool(path, row.ts, DEFAULT_PRESENCE_TTL);
    }
    let value = serde_json::to_value(row)?;
    crate::jsonl::append_row(path, &value)
}

/// Last heartbeat per npub inside `ttl`, optionally restricted to one project.
/// Malformed rows are skipped, exactly as a dropping presence channel would.
pub fn read_spool(
    path: &Path,
    project: Option<&str>,
    now_secs: u64,
    ttl: Duration,
) -> Vec<PresenceSpoolRow> {
    let Ok(body) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut by_npub: HashMap<String, PresenceSpoolRow> = HashMap::new();
    for line in body.lines() {
        let Ok(row) = serde_json::from_str::<PresenceSpoolRow>(line) else {
            continue;
        };
        if project.is_some_and(|wanted| row.project != wanted) {
            continue;
        }
        if now_secs.saturating_sub(row.ts) > ttl.as_secs() {
            continue;
        }
        if by_npub.get(&row.npub).is_none_or(|old| old.ts <= row.ts) {
            by_npub.insert(row.npub.clone(), row);
        }
    }
    let mut rows: Vec<PresenceSpoolRow> = by_npub.into_values().collect();
    rows.sort_by_key(|row| std::cmp::Reverse(row.ts));
    rows
}

fn presence_from_spool(row: &PresenceSpoolRow) -> Option<Presence> {
    let pubkey = PublicKey::from_hex(&row.npub).ok()?;
    let state =
        serde_json::from_value::<PresenceState>(serde_json::Value::String(row.state.clone()))
            .ok()?;
    Some(Presence {
        pubkey,
        state,
        status: row.status.clone(),
        ts: row.ts,
    })
}

/// Presence broker: subscribes to kind-20001 on the bus, keeps last-heartbeat-
/// wins state (keyed by pubkey) with expiry, and re-broadcasts live presence
/// on a lossy channel.
#[derive(Clone)]
pub struct PresenceBroker {
    state: Arc<RwLock<HashMap<String, Presence>>>,
    live_tx: broadcast::Sender<Presence>,
    ttl: Duration,
    handle: Arc<JoinHandle<()>>,
}

impl PresenceBroker {
    /// Start the broker on a bus. Spawns a background task that subscribes to
    /// kind-20001 and updates state.
    pub fn start(bus: Arc<dyn Bus>, ttl: Duration) -> Self {
        Self::start_with_spool(bus, ttl, None)
    }

    /// Start the broker with an optional cross-process spool path. Rows in
    /// the spool are folded in at start and on every expiry tick, so a CLI
    /// heartbeat in another process is visible to an embedding host.
    pub fn start_with_spool(bus: Arc<dyn Bus>, ttl: Duration, spool: Option<PathBuf>) -> Self {
        let state: Arc<RwLock<HashMap<String, Presence>>> = Arc::new(RwLock::new(HashMap::new()));
        let (live_tx, _) = broadcast::channel(256);

        if let Some(path) = spool.as_ref() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            for row in read_spool(path, None, now, ttl) {
                if let Some(p) = presence_from_spool(&row) {
                    state.write().insert(row.npub.clone(), p);
                }
            }
        }

        let state_for_task = state.clone();
        let live_for_task = live_tx.clone();
        let bus_for_task = bus.clone();
        let spool_for_task = spool.clone();
        let handle = tokio::spawn(async move {
            let filter =
                crate::bus::Filter::new().kinds([crate::nostr::kinds::toron_kind(PRESENCE)]);
            let mut sub = match bus_for_task.subscribe(filter).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let mut gc = tokio::time::interval(ttl);
            gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    ev = sub.next() => {
                        match ev {
                            Some(crate::bus::SubscriptionEvent::Event(ev)) => {
                                if let Some(payload) = parse_presence(&ev) {
                                    let presence = Presence {
                                        pubkey: ev.pubkey,
                                        state: payload.state,
                                        status: payload.status,
                                        ts: payload.ts,
                                    };
                                    state_for_task
                                        .write()
                                        .insert(ev.pubkey.to_hex(), presence.clone());
                                    let _ = live_for_task.send(presence);
                                }
                            }
                            Some(crate::bus::SubscriptionEvent::Eose) | Some(_) => {}
                            None => break,
                        }
                    }
                    _ = gc.tick() => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state_for_task
                            .write()
                            .retain(|_, p| !p.is_stale(now, ttl));
                        if let Some(path) = spool_for_task.as_ref() {
                            let _ = compact_spool(path, now, ttl);
                            for row in read_spool(path, None, now, ttl) {
                                let Some(p) = presence_from_spool(&row) else {
                                    continue;
                                };
                                let newer = state_for_task
                                    .read()
                                    .get(&row.npub)
                                    .is_none_or(|old| old.ts < p.ts);
                                if newer {
                                    state_for_task.write().insert(row.npub.clone(), p.clone());
                                    let _ = live_for_task.send(p);
                                }
                            }
                        }
                    }
                }
            }
        });

        Self {
            state,
            live_tx,
            ttl,
            handle: Arc::new(handle),
        }
    }

    /// Subscribe to live presence broadcasts (lossy — fine for presence).
    pub fn subscribe_live(&self) -> broadcast::Receiver<Presence> {
        self.live_tx.subscribe()
    }

    /// Current presence for a pubkey (last heartbeat wins), or `None` if the
    /// agent never heartbeated or is stale.
    pub fn current(&self, pubkey: &PublicKey, now_secs: u64) -> Option<Presence> {
        let state = self.state.read();
        state
            .get(&pubkey.to_hex())
            .filter(|p| !p.is_stale(now_secs, self.ttl))
            .cloned()
    }

    /// All currently-live agents (not stale), newest heartbeat first.
    pub fn live_agents(&self, now_secs: u64) -> Vec<Presence> {
        let mut v: Vec<Presence> = self
            .state
            .read()
            .values()
            .filter(|p| !p.is_stale(now_secs, self.ttl))
            .cloned()
            .collect();
        v.sort_by_key(|p| std::cmp::Reverse(p.ts));
        v
    }

    /// Expire stale heartbeats (GC the map so it does not grow unbounded).
    pub fn expire_stale(&self, now_secs: u64) {
        self.state
            .write()
            .retain(|_, p| !p.is_stale(now_secs, self.ttl));
    }

    /// Stop the background task.
    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::InProcessBus;
    use crate::bus::in_process::InProcessBusConfig;
    use crate::index::schema;
    use crate::nostr::presence::{PresencePayload, build_presence_event};
    use crate::nostr::{Event, Keys, Timestamp};

    async fn temp_bus() -> (InProcessBus, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("toron-pres-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool, InProcessBusConfig::default())
            .await
            .expect("bus");
        (bus, path)
    }

    fn heartbeat(keys: &Keys, state: PresenceState, ts: u64) -> Event {
        let payload = PresencePayload {
            state,
            status: Some("agent says hi".into()),
            ts,
        };
        build_presence_event(keys, &payload, Timestamp::from(ts)).expect("build")
    }

    #[test]
    fn broker_start_gcs_stale_on_live_path() {
        let src = include_str!("presence.rs");
        let start = src.find("pub fn start(").expect("start");
        let slice = &src[start..src.len().min(start + 3500)];
        assert!(
            slice.contains("is_stale") && slice.contains("interval"),
            "PresenceBroker::start must GC stale heartbeats on a timer, not only from tests"
        );
    }

    #[test]
    fn test_presence_stale() {
        let keys = Keys::generate();
        let p = Presence {
            pubkey: keys.public_key(),
            state: PresenceState::Working,
            status: None,
            ts: 1_700_000_000,
        };
        assert!(p.is_stale(1_700_000_100, Duration::from_secs(60)));
        assert!(!p.is_stale(1_700_000_050, Duration::from_secs(60)));
    }

    /// A heartbeat appended by another process to the shared TTL projection
    /// is visible when the broker starts, without a 20001 store row.
    #[tokio::test]
    async fn broker_reads_a_cross_process_spool_heartbeat() {
        let (bus, db_path) = temp_bus().await;
        let keys = Keys::generate();
        let dir =
            std::env::temp_dir().join(format!("toron-presence-spool-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        append_spool(
            &spool_path(&dir),
            &PresenceSpoolRow {
                name: "OtherProcess".into(),
                npub: keys.public_key().to_hex(),
                state: "working".into(),
                status: Some("from the CLI".into()),
                ts: now,
                project: "demo".into(),
            },
        )
        .expect("append spool");

        let broker = PresenceBroker::start_with_spool(
            Arc::new(bus),
            Duration::from_secs(60),
            Some(spool_path(&dir)),
        );
        let current = broker
            .current(&keys.public_key(), now)
            .expect("spool heartbeat visible");
        assert_eq!(current.state, PresenceState::Working);
        assert_eq!(current.status.as_deref(), Some("from the CLI"));
        assert!(
            broker
                .live_agents(now)
                .iter()
                .any(|p| p.pubkey == keys.public_key())
        );
        broker.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn test_broker_tracks_last_heartbeat_wins() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let bus: Arc<dyn Bus> = Arc::new(bus);
        let broker = PresenceBroker::start(bus.clone(), Duration::from_secs(60));
        let mut rx = broker.subscribe_live();

        let idle = heartbeat(&keys, PresenceState::Idle, 1_700_000_000);
        for _ in 0..20 {
            let _ = bus.publish(idle.clone()).await;
            if tokio::time::timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_ok()
            {
                break;
            }
        }
        let working = heartbeat(&keys, PresenceState::Working, 1_700_000_005);
        bus.publish(working).await.expect("publish working");
        tokio::time::sleep(Duration::from_millis(100)).await;

        let cur = broker
            .current(&keys.public_key(), 1_700_000_020)
            .expect("presence present");
        assert_eq!(cur.state, PresenceState::Working, "last heartbeat wins");
        broker.shutdown();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_broker_expires_stale() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let bus: Arc<dyn Bus> = Arc::new(bus);
        let broker = PresenceBroker::start(bus.clone(), Duration::from_secs(60));
        let mut rx = broker.subscribe_live();

        let ev = heartbeat(&keys, PresenceState::Working, 1_700_000_000);
        let mut delivered = false;
        for _ in 0..20 {
            let _ = bus.publish(ev.clone()).await;
            if tokio::time::timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_ok()
            {
                delivered = true;
                break;
            }
        }
        assert!(delivered, "broker must receive the live presence heartbeat");

        assert!(broker.current(&keys.public_key(), 1_700_000_050).is_some());
        assert!(broker.current(&keys.public_key(), 1_700_000_200).is_none());
        assert!(broker.live_agents(1_700_000_200).is_empty());
        broker.expire_stale(1_700_000_200);
        broker.shutdown();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_broker_live_broadcast() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();
        let bus: Arc<dyn Bus> = Arc::new(bus);
        let broker = PresenceBroker::start(bus.clone(), Duration::from_secs(60));
        let mut rx = broker.subscribe_live();

        let ev = heartbeat(&keys, PresenceState::Done, 1_700_000_000);
        let mut got = None;
        for _ in 0..20 {
            let _ = bus.publish(ev.clone()).await;
            if let Ok(Ok(p)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
                got = Some(p);
                break;
            }
        }
        let got = got.expect("live presence broadcast received");
        assert_eq!(got.pubkey, keys.public_key());
        assert_eq!(got.state, PresenceState::Done);
        broker.shutdown();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compact_spool_keeps_last_row_per_npub_and_drops_stale() {
        let dir =
            std::env::temp_dir().join(format!("toron-presence-compact-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        let path = spool_path(&dir);
        for (npub, ts) in [("aa", 100), ("aa", 200), ("bb", 1)] {
            append_spool(
                &path,
                &PresenceSpoolRow {
                    name: npub.into(),
                    npub: npub.into(),
                    state: "working".into(),
                    status: None,
                    ts,
                    project: "demo".into(),
                },
            )
            .expect("append");
        }
        compact_spool(&path, 200, Duration::from_secs(60)).expect("compact");
        let body = std::fs::read_to_string(&path).expect("read");
        assert_eq!(body.lines().count(), 1, "one live row per npub: {body}");
        assert!(body.contains("\"npub\":\"aa\""));
        assert!(body.contains("\"ts\":200"));
        let _ = std::fs::remove_dir_all(dir);
    }
}
