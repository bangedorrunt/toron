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
    pub fn is_stale(&self, now_secs: u64, ttl: Duration) -> bool {
        now_secs.saturating_sub(self.ts) > ttl.as_secs()
    }
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
        let state: Arc<RwLock<HashMap<String, Presence>>> = Arc::new(RwLock::new(HashMap::new()));
        let (live_tx, _) = broadcast::channel(256);

        let state_for_task = state.clone();
        let live_for_task = live_tx.clone();
        let bus_for_task = bus.clone();
        let handle = tokio::spawn(async move {
            // Subscribe to presence events on the bus.
            let filter =
                crate::bus::Filter::new().kinds([crate::nostr::kinds::toron_kind(PRESENCE)]);
            let mut sub = match bus_for_task.subscribe(filter).await {
                Ok(s) => s,
                Err(_) => return,
            };
            // Drain replay/EOSE then live events.
            loop {
                match sub.next().await {
                    Some(crate::bus::SubscriptionEvent::Event(ev)) => {
                        if let Some(payload) = parse_presence(&ev) {
                            let presence = Presence {
                                pubkey: ev.pubkey,
                                state: payload.state,
                                status: payload.status,
                                ts: payload.ts,
                            };
                            // Last heartbeat wins — keyed by pubkey so a newer
                            // heartbeat for the same agent replaces the old.
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
        let state = self.state.read();
        let mut v: Vec<Presence> = state
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
    use crate::index::schema;
    use crate::nostr::presence::{PresencePayload, build_presence_event};
    use crate::nostr::{Event, Keys, Timestamp};

    async fn temp_bus() -> (InProcessBus, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!("toron-pres-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool, Default::default())
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

    #[tokio::test]
    async fn test_broker_tracks_last_heartbeat_wins() {
        let (bus, path) = temp_bus().await;
        let keys = Keys::generate();

        let bus: Arc<dyn Bus> = Arc::new(bus);
        let broker = PresenceBroker::start(bus.clone(), Duration::from_secs(60));
        let mut rx = broker.subscribe_live();

        // Retry-publish until the broker's bus sub is attached (ephemeral,
        // no replay), then send the two heartbeats and wait for both.
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

        // Publish + retry until the broker's bus subscription has attached and
        // delivered (presence is ephemeral: no replay to catch a missed event).
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

        // Fresh now → present.
        assert!(broker.current(&keys.public_key(), 1_700_000_050).is_some());
        // Stale now → expired.
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

        // Publish + retry until the broker's bus subscription is attached and
        // the live broadcast fires (ephemeral: no replay fallback).
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
}
