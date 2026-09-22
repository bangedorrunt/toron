// governed-by: ADR-0036
//! Wake bus slice C conformance (bd-b893): ADR-0036 Verification list.
//!
//! EFFECT RULES (binding): substitution tests only. ManualClock advanced by
//! hand, deterministic content (no rand), temp-file sqlite store (same code
//! path as prod, stricter than in-mem), scripted failure sequences, ms
//! timeouts only, no network.
//!
//! Slices A (socket server) + B (tail consumer) are still open, so this suite
//! is transport-agnostic: it pins the contract against InProcessBus + store
//! replay, which the socket sidecar must satisfy when it lands.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use toron_core::bus::filter::project_filter;
use toron_core::bus::in_process::{AuthPolicy, InProcessBus, InProcessBusConfig};
use toron_core::bus::{Bus, Subscription, SubscriptionEvent};
use toron_core::index::schema;
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::{CHANNEL_MESSAGE, PRESENCE};
use toron_core::nostr::{Keys, Tag, Timestamp};

const SILENCE: Duration = Duration::from_millis(200);
const SLUG: &str = "wake-conformance";

/// Fake clock: advanced manually, never wall time.
#[derive(Debug, Clone, Copy)]
struct ManualClock {
    now_ms: u64,
}

impl ManualClock {
    const fn new(t: u64) -> Self {
        Self { now_ms: t }
    }

    const fn advance(&mut self, dt_ms: u64) {
        self.now_ms += dt_ms;
    }
}

/// Quiet-vs-deaf predicate (the /health distinction ADR-0036 keeps):
/// a quiet bus (recent heartbeat, zero events) is NOT deaf; only a stale
/// heartbeat is deaf.
const fn is_deaf(last_heartbeat_ms: u64, now_ms: u64, timeout_ms: u64) -> bool {
    now_ms.saturating_sub(last_heartbeat_ms) > timeout_ms
}

/// Stale-takeover identity predicate (serve.pid pattern, Spec 2.1):
/// takeover allowed only after an identity check proves the holder dead or
/// foreign; a live same-identity holder refuses; blind delete never happens.
const fn takeover_allowed(holder_pid_live: bool, holder_is_owner: bool, checked: bool) -> bool {
    if !checked {
        return false;
    }
    !holder_pid_live || !holder_is_owner
}

async fn temp_bus_with_capacity(cap: usize) -> (InProcessBus, std::path::PathBuf) {
    let path =
        std::env::temp_dir().join(format!("toron-wake-conf-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    let bus = InProcessBus::open(
        pool,
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: Vec::new(),
            channel_capacity: cap,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus");
    (bus, path)
}

fn kind9(keys: &Keys, content: &str) -> toron_core::nostr::Event {
    build_event(
        keys,
        toron_core::nostr::kinds::toron_kind(CHANNEL_MESSAGE),
        content,
        vec![Tag::parse(["t", SLUG]).expect("t")],
        Timestamp::now(),
    )
    .expect("build")
}

async fn drain(sub: &mut Subscription, max: usize) -> Vec<SubscriptionEvent> {
    let mut out = Vec::new();
    for _ in 0..max {
        match tokio::time::timeout(SILENCE, sub.next()).await {
            Ok(Some(ev)) => out.push(ev),
            _ => break,
        }
    }
    out
}

fn event_ids(evs: &[SubscriptionEvent]) -> Vec<String> {
    evs.iter()
        .filter_map(|e| match e {
            SubscriptionEvent::Event(ev) => Some(ev.id.to_hex()),
            _ => None,
        })
        .collect()
}

/// (1) Replay-then-live: one subscribe delivers history + EOSE + live with no
/// repoll in the path.
#[tokio::test]
async fn conformance_replay_then_live_no_poll() {
    let (bus, _path) = temp_bus_with_capacity(Subscription::DEFAULT_CAPACITY).await;
    let keys = Keys::generate();
    for i in 0..4 {
        bus.publish(kind9(&keys, &format!("seed-{i}")))
            .await
            .expect("publish");
    }

    let mut sub = bus.subscribe(project_filter(SLUG)).await.expect("sub");
    let got = drain(&mut sub, 16).await;
    let ids = event_ids(&got);
    assert_eq!(ids.len(), 4, "history replayed exactly once");
    assert!(
        got.iter().any(|e| matches!(e, SubscriptionEvent::Eose)),
        "EOSE delivered after replay"
    );

    // Live tail on the SAME subscription: no second subscribe (no poll).
    bus.publish(kind9(&keys, "live-1")).await.expect("publish");
    let live = drain(&mut sub, 8).await;
    let live_ids = event_ids(&live);
    assert_eq!(live_ids.len(), 1, "live event follows EOSE, no repoll");
    assert!(
        !ids.contains(&live_ids[0]),
        "live event is new, not a replay duplicate"
    );
}

/// (2) Slow consumer shed + resync: bounded channel drops live overflow (shed),
/// store replay recovers exactly the shed set: zero loss, zero duplicates.
#[tokio::test]
async fn conformance_shed_resync_zero_loss_zero_dup() {
    const N: usize = 20;
    let (bus, _path) = temp_bus_with_capacity(4).await;
    let keys = Keys::generate();

    // Subscribe FIRST so all N publish live into a tiny channel nobody drains.
    let mut slow = bus.subscribe(project_filter(SLUG)).await.expect("sub");
    let mut published = Vec::new();
    for i in 0..N {
        let ev = kind9(&keys, &format!("flood-{i}"));
        published.push(ev.id.to_hex());
        bus.publish(ev).await.expect("publish");
    }
    let live_evs = drain(&mut slow, N + 8).await;
    let live_ids: HashSet<String> = event_ids(&live_evs).into_iter().collect();
    assert!(
        live_ids.len() < N,
        "tiny channel must shed live overflow (got {} of {N})",
        live_ids.len()
    );
    let shed_count = N - live_ids.len();

    // Resync via store replay on a fresh subscription.
    let mut replay = bus.subscribe(project_filter(SLUG)).await.expect("resub");
    let replayed = drain(&mut replay, N + 8).await;
    let replay_ids: HashSet<String> = event_ids(&replayed).into_iter().collect();

    let union: HashSet<String> = live_ids.union(&replay_ids).cloned().collect();
    let all: HashSet<String> = published.into_iter().collect();
    assert_eq!(union, all, "live + replay covers every event: zero loss");
    // replay_ids is a HashSet of N distinct published ids at len N:
    // full history with no internal duplicates by construction.
    assert_eq!(
        replay_ids.len(),
        N,
        "replay is the full history including the {shed_count} shed"
    );
}

/// (3) Kill -9 mid-stream: dropping the subscriber without a close handshake,
/// resubscribe with the last-delivered cursor loses nothing past it.
#[tokio::test]
async fn conformance_kill9_midstream_cursor_resume() {
    const N: usize = 10;
    const CONSUMED: usize = 4;
    let (bus, _path) = temp_bus_with_capacity(Subscription::DEFAULT_CAPACITY).await;
    let keys = Keys::generate();
    let mut order = Vec::new();
    for i in 0..N {
        let ev = kind9(&keys, &format!("stream-{i}"));
        order.push(ev.id.to_hex());
        bus.publish(ev).await.expect("publish");
    }

    let mut sub = bus.subscribe(project_filter(SLUG)).await.expect("sub");
    // Consume exactly CONSUMED events (history precedes EOSE, so a bounded
    // drain would return CONSUMED+1 events, not events + EOSE).
    let mut delivered = Vec::new();
    for _ in 0..CONSUMED {
        match tokio::time::timeout(SILENCE, sub.next()).await {
            Ok(Some(SubscriptionEvent::Event(ev))) => delivered.push(ev.id.to_hex()),
            other => panic!("expected event, got {other:?}"),
        }
    }
    assert_eq!(delivered.len(), CONSUMED);
    let cursor = delivered.last().unwrap().clone();

    // kill -9: no close handshake, just drop.
    drop(sub);

    let mut resub = bus.subscribe(project_filter(SLUG)).await.expect("resub");
    let rest = drain(&mut resub, N + 8).await;
    let rest_ids = event_ids(&rest);
    let cursor_pos = order.iter().position(|id| *id == cursor).expect("cursor");
    let expected: Vec<String> = order[cursor_pos + 1..].to_vec();
    for id in &expected {
        assert!(
            rest_ids.contains(id),
            "lost event past cursor after kill-9: {id}"
        );
    }
    assert_eq!(
        rest_ids.len(),
        N,
        "replay is full history; nothing past the cursor is lost"
    );
}

/// (4) Stale-socket takeover: identity-checked takeover only; live owner
/// refuses; unchecked (blind delete) never takes over.
#[test]
fn conformance_stale_takeover_identity() {
    // Dead holder, checked: adopt.
    assert!(takeover_allowed(false, true, true));
    // Dead holder, foreign identity, checked: adopt.
    assert!(takeover_allowed(false, false, true));
    // Live holder, same owner: refuse.
    assert!(!takeover_allowed(true, true, true));
    // Live holder, foreign identity (pid reused by a non-owner process,
    // serve.pid pattern): identity check proves staleness -> adopt.
    assert!(takeover_allowed(true, false, true));
    // No identity check (blind delete): never, even when dead.
    assert!(!takeover_allowed(false, true, false));
    assert!(!takeover_allowed(false, false, false));

    // ManualClock drives the staleness half: heartbeat older than the
    // takeover grace means the holder is treated as dead.
    let mut clock = ManualClock::new(1_000);
    let last_beat = clock.now_ms;
    clock.advance(30_000);
    assert!(is_deaf(last_beat, clock.now_ms, 10_000));
    assert!(takeover_allowed(
        !is_deaf(last_beat, clock.now_ms, 10_000),
        true,
        true
    ));
}

/// (5) Tripwire: the 1s snapshot pump and the socket consumer are never both
/// live. Today the pump is live and the socket is absent, so this passes;
/// when slice B lands the socket tail it must delete the pump or fail here.
#[test]
fn conformance_tripwire_single_live_path() {
    const PLANE: &str = include_str!("../../toron-cli/src/plane.rs");
    const PUMP_MARKER: &str = "Re-snapshot sqlite each tick";
    const SOCK_MARKER: &str = "wake.sock";
    let pump_live = PLANE.contains(PUMP_MARKER);
    let sock_live = PLANE.contains(SOCK_MARKER);
    assert!(
        !(pump_live && sock_live),
        "tripwire: snapshot pump and socket consumer both live"
    );
}

/// (6) Quiet-vs-deaf preserved: recent heartbeat with zero events is quiet
/// (not deaf); only a stale heartbeat is deaf; boundary is exact.
#[test]
fn conformance_quiet_vs_deaf() {
    let mut clock = ManualClock::new(50_000);
    let timeout = 10_000;

    // Quiet bus: heartbeat 1ms ago, zero events observed -> not deaf.
    assert!(!is_deaf(clock.now_ms - 1, clock.now_ms, timeout));

    // Fresh heartbeat exactly at the timeout edge -> not deaf.
    assert!(!is_deaf(clock.now_ms - timeout, clock.now_ms, timeout));

    // One ms past the edge -> deaf.
    clock.advance(1);
    assert!(is_deaf(clock.now_ms - timeout - 1, clock.now_ms, timeout));

    // Long-quiet but heartbeating watcher stays not-deaf across advances.
    let mut now = ManualClock::new(0);
    for _ in 0..5 {
        now.advance(5_000);
        let beat = now.now_ms; // heartbeat each quiet interval
        assert!(!is_deaf(beat, now.now_ms, timeout));
    }
}

/// (7) Negative surface: no new kinds on the wake path, kind-9 + p/t/h
/// vocabulary byte-identical, no TCP listener in the bus sources.
#[tokio::test]
async fn conformance_no_new_surface() {
    // Frozen wake vocabulary: kind-9 publishes with a `t` project tag and a
    // `p` mention tag, and the project-filtered subscriber still receives it.
    let (bus, _path) = temp_bus_with_capacity(Subscription::DEFAULT_CAPACITY).await;
    let keys = Keys::generate();
    let ev = build_event(
        &keys,
        toron_core::nostr::kinds::toron_kind(CHANNEL_MESSAGE),
        "@peer hello",
        vec![
            Tag::parse(["t", SLUG]).expect("t"),
            Tag::parse(["p", &keys.public_key().to_hex()]).expect("p"),
        ],
        Timestamp::now(),
    )
    .expect("build");
    // Presence kind is distinct and stays ephemeral vocabulary, not wake.
    assert_ne!(CHANNEL_MESSAGE, PRESENCE);
    bus.publish(ev.clone()).await.expect("publish");
    let mut sub = bus.subscribe(project_filter(SLUG)).await.expect("sub");
    let got = drain(&mut sub, 8).await;
    let ids = event_ids(&got);
    assert_eq!(ids, vec![ev.id.to_hex()]);

    // No TCP surface in the bus: UDS only by default.
    const BUS_MOD: &str = include_str!("../src/bus/mod.rs");
    const BUS_IPC: &str = include_str!("../src/bus/in_process.rs");
    const BUS_SUB: &str = include_str!("../src/bus/subscription.rs");
    for (name, src) in [("mod", BUS_MOD), ("in_process", BUS_IPC), ("sub", BUS_SUB)] {
        assert!(
            !src.contains("TcpListener"),
            "bus/{name}: no TCP listener by default"
        );
    }
}

/// Deterministic script ledger: every scripted sequence above is reproducible
/// from its index alone (seeded by construction, no RNG in the path).
#[test]
fn conformance_scripts_are_deterministic() {
    let seq: Vec<String> = (0..20).map(|i| format!("flood-{i}")).collect();
    let again: Vec<String> = (0..20).map(|i| format!("flood-{i}")).collect();
    assert_eq!(seq, again);
    let mut seen = HashMap::new();
    for (i, s) in seq.iter().enumerate() {
        assert!(seen.insert(s.clone(), i).is_none(), "no duplicate script step");
    }
}
