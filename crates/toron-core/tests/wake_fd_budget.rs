// governed-by: ADR-0036
//! bd-j2vo: the wake plane must never be what takes the daemon down.
//!
//! Measured on the live daemon (2026-10-22): the daemon held 266 fds (229 of
//! them `wake.sock` connections) against launchd's 256 soft limit, the accept
//! loop spun on `EMFILE`, and the MCP plane stopped accepting — so `toron
//! mail send`, `toron reserve acquire` and the commit guard's roster read all
//! failed. The subscriber count was unbounded, and a dead subscriber's socket
//! was never released.
//!
//! Two fences, one per half of the failure:
//!
//! 1. The concurrent subscriber CAP. Over the cap the SUBSCRIBER is refused
//!    (counted + logged); the daemon keeps serving and the MCP plane keeps
//!    answering. An accept error must not end the wake plane either.
//! 2. The RECLAIM SIGNAL. The daemon learns a subscriber is gone from a
//!    failed WRITE, and the heartbeat is the write — so the heartbeat must
//!    fire on its own cadence, not only when the bus is quiet. It used to be
//!    written only on a quiet tick, so on a busy bus the ring arm won every
//!    round, the heartbeat never fired, and the dead subscriber's fd was held
//!    for the life of the daemon.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::wake::{SystemClock, WakeConfig, WakeHandle, WakeServer};
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;
use toron_core::nostr::event::build_event;
use toron_core::nostr::{Keys, Kind, Tag, Timestamp};

const PROJECT: &str = "fdbudget";

struct Fixture {
    bus: Arc<InProcessBus>,
    sock: PathBuf,
    db: PathBuf,
    handle: Option<WakeHandle>,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let tag = format!("{}-{}-{}", name, std::process::id(), uuid::Uuid::new_v4());
        let db = std::env::temp_dir().join(format!("toron-wake-fd-{tag}.sqlite"));
        let sock = std::env::temp_dir()
            .join(format!("twf-{}.sock", &uuid::Uuid::new_v4().simple().to_string()[..12]));
        let pool = schema::connect(db.to_str().expect("utf8 db path"))
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = Arc::new(
            InProcessBus::open(
                pool,
                InProcessBusConfig {
                    auth: AuthPolicy::AcceptAll,
                    allowlist: Vec::new(),
                    channel_capacity: 1024,
                    detailed_stage_logging: false,
                },
            )
            .await
            .expect("bus"),
        );
        Self {
            bus,
            sock,
            db,
            handle: None,
        }
    }

    async fn serve(&mut self, max_subscribers: usize, heartbeat_ms: u64) {
        let _ = std::fs::remove_file(&self.sock);
        let mut cfg = WakeConfig::new(&self.sock);
        cfg.poll_interval_ms = 0;
        cfg.heartbeat_ms = heartbeat_ms;
        cfg.max_subscribers = max_subscribers;
        let bus: Arc<dyn Bus> = self.bus.clone();
        self.handle = Some(
            WakeServer::new(cfg, bus, Arc::new(SystemClock))
                .start()
                .await
                .expect("bind wake.sock"),
        );
    }

}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.handle.take();
        let _ = std::fs::remove_file(&self.sock);
        for suffix in ["", "-wal", "-shm"] {
            let mut p = self.db.clone().into_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(p));
        }
    }
}

/// Connect + subscribe. `kinds` is what the subscriber asks for.
async fn subscribe(fx: &Fixture, kinds: &[u16]) -> BufReader<tokio::net::unix::OwnedReadHalf> {
    let stream = UnixStream::connect(&fx.sock).await.expect("connect");
    let (reader, mut writer) = stream.into_split();
    let mut req = serde_json::json!({
        "subscribe": {"kinds": kinds, "project": PROJECT}
    })
    .to_string();
    req.push('\n');
    // A refused subscriber's socket is already closed: the write failing IS
    // the refusal, so it must not fail the test.
    let _ = writer.write_all(req.as_bytes()).await;
    BufReader::new(reader)
}

async fn read_frame(reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>) -> Option<String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    match tokio::time::timeout_at(deadline, reader.lines().next_line()).await {
        Ok(Ok(Some(line))) => Some(line),
        _ => None,
    }
}

async fn wait_live(h: &WakeHandle, want: usize) -> bool {
    for _ in 0..200 {
        if h.aggregate_stats().subscribers_live == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// bd-j2vo, half 1: over the cap the SUBSCRIBER is refused, never the daemon.
#[tokio::test]
async fn the_cap_refuses_a_subscriber_and_the_plane_keeps_serving() {
    let mut fx = Fixture::new("cap").await;
    fx.serve(1, 100).await;
    let h = fx.handle.as_ref().expect("handle");

    // One subscriber inside the cap is served.
    let mut first = subscribe(&fx, &[9]).await;
    let eose = read_frame(&mut first).await.expect("the served subscriber gets a frame");
    assert!(eose.contains("eose"), "expected eose, got {eose:?}");
    assert!(
        wait_live(h, 1).await,
        "the served subscriber is live: {:?}",
        h.aggregate_stats()
    );

    // The next subscriber is refused: its connection is dropped, so it reads
    // EOF instead of a frame.
    let mut refused = subscribe(&fx, &[9]).await;
    assert!(
        read_frame(&mut refused).await.is_none(),
        "a subscriber over the cap gets no frame — its connection is dropped, \
         the daemon is not"
    );

    let stats = h.aggregate_stats();
    assert_eq!(stats.refused, 1, "the refusal is counted: {stats:?}");
    assert_eq!(
        stats.subscribers_live, 1,
        "the refused subscriber took no slot: {stats:?}"
    );

    // The plane still serves: once the first subscriber leaves, a new one is
    // served normally. The daemon survived the refusal.
    drop(first);
    assert!(wait_live(h, 0).await, "the first subscriber left");
    let mut third = subscribe(&fx, &[9]).await;
    let eose = read_frame(&mut third).await.expect("the plane still serves");
    assert!(eose.contains("eose"), "expected eose, got {eose:?}");
}

/// bd-j2vo, half 2: the heartbeat is the reclaim signal, so it must fire on a
/// BUSY bus. The subscriber below asks for a kind the bus never publishes,
/// so the drain writes it nothing — the heartbeat is the only write, and it
/// is the only thing that can release the dead subscriber's fd.
#[tokio::test]
async fn a_dead_subscriber_is_reclaimed_while_the_bus_rings() {
    let mut fx = Fixture::new("reclaim").await;
    fx.serve(8, 100).await;
    let h = fx.handle.as_ref().expect("handle");

    let mut reader = subscribe(&fx, &[30031]).await;
    let eose = read_frame(&mut reader).await.expect("the subscriber gets a frame");
    assert!(eose.contains("eose"), "expected eose, got {eose:?}");
    assert!(wait_live(h, 1).await, "the subscriber is live");

    // A busy bus: rings faster than the heartbeat interval, so the quiet
    // tick never wins.
    let publisher = {
        let bus = fx.bus.clone();
        tokio::spawn(async move {
            for i in 0..2_000u32 {
                let ev = build_event(
                    &Keys::generate(),
                    Kind::from(9),
                    format!("ring {i}"),
                    vec![Tag::parse(["t", PROJECT]).expect("project tag")],
                    Timestamp::from(1_800_000_000 + i as u64),
                )
                .expect("event");
                let _ = bus.publish(ev).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };

    // The subscriber's peer goes away.
    drop(reader);

    // The reclaim bound is one heartbeat interval: the daemon's next
    // heartbeat write fails and the fd is released.
    let mut released = false;
    for _ in 0..200 {
        let stats = h.aggregate_stats();
        if stats.subscribers_live == 0 && stats.heartbeat_deaf >= 1 {
            released = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    publisher.abort();
    let stats = h.aggregate_stats();
    assert!(
        released,
        "a dead subscriber's socket must be released by the heartbeat write, not held \
         for the life of the daemon: {stats:?}"
    );
}
