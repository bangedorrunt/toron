// governed-by: ADR-0036
//! No silent loss on the wake socket (bd-wake-live-burst-loss-j86v,
//! bd-wake-replay-cap-qk8n).
//!
//! flywheel's live mcc9 run measured two holes upstream of the doorbell:
//!
//! 1. Live events in a burst vanished with no shed or gap frame (about 46%
//!    under normal load). The subscriber's resume cursor was
//!    `(created_at seconds, event id)`, and every live event that sorted at
//!    or below the last one delivered was suppressed as a "replay overlap".
//!    In a burst, `created_at` ties and event ids are random hex, so roughly
//!    half of a same-second burst sorted below its predecessor and was
//!    dropped.
//! 2. A resume replayed through a plain store subscription, which returns
//!    the newest 1000 rows. A cursor more than 1000 rows behind silently lost
//!    everything between it and that window (1000 of 2655 came back).
//!
//! The contract pinned here: a subscriber receives EVERY stored event after
//! its cursor, in arrival order, or an explicit `events_lost` frame naming
//! the range it can no longer have. The two loss repros each have a negative
//! control that passed before the fix too, so a green run is about the
//! defect, not luck. The gap pair is new behaviour: its control pins that a
//! subscriber with nothing evicted after its cursor is never told otherwise.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::wake::{SystemClock, WakeConfig, WakeHandle, WakeServer};
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;
use toron_core::nostr::event::build_event;
use toron_core::nostr::{Event, Keys, Kind, Tag, Timestamp};

const PROJECT: &str = "lossless";

struct Fixture {
    bus: Arc<InProcessBus>,
    pool: sqlx::SqlitePool,
    sock: PathBuf,
    db: PathBuf,
    keys: Keys,
    handle: Option<WakeHandle>,
}

impl Fixture {
    async fn new(name: &str) -> Self {
        let tag = format!("{}-{}-{}", name, std::process::id(), uuid::Uuid::new_v4());
        let db = std::env::temp_dir().join(format!("toron-wake-lossless-{tag}.sqlite"));
        let sock = std::env::temp_dir().join(format!(
            "twl-{}.sock",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        let pool = schema::connect(db.to_str().expect("utf8 db path"))
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = Arc::new(
            InProcessBus::open(
                pool.clone(),
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
            pool,
            sock,
            db,
            keys: Keys::generate(),
            handle: None,
        }
    }

    async fn serve(&mut self) {
        let _ = std::fs::remove_file(&self.sock);
        let mut cfg = WakeConfig::new(&self.sock);
        cfg.poll_interval_ms = 50;
        cfg.heartbeat_ms = 200;
        let bus: Arc<dyn Bus> = self.bus.clone();
        self.handle = Some(
            WakeServer::new(cfg, bus, Arc::new(SystemClock))
                .start()
                .await
                .expect("bind wake.sock"),
        );
    }

    fn event(&self, i: usize, created_at: u64) -> Event {
        build_event(
            &self.keys,
            Kind::from(9),
            format!("msg {i}"),
            vec![Tag::parse(["t", PROJECT]).expect("project tag")],
            Timestamp::from(created_at),
        )
        .expect("event")
    }

    /// Publish `n` events; `same_second` pins them all to one `created_at`.
    async fn publish(&self, from: usize, n: usize, same_second: bool) -> Vec<Event> {
        let base = 1_800_000_000_u64;
        let mut out = Vec::with_capacity(n);
        for i in from..from + n {
            let at = if same_second { base } else { base + i as u64 };
            let ev = self.event(i, at);
            self.bus.publish(ev.clone()).await.expect("publish");
            out.push(ev);
        }
        out
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

struct Client {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    _writer: OwnedWriteHalf,
}

/// Everything a subscriber saw, split by frame type.
#[derive(Default, Debug)]
struct Seen {
    ids: Vec<String>,
    shed: Vec<Value>,
    lost: Vec<Value>,
    eose: Option<String>,
}

impl Client {
    async fn connect(fx: &Fixture, since: Option<&str>) -> Self {
        let stream = UnixStream::connect(&fx.sock).await.expect("connect");
        let (reader, mut writer) = stream.into_split();
        let mut req = serde_json::json!({
            "subscribe": {"kinds": [9], "project": PROJECT, "since_cursor": since}
        })
        .to_string();
        req.push('\n');
        writer.write_all(req.as_bytes()).await.expect("subscribe");
        Self {
            lines: BufReader::new(reader).lines(),
            _writer: writer,
        }
    }

    /// Read one frame, or `None` at the deadline / EOF.
    async fn frame(&mut self, deadline: tokio::time::Instant) -> Option<Value> {
        let line = tokio::time::timeout_at(deadline, self.lines.next_line())
            .await
            .ok()?
            .ok()??;
        Some(serde_json::from_str(&line).expect("frame is json"))
    }

    fn sort(seen: &mut Seen, frame: Value) {
        if let Some(ev) = frame.get("event") {
            seen.ids
                .push(ev["id"].as_str().expect("event id").to_string());
        } else if let Some(e) = frame.get("eose") {
            seen.eose = Some(e["cursor"].as_str().expect("eose cursor").to_string());
        } else if let Some(s) = frame.get("shed") {
            seen.shed.push(s.clone());
        } else if let Some(l) = frame.get("events_lost") {
            seen.lost.push(l.clone());
        }
    }

    /// Replay phase: every frame up to and including EOSE.
    async fn replay(&mut self) -> Seen {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut seen = Seen::default();
        while seen.eose.is_none() {
            let frame = self.frame(deadline).await.expect("EOSE before deadline");
            Self::sort(&mut seen, frame);
        }
        seen
    }

    /// Live phase: read until `want` events arrived, a shed frame ends the
    /// stream, or the deadline passes.
    async fn live(&mut self, want: usize, within: Duration) -> Seen {
        let deadline = tokio::time::Instant::now() + within;
        let mut seen = Seen::default();
        while seen.ids.len() < want && seen.shed.is_empty() {
            let Some(frame) = self.frame(deadline).await else {
                break;
            };
            Self::sort(&mut seen, frame);
        }
        seen
    }
}

fn ids(events: &[Event]) -> Vec<String> {
    events.iter().map(|e| e.id.to_hex()).collect()
}

fn legacy_cursor(ev: &Event) -> String {
    format!("{}:{}", ev.created_at.as_secs(), ev.id.to_hex())
}

/// REPRO (b): flywheel's 2655-row resume, from a cursor at the very first
/// row. Before the fix 1000 rows came back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_far_behind_replays_every_row_after_the_cursor() {
    let mut fx = Fixture::new("far-behind").await;
    let history = fx.publish(0, 2655, false).await;
    fx.serve().await;

    let mut client = Client::connect(&fx, Some(&legacy_cursor(&history[0]))).await;
    let seen = client.replay().await;
    assert!(seen.lost.is_empty(), "nothing was evicted: {:?}", seen.lost);
    assert_eq!(
        seen.ids.len(),
        2654,
        "every row after the cursor, not the newest-1000 window"
    );
    assert_eq!(seen.ids, ids(&history[1..]), "in arrival order");
}

/// REPRO (b), fresh subscribe: no cursor replays the whole stored history.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_subscribe_replays_the_whole_history_not_the_newest_1000() {
    let mut fx = Fixture::new("fresh").await;
    let history = fx.publish(0, 1500, false).await;
    fx.serve().await;

    let seen = Client::connect(&fx, None).await.replay().await;
    assert_eq!(seen.ids, ids(&history));
}

/// REPRO (b), round trip: the EOSE cursor a subscriber was handed resumes
/// exactly after itself, even when more than 1000 rows landed in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_eose_cursor_resumes_exactly_after_a_long_absence() {
    let mut fx = Fixture::new("eose-resume").await;
    fx.publish(0, 10, false).await;
    fx.serve().await;
    let first = Client::connect(&fx, None).await.replay().await;
    let cursor = first.eose.expect("eose cursor");
    assert_ne!(cursor, "head");

    let later = fx.publish(10, 1500, false).await;
    let seen = Client::connect(&fx, Some(&cursor)).await.replay().await;
    assert_eq!(seen.ids, ids(&later));
}

/// CONTROL (b): a cursor inside the newest 1000 rows. This passed before the
/// fix too; it pins that the fix did not break the common resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_a_resume_inside_the_newest_window_was_always_exact() {
    let mut fx = Fixture::new("near").await;
    let history = fx.publish(0, 1200, false).await;
    fx.serve().await;

    let seen = Client::connect(&fx, Some(&legacy_cursor(&history[1000])))
        .await
        .replay()
        .await;
    assert_eq!(seen.ids, ids(&history[1001..]));
}

/// REPRO (a): a live burst inside one second. Before the fix about half of
/// it was suppressed by the `(created_at, id)` cursor, with no frame at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_same_second_live_burst_is_delivered_in_full() {
    let mut fx = Fixture::new("burst").await;
    fx.serve().await;
    let mut client = Client::connect(&fx, None).await;
    let _ = client.replay().await;

    let burst = fx.publish(0, 400, true).await;
    let seen = client.live(burst.len(), Duration::from_secs(20)).await;
    assert!(seen.shed.is_empty(), "no shed: {:?}", seen.shed);
    assert!(seen.lost.is_empty(), "no loss frame: {:?}", seen.lost);
    assert_eq!(seen.ids, ids(&burst), "every event, in publish order");
}

/// REPRO (a) under backpressure: the reader is away while a burst larger
/// than every internal queue lands. It must still get the whole burst once it
/// reads, with no shed and no loss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_reader_still_receives_a_burst_larger_than_every_queue() {
    let mut fx = Fixture::new("slow").await;
    fx.serve().await;
    let mut client = Client::connect(&fx, None).await;
    let _ = client.replay().await;

    let burst = fx.publish(0, 3000, true).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let seen = client.live(burst.len(), Duration::from_secs(30)).await;
    assert!(seen.shed.is_empty(), "no shed: {:?}", seen.shed);
    assert!(seen.lost.is_empty(), "no loss frame: {:?}", seen.lost);
    assert_eq!(seen.ids.len(), burst.len());
    assert_eq!(seen.ids, ids(&burst));
}

/// CONTROL (a): the same burst size with strictly increasing seconds. This
/// passed before the fix; it pins that the defect was the same-second
/// ordering, and that ordinary live delivery is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_a_live_burst_in_distinct_seconds_was_always_whole() {
    let mut fx = Fixture::new("distinct").await;
    fx.serve().await;
    let mut client = Client::connect(&fx, None).await;
    let _ = client.replay().await;

    let burst = fx.publish(0, 400, false).await;
    let seen = client.live(burst.len(), Duration::from_secs(20)).await;
    assert_eq!(seen.ids, ids(&burst));
}

/// Pretend the cap evicted arrivals up to `through` (prune's own recording is
/// pinned in the event_store unit tests; this drives the frame).
async fn mark_evicted_through(pool: &sqlx::SqlitePool, through: i64) {
    sqlx::query(
        "INSERT INTO bus_meta (key, value) VALUES ('arrivals_evicted_hwm', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(through)
    .execute(pool)
    .await
    .expect("mark eviction");
}

fn seq_of(cursor: &str) -> i64 {
    cursor
        .strip_prefix("seq:")
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("not a seq cursor: {cursor}"))
}

/// REPRO (gap): a cursor behind evicted rows gets an explicit events_lost
/// frame before the rows it can still have. Before the fix it got nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cursor_behind_evicted_rows_gets_an_explicit_events_lost_frame() {
    let mut fx = Fixture::new("gap").await;
    fx.publish(0, 5, false).await;
    fx.serve().await;
    let cursor = Client::connect(&fx, None)
        .await
        .replay()
        .await
        .eose
        .expect("eose");
    let at = seq_of(&cursor);
    let later = fx.publish(5, 5, false).await;
    mark_evicted_through(&fx.pool, at + 2).await;

    let seen = Client::connect(&fx, Some(&cursor)).await.replay().await;
    assert_eq!(seen.lost.len(), 1, "exactly one loss frame: {seen:?}");
    assert_eq!(seen.lost[0]["from"], cursor);
    assert_eq!(seen.lost[0]["through"], format!("seq:{}", at + 2));
    assert_eq!(seen.ids, ids(&later), "rows still stored are replayed");
}

/// CONTROL (gap): a cursor at or past the eviction mark, and a fresh
/// subscribe, get no loss frame.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_no_events_lost_frame_when_nothing_after_the_cursor_was_evicted() {
    let mut fx = Fixture::new("nogap").await;
    fx.publish(0, 5, false).await;
    fx.serve().await;
    let cursor = Client::connect(&fx, None)
        .await
        .replay()
        .await
        .eose
        .expect("eose");
    mark_evicted_through(&fx.pool, seq_of(&cursor)).await;
    let later = fx.publish(5, 3, false).await;

    let resumed = Client::connect(&fx, Some(&cursor)).await.replay().await;
    assert!(resumed.lost.is_empty(), "{resumed:?}");
    assert_eq!(resumed.ids, ids(&later));

    let fresh = Client::connect(&fx, None).await.replay().await;
    assert!(fresh.lost.is_empty(), "fresh subscribe never reports a gap");
}
