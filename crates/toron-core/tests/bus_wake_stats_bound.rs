// governed-by: ADR-0003
//! bd-qpm0 fence: the wake sidecar's finished-subscriber accounting must be
//! bounded in memory, while the exported aggregate stays exact.
//!
//! Before this, every finished subscriber was pushed onto an unbounded
//! `Vec<SubscriberStats>` that nothing ever removed from, and
//! `aggregate_stats()` cloned that whole Vec and rescanned it on every call.
//! Retention grew for the life of the process and the aggregate got
//! progressively more expensive to read.
//!
//! The trap this test pins: bounding the retained detail must NOT cap the
//! aggregate. `subscribers_finished` counts everyone ever served, so a naive
//! ring buffer would silently under-report it once the window filled. The
//! aggregate is served from running totals instead, so it must stay exact and
//! must agree with the detail view for every subscriber served so far.
//!
//! Driven through the public `WakeServer` / `WakeHandle` surface with a real
//! Unix-socket subscriber per connection, so the accounting under test is the
//! production path, not a re-implementation of it.

use std::sync::Arc;
use std::time::Duration;

use toron_core::bus::in_process::InProcessBusConfig;
use toron_core::bus::wake::{WakeConfig, WakeServer, WakeStats};
use toron_core::bus::{Bus, InProcessBus};
use toron_core::index::schema;

/// Parse one `ps -o rss=` sample, in KB.
///
/// Refuses an unusable sample instead of defaulting to 0. The old
/// `unwrap_or(0)` turned a failed measurement into "RSS grew by nothing",
/// which made the caller's growth bound pass for the wrong reason — a
/// silent-success stub, and this repo forbids those. If `ps` is missing or
/// emits something unparseable, the test must go red and say so.
fn parse_rss_kb(sample: &str) -> u64 {
    sample
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("unparseable ps rss sample: {sample:?}"))
}

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    parse_rss_kb(&String::from_utf8_lossy(&out.stdout))
}

#[test]
fn parse_rss_kb_reads_a_numeric_sample() {
    assert_eq!(parse_rss_kb("  12345 \n"), 12_345);
}

#[test]
#[should_panic(expected = "unparseable")]
fn parse_rss_kb_refuses_an_unusable_sample() {
    parse_rss_kb("not-a-number");
}

async fn serve_one_subscriber(sock: &std::path::Path) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::UnixStream::connect(sock)
        .await
        .expect("connect");
    let (r, mut w) = stream.into_split();
    w.write_all(b"{\"subscribe\":{}}\n").await.expect("sub");
    let mut lines = BufReader::new(r).lines();
    let eose = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("eose bound")
        .expect("io")
        .expect("eose line");
    assert!(eose.contains("eose"), "expected eose, got {eose:?}");
    drop(lines);
    drop(w);
}

async fn wait_for_finished(h: &toron_core::bus::wake::WakeHandle, want: usize) -> WakeStats {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let a = h.aggregate_stats();
            if a.subscribers_finished >= want && a.subscribers_live == 0 {
                return a;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{want} finished subscribers never landed"))
}

async fn boot() -> (
    std::path::PathBuf,
    Arc<dyn Bus>,
    toron_core::bus::wake::WakeHandle,
) {
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let mut sock = std::env::temp_dir();
    sock.push(format!("tw-{}-{}.sock", std::process::id(), &tag[..8]));
    let mut db = std::env::temp_dir();
    db.push(format!("tw-{}-{}.sqlite", std::process::id(), &tag[..8]));

    let pool = schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    let bus: Arc<dyn Bus> = Arc::new(
        InProcessBus::open(pool, InProcessBusConfig::default())
            .await
            .expect("bus"),
    );
    let mut cfg = WakeConfig::new(sock.clone());
    cfg.heartbeat_ms = 50;
    cfg.max_subscribers = 4_096;
    let h = WakeServer::new(
        cfg,
        bus.clone(),
        Arc::new(toron_core::bus::wake::SystemClock),
    )
    .start()
    .await
    .expect("start");
    (sock, bus, h)
}

#[tokio::test]
async fn aggregate_counts_every_subscriber_and_matches_the_detail_view() {
    let (sock, _bus, h) = boot().await;

    assert_eq!(
        h.aggregate_stats(),
        WakeStats::default(),
        "nothing served yet: both views empty"
    );
    assert!(h.stats().is_empty());

    let served = 12usize;
    for _ in 0..served {
        serve_one_subscriber(&sock).await;
    }

    let agg = wait_for_finished(&h, served).await;

    assert_eq!(
        agg.subscribers_finished, served,
        "aggregate finished-count is exact"
    );
    assert_eq!(agg.subscribers_live, 0, "all connections reaped");
    assert_eq!(
        h.stats().len(),
        served,
        "detail view holds every subscriber while under the cap"
    );
    assert!(
        agg.delivered >= served as u64,
        "every subscriber got at least its EOSE: delivered {} for {served} subscribers",
        agg.delivered
    );
}

#[tokio::test]
async fn serving_many_subscribers_in_sequence_does_not_retain_them_all() {
    let (sock, _bus, h) = boot().await;

    let rounds = 1_500usize;
    let before = rss_kb();
    for _ in 0..rounds {
        serve_one_subscriber(&sock).await;
    }
    let agg = wait_for_finished(&h, rounds).await;
    let after = rss_kb();

    let retained = h.stats().len();
    assert!(
        retained < rounds,
        "detail view is bounded: retained {retained} of {rounds} served"
    );
    assert_eq!(
        agg.subscribers_finished, rounds,
        "aggregate counts all {rounds} despite the bounded detail view"
    );
    assert!(
        agg.delivered >= rounds as u64,
        "delivered is not capped by the detail window: {} for {rounds} subscribers",
        agg.delivered
    );

    let grew_kb = after.saturating_sub(before);
    assert!(
        grew_kb < 32_768,
        "serving {rounds} subscribers grew RSS by {grew_kb} KB, past the coarse \
         32 MB guard (the retention fence itself is `retained < rounds` above)"
    );
}
