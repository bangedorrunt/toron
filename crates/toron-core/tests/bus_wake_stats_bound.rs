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

fn rss_kb() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

async fn serve_one_subscriber(sock: &std::path::Path) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let stream = tokio::net::UnixStream::connect(sock)
        .await
        .expect("connect");
    let (r, mut w) = stream.into_split();
    w.write_all(b"{\"subscribe\":{}}\n").await.expect("sub");
    let mut lines = BufReader::new(r).lines();
    // Drain the EOSE so the handshake completes and the task is live.
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
    // A Unix socket path must fit SUN_LEN, so keep it short and unique rather
    // than nesting a uuid directory under the temp dir.
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
    // Short heartbeat so a dropped connection is reaped promptly.
    let mut cfg = WakeConfig::new(sock.clone());
    cfg.heartbeat_ms = 50;
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

    // The aggregate is a true count of everyone served, not a count of what
    // the detail window happens to still retain.
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
    // At least one frame per subscriber (the EOSE). The exact frame count is
    // timing-dependent on a real clock and is pinned deterministically by the
    // in-crate ManualClock test; what matters here is that it is not capped.
    assert!(
        agg.delivered >= served as u64,
        "every subscriber got at least its EOSE: delivered {} for {served} subscribers",
        agg.delivered
    );
}

#[tokio::test]
async fn serving_many_subscribers_in_sequence_does_not_retain_them_all() {
    // The actual leak: retention grew one record per finished subscriber for
    // the life of the process. Serving a long sequence must not grow the
    // process in step with the count. The aggregate is still exact at the end,
    // which is what a naive ring buffer would get wrong.
    let (sock, _bus, h) = boot().await;

    // More than STATS_CAP (1024) so the cap demonstrably engages. Below the
    // cap this test could not distinguish bounded from unbounded retention.
    let rounds = 1_500usize;
    let before = rss_kb();
    for _ in 0..rounds {
        serve_one_subscriber(&sock).await;
    }
    let agg = wait_for_finished(&h, rounds).await;
    let after = rss_kb();

    // The falsifiable pair: the detail view dropped the oldest records...
    let retained = h.stats().len();
    assert!(
        retained < rounds,
        "detail view is bounded: retained {retained} of {rounds} served"
    );
    // ...while the aggregate still counts every single one. This is what a
    // naive ring buffer gets wrong.
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
        "serving {rounds} subscribers grew RSS by {grew_kb} KB; retention is not bounded"
    );
}
