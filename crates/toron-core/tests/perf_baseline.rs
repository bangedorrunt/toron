// governed-by: ADR-0003
// (not forbid(unsafe_code): this test sets no unsafe; it is a measurement
// probe only.)

//! P1 performance baseline (bd-p1-backpressure-baseline-6gmq).
//!
//! Measured numbers feed the ADR-0009 SLO proposal (findings.md table). The
//! assertions are GENEROUS pathological bounds (p99 < 100ms projection, etc.)
//! — they fail only on a real regression, not on CI jitter. Run with
//! `--nocapture` to capture the percentiles:
//!
//! ```text
//! cargo test -p toron-core --test perf_baseline -- --nocapture --ignored
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use nostr_sdk::nostr::{Keys, Kind, Tag, Timestamp};
use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
use toron_core::bus::{Bus, Filter, SubscriptionEvent};
use toron_core::index::schema;
use toron_core::mailbox::unwrap::{ProjectionSink, SqliteProjectionSink, UnwrappedMail};

fn percentiles(mut samples: Vec<Duration>) -> (Duration, Duration, Duration) {
    samples.sort();
    let p = |q: f64| samples[((samples.len() as f64 * q) as usize).min(samples.len() - 1)];
    (p(0.50), p(0.95), p(0.99))
}

fn fmt(d: Duration) -> String {
    format!("{:?}", d)
}

async fn temp_pool(tag: &str) -> (sqlx::SqlitePool, std::path::PathBuf) {
    let path =
        std::env::temp_dir().join(format!("toron-perf-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
    let pool = schema::connect(path.to_str().unwrap())
        .await
        .expect("connect");
    schema::migrate(&pool).await.expect("migrate");
    (pool, path)
}

fn rumor_for(sender: &Keys, recipient: &Keys, i: u64) -> UnwrappedMail {
    let rumor = toron_core::nostr::gift_wrap::build_rumor(
        sender.public_key(),
        Timestamp::from(1_700_000_000 + i),
        Kind::PrivateDirectMessage,
        vec![
            Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p"),
            Tag::parse(["subject", &format!("perf {i}")]).expect("subject"),
        ],
        format!("perf body {i} — searchable content marker {i}"),
    );
    let id = rumor.clone().id();
    UnwrappedMail {
        recipient: recipient.public_key(),
        sender: sender.public_key(),
        rumor,
        rumor_id: id,
        wrap_event_id: id,
        created_at: Timestamp::from(1_700_000_000 + i),
    }
}

/// 1. Broadcast bus: burst throughput + subscriber lag ceiling.
///
/// Publishes N gift-wrap events back-to-back; a live subscriber drains and
/// records per-event receipt lag. The lossless mpsc means a full channel
/// drops publisher-side into the store (replay recovers) — the lag ceiling
/// captures that backpressure behavior.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf probe: run explicitly with --ignored --nocapture"]
async fn bus_burst_throughput_and_lag_ceiling() {
    const N: usize = 2_000;
    let (pool, path) = temp_pool("bus").await;
    let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
        .await
        .expect("bus");
    let mut sub = bus
        .subscribe(Filter::new().kinds([Kind::GiftWrap]))
        .await
        .expect("sub");

    let sender = Keys::generate();
    let recipient = Keys::generate();

    // Drainer runs CONCURRENTLY with the publisher on its OWN thread/runtime
    // (sharing the publisher's runtime would starve the drainer during the
    // CPU-bound gift-wrap burst — that's a measurement artifact, not bus lag).
    // Receipts carry the receipt INSTANT so lag is timestamped at receipt.
    let (lag_tx, lag_rx) = std::sync::mpsc::channel::<(nostr_sdk::nostr::EventId, Instant)>();
    let drainer = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("drainer rt");
        let mut received = 0usize;
        rt.block_on(async {
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(500), sub.next()).await {
                    Ok(Some(SubscriptionEvent::Event(ev))) => {
                        let _ = lag_tx.send((ev.id, Instant::now()));
                        received += 1;
                    }
                    Ok(_) => continue,
                    // Only exit on silence AFTER the burst started (the
                    // pre-publish wrap-build phase is legitimately quiet).
                    Err(_) if received > 0 => break,
                    Err(_) => continue,
                }
            }
        });
        received
    });

    // Phase 1: pre-build all wraps (NIP-44 cost measured separately from bus).
    let wrap_t0 = Instant::now();
    let mut wraps = Vec::with_capacity(N);
    for i in 0..N {
        let rumor = toron_core::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000 + i as u64),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            format!("burst {i}"),
        );
        wraps.push(
            toron_core::nostr::gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor)
                .await
                .expect("wrap"),
        );
    }
    let build_elapsed = wrap_t0.elapsed();

    // Phase 2: publish loop only (bus throughput).
    let t0 = Instant::now();
    let mut publish_starts = Vec::with_capacity(N);
    for wrap in &wraps {
        publish_starts.push((wrap.id, Instant::now()));
        bus.publish(wrap.clone()).await.expect("publish");
    }
    let publish_elapsed = t0.elapsed();

    // Lags: receipt_instant - publish-start (true ingest latency per event).
    // Give the isolated drainer a beat to schedule, then drain its channel.
    std::thread::sleep(Duration::from_millis(200));
    let mut lags = Vec::new();
    while let Ok((id, receipt)) = lag_rx.try_recv() {
        if let Some((_, start)) = publish_starts.iter().find(|(pid, _)| *pid == id) {
            lags.push(receipt.saturating_duration_since(*start));
        }
    }
    let received = drainer.join().expect("drainer");
    eprintln!(
        "DEBUG: drainer received={received} lag_samples={}",
        lags.len()
    );
    assert!(
        !lags.is_empty(),
        "drainer must have received at least some events during the burst (received={received})"
    );
    let (p50, p95, p99) = percentiles(lags.clone());
    let max = lags.iter().copied().max().unwrap_or_default();
    let throughput = N as f64 / publish_elapsed.as_secs_f64();

    // Backpressure contract: the lossless mpsc drops publisher-side into the
    // STORE when the channel is full (never lost) — a fresh subscription
    // replays them. Count live + replayed = N proves nothing was lost.
    let mut replayed = 0usize;
    let mut recovered = bus
        .subscribe(Filter::new().kinds([Kind::GiftWrap]).limit(N + 10))
        .await
        .expect("replay sub");
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), recovered.next()).await {
            Ok(Some(SubscriptionEvent::Event(_))) => replayed += 1,
            Ok(Some(SubscriptionEvent::Eose)) => break,
            Ok(_) => continue,
            Err(_) => break,
        }
    }
    println!(
        "bus_burst: N={N} live_delivered={received} replayed_from_store={replayed} wrap_build={} publish_total={} bus_throughput={:.0} ev/s lag p50={} p95={} p99={} max={}",
        fmt(build_elapsed),
        fmt(publish_elapsed),
        throughput,
        fmt(p50),
        fmt(p95),
        fmt(p99),
        fmt(max)
    );
    assert_eq!(
        replayed, N,
        "backpressure must never LOSE events (store recovers all {N})"
    );
    let _ = std::fs::remove_file(&path);
}

/// 2. SQLite projection latency: p50/p95/p99 over 1000 sink writes.
#[tokio::test]
#[ignore = "perf probe: run explicitly with --ignored --nocapture"]
async fn sqlite_projection_latency_per_1000() {
    const N: usize = 1_000;
    let (pool, path) = temp_pool("proj").await;
    let sink = SqliteProjectionSink::new(pool.clone());
    let sender = Keys::generate();
    let recipient = Keys::generate();

    let mut samples = Vec::with_capacity(N);
    for i in 0..N {
        let mail = rumor_for(&sender, &recipient, i as u64);
        let t = Instant::now();
        let wrote = sink.write(&mail, i as u64).await.expect("write");
        samples.push(t.elapsed());
        assert!(wrote, "unique mail {i} lands");
    }
    let (p50, p95, p99) = percentiles(samples);
    println!(
        "sqlite_projection N={N}: p50={} p95={} p99={}",
        fmt(p50),
        fmt(p95),
        fmt(p99)
    );
    assert!(
        p99 < Duration::from_millis(100),
        "projection p99 must stay < 100ms (pathological bound), got {p99:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// 3. Search landing latency: FTS query against the projected index.
#[tokio::test]
#[ignore = "perf probe: run explicitly with --ignored --nocapture"]
async fn search_landing_latency() {
    let (pool, path) = temp_pool("search").await;
    let sink = SqliteProjectionSink::new(pool.clone());
    let sender = Keys::generate();
    let recipient = Keys::generate();
    for i in 0..500 {
        let mail = rumor_for(&sender, &recipient, i as u64);
        sink.write(&mail, i as u64).await.expect("write");
    }

    let mut samples = Vec::new();
    for _ in 0..20 {
        let t = Instant::now();
        let rows: Vec<sqlx::sqlite::SqliteRow> = sqlx::query(
            "SELECT m.event_id FROM messages m \
             JOIN messages_fts f ON f.rowid = m.id \
             WHERE messages_fts MATCH ?1 LIMIT 10",
        )
        .bind("marker")
        .fetch_all(&pool)
        .await
        .expect("search");
        samples.push(t.elapsed());
        assert!(!rows.is_empty(), "MATCH returns rows");
    }
    let (p50, p95, p99) = percentiles(samples);
    println!(
        "search_landing: p50={} p95={} p99={}",
        fmt(p50),
        fmt(p95),
        fmt(p99)
    );
    assert!(
        p95 < Duration::from_millis(100),
        "search landing p95 must stay < 100ms, got {p95:?}"
    );
    let _ = std::fs::remove_file(&path);
}

/// 4. KDF cold-start reference: the F2.KDF gate test owns the many-keys
/// measurement (unwrap_loop_kdf_cached_between_rebuilds_many_keys, ~7.9s for
/// 50 keys). This probe records a single-key cold load as the per-key floor.
#[tokio::test]
#[ignore = "perf probe: run explicitly with --ignored --nocapture"]
async fn kdf_single_key_cold_load() {
    let dir = std::env::temp_dir().join(format!("toron-perf-kdf-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("dir");
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(pool, InProcessBusConfig::default())
        .await
        .expect("bus");
    let bus: Arc<dyn Bus> = Arc::new(bus);
    let mailbox = Arc::new(
        toron_core::mailbox::Mailbox::new(bus, dir.clone(), None)
            .with_agent_passphrase("perf-kdf-pass"),
    );
    mailbox
        .register_agent(
            "demo",
            &toron_core::nostr::identity::AgentProfile::new("KdfProbe"),
        )
        .await
        .expect("register");

    // Cold KDF load via the public fetch_inbox path (loads + decrypts the
    // agent key with argon2 on first use).
    let t = Instant::now();
    let _ = mailbox
        .fetch_inbox(&toron_core::mailbox::messaging::FetchInbox {
            project_slug: "demo".into(),
            agent_name: "KdfProbe".into(),
            unread_only: false,
            limit: None,
            include_bodies: true,
        })
        .await
        .expect("inbox");
    let cold = t.elapsed();
    println!("kdf_single_key_cold_load: {cold:?}");
    assert!(
        cold < Duration::from_secs(30),
        "single-key cold KDF load must stay < 30s, got {cold:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
