// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Harness: boots an in-process toron daemon for scenario tests.
//!
//! * InProcessBus on a temp SQLite pool.
//! * MockBackend (no real herdr).
//! * Temp Git archive + index for the projection.
//! * Fixed keypairs + virtual clock for determinism.
//!
//! BuzzTestClient-style surface: `connect` / `send_event` / `subscribe` /
//! `collect_until_eose`.

use std::sync::Arc;

use toron_core::backend::mock::MockBackend;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::{Bus, InProcessBus, SubscriptionEvent};
use toron_core::index::schema;

/// Deterministic clock (i64-µs) for scenario tests — no wall-clock sleeps.
#[derive(Debug, Clone)]
pub struct VirtualClock {
    now: Arc<std::sync::atomic::AtomicI64>,
}

impl VirtualClock {
    pub fn new(start_us: i64) -> Self {
        Self {
            now: Arc::new(std::sync::atomic::AtomicI64::new(start_us)),
        }
    }

    pub fn now_us(&self) -> i64 {
        self.now.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn advance(&self, us: i64) {
        self.now.fetch_add(us, std::sync::atomic::Ordering::Relaxed);
    }
}

/// An in-process toron test daemon.
pub struct TestDaemon {
    pub bus: InProcessBus,
    pub backend: MockBackend,
    pub clock: VirtualClock,
    pub db_path: std::path::PathBuf,
    pub archive_root: std::path::PathBuf,
}

impl TestDaemon {
    /// Boot the daemon: temp dir, SQLite pool + migrations, InProcessBus,
    /// MockBackend, virtual clock at a fixed epoch.
    pub async fn boot() -> anyhow::Result<Self> {
        let dir = std::env::temp_dir().join(format!("toron-e2e-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir)?;
        let db_path = dir.join("index.sqlite");
        let archive_root = dir.join("archive");

        let pool = schema::connect(db_path.to_str().unwrap()).await?;
        schema::migrate(&pool).await?;

        let config = InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: true,
        };
        let bus = InProcessBus::open(pool, config).await?;

        Ok(Self {
            bus,
            backend: MockBackend::new(),
            clock: VirtualClock::new(1_700_000_000_000_000),
            db_path,
            archive_root,
        })
    }

    pub async fn health(&self) -> anyhow::Result<bool> {
        Ok(self.bus.health().await?.ok)
    }

    /// Publish a signed event onto the bus (send_event).
    pub async fn send_event(&self, event: &nostr_sdk::prelude::Event) -> anyhow::Result<()> {
        self.bus.publish(event.clone()).await?;
        Ok(())
    }

    /// Open a subscription for a filter (BuzzTestClient shape).
    pub async fn subscribe(
        &self,
        filter: toron_core::bus::Filter,
    ) -> anyhow::Result<toron_core::bus::Subscription> {
        Ok(self.bus.subscribe(filter).await?)
    }

    /// Drain a subscription until EOSE (or `max` events), collecting events.
    pub async fn collect_until_eose(
        &self,
        mut sub: toron_core::bus::Subscription,
        max: usize,
    ) -> Vec<nostr_sdk::prelude::Event> {
        let mut events = Vec::new();
        for _ in 0..max {
            match sub.next().await {
                Some(SubscriptionEvent::Event(evt)) => events.push(evt),
                Some(SubscriptionEvent::Eose) | None => break,
                Some(SubscriptionEvent::Closed(_)) => break,
            }
        }
        events
    }

    pub fn cleanup(&self) {
        let _ = std::fs::remove_dir_all(self.db_path.parent().unwrap());
    }
}
