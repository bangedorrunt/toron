// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Mailbox core — the 38-tool AM-Rust parity surface mapped onto the Nostr
//! bus (ADR-0003 Decision 1A + the Business Logic parity contract).
//!
//! This is the **mailbox layer**: tools publish signed events to the [`Bus`]
//! and query the projection. It depends only on the bus + nostr/keys/archive
//! layers — swap `InProcessBus` ↔ `RelayBus` without tool changes.
//!
//! bd-led.5.1 ships the Infrastructure cluster (health_check, ensure_project,
//! install/uninstall_precommit_guard). Subsequent `.5.x` beads add the other
//! clusters in-place.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nostr_sdk::nostr::{Event, Keys};

use crate::bus::{Bus, BusError, Filter, SubscriptionEvent};

pub mod build_slots;
pub mod contacts;
pub mod guard;
pub mod identity;
pub mod infra;
pub mod invariants;
pub mod macros;
pub mod matrix;
pub mod messaging;
pub mod metrics;
pub mod product;
pub mod reservations;
pub mod search;
pub mod unwrap;

pub use build_slots::AcquireBuildSlotResult;
pub use contacts::{BusPolicySource, ContactLink, LinkStatus, RequestContact, RespondContact};
pub use guard::{
    Reservation as GuardReservation, guard_blocked_paths, install_precommit_guard,
    uninstall_precommit_guard,
};
pub use identity::{ListAgentsOptions, RegisteredAgent, WhoIs};
pub use infra::{HealthComponent, HealthReport, ProjectState};
pub use macros::{
    ContactHandshakeResult, PrepareThreadResult, ReservationCycleResult, StartSessionResult,
};
pub use messaging::{
    Delivery, DeliveryPayload, FetchInbox, InboxMessage, ReceiptResult, RecipientRole, SendMessage,
    SendResult,
};
pub use metrics::{BenchCase, BenchReport, BenchTimer, MailboxMetrics, MetricsSnapshot};
pub use product::ProductInfo;
pub use reservations::{
    AcquireResult, ConflictReport, GrantedReservation, PathConflict, ReleaseResult, RenewResult,
    Reservation, overlaps,
};
pub use search::{SearchMessage, SearchMessages, ThreadSummary};
pub use unwrap::{
    AckCache, AutoPolicy, ContactPolicy, DedupKey, FixedPolicy, GitProjectionSink, PolicySource,
    ProjectOutcome, ProjectionSink, SqliteProjectionSink, UnwrapPipeline, UnwrappedMail,
    order_by_rumor_time,
};

/// Mailbox context: the bus every tool publishes to + the on-disk project
/// root layout. The operator key signs privileged events (kind-30010 project
/// manifests, revocations); `None` means no operator configured (reads only).
#[derive(Clone)]
pub struct Mailbox {
    bus: Arc<dyn Bus>,
    data_dir: PathBuf,
    operator: Option<Keys>,
    agent_passphrase: Option<String>,
    receipt_cache: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    index_pool: Option<sqlx::SqlitePool>,
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox")
            .field("data_dir", &self.data_dir)
            .field("operator", &self.operator.is_some())
            .field("has_agent_passphrase", &self.agent_passphrase.is_some())
            .field("has_index_pool", &self.index_pool.is_some())
            .finish()
    }
}

impl Mailbox {
    /// Construct a mailbox over a bus + a data directory holding project
    /// roots (`{data_dir}/{slug}`).
    pub fn new(bus: Arc<dyn Bus>, data_dir: PathBuf, operator: Option<Keys>) -> Self {
        Self {
            bus,
            data_dir,
            operator,
            agent_passphrase: None,
            receipt_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            index_pool: None,
        }
    }

    /// Set the agent-key passphrase (G1: unlocks ncryptsec at process start).
    pub fn with_agent_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.agent_passphrase = Some(passphrase.into());
        self
    }

    /// Attach the SQLite index pool (contacts/agent links etc.). Without it,
    /// index-backed tools return a "no index pool" error.
    pub fn with_index_pool(mut self, pool: sqlx::SqlitePool) -> Self {
        self.index_pool = Some(pool);
        self
    }

    /// The SQLite index pool, if attached.
    pub fn index_pool(&self) -> Option<&sqlx::SqlitePool> {
        self.index_pool.as_ref()
    }

    /// The bus backing this mailbox.
    pub fn bus(&self) -> &Arc<dyn Bus> {
        &self.bus
    }

    /// The on-disk directory under which project roots live.
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// The operator key (if configured).
    pub fn operator(&self) -> Option<&Keys> {
        self.operator.as_ref()
    }
}

impl Mailbox {
    /// Spawn the ADR-0004 inbound unwrap loop as a background task: subscribes
    /// to gift-wraps on the bus and projects each through [`UnwrapPipeline`]
    /// (G14 contact-policy gate + operator Overseer bypass + idempotent
    /// sink write). Rebuilds the pipeline when the local agent-key set
    /// changes (agents register at runtime), so `block_all` enforcement and
    /// the overseer path are LIVE in the daemon, not just in tests.
    ///
    /// Without an index pool the loop degrades to a no-op (nothing to sink
    /// into); it logs once and exits. This is the production construction
    /// site the ADR-0004 primitives were missing (bd-pkc).
    pub fn spawn_unwrap_loop(&self) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.run_unwrap_loop().await {
                tracing::warn!("unwrap loop exited: {e}");
            }
        })
    }

    /// Drive the unwrap loop to completion (runs until the subscription
    /// closes). See [`Self::spawn_unwrap_loop`].
    async fn run_unwrap_loop(&self) -> anyhow::Result<()> {
        let mut sub = self
            .bus
            .subscribe(Filter::new().kinds([nostr_sdk::nostr::Kind::GiftWrap]))
            .await
            .map_err(|e| anyhow::anyhow!("bus subscribe: {e}"))?;
        let mut current: Option<(String, UnwrapPipeline)> = None;
        let mut seen = 0usize;
        while let Some(ev) = sub.next().await {
            let SubscriptionEvent::Event(event) = ev else { continue };
            seen += 1;
            if event.kind != nostr_sdk::nostr::Kind::GiftWrap {
                continue;
            }
            let sig = self.local_agent_signature();
            let rebuild = match &current {
                Some((s, _)) => s != &sig,
                None => true,
            };
            if rebuild {
                current = self.build_unwrap_pipeline().await.map(|p| (sig, p));
            }
            if let Some((_, pipeline)) = &current {
                let out = pipeline.project(&event).await;
                tracing::debug!(out = ?out, "unwrap loop projected {seen}");
            }
        }
        Ok(())
    }

    /// Signature of the current local agent-key set (sorted key-file paths).
    /// Cheap — lists the data dir, does NOT touch the KDF, so it can run per
    /// inbound wrap without stalling the daemon.
    fn local_agent_signature(&self) -> String {
        let mut paths: Vec<String> = self
            .local_agent_key_paths()
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        paths.sort();
        paths.join(",")
    }

    /// All local agent key files on disk (`{data_dir}/{project}/agents/{name}/key.ncryptsec`).
    /// Listing only — never decrypts. Callers load keys on demand (the argon2
    /// KDF is ~1s per key and must NOT run inside the async daemon loop).
    ///
    /// Bounded recursive walk: absolute project slugs (AM parity) nest to
    /// arbitrary depth under data_dir (`{data_dir}/tmp/proj/agents/...`), so
    /// a single-level scan would miss them and the projection would never
    /// run. Symlinks are not followed (the scan must not escape data_dir).
    fn local_agent_key_paths(&self) -> Vec<std::path::PathBuf> {
        const MAX_DEPTH: usize = 16;
        let mut out = Vec::new();
        let mut stack = vec![(self.data_dir.clone(), 0usize)];
        while let Some((dir, depth)) = stack.pop() {
            if depth >= MAX_DEPTH {
                continue;
            }
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let Ok(ft) = entry.file_type() else { continue };
                let path = entry.path();
                if ft.is_dir() {
                    stack.push((path, depth + 1));
                } else if ft.is_file()
                    && path.file_name().and_then(|n| n.to_str()) == Some("key.ncryptsec")
                    && path
                        .parent()
                        .and_then(|p| p.parent())
                        .and_then(|p| p.file_name())
                        .and_then(|n| n.to_str())
                        == Some("agents")
                {
                    out.push(path);
                }
            }
        }
        out
    }

    /// Build the unwrap pipeline over the current local agent keys. `None`
    /// when no index pool is attached (nothing to sink into — the loop
    /// skips projection until a pool is available). Called only when the
    /// agent-key signature changes, so the argon2 KDF (≈1s/key) runs at most
    /// once per agent registration — never per inbound wrap.
    async fn build_unwrap_pipeline(&self) -> Option<UnwrapPipeline> {
        let paths = self.local_agent_key_paths();
        if paths.is_empty() {
            return None;
        }
        let pool = self.index_pool.as_ref()?;
        let mut keys = Vec::with_capacity(paths.len());
        for p in &paths {
            if let Some(k) = self.load_agent_key(p) {
                keys.push(k);
            }
        }
        if keys.is_empty() {
            return None;
        }
        let npubs: Vec<nostr_sdk::nostr::PublicKey> =
            keys.iter().map(|k| k.public_key()).collect();
        let refs: Vec<&nostr_sdk::nostr::PublicKey> = npubs.iter().collect();
        let source = Arc::new(BusPolicySource::load_many(self, &refs).await);
        // Recipient-npub → verbatim project slug, from the registration-time
        // `agents` index (gift wraps hide the project; the sink cannot read
        // it off the wrap). Last-write-wins: one key per project in practice.
        let npub_to_project: std::collections::HashMap<String, String> = sqlx::query_as(
            "SELECT project_slug, pubkey FROM agents",
        )
        .fetch_all(pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|(slug, npub)| (npub, slug))
                .collect()
        })
        .unwrap_or_default();
        let sink: Arc<dyn ProjectionSink> = Arc::new(
            SqliteProjectionSink::new(pool.clone()).with_projects(npub_to_project),
        );
        let mut pipeline = UnwrapPipeline::new(keys, source, sink);
        if let Some(op) = &self.operator {
            pipeline = pipeline.with_operator(op.public_key());
        }
        Some(pipeline)
    }
}

/// Drain a bus subscription to EOSE, returning all replayed events.
///
/// A subscription replays stored history matching `filter`, emits an
/// EOSE-equivalent marker, then streams live events. Callers that want a
/// consistent point-in-time snapshot read up to EOSE and ignore the live tail
/// (or keep the subscription for push delivery).
pub async fn collect_replay(bus: &dyn Bus, filter: Filter) -> Result<Vec<Event>, BusError> {
    let mut sub = bus.subscribe(filter).await?;
    let mut events = Vec::new();
    while let Some(SubscriptionEvent::Event(e)) = sub.next().await {
        events.push(e);
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::nostr::identity::AgentProfile;

    async fn loop_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db = std::env::temp_dir().join(format!(
            "toron-unwrap-loop-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-unwrap-loop-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(crate::keys::generate()))
            .with_agent_passphrase("loop-test-pass")
            .with_index_pool(pool);
        (mailbox, db, data_dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwrap_loop_projects_inbound_gift_wrap() {
        let (mailbox, db, data_dir) = loop_mailbox("project").await;
        mailbox
            .register_agent("demo", &AgentProfile {
                name: "RoseDune".into(),
                program: Some("opencode".into()),
                model: None,
                persona: None,
                task_description: None,
            })
            .await
            .expect("register");
        let sender = Keys::generate();

        // Build a gift-wrap addressed to the recipient's npub using the same
        // key material the registered agent holds — load from disk like the
        // loop does.
        let path = mailbox.agent_key_path("demo", "RoseDune");
        let agent_keys = mailbox.load_agent_key(&path).expect("disk key");
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            nostr_sdk::nostr::Timestamp::now(),
            nostr_sdk::nostr::Kind::PrivateDirectMessage,
            vec![nostr_sdk::nostr::Tag::parse(["p", &agent_keys.public_key().to_hex()]).expect("p")],
            "hello via daemon loop",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &agent_keys.public_key(), rumor)
            .await
            .expect("wrap");
        assert_eq!(wrap.kind.as_u16(), 1059, "wrap must be kind-1059 gift wrap");
        let stored = mailbox.bus.publish(wrap).await.expect("publish");
        assert!(stored.stored, "wrap must be stored on the bus");

        // Spawn the daemon inbound loop and let it discover the agent key.
        let handle = mailbox.spawn_unwrap_loop();
        tokio::time::sleep(std::time::Duration::from_secs(8)).await;

        // The wrap was projected into the messages table via the loop's sink.
        let pool = mailbox.index_pool().unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(pool)
            .await
            .expect("count");
        assert_eq!(count, 1, "inbound wrap must be projected by the unwrap loop");

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwrap_loop_projects_inbound_gift_wrap_absolute_slug() {
        // MCP clients pass absolute project keys (AM parity). The agent key
        // MUST stay discoverable under the mailbox data_dir regardless of
        // slug depth (regression: PathBuf::join discarded the data_dir prefix
        // on absolute slugs, so the unwrap-loop key scan never found the key
        // and search never projected — live-parity failure 2026-08-08).
        let (mailbox, db, data_dir) = loop_mailbox("project").await;
        let slug = format!("/tmp/toron-abs-slug-{}", uuid::Uuid::new_v4());
        mailbox
            .register_agent(&slug, &AgentProfile {
                name: "RoseDune".into(),
                program: Some("opencode".into()),
                model: None,
                persona: None,
                task_description: None,
            })
            .await
            .expect("register");
        let sender = Keys::generate();

        let path = mailbox.agent_key_path(&slug, "RoseDune");
        let agent_keys = mailbox.load_agent_key(&path).expect("disk key");
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            nostr_sdk::nostr::Timestamp::now(),
            nostr_sdk::nostr::Kind::PrivateDirectMessage,
            vec![nostr_sdk::nostr::Tag::parse(["p", &agent_keys.public_key().to_hex()]).expect("p")],
            "hello via daemon loop (absolute slug)",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &agent_keys.public_key(), rumor)
            .await
            .expect("wrap");
        let stored = mailbox.bus.publish(wrap).await.expect("publish");
        assert!(stored.stored, "wrap must be stored on the bus");

        let handle = mailbox.spawn_unwrap_loop();
        tokio::time::sleep(std::time::Duration::from_secs(8)).await;

        let pool = mailbox.index_pool().unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(pool)
            .await
            .expect("count");
        assert_eq!(
            count, 1,
            "inbound wrap under an absolute project slug must be projected"
        );
        // The projected row must carry the VERBATIM registration slug — the
        // search project filter matches exactly what the client passes.
        let projected_slug: String =
            sqlx::query_scalar("SELECT project_slug FROM messages")
                .fetch_one(pool)
                .await
                .expect("project_slug");
        assert_eq!(
            projected_slug, slug,
            "project_slug must round-trip verbatim (leading separator preserved)"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }
}
