// governed-by: ADR-0003
// governed-by: ADR-0017 (project-root canonicalization)
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::nostr::{Event, Keys};

use crate::bus::{Bus, BusError, Filter, SubscriptionEvent};

pub mod aliases;
pub mod build_slots;
pub mod contacts;
pub mod error;
pub mod guard;
pub mod identity;
pub mod ids;
pub mod infra;
pub mod macros;
pub mod matrix;
pub mod messaging;
pub mod metrics;
pub mod naming;
pub mod product;
pub mod receipts;
pub mod reservations;
pub mod search;
pub mod send_tags;
pub mod surface;
pub mod unwrap;

pub use crate::nostr::identity::AgentProfile;
pub use build_slots::AcquireBuildSlotResult;
pub use contacts::{BusPolicySource, ContactLink, LinkStatus, RequestContact, RespondContact};
pub use guard::{
    Reservation as GuardReservation, guard_blocked_paths, guard_check_blocked,
    install_precommit_guard, staged_paths_in, uninstall_precommit_guard,
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
pub use receipts::{Receipt, ReceiptState, ReceiptStore};
pub use reservations::{
    AcquireResult, ConflictReport, GrantedReservation, IntentReservation, IntentResult,
    PathConflict, ReleaseAsk, ReleaseDecision, ReleaseResponse, ReleaseResult, RenewResult,
    RequestRelease, RequestReleaseResult, RequestReleaseStatus, Reservation, ReservationStatus,
    TransferResult, overlaps,
};
pub use search::{SearchMessage, SearchMessages, ThreadSummary};
pub use unwrap::{
    AckCache, AutoPolicy, ContactPolicy, DedupKey, DualProjectionSink, FixedPolicy,
    GitProjectionSink, PolicySource, ProjectGitProjectionSink, ProjectOutcome, ProjectionSink,
    SqliteProjectionSink, TriggerNotifySink, UnwrapPipeline, UnwrappedMail, order_by_rumor_time,
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
    /// NIP-17 wrap idempotency only (event-id keys). Not SLA truth —
    /// assignment receipts live in `receipts` sqlite (ADR-0027 E2).
    wrap_idempotency: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    index_pool: Option<sqlx::SqlitePool>,
    /// R2.0 (C2 / F2.KDF): decrypted agent keys cached by file CONTENT, so an
    /// unwrap-pipeline rebuild re-runs the argon2 KDF ONLY for key files that
    /// actually changed — never a full re-scan of every key (~1s/key at
    /// log-n 16). Keyed by content string (cheap — ncryptsec files are ~300B).
    ///
    /// Process-global (shared across ALL Mailbox instances): a fresh mailbox
    /// on the same data dir must NOT re-pay the N-key KDF — the bench loop and
    /// CLI-per-invocation mailbox builds reuse the unlocked keys of every
    /// earlier instance in the process.
    key_cache: Arc<parking_lot::Mutex<HashMap<PathBuf, CachedAgentKey>>>,

    /// Total argon2 KDF invocations this process has performed (test
    /// observable; the many-keys gate asserts the warm-rebuild bound).
    kdf_loads: Arc<AtomicU64>,
    /// ADR-0007 R3: optional side-channel for post-unwrap mail (workflow
    /// `message_posted` triggers). When set, [`TriggerNotifySink`] wraps the
    /// projection sink.
    trigger_tx: Option<tokio::sync::mpsc::UnboundedSender<UnwrappedMail>>,

    /// bd-py9o: memoized gift-wrap unwraps keyed by
    /// `"{recipient_hex}/{wrap_id_hex}"`. `fetch_inbox` previously ran the
    /// ECDH+decrypt per stored wrap on EVERY poll — the daemon burned a core
    /// re-decrypting the same history for fleet polling. Same key + same
    /// ciphertext = same outcome, so negative results cache too. Bounded by
    /// the store cap; entries are pure functions of (key, wrap).
    unwrap_cache:
        Arc<parking_lot::Mutex<HashMap<String, Option<crate::mailbox::unwrap::UnwrappedMail>>>>,
    /// Test/metrics observable: memo hits in [`Self::cached_unwrap`].
    unwrap_cache_hits: Arc<AtomicU64>,
    /// Test/metrics observable: cold decrypts in [`Self::cached_unwrap`].
    unwrap_cache_misses: Arc<AtomicU64>,
}

/// A decrypted agent key plus the exact ncryptsec content it was derived
/// from. Cache hits compare content strings — unchanged files never re-KDF.
struct CachedAgentKey {
    content: String,
    keys: Keys,
}

/// Process-global decrypted-key cache (see `Mailbox::key_cache`): one argon2
/// pass per unique (path, content) ever unlocked in this process, across all
/// mailbox instances. Prevents per-instance cold rebuilds (bench loops, CLI
/// per-invocation mailboxes) from re-paying N × ~1s KDF.
static AGENT_KEY_CACHE: OnceLock<Arc<parking_lot::Mutex<HashMap<PathBuf, CachedAgentKey>>>> =
    OnceLock::new();

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
            wrap_idempotency: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            index_pool: None,
            key_cache: AGENT_KEY_CACHE
                .get_or_init(|| Arc::new(parking_lot::Mutex::new(HashMap::new())))
                .clone(),
            kdf_loads: Arc::new(AtomicU64::new(0)),
            trigger_tx: None,
            unwrap_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            unwrap_cache_hits: Arc::new(AtomicU64::new(0)),
            unwrap_cache_misses: Arc::new(AtomicU64::new(0)),
        }
    }

    /// bd-py9o: decrypt a gift wrap through the per-(recipient, event) memo.
    /// Cold results (including decryption failures — same key and ciphertext
    /// cannot succeed on retry within a process) are cached; hits skip the
    /// secp256k1 ECDH + AES decrypt entirely. Counters are test observables
    /// for the incremental-fetch gate.
    pub(crate) fn cached_unwrap(
        &self,
        agent_keys: &crate::nostr::Keys,
        wrap: &crate::nostr::Event,
        recipient: crate::nostr::PublicKey,
    ) -> Option<crate::mailbox::unwrap::UnwrappedMail> {
        use crate::mailbox::unwrap::UnwrappedMail;
        let key = format!("{}/{}", recipient.to_hex(), wrap.id.to_hex());
        if let Some(cached) = self.unwrap_cache.lock().get(&key) {
            self.unwrap_cache_hits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return cached.clone();
        }
        self.unwrap_cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let built = match crate::nostr::gift_wrap::unwrap(agent_keys, wrap) {
            Ok(gift) => {
                let rumor = gift.rumor;
                Some(UnwrappedMail {
                    recipient,
                    sender: gift.sender,
                    rumor_id: rumor.clone().id(),
                    wrap_event_id: wrap.id,
                    created_at: rumor.created_at,
                    rumor,
                })
            }
            Err(_) => None,
        };
        // Bound the memo; clear wholesale on overflow (a memo, not truth).
        {
            const UNWRAP_CACHE_CAP: usize = 10_000;
            let mut cache = self.unwrap_cache.lock();
            if cache.len() >= UNWRAP_CACHE_CAP {
                cache.clear();
            }
            cache.insert(key, built.clone());
        }
        built
    }

    /// Memo hits since process start (test/metrics observable, bd-py9o).
    pub fn unwrap_cache_hits(&self) -> u64 {
        self.unwrap_cache_hits
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Cold decrypts since process start (test/metrics observable, bd-py9o).
    pub fn unwrap_cache_misses(&self) -> u64 {
        self.unwrap_cache_misses
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Attach a post-unwrap trigger notify channel (ADR-0007 R3).
    pub fn with_trigger_tx(
        mut self,
        tx: tokio::sync::mpsc::UnboundedSender<UnwrappedMail>,
    ) -> Self {
        self.trigger_tx = Some(tx);
        self
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

    /// ADR-0017: resolve any project key to its canonical short slug (the
    /// alias table in the archive DB maps path-derived keys to the canonical
    /// root; pure basename derivation when no pool is attached).
    pub async fn canonical_project(&self, slug: &str) -> String {
        aliases::canonicalize(self.index_pool.as_ref(), slug).await
    }

    /// ADR-0017: the read-side alias expansion of `slug` — the caller's own
    /// spelling, the canonical root, and every stored alias of that root.
    /// Queries over this set see rows written under any spelling (legacy
    /// path-slug rows included, aliased read-only).
    pub async fn project_read_set(&self, slug: &str) -> Vec<String> {
        aliases::alias_read_set(self.index_pool.as_ref(), slug).await
    }

    /// ADR-0017: fold pre-existing data-dir path-slug roots into the alias
    /// table (daemon open migration; best-effort no-op without a pool).
    pub async fn reconcile_project_aliases(&self) -> anyhow::Result<usize> {
        match self.index_pool.as_ref() {
            Some(pool) => aliases::reconcile_aliases(pool, &self.data_dir).await,
            None => Ok(0),
        }
    }

    /// The operator key (if configured).
    pub fn operator(&self) -> Option<&Keys> {
        self.operator.as_ref()
    }
    /// Load a local agent key with the content-keyed KDF cache (R2.0 C2):
    /// a rebuild only re-runs argon2 for key files whose CONTENT changed.
    /// Cache hits are pure memory — the ~1s/key KDF never re-runs for
    /// unchanged keys within this process.
    fn cached_load_agent_key(&self, path: &std::path::Path) -> Option<Keys> {
        let content = std::fs::read_to_string(path).ok()?;
        if let Some(cached) = self.key_cache.lock().get(path)
            && cached.content == content
        {
            return Some(cached.keys.clone());
        }
        let keys = crate::keys::decrypt(&content, self.agent_passphrase()).ok()?;
        self.kdf_loads.fetch_add(1, Ordering::Relaxed);
        self.key_cache.lock().insert(
            path.to_path_buf(),
            CachedAgentKey {
                content,
                keys: keys.clone(),
            },
        );
        Some(keys)
    }

    /// Total argon2 KDF invocations this process has performed (the R2.0
    /// many-keys gate asserts warm rebuilds stay O(new/changed keys)).
    pub fn kdf_load_count(&self) -> u64 {
        self.kdf_loads.load(Ordering::Relaxed)
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

    /// Minimum spacing between unwrap-pipeline build attempts (bd-gtqe).
    /// A build fans out into per-agent bus replays (`load_many` →
    /// `contact_policy_for` → `collect_replay` per roster npub); retrying it
    /// per inbound event starved the runtime and hung every tool call.
    const UNWRAP_REBUILD_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

    /// Drive the unwrap loop to completion (runs until the subscription
    /// closes). See [`Self::spawn_unwrap_loop`].
    async fn run_unwrap_loop(&self) -> anyhow::Result<()> {
        let mut policy = UnwrapRebuildPolicy::new(Self::UNWRAP_REBUILD_MIN_INTERVAL);
        let mut sub = self
            .bus
            .subscribe(Filter::new().kinds([crate::nostr::Kind::GiftWrap]))
            .await
            .map_err(|e| anyhow::anyhow!("bus subscribe: {e}"))?;
        let mut current: Option<(String, UnwrapPipeline)> = None;
        let mut seen = 0usize;
        while let Some(ev) = sub.next().await {
            let SubscriptionEvent::Event(event) = ev else {
                continue;
            };
            seen += 1;
            if event.kind != crate::nostr::Kind::GiftWrap {
                continue;
            }
            let sig = self.local_agent_signature();
            let rebuild = policy.should_rebuild(
                current.as_ref().map(|(s, _)| s.as_str()),
                &sig,
                std::time::Instant::now(),
            );
            if rebuild {
                // Record BEFORE the await: a build that fails or yields None
                // must still consume this event's rebuild budget (bd-gtqe —
                // per-event retry of a doomed build pinned a core at 100%).
                policy.record_attempt(sig.clone(), std::time::Instant::now());
                current = self.build_unwrap_pipeline().await.map(|p| (sig, p));
            }
            if let Some((_, pipeline)) = &current {
                // bd-verify-once-ph7t: this loop consumes ONLY bus-delivered
                // events (verified at publish ingress) — project_verified
                // skips the redundant re-verify (exactly one schnorr per
                // event end-to-end).
                let out = pipeline.project_verified(&event).await;
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

    /// Build the unwrap pipeline. **Lazy keyring** (bd-unwrap-ptag-lazy):
    /// the build performs ZERO key decryption — the pipeline resolves the
    /// wrap's `p`-tag recipient through the agents index at project time and
    /// decrypts exactly that key (content-keyed cache). `None` when no
    /// index pool is attached, or when there is neither a registered agent
    /// nor any local key file (nothing could ever unwrap).
    ///
    /// Policy source covers the REGISTERED roster (agents table) — the
    /// registration index is the source of truth; unregistered key files
    /// (fallback-scan recipients) resolve with the default policy.
    async fn build_unwrap_pipeline(&self) -> Option<UnwrapPipeline> {
        let pool = self.index_pool.as_ref()?;
        let paths = self.local_agent_key_paths();
        // Roster from the registration index — pubkeys WITHOUT decryption.
        let roster: Vec<(String, String, String)> =
            sqlx::query_as("SELECT project_slug, name, pubkey FROM agents")
                .fetch_all(pool)
                .await
                .unwrap_or_default();
        if roster.is_empty() && paths.is_empty() {
            return None;
        }
        // Recipient-npub → verbatim project slug + name (the wrap hides the
        // project; the sinks + resolver read it off this map).
        let npub_to_agent: std::collections::HashMap<String, (String, String)> = roster
            .iter()
            .map(|(slug, name, npub)| (npub.clone(), (slug.clone(), name.clone())))
            .collect();
        // Sender-npub → display name (AM-parity sender naming).
        let npub_to_name: std::collections::HashMap<String, String> = roster
            .iter()
            .map(|(_slug, name, npub)| (npub.clone(), name.clone()))
            .collect();
        let npub_to_project: std::collections::HashMap<String, String> = roster
            .iter()
            .map(|(slug, _name, npub)| (npub.clone(), slug.clone()))
            .collect();
        // Policy over the registered roster (pubkeys from the index — no KDF).
        // bd-gtqe: roster-direct bulk load — ONE kind-30014 replay per
        // distinct project, no per-npub profile scans.
        let source = Arc::new(BusPolicySource::load_roster(self, &roster).await);
        let sqlite: Arc<dyn ProjectionSink> = Arc::new(
            SqliteProjectionSink::new(pool.clone())
                .with_projects(npub_to_project.clone())
                .with_names(npub_to_name),
        );
        // G11 (bd-p3-projection-atomicity-rtjz): the production projection
        // writes BOTH recovery stores — the per-project Git archive (canonical
        // system of record; `doctor reconstruct` rebuilds from it) AND the
        // SQLite index. Git first: a git failure fails the whole write so the
        // archive never diverges from a "projected" claim; sqlite failure
        // after git success heals via re-delivery/rebuild.
        let git: Arc<dyn ProjectionSink> = Arc::new(ProjectGitProjectionSink::for_mailbox(
            self.data_dir.clone(),
            npub_to_project,
        ));
        let dual: Arc<dyn ProjectionSink> = Arc::new(DualProjectionSink::new(git, sqlite));
        // ADR-0007 R3: wrap with TriggerNotifySink when the host subscribed
        // a post-unwrap channel (message_posted workflows).
        let sink: Arc<dyn ProjectionSink> = match &self.trigger_tx {
            Some(tx) => Arc::new(TriggerNotifySink::new(dual, tx.clone())),
            None => dual,
        };
        let resolver = Arc::new(MailboxKeyResolver {
            mailbox: self.clone(),
            npub_to_agent,
        });
        let mut pipeline = UnwrapPipeline::new(Vec::new(), source, sink).with_resolver(resolver);
        if let Some(op) = &self.operator {
            pipeline = pipeline.with_operator(op.public_key());
        }
        Some(pipeline)
    }
}

/// bd-unwrap-ptag-lazy: DB-directed recipient key resolution. The agents
/// index maps recipient npub → (project, name) at registration time;
/// `resolve` decrypts exactly that agent's key file through the
/// content-keyed cache. On a DB miss (raw unregistered key files — the
/// pre-registration state some tests rely on) it falls back to a cached
/// scan of local key files, the pre-lazy semantics, paid only for npubs
/// the index does not know.
struct MailboxKeyResolver {
    mailbox: Mailbox,
    npub_to_agent: std::collections::HashMap<String, (String, String)>,
}

impl unwrap::RecipientKeyResolver for MailboxKeyResolver {
    fn resolve(&self, recipient: &crate::nostr::PublicKey) -> Option<Keys> {
        let hex = recipient.to_hex();
        if let Some((project, name)) = self.npub_to_agent.get(&hex) {
            let path = self.mailbox.agent_key_path(project, name);
            return self.mailbox.cached_load_agent_key(&path);
        }
        for p in self.mailbox.local_agent_key_paths() {
            if let Some(k) = self.mailbox.cached_load_agent_key(&p)
                && k.public_key() == *recipient
            {
                return Some(k);
            }
        }
        None
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

/// Rebuild gate for the inbound unwrap pipeline (bd-gtqe).
///
/// A pipeline build is expensive: it queries the agents roster and fans out
/// into a bus replay per registered npub (`load_many` → `contact_policy_for`
/// → `collect_replay`). Re-attempting it on every inbound event — because the
/// previous attempt failed, or because key files under `data_dir` flap the
/// signature — pinned a daemon core at 100% CPU and starved every other tool
/// handler (the bd-gtqe silent-hang family). The policy: rebuild when the
/// live pipeline's signature no longer matches, but never more often than
/// `min_interval` regardless of how events arrive.
struct UnwrapRebuildPolicy {
    last_attempt_at: Option<std::time::Instant>,
    min_interval: std::time::Duration,
}

impl UnwrapRebuildPolicy {
    fn new(min_interval: std::time::Duration) -> Self {
        Self {
            last_attempt_at: None,
            min_interval,
        }
    }

    /// Decide whether a pipeline build may start now.
    ///
    /// `current_sig` is the signature of the successfully-built pipeline
    /// (`None` until one succeeds); `sig` is the current key-set signature.
    fn should_rebuild(
        &self,
        current_sig: Option<&str>,
        sig: &str,
        now: std::time::Instant,
    ) -> bool {
        if current_sig == Some(sig) {
            return false;
        }
        match self.last_attempt_at {
            Some(t) => now.duration_since(t) >= self.min_interval,
            None => true,
        }
    }

    /// Record an attempt BEFORE awaiting the build so failures consume this
    /// event's rebuild budget too.
    fn record_attempt(&mut self, _sig: String, now: std::time::Instant) {
        self.last_attempt_at = Some(now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::nostr::identity::AgentProfile;

    // ── bd-gtqe: unwrap-loop rebuild policy ─────────────────────────────
    // The daemon previously rebuilt the unwrap pipeline on EVERY inbound
    // event whenever a build could not succeed (and re-built at full speed
    // under flapping key-set signatures), pinning a worker core at 100% CPU
    // and starving every other tool handler — the bd-gtqe silent-hang root.
    use std::time::Duration;

    #[test]
    fn rebuild_policy_first_event_builds() {
        let policy = UnwrapRebuildPolicy::new(Duration::from_secs(2));
        let now = std::time::Instant::now();
        assert!(policy.should_rebuild(None, "sig-a", now));
    }

    #[test]
    fn rebuild_policy_failed_build_not_retried_per_event() {
        // THE regression: build failed (current stays None) → identical sig
        // must NOT trigger another attempt per inbound event.
        let mut policy = UnwrapRebuildPolicy::new(Duration::from_secs(2));
        let now = std::time::Instant::now();
        assert!(policy.should_rebuild(None, "sig-a", now));
        policy.record_attempt("sig-a".into(), now);
        assert!(
            !policy.should_rebuild(None, "sig-a", now + Duration::from_millis(100)),
            "identical failed sig must be rate-limited, not retried per event"
        );
    }

    #[test]
    fn rebuild_policy_rate_limits_changed_sig_within_interval() {
        let mut policy = UnwrapRebuildPolicy::new(Duration::from_secs(2));
        let now = std::time::Instant::now();
        policy.record_attempt("sig-a".into(), now);
        assert!(
            !policy.should_rebuild(None, "sig-b", now + Duration::from_millis(1999)),
            "inside the interval even a changed sig waits (flap guard)"
        );
        assert!(
            policy.should_rebuild(None, "sig-b", now + Duration::from_secs(2)),
            "after the interval a new sig builds"
        );
    }

    #[test]
    fn rebuild_policy_live_pipeline_matching_sig_skips() {
        let mut policy = UnwrapRebuildPolicy::new(Duration::from_secs(2));
        let now = std::time::Instant::now();
        policy.record_attempt("sig-a".into(), now);
        assert!(
            !policy.should_rebuild(Some("sig-a"), "sig-a", now + Duration::from_secs(60)),
            "live pipeline already matches — no rebuild"
        );
        assert!(
            policy.should_rebuild(Some("sig-old"), "sig-new", now + Duration::from_secs(60)),
            "changed key set after interval — rebuild"
        );
    }

    /// Recursively collect `*.json` files under `root` (the Git archive's
    /// `messages/{YYYY}/{MM}/` tree) — the G11 path-1 rebuild source.
    fn collect_message_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(years) = std::fs::read_dir(root) else {
            return out;
        };
        for year in years.flatten() {
            if !year.path().is_dir() {
                continue;
            }
            let Ok(months) = std::fs::read_dir(year.path()) else {
                continue;
            };
            for month in months.flatten() {
                if !month.path().is_dir() {
                    continue;
                }
                let Ok(entries) = std::fs::read_dir(month.path()) else {
                    continue;
                };
                for e in entries.flatten() {
                    if e.path().extension().map(|x| x == "json") == Some(true) {
                        out.push(e.path());
                    }
                }
            }
        }
        out
    }

    /// Deterministic projection wait: poll the probe until it passes or the
    /// 90s deadline elapses. The unwrap loop's argon2 (log_n=16) key scan is
    /// CPU/memory-hard and can exceed ANY fixed sleep under parallel test
    /// load (bd-projection-flake-gyox) — never fixed-sleep and hope. The
    /// `messages` count is the completion signal: the sink chain writes the
    /// Git archive BEFORE the SQLite row (DualProjectionSink, unwrap.rs), so
    /// a visible row implies the canonical archive write already landed.
    async fn wait_projected<F, Fut>(mut probe: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = std::time::Duration::from_secs(90);
        let start = std::time::Instant::now();
        loop {
            if probe().await {
                return true;
            }
            if start.elapsed() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }

    async fn loop_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db = std::env::temp_dir().join(format!(
            "toron-unwrap-loop-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-unwrap-loop-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_agent_passphrase("loop-test-pass")
        .with_index_pool(pool);
        (mailbox, db, data_dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwrap_loop_projects_inbound_gift_wrap() {
        let (mailbox, db, data_dir) = loop_mailbox("project").await;
        mailbox
            .register_agent(
                "demo",
                &AgentProfile {
                    name: "RoseDune".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
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
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![crate::nostr::Tag::parse(["p", &agent_keys.public_key().to_hex()]).expect("p")],
            "hello via daemon loop",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &agent_keys.public_key(), rumor)
            .expect("wrap");
        assert_eq!(wrap.kind.as_u16(), 1059, "wrap must be kind-1059 gift wrap");
        let stored = mailbox.bus.publish(wrap).await.expect("publish");
        assert!(stored.stored, "wrap must be stored on the bus");

        // Spawn the daemon inbound loop and let it discover the agent key.
        let handle = mailbox.spawn_unwrap_loop();
        // No fixed sleep: the loop's argon2 key scan is load-sensitive
        // (bd-projection-flake-gyox). Poll to completion instead.
        let pool = mailbox.index_pool().unwrap();
        let projected = wait_projected(|| async {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(pool)
                .await
                .expect("count");
            count == 1
        })
        .await;
        assert!(
            projected,
            "inbound wrap must be projected by the unwrap loop (90s poll deadline)"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// bd-unwrap-ptag-lazy: a wrap for a REGISTERED agent decrypts exactly
    /// ONE key — the wrap's p-tag names the recipient npub, the resolver
    /// decrypts just that key, and the pipeline build itself performs ZERO
    /// KDF (no eager keyring). RED against the eager-scan design (which
    /// decrypts every local key at build time).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ptag_directed_unwrap_decrypts_only_the_recipient_key() {
        let (mailbox, db, data_dir) = loop_mailbox("ptag-lazy").await;
        let reg_a = mailbox
            .register_agent(
                "proj",
                &AgentProfile {
                    name: "AlphaOne".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register a");
        let reg_b = mailbox
            .register_agent(
                "proj",
                &AgentProfile {
                    name: "BetaTwo".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register b");

        // Lazy contract: the pipeline BUILD performs zero KDF.
        let pipeline = mailbox.build_unwrap_pipeline().await.expect("pipeline");
        assert_eq!(
            mailbox.kdf_load_count(),
            0,
            "lazy keyring: build decrypts nothing"
        );

        // Wrap addressed to BetaTwo's npub: exactly ONE key decrypts.
        let recipient_npub: crate::nostr::PublicKey =
            crate::nostr::PublicKey::parse(&reg_b.npub).expect("beta npub");
        let sender = Keys::generate();
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![crate::nostr::Tag::parse(["p", &reg_b.npub]).expect("p")],
            "ptag-directed hello",
        );
        let wrap =
            crate::nostr::gift_wrap::gift_wrap(&sender, &recipient_npub, rumor).expect("wrap");
        let out = pipeline.project(&wrap).await;
        assert_eq!(out, ProjectOutcome::Projected, "wrap projects");
        assert_eq!(
            mailbox.kdf_load_count(),
            1,
            "exactly the p-tag recipient key decrypted (not {}/2 eager)",
            mailbox.kdf_load_count()
        );

        // A second wrap to AlphaOne adds exactly one more decrypt.
        let rumor2 = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![crate::nostr::Tag::parse(["p", &reg_a.npub]).expect("p")],
            "ptag-directed hello two",
        );
        let wrap2 = crate::nostr::gift_wrap::gift_wrap(
            &sender,
            &crate::nostr::PublicKey::parse(&reg_a.npub).expect("alpha npub"),
            rumor2,
        )
        .expect("wrap2");
        let out2 = pipeline.project(&wrap2).await;
        assert_eq!(out2, ProjectOutcome::Projected);
        assert_eq!(mailbox.kdf_load_count(), 2, "one decrypt per recipient");

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// G11 / bd-p3-projection-atomicity-rtjz: the PRODUCTION unwrap loop must
    /// land mail in BOTH recovery stores — the SQLite index AND the per-project
    /// Git archive (`{data_dir}/{slug}/messages/{YYYY}/{MM}/{rumor_id}.json`).
    /// ADR-0003's "either alone rebuilds" posture requires the archive to hold
    /// the unwrapped canonical mail; a loop that writes only SQLite makes
    /// `rebuild_index_from_git` a no-op (and `bus_events` shares the same
    /// index.sqlite file, so losing that file loses both). RED today: the
    /// production sink chain is SQLite-only (mailbox/mod.rs build_unwrap_pipeline).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwrap_loop_projects_mail_to_both_sqlite_and_git_archive() {
        let (mailbox, db, data_dir) = loop_mailbox("both-stores").await;
        mailbox
            .register_agent(
                "demo",
                &AgentProfile {
                    name: "RoseDune".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register");
        let sender = Keys::generate();

        let path = mailbox.agent_key_path("demo", "RoseDune");
        let agent_keys = mailbox.load_agent_key(&path).expect("disk key");
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![crate::nostr::Tag::parse(["p", &agent_keys.public_key().to_hex()]).expect("p")],
            "mail must reach the git archive too",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &agent_keys.public_key(), rumor)
            .expect("wrap");
        mailbox.bus.publish(wrap).await.expect("publish");

        let handle = mailbox.spawn_unwrap_loop();
        // Poll to completion — the argon2 key scan is load-sensitive
        // (bd-projection-flake-gyox); the Git leg writes before the SQLite
        // leg in the dual sink, so a visible row implies archive completion.
        let pool = mailbox.index_pool().unwrap();
        let projected = wait_projected(|| async {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(pool)
                .await
                .expect("count");
            count == 1
        })
        .await;
        assert!(
            projected,
            "mail must be projected into the SQLite index (90s poll deadline)"
        );

        // 2. The per-project Git archive holds the canonical unwrapped mail:
        //    {data_dir}/demo/messages/{YYYY}/{MM}/{rumor_id}.json. This is the
        //    G11 path-1 rebuild source — without it, "either alone rebuilds"
        //    is false and SQLite loss = total mail loss.
        let archive_root = data_dir.join("demo");
        let messages_root = archive_root.join("messages");
        let json_files = collect_message_files(&messages_root);
        assert_eq!(
            json_files.len(),
            1,
            "unwrap loop must write the canonical mail into the Git archive \
             (found {} message files under {})",
            json_files.len(),
            messages_root.display()
        );

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
            .register_agent(
                &slug,
                &AgentProfile {
                    name: "RoseDune".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register");
        let sender = Keys::generate();

        let path = mailbox.agent_key_path(&slug, "RoseDune");
        let agent_keys = mailbox.load_agent_key(&path).expect("disk key");
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![crate::nostr::Tag::parse(["p", &agent_keys.public_key().to_hex()]).expect("p")],
            "hello via daemon loop (absolute slug)",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &agent_keys.public_key(), rumor)
            .expect("wrap");
        let stored = mailbox.bus.publish(wrap).await.expect("publish");
        assert!(stored.stored, "wrap must be stored on the bus");

        let handle = mailbox.spawn_unwrap_loop();
        // Poll to completion — load-sensitive argon2 scan
        // (bd-projection-flake-gyox), no fixed sleep.
        let pool = mailbox.index_pool().unwrap();
        let projected = wait_projected(|| async {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
                .fetch_one(pool)
                .await
                .expect("count");
            count == 1
        })
        .await;
        assert!(
            projected,
            "inbound wrap under an absolute project slug must be projected \
             (90s poll deadline)"
        );
        // ADR-0017: the projected row carries the CANONICAL slug — the
        // absolute path form resolves to its basename at registration, so
        // search (which canonicalizes the client's key the same way) matches
        // exactly what the client passes.
        let projected_slug: String = sqlx::query_scalar("SELECT project_slug FROM messages")
            .fetch_one(pool)
            .await
            .expect("project_slug");
        let canonical = std::path::Path::new(&slug)
            .file_name()
            .and_then(|f| f.to_str())
            .expect("basename");
        assert_eq!(
            projected_slug, canonical,
            "project_slug is the canonical short slug (ADR-0017)"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// S10 (workspace hygiene): a mailbox booted with a cwd-derived root —
    /// the gate-test pattern that polluted ~/workspace with ~40 scratch dirs
    /// (`.archive.lock` + `messages/` + `.git` trees at
    /// `/Users/.../workspace/s66667` etc.) — must NEVER write outside its
    /// data_dir. An absolute project key like `/tmp/workspace-sim/s66667`
    /// resolves to the canonical short slug under data_dir; the "workspace"
    /// dir stays untouched. Snapshot-before/after is the no-new-workspace-
    /// dirs gate for `cargo test` runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gate_no_new_dirs_outside_data_dir_for_absolute_slug() {
        let (mailbox, db, data_dir) = loop_mailbox("s10-gate").await;
        // A fake "workspace root" the mailbox must never touch: it stands in
        // for ~/workspace under the old cwd-derived-root bug.
        let fake_ws = std::env::temp_dir().join(format!("toron-s10-ws-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&fake_ws).expect("fake ws");
        let snapshot: Vec<String> = std::fs::read_dir(&fake_ws)
            .expect("read ws")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();

        let slug = format!("{}/s66667", fake_ws.display());
        mailbox
            .register_agent(
                &slug,
                &AgentProfile {
                    name: "RoseDune".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register");
        mailbox
            .send_message(&crate::mailbox::SendMessage {
                project_slug: slug.clone(),
                sender_name: "RoseDune".into(),
                to: vec!["RoseDune".into()],
                subject: "s10".into(),
                body_md: "workspace must stay clean".into(),
                ..Default::default()
            })
            .await
            .expect("send");

        let after: Vec<String> = std::fs::read_dir(&fake_ws)
            .expect("read ws")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            after, snapshot,
            "no new dirs may appear under the cwd-derived root \
             (mailbox escaped data_dir): {after:?}"
        );

        // The mailbox root itself lives under data_dir under the canonical
        // short slug — NOT at the absolute key path.
        let canonical = std::path::Path::new(&slug)
            .file_name()
            .and_then(|f| f.to_str())
            .expect("basename");
        assert!(
            data_dir.join(canonical).is_dir(),
            "canonical root under data_dir: {}",
            data_dir.join(canonical).display()
        );

        let _ = std::fs::remove_dir_all(&fake_ws);
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// bd-unwrap-ptag-lazy cost model: pipeline builds are KDF-FREE (the
    /// lazy keyring never bulk-decrypts), a wrap to an UNREGISTERED key file
    /// (DB-miss fallback scan) still projects end-to-end, and a repeat wrap
    /// is a pure cache hit. The pre-lazy eager-bound assertions (cold=50,
    /// warm=+1) described the deleted design.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unwrap_loop_kdf_cached_between_rebuilds_many_keys() {
        use crate::nostr::gift_wrap::{build_rumor, gift_wrap};
        use crate::nostr::{Kind, Tag, Timestamp};

        let (mailbox, db, data_dir) = loop_mailbox("kdf-bound").await;
        let pass = "loop-test-pass";
        let agent_dir = data_dir.join("proj").join("agents");

        // Fixture: 50 UNREGISTERED agent keys on disk (TEST_LOG_N — ~ms
        // each, CI-safe). None of them sit in the agents index, so wraps to
        // them exercise the fallback scan.
        for i in 0..50usize {
            let p = agent_dir.join(format!("agent-{i}")).join("key.ncryptsec");
            crate::keys::write_encrypted_key(
                &p,
                &crate::keys::generate(),
                pass,
                crate::keys::TEST_LOG_N,
            )
            .expect("write key");
        }
        let target = crate::keys::generate();
        let p50 = agent_dir.join("agent-50").join("key.ncryptsec");
        crate::keys::write_encrypted_key(&p50, &target, pass, crate::keys::TEST_LOG_N)
            .expect("write target key");

        // Every build is KDF-free — cold, after a new key, after a rewrite,
        // and no-op. (The eager keyring is deleted; nothing decrypts at
        // build time.)
        let pipeline = mailbox.build_unwrap_pipeline().await.expect("cold build");
        assert_eq!(mailbox.kdf_load_count(), 0, "cold build is KDF-free");
        let other = crate::keys::generate();
        crate::keys::write_encrypted_key(
            &agent_dir.join("agent-51").join("key.ncryptsec"),
            &other,
            pass,
            crate::keys::TEST_LOG_N,
        )
        .expect("add key");
        let _ = mailbox.build_unwrap_pipeline().await.expect("rebuild");
        assert_eq!(
            mailbox.kdf_load_count(),
            0,
            "rebuild after a new key file is still KDF-free"
        );

        // A wrap to the unregistered target projects via the fallback scan
        // (bounded by the local key-file count) — the pre-lazy contract for
        // on-disk-but-unregistered keys is preserved.
        let sender = crate::keys::generate();
        let rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000u64),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &target.public_key().to_hex()]).expect("p")],
            "hello",
        );
        let wrap = gift_wrap(&sender, &target.public_key(), rumor).expect("wrap");
        let outcome = pipeline.project(&wrap).await;
        assert_eq!(
            outcome,
            ProjectOutcome::Projected,
            "unregistered on-disk key still unwraps (fallback scan)"
        );
        let after_first = mailbox.kdf_load_count();
        assert!(
            (1..=51).contains(&after_first),
            "fallback scan decrypts at most the local key files ({after_first})"
        );

        // Repeat wrap to the same recipient: pure cache hit.
        let rumor2 = build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_001u64),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &target.public_key().to_hex()]).expect("p")],
            "hello again",
        );
        let wrap2 = gift_wrap(&sender, &target.public_key(), rumor2).expect("wrap2");
        let outcome2 = pipeline.project(&wrap2).await;
        assert_eq!(
            outcome2,
            ProjectOutcome::Projected,
            "a NEW rumor to the same recipient projects (not a duplicate)"
        );
        assert_eq!(
            mailbox.kdf_load_count(),
            after_first,
            "repeat recipient is a cache hit — zero additional KDF"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }
}
