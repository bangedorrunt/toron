// governed-by: ADR-0003
// governed-by: ADR-0004 (OVERSEER_TAG, is_overseer_bypass, with_operator — Ship 5)
#![forbid(unsafe_code)]

//! Unwrap pipeline + mail-timeline ordering (G7) + contact-policy drop (G14).
//!
//! The projection subscribes to the bus, unwraps kind-1059 gift wraps with the
//! local agent keys, and projects the rumor. **Ordering landmine (G7):** NIP-17
//! randomizes the seal AND wrap `created_at` (up to 2 days back), so the
//! envelope timestamp is meaningless for the mail timeline. Mail must be
//! re-sorted by the **rumor's** `created_at` (+ optional `ts_ms` µs tag) + a
//! monotonic `local_seq` — never the envelope `created_at`.
//!
//! **Idempotency (beads-review):** a re-delivered kind-1059 (bus redelivery,
//! federation replay, duplicate wrap) MUST NOT duplicate a Git commit or a
//! SQLite row. Dedup key = `(recipient-npub, rumor-id)`; the rumor id is the
//! canonical `UnsignedEvent` id (content-addressed). The pipeline also keeps a
//! per-wrap event-id guard so the same wrap arriving twice is a no-op.
//!
//! **Contact policy (G14):** the bus is dumb transport. Incoming gift wraps
//! from keys violating the recipient's policy are dropped BEFORE any index /
//! Git write. The policy source is injected (default `Auto`); the kind-30014
//! bus-backed resolver lands with the contacts tools (bd-led.5.4).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;

use nostr_sdk::nostr::event::unsigned::UnsignedEvent;
use nostr_sdk::nostr::{Event, EventId, JsonUtil, Keys, PublicKey, Timestamp};

use crate::bus::scoping::is_metadata_hiding_kind;
use crate::nostr::gift_wrap::{self};

/// Contact policy of a recipient agent (ADR-0003 G14; AM-Rust parity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ContactPolicy {
    /// All inbound mail accepted.
    Open,
    /// Default: accepted, no explicit approval required.
    #[default]
    Auto,
    /// Only accepted when a contact link (AgentLink) exists.
    ContactsOnly,
    /// All inbound mail from this key dropped.
    BlockAll,
}

impl ContactPolicy {
    /// Parse from a kind-30014 policy string (or config).
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "open" => Self::Open,
            "contacts_only" => Self::ContactsOnly,
            "block_all" => Self::BlockAll,
            _ => Self::Auto,
        }
    }

    /// The canonical string form.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Auto => "auto",
            Self::ContactsOnly => "contacts_only",
            Self::BlockAll => "block_all",
        }
    }

    /// Decision for an incoming sender: is this mail accepted?
    ///
    /// `is_approved_contact` is true when an AgentLink exists (used by
    /// `ContactsOnly`). `Open` and `Auto` accept; `BlockAll` always drops.
    pub fn allows(self, is_approved_contact: bool) -> bool {
        match self {
            Self::Open | Self::Auto => true,
            Self::ContactsOnly => is_approved_contact,
            Self::BlockAll => false,
        }
    }
}

/// A projected mail unit: the unwrapped rumor + its G7 ordering key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwrappedMail {
    /// Local recipient key that successfully unwrapped the wrap.
    pub recipient: PublicKey,
    /// Sender pubkey (seal signer == rumor author, NIP-59 enforced).
    pub sender: PublicKey,
    /// The unwrapped rumor (kind 14/15/ack…).
    pub rumor: UnsignedEvent,
    /// Canonical content-addressed id of the rumor (thread root, G7).
    pub rumor_id: EventId,
    /// The kind-1059 wrap event id (idempotency guard).
    pub wrap_event_id: EventId,
    /// Rumor `created_at` (unix seconds — the mail timeline anchor).
    pub created_at: Timestamp,
}

impl UnwrappedMail {
    /// The G7 ordering timestamp in i64-µs:
    /// rumor `created_at*1e6`, overridden by an optional `["ts_ms", µs]` tag
    /// when the sender provides microsecond precision.
    pub fn ts_us(&self) -> i64 {
        if let Some(ms) = ts_ms_tag(&self.rumor) {
            ms
        } else {
            self.created_at.as_secs() as i64 * 1_000_000
        }
    }
}

/// The `t`-tag value marking an operator Overseer override message (ADR-0004
/// Ship 5). Rides on kind-14 as a topic tag `['t', 'toron:overseer']` —
/// consistent with the `toron:workflow-emitted` marker (Ship 2).
pub const OVERSEER_TAG: &str = "toron:overseer";

/// Operator-bypass (Overseer) predicate (ADR-0004 Ship 5, G14 carve-out).
///
/// Returns `true` iff ALL hold (key-scoped, not name-scoped — strictly
/// stronger than AM-Rust's spoofable `HumanOverseer`):
/// 1. an operator key is configured (`operator.is_some()`),
/// 2. the mail's seal signer (`mail.sender`) IS that operator key,
/// 3. the rumor carries a `['t', OVERSEER_TAG]` tag (intentional override),
/// 4. the rumor carries an `importance` tag of `high` or `urgent`.
///
/// When true, the projection SKIPS contact-policy enforcement (a `block_all`
/// recipient still receives + indexes + archives the operator's override).
/// Pure + sync + O(tags).
pub fn is_overseer_bypass(mail: &UnwrappedMail, operator: Option<&PublicKey>) -> bool {
    let Some(op) = operator else {
        return false;
    };
    if &mail.sender != op {
        return false;
    }
    let tags = &mail.rumor.tags;
    let has_overseer = tags.iter().any(|t| {
        let p = t.as_slice();
        p.first().map(String::as_str) == Some("t")
            && p.get(1).map(String::as_str) == Some(OVERSEER_TAG)
    });
    if !has_overseer {
        return false;
    }
    tags.iter().any(|t| {
        let p = t.as_slice();
        p.first().map(String::as_str) == Some("importance")
            && matches!(p.get(1).map(String::as_str), Some("high") | Some("urgent"))
    })
}

/// Read the optional `["ts_ms", <µs>]` tag (µs precision, G7).
fn ts_ms_tag(rumor: &UnsignedEvent) -> Option<i64> {
    rumor.tags.iter().find_map(|t| {
        let parts = t.as_slice();
        if parts.first().map(|s| s.as_str()) == Some("ts_ms") && parts.len() >= 2 {
            parts[1].parse().ok()
        } else {
            None
        }
    })
}

/// Sort unwrapped mail by rumor time (G7), stable, never envelope time.
///
/// Assigns a monotonic `local_seq` by arrival order, then orders by
/// `(ts_us, local_seq)` — same-µs ties resolve to arrival order, so timelines
/// are reproducible and deterministic regardless of relay/wrap timestamp noise.
pub fn order_by_rumor_time(mut mails: Vec<UnwrappedMail>) -> Vec<(UnwrappedMail, u64)> {
    let mut seqd: Vec<(UnwrappedMail, u64)> = mails
        .drain(..)
        .enumerate()
        .map(|(i, m)| (m, i as u64 + 1))
        .collect();
    seqd.sort_by_key(|(m, seq)| (m.ts_us(), *seq));
    seqd
}

/// Idempotency key: `(recipient-npub, rumor-id)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey {
    pub recipient: String,
    pub rumor_id: String,
}

impl DedupKey {
    pub fn of(recipient: &PublicKey, rumor_id: &EventId) -> Self {
        Self {
            recipient: recipient.to_hex(),
            rumor_id: rumor_id.to_hex(),
        }
    }
}

/// Outcome of projecting one wrap event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectOutcome {
    /// Not a gift wrap, or no local key could unwrap it.
    NotForUs,
    /// Already projected (idempotent duplicate — no write).
    Duplicate,
    /// Dropped by the recipient's contact policy BEFORE any write.
    Dropped(ContactPolicy),
    /// NIP-40: the wrap's `expiration` tag is in the past — rejected before
    /// any write (ADR-0003 rev6 replay/expiry parity, bd-p3-nip40-expiration-fix).
    Expired,
    /// Projected + written to the archive/index.
    Projected,
}

/// Where the pipeline writes projected mail. Kept as a trait so tests can
/// assert "dropped before write" against a real archive without a live index.
#[async_trait::async_trait]
pub trait ProjectionSink: Send + Sync {
    /// True when the message is already projected (idempotency check).
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool>;
    /// Persist one projected message. Must be idempotent on `key`.
    async fn write(&self, mail: &UnwrappedMail, local_seq: u64) -> anyhow::Result<bool>;
    /// NIP-09 (F19.NIP09, batch-fix): tombstone the projected message with
    /// `event_id == target` (author-signed kind-5; `author` = the kind-5
    /// signer's hex pubkey — enforced so only the original sender can
    /// tombstone). Default: no-op — sinks that cannot tombstone
    /// (append-only) leave the row readable; the SQLite index sink
    /// implements the real tombstone.
    async fn tombstone(&self, target: &str, author: &str, ts_us: i64) -> anyhow::Result<()> {
        let _ = (target, author, ts_us);
        Ok(())
    }
}

/// ADR-0007 R3 / C6: fan out projected mail to a workflow trigger channel
/// while still writing through an inner sink (Git/SQLite).
///
/// This is the narrow TriggerIngress plane — post-unwrap, not raw gift wraps.
/// The host consumes [`UnwrappedMail`] and evaluates `message_posted` without
/// putting plaintext kind-14 on the bus.
pub struct TriggerNotifySink {
    inner: Arc<dyn ProjectionSink>,
    tx: tokio::sync::mpsc::UnboundedSender<UnwrappedMail>,
}

impl TriggerNotifySink {
    /// Wrap an existing sink; every successful `write` also notifies `tx`.
    pub fn new(
        inner: Arc<dyn ProjectionSink>,
        tx: tokio::sync::mpsc::UnboundedSender<UnwrappedMail>,
    ) -> Self {
        Self { inner, tx }
    }
}

#[async_trait::async_trait]
impl ProjectionSink for TriggerNotifySink {
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool> {
        self.inner.is_projected(key).await
    }

    async fn write(&self, mail: &UnwrappedMail, local_seq: u64) -> anyhow::Result<bool> {
        let written = self.inner.write(mail, local_seq).await?;
        if written {
            // Best-effort notify; a dropped receiver must not fail projection.
            let _ = self.tx.send(mail.clone());
        }
        Ok(written)
    }

    async fn tombstone(&self, target: &str, author: &str, ts_us: i64) -> anyhow::Result<()> {
        self.inner.tombstone(target, author, ts_us).await
    }
}

/// Contact-policy source for the pipeline (G14). Injected so the projection
/// stays decoupled from the contacts store; the kind-30014-backed impl lands
/// with bd-led.5.4. (Sync — the projection gate is a non-async decision.)
pub trait PolicySource: Send + Sync {
    /// The policy the recipient applies to inbound mail.
    fn policy_for(&self, recipient: &PublicKey) -> ContactPolicy;
    /// Is `sender` an approved contact of `recipient`? (ContactsOnly).
    fn is_approved_contact(&self, recipient: &PublicKey, sender: &PublicKey) -> bool;
}

/// A [`PolicySource`] with a fixed policy for every recipient (test/simple).
#[derive(Debug, Clone, Copy)]
pub struct FixedPolicy(pub ContactPolicy);

impl PolicySource for FixedPolicy {
    fn policy_for(&self, _recipient: &PublicKey) -> ContactPolicy {
        self.0
    }

    fn is_approved_contact(&self, _recipient: &PublicKey, _sender: &PublicKey) -> bool {
        false
    }
}

/// Default policy source: everyone is `Auto` (accepted).
#[derive(Debug, Default)]
pub struct AutoPolicy;

impl PolicySource for AutoPolicy {
    fn policy_for(&self, _recipient: &PublicKey) -> ContactPolicy {
        ContactPolicy::Auto
    }

    fn is_approved_contact(&self, _recipient: &PublicKey, _sender: &PublicKey) -> bool {
        false
    }
}

/// Ack cache (ADR-0003 G8): acks are private NIP-17 kind-14 to the sender,
/// cached per `(acker-npub, message-id)` and idempotent (second ack returns the
/// stored timestamp).
#[derive(Debug, Default)]
pub struct AckCache {
    inner: Mutex<HashSet<(String, String)>>,
}

impl AckCache {
    /// Record an ack/read receipt. Returns `true` when newly recorded (first
    /// time), `false` when already cached (idempotent duplicate).
    pub fn record(&self, acker: &PublicKey, message_id: &str) -> bool {
        self.inner
            .lock()
            .insert((acker.to_hex(), message_id.to_string()))
    }

    /// Is this `(acker, message-id)` receipt already known?
    pub fn contains(&self, acker: &PublicKey, message_id: &str) -> bool {
        self.inner
            .lock()
            .contains(&(acker.to_hex(), message_id.to_string()))
    }

    /// Number of cached receipts (observable for tests).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Empty cache.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }
}

/// A [`ProjectionSink`] that writes unwrapped canonical content to the Git
/// archive (ADR-0003: Git = permanent, auditable system of record) using the
/// AM-Rust layout, and persists the dedup set in the archive itself.
///
/// Idempotency: `write` stages `messages/{YYYY}/{MM}/{rumor_id}.json` (the
/// year/month from the rumor's created_at) and records a date-independent
/// marker `.projected/{rumor_id}`. The dedup check consults the marker, so a
/// re-delivered wrap across process restarts is still a no-op, and the message
/// file is never written twice.
pub struct GitProjectionSink {
    archive: Arc<parking_lot::Mutex<crate::archive::git::GitArchive>>,
    projected: Mutex<HashSet<String>>,
}

impl GitProjectionSink {
    /// Open (or init) the archive at `root` and bind this sink to it.
    pub fn open(root: &std::path::Path) -> anyhow::Result<Self> {
        let archive = crate::archive::git::GitArchive::open_or_init(root)?;
        Ok(Self {
            archive: Arc::new(parking_lot::Mutex::new(archive)),
            projected: Mutex::new(HashSet::new()),
        })
    }

    /// The in-memory dedup set size (test observable).
    pub fn projected_count(&self) -> usize {
        self.projected.lock().len()
    }

    /// Relative path of the dedup marker for a rumor id.
    fn marker_rel(id: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(".projected").join(format!("{id}.marker"))
    }

    /// Check the marker on disk (survives process restarts).
    fn marker_exists(&self, id: &str) -> bool {
        let root = self.archive.lock().project_root().to_path_buf();
        root.join(Self::marker_rel(id)).exists()
    }
}

#[async_trait::async_trait]
impl ProjectionSink for GitProjectionSink {
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool> {
        Ok(self.projected.lock().contains(&key.rumor_id) || self.marker_exists(&key.rumor_id))
    }

    async fn write(&self, mail: &UnwrappedMail, _local_seq: u64) -> anyhow::Result<bool> {
        let id = mail.rumor_id.to_hex();
        if self.projected.lock().contains(&id) || self.marker_exists(&id) {
            return Ok(false);
        }
        let (yyyy, mm) = civil_from_secs(mail.created_at.as_secs());
        let archive = self.archive.lock();
        let root = archive.project_root().to_path_buf();
        let rel = crate::archive::git::layout::message(&root, yyyy, mm, &id);
        let rel = rel.strip_prefix(&root).expect("inside root").to_path_buf();
        let json = mail.rumor.as_json();
        archive.stage_file(&rel, json.as_bytes())?;
        archive.stage_file(&Self::marker_rel(&id), b"projected")?;
        archive.commit(&format!("project message {id}"))?;
        self.projected.lock().insert(id);
        Ok(true)
    }
}

/// Per-project Git archive routing (G11 / bd-p3-projection-atomicity-rtjz):
/// the production mailbox spans many projects, each with its OWN archive root
/// at `{data_dir}/{project_slug}` (ADR-0003 per-project layout; the operator
/// `doctor reconstruct` rebuilds per-project from that root). A single-root
/// sink cannot serve the mailbox — this variant resolves the archive per
/// recipient via the same npub→slug map the SQLite sink uses.
///
/// Dedup markers stay per-project (`.projected/{id}.marker` under the project
/// root), so `is_projected` is project-scoped like the archive itself.
pub struct ProjectGitProjectionSink {
    data_dir: PathBuf,
    projects: HashMap<String, String>,
    /// slug → open archive (opened lazily per project on first write).
    archives: Mutex<HashMap<String, Arc<parking_lot::Mutex<crate::archive::git::GitArchive>>>>,
    projected: Mutex<HashSet<String>>,
}

impl ProjectGitProjectionSink {
    /// Open a per-project routing sink over `data_dir` (project roots hang
    /// off it: `{data_dir}/{slug}`) with the recipient-npub → slug map.
    pub fn for_mailbox(data_dir: PathBuf, projects: HashMap<String, String>) -> Self {
        Self {
            data_dir,
            projects,
            archives: Mutex::new(HashMap::new()),
            projected: Mutex::new(HashSet::new()),
        }
    }

    /// The archive for a project slug (opened + cached on first touch).
    fn archive_for(
        &self,
        slug: &str,
    ) -> anyhow::Result<Arc<parking_lot::Mutex<crate::archive::git::GitArchive>>> {
        if let Some(a) = self.archives.lock().get(slug).cloned() {
            return Ok(a);
        }
        let root = self.data_dir.join(slug);
        let archive = Arc::new(parking_lot::Mutex::new(
            crate::archive::git::GitArchive::open_or_init(&root)?,
        ));
        self.archives
            .lock()
            .insert(slug.to_string(), archive.clone());
        Ok(archive)
    }

    /// The project slug for a recipient npub ("" when unregistered).
    fn slug_for(&self, recipient: &str) -> String {
        self.projects.get(recipient).cloned().unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl ProjectionSink for ProjectGitProjectionSink {
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool> {
        if self.projected.lock().contains(&key.rumor_id) {
            return Ok(true);
        }
        let slug = self.slug_for(&key.recipient);
        if slug.is_empty() {
            return Ok(false);
        }
        let archive = self.archive_for(&slug)?;
        let marker = archive
            .lock()
            .project_root()
            .join(GitProjectionSink::marker_rel(&key.rumor_id));
        Ok(marker.exists())
    }

    async fn write(&self, mail: &UnwrappedMail, _local_seq: u64) -> anyhow::Result<bool> {
        let id = mail.rumor_id.to_hex();
        if self.projected.lock().contains(&id) {
            return Ok(false);
        }
        let slug = self.slug_for(&mail.recipient.to_hex());
        if slug.is_empty() {
            // Unregistered recipient — no project archive to write to. The
            // SQLite leg (composite sink) still indexes it; the archive gap
            // is healed by `doctor reconstruct` (G11 path 1 needs a project
            // root, and an unregistered recipient has none by definition).
            return Ok(false);
        }
        let archive = self.archive_for(&slug)?;
        let (yyyy, mm) = civil_from_secs(mail.created_at.as_secs());
        let guard = archive.lock();
        let root = guard.project_root().to_path_buf();
        let rel = crate::archive::git::layout::message(&root, yyyy, mm, &id);
        let rel = rel.strip_prefix(&root).expect("inside root").to_path_buf();
        let json = mail.rumor.as_json();
        guard.stage_file(&rel, json.as_bytes())?;
        guard.stage_file(&GitProjectionSink::marker_rel(&id), b"projected")?;
        guard.commit(&format!("project message {id}"))?;
        drop(guard);
        self.projected.lock().insert(id);
        Ok(true)
    }
}

/// G11 / bd-p3-projection-atomicity-rtjz: write ONE message through TWO
/// recovery stores — the Git archive (canonical system of record, per-project)
/// FIRST, then the SQLite index. Ordering + failure semantics:
///
/// - **Git fails** → the whole write fails (`Err` propagates). Nothing is
///   marked projected; the wrap stays on the bus and is re-projected on the
///   next delivery/restart. The system of record never diverges from a
///   "projected" claim.
/// - **Git ok, SQLite fails** → the canonical mail IS durably archived
///   (rebuild-from-Git can restore the index, G11 path 1); the write still
///   counts as projected (archive is the source of truth) with a warn log.
///   `is_projected` is the AND of both stores, so a later re-delivery of the
///   same wrap re-runs the SQLite leg (git write idempotently returns false)
///   and the index heals itself; `doctor reconstruct` is the operator backstop.
pub struct DualProjectionSink {
    git: Arc<dyn ProjectionSink>,
    sqlite: Arc<dyn ProjectionSink>,
}

impl DualProjectionSink {
    /// Chain `git` (canonical) then `sqlite` (index).
    pub fn new(git: Arc<dyn ProjectionSink>, sqlite: Arc<dyn ProjectionSink>) -> Self {
        Self { git, sqlite }
    }
}

#[async_trait::async_trait]
impl ProjectionSink for DualProjectionSink {
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool> {
        // AND: "projected" means BOTH stores have it. A mail that is in the
        // archive but missing from the index (sqlite write failed earlier) is
        // NOT yet projected — a re-delivery re-runs the sqlite leg (git write
        // is idempotent) and heals the lag.
        Ok(self.git.is_projected(key).await? && self.sqlite.is_projected(key).await?)
    }

    async fn write(&self, mail: &UnwrappedMail, local_seq: u64) -> anyhow::Result<bool> {
        // Canonical store first: a git failure must NOT leave a "projected"
        // claim in SQLite while the archive (rebuild source) misses the mail.
        let git_written = self.git.write(mail, local_seq).await?;
        match self.sqlite.write(mail, local_seq).await {
            Ok(w) => Ok(git_written || w),
            Err(e) => {
                tracing::warn!(
                    rumor_id = %mail.rumor_id,
                    "SQLite projection failed after Git archive write — index lag heals via re-delivery or doctor reconstruct (G11 path 1): {e}"
                );
                Ok(git_written)
            }
        }
    }

    /// F19.NIP09 (batch-fix): tombstone the SQLite index leg; the git archive
    /// leg is append-only by design (no-op — DR rebuild re-reads the archive,
    /// and the tombstone is re-applied by the kind-5 event on the bus).
    async fn tombstone(&self, target: &str, author: &str, ts_us: i64) -> anyhow::Result<()> {
        self.sqlite.tombstone(target, author, ts_us).await
    }
}

/// (year, month) from a unix-seconds timestamp (UTC), for the AM-Rust
/// `messages/{YYYY}/{MM}/` layout. Howard Hinnant's civil-from-days algorithm.
fn civil_from_secs(secs: u64) -> (u32, u32) {
    let z = (secs / 86_400) as i64;
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let _d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    ((if m <= 2 { y + 1 } else { y }) as u32, m as u32)
}

/// A [`ProjectionSink`] that writes unwrapped mail into the SQLite `messages`
/// table (the search/query index; oc-b's FTS5 triggers keep `messages_fts`
/// synced). Idempotent on the message `event_id` (`UNIQUE` constraint).
pub struct SqliteProjectionSink {
    pool: sqlx::SqlitePool,
    projected: Mutex<HashSet<String>>,
    /// npub (hex) → project_slug. Gift wraps hide metadata, so the project
    /// cannot be read off the wrap itself; the pipeline resolves it via the
    /// recipient key (populated from the `agents` table at build time —
    /// registration stores the VERBATIM slug, so absolute and relative slugs
    /// both round-trip exactly).
    projects: std::collections::HashMap<String, String>,
    /// npub (hex) → display name. AM-parity: `messages.sender` should read as
    /// the sender's NAME, not the raw pubkey (the wrap only yields the pubkey).
    /// Populated from the `agents` table at build time; hex fallback when the
    /// sender never registered locally (cross-project / external senders).
    names: std::collections::HashMap<String, String>,
}

impl SqliteProjectionSink {
    /// New sink over the index pool.
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            projected: Mutex::new(HashSet::new()),
            projects: std::collections::HashMap::new(),
            names: std::collections::HashMap::new(),
        }
    }

    /// Attach the recipient-npub → project-slug resolution map.
    pub fn with_projects(mut self, projects: std::collections::HashMap<String, String>) -> Self {
        self.projects = projects;
        self
    }

    /// Attach the sender-npub → display-name resolution map (AM-parity
    /// sender naming). Built alongside [`Self::with_projects`] from the same
    /// `agents` table read.
    pub fn with_names(mut self, names: std::collections::HashMap<String, String>) -> Self {
        self.names = names;
        self
    }
}

#[async_trait::async_trait]
impl ProjectionSink for SqliteProjectionSink {
    async fn is_projected(&self, key: &DedupKey) -> anyhow::Result<bool> {
        if self.projected.lock().contains(&key.rumor_id) {
            return Ok(true);
        }
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE event_id = ?1")
            .bind(&key.rumor_id)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);
        Ok(n > 0)
    }

    async fn write(&self, mail: &UnwrappedMail, local_seq: u64) -> anyhow::Result<bool> {
        let id = mail.rumor_id.to_hex();
        if self.projected.lock().contains(&id) {
            return Ok(false);
        }
        let subject = tag_text(&mail.rumor, "subject").unwrap_or_default();
        let importance = tag_text(&mail.rumor, "importance").unwrap_or("normal");
        let ack_required = mail
            .rumor
            .tags
            .iter()
            .any(|t| t.as_slice().first().map(|s| s.as_str()) == Some("ack-required"));
        // Prefer the literal `thread` tag (free-form AM-style ids); fall
        // back to the NIP-10 `e` root tag (event-id threads); a HEAD message
        // (no thread refs) is its own thread — self-thread with its rumor id
        // so `summarize_thread(head_id)` finds it (AM parity: every message
        // belongs to a thread, the head IS its own).
        let thread_id = Some(
            tag_text(&mail.rumor, "thread")
                .or_else(|| tag_text(&mail.rumor, "e"))
                .map(|s| s.to_string())
                .unwrap_or_else(|| id.clone()),
        );

        // The wrap hides the project (metadata hiding) — resolve it via the
        // recipient key from the agents-table map built at pipeline time.
        let project_slug = self
            .projects
            .get(&mail.recipient.to_hex())
            .cloned()
            .unwrap_or_default();
        // AM-parity sender naming: resolve the sender pubkey → display name
        // (hex fallback for unregistered/external senders). `sender_pubkey`
        // stays the raw hex so sender-filtered search keeps working.
        let sender_pubkey = mail.sender.to_hex();
        let sender_name = self
            .names
            .get(&sender_pubkey)
            .cloned()
            .unwrap_or_else(|| sender_pubkey.clone());
        let inserted = sqlx::query(
            "INSERT INTO messages \
             (event_id, project_slug, sender, sender_pubkey, subject, body, \
              importance, ack_required, thread_id, created_ts_us, local_seq) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&id)
        .bind(project_slug)
        .bind(sender_name)
        .bind(sender_pubkey)
        .bind(subject)
        .bind(mail.rumor.content.clone())
        .bind(importance)
        .bind(ack_required)
        .bind(thread_id)
        .bind(mail.ts_us())
        .bind(local_seq as i64)
        .execute(&self.pool)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(false);
        }
        self.projected.lock().insert(id);
        Ok(true)
    }

    /// F19.NIP09 (batch-fix): mark the target message `deleted = 1` — the
    /// message is tombstoned, not removed (search/inbox/thread filters hide
    /// it; the row stays for audit/DR). Author gate: `author` (the kind-5
    /// signer) must equal the target message's `sender_pubkey` — a foreign
    /// signer's delete is a no-op (NIP-09 author-only deletion).
    async fn tombstone(&self, target: &str, author: &str, ts_us: i64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE messages SET deleted = 1, deleted_ts_us = ?3 \
             WHERE event_id = ?1 AND sender_pubkey = ?2",
        )
        .bind(target)
        .bind(author)
        .bind(ts_us)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// First value of a single-letter tag (e.g. `subject`, `e`). The canonical
/// tag-value helper (F22.SURFACE-MESSAGING, batch-fix): messaging.rs's
/// duplicate was deleted; callers pass `&rumor.tags` directly.
pub fn tag_value<'a>(tags: &'a nostr_sdk::nostr::Tags, name: &str) -> Option<&'a str> {
    tags.iter().find_map(|t| {
        let parts = t.as_slice();
        (parts.first().map(|s| s.as_str()) == Some(name))
            .then(|| parts.get(1).map(|s| s.as_str()))?
    })
}

/// First value of a single-letter tag on an unsigned rumor (delegates to
/// [`tag_value`]).
fn tag_text<'a>(rumor: &'a UnsignedEvent, name: &str) -> Option<&'a str> {
    tag_value(&rumor.tags, name)
}

/// The unwrap pipeline: bus events → unwrap with local keys → policy gate →
/// G7 order → idempotent sink write.
pub struct UnwrapPipeline {
    recipients: Vec<Keys>,
    policy: Arc<dyn PolicySource>,
    sink: Arc<dyn ProjectionSink>,
    acks: AckCache,
    seen_wraps: Mutex<HashSet<String>>,
    /// The operator npub for the Overseer bypass (ADR-0004 Ship 5). `None`
    /// disables the bypass (no operator key configured → policy is absolute).
    operator: Option<PublicKey>,
}

impl UnwrapPipeline {
    /// New pipeline over the local recipient keys + a write sink.
    pub fn new(
        recipients: Vec<Keys>,
        policy: Arc<dyn PolicySource>,
        sink: Arc<dyn ProjectionSink>,
    ) -> Self {
        Self {
            recipients,
            policy,
            sink,
            acks: AckCache::default(),
            seen_wraps: Mutex::new(HashSet::new()),
            operator: None,
        }
    }

    /// Set the operator npub for the Overseer bypass (ADR-0004 Ship 5).
    /// Without this, the policy gate is absolute (no override path).
    pub fn with_operator(mut self, operator: PublicKey) -> Self {
        self.operator = Some(operator);
        self
    }

    /// The ack/read-receipt cache (observable for tests + mailbox queries).
    pub fn acks(&self) -> &AckCache {
        &self.acks
    }

    /// Unwrap `wrap` with the local keys, returning the first success.
    fn try_unwrap(&self, wrap: &Event) -> Option<(Keys, gift_wrap::UnwrappedGift)> {
        if wrap.kind != nostr_sdk::nostr::Kind::GiftWrap {
            return None;
        }
        self.recipients
            .iter()
            .find_map(|k| gift_wrap::unwrap(k, wrap).ok().map(|g| (k.clone(), g)))
    }

    /// Project a single wrap event. Returns the outcome (never panics on a
    /// bad/unrelated event — it's a `NotForUs`).
    pub async fn project(&self, wrap: &Event) -> ProjectOutcome {
        // P3 (bd-p3-schnorr-verify-every-ingest-q0wo): verify the ENVELOPE's
        // id+signature BEFORE unwrap/projection. The bus verifies on publish,
        // but direct callers (and partial relay replays) can hand the
        // pipeline a wrap whose content decrypts while its sig is invalid —
        // projecting it would archive a non-canonical event.
        if crate::nostr::event::verify_event(wrap).is_err() {
            return ProjectOutcome::NotForUs;
        }

        // Idempotency guard 1: the wrap event id itself.
        if !self.seen_wraps.lock().insert(wrap.id.to_hex()) {
            return ProjectOutcome::Duplicate;
        }

        // NIP-40 (ADR-0003 rev6): an expired gift wrap is rejected BEFORE any
        // unwrap cost or write — never projected to SQLite/Git. The bus
        // replay filter is not enough: live-published expired wraps bypass it
        // (probe bd-p0-nip40-ingest-probe-oyaa verdict RED, F2.NIP40).
        if wrap.is_expired() {
            return ProjectOutcome::Expired;
        }

        // F19.NIP09 (batch-fix): NIP-09 kind-5 deletion — the AUTHOR signs a
        // deletion referencing the target message by `e`-tag. Tombstone the
        // target row on the index (author-gated: the signer must be the
        // message's sender_pubkey — enforced inside the sink's tombstone).
        // Verified canonical above (id+sig).
        if wrap.kind == nostr_sdk::nostr::Kind::EventDeletion {
            let Some(target) = wrap
                .tags
                .iter()
                .find(|t| t.as_slice().first().map(|s| s.as_str()) == Some("e"))
                .and_then(|t| t.as_slice().get(1).map(|s| s.to_string()))
            else {
                return ProjectOutcome::NotForUs;
            };
            let ts_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
            let author = wrap.pubkey.to_hex();
            match self.sink.tombstone(&target, &author, ts_us).await {
                Ok(()) => return ProjectOutcome::Projected,
                Err(e) => {
                    tracing::warn!(%target, "kind-5 tombstone write failed: {e}");
                    return ProjectOutcome::NotForUs;
                }
            }
        }

        let Some((recipient, gift)) = self.try_unwrap(wrap) else {
            return ProjectOutcome::NotForUs;
        };

        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: gift.sender,
            rumor: gift.rumor.clone(),
            rumor_id: gift.rumor.clone().id(),
            wrap_event_id: wrap.id,
            created_at: gift.rumor.created_at,
        };

        // Idempotency guard 2: (recipient-npub, rumor-id).
        let key = DedupKey::of(&mail.recipient, &mail.rumor_id);
        if self.sink.is_projected(&key).await.unwrap_or(true) {
            return ProjectOutcome::Duplicate;
        }

        // G14: policy gate BEFORE any write. block_all / non-approved
        // contacts_only mail is dropped pre-index, never reaches Git.
        // ADR-0004 Ship 5 carve-out: an operator-signed Overseer override
        // (toron:overseer tag + high/urgent importance) SKIPS the gate.
        let policy = self.policy.policy_for(&mail.recipient);
        let bypass = is_overseer_bypass(&mail, self.operator.as_ref());
        if !bypass
            && !policy.allows(
                self.policy
                    .is_approved_contact(&mail.recipient, &mail.sender),
            )
        {
            return ProjectOutcome::Dropped(policy);
        }

        // G8: ack/read receipts are cached (private NIP-17), not projected as
        // mail rows — but they ARE recorded for idempotent acknowledgement.
        if is_ack_rumor(&mail.rumor) {
            self.acks.record(&mail.sender, &mail.rumor_id.to_hex());
        }

        // G7: local_seq assigned here (arrival order); the sink persists it.
        let local_seq = self.seen_wraps.lock().len() as u64;
        let written = self.sink.write(&mail, local_seq).await.unwrap_or(false);
        if written {
            ProjectOutcome::Projected
        } else {
            ProjectOutcome::Duplicate
        }
    }

    /// Drain a subscription, projecting every delivered event. Returns the
    /// number of newly projected messages.
    pub async fn run(&self, mut sub: crate::bus::Subscription) -> anyhow::Result<usize> {
        let mut projected = 0usize;
        while let Some(ev) = sub.next().await {
            if let crate::bus::SubscriptionEvent::Event(event) = ev
                && event.kind == nostr_sdk::nostr::Kind::GiftWrap
                && self.project(&event).await == ProjectOutcome::Projected
            {
                projected += 1;
            }
        }
        Ok(projected)
    }
}

/// True when a rumor is a G8 ack/read receipt: kind 14 with a `toron-kind`
/// tag of `ack` or `read` plus an `e` reference to the original message.
pub fn is_ack_rumor(rumor: &UnsignedEvent) -> bool {
    if rumor.kind != nostr_sdk::nostr::Kind::PrivateDirectMessage {
        return false;
    }
    let mut toron_kind = None;
    let mut has_e = false;
    for t in rumor.tags.iter() {
        let parts = t.as_slice();
        match parts.first().map(|s| s.as_str()) {
            Some("toron-kind") => toron_kind = parts.get(1).map(|s| s.as_str()),
            Some("e") => has_e = true,
            _ => {}
        }
    }
    matches!(toron_kind, Some("ack" | "read")) && has_e
}

/// True when a kind is eligible for projection unwrapping (metadata-hiding
/// envelope class: gift wraps). Re-exported for the run loop's filter.
pub fn is_wrap_kind(kind: nostr_sdk::nostr::Kind) -> bool {
    is_metadata_hiding_kind(kind) && kind == nostr_sdk::nostr::Kind::GiftWrap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::gift_wrap::build_rumor;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST};
    use nostr_sdk::nostr::{Kind, Tag};

    fn keys() -> Keys {
        Keys::generate()
    }

    async fn wrap_for(sender: &Keys, recipient: &Keys, created_at: u64) -> Event {
        let rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(created_at),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "hello",
        );
        gift_wrap::gift_wrap(sender, &recipient.public_key(), rumor)
            .await
            .expect("wrap")
    }

    #[test]
    fn test_policy_parse_and_allows() {
        assert_eq!(ContactPolicy::parse("open"), ContactPolicy::Open);
        assert_eq!(ContactPolicy::parse("auto"), ContactPolicy::Auto);
        assert_eq!(
            ContactPolicy::parse("contacts_only"),
            ContactPolicy::ContactsOnly
        );
        assert_eq!(ContactPolicy::parse("block_all"), ContactPolicy::BlockAll);
        assert_eq!(ContactPolicy::parse("BOGUS"), ContactPolicy::Auto);

        assert!(ContactPolicy::Open.allows(false));
        assert!(ContactPolicy::Auto.allows(false));
        assert!(ContactPolicy::ContactsOnly.allows(true));
        assert!(!ContactPolicy::ContactsOnly.allows(false));
        assert!(!ContactPolicy::BlockAll.allows(true));
    }

    #[tokio::test]
    async fn test_order_by_rumor_time_ignores_envelope() {
        // Envelope times are randomized by NIP-17 — inverted relative to the
        // rumor timeline. Sorting MUST follow rumor created_at.
        let sender = keys();
        let recipient = keys();
        let older = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let newer = wrap_for(&sender, &recipient, 1_700_000_100).await;

        let rumor_of = |wrap: &Event| -> UnsignedEvent {
            gift_wrap::unwrap(&recipient, wrap).expect("unwrap").rumor
        };

        // Inputs are deliberately out of rumor order: [later rumor, earlier
        // rumor]. The envelope created_at is already randomized by NIP-17, so
        // this is the worst case a projection must survive.
        let mails = vec![
            UnwrappedMail {
                recipient: recipient.public_key(),
                sender: sender.public_key(),
                rumor: rumor_of(&newer),
                rumor_id: EventId::from_hex(
                    "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
                )
                .expect("id"),
                wrap_event_id: newer.id,
                created_at: Timestamp::from(1_700_000_100),
            },
            UnwrappedMail {
                recipient: recipient.public_key(),
                sender: sender.public_key(),
                rumor: rumor_of(&older),
                rumor_id: EventId::from_hex(
                    "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
                )
                .expect("id"),
                wrap_event_id: older.id,
                created_at: Timestamp::from(1_700_000_000),
            },
        ];

        let ordered = order_by_rumor_time(mails);
        assert_eq!(
            ordered[0].0.created_at.as_secs(),
            1_700_000_000,
            "earlier rumor first"
        );
        assert_eq!(
            ordered[1].0.created_at.as_secs(),
            1_700_000_100,
            "later rumor second"
        );
        assert_eq!(
            ordered[0].1, 2,
            "local_seq is arrival order, not time order"
        );
        assert_eq!(ordered[1].1, 1);
    }

    #[test]
    fn test_ts_ms_tag_override() {
        let sender = keys();
        let recipient = keys();
        let rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["ts_ms", "1700000000123456"]).expect("ts_ms")],
            "x",
        );
        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: sender.public_key(),
            rumor: rumor.clone(),
            rumor_id: rumor.clone().id(),
            wrap_event_id: EventId::from_hex(
                "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
            )
            .expect("id"),
            created_at: Timestamp::from(1_700_000_000),
        };
        assert_eq!(mail.ts_us(), 1_700_000_000_123_456);
    }

    #[test]
    fn test_ack_rumor_detection() {
        let sender = keys();
        let target =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
        let ack = build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["toron-kind", "ack"]).expect("ack"),
                Tag::event(target),
            ],
            "",
        );
        assert!(is_ack_rumor(&ack));

        let read = build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["toron-kind", "read"]).expect("read"),
                Tag::event(target),
            ],
            "",
        );
        assert!(is_ack_rumor(&read));

        let plain = build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", "abc"]).expect("p")],
            "hi",
        );
        assert!(!is_ack_rumor(&plain));

        // Missing e tag → not an ack.
        let no_e = build_rumor(
            sender.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["toron-kind", "ack"]).expect("ack")],
            "",
        );
        assert!(!is_ack_rumor(&no_e));
    }

    #[test]
    fn test_ack_cache_idempotent() {
        let cache = AckCache::default();
        let acker = keys().public_key();
        let msg = "evt-123";
        assert!(cache.record(&acker, msg), "first ack records");
        assert!(!cache.record(&acker, msg), "second ack is a no-op");
        assert!(cache.contains(&acker, msg));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_wrap_kind_detection() {
        assert!(is_wrap_kind(Kind::GiftWrap));
        assert!(!is_wrap_kind(Kind::PrivateDirectMessage));
        assert!(!is_wrap_kind(crate::nostr::kinds::toron_kind(
            PROJECT_MANIFEST
        )));
        assert!(!is_wrap_kind(crate::nostr::kinds::toron_kind(
            AGENT_PROFILE
        )));
    }

    #[test]
    fn test_dedup_key_shape() {
        let recipient = keys().public_key();
        let rumor_id =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
        let key = DedupKey::of(&recipient, &rumor_id);
        assert_eq!(key.recipient, recipient.to_hex());
        assert_eq!(key.rumor_id, rumor_id.to_hex());
    }

    fn temp_archive(tag: &str) -> std::path::PathBuf {
        let root =
            std::env::temp_dir().join(format!("toron-unwrap-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create");
        root
    }

    #[tokio::test]
    async fn test_block_all_drops_before_git_write() {
        // G14: block_all mail must be dropped BEFORE any Git/archive write.
        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;

        let root = temp_archive("blockall");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let policy = Arc::new(FixedPolicy(ContactPolicy::BlockAll));
        let pipeline = UnwrapPipeline::new(vec![recipient], policy, sink.clone());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(outcome, ProjectOutcome::Dropped(ContactPolicy::BlockAll));

        // No message file, no marker, no commit: nothing written.
        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            0,
            "blocked mail must not commit"
        );
        let root_now = archive.project_root().to_path_buf();
        drop(archive);
        assert!(
            !root_now.join("messages").exists(),
            "no messages dir for blocked mail"
        );
        assert!(
            !root_now.join(".projected").exists(),
            "no dedup marker for blocked mail"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Build a gift-wrap whose rumor carries the Overseer marker tags
    /// (ADR-0004 Ship 5): `['t', OVERSEER_TAG]` + `['importance', 'high']`.
    async fn wrap_overseer_for(sender: &Keys, recipient: &Keys, created_at: u64) -> Event {
        let rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(created_at),
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p"),
                Tag::parse(["t", OVERSEER_TAG]).expect("overseer tag"),
                Tag::parse(["importance", "high"]).expect("importance"),
            ],
            "operator override: pause and handle",
        );
        gift_wrap::gift_wrap(sender, &recipient.public_key(), rumor)
            .await
            .expect("wrap")
    }

    #[tokio::test]
    async fn overseer_bypasses_block_all() {
        // ADR-0004 Ship 5 acceptance: operator-signed overseer msg to a
        // block_all agent → DELIVERED (not dropped at projection).
        let operator = keys();
        let recipient = keys();
        let wrap = wrap_overseer_for(&operator, &recipient, 1_700_000_000).await;

        let root = temp_archive("overseer-bypass");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let policy = Arc::new(FixedPolicy(ContactPolicy::BlockAll));
        let pipeline = UnwrapPipeline::new(vec![recipient], policy, sink.clone())
            .with_operator(operator.public_key());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(
            outcome,
            ProjectOutcome::Projected,
            "overseer bypasses block_all"
        );
        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            1,
            "overseer mail is archived (not dropped)"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn non_overseer_blocked() {
        // ADR-0004 Ship 5 acceptance: an AGENT (non-operator key) sending to a
        // block_all agent → still dropped. No bypass for non-operator keys.
        let agent = keys();
        let recipient = keys();
        let wrap = wrap_overseer_for(&agent, &recipient, 1_700_000_000).await;

        let root = temp_archive("non-overseer");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let policy = Arc::new(FixedPolicy(ContactPolicy::BlockAll));
        // The operator is a DIFFERENT key than the sender.
        let operator = keys();
        let pipeline = UnwrapPipeline::new(vec![recipient], policy, sink.clone())
            .with_operator(operator.public_key());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(
            outcome,
            ProjectOutcome::Dropped(ContactPolicy::BlockAll),
            "agent-signed mail stays blocked even with overseer tags"
        );
        let archive = sink.archive.lock();
        assert_eq!(archive.commit_count().expect("count"), 0);
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn overseer_requires_tag() {
        // ADR-0004 Ship 5 acceptance: operator-signed msg WITHOUT the
        // toron:overseer tag → normal policy applies, no bypass. The operator
        // key alone is not a blanket bypass.
        let operator = keys();
        let recipient = keys();
        // Same sender, same importance, but NO overseer tag (plain wrap).
        let wrap = wrap_for(&operator, &recipient, 1_700_000_000).await;

        let root = temp_archive("overseer-no-tag");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let policy = Arc::new(FixedPolicy(ContactPolicy::BlockAll));
        let pipeline = UnwrapPipeline::new(vec![recipient], policy, sink.clone())
            .with_operator(operator.public_key());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(
            outcome,
            ProjectOutcome::Dropped(ContactPolicy::BlockAll),
            "operator without the overseer tag is still policy-gated"
        );
        let archive = sink.archive.lock();
        assert_eq!(archive.commit_count().expect("count"), 0);
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_block_all_contacts_only_gates() {
        // contacts_only without an approved contact link → dropped pre-write.
        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;

        let root = temp_archive("contactsonly");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let policy = Arc::new(FixedPolicy(ContactPolicy::ContactsOnly));
        let pipeline = UnwrapPipeline::new(vec![recipient], policy, sink.clone());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(
            outcome,
            ProjectOutcome::Dropped(ContactPolicy::ContactsOnly)
        );
        let archive = sink.archive.lock();
        assert_eq!(archive.commit_count().expect("count"), 0);
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn test_delivered_twice_is_single_write() {
        // Idempotency (beads-review): the same wrap arriving twice (bus
        // redelivery / federation replay) MUST produce one Git commit.
        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;

        let root = temp_archive("idempotent");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let pipeline =
            UnwrapPipeline::new(vec![recipient.clone()], Arc::new(AutoPolicy), sink.clone());

        let first = pipeline.project(&wrap).await;
        assert_eq!(first, ProjectOutcome::Projected);

        let second = pipeline.project(&wrap).await;
        assert_eq!(second, ProjectOutcome::Duplicate);

        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            1,
            "two deliveries -> one commit"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Build a gift wrap whose OUTER event carries a NIP-40 `expiration` tag
    /// (the wire-level event relays/peers enforce — NIP-17 randomized inner
    /// timestamps are irrelevant to NIP-40, which reads the wrap itself).
    async fn wrap_with_expiration(sender: &Keys, recipient: &Keys, expires_at: Timestamp) -> Event {
        let rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "expiring hello",
        );
        nostr_sdk::nostr::EventBuilder::gift_wrap(
            sender,
            &recipient.public_key(),
            rumor,
            [Tag::expiration(expires_at)],
        )
        .await
        .expect("wrap with expiration")
    }

    /// P0 finding (bd-p0-nip40-ingest-probe-oyaa): a gift wrap whose NIP-40
    /// expiration is already in the past must be REJECTED on the unwrap
    /// ingest path — never projected to SQLite/Git. The ADR-0003
    /// "Parity Superiority" replay/expiry claim requires expired envelopes
    /// to be dropped BEFORE projection.
    ///
    /// RED: currently fails — `UnwrapPipeline::project` has no TTL check and
    /// the live bus fanout delivers the expired wrap.
    #[tokio::test]
    async fn expired_gift_wrap_rejected_on_unwrap() {
        let sender = keys();
        let recipient = keys();
        let past = Timestamp::from(Timestamp::now().as_secs().saturating_sub(60));
        let expired = wrap_with_expiration(&sender, &recipient, past).await;

        let root = temp_archive("nip40-expired");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let pipeline = UnwrapPipeline::new(vec![recipient], Arc::new(AutoPolicy), sink.clone());

        let outcome = pipeline.project(&expired).await;
        assert_ne!(
            outcome,
            ProjectOutcome::Projected,
            "expired gift wrap must not be projected"
        );
        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            0,
            "expired mail must not reach the Git archive"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Regression: NIP-40 expiration in the FUTURE must not block delivery.
    #[tokio::test]
    async fn future_expiration_gift_wrap_accepted_on_unwrap() {
        let sender = keys();
        let recipient = keys();
        let future = Timestamp::from(Timestamp::now().as_secs() + 86_400);
        let wrap = wrap_with_expiration(&sender, &recipient, future).await;

        let root = temp_archive("nip40-future");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let pipeline = UnwrapPipeline::new(vec![recipient], Arc::new(AutoPolicy), sink.clone());

        let outcome = pipeline.project(&wrap).await;
        assert_eq!(outcome, ProjectOutcome::Projected);
        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            1,
            "future-expiring mail projects normally"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// P3 (bd-p3-schnorr-verify-every-ingest-q0wo): the unwrap pipeline must
    /// verify the WRAP envelope's own id+signature BEFORE projection — the
    /// bus verifies on publish, but a direct `project()` call (or a partial
    /// relay replay) can hand the pipeline a wrap whose CONTENT is valid
    /// (seal decrypts) while its ENVELOPE sig is invalid. Projecting it
    /// would archive a non-canonical event.
    ///
    /// RED today: `project()` never verifies the wrap — decrypt ignores the
    /// sig, so a sig-corrupted wrap is accepted and committed.
    #[tokio::test]
    async fn forged_wrap_signature_rejected_on_unwrap() {
        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        // Corrupt ONLY the wrap's signature (content untouched → decrypt
        // still succeeds today).
        let forged = nostr_sdk::nostr::Event::new(
            wrap.id,
            wrap.pubkey,
            wrap.created_at,
            wrap.kind,
            wrap.tags.clone(),
            wrap.content.clone(),
            nostr_sdk::nostr::prelude::Signature::from_slice(&[0u8; 64]).expect("sig"),
        );
        assert!(
            crate::nostr::event::verify_event(&forged).is_err(),
            "precondition: forged wrap has an invalid signature"
        );

        let root = temp_archive("forge-sig");
        let sink = Arc::new(GitProjectionSink::open(&root).expect("open"));
        let pipeline =
            UnwrapPipeline::new(vec![recipient.clone()], Arc::new(AutoPolicy), sink.clone());

        let outcome = pipeline.project(&forged).await;
        assert_ne!(
            outcome,
            ProjectOutcome::Projected,
            "forged (invalid-signature) wrap must NOT be projected"
        );
        let archive = sink.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            0,
            "forged wrap must not reach the archive"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn test_civil_date_layout() {
        // 2026-01-01T00:00:00Z -> (2026, 1); a mid-year date -> (2026, 8).
        assert_eq!(civil_from_secs(1_767_225_600), (2026, 1));
        assert_eq!(civil_from_secs(1_700_000_000), (2023, 11));
    }

    /// P3 probe #6 (bd-p3-nip09-deletion-projection-uzgy): a NIP-09 kind-5
    /// deletion (author-signed, `e`-tag → message) must TOMBSTONE the
    /// projected message — deleted mail must not stay readable on either
    /// projection.
    ///
    /// RED today: NO kind-5/62 handling exists anywhere (no `deleted` column,
    /// no delete path in either sink, the unwrap loop only watches gift
    /// wraps) — a deleted message lingers fully readable. The batch fix adds
    /// the tombstone column + a kind-5 ingest that marks the target row.
    #[tokio::test]
    async fn nip09_delete_tombstones_projected_message() {
        use crate::index::schema;

        let path = std::env::temp_dir().join(format!("nip09-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(path.to_str().unwrap()).await.expect("pool");
        schema::migrate(&pool).await.expect("migrate");

        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let rumor = gift_wrap::unwrap(&recipient, &wrap).expect("unwrap").rumor;
        let rumor_id = rumor.clone().id();
        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: sender.public_key(),
            rumor: rumor.clone(),
            rumor_id: rumor_id.clone(),
            wrap_event_id: wrap.id,
            created_at: Timestamp::from(1_700_000_000),
        };

        let sink = SqliteProjectionSink::new(pool.clone());
        let pipeline = UnwrapPipeline::new(
            vec![recipient.clone()],
            Arc::new(AutoPolicy),
            Arc::new(sink),
        );
        assert_eq!(pipeline.project(&wrap).await, ProjectOutcome::Projected);

        // NIP-09: the AUTHOR signs a kind-5 deletion referencing the message.
        let delete = nostr_sdk::nostr::EventBuilder::new(nostr_sdk::nostr::Kind::EventDeletion, "")
            .tags([nostr_sdk::nostr::Tag::event(rumor_id.clone())])
            .sign_with_keys(&sender)
            .expect("sign kind-5");
        pipeline.project(&delete).await;

        // Contract: the message must be tombstoned (deleted flag), NOT
        // readable. Pre-fix: the `deleted` column does not exist -> the query
        // errors -> RED.
        let deleted: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE event_id = ?1 AND deleted = 1")
                .bind(rumor_id.to_hex())
                .fetch_one(&pool)
                .await
                .expect("deleted column must exist (NIP-09 tombstone support)");
        assert_eq!(
            deleted, 1,
            "author kind-5 deletion must tombstone the projected message"
        );

        let _ = std::fs::remove_file(&path);
    }

    /// P5 (bd-p5-projection-differential-tests-vhz9): the three projections of
    /// the SAME input must agree bar-for-bar — bus (envelope replay → unwrap),
    /// git archive (messages/{YYYY}/{MM}/{id}.json), sqlite (messages table).
    /// Same rumor-id set, same G7 order (rumor created_at), same count.
    #[tokio::test]
    async fn projection_differential_bus_git_sqlite_agree() {
        use crate::bus::Filter;
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;

        const N: usize = 10;
        let db = std::env::temp_dir().join(format!("proj-diff-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");

        let sender = keys();
        let recipient = keys();
        let bus: std::sync::Arc<dyn crate::bus::Bus> = std::sync::Arc::new(bus);
        let mut wraps = Vec::with_capacity(N);
        for i in 0..N {
            let wrap = wrap_for(&sender, &recipient, 1_700_000_000 + i as u64 * 100).await;
            bus.publish(wrap.clone()).await.expect("publish to bus");
            wraps.push(wrap);
        }

        // Same wraps → git sink pipeline.
        let git_root = temp_archive("proj-diff-git");
        let git_sink = Arc::new(GitProjectionSink::open(&git_root).expect("git sink"));
        let git_pipeline = UnwrapPipeline::new(
            vec![recipient.clone()],
            Arc::new(AutoPolicy),
            git_sink.clone(),
        );
        // Same wraps → sqlite sink pipeline.
        let sqlite_sink = SqliteProjectionSink::new(pool.clone());
        let sqlite_pipeline = UnwrapPipeline::new(
            vec![recipient.clone()],
            Arc::new(AutoPolicy),
            Arc::new(sqlite_sink),
        );
        for wrap in &wraps {
            assert_eq!(
                git_pipeline.project(wrap).await,
                ProjectOutcome::Projected,
                "git projection lands"
            );
            assert_eq!(
                sqlite_pipeline.project(wrap).await,
                ProjectOutcome::Projected,
                "sqlite projection lands"
            );
        }

        // Projection 1 — bus: replay envelopes, unwrap, order by rumor time.
        let replayed = crate::mailbox::collect_replay(
            bus.as_ref(),
            Filter::new().kinds([nostr_sdk::nostr::Kind::GiftWrap]),
        )
        .await
        .expect("bus replay");
        assert_eq!(replayed.len(), N, "bus holds all N envelopes");
        let mut bus_rumors: Vec<(String, u64)> = replayed
            .iter()
            .filter_map(|w| {
                gift_wrap::unwrap(&recipient, w)
                    .ok()
                    .map(|g| (g.rumor.clone().id().to_hex(), g.rumor.created_at.as_secs()))
            })
            .collect();
        bus_rumors.sort_by_key(|(_, ts)| *ts);

        // Projection 2 — git: walk messages/{YYYY}/{MM}/*.json.
        let mut git_rumors: Vec<(String, u64)> = Vec::new();
        let messages = git_root.join("messages");
        let mut stack = vec![messages.clone()];
        while let Some(dir) = stack.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().and_then(|s| s.to_str()) == Some("json") {
                    let rumor = nostr_sdk::nostr::event::unsigned::UnsignedEvent::from_json(
                        std::fs::read_to_string(&p).expect("read"),
                    )
                    .expect("rumor json");
                    git_rumors.push((rumor.clone().id().to_hex(), rumor.created_at.as_secs()));
                }
            }
        }
        git_rumors.sort_by_key(|(_, ts)| *ts);

        // Projection 3 — sqlite: event_id + created_ts_us in G7 order.
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT event_id, created_ts_us FROM messages ORDER BY created_ts_us, local_seq",
        )
        .fetch_all(&pool)
        .await
        .expect("sqlite rows");
        assert_eq!(rows.len(), N, "sqlite projects all N");

        let sqlite_ids: Vec<String> = rows.into_iter().map(|(id, _)| id).collect();
        let bus_ids: Vec<String> = bus_rumors.iter().map(|(id, _)| id.clone()).collect();
        let git_ids: Vec<String> = git_rumors.iter().map(|(id, _)| id.clone()).collect();

        assert_eq!(
            bus_ids, git_ids,
            "bus + git projections agree bar-for-bar (same rumor ids, G7 order)"
        );
        assert_eq!(
            git_ids, sqlite_ids,
            "git + sqlite projections agree bar-for-bar (same rumor ids, G7 order)"
        );

        let _ = std::fs::remove_dir_all(&git_root);
        let _ = std::fs::remove_file(&db);
    }

    /// S4 (bd-indexer-coverage-dp9t): every production write path to
    /// `messages` must be FTS-findable. Write-path inventory:
    ///
    /// 1. live projection — unwrap loop → SqliteProjectionSink
    ///    (`INSERT INTO messages`, unwrap.rs:690)
    /// 2. recovery/replay — `rebuild::insert_record` (Git rebuild G11 path 1
    ///    + bus-replay sink G11 path 2, `INSERT OR IGNORE`)
    ///
    /// `messages_fts` is an external-content FTS5 table kept in sync by
    /// AFTER INSERT triggers (0001_schema.sql), so every `messages` insert is
    /// index-visible — this test pins that per path with a unique token.
    #[tokio::test]
    async fn every_write_path_is_fts_visible() {
        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index::schema;
        use crate::index::writer::IndexWriter;
        use crate::index::rebuild::{insert_record, MailRecord};
        use crate::mailbox::Mailbox;

        let db = std::env::temp_dir().join(format!("fts-coverage-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "fts-coverage-data-{}",
            uuid::Uuid::new_v4()
        ));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_index_pool(pool.clone())
            .with_agent_passphrase("fts-coverage-pass");
        mailbox.spawn_unwrap_loop();

        // Path 1 — live projection: a real send through the bus + unwrap
        // loop lands in messages and the FTS index.
        mailbox
            .register_agent(
                "demo",
                &crate::nostr::identity::AgentProfile {
                    name: "GreenCastle".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register sender");
        mailbox
            .register_agent(
                "demo",
                &crate::nostr::identity::AgentProfile {
                    name: "BlueLake".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register recipient");
        mailbox
            .send_message(&crate::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "live-path".into(),
                body_md: "fts-coverage-token-live".into(),
                ..Default::default()
            })
            .await
            .expect("send live");

        // Path 2 — recovery/replay: insert_record (shared by Git rebuild +
        // bus replay) with its own unique token.
        let writer = IndexWriter::new(pool.clone(), pool.clone());
        insert_record(
            &writer,
            &MailRecord {
                event_id: "replay-evt-1".into(),
                project_slug: "demo".into(),
                sender: "ReplaySender".into(),
                sender_pubkey: "replaypubkey".into(),
                subject: "replay-path".into(),
                body: "fts-coverage-token-replay".into(),
                importance: "normal".into(),
                ack_required: false,
                thread_id: None,
                reply_to: None,
                to_list: vec!["BlueLake".into()],
                cc_list: vec![],
                created_ts_us: 1_700_000_000_500_000,
                local_seq: 7,
            },
        )
        .await
        .expect("insert_record");

        // Both tokens must be findable through the real search surface.
        for token in ["fts-coverage-token-live", "fts-coverage-token-replay"] {
            let mut found = false;
            for _ in 0..200 {
                let hits = mailbox
                    .search_messages(&crate::mailbox::SearchMessages {
                        project_key: "demo".into(),
                        query: token.into(),
                        ..Default::default()
                    })
                    .await
                    .expect("search");
                if !hits.is_empty() {
                    found = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            assert!(found, "token {token} must be FTS-findable");
        }

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// P3 (bd-p3-replay-dedup-per-sink-nve6): the SQLite sink is idempotent —
    /// the same mail written twice produces ONE row (in-mem set + UNIQUE
    /// event_id + `rows_affected()==0` backstop).
    #[tokio::test]
    async fn sqlite_sink_double_write_single_row() {
        let path = std::env::temp_dir().join(format!(
            "toron-sqlite-dedup-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("pool");
        crate::index::schema::migrate(&pool).await.expect("migrate");

        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: sender.public_key(),
            rumor: gift_wrap::unwrap(&recipient, &wrap).expect("unwrap").rumor,
            rumor_id: wrap.id,
            wrap_event_id: wrap.id,
            created_at: Timestamp::from(1_700_000_000),
        };

        let sink = SqliteProjectionSink::new(pool.clone());
        let first = sink.write(&mail, 1).await.expect("first write");
        assert!(first, "first projection lands");
        let second = sink.write(&mail, 2).await.expect("second write");
        assert!(!second, "replay must be a no-op");

        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&pool)
            .await
            .expect("count");
        assert_eq!(n, 1, "double write -> one row");

        // is_projected agrees (fresh lookups hit the in-mem set).
        let key = DedupKey::of(&recipient.public_key(), &wrap.id);
        assert!(sink.is_projected(&key).await.expect("is_projected"));
        let _ = std::fs::remove_file(&path);
    }

    /// P3 (bd-p3-replay-dedup-per-sink-nve6): the git sink's dedup SURVIVES a
    /// process restart — a NEW sink instance on the same archive root must
    /// not re-project (marker file on disk), so federation replay after a
    /// crash cannot duplicate commits.
    #[tokio::test]
    async fn git_sink_restart_persists_dedup() {
        let sender = keys();
        let recipient = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let rumor = gift_wrap::unwrap(&recipient, &wrap).expect("unwrap").rumor;
        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: sender.public_key(),
            rumor: rumor.clone(),
            rumor_id: rumor.clone().id(),
            wrap_event_id: wrap.id,
            created_at: Timestamp::from(1_700_000_000),
        };

        let root = temp_archive("restart-dedup");
        let sink_a = GitProjectionSink::open(&root).expect("open");
        assert!(sink_a.write(&mail, 1).await.expect("write"), "first lands");
        assert_eq!(sink_a.projected_count(), 1);

        // "Restart": a brand-new sink instance over the same archive root.
        drop(sink_a);
        let sink_b = GitProjectionSink::open(&root).expect("reopen");
        assert_eq!(sink_b.projected_count(), 0, "fresh in-mem set");
        let key = DedupKey::of(&recipient.public_key(), &rumor.clone().id());
        assert!(
            sink_b.is_projected(&key).await.expect("is_projected"),
            "marker on disk survives restart"
        );
        assert!(
            !sink_b.write(&mail, 2).await.expect("replay write"),
            "replayed mail after restart is a no-op"
        );
        let archive = sink_b.archive.lock();
        assert_eq!(
            archive.commit_count().expect("count"),
            1,
            "restart replay must not add commits"
        );
        drop(archive);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// G11 / bd-p3-projection-atomicity-rtjz: the per-project routing sink
    /// writes each mail under `{data_dir}/{project_slug}/messages/...`, and
    /// `is_projected` is project-scoped (recipient → slug via the map).
    #[tokio::test]
    async fn project_git_sink_routes_by_project_and_dedups() {
        let data_dir = std::env::temp_dir().join(format!("toron-projgit-{}", uuid::Uuid::new_v4()));
        let recipient = keys();
        let slug = "demo";
        let projects: HashMap<String, String> =
            [(recipient.public_key().to_hex(), slug.to_string())]
                .into_iter()
                .collect();
        let sink = ProjectGitProjectionSink::for_mailbox(data_dir.clone(), projects);
        let pipeline = UnwrapPipeline::new(
            vec![recipient.clone()],
            Arc::new(AutoPolicy),
            Arc::new(sink),
        );

        let sender = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let outcome = pipeline.project(&wrap).await;
        assert_eq!(outcome, ProjectOutcome::Projected, "mail projected");

        // Archive root is per-project: {data_dir}/demo/messages/{YYYY}/{MM}/*.json
        let messages_root = data_dir.join(slug).join("messages");
        let json_files = walk_json(&messages_root);
        assert_eq!(
            json_files.len(),
            1,
            "exactly one canonical file under {}",
            messages_root.display()
        );

        // Dedup: same mail → Duplicate (marker on disk under the project root).
        let outcome2 = pipeline.project(&wrap).await;
        assert_eq!(
            outcome2,
            ProjectOutcome::Duplicate,
            "per-project marker dedups re-delivery"
        );
        assert_eq!(
            walk_json(&messages_root).len(),
            1,
            "no second canonical file on re-delivery"
        );
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// G11 / bd-p3-projection-atomicity-rtjz: dual-sink ordering — git FIRST.
    /// A failing git leg fails the whole write (nothing marked projected); a
    /// failing sqlite leg after a successful git write still counts as
    /// projected (archive = canonical) and `is_projected` stays false so a
    /// re-delivery heals the index.
    #[tokio::test]
    async fn dual_sink_git_failure_blocks_and_sqlite_failure_heals() {
        use crate::index::schema;

        let recipient = keys();
        let sender = keys();
        let wrap = wrap_for(&sender, &recipient, 1_700_000_000).await;
        let mail = UnwrappedMail {
            recipient: recipient.public_key(),
            sender: sender.public_key(),
            rumor: gift_wrap::unwrap(&recipient, &wrap).expect("unwrap").rumor,
            rumor_id: wrap.id,
            wrap_event_id: wrap.id,
            created_at: wrap.created_at,
        };
        let key = DedupKey::of(&recipient.public_key(), &mail.rumor_id);

        // ---- case 1: git leg fails -> write Err, nothing projected ----
        let failing_git = FailSink::new();
        let db = std::env::temp_dir().join(format!("dual-sink-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
        schema::migrate(&pool).await.expect("migrate");
        let sqlite: Arc<dyn ProjectionSink> = Arc::new(SqliteProjectionSink::new(pool.clone()));
        let dual = DualProjectionSink::new(Arc::new(FailSink::new()), sqlite.clone());
        let res = dual.write(&mail, 1).await;
        assert!(res.is_err(), "git failure must fail the whole write");

        // ---- case 2: sqlite leg fails after git ok -> Ok(true), healable ----
        let git_root = temp_archive("dual-git-ok");
        let git_ok: Arc<dyn ProjectionSink> =
            Arc::new(GitProjectionSink::open(&git_root).expect("git sink"));
        let failing_sqlite: Arc<dyn ProjectionSink> = Arc::new(FailSink::new());
        let dual2 = DualProjectionSink::new(git_ok.clone(), failing_sqlite.clone());
        let written = dual2.write(&mail, 1).await.expect("write");
        assert!(
            written,
            "archive write counts as projected despite index failure"
        );
        // is_projected = AND: git has it, sqlite (failing) reports not-projected
        // -> composite reports NOT projected, so a re-delivery heals.
        assert!(
            !dual2.is_projected(&key).await.expect("is_projected"),
            "partial (git-only) projection must read as not-yet-projected"
        );
        // Heal: swap in the real sqlite sink; re-write; now fully projected.
        let dual3 = DualProjectionSink::new(git_ok.clone(), sqlite.clone());
        let healed = dual3.write(&mail, 1).await.expect("heal write");
        assert!(
            healed,
            "heal write: git leg idempotent (false) OR sqlite leg inserted (true)"
        );
        assert!(
            dual3.is_projected(&key).await.expect("is_projected"),
            "after heal, both stores have it -> projected"
        );

        let _ = std::fs::remove_dir_all(&git_root);
        let _ = std::fs::remove_file(&db);
    }

    /// A sink that always fails — for dual-sink failure-semantics tests.
    struct FailSink;

    impl FailSink {
        fn new() -> Self {
            FailSink
        }
    }

    #[async_trait::async_trait]
    impl ProjectionSink for FailSink {
        async fn is_projected(&self, _key: &DedupKey) -> anyhow::Result<bool> {
            Ok(false)
        }

        async fn write(&self, _mail: &UnwrappedMail, _local_seq: u64) -> anyhow::Result<bool> {
            Err(anyhow::anyhow!("fail sink"))
        }
    }

    /// Collect every `*.json` under `messages/` (recursive).
    fn walk_json(root: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        let Ok(years) = std::fs::read_dir(root) else {
            return out;
        };
        for year in years.flatten() {
            let Ok(months) = std::fs::read_dir(year.path()) else {
                continue;
            };
            for month in months.flatten() {
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
}
