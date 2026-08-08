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

use std::collections::HashSet;
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
}

impl SqliteProjectionSink {
    /// New sink over the index pool.
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            projected: Mutex::new(HashSet::new()),
            projects: std::collections::HashMap::new(),
        }
    }

    /// Attach the recipient-npub → project-slug resolution map.
    pub fn with_projects(mut self, projects: std::collections::HashMap<String, String>) -> Self {
        self.projects = projects;
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
        let thread_id = tag_text(&mail.rumor, "e").map(|s| s.to_string());

        // The wrap hides the project (metadata hiding) — resolve it via the
        // recipient key from the agents-table map built at pipeline time.
        let project_slug = self
            .projects
            .get(&mail.recipient.to_hex())
            .cloned()
            .unwrap_or_default();
        let inserted = sqlx::query(
            "INSERT INTO messages \
             (event_id, project_slug, sender, sender_pubkey, subject, body, \
              importance, ack_required, thread_id, created_ts_us, local_seq) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) \
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(&id)
        .bind(project_slug)
        .bind(mail.sender.to_hex())
        .bind(mail.sender.to_hex())
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
}

/// First value of a single-letter tag (e.g. `subject`, `e`).
fn tag_text<'a>(rumor: &'a UnsignedEvent, name: &str) -> Option<&'a str> {
    rumor.tags.iter().find_map(|t| {
        let parts = t.as_slice();
        (parts.first().map(|s| s.as_str()) == Some(name))
            .then(|| parts.get(1).map(|s| s.as_str()))?
    })
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
        // Idempotency guard 1: the wrap event id itself.
        if !self.seen_wraps.lock().insert(wrap.id.to_hex()) {
            return ProjectOutcome::Duplicate;
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

    #[test]
    fn test_civil_date_layout() {
        // 2026-01-01T00:00:00Z -> (2026, 1); a mid-year date -> (2026, 8).
        assert_eq!(civil_from_secs(1_767_225_600), (2026, 1));
        assert_eq!(civil_from_secs(1_700_000_000), (2023, 11));
    }
}
