// governed-by: ADR-0003
// governed-by: ADR-0011 (Spec 2/4/6 — intents, soft-conflict, request-release)
// governed-by: ADR-0017 (project-root canonicalization)
#![forbid(unsafe_code)]

//! File reservation cluster (bd-led.5.5): file_reservation_paths (acquire),
//! renew, release, check_file_reservation_conflicts, force_release.
//!
//! Wire (ADR-0003):
//! - kind-30031 addressable; `d`=`project:path-glob`; NIP-40 `expiration`=TTL;
//!   dual-emit scoping (`#t` + NIP-32 label — kind-30031 is not hiding).
//! - Conflict = local **symmetric** fnmatch both ways (`src/**` ↔ `src/lib.rs`).
//!
//! **Force-release (G5 A1) — never cross-key NIP-09:**
//! 1. staleness heuristic (holder inactive beyond threshold);
//! 2. the **operator** signs a kind-30031 `status:revoked` event + `e`/`a`
//!    → target lease (same `(kind,d)` coordinate);
//! 3. projection key-priority `operator > agent` resolves it (all toron nodes
//!    identical — [`crate::bus::resolve_coordinate`]);
//! 4. NIP-09 is never the correctness mechanism (author-only);
//! 5. previous holder notified via NIP-17 with the heuristic summary;
//! 6. Git archive records the acquire → revoked transition (G13 history).
//!
//! **Intents + soft conflicts (ADR-0011 Spec 2/4):** a 30031 may carry a
//! `status:intent` marker — a declared plan to edit a path set BEFORE
//! acquiring. Status-less 30031s = held (backward compat, unchanged). The
//! conflict view splits TWO tiers over the SAME atomic projection:
//! - an acquire against someone's **held** exclusive lease → HARD conflict →
//!   blocked (the corruption-prevention floor, NEVER removed);
//! - an acquire against someone's in-TTL **intent** → SOFT conflict → the
//!   acquirer is frozen (not granted, classified soft — a hard block would
//!   collapse the intent layer into request-release).
//!
//! Intents auto-expire after [`INTENT_TTL_SECS`] unless they convert to held
//!   (the same author acquiring the same `(kind,d)` coordinate supersedes the
//!   intent — must-convert, anti-griefing).
//!
//! **Request-release negotiation (ADR-0011 Spec 6):** replaces the binary
//! voluntary-release | force-release with a ladder: a typed kind-14 DM asks
//! the holder to release/defer (toron-kind=request-release), the holder
//! answers (request-release-response), and a request unanswered within
//! [`REQUEST_RELEASE_TIER_WINDOW_SECS`] escalates to a kind-14 DM to the
//! captain (the operator key). Force-release stays the operator last resort.

use crate::nostr::{Event, EventId, Keys, Kind, PublicKey, Tag, Timestamp};

use crate::bus::addressable::is_revocation;
use crate::bus::filter::{ToronFilter, kind_project_filter};
use crate::bus::scoping::with_project_scoping;
use crate::bus::{EnvelopeTtl, resolve_coordinate};
use crate::mailbox::guard::{cross_project_pattern, pattern_matches};
use crate::nostr::event::build_event;
use crate::nostr::gift_wrap;
use crate::nostr::kinds::{FILE_RESERVATION, toron_kind};

use super::Mailbox;
use super::collect_replay;
use super::messaging::{SendMessage, SendResult};
use super::reservations_core::{self, EntryState, KernelModel, LeaseEntry};

/// Default staleness threshold for force-release (holder inactive this long
/// counts as stale). Matches the AM-Rust inactivity heuristic spirit (30 min).
pub const STALE_AFTER_SECS: u64 = 30 * 60;

/// Intent TTL (ADR-0011 Spec 2): an intent auto-expires this long after
/// declaration UNLESS it converts to a held reservation (must-convert). A
/// speculative intent cannot freeze peers indefinitely. Tunable per
/// declaration via `declare_intent(ttl_secs)`.
pub const INTENT_TTL_SECS: u64 = 5 * 60;

/// Request-release escalation tier (ADR-0011 Spec 6): a request unanswered
/// within this window escalates to a captain DM (kind-14, importance high).
pub const REQUEST_RELEASE_TIER_WINDOW_SECS: u64 = 15 * 60;
/// Default TTL for a HELD file lease (bd-lease-ttl-180s).
///
/// This was the envelope TTL (`DEFAULT_ENVELOPE_TTL_SECS`, 90 days) — an
/// event-retention number used as a work-claim number. A crashed agent then
/// blocked its paths for a quarter, and every cleanup was manual
/// (`force-release`). A lease is a heartbeat, not a deed: 180 s is longer
/// than an LLM turn plus a tool call, and `with-lease` renews every half-TTL
/// while its command runs, so a live worker never loses a hold. A dead one
/// frees the path inside three minutes.
pub const DEFAULT_RESERVATION_TTL_SECS: u64 = 3 * 60;

/// bd-lease-liveness-bo0g: session-liveness oracle for lease holders.
/// Input = the holder's pubkey hex; output = true when that holder's
/// session is STILL LIVE (herdr pane alive / fresh presence heartbeat).
/// Installed by the embedder (the daemon knows panes; core does not).
/// With NO oracle installed, expired leases clear — pure NIP-40 semantics,
/// byte-identical to pre-bo0g behavior.
pub type LeaseLivenessFn = std::sync::Arc<dyn Fn(&str) -> bool + Send + Sync>;

static LEASE_LIVENESS: std::sync::Mutex<Option<LeaseLivenessFn>> = std::sync::Mutex::new(None);

/// Install (or replace) the holder-liveness oracle. Idempotent per install;
/// tests should [`clear_lease_liveness_checker`] when done (global state).
pub fn install_lease_liveness_checker(f: LeaseLivenessFn) {
    if let Ok(mut slot) = LEASE_LIVENESS.lock() {
        *slot = Some(f);
    }
}

/// Drop the oracle (back to pure TTL semantics). Test reset + daemon
/// shutdown hygiene.
pub fn clear_lease_liveness_checker() {
    if let Ok(mut slot) = LEASE_LIVENESS.lock() {
        *slot = None;
    }
}

fn installed_lease_liveness() -> Option<LeaseLivenessFn> {
    LEASE_LIVENESS.lock().ok().and_then(|s| s.clone())
}

/// The 30031 status state machine (ADR-0011 Spec 2). Status-less 30031s are
/// `Held` (backward compat — existing reservations are byte-identical).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReservationStatus {
    /// A live exclusive/shared lease (default for status-less 30031s).
    Held,
    /// A declared plan to edit a path set (soft-conflict tier, TTL-bounded).
    Intent,
    /// Holder-released marker (terminal).
    Released,
    /// Operator revocation (A1, terminal).
    Revoked,
}

/// bd-adr-0028-implementation-zfnx.11 — claim fencing epochs.
///
/// One monotonic integer per reservation coordinate decides ownership, so a
/// split-brain reclaim (stale-pane false negative: force-release fires while
/// the old holder is actually alive) yields exactly one loud loser instead
/// of two agents double-running VERIFY. The epoch rides the existing 30031
/// plumbing: it is a content key on lease + marker events (no new store, no
/// new kind, no MCP/CLI surface change).
///
/// Rules: a fresh coordinate claims genesis `0`; EVERY new lease event at a
/// coordinate writes `max_epoch_at + 1` (claims AND reclaims bump — never a
/// silent reset); renewals republish the SAME epoch (a renew is not a
/// claim); every terminal marker stamps the epoch it terminalizes. Legacy
/// epoch-less events read as `0`.
pub fn lease_epoch(content: &str) -> u64 {
    serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .and_then(|v| v.get("epoch").and_then(serde_json::Value::as_u64))
        .unwrap_or(0)
}

/// Lease content with a fencing epoch (acquire / transfer re-acquire).
fn lease_content(exclusive: bool, reason: &str, epoch: u64) -> String {
    format!(
        r#"{{"exclusive":{},"reason":{},"epoch":{}}}"#,
        exclusive,
        serde_json::to_string(reason).unwrap_or_else(|_| "\"\"".into()),
        epoch
    )
}

/// Terminal-marker content with the stamped epoch it terminalizes
/// (release / force-release / reap markers).
fn marker_content(status: &str, heuristic: Option<&str>, epoch: u64) -> String {
    heuristic.map_or_else(
        || {
            format!(
                r#"{{"status":{},"epoch":{}}}"#,
                serde_json::to_string(status).unwrap_or_else(|_| "\"\"".into()),
                epoch
            )
        },
        |h| {
            format!(
                r#"{{"status":{},"heuristic":{},"epoch":{}}}"#,
                serde_json::to_string(status).unwrap_or_else(|_| "\"\"".into()),
                serde_json::to_string(h).unwrap_or_else(|_| "\"\"".into()),
                epoch
            )
        },
    )
}

/// The typed ask in a request-release message (ADR-0011 Spec 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReleaseAsk {
    /// Release the reservation now.
    Release,
    /// Defer the reservation for `defer_secs`.
    Defer { defer_secs: u64 },
}

/// The holder's decision in a request-release response (ADR-0011 Spec 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReleaseDecision {
    /// Released (or will release) the reservation.
    Agreed,
    /// Refused.
    Denied,
    /// Released for `defer_secs`; will re-hold afterwards.
    Deferred { defer_secs: u64 },
}

/// Result of `declare_intent` (ADR-0011 Spec 2).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntentResult {
    pub declared: Vec<GrantedReservation>,
}

/// An active intent (projection view, in-TTL only).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IntentReservation {
    /// The intent event id (the kind-30031 event id of the current intent).
    pub id: String,
    /// Agent name (declarer).
    pub agent_name: String,
    /// Declarer npub (hex).
    pub agent_pubkey: String,
    /// Declared path glob (`project:path-glob` d-tag, project part stripped).
    pub path_pattern: String,
    /// Optional reason.
    pub reason: String,
    /// Declaration time (epoch secs).
    pub declared_at_secs: i64,
    /// Intent expiry (NIP-40 or `declared_at + ttl`, epoch µs).
    pub expires_ts_us: Option<i64>,
    /// The TTL the intent was declared with.
    pub ttl_secs: u64,
}

/// `request_release` (ADR-0011 Spec 6): Crew A asks holder B to release or
/// defer a held reservation via a typed kind-14 DM.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestRelease {
    pub project_slug: String,
    /// The requesting agent (crew A).
    pub sender_name: String,
    /// The holder being asked (crew B).
    pub target_agent: String,
    /// Reservation id = the kind-30031 event id of the held lease.
    pub reservation_id: String,
    pub ask: ReleaseAsk,
    pub note: Option<String>,
}

/// Result of `request_release`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestReleaseResult {
    /// The request message id (the kind-14 rumor id).
    pub message_id: String,
    pub delivered: bool,
    /// The contested path pattern.
    pub path_pattern: String,
    /// The holder asked (crew B).
    pub holder: String,
}

/// Result of `transfer_reservation` (bd-pin-public-plane-s-easy.13).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransferResult {
    /// Path whose lease moved.
    pub path_pattern: String,
    /// Previous holder (released).
    pub from_agent: String,
    /// New holder (acquired).
    pub to_agent: String,
    /// Exclusivity preserved across the transfer.
    pub exclusive: bool,
    /// Reason preserved (or the explicit override).
    pub reason: String,
    /// The released lease's path (mirror of the release breakdown).
    pub released_id: Option<String>,
}

/// `respond_request_release` (ADR-0011 Spec 6): the holder answers the
/// request; the response carries a `decision` tag back to the requester.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReleaseResponse {
    pub project_slug: String,
    /// The responding holder (crew B).
    pub sender_name: String,
    /// The request's message id (the kind-14 rumor id from `request_release`).
    pub request_message_id: String,
    pub decision: ReleaseDecision,
    pub note: Option<String>,
}

/// Resolution of a request-release negotiation (ADR-0011 Spec 6).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RequestReleaseStatus {
    /// Request sent, no response yet, escalation window not elapsed.
    Pending { sent_at_secs: i64, window_secs: u64 },
    /// The holder responded with a decision.
    Responded { decision: ReleaseDecision },
    /// Request unanswered past the tier window → escalate (captain DM).
    EscalationDue { sent_at_secs: i64, window_secs: u64 },
    /// Request message not found (or not authored by the caller).
    Unknown,
}

/// A resolved active reservation (projection view).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Reservation {
    /// The reservation id (the kind-30031 event id of the current lease).
    pub id: String,
    /// Agent name (holder).
    pub agent_name: String,
    /// Holder npub (hex).
    pub agent_pubkey: String,
    /// Reserved path glob (`project:path-glob` d-tag, project part stripped).
    pub path_pattern: String,
    /// Exclusive blocks others; shared does not.
    pub exclusive: bool,
    /// Optional reason.
    pub reason: String,
    /// NIP-40 expiration (µs); `None` = no expiry.
    pub expires_ts_us: Option<i64>,
    /// 30031 status state (ADR-0011 Spec 2). `active_reservations` returns
    /// only `Held`; intents live in [`Mailbox::active_intents`].
    pub status: ReservationStatus,
    /// Fencing epoch (bd-adr-0028-implementation-zfnx.11): monotonic per
    /// coordinate, parsed from the winning event's content (`0` = legacy /
    /// genesis). Mutating ops present the epoch they claimed with; a stale
    /// epoch fails loud via [`Mailbox::check_claim_epoch`].
    pub epoch: u64,
}

/// Result of `file_reservation_paths` (parity shape).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcquireResult {
    pub granted: Vec<GrantedReservation>,
    pub conflicts: Vec<PathConflict>,
    /// Paths NOT granted because an other agent's in-TTL intent overlaps
    /// (ADR-0011 Spec 4 — SOFT conflict: freeze + propose, NOT a hard block).
    /// Absent in AM-Rust (additive; the held tier is unchanged).
    pub soft_conflicts: Vec<PathConflict>,
    /// Detector proposal attached on a soft-conflict FREEZE (P11.3b,
    /// bd-foh8.4): the tiered rule-split for the frozen paths. Opaque JSON —
    /// the caller (flywheel coalition.rs) serializes its own proposal type.
    /// `None` when no freeze fired or no rule applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal: Option<serde_json::Value>,
}

/// bd-reservations-not-projected-5ohc: one granted/renewed lease to mirror
/// into the durable `file_reservations` SQLite projection.
struct LeaseUpsert<'a> {
    project_slug: &'a str,
    agent_name: &'a str,
    agent_pubkey: &'a str,
    path_pattern: &'a str,
    exclusive: bool,
    reason: &'a str,
    event_id: &'a str,
    expires_ts_us: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GrantedReservation {
    pub id: String,
    pub path_pattern: String,
    pub exclusive: bool,
    pub reason: String,
    pub expires_ts_us: Option<i64>,
    /// Fencing epoch (bd-adr-0028-implementation-zfnx.11). Internal
    /// plumbing only — deliberately NOT in the MCP `ToJson` wire shape, so
    /// the 38-parity catalog stays byte-identical (no regen, and the served
    /// count stays 41 — 38 parity + 3 additive).
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PathConflict {
    pub path: String,
    pub holders: Vec<String>,
    /// The kind-30031 event id of each conflicting lease (bd-dsu1): the
    /// operator lock board needs the reservation id for diagnostics
    /// (force-release, request-release, replay) without DB access. Parallel
    /// to `holders` — index-safe, one id per holder. Omitted from JSON when
    /// empty (additive, never breaks old consumers).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reservation_ids: Vec<String>,
}

/// Result of `check_file_reservation_conflicts` (parity shape).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConflictReport {
    /// True when no HARD conflicts (held-tier). SOFT conflicts (intents) do
    /// not block — see `has_soft_conflicts`.
    pub conflict_free: bool,
    /// Hard conflicts (other agents' held exclusive leases).
    pub conflicts: Vec<PathConflict>,
    pub clear_paths: Vec<String>,
    pub checked_paths: usize,
    pub total_conflicting_reservations: usize,
    /// Soft conflicts — ADR-0011 Spec 4: other agents' in-TTL intents, and
    /// (bd-4dl4) their live SHARED leases. Neither blocks; both mean "someone
    /// is here". A path appears in `clear_paths` only when nobody holds AND
    /// nobody declared it.
    pub soft_conflicts: Vec<PathConflict>,
    /// True when any path has a soft conflict.
    pub has_soft_conflicts: bool,
    /// bd-slot-visibility-gv17: ACTIVE build-slot leases held by OTHERS
    /// (the caller's own slots are excluded — own never conflicts). Lets a
    /// pre-write check answer "is the cargo fence free?" in ONE call.
    #[serde(default)]
    pub slot_holders: Vec<SlotHolder>,
}

/// One active build-slot lease as seen by a pre-write collision check
/// (bd-slot-visibility-gv17).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SlotHolder {
    pub slot: String,
    pub agent_name: String,
    pub exclusive: bool,
    pub expires_ts_us: Option<i64>,
}

/// Result of `renew_file_reservations`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RenewResult {
    pub renewed: usize,
    pub reservations: Vec<GrantedReservation>,
}

/// Result of `release_file_reservations` (S6: per-path breakdown).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReleaseResult {
    pub released: usize,
    pub leases: Vec<ReleasedLease>,
}

/// A single released lease (S6, bd-release-breakdown-xom3).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReleasedLease {
    /// Path glob of the released lease.
    pub path: String,
    /// "exclusive" | "shared".
    pub mode: String,
    /// Agent name of the holder (the releaser).
    pub holder: String,
}

/// The live leases a bead-close event for `bead_id` releases
/// (bd-close-releases-leases-qkbo). Exact reason match, never a prefix:
/// a close of `bd-1` must not release a lease reasoned `bd-10`. An empty
/// `bead_id` releases nothing, since it would otherwise match every
/// reasonless lease.
///
/// Pure decision half of release-on-close: [`Mailbox::release_by_reason`]
/// executes the revocation, while the audit surfaces (`reserve list`'s stale
/// marker and `reserve sweep --closed-beads`) route through this, so the
/// sweep and any host hook can never disagree about what a close releases.
pub fn release_on_close_event<'a>(
    leases: &'a [Reservation],
    bead_id: &str,
) -> Vec<&'a Reservation> {
    if bead_id.is_empty() {
        return Vec::new();
    }
    leases
        .iter()
        .filter(|lease| lease.reason == bead_id)
        .collect()
}

/// Stale-lease audit (bd-close-releases-leases-qkbo): every live lease whose
/// reason names one of `closed_beads`. Folded from
/// [`release_on_close_event`] over the closed set, so the stale view and the
/// close-driven release view are one predicate by construction. The caller
/// supplies the closed set it observed (the CLI reads a repo's
/// `.beads/issues.jsonl`); this never reads a tracker itself.
pub fn stale_closed_leases<'a>(
    leases: &'a [Reservation],
    closed_beads: &std::collections::BTreeSet<String>,
) -> Vec<&'a Reservation> {
    closed_beads
        .iter()
        .flat_map(|bead_id| release_on_close_event(leases, bead_id))
        .collect()
}

/// Probe inputs for one kernel acquire decision (travel together).
struct AcquireProbe {
    holder_hex: String,
    path: String,
    exclusive: bool,
    expires_us: Option<i64>,
    epoch: u64,
    now_us: i64,
}

impl Mailbox {
    /// `file_reservation_paths` (acquire): request leases on `paths`. Conflict
    /// is checked symmetrically against OTHER agents' active reservations; own
    /// never conflicts. Two tiers (ADR-0011 Spec 4):
    /// - other's held exclusive lease → HARD conflict → blocked (as before);
    /// - other's in-TTL intent → SOFT conflict → frozen (not granted, listed
    ///   in `soft_conflicts`, NOT a hard block).
    ///
    /// Granted leases are published as kind-30031 events. An acquire on the
    ///   acquirer's OWN intent converts it to held (same `(kind,d)` coordinate,
    ///   same author → NIP-33 supersession — must-convert, Spec 2).
    pub async fn file_reservation_paths(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        ttl_secs: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
    ) -> anyhow::Result<AcquireResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mut result = AcquireResult::default();

        for path in paths {
            // Decided by the kernel (bd-y6t7): build once per call, tick
            // per path. Granted publishes below; Denied maps to hard/soft.
            let now_us = Self::kernel_now_us();
            let (model, meta) = self.kernel_model_for(project_slug).await;
            let ttl_us = ttl_secs.unwrap_or(DEFAULT_RESERVATION_TTL_SECS);
            let blockers = Self::kernel_blockers_for(
                &model,
                &meta,
                &AcquireProbe {
                    holder_hex: agent_keys.public_key().to_hex(),
                    path: path.clone(),
                    exclusive,
                    expires_us: Some(now_us + ttl_us as i64 * 1_000_000),
                    epoch: 0,
                    now_us,
                },
            );
            let (hard, hard_ids, soft, soft_ids) = Self::split_blockers(&blockers);
            if !hard.is_empty() {
                result.conflicts.push(PathConflict {
                    path: path.clone(),
                    holders: hard,
                    reservation_ids: hard_ids,
                });
                continue;
            }
            if !soft.is_empty() {
                // Freeze + propose (Spec 4): not a hard block, not granted.
                // The proposer/detector consumer (P11.3b) fires on this.
                result.soft_conflicts.push(PathConflict {
                    path: path.clone(),
                    holders: soft,
                    reservation_ids: soft_ids,
                });
                continue;
            }

            // Publish the lease: NIP-40 expiration (TTL) + dual-emit scoping.
            let d = format!("{project_slug}:{path}");
            let base = vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["reason", reason.unwrap_or("")]).expect("reason"),
            ];
            let mut tags =
                with_project_scoping(toron_kind(FILE_RESERVATION), base, Some(project_slug));
            let ttl = EnvelopeTtl {
                secs: Some(ttl_secs.unwrap_or(DEFAULT_RESERVATION_TTL_SECS)),
            };
            let created = Timestamp::now();
            if let Some(exp) = ttl.expiration_tag(created) {
                tags.push(exp);
            }
            // bd-adr-0028-implementation-zfnx.11: every new lease bumps the coordinate epoch (fresh
            // claim, must-convert, or post-release/revoke reclaim — the
            // previous holder's presented epoch goes stale by construction).
            let d_epoch = format!("{project_slug}:{path}");
            let epoch = self.next_epoch_at(project_slug, &d_epoch).await;
            let content = lease_content(exclusive, reason.unwrap_or(""), epoch);
            let event = build_event(
                &agent_keys,
                toron_kind(FILE_RESERVATION),
                content,
                tags,
                created,
            )?;
            let ok = self.bus.publish(event.clone()).await?;
            if !ok.stored {
                anyhow::bail!("reservation event rejected by bus");
            }
            self.project_lease_upsert(LeaseUpsert {
                project_slug,
                agent_name,
                agent_pubkey: &agent_keys.public_key().to_hex(),
                path_pattern: path,
                exclusive,
                reason: reason.unwrap_or(""),
                event_id: &event.id.to_hex(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
            })
            .await;
            result.granted.push(GrantedReservation {
                id: event.id.to_hex(),
                path_pattern: path.clone(),
                exclusive,
                reason: reason.unwrap_or("").to_string(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
                epoch,
            });
        }
        Ok(result)
    }

    /// `declare_intent` (ADR-0011 Spec 2): publish a `status:intent` 30031 for
    /// each path — a declared plan to edit it BEFORE acquiring. Intents are
    /// advisory + TTL-bounded (auto-expire after `ttl_secs`, default
    /// [`INTENT_TTL_SECS`]) unless converted to held by the declarer's own
    /// acquire. Declaring against a held lease is allowed (request-release
    /// exists exactly for that path).
    pub async fn declare_intent(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        reason: Option<&str>,
        ttl_secs: Option<u64>,
    ) -> anyhow::Result<IntentResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mut result = IntentResult::default();
        for path in paths {
            let d = format!("{project_slug}:{path}");
            let ttl = ttl_secs.unwrap_or(INTENT_TTL_SECS);
            let mut tags = with_project_scoping(
                toron_kind(FILE_RESERVATION),
                vec![
                    Tag::parse(["d", &d]).expect("d"),
                    Tag::parse(["status", "intent"]).expect("status"),
                    Tag::parse(["reason", reason.unwrap_or("")]).expect("reason"),
                ],
                Some(project_slug),
            );
            let created = Timestamp::now();
            if let Some(exp) = (EnvelopeTtl { secs: Some(ttl) }).expiration_tag(created) {
                tags.push(exp);
            }
            let content = format!(
                r#"{{"status":"intent","exclusive":true,"reason":{}}}"#,
                serde_json::to_string(reason.unwrap_or("")).expect("reason json")
            );
            let event = build_event(
                &agent_keys,
                toron_kind(FILE_RESERVATION),
                content,
                tags,
                created,
            )?;
            let ok = self.bus.publish(event.clone()).await?;
            if !ok.stored {
                anyhow::bail!("intent event rejected by bus");
            }
            result.declared.push(GrantedReservation {
                id: event.id.to_hex(),
                path_pattern: path.clone(),
                exclusive: true,
                reason: reason.unwrap_or("").to_string(),
                expires_ts_us: (EnvelopeTtl { secs: Some(ttl) })
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
                // bd-adr-0028-implementation-zfnx.11: intents are advisory, not ownership — they
                // neither read nor bump the coordinate epoch. The
                // must-convert acquire bumps past them.
                epoch: 0,
            });
        }
        Ok(result)
    }

    /// `check_file_reservation_conflicts`: symmetric-glob conflict report for
    /// paths vs OTHER agents' active reservations, split into hard (held) and
    /// soft (in-TTL intent) tiers (ADR-0011 Spec 4). `conflict_free` keeps its
    /// contract = no HARD conflicts; `has_soft_conflicts` reports the advisory
    /// tier (soft conflicts never hard-block).
    pub async fn check_file_reservation_conflicts(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
    ) -> anyhow::Result<ConflictReport> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mut report = ConflictReport {
            checked_paths: paths.len(),
            ..Default::default()
        };
        for path in paths {
            // Dry-run decision through the kernel (bd-y6t7): no publish,
            // same decider as the acquire path.
            let now_us = Self::kernel_now_us();
            let (model, meta) = self.kernel_model_for(project_slug).await;
            let blockers = Self::kernel_blockers_for(
                &model,
                &meta,
                &AcquireProbe {
                    holder_hex: agent_keys.public_key().to_hex(),
                    path: path.clone(),
                    exclusive: true,
                    expires_us: None,
                    epoch: 0,
                    now_us,
                },
            );
            let (hard, hard_ids, soft, soft_ids) = Self::split_blockers(&blockers);

            // bd-4dl4: a live SHARED lease by another agent blocks nobody, so
            // the kernel above returns no blocker for it and the path would
            // land in `clear_paths` with nothing else set — the reader
            // concludes nobody is there when someone declared they are. Fold
            // those holders into the ADVISORY tier: `conflict_free` stays true
            // (a shared lease genuinely does not fence anyone, ADR-0011 Spec
            // 4), but the path is no longer reported as bare clear, so
            // `clear_paths` means "nobody holds or declared this".
            //
            // Only the shared ones are added here; the exclusive ones are
            // already in `hard` and adding them again would double-count
            // `total_conflicting_reservations`.
            let mut soft = soft;
            let mut soft_ids = soft_ids;
            for id in reservations_core::advisory_viewers(
                &model,
                &agent_keys.public_key().to_hex(),
                path,
                now_us,
            ) {
                if hard_ids.contains(&id) {
                    continue;
                }
                if let Some((name, ReservationStatus::Held)) = meta.get(&id) {
                    soft.push(name.clone());
                    soft_ids.push(id);
                }
            }

            if hard.is_empty() && soft.is_empty() {
                report.clear_paths.push(path.clone());
            } else {
                if !hard.is_empty() {
                    report.conflicts.push(PathConflict {
                        path: path.clone(),
                        holders: hard,
                        reservation_ids: hard_ids,
                    });
                }
                if !soft.is_empty() {
                    report.soft_conflicts.push(PathConflict {
                        path: path.clone(),
                        holders: soft,
                        reservation_ids: soft_ids,
                    });
                }
            }
        }
        report.conflict_free = report.conflicts.is_empty();
        report.has_soft_conflicts = !report.soft_conflicts.is_empty();
        report.total_conflicting_reservations =
            report.conflicts.iter().map(|c| c.holders.len()).sum();
        // bd-slot-visibility-gv17: ACTIVE build-slot leases held by others —
        // the pre-write check answers "is the cargo fence free?" in ONE call.
        report.slot_holders = match self.index_pool() {
            Some(pool) => crate::index::slots::active_slots(pool, project_slug)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|s| s.agent_pubkey != agent_keys.public_key().to_hex())
                .map(|s| SlotHolder {
                    slot: s.slot,
                    agent_name: s.agent_name,
                    exclusive: s.exclusive,
                    expires_ts_us: s.expires_ts_us,
                })
                .collect(),
            None => Vec::new(),
        };
        Ok(report)
    }

    /// `renew_file_reservations`: converge the holder's active leases to ≥
    /// `extend_secs` of remaining TTL (same `(kind,d)` coordinate →
    /// supersedes per author). `renewed` counts EVERY live matching lease
    /// after the call: freshly republished ones AND already-healthy ones —
    /// a healthy lease is covered by the operation (its expiry already
    /// guarantees the runway), so `renewed` equals the holder's live lease
    /// set. Before bd-mbh0 the healthy window was excluded and callers read
    /// `renewed:0` as failure while leases lapsed un-renewed.
    ///
    /// bd-evqs renewal cap (unchanged): a lease whose remaining TTL is
    /// ALREADY ≥ `extend_secs` is NOT republished — zero events for the
    /// healthy window. The durable loop heartbeat renews every tick; without
    /// the cap it re-publishes every lease each tick — a single d-tag
    /// accumulated 1218 renewal events over ~10h (the reservation renewal
    /// storm, holder inactive, owning bead closed). Republishing only when
    /// remaining < extend_secs keeps the lease healthy in [extend,
    /// 2×extend) forever while collapsing the churn to ~1 event per extend
    /// window per lease. Extension math is ABSOLUTE (bd-XXX pin below): new
    /// expiry = old expiry + extend_secs, never now-derived. Permanent
    /// leases (no expiry) are reported but never republished (they never
    /// lapse) — and counted: the holder's set is fully converged after the
    /// call.
    pub async fn renew_file_reservations(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        extend_secs: u64,
    ) -> anyhow::Result<RenewResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let now_secs = Timestamp::now().as_secs() as i64;
        let mine = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .filter(|r| {
                r.agent_pubkey == agent_keys.public_key().to_hex()
                    && (paths.is_empty() || paths.iter().any(|p| p == &r.path_pattern))
            });
        let mut result = RenewResult::default();
        for res in mine {
            let d = format!("{project_slug}:{}", res.path_pattern);
            // bd-evqs cap: no republish when the lease still has ≥
            // extend_secs of TTL left (or is permanent) — zero events for
            // the healthy window. bd-mbh0: the lease is still COUNTED as
            // renewed (its expiry is already converged ≥ the requested
            // runway) and REPORTED so the embedding's closed-bead sweep can
            // see + release it: a reservation whose owning bead closed is
            // stale even while its TTL is healthy.
            let old_expiry_secs = res.expires_ts_us.map(|t| t / 1_000_000);
            // permanent lease (no expiry) needs no renewal runway — converged.
            let skip = old_expiry_secs
                .is_none_or(|abs| abs.saturating_sub(now_secs) >= extend_secs as i64);
            if skip {
                result.reservations.push(GrantedReservation {
                    id: res.id.clone(),
                    path_pattern: res.path_pattern.clone(),
                    exclusive: res.exclusive,
                    reason: res.reason.clone(),
                    expires_ts_us: res.expires_ts_us,
                    epoch: res.epoch,
                });
                result.renewed += 1;
                continue;
            }
            let base = vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["reason", &res.reason]).expect("reason"),
            ];
            let mut tags =
                with_project_scoping(toron_kind(FILE_RESERVATION), base, Some(project_slug));
            let created = Timestamp::now();
            // Renewal: the new lease must expire at the OLD absolute expiry PLUS
            // the extension, in absolute epoch seconds — not re-derived from
            // `now`. Treating `expires_ts_us` (absolute epoch µs) as a relative
            // duration and re-adding to `now` double-counts the elapsed time
            // and produced a 2x expiry (parity bug bd-XXX).
            let new_expiry_secs = old_expiry_secs
                .map(|abs| (abs as u64).saturating_add(extend_secs))
                .unwrap_or_else(|| created.as_secs().saturating_add(extend_secs));
            let ttl = EnvelopeTtl {
                secs: Some(new_expiry_secs.saturating_sub(created.as_secs())),
            };
            if let Some(exp) = ttl.expiration_tag(created) {
                tags.push(exp);
            }
            let event = build_event(
                &agent_keys,
                toron_kind(FILE_RESERVATION),
                // bd-adr-0028-implementation-zfnx.11: a renew is not a claim — republish the SAME
                // epoch so the holder's fencing belief survives renewal.
                lease_content(res.exclusive, &res.reason, res.epoch),
                tags,
                created,
            )?;
            self.bus.publish(event.clone()).await?;
            self.project_lease_upsert(LeaseUpsert {
                project_slug,
                agent_name,
                agent_pubkey: &agent_keys.public_key().to_hex(),
                path_pattern: &res.path_pattern,
                exclusive: res.exclusive,
                reason: &res.reason,
                event_id: &event.id.to_hex(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
            })
            .await;
            result.reservations.push(GrantedReservation {
                id: event.id.to_hex(),
                path_pattern: res.path_pattern.clone(),
                exclusive: res.exclusive,
                reason: res.reason.clone(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
                epoch: res.epoch,
            });
            result.renewed += 1;
        }
        Ok(result)
    }

    /// `release_file_reservations`: the holder releases own leases (publishes
    /// a `status:released` marker; same-author NIP-33 supersession). Response
    /// carries the per-path breakdown (S6).
    pub async fn release_file_reservations(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
    ) -> anyhow::Result<ReleaseResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mine: Vec<Reservation> = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .filter(|r| {
                r.agent_pubkey == agent_keys.public_key().to_hex()
                    && (paths.is_empty() || paths.iter().any(|p| p == &r.path_pattern))
            })
            .collect();
        let mut released = 0usize;
        let mut leases = Vec::new();
        for res in mine {
            let d = format!("{project_slug}:{}", res.path_pattern);
            let base = vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["status", "released"]).expect("status"),
            ];
            let tags = with_project_scoping(toron_kind(FILE_RESERVATION), base, Some(project_slug));
            let event = build_event(
                &agent_keys,
                toron_kind(FILE_RESERVATION),
                // bd-adr-0028-implementation-zfnx.11: stamp the terminalized epoch so the next claim
                // bumps past it (reclaim invalidates the old belief).
                marker_content("released", None, res.epoch),
                tags,
                Timestamp::now(),
            )?;
            self.bus.publish(event.clone()).await?;
            self.project_lease_status(
                project_slug,
                &res.agent_pubkey,
                &res.path_pattern,
                "released",
            )
            .await;
            self.record_transition(project_slug, &d, "released", &event.id);
            leases.push(ReleasedLease {
                path: res.path_pattern.clone(),
                mode: if res.exclusive { "exclusive" } else { "shared" }.into(),
                holder: res.agent_name.clone(),
            });
            released += 1;
        }
        Ok(ReleaseResult { released, leases })
    }

    /// `force_release_file_reservation` (G5 A1): staleness heuristic → the
    /// OPERATOR signs a kind-30031 `status:revoked` at the target coordinate +
    /// `e`/`a` refs. Projection key-priority resolves it; no NIP-09.
    ///
    /// The target is resolved by **path** (+ optional `holder` name) via the
    /// SAME `active_reservations` view `check_file_reservation_conflicts` uses
    /// (bd-cumj). An operator force-releasing a stale lock held by ANOTHER
    /// agent cannot know that holder's event id (only the holder received it on
    /// acquire), and the daemon's resolved winner id is the source of truth —
    /// not a caller-supplied id that may diverge from the resolved one. So the
    /// revocation always uses the daemon's own resolved lease id.
    pub async fn force_release_file_reservation(
        &self,
        project_slug: &str,
        requestor_name: &str,
        path: &str,
        holder: Option<&str>,
        note: Option<&str>,
        notify_previous: bool,
    ) -> anyhow::Result<()> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let operator = self
            .operator
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no operator key configured"))?;

        // Locate the lease by path (+ optional holder) through the same view
        // conflict-check reads — never a caller-supplied event id.
        let lease = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .filter(|r| overlaps(&r.path_pattern, path))
            .find(|r| holder.is_none_or(|h| r.agent_name == h))
            .ok_or_else(|| {
                let where_ = holder.map(|h| format!(" held by {h}")).unwrap_or_default();
                anyhow::anyhow!("no active reservation on {path}{where_}")
            })?;

        // 1. Staleness heuristic.
        let stale = self.is_holder_stale(project_slug, &lease).await;
        if !stale {
            anyhow::bail!(
                "reservation on {} ({} by {}) not stale (holder active) — force-release denied",
                lease.path_pattern,
                lease.id,
                lease.agent_name
            );
        }

        // 2. Operator signs the revocation at the SAME (kind, d) coordinate.
        let d = format!("{project_slug}:{}", lease.path_pattern);
        let target = EventId::from_hex(&lease.id).unwrap_or_else(|_| {
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id")
        });
        let mut tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["status", "revoked"]).expect("status"),
                Tag::event(target),
                Tag::parse([
                    "a",
                    &format!("30031:{}:{d}", operator.public_key().to_hex()),
                ])
                .expect("a"),
            ],
            Some(project_slug),
        );
        if let Some(n) = note {
            tags.push(Tag::parse(["note", n]).expect("note"));
        }
        let revoke = build_event(
            operator,
            toron_kind(FILE_RESERVATION),
            // bd-adr-0028-implementation-zfnx.11: stamp the revoked lease's epoch — the next claim
            // bumps past it, the old holder's belief goes stale by construction.
            marker_content("revoked", Some("stale-holder"), lease.epoch),
            tags,
            Timestamp::now(),
        )?;
        assert!(is_revocation(&revoke), "revocation must be detectable");
        self.bus.publish(revoke.clone()).await?;
        self.project_lease_status(
            project_slug,
            &lease.agent_pubkey,
            &lease.path_pattern,
            "revoked",
        )
        .await;
        self.record_transition(project_slug, &d, "revoked", &revoke.id);

        // 5. Notify previous holder via NIP-17 (heuristic summary).
        if notify_previous {
            let holder_keys = Keys::generate(); // sender key for the wrap
            let holder_npub = PublicKey::from_hex(&lease.agent_pubkey)
                .map_err(|_| anyhow::anyhow!("bad holder npub"))?;
            let rumor = crate::nostr::gift_wrap::build_rumor(
                holder_keys.public_key(),
                Timestamp::now(),
                crate::nostr::Kind::PrivateDirectMessage,
                vec![
                    Tag::parse(["toron-kind", "reservation-revoked"]).expect("kind"),
                    Tag::parse(["p", &lease.agent_pubkey]).expect("p"),
                    Tag::parse(["reason", "stale-holder"]).expect("reason"),
                    Tag::parse(["reservation-id", &lease.id]).expect("res"),
                ],
                format!(
                    "reservation {} ({} by {}) force-released by operator{}",
                    lease.path_pattern,
                    lease.agent_name,
                    requestor_name,
                    note.map(|n| format!(" — {n}")).unwrap_or_default()
                ),
            );
            let wrap = crate::nostr::gift_wrap::gift_wrap(&holder_keys, &holder_npub, rumor)
                .expect("wrap notify");
            // F2.SILENT-DROP (batch-fix): a failed force-release notification
            // DM must be loud — the force-released holder is entitled to know.
            if let Err(e) = self.publish_dm(&wrap).await {
                tracing::warn!(
                    d,
                    "force-release notification DM to {holder_npub} failed: {e}"
                );
            }
        }
        Ok(())
    }

    /// bd-gc7k: revoke every EXCLUSIVE held lease whose holder pin is in
    /// `dead_holders` — the prune list from `cleanup_pane_identities`
    /// (pins whose herdr panes are gone). A dead pin must not own the
    /// ledger: the pane-death query IS the freshness evidence, so unlike
    /// [`Self::force_release_file_reservation`] there is NO staleness
    /// heuristic. Revocations are operator-signed kind-30031
    /// `status:revoked` events at the same `(kind, d)` coordinate, the
    /// durable projection is mirrored, and the transition is archived.
    ///
    /// Only holders NAMED in the prune list are reachable here — live
    /// holders never appear in it, so a healthy peer can never be reaped.
    /// Projects are enumerated from lease rows' `d_tag` prefix (the same
    /// coordinate [`Self::resolved_leases`] splits). `Ok(vec![])` is the
    /// clean no-op; per-project failures are best-effort skips.
    pub async fn reap_ghost_leases(
        &self,
        dead_holders: &[String],
        note: Option<&str>,
    ) -> anyhow::Result<Vec<GhostRelease>> {
        let mut out = Vec::new();
        if dead_holders.is_empty() {
            return Ok(out);
        }
        let operator = self.operator.as_ref().ok_or_else(|| {
            anyhow::anyhow!("no operator key configured — cannot reap ghost leases")
        })?;
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(out);
        };
        let projects: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT substr(d_tag, 1, instr(d_tag, ':') - 1) FROM bus_events \
             WHERE kind = ?1 AND d_tag IS NOT NULL AND instr(d_tag, ':') > 1",
        )
        .bind(i64::from(toron_kind(FILE_RESERVATION).as_u16()))
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        for project_slug in projects {
            let held = match self.active_reservations(&project_slug).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            for lease in held {
                if !lease.exclusive || !dead_holders.iter().any(|h| h == &lease.agent_name) {
                    continue;
                }
                let agent_pubkey = lease.agent_pubkey.clone();
                let path_pattern = lease.path_pattern.clone();
                let holder = lease.agent_name.clone();
                let d = format!("{project_slug}:{path_pattern}");
                let target = EventId::from_hex(&lease.id).unwrap_or_else(|_| {
                    EventId::from_hex(
                        "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
                    )
                    .expect("id")
                });
                let mut tags = with_project_scoping(
                    toron_kind(FILE_RESERVATION),
                    vec![
                        Tag::parse(["d", &d]).expect("d"),
                        Tag::parse(["status", "revoked"]).expect("status"),
                        Tag::event(target),
                        Tag::parse([
                            "a",
                            &format!("30031:{}:{d}", operator.public_key().to_hex()),
                        ])
                        .expect("a"),
                    ],
                    Some(&project_slug),
                );
                tags.push(
                    Tag::parse(["note", note.unwrap_or("ghost-pane-cleanup")]).expect("note"),
                );
                let revoke = build_event(
                    operator,
                    toron_kind(FILE_RESERVATION),
                    // bd-adr-0028-implementation-zfnx.11: stamp the terminalized epoch.
                    marker_content("revoked", Some("ghost-pane"), lease.epoch),
                    tags,
                    Timestamp::now(),
                )?;
                self.bus.publish(revoke.clone()).await?;
                self.project_lease_status(&project_slug, &agent_pubkey, &path_pattern, "revoked")
                    .await;
                self.record_transition(&project_slug, &d, "revoked", &revoke.id);
                out.push(GhostRelease {
                    project_slug: project_slug.clone(),
                    path_pattern,
                    holder,
                });
            }
        }
        Ok(out)
    }

    /// `release_by_reason` (bd-h3oj): operator-signed revocation of every
    /// live lease whose `reason` tag equals the bead id. Flywheel's
    /// after-close hook calls this the moment a watched bead closes, so its
    /// file reservations never linger to TTL.
    ///
    /// Unlike `force_release_file_reservation` there is NO staleness
    /// heuristic: the bead closing IS the evidence the guarded work is done,
    /// and the holder may still be active on another bead. The revocations
    /// reuse the force-release mechanics (kind-30031 `status:revoked` at the
    /// same `(kind, d)` coordinate + `e`/`a` refs, operator priority wins,
    /// NIP-09 never the mechanism). `released == 0` is a clean no-op, never
    /// an error — the loop hook is idempotent.
    pub async fn release_by_reason(
        &self,
        project_slug: &str,
        requestor_name: &str,
        bead_id: &str,
        notify_previous: bool,
    ) -> anyhow::Result<ReleaseResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let operator = self
            .operator
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no operator key configured"))?;
        let leases: Vec<Reservation> = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .filter(|r| r.reason == bead_id)
            .collect();

        let mut released = 0usize;
        let mut released_leases = Vec::new();
        for lease in leases {
            let d = format!("{project_slug}:{}", lease.path_pattern);
            let target = EventId::from_hex(&lease.id).unwrap_or_else(|_| {
                EventId::from_hex(
                    "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
                )
                .expect("id")
            });
            let tags = with_project_scoping(
                toron_kind(FILE_RESERVATION),
                vec![
                    Tag::parse(["d", &d]).expect("d"),
                    Tag::parse(["status", "revoked"]).expect("status"),
                    Tag::event(target),
                    Tag::parse([
                        "a",
                        &format!("30031:{}:{d}", operator.public_key().to_hex()),
                    ])
                    .expect("a"),
                    Tag::parse(["note", &format!("bead-closed:{bead_id}")]).expect("note"),
                ],
                Some(project_slug),
            );
            let revoke = build_event(
                operator,
                toron_kind(FILE_RESERVATION),
                // bd-adr-0028-implementation-zfnx.11: stamp the terminalized epoch.
                marker_content("revoked", Some("bead-closed"), lease.epoch),
                tags,
                Timestamp::now(),
            )?;
            assert!(is_revocation(&revoke), "revocation must be detectable");
            self.bus.publish(revoke.clone()).await?;
            self.project_lease_status(
                project_slug,
                &lease.agent_pubkey,
                &lease.path_pattern,
                "revoked",
            )
            .await;
            self.record_transition(project_slug, &d, "revoked", &revoke.id);
            released += 1;

            // Notify the previous holder via NIP-17 (the lease was revoked
            // by bead close, not by them).
            if notify_previous {
                let holder_keys = Keys::generate(); // sender key for the wrap
                let holder_npub = PublicKey::from_hex(&lease.agent_pubkey)
                    .map_err(|_| anyhow::anyhow!("bad holder npub"))?;
                let rumor = crate::nostr::gift_wrap::build_rumor(
                    holder_keys.public_key(),
                    Timestamp::now(),
                    crate::nostr::Kind::PrivateDirectMessage,
                    vec![
                        Tag::parse(["toron-kind", "reservation-revoked"]).expect("kind"),
                        Tag::parse(["p", &lease.agent_pubkey]).expect("p"),
                        Tag::parse(["reason", bead_id]).expect("reason"),
                        Tag::parse(["reservation-id", &lease.id]).expect("res"),
                    ],
                    format!(
                        "reservation {} ({} by {}) released because bead {} closed",
                        lease.path_pattern, lease.agent_name, requestor_name, bead_id
                    ),
                );
                let wrap = crate::nostr::gift_wrap::gift_wrap(&holder_keys, &holder_npub, rumor)
                    .expect("wrap notify");
                // F2.SILENT-DROP: a failed release notification DM must be
                // loud — the released holder is entitled to know.
                if let Err(e) = self.publish_dm(&wrap).await {
                    tracing::warn!(
                        d,
                        "release-by-reason notification DM to {holder_npub} failed: {e}"
                    );
                }
            }
            released_leases.push(ReleasedLease {
                path: lease.path_pattern.clone(),
                mode: if lease.exclusive {
                    "exclusive"
                } else {
                    "shared"
                }
                .into(),
                holder: lease.agent_name.clone(),
            });
        }
        Ok(ReleaseResult {
            released,
            leases: released_leases,
        })
    }

    /// Resolve a coordinate's current state with tombstone semantics: a
    /// revocation/release marker covers only events OLDER than itself, so a
    /// lease acquired after a force-release or release-by-reason is visible
    /// again — the release unlocked the path (bd-reacquire regression).
    /// Without any marker, plain key-priority resolution applies.
    fn resolve_lease_state<'a>(evs: Vec<&'a Event>, operator: Option<&PublicKey>) -> Option<Event> {
        let newest_marker = evs
            .iter()
            .filter(|e| {
                matches!(
                    lease_status(e),
                    ReservationStatus::Revoked | ReservationStatus::Released
                )
            })
            .max_by(|a, b| a.created_at.as_secs().cmp(&b.created_at.as_secs()));
        let Some(marker) = newest_marker else {
            return resolve_coordinate(evs, operator);
        };
        let cutoff = marker.created_at.as_secs();
        let newer: Vec<&'a Event> = evs
            .iter()
            .filter(|e| e.created_at.as_secs() > cutoff)
            .copied()
            .collect();
        if newer.is_empty() {
            return Some((*marker).clone());
        }
        resolve_coordinate(newer, operator)
    }

    /// Active (non-revoked, non-released, non-expired) kind-30031 events in a
    /// project, resolved with operator priority (bd-toh) and classified by
    /// status. Both held leases and intents flow through here — the single
    /// replay feeds every consumer (held view, intent board, conflict view).
    pub(crate) async fn resolved_leases(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<ResolvedLease>> {
        let filter = kind_project_filter(toron_kind(FILE_RESERVATION), project_slug);
        let events = collect_replay(self.bus.as_ref(), filter).await?;
        let operator = self.operator.as_ref().map(nostr::key::Keys::public_key);
        let now_secs = Timestamp::now().as_secs() as i64;
        let mut out = Self::resolve_leases_from_events(&events, operator, now_secs);

        // bd-lease-liveness-bo0g: the bus prunes EXPIRED envelopes on replay
        // (NIP-40), so an expired-but-still-blocking lease is invisible to
        // the event view above. Its SQL MIRROR row persists (`status=active`,
        // `expires_ts_us` past) — resurrect that row ONLY while a liveness
        // oracle vouches the holder's session is still live. No oracle →
        // pure NIP-40 semantics (expired stays invisible), byte-identical to
        // pre-bo0g behavior.
        let liveness = installed_lease_liveness();
        if let Some(pool) = self.index_pool() {
            let now_us = now_secs * 1_000_000;
            let expired_mirrors: Vec<(String, String, String, i64, i64, Option<String>)> =
                sqlx::query_as(
                    "SELECT agent_pubkey, path_pattern, COALESCE(reason,''), exclusive, \
                     expires_ts_us, event_id \
                     FROM file_reservations \
                     WHERE project_slug = ?1 AND status = 'active' \
                       AND expires_ts_us IS NOT NULL AND expires_ts_us < ?2",
                )
                .bind(project_slug)
                .bind(now_us)
                .fetch_all(pool)
                .await
                .unwrap_or_default();
            for (pubkey, path, reason, excl, exp_us, event_id) in expired_mirrors {
                let dup = out
                    .iter()
                    .any(|l| l.agent_pubkey == pubkey && l.path_pattern == path && !l.expired);
                if dup || !liveness.as_ref().is_some_and(|f| f(&pubkey)) {
                    continue;
                }
                out.push(ResolvedLease {
                    id: event_id.unwrap_or_else(|| format!("mirror:{path}:{pubkey}")),
                    agent_pubkey: pubkey,
                    path_pattern: path,
                    exclusive: excl != 0,
                    reason,
                    expires_ts_us: Some(exp_us),
                    status: ReservationStatus::Held,
                    created_at_secs: 0,
                    // bd-adr-0028-implementation-zfnx.11: the oracle-resurrected mirror row predates
                    // epoch tracking (no new store column); fencing treats it
                    // as genesis — a fenced op must re-read, never assume.
                    epoch: 0,
                    expired: true,
                    liveness_extended: true,
                });
            }
        }
        Ok(out)
    }

    /// Resolve lease state from raw kind-30031 events (any project scope).
    /// Group by (kind, d) coordinate; resolve latest per priority.
    fn resolve_leases_from_events(
        events: &[Event],
        operator: Option<PublicKey>,
        now_secs: i64,
    ) -> Vec<ResolvedLease> {
        let mut by_d: std::collections::HashMap<String, Vec<&Event>> =
            std::collections::HashMap::new();
        for ev in events {
            if let Some(d) = ev
                .tags
                .iter()
                .find_map(|t| (t.kind() == "d").then(|| t.content()))
                .flatten()
            {
                by_d.entry(d.to_string()).or_default().push(ev);
            }
        }
        let mut out = Vec::new();
        for (d, evs) in by_d {
            let Some(winning) = Self::resolve_lease_state(evs, operator.as_ref()) else {
                continue;
            };
            let status = lease_status(&winning);
            if status == ReservationStatus::Revoked || status == ReservationStatus::Released {
                continue;
            }
            // TTL expiry clears the lease here (NIP-40, held and intent
            // alike). bd-lease-liveness-bo0g: an expired lease whose holder
            // is STILL LIVE is resurrected in `resolved_leases` from the SQL
            // mirror under oracle vouch — this view stays pure NIP-40.
            if let Some(exp) = winning.tags.expiration()
                && (exp.as_secs() as i64) < now_secs
            {
                continue;
            }
            let (_, path) = d.split_once(':').unwrap_or(("", &d));
            let exclusive = winning.content.contains("\"exclusive\":true")
                || !winning.content.contains("\"exclusive\":false");
            let reason = winning
                .tags
                .iter()
                .find_map(|t| {
                    (t.as_slice().first().map(String::as_str) == Some("reason"))
                        .then(|| t.as_slice().get(1).map(ToString::to_string))
                })
                .flatten()
                .unwrap_or_default();
            out.push(ResolvedLease {
                id: winning.id.to_hex(),
                agent_pubkey: winning.pubkey.to_hex(),
                path_pattern: path.to_string(),
                exclusive,
                reason,
                expires_ts_us: winning
                    .tags
                    .expiration()
                    .map(|t| t.as_secs() as i64 * 1_000_000),
                status,
                created_at_secs: winning.created_at.as_secs() as i64,
                epoch: lease_epoch(&winning.content),
                expired: false,
                liveness_extended: false,
            });
        }
        out
    }

    /// bd-guard-crossproject-lease-er38: active HELD leases owned by OTHER
    /// projects that spell paths into a sibling repo via `../<repo>/...`.
    /// `guard check --repo <repo>` merges these into its evaluation set
    /// (translated to repo-relative globs) so ONE reservation satisfies
    /// every spelling - a crew never pays a double-reservation tax for
    /// committing in the repo its lease names. Excludes `project_slug`
    /// and all of its slug aliases; holder names resolve against the
    /// OWNING project's profiles, best-effort.
    ///
    /// Enumeration reads the project slug straight off each lease row's
    /// `d_tag` (`<slug>:<path>` - the same coordinate
    /// [`Self::resolved_leases`] splits), NOT the tag index: kind-30031
    /// rows carry no persisted `t` tag rows, and an unscoped kinds-only
    /// replay sweep is not guaranteed by the store's pushdown contract.
    /// Per-slug failures are best-effort skips - a hiccup in one project
    /// never blanks the guard.
    pub async fn active_reservations_escaping_into(
        &self,
        project_slug: &str,
        repo_name: &str,
    ) -> anyhow::Result<Vec<Reservation>> {
        let canonical = self.canonical_project(project_slug).await; // ADR-0017
        let own_aliases: std::collections::HashSet<String> =
            self.project_slug_aliases(&canonical).into_iter().collect();
        let Some(pool) = self.index_pool.as_ref() else {
            return Ok(Vec::new());
        };
        let slugs: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT substr(d_tag, 1, instr(d_tag, ':') - 1) \
             FROM bus_events \
             WHERE kind = ?1 AND d_tag IS NOT NULL AND instr(d_tag, ':') > 1",
        )
        .bind(i64::from(toron_kind(FILE_RESERVATION).as_u16()))
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let mut out = Vec::new();
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for slug in slugs {
            if own_aliases.contains(&slug) {
                continue;
            }
            let held = match self.active_reservations(&slug).await {
                Ok(v) => v,
                Err(_) => continue,
            };
            let leases: Vec<Reservation> = held
                .into_iter()
                .filter(|r| cross_project_pattern(&r.path_pattern, repo_name).is_some())
                .filter(|r| seen_ids.insert(r.id.clone()))
                .collect();
            if leases.is_empty() {
                continue;
            }
            out.extend(leases);
        }
        Ok(out)
    }

    /// Active (non-revoked, non-expired) HELD reservations in a project —
    /// the pre-intent view, byte-compatible with the old
    /// `active_reservations` (status-less 30031s = held). Public since
    /// P11.3a: the coalition status surface (flywheel) lists member-held
    /// leases (ADR-0011 Spec 1).
    pub async fn active_reservations(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<Reservation>> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let mut out: Vec<Reservation> = self
            .resolved_leases(project_slug)
            .await?
            .into_iter()
            .filter(|l| l.status == ReservationStatus::Held)
            .map(|l| Reservation {
                id: l.id,
                agent_name: String::new(), // resolved via profile below
                agent_pubkey: l.agent_pubkey,
                path_pattern: l.path_pattern,
                exclusive: l.exclusive,
                reason: l.reason,
                expires_ts_us: l.expires_ts_us,
                status: ReservationStatus::Held,
                epoch: l.epoch,
            })
            .collect();
        // Fill agent names from profiles (best-effort).
        self.fill_agent_names(project_slug, &mut out).await;
        Ok(out)
    }

    /// The lease board view shared by the `list_file_reservations` MCP tool
    /// and `toron reserve list`: active HELD reservations filtered by an
    /// exact holder name and/or a symmetric-glob path overlap. One filter
    /// implementation keeps the CLI and MCP surfaces from drifting.
    pub async fn list_file_reservations(
        &self,
        project_slug: &str,
        holder: Option<&str>,
        path: Option<&str>,
    ) -> anyhow::Result<Vec<Reservation>> {
        let rows = self.active_reservations(project_slug).await?;
        Ok(rows
            .into_iter()
            .filter(|r| holder.is_none_or(|h| r.agent_name == h))
            .filter(|r| {
                path.is_none_or(|p| {
                    // the guard's own symmetric fnmatch semantics
                    super::guard::pattern_matches(&r.path_pattern, p)
                        || super::guard::pattern_matches(p, &r.path_pattern)
                })
            })
            .collect())
    }

    /// bd-f5tt: active HELD reservations across every slug alias of
    /// `project_slug` (CLI absolute-path key + MCP short key). A lease is
    /// tagged with the slug as-written at acquire time; the guard check
    /// queries all aliases so a reservation acquired under one surface
    /// blocks a commit checked under the other. Deduplicated by id.
    pub async fn active_reservations_all_aliases(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<Reservation>> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let aliases = self.project_slug_aliases(project_slug);
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out: Vec<Reservation> = Vec::new();
        for slug in aliases {
            for r in self.active_reservations(&slug).await? {
                if seen.insert(r.id.clone()) {
                    out.push(r);
                }
            }
        }
        Ok(out)
    }

    /// bd-adr-0028-implementation-zfnx.11 — next fencing epoch for a
    /// coordinate: `max(epoch)` over EVERY 30031 event at `(kind, d)`
    /// (leases AND terminal markers, which stamp what they terminalize),
    /// plus one. A fresh coordinate claims genesis `0`. Never resets:
    /// reclaim-after-revoke/release always moves forward, so the previous
    /// holder's presented epoch goes stale by construction.
    async fn next_epoch_at(&self, project_slug: &str, d: &str) -> u64 {
        let filter = kind_project_filter(toron_kind(FILE_RESERVATION), project_slug);
        let events = collect_replay(self.bus.as_ref(), filter)
            .await
            .unwrap_or_default();
        let max = events
            .iter()
            .filter(|e| {
                e.tags.iter().any(|t| {
                    let s = t.as_slice();
                    s.first().map(|x| x == "d").unwrap_or(false)
                        && s.get(1).map(|x| x == d).unwrap_or(false)
                })
            })
            .map(|e| lease_epoch(&e.content))
            .max();
        max.map(|m| m.saturating_add(1)).unwrap_or(0)
    }

    /// bd-adr-0028-implementation-zfnx.11 — fencing check: the caller presents
    /// the epoch it claimed with; a mismatch fails LOUD with the current
    /// lease as the re-read pointer (id + holder + current epoch). Success
    /// returns the current lease. No live lease at the coordinate is also a
    /// loud failure (released/revoked since — re-read, never assume).
    pub async fn check_claim_epoch(
        &self,
        project_slug: &str,
        path: &str,
        presented_epoch: u64,
    ) -> anyhow::Result<Reservation> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let current = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .find(|r| r.path_pattern == path)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "stale claim epoch on {path}: no live lease (released or revoked since) — \
                     re-read via active_reservations, never assume"
                )
            })?;
        if current.epoch != presented_epoch {
            anyhow::bail!(
                "stale claim epoch on {path}: presented {presented_epoch}, current {} \
                 (lease {} by {}) — re-read via active_reservations, never assume",
                current.epoch,
                current.id,
                current.agent_name,
            );
        }
        Ok(current)
    }

    /// Fenced release: present the claimed epoch, then release. A stale
    /// epoch fails loud BEFORE anything is published (the path stays owned
    /// by whoever the epoch says owns it).
    pub async fn release_fenced(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        presented_epoch: u64,
    ) -> anyhow::Result<ReleaseResult> {
        for path in paths {
            self.check_claim_epoch(project_slug, path, presented_epoch)
                .await?;
        }
        self.release_file_reservations(project_slug, agent_name, paths)
            .await
    }

    /// Fenced renew: present the claimed epoch, then renew. Same fail-loud
    /// contract as [`Self::release_fenced`].
    pub async fn renew_fenced(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        extend_secs: u64,
        presented_epoch: u64,
    ) -> anyhow::Result<RenewResult> {
        for path in paths {
            self.check_claim_epoch(project_slug, path, presented_epoch)
                .await?;
        }
        self.renew_file_reservations(project_slug, agent_name, paths, extend_secs)
            .await
    }

    /// Active (in-TTL, non-expired) intents in a project (ADR-0011 Spec 2):
    /// the intent board. An intent auto-expires `INTENT_TTL_SECS` after
    /// declaration unless converted to held (must-convert).
    pub async fn active_intents(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<IntentReservation>> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let leases = self.resolved_leases(project_slug).await?;
        let now_secs = Timestamp::now().as_secs() as i64;
        let mut intents: Vec<IntentReservation> = leases
            .into_iter()
            .filter(|l| l.status == ReservationStatus::Intent)
            .filter(|l| l.created_at_secs.saturating_add(INTENT_TTL_SECS as i64) >= now_secs)
            .map(|l| {
                let ttl_end_us =
                    (l.created_at_secs.saturating_add(INTENT_TTL_SECS as i64)) * 1_000_000;
                IntentReservation {
                    id: l.id,
                    agent_name: String::new(), // filled below
                    agent_pubkey: l.agent_pubkey,
                    path_pattern: l.path_pattern,
                    reason: l.reason,
                    declared_at_secs: l.created_at_secs,
                    expires_ts_us: Some(l.expires_ts_us.unwrap_or(ttl_end_us).min(ttl_end_us)),
                    ttl_secs: INTENT_TTL_SECS,
                }
            })
            .collect();
        let names = self.agent_name_map(project_slug).await;
        for i in &mut intents {
            // Roster miss stays empty: never invent a pubkey prefix (bd-ow6c —
            // an invented name later compared as a peer pin).
            i.agent_name = names.get(&i.agent_pubkey).cloned().unwrap_or_default();
        }
        Ok(intents)
    }

    /// Build the kernel model from projected leases (bd-y6t7): the host
    /// owns I/O (bus replay, mirrors, names); the tick owns grant/deny.
    /// Only Held/Intent rows decide — terminal markers never reach the
    /// kernel. Errors degrade to an empty model (same as the old inline
    /// view: no rows, no conflicts).
    async fn kernel_model_for(
        &self,
        project_slug: &str,
    ) -> (
        KernelModel,
        std::collections::HashMap<String, (String, ReservationStatus)>,
    ) {
        let leases = self.resolved_leases(project_slug).await.unwrap_or_default();
        let names = self.agent_name_map(project_slug).await;
        let mut model = KernelModel::new();
        let mut meta = std::collections::HashMap::new();
        for l in leases {
            let state = match l.status {
                ReservationStatus::Held => EntryState::Held,
                ReservationStatus::Intent => EntryState::Intent,
                _ => continue,
            };
            // Roster miss stays empty: never invent a pubkey prefix (bd-ow6c —
            // an invented name later compared as a peer pin).
            let name = names.get(&l.agent_pubkey).cloned().unwrap_or_default();
            meta.insert(l.id.clone(), (name, l.status));
            model.leases.insert(
                l.id.clone(),
                LeaseEntry {
                    id: l.id.clone(),
                    holder: l.agent_pubkey.clone(),
                    path_pattern: l.path_pattern.clone(),
                    exclusive: l.exclusive,
                    expires_us: l.expires_ts_us,
                    created_at_secs: l.created_at_secs,
                    epoch: l.epoch,
                    state,
                    liveness_extended: l.liveness_extended,
                },
            );
        }
        (model, meta)
    }

    /// Host clock for kernel Msgs (µs, ADR G7): read once per decision at
    /// the host edge, never inside the tick.
    fn kernel_now_us() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    }

    /// Split kernel blockers into hard (Held) and soft (Intent) tiers.
    /// One splitter for both the acquire and check paths.
    fn split_blockers(
        blockers: &[(String, String, ReservationStatus)],
    ) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
        let mut hard = Vec::new();
        let mut hard_ids = Vec::new();
        let mut soft = Vec::new();
        let mut soft_ids = Vec::new();
        for (name, id, status) in blockers {
            match status {
                ReservationStatus::Held => {
                    hard.push(name.clone());
                    hard_ids.push(id.clone());
                }
                ReservationStatus::Intent => {
                    soft.push(name.clone());
                    soft_ids.push(id.clone());
                }
                _ => {}
            }
        }
        (hard, hard_ids, soft, soft_ids)
    }

    /// Decide one acquire through the kernel (bd-y6t7): returns every
    /// blocking (name, id, status) by re-ticking past each blocker on a
    /// scratch model. One decider — the old inline `conflict_view` is gone.
    fn kernel_blockers_for(
        model: &KernelModel,
        meta: &std::collections::HashMap<String, (String, ReservationStatus)>,
        probe: &AcquireProbe,
    ) -> Vec<(String, String, ReservationStatus)> {
        use super::reservations_core::{KernelEffect, KernelMsg};
        let mut scratch = model.clone();
        let mut out = Vec::new();
        for _ in 0..=scratch.leases.len() {
            let (_, fx) = reservations_core::tick(
                &scratch,
                &KernelMsg::Acquire {
                    id: format!("probe:{}:{}", probe.path, probe.now_us),
                    holder: probe.holder_hex.clone(),
                    path_pattern: probe.path.clone(),
                    exclusive: probe.exclusive,
                    expires_us: probe.expires_us,
                    created_at_secs: probe.now_us / 1_000_000,
                    epoch: probe.epoch,
                    state: EntryState::Held,
                    now_us: probe.now_us,
                },
            );
            match fx.into_iter().next() {
                Some(KernelEffect::Denied {
                    reason: super::reservations_core::DenyReason::HardConflict { blocking_id },
                    ..
                }) => {
                    if let Some((name, status)) = meta.get(&blocking_id) {
                        out.push((name.clone(), blocking_id.clone(), *status));
                    }
                    scratch.leases.remove(&blocking_id);
                }
                _ => break,
            }
        }
        out
    }

    /// Staleness heuristic: holder has no signed profile activity within
    /// `STALE_AFTER_SECS`.
    async fn is_holder_stale(&self, project_slug: &str, res: &Reservation) -> bool {
        let filter =
            kind_project_filter(toron_kind(crate::nostr::kinds::AGENT_PROFILE), project_slug);
        collect_replay(self.bus.as_ref(), filter)
            .await
            .is_ok_and(|events| {
                let now = Timestamp::now().as_secs() as i64;
                let holder_pubkey = res.agent_pubkey.clone();
                let last_active = events
                    .iter()
                    .filter(|e| e.pubkey.to_hex() == holder_pubkey)
                    .map(|e| e.created_at.as_secs() as i64)
                    .max();
                // No profile at all → treat as stale (unknown agent).
                last_active.is_none_or(|ts| now - ts > STALE_AFTER_SECS as i64)
            })
    }

    /// Best-effort pubkey → agent-name map from kind-30013 profiles, with the
    /// durable roster filling the gaps.
    async fn agent_name_map(
        &self,
        project_slug: &str,
    ) -> std::collections::HashMap<String, String> {
        let filter =
            kind_project_filter(toron_kind(crate::nostr::kinds::AGENT_PROFILE), project_slug);
        let Ok(events) = collect_replay(self.bus.as_ref(), filter).await else {
            return std::collections::HashMap::new();
        };
        let mut names: std::collections::HashMap<String, String> = events
            .iter()
            .filter_map(|e| {
                let name = crate::nostr::identity::parse_profile(e)
                    .map(|p| p.name)
                    .unwrap_or_default();
                if name.is_empty() {
                    None
                } else {
                    Some((e.pubkey.to_hex(), name))
                }
            })
            .collect();
        // bd-ow6c: the roster fills what the live profiles do not cover. The
        // fallback used to be the first 8 chars of the pubkey — a string no
        // agent name equals — so a lane that compared the lease holder with
        // its own pin treated its OWN lease as a peer's and starved on the
        // bead it held.
        for (pubkey, name) in self.roster_names_by_pubkey(project_slug).await {
            names.entry(pubkey).or_insert(name);
        }
        names
    }

    /// Best-effort: fill `agent_name` from kind-30013 profiles. A roster
    /// miss leaves the name EMPTY — never an invented pubkey prefix, which
    /// later compared as a peer pin and made a lane starve on its own lease.
    async fn fill_agent_names(&self, project_slug: &str, reservations: &mut [Reservation]) {
        let name_by_pubkey = self.agent_name_map(project_slug).await;
        for r in reservations.iter_mut() {
            if let Some(name) = name_by_pubkey.get(&r.agent_pubkey) {
                r.agent_name = name.clone();
            } else {
                r.agent_name = String::new();
            }
        }
    }

    /// bd-pin-public-plane-s-easy.13: lease transfer — a grant is a lease
    /// transfer, never "commit as me". The holder (`from`) releases the
    /// path; the target (`to`) acquires the SAME path preserving exclusivity
    /// and reason. After transfer the `from` guard no longer covers the path
    /// and the `to` guard does; `to` commits as itself, never on behalf.
    ///
    /// Fails loudly (no silent no-op) when `from` is not the current holder,
    /// when the target cannot acquire (hard conflict), or when either key is
    /// missing. The `request-release` Agreed path MAY call this helper (same
    /// release-then-acquire semantics).
    ///
    /// ORDERING INVARIANT: the acquire event is stamped STRICTLY after the
    /// release marker (created_at + 1s). `resolve_lease_state` keeps only
    /// events with `created_at > marker.cutoff`; two `Timestamp::now()`
    /// events in the same second would let the release marker win the
    /// coordinate and the path would vanish from the held view.
    pub async fn transfer_reservation(
        &self,
        project_slug: &str,
        from_agent: &str,
        to_agent: &str,
        path: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<TransferResult> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        if from_agent == to_agent {
            anyhow::bail!("transfer to self is a no-op — {from_agent} already holds {path}");
        }
        let holder = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .find(|r| r.path_pattern == path)
            .ok_or_else(|| anyhow::anyhow!("no active held reservation on {path}"))?;
        if holder.agent_name != from_agent {
            anyhow::bail!(
                "reservation {path} is held by {} (not {from_agent})",
                holder.agent_name
            );
        }
        // Target must have a local key before we touch the holder's lease.
        self.agent_keys_for(project_slug, to_agent)?;

        let release_ts = Timestamp::now();
        let acquire_ts = Timestamp::from(release_ts.as_secs() + 1);
        let from_keys = self.agent_keys_for(project_slug, from_agent)?;
        let to_keys = self.agent_keys_for(project_slug, to_agent)?;
        let d = format!("{project_slug}:{path}");
        let effective_reason = reason
            .map(ToString::to_string)
            .or_else(|| (!holder.reason.is_empty()).then(|| holder.reason.clone()))
            .unwrap_or_default();

        // 1. Release marker (same-author NIP-33 supersession, status:released).
        let release_event = build_event(
            &from_keys,
            toron_kind(FILE_RESERVATION),
            // bd-adr-0028-implementation-zfnx.11: stamp the terminalized epoch.
            marker_content("released", None, holder.epoch),
            with_project_scoping(
                toron_kind(FILE_RESERVATION),
                vec![
                    Tag::parse(["d", &d]).expect("d"),
                    Tag::parse(["status", "released"]).expect("status"),
                ],
                Some(project_slug),
            ),
            release_ts,
        )?;
        self.bus.publish(release_event.clone()).await?;
        self.project_lease_status(project_slug, &holder.agent_pubkey, path, "released")
            .await;
        self.record_transition(project_slug, &d, "released", &release_event.id);

        // 2. Re-acquire for the target, preserving exclusivity + reason,
        //    stamped strictly after the release marker. bd-adr-0028-implementation-zfnx.11: the
        //    reclaim bumps past the release marker's stamped epoch, so the
        //    previous holder's presented epoch goes stale by construction.
        let acquire_epoch = self.next_epoch_at(project_slug, &d).await;
        let acquire_event = build_event(
            &to_keys,
            toron_kind(FILE_RESERVATION),
            lease_content(holder.exclusive, &effective_reason, acquire_epoch),
            with_project_scoping(
                toron_kind(FILE_RESERVATION),
                vec![
                    Tag::parse(["d", &d]).expect("d"),
                    Tag::parse(["reason", &effective_reason]).expect("reason"),
                ],
                Some(project_slug),
            ),
            acquire_ts,
        )?;
        self.bus.publish(acquire_event.clone()).await?;
        self.project_lease_upsert(LeaseUpsert {
            project_slug,
            agent_name: to_agent,
            agent_pubkey: &to_keys.public_key().to_hex(),
            path_pattern: path,
            exclusive: holder.exclusive,
            reason: &effective_reason,
            event_id: &acquire_event.id.to_hex(),
            expires_ts_us: None,
        })
        .await;
        self.record_transition(project_slug, &d, "acquired", &acquire_event.id);

        // 3. Verify the target actually holds the path now (the acquire
        //    resolved past the release marker — never a phantom transfer).
        let held_by_target = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .any(|r| r.path_pattern == path && r.agent_name == to_agent);
        if !held_by_target {
            anyhow::bail!(
                "transfer verification failed: {to_agent} does not hold {path} after transfer \
                 (coordinate resolution did not pick the new lease)"
            );
        }

        Ok(TransferResult {
            path_pattern: path.to_string(),
            from_agent: from_agent.to_string(),
            to_agent: to_agent.to_string(),
            exclusive: holder.exclusive,
            reason: effective_reason,
            released_id: Some(release_event.id.to_hex()),
        })
    }

    /// `request_release` (ADR-0011 Spec 6): Crew A asks holder B to release
    /// or defer a held reservation via a typed kind-14 DM (toron-kind=
    /// `request-release`, importance high, ack-required). Validates that the
    /// reservation exists, is HELD, and is held by `target_agent`.
    pub async fn request_release(
        &self,
        args: &RequestRelease,
    ) -> anyhow::Result<RequestReleaseResult> {
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await; // ADR-0017
        if args.sender_name == args.target_agent {
            anyhow::bail!(
                "request-release to self is a no-op — {} already holds it",
                args.target_agent
            );
        }
        let lease = self
            .active_reservations(&args.project_slug)
            .await?
            .into_iter()
            .find(|r| r.id == args.reservation_id)
            .ok_or_else(|| {
                anyhow::anyhow!("no active held reservation with id {}", args.reservation_id)
            })?;
        if lease.status != ReservationStatus::Held {
            anyhow::bail!(
                "reservation {} is not held (status {:?})",
                args.reservation_id,
                lease.status
            );
        }
        if lease.agent_name != args.target_agent {
            anyhow::bail!(
                "reservation {} is held by {} (not {})",
                args.reservation_id,
                lease.agent_name,
                args.target_agent
            );
        }
        let ask_tag = ask_to_tag(&args.ask);
        let mut extra = vec![
            Tag::parse(["toron-kind", "request-release"]).expect("kind"),
            Tag::parse(["ask", &ask_tag]).expect("ask"),
            Tag::parse(["reservation-id", &args.reservation_id]).expect("res"),
        ];
        if let Ok(lease_id) = EventId::from_hex(&args.reservation_id) {
            // NIP-10 e-reference to the held lease event.
            extra.push(Tag::event(lease_id));
        }
        let send = SendMessage {
            project_slug: args.project_slug.clone(),
            sender_name: args.sender_name.clone(),
            to: vec![args.target_agent.clone()],
            subject: format!("[request-release] {}", lease.path_pattern),
            body_md: format!(
                "{} asks you to {} reservation {} ({}){}\nReply with a request-release-response: agreed | denied | deferred:<secs>.",
                args.sender_name,
                ask_tag,
                args.reservation_id,
                lease.path_pattern,
                args.note
                    .as_ref()
                    .map(|n| format!(" — {n}"))
                    .unwrap_or_default()
            ),
            importance: Some("high".into()),
            ack_required: true,
            extra_tags: extra,
            ..Default::default()
        };
        let res = self.send_message(&send).await?;
        let message_id = res
            .deliveries
            .iter()
            .find(|d| d.payload.recipient == args.target_agent)
            .map(|d| d.payload.event_id.clone())
            .unwrap_or_default();
        Ok(RequestReleaseResult {
            delivered: !message_id.is_empty(),
            message_id,
            path_pattern: lease.path_pattern,
            holder: lease.agent_name,
        })
    }

    /// `respond_request_release` (ADR-0011 Spec 6): the holder answers the
    ///   request with a typed kind-14 DM (toron-kind=`request-release-response`
    ///   plus a `decision` tag and a NIP-10 reply ref to the request).
    ///
    /// Only the request's recipient can respond (the wrap is sealed for
    ///   them); the reply goes back to the requester.
    pub async fn respond_request_release(
        &self,
        args: &ReleaseResponse,
    ) -> anyhow::Result<SendResult> {
        let mut args = args.clone();
        args.project_slug = self.canonical_project(&args.project_slug).await; // ADR-0017
        let request = self
            .find_request_by_id(
                &args.project_slug,
                &args.sender_name,
                &args.request_message_id,
            )
            .await?;
        // The requester = the request rumor's author → name via profiles.
        let profiles = self.project_profiles(&args.project_slug).await?;
        let requester_name = profiles
            .iter()
            .find_map(|ev| {
                (ev.pubkey == request.author_pubkey)
                    .then(|| crate::nostr::identity::parse_profile(ev))
                    .flatten()
                    .map(|p| p.name)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "requester {} has no registered profile in project {}",
                    request
                        .author_pubkey
                        .to_hex()
                        .chars()
                        .take(8)
                        .collect::<String>(),
                    args.project_slug
                )
            })?;
        let decision_tag = decision_to_tag(&args.decision);
        let mut extra = vec![
            Tag::parse(["toron-kind", "request-release-response"]).expect("kind"),
            Tag::parse(["decision", &decision_tag]).expect("decision"),
            Tag::parse(["reservation-id", &request.reservation_id]).expect("res"),
        ];
        if let Ok(req_id) = EventId::from_hex(&args.request_message_id) {
            // NIP-10 reply ref to the request rumor.
            extra.push(Tag::event(req_id));
        }
        let send = SendMessage {
            project_slug: args.project_slug.clone(),
            sender_name: args.sender_name.clone(),
            to: vec![requester_name],
            subject: format!("[request-release-response] {decision_tag}"),
            body_md: format!(
                "decision: {decision_tag}{}",
                args.note
                    .as_ref()
                    .map(|n| format!(" — {n}"))
                    .unwrap_or_default()
            ),
            importance: Some("high".into()),
            ack_required: true,
            extra_tags: extra,
            ..Default::default()
        };
        let sent = self.send_message(&send).await?;
        // bd-wake-id-contract-1jz1: a DECIDED request must never re-fire a
        // wake for the responder. Record the responder's READ receipt on the
        // request rumor — the urgent/unread wake filter (bd-dbre: read rows
        // do not wake) then drops it permanently, loop-side and everywhere.
        // Best-effort: the decision DM is already out; a receipt failure
        // must not fail the response.
        let _ = self
            .mark_message_read(
                &args.project_slug,
                &args.sender_name,
                &args.request_message_id,
            )
            .await;
        Ok(sent)
    }

    /// `request_release_status` (ADR-0011 Spec 6): resolve a negotiation by
    /// its request message id: `Pending` (window not elapsed), `Responded`
    /// (holder answered), `EscalationDue` (window elapsed, no response — the
    /// caller fires [`Mailbox::escalate_request_release`]), or `Unknown`
    /// (message missing or not authored by the caller).
    pub async fn request_release_status(
        &self,
        project_slug: &str,
        requestor_name: &str,
        request_message_id: &str,
        window_secs: Option<u64>,
    ) -> RequestReleaseStatus {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let window = window_secs.unwrap_or(REQUEST_RELEASE_TIER_WINDOW_SECS);
        let Ok(keys) = self.agent_keys_for(project_slug, requestor_name) else {
            return RequestReleaseStatus::Unknown;
        };
        let mut rumors = self.project_rumors(project_slug).await;
        let now_secs = Timestamp::now().as_secs() as i64;
        let Some(request) = rumors.iter_mut().find(|r| {
            r.kind == Kind::PrivateDirectMessage
                && r.pubkey == keys.public_key()
                && (**r).clone().id().to_hex() == request_message_id
                && r.tags.iter().any(|t| {
                    t.as_slice() == ["toron-kind".to_string(), "request-release".to_string()]
                })
        }) else {
            return RequestReleaseStatus::Unknown;
        };
        let sent_at = request.created_at.as_secs() as i64;
        // A typed response to THIS request (e-ref matches) with a parseable
        // decision settles the negotiation; anything else is not a response.
        // Multiple responses → the LATEST decision wins (projection rule).
        let decided = rumors
            .iter_mut()
            .filter_map(|r| {
                (r.kind == Kind::PrivateDirectMessage
                    && r.tags.iter().any(|t| {
                        t.as_slice()
                            == [
                                "toron-kind".to_string(),
                                "request-release-response".to_string(),
                            ]
                    })
                    && r.tags.iter().any(|t| {
                        t.as_slice().first().map(|s| s == "e").unwrap_or(false)
                            && t.as_slice()
                                .get(1)
                                .map(|s| s == request_message_id)
                                .unwrap_or(false)
                    }))
                .then(|| decision_from_tags(&r.tags))
                .flatten()
                .map(|d| (r.created_at.as_secs() as i64, d))
            })
            .max_by_key(|(ts, _)| *ts)
            .map(|(_, d)| d);
        if let Some(decision) = decided {
            return RequestReleaseStatus::Responded { decision };
        }
        if now_secs.saturating_sub(sent_at) > window as i64 {
            return RequestReleaseStatus::EscalationDue {
                sent_at_secs: sent_at,
                window_secs: window,
            };
        }
        RequestReleaseStatus::Pending {
            sent_at_secs: sent_at,
            window_secs: window,
        }
    }

    /// `escalate_request_release` (ADR-0011 Spec 6): a request unanswered past
    /// the tier window escalates to a kind-14 DM to the captain (the operator
    /// key — the project manifest authority), importance high. Idempotency is
    /// the caller's job (the DM carries the request id so the captain can
    /// dedupe). Force-release remains the operator's last resort.
    pub async fn escalate_request_release(
        &self,
        project_slug: &str,
        requestor_name: &str,
        request_message_id: &str,
        note: Option<&str>,
    ) -> anyhow::Result<()> {
        let project_slug = &self.canonical_project(project_slug).await; // ADR-0017
        let operator = self.operator.as_ref().ok_or_else(|| {
            anyhow::anyhow!("no operator key configured; cannot escalate to captain")
        })?;
        let request = self
            .find_request_by_id(project_slug, requestor_name, request_message_id)
            .await?;
        let keys = self.agent_keys_for(project_slug, requestor_name)?;
        if request.author_pubkey != keys.public_key() {
            anyhow::bail!("message {request_message_id} is not your request-release");
        }
        let operator_npub = operator.public_key();
        // Best-effort path label for the DM body (the lease may be gone).
        let path_label = match self
            .active_reservations(project_slug)
            .await
            .ok()
            .and_then(|res| res.into_iter().find(|r| r.id == request.reservation_id))
        {
            Some(r) => r.path_pattern,
            None => format!("reservation {}", request.reservation_id),
        };
        let rumor = gift_wrap::build_rumor(
            keys.public_key(),
            Timestamp::now(),
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["toron-kind", "request-release-escalation"]).expect("kind"),
                Tag::parse(["p", &operator_npub.to_hex()]).expect("p"),
                Tag::parse(["importance", "high"]).expect("importance"),
                Tag::parse([
                    "subject",
                    &format!("[request-release-escalation] {path_label}"),
                ])
                .expect("subject"),
                Tag::parse(["reservation-id", &request.reservation_id]).expect("res"),
                Tag::event(EventId::from_hex(request_message_id).expect("request id")),
            ],
            format!(
                "request-release from {requestor_name} for {path_label} (message {request_message_id}) unanswered past the tier window{}.",
                note.map(|n| format!(" — {n}")).unwrap_or_default()
            ),
        );
        let wrap = gift_wrap::gift_wrap(&keys, &operator_npub, rumor)?;
        self.publish_dm(&wrap).await?;
        Ok(())
    }

    /// Locate a `request-release` rumor by its message id and confirm the
    /// reader is involved (author or addressed via the rumor's `p` tags).
    /// Gift-wrap seals hide recipients — the involvement check runs on the
    /// unwrapped rumor (authentic: the rumor is signed by the requester).
    async fn find_request_by_id(
        &self,
        project_slug: &str,
        reader_name: &str,
        request_message_id: &str,
    ) -> anyhow::Result<RequestInfo> {
        let keys = self.agent_keys_for(project_slug, reader_name)?;
        let reader_pk = keys.public_key();
        let rumors = self.project_rumors(project_slug).await;
        let rumor = rumors
            .iter()
            .find(|r| {
                r.kind == Kind::PrivateDirectMessage
                    && (**r).clone().id().to_hex() == request_message_id
                    && r.tags.iter().any(|t| {
                        t.as_slice() == ["toron-kind".to_string(), "request-release".to_string()]
                    })
                    && (r.pubkey == reader_pk || rumor_addresses(r, &reader_pk.to_hex()))
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no request-release message with id {request_message_id} addressed to {reader_name}"
                )
            })?;
        Ok(RequestInfo {
            author_pubkey: rumor.pubkey,
            sent_at_secs: rumor.created_at.as_secs() as i64,
            reservation_id: lease_tag_value(&rumor.tags, "reservation-id").unwrap_or_default(),
        })
    }

    /// Collect kind-1059 wraps readable by ANY registered agent key in the
    /// project, deduped by rumor id. Mirrors the projection's per-key unwrap
    /// (G14): a sender cannot unwrap its own wrap (sealed for the recipient),
    /// so status/escalation lookups must scan the recipient side too.
    async fn project_rumors(&self, project_slug: &str) -> Vec<crate::nostr::UnsignedEvent> {
        let filter = ToronFilter::new().kinds([Kind::GiftWrap]).build();
        let Ok(wraps) = collect_replay(self.bus.as_ref(), filter).await else {
            return Vec::new();
        };
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out = Vec::new();
        for name in self.registered_agent_names(project_slug).await {
            let Some(keys) = self.load_agent_key(&self.agent_key_path(project_slug, &name)) else {
                continue;
            };
            for w in &wraps {
                let Ok(g) = gift_wrap::unwrap(&keys, w) else {
                    continue;
                };
                let mut rumor = g.rumor;
                let id = rumor.id().to_hex();
                if seen.insert(id) {
                    out.push(rumor);
                }
            }
        }
        out
    }

    /// Names of all registered agents in a project (kind-30013 profiles).
    async fn registered_agent_names(&self, project_slug: &str) -> Vec<String> {
        let Ok(profiles) = self.project_profiles(project_slug).await else {
            return Vec::new();
        };
        profiles
            .iter()
            .filter_map(crate::nostr::identity::parse_profile)
            .map(|p| p.name)
            .collect()
    }

    /// Append an acquire/release/revoked transition record to the Git archive
    /// (G13 history; permanent, append-only).
    fn record_transition(&self, project_slug: &str, d: &str, status: &str, event_id: &EventId) {
        let root = crate::mailbox::aliases::project_root(&self.data_dir, project_slug);
        if let Ok(archive) = crate::archive::git::GitArchive::open_or_init(&root) {
            let rel = std::path::Path::new(".audit").join(format!("{event_id}.{status}.json"));
            let content = format!(
                r#"{{"d":"{d}","status":"{status}","event_id":"{event_id}","ts_us":{}}}"#,
                crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
            );
            // F2.SILENT-DROP (batch-fix): the G13 audit-history write must not
            // fail silently — an unwritable archive loses the
            // acquire/release/revoked record with no signal.
            if let Err(e) = archive.stage_file(&rel, content.as_bytes()) {
                tracing::warn!("reservation audit stage_file failed ({status} {d}): {e}");
            }
            if let Err(e) = archive.commit(&format!("reservation {status} {d}")) {
                tracing::warn!("reservation audit commit failed ({status} {d}): {e}");
            }
        }
    }

    /// bd-reservations-not-projected-5ohc: mirror a granted/renewed lease
    /// into the durable SQLite `file_reservations` projection (the read model
    /// external readers query directly). Best-effort by design: no attached
    /// index pool → no-op; a failed projection logs but never fails the
    /// signed lease event (the bus replay stays the source of truth).
    async fn project_lease_upsert(&self, u: LeaseUpsert<'_>) {
        let Some(pool) = self.index_pool() else {
            return;
        };
        let r = sqlx::query(
            "INSERT INTO file_reservations \
             (project_slug, agent_name, agent_pubkey, path_pattern, exclusive, reason, \
              status, event_id, expires_ts_us, created_ts_us) \
             VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?) \
             ON CONFLICT(project_slug, path_pattern, agent_pubkey) DO UPDATE SET \
               agent_name=excluded.agent_name, exclusive=excluded.exclusive, \
               reason=excluded.reason, status='active', event_id=excluded.event_id, \
               expires_ts_us=excluded.expires_ts_us, created_ts_us=excluded.created_ts_us",
        )
        .bind(u.project_slug)
        .bind(u.agent_name)
        .bind(u.agent_pubkey)
        .bind(u.path_pattern)
        .bind(u.exclusive)
        .bind(u.reason)
        .bind(u.event_id)
        .bind(u.expires_ts_us)
        .bind(crate::index::writer::Clock::now_us(
            &crate::index::writer::SystemClock,
        ))
        .execute(pool)
        .await;
        if let Err(e) = r {
            tracing::warn!(
                "reservation projection failed ({}:{}): {e}",
                u.project_slug,
                u.path_pattern
            );
        }
    }

    /// bd-reservations-not-projected-5ohc: mark the projected row for a
    /// terminal lease transition (`released` / `revoked`). Same best-effort
    /// contract as [`Self::project_lease_upsert`].
    async fn project_lease_status(
        &self,
        project_slug: &str,
        agent_pubkey: &str,
        path_pattern: &str,
        status: &str,
    ) {
        let Some(pool) = self.index_pool() else {
            return;
        };
        let r = sqlx::query(
            "UPDATE file_reservations SET status=? \
             WHERE project_slug=? AND path_pattern=? AND agent_pubkey=?",
        )
        .bind(status)
        .bind(project_slug)
        .bind(path_pattern)
        .bind(agent_pubkey)
        .execute(pool)
        .await;
        if let Err(e) = r {
            tracing::warn!(
                "reservation projection update failed ({status} {project_slug}:{path_pattern}): {e}"
            );
        }
    }
}

/// Symmetric glob overlap: `a` matches `b`, or `b` matches `a` (fnmatch both
/// ways — AM-Rust parity).
pub fn overlaps(a: &str, b: &str) -> bool {
    pattern_matches(a, b) || pattern_matches(b, a)
}

/// A located `request-release` rumor (author + metadata).
#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestInfo {
    author_pubkey: PublicKey,
    sent_at_secs: i64,
    reservation_id: String,
}

/// One coordinate-resolved kind-30031 lease (held or intent), before
/// agent-name filling. `status` classifies the winning event (Spec 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedLease {
    pub id: String,
    pub agent_pubkey: String,
    pub path_pattern: String,
    pub exclusive: bool,
    pub reason: String,
    pub expires_ts_us: Option<i64>,
    pub status: ReservationStatus,
    pub created_at_secs: i64,
    /// Fencing epoch parsed from the winning event's content
    /// (bd-adr-0028-implementation-zfnx.11; `0` = legacy / genesis).
    pub epoch: u64,
    /// NIP-40 TTL has passed. Kept (flagged) when a liveness oracle says
    /// the holder's session is still live — the lease keeps blocking.
    pub expired: bool,
    /// This lease blocks ONLY because of the liveness oracle
    /// (bd-lease-liveness-bo0g); surfaces may report the reason.
    pub liveness_extended: bool,
}

/// One ghost lease released by the pane-prune cascade (bd-gc7k).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GhostRelease {
    pub project_slug: String,
    pub path_pattern: String,
    pub holder: String,
}

/// Classify a coordinate-winning 30031 event's status (ADR-0011 Spec 2).
/// Status-less = `Held` (backward compat); `status:intent` = `Intent`;
/// released/revoked markers are terminal. Recognized released forms: plain
/// `status:released` in content, JSON `"status":"released"`, or a
/// `["status", "released"]` tag.
pub fn lease_status(winning: &Event) -> ReservationStatus {
    if is_revocation(winning) {
        return ReservationStatus::Revoked;
    }
    let status_tag = winning
        .tags
        .iter()
        .find(|t| t.as_slice().first().map(|s| s == "status").unwrap_or(false))
        .and_then(|t| t.as_slice().get(1).map(ToString::to_string));
    match status_tag.as_deref() {
        Some("intent") => ReservationStatus::Intent,
        Some("released") => ReservationStatus::Released,
        // Canonical forms only: a bare `contains("released")` classifies an
        // ACQUIRE whose free-text reason mentions "released" as Released —
        // the lease vanishes from the held view and the exclusive-path
        // conflict floor stops blocking (bug hunt 2026-08-17).
        _ if winning.content.contains("status:released")
            || winning.content.contains("\"status\":\"released\"") =>
        {
            ReservationStatus::Released
        }
        _ => ReservationStatus::Held,
    }
}

/// First value of a `["<name>", <value>, ...]` tag (pair form only).
fn lease_tag_value(tags: &crate::nostr::Tags, name: &str) -> Option<String> {
    tags.iter().find_map(|t| {
        (t.as_slice().first().map(|s| s == name).unwrap_or(false))
            .then(|| t.as_slice().get(1).map(ToString::to_string))
            .flatten()
    })
}

/// True when a rumor's `p` tags address `npub_hex` (kind-14 recipient list).
fn rumor_addresses(rumor: &crate::nostr::UnsignedEvent, npub_hex: &str) -> bool {
    rumor.tags.iter().any(|t| {
        t.as_slice().first().map(|s| s == "p").unwrap_or(false)
            && t.as_slice().get(1).map(|s| s == npub_hex).unwrap_or(false)
    })
}

/// Wire encoding of a request ask.
fn ask_to_tag(ask: &ReleaseAsk) -> String {
    match ask {
        ReleaseAsk::Release => "release".to_string(),
        ReleaseAsk::Defer { defer_secs } => format!("defer:{defer_secs}"),
    }
}

/// Wire encoding of a response decision.
fn decision_to_tag(decision: &ReleaseDecision) -> String {
    match decision {
        ReleaseDecision::Agreed => "agreed".to_string(),
        ReleaseDecision::Denied => "denied".to_string(),
        ReleaseDecision::Deferred { defer_secs } => format!("deferred:{defer_secs}"),
    }
}

/// Decode a response `decision` tag. Unparseable → `None` (not a response).
fn decision_from_tags(tags: &crate::nostr::Tags) -> Option<ReleaseDecision> {
    let v = lease_tag_value(tags, "decision")?;
    match v.as_str() {
        "agreed" => Some(ReleaseDecision::Agreed),
        "denied" => Some(ReleaseDecision::Denied),
        _ => v
            .strip_prefix("deferred:")
            .and_then(|s| s.parse::<u64>().ok())
            .map(|defer_secs| ReleaseDecision::Deferred { defer_secs }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::filter::ToronFilter;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

    const PASSPHRASE: &str = "reservation-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-res-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-res-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    /// bd-ow6c: the lease holder's name must be the PIN, never an `npub[..8]`
    /// prefix. A prefix is a string no agent name equals, so a lane that
    /// compares the lease holder with its own pin treated its OWN lease as a
    /// peer's and starved on the bead it held. The durable roster names the
    /// pin even when the live profile event does not reach the project.
    #[tokio::test]
    async fn lease_holder_resolves_to_the_canonical_pin_not_a_pubkey_prefix() {
        let (mailbox, db_path, data_dir) = test_mailbox("holder-canonical").await;
        mailbox
            .ensure_project("demo")
            .await
            .expect("ensure project");
        let pubkey = "93bb5e316de4c7e0a99a7e00000000000000000000000000000000000000dead";
        let pool = mailbox.index_pool.as_ref().expect("pool").clone();
        sqlx::query(
            "INSERT INTO agents (project_slug, name, pubkey, created_ts_us) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind("demo")
        .bind("RoseDune")
        .bind(pubkey)
        .bind(1_700_000_000_000_000i64)
        .execute(&pool)
        .await
        .expect("seed the roster");
        let names = mailbox.agent_name_map("demo").await;
        assert_eq!(
            names.get(pubkey).map(String::as_str),
            Some("RoseDune"),
            "the roster resolves the holder to the pin, not an npub prefix"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ow6c: a roster miss leaves the holder name EMPTY — never an 8-char
    /// pubkey prefix. The old fallback invented a string that later compared
    /// as a peer pin, so a lane treated its OWN lease as a peer's and
    /// starved on the bead it held. Empty names no lane's pin.
    #[tokio::test]
    async fn roster_miss_leaves_holder_name_empty_never_a_peer_pin() {
        let (mailbox, db_path, data_dir) = test_mailbox("holder-empty").await;
        mailbox
            .ensure_project("demo")
            .await
            .expect("ensure project");
        // Keys with NO profile and NO roster row: both name sources miss.
        let ghost = crate::keys::generate();
        publish_raw_stale_lease(&mailbox, &ghost, "demo", "ghost-lane.md").await;
        let held = mailbox.active_reservations("demo").await.expect("active");
        let lease = held
            .iter()
            .find(|r| r.path_pattern == "ghost-lane.md")
            .expect("ghost lease is held");
        assert!(
            lease.agent_name.is_empty(),
            "roster miss leaves the name empty, no invented prefix: {:?}",
            lease.agent_name
        );
        assert_ne!(
            lease.agent_name, "CobaltVireo",
            "a missing name must never compare as a peer pin"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ow6c: a roster miss leaves an intent holder name EMPTY — the same
    /// invented-prefix fallback `fill_agent_names` already lost. Empty never
    /// compares as a peer pin.
    #[tokio::test]
    async fn roster_miss_leaves_intent_holder_name_empty() {
        let (mailbox, db_path, data_dir) = test_mailbox("intent-empty").await;
        mailbox
            .ensure_project("demo")
            .await
            .expect("ensure project");
        // Keys with NO profile and NO roster row: both name sources miss.
        let ghost = crate::keys::generate();
        publish_raw_intent(&mailbox, &ghost, "demo", "ghost-plan.md").await;
        let intents = mailbox.active_intents("demo").await.expect("intents");
        let intent = intents
            .iter()
            .find(|i| i.path_pattern == "ghost-plan.md")
            .expect("ghost intent is boarded");
        assert!(
            intent.agent_name.is_empty(),
            "roster miss leaves the name empty, no invented prefix: {:?}",
            intent.agent_name
        );
        assert_ne!(
            intent.agent_name, "CobaltVireo",
            "a missing name must never compare as a peer pin"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ow6c: a roster miss leaves the kernel blocker name EMPTY — the same
    /// invented-prefix fallback `fill_agent_names` already lost. The conflict
    /// report carries the empty holder; empty never compares as a peer pin.
    #[tokio::test]
    async fn roster_miss_leaves_conflict_holder_name_empty() {
        let (mailbox, db_path, data_dir) = test_mailbox("blocker-empty").await;
        register(&mailbox, "demo", "RoseDune").await;
        // Keys with NO profile and NO roster row: both name sources miss.
        let ghost = crate::keys::generate();
        publish_raw_stale_lease(&mailbox, &ghost, "demo", "ghost-hold.md").await;
        let report = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["ghost-hold.md".into()])
            .await
            .expect("conflicts");
        assert!(
            !report.conflict_free,
            "the ghost lease holds the path: {report:?}"
        );
        let holders: Vec<&str> = report
            .conflicts
            .iter()
            .flat_map(|c| c.holders.iter().map(String::as_str))
            .collect();
        assert_eq!(
            holders,
            vec![""],
            "roster miss stays empty, no invented prefix: {holders:?}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-ow6c: ONE project key on both halves of the yield join. The marker
    /// table was written and read RAW while the lease board was canonicalized,
    /// so a marker written under a path-form spelling was invisible to the
    /// join — and the loop resumed a bead a live peer held.
    #[tokio::test]
    async fn yield_markers_join_the_lease_board_under_one_canonical_project_key() {
        let (mailbox, db_path, data_dir) = test_mailbox("yield-canonical").await;
        mailbox
            .ensure_project("demo")
            .await
            .expect("ensure project");
        let pool = mailbox.index_pool.as_ref().expect("pool").clone();
        sqlx::query(
            "INSERT INTO project_aliases (alias, canonical, created_ts_us) \
             VALUES (?1, ?2, ?3)",
        )
        .bind("/w/demo")
        .bind("demo")
        .bind(1_i64)
        .execute(&pool)
        .await
        .expect("seed the alias");
        mailbox
            .record_reservation_yield("/w/demo", "Yielder", "yield-bead-1", "Holder", "note")
            .await
            .expect("record the yield");
        let (views, _) = mailbox
            .reservation_yield_board("demo")
            .await
            .expect("the yield board");
        assert_eq!(
            views.len(),
            1,
            "the marker joins the canonical board: {views:?}"
        );
        assert_eq!(views[0].bead_id, "yield-bead-1");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> PublicKey {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        PublicKey::from_hex(&agent.npub).expect("npub")
    }

    /// bd-gc7k: a lease whose holder pin is DEAD (pane gone — the prune
    /// list from `cleanup_pane_identities`) must not own the ledger
    /// forever. The cleanup cascade revokes every exclusive held lease of
    /// the dead pins (operator-signed), and a peer's acquire of the same
    /// path then SUCCEEDS. never-block-on-the-human.
    #[tokio::test]
    async fn test_ghost_holder_cleanup_cascades_to_release() {
        let (mailbox, _db, _data) = test_mailbox("gc7k").await;
        register(&mailbox, "demo", "CoralFinch").await;
        register(&mailbox, "demo", "CrimsonIsland").await;

        // The ghost acquires the shared ledger exclusively.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "CoralFinch",
                &[".beads/issues.jsonl".into()],
                Some(3600),
                true,
                Some("ghost"),
            )
            .await
            .expect("ghost acquire");
        assert_eq!(acq.granted.len(), 1);

        // A peer is hard-blocked while the holder lives.
        let blocked = mailbox
            .file_reservation_paths(
                "demo",
                "CrimsonIsland",
                &[".beads/issues.jsonl".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("peer acquire");
        assert!(
            blocked.granted.is_empty() && !blocked.conflicts.is_empty(),
            "{blocked:?}"
        );

        // The pane dies; the prune names the pin. The cascade MUST revoke
        // every exclusive held lease of the dead pins.
        let released = mailbox
            .reap_ghost_leases(&["CoralFinch".to_string()], Some("pane-pruned"))
            .await
            .expect("reap");
        assert_eq!(released.len(), 1, "{released:?}");
        assert_eq!(released[0].holder, "CoralFinch");
        assert_eq!(released[0].path_pattern, ".beads/issues.jsonl");
        assert_eq!(released[0].project_slug, "demo");

        // Acquire now succeeds — the ghost no longer owns the ledger.
        let now_ok = mailbox
            .file_reservation_paths(
                "demo",
                "CrimsonIsland",
                &[".beads/issues.jsonl".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("post-reap acquire");
        assert_eq!(now_ok.granted.len(), 1, "{now_ok:?}");

        // Idempotent: a second reap has nothing to release.
        let again = mailbox
            .reap_ghost_leases(&["CoralFinch".to_string()], None)
            .await
            .expect("reap again");
        assert!(again.is_empty(), "{again:?}");

        // A live holder is NEVER reaped — only pins in the prune list are.
        let untouched = mailbox
            .reap_ghost_leases(&["CrimsonIsland".to_string()], None)
            .await
            .expect("reap live");
        assert!(
            untouched.is_empty(),
            "live holder must survive: {untouched:?}"
        );
    }

    #[test]
    fn test_symmetric_conflict_both_directions() {
        // src/** matches src/lib.rs; src/lib.rs matches src/** (symmetric).
        assert!(overlaps("src/**", "src/lib.rs"));
        assert!(overlaps("src/lib.rs", "src/**"), "symmetric the other way");
        // No overlap.
        assert!(!overlaps("src/**", "docs/x.md"));
        // Distinct files.
        assert!(!overlaps("src/a.rs", "src/b.rs"));
    }

    /// bd-reservations-not-projected-5ohc: a granted lease must reach the
    /// durable SQLite projection (`file_reservations`) that external readers
    /// query — not only the bus replay. Release must mark the row
    /// `released`, and a re-acquire must flip the SAME row back to `active`
    /// (idempotent upsert on the UNIQUE coordinate).
    #[tokio::test]
    async fn test_grant_and_release_project_to_index() {
        let (mailbox, db_path, _data_dir) = test_mailbox("project").await;
        register(&mailbox, "demo", "ProbePin").await;

        async fn row_status(db_path: &std::path::Path) -> Option<String> {
            let pool = schema::connect(db_path.to_str().unwrap())
                .await
                .expect("connect");
            sqlx::query_scalar("SELECT status FROM file_reservations WHERE path_pattern=?")
                .bind("t.txt")
                .fetch_optional(&pool)
                .await
                .expect("query")
        }

        mailbox
            .file_reservation_paths(
                "demo",
                "ProbePin",
                &["t.txt".to_string()],
                None,
                true,
                Some("bd-reservations-not-projected-5ohc"),
            )
            .await
            .expect("acquire");
        assert_eq!(
            row_status(&db_path).await.as_deref(),
            Some("active"),
            "reservation not projected"
        );

        mailbox
            .release_file_reservations("demo", "ProbePin", &["t.txt".to_string()])
            .await
            .expect("release");
        assert_eq!(row_status(&db_path).await.as_deref(), Some("released"));

        // Re-acquire flips the same row back to active (UNIQUE coordinate).
        mailbox
            .file_reservation_paths(
                "demo",
                "ProbePin",
                &["t.txt".to_string()],
                None,
                true,
                Some("bd-reservations-not-projected-5ohc"),
            )
            .await
            .expect("re-acquire");
        assert_eq!(row_status(&db_path).await.as_deref(), Some("active"));
    }

    /// bd-pin-public-plane-s-easy.13: lease transfer — a grant is a lease
    /// transfer, never "commit as me". A releases, B acquires the SAME path
    /// (exclusive + reason preserved); after transfer A's guard no longer
    /// covers the path and B's does. B commits as B.
    #[tokio::test]
    async fn test_lease_transfer_moves_coverage_from_a_to_b() {
        let (mailbox, db_path, data_dir) = test_mailbox("transfer").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // A acquires src/transfer.rs exclusively.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/transfer.rs".to_string()],
                None,
                true,
                Some("handoff"),
            )
            .await
            .expect("A acquire");
        assert_eq!(acq.granted.len(), 1);

        // Transfer: A → B, same path, reason preserved.
        let transfer = mailbox
            .transfer_reservation("demo", "BlueLake", "RoseDune", "src/transfer.rs", None)
            .await
            .expect("transfer");
        assert_eq!(transfer.path_pattern, "src/transfer.rs");
        assert_eq!(transfer.from_agent, "BlueLake");
        assert_eq!(transfer.to_agent, "RoseDune");

        // B now holds the path (exclusive, reason preserved).
        let holders: Vec<Reservation> = mailbox.active_reservations("demo").await.expect("active");
        let b_lease = holders
            .iter()
            .find(|r| r.path_pattern == "src/transfer.rs")
            .expect("path still leased");
        assert_eq!(b_lease.agent_name, "RoseDune", "B holds after transfer");
        assert!(b_lease.exclusive, "exclusivity preserved");
        assert_eq!(b_lease.reason, "handoff", "reason preserved");

        // A cannot re-acquire the same exclusive path (B holds it).
        let reacquire = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/transfer.rs".to_string()],
                None,
                true,
                None,
            )
            .await
            .expect("A reacquire attempt (hard conflict, granted empty)");
        assert!(
            reacquire.granted.is_empty(),
            "A must NOT be granted the transferred path"
        );
        assert_eq!(
            reacquire.conflicts.len(),
            1,
            "hard conflict names B's lease"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4dl4: a holder who took a broad GLOB must be visible to a second
    /// agent asking about a literal path INSIDE it. The matrix is the bead's,
    /// unchanged, including the row that makes the diagnosis unambiguous:
    /// asking about the IDENTICAL glob has to collide, and if it does, the
    /// request reached the board and the defect is in the comparison; if it
    /// does not, the request never reached the board at all.
    ///
    /// A false clear here is not cosmetic. The pre-commit guard blocks a
    /// commit whose paths another agent holds, and AGENTS.md tells an agent to
    /// read this board when blocked, so a matcher that cannot see glob-vs-
    /// literal overlap makes the guard blind in exactly the case it exists
    /// for. The broad claim is the NORMAL form here, because the repo's own
    /// instructions tell agents to reserve with globs.
    #[tokio::test]
    async fn test_glob_holder_is_visible_to_a_literal_inside_it() {
        const HELD: &str = "crates/*/src/*.rs";
        const MATRIX: &[(&str, bool)] = &[
            ("crates/doorbell-rt/src/store.rs", true),
            ("crates/doorbell-rt/src/*.rs", true),
            (HELD, true),
            ("crates/other/src/x.rs", true),
            ("docs/readme.md", false),
        ];

        let (mailbox, db_path, data_dir) = test_mailbox("glob-matrix").await;
        register(&mailbox, "demo", "IvoryRidge").await;
        register(&mailbox, "demo", "TealMarlin").await;

        // Granted, so the broad claim really IS on the board. A test that
        // silently asserts a conflict on an empty board would pass for the
        // wrong reason.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "IvoryRidge",
                &[HELD.to_string()],
                Some(3600),
                true,
                Some("bd-4dl4"),
            )
            .await
            .expect("acquire");
        assert_eq!(
            acq.granted.len(),
            1,
            "the broad claim is GRANTED, so it is really on the board: {acq:?}"
        );

        let report = mailbox
            .check_file_reservation_conflicts(
                "demo",
                "TealMarlin",
                &MATRIX
                    .iter()
                    .map(|(p, _)| (*p).to_string())
                    .collect::<Vec<_>>(),
            )
            .await
            .expect("conflicts");

        let conflicted: Vec<&str> = report.conflicts.iter().map(|c| c.path.as_str()).collect();
        for (path, should_conflict) in MATRIX {
            assert_eq!(
                conflicted.contains(path),
                *should_conflict,
                "`{path}` should {}. conflict_free={} clear_paths={:?} conflicts={:?} \
                 soft={:?} total={}",
                if *should_conflict {
                    "REPORT"
                } else {
                    "NOT report"
                },
                report.conflict_free,
                report.clear_paths,
                conflicted,
                report
                    .soft_conflicts
                    .iter()
                    .map(|c| c.path.as_str())
                    .collect::<Vec<_>>(),
                report.total_conflicting_reservations
            );
        }
        // A holder is only ever NAMED, never a self-match: this check is about
        // other agents' claims.
        for c in &report.conflicts {
            assert!(
                c.holders.iter().any(|h| h == "IvoryRidge"),
                "the holder is named on `{}`, so the board really was read: {c:?}",
                c.path
            );
        }

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4dl4, the part that IS a real defect. The bead's repro omitted
    /// `--exclusive`, which is a bare bool defaulting to false, so the lease
    /// it took was SHARED. The matcher was never broken: the identical glob
    /// collides correctly as soon as the lease is exclusive.
    ///
    /// The real bug is one layer down and worse for being quiet. A live shared
    /// lease matched no branch of the decision, so it appeared in NEITHER tier
    /// and the path came back in `clear_paths` with `conflict_free: true` and
    /// `has_soft_conflicts: false`. A reader has no way to tell that from
    /// "checked, nobody here" — which is the confident wrong answer a
    /// coordination primitive must never produce. It must now surface in the
    /// ADVISORY tier: still not a fence, but named.
    #[tokio::test]
    async fn test_a_shared_lease_is_advisory_not_invisible() {
        let (mailbox, db_path, data_dir) = test_mailbox("shared-soft").await;
        register(&mailbox, "demo", "IvoryRidge").await;
        register(&mailbox, "demo", "TealMarlin").await;

        // NOT --exclusive: this is the case the bead's repro actually created.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "IvoryRidge",
                &["crates/*/src/*.rs".to_string()],
                Some(3600),
                false,
                Some("shared"),
            )
            .await
            .expect("acquire shared");
        assert_eq!(acq.granted.len(), 1);
        assert!(
            !acq.granted[0].exclusive,
            "the lease really is shared, or this test proves nothing"
        );

        let report = mailbox
            .check_file_reservation_conflicts(
                "demo",
                "TealMarlin",
                &[
                    "crates/doorbell-rt/src/store.rs".to_string(),
                    "docs/readme.md".to_string(),
                ],
            )
            .await
            .expect("conflicts");

        // Still not a fence: a shared lease must not change who gets denied.
        assert!(
            report.conflict_free,
            "a shared lease does not fence anyone: {:?}",
            report.conflicts
        );
        assert!(
            report.conflicts.is_empty(),
            "and it must not be promoted into the hard tier"
        );

        // But it must be NAMED. This is the assertion that fails before the
        // fix, and it is the whole point: `clear_paths` claimed this path was
        // free while another agent had declared they were working in it.
        assert!(
            report.has_soft_conflicts,
            "a live shared lease is advisory, so it must surface: \
             soft={:?} clear={:?}",
            report.soft_conflicts, report.clear_paths
        );
        let soft_paths: Vec<&str> = report
            .soft_conflicts
            .iter()
            .map(|c| c.path.as_str())
            .collect();
        assert!(
            soft_paths.contains(&"crates/doorbell-rt/src/store.rs"),
            "the declared path is named, with its holder: {:?}",
            report.soft_conflicts
        );
        assert!(
            !soft_paths.contains(&"docs/readme.md"),
            "a genuinely disjoint path stays clear"
        );
        assert!(
            report.clear_paths.contains(&"docs/readme.md".to_string()),
            "and lands in clear_paths, so clear_paths now means 'nobody holds \
             or declared it': {:?}",
            report.clear_paths
        );
        assert!(
            report.soft_conflicts[0]
                .holders
                .iter()
                .any(|h| h == "IvoryRidge"),
            "the shared holder is named, so the board really was read: {:?}",
            report.soft_conflicts[0]
        );
        // Own leases are not an advisory signal against yourself.
        let own = mailbox
            .check_file_reservation_conflicts(
                "demo",
                "IvoryRidge",
                &["crates/x/src/y.rs".to_string()],
            )
            .await
            .expect("own conflicts");
        assert!(
            !own.has_soft_conflicts && own.clear_paths.contains(&"crates/x/src/y.rs".to_string()),
            "your own shared lease is not a conflict with yourself: {own:?}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-4dl4, the separating half: if the PURE matcher already says these
    /// overlap, the board is at fault; if it does not, the matcher is. Keeping
    /// both tests means a fix that patches the wrong one is caught here rather
    /// than three layers up in a guard message nobody reads.
    #[test]
    fn test_glob_matcher_agrees_with_the_matrix() {
        const HELD: &str = "crates/*/src/*.rs";
        for (path, should_conflict) in [
            ("crates/doorbell-rt/src/store.rs", true),
            ("crates/doorbell-rt/src/*.rs", true),
            (HELD, true),
            ("crates/other/src/x.rs", true),
            ("docs/readme.md", false),
        ] {
            assert_eq!(
                super::overlaps(HELD, path),
                should_conflict,
                "the pure matcher disagrees on `{path}`"
            );
        }
    }

    /// bd-pin-public-plane-s-easy.13 unhappy path: transferring a path the
    /// `from` agent does NOT hold fails loudly (never a silent no-op).
    #[tokio::test]
    async fn test_lease_transfer_requires_holder() {
        let (mailbox, db_path, data_dir) = test_mailbox("transfer-nohold").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/held.rs".to_string()],
                None,
                true,
                None,
            )
            .await
            .expect("A acquire");

        // RoseDune is not the holder — transfer must fail, not silently pass.
        let err = mailbox
            .transfer_reservation("demo", "RoseDune", "BlueLake", "src/held.rs", None)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("held by BlueLake"),
            "error names the real holder: {err}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_acquire_conflicts_check() {
        let (mailbox, db_path, data_dir) = test_mailbox("acquire").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // BlueLake acquires src/**.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                Some("auth"),
            )
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1);
        assert!(acq.conflicts.is_empty());

        // RoseDune conflicts on src/lib.rs (symmetric).
        let report = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["src/lib.rs".into()])
            .await
            .expect("conflicts");
        assert!(!report.conflict_free);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].path, "src/lib.rs");
        assert!(
            report.conflicts[0].holders.iter().any(|h| h == "BlueLake"),
            "holder named"
        );

        // RoseDune's own acquire of src/lib.rs → conflict.
        let acq2 = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/lib.rs".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire 2");
        assert!(acq2.granted.is_empty());
        assert_eq!(acq2.conflicts.len(), 1);

        // A clear path for RoseDune.
        let clear = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["docs/x.md".into()])
            .await
            .expect("clear");
        assert!(clear.conflict_free);
        assert!(clear.clear_paths.contains(&"docs/x.md".to_string()));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-lease-liveness-bo0g: an EXPIRED lease whose holder's session is
    /// still LIVE keeps blocking — the TTL outlived the work, not the work
    /// the TTL. A second agent's conflict check (and acquire) against a
    /// path being actively edited by a live peer MUST name that peer as
    /// holder. No liveness oracle installed → pure NIP-40 semantics
    /// (expired = clear), so every pre-existing behavior is preserved.
    #[tokio::test]
    async fn expired_lease_blocks_iff_holder_session_live() {
        let (mailbox, db_path, data_dir) = test_mailbox("liveness").await;
        let holder = register(&mailbox, "demo", "BlueLake").await;
        let peer = register(&mailbox, "demo", "RoseDune").await;
        let holder_hex = holder.to_hex();

        // Holder acquires two paths: one with a 1s TTL (expires below), one
        // with a healthy TTL (control — never expires mid-test).
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/fleeting.rs".into()],
                Some(1),
                true,
                Some("bd-liveness"),
            )
            .await
            .expect("acquire fleeting");
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/stable.rs".into()],
                Some(3600),
                true,
                Some("bd-liveness"),
            )
            .await
            .expect("acquire stable");
        tokio::time::sleep(std::time::Duration::from_millis(1300)).await;

        let check = |mailbox: &Mailbox| {
            let mailbox = mailbox.clone();
            async move {
                mailbox
                    .check_file_reservation_conflicts(
                        "demo",
                        "RoseDune",
                        &["src/fleeting.rs".into(), "src/stable.rs".into()],
                    )
                    .await
                    .expect("conflict check")
            }
        };

        // Phase 1 — no liveness oracle: expired = clear (NIP-40 baseline),
        // the healthy lease still blocks regardless.
        let report = check(&mailbox).await;
        assert!(
            report.clear_paths.contains(&"src/fleeting.rs".to_string()),
            "baseline: expired lease clears without an oracle"
        );
        assert!(
            report.conflicts.iter().any(|c| c.path == "src/stable.rs"),
            "healthy lease unaffected"
        );

        // Phase 2 — oracle says BlueLake's session is LIVE: the expired
        // lease MUST block again, naming BlueLake, and refuse RoseDune's
        // acquire of the same path.
        install_lease_liveness_checker(std::sync::Arc::new(move |pubkey_hex| {
            pubkey_hex == holder_hex
        }));
        let report = check(&mailbox).await;
        let hit = report
            .conflicts
            .iter()
            .find(|c| c.path == "src/fleeting.rs")
            .expect("live holder keeps the expired lease blocking");
        assert!(
            hit.holders.iter().any(|h| h == "BlueLake"),
            "peer named as holder: {hit:?}"
        );
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/fleeting.rs".into()],
                Some(600),
                true,
                None,
            )
            .await
            .expect("acquire attempt");
        assert!(
            acq.granted.is_empty() && !acq.conflicts.is_empty(),
            "acquire refused while live peer holds: {acq:?}"
        );

        // Phase 3 — oracle says the session is GONE: back to clear.
        install_lease_liveness_checker(std::sync::Arc::new(|_| false));
        let report = check(&mailbox).await;
        assert!(
            report.clear_paths.contains(&"src/fleeting.rs".to_string()),
            "gone holder's expired lease clears: {:?}",
            report.conflicts
        );
        clear_lease_liveness_checker();

        // The peer's own identity never trips its own check (sanity).
        let own = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["docs/own.md".into()])
            .await
            .expect("own check");
        assert!(own.conflict_free);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = peer;
    }

    /// bd-slot-visibility-gv17: the pre-write collision check must expose
    /// ACTIVE build-slot holders — an agent about to run the VERIFY fence
    /// discovers a held cargo slot in ONE call. Own slots are excluded
    /// (own never conflicts); a released slot disappears from the report.
    #[tokio::test]
    async fn conflict_check_exposes_active_slot_holders() {
        let (mailbox, db_path, data_dir) = test_mailbox("slot-visibility").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // BlueLake holds the cargo slot.
        mailbox
            .acquire_build_slot("demo", "BlueLake", "cargo", Some(3600), true)
            .await
            .expect("acquire cargo");

        // RoseDune's pre-write check on an UNRELATED path still sees the
        // fence is busy.
        let report = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["docs/x.md".into()])
            .await
            .expect("conflicts");
        let holder = report
            .slot_holders
            .iter()
            .find(|s| s.slot == "cargo")
            .expect("cargo holder visible in one call");
        assert_eq!(holder.agent_name, "BlueLake");
        assert!(holder.exclusive);
        assert!(holder.expires_ts_us.is_some());

        // Own slots never surface (own never conflicts).
        let own = mailbox
            .check_file_reservation_conflicts("demo", "BlueLake", &["docs/x.md".into()])
            .await
            .expect("own conflicts");
        assert!(
            own.slot_holders.iter().all(|s| s.slot != "cargo"),
            "own slot excluded: {:?}",
            own.slot_holders
        );

        // Released → gone.
        mailbox
            .release_build_slot("demo", "BlueLake", "cargo")
            .await
            .expect("release cargo");
        let after = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["docs/x.md".into()])
            .await
            .expect("after release");
        assert!(after.slot_holders.is_empty(), "{:?}", after.slot_holders);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_own_reservation_never_conflicts() {
        let (mailbox, db_path, data_dir) = test_mailbox("own").await;
        register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        // Same agent re-checking its own path → no conflict.
        let report = mailbox
            .check_file_reservation_conflicts("demo", "BlueLake", &["src/lib.rs".into()])
            .await
            .expect("conflicts");
        assert!(report.conflict_free, "own reservation never blocks");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// P0 regression (bd-1pzd): TWO DISTINCT agents on the SAME exclusive
    /// path — the corruption-prevention floor. The second acquire MUST be
    /// hard-blocked (conflicts, granted 0); the conflict check MUST see the
    /// first holder. NIP-33 dedup is per-(kind, pubkey, d): two authors on
    /// one d are TWO coordinates, both leases survive the projection.
    #[tokio::test]
    async fn test_two_agents_same_exclusive_path_hard_blocks() {
        let (mailbox, db_path, data_dir) = test_mailbox("p0two").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // A acquires the exclusive path.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/shared/*".into()],
                Some(3600),
                true,
                Some("a"),
            )
            .await
            .expect("A acquire");
        assert_eq!(acq.granted.len(), 1);
        assert!(acq.conflicts.is_empty());

        // B's acquire of the SAME path → HARD block.
        let acq2 = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/shared/*".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("B acquire");
        assert!(
            acq2.granted.is_empty(),
            "B must NOT be granted the same exclusive path: {acq2:?}"
        );
        assert_eq!(acq2.conflicts.len(), 1, "{acq2:?}");
        assert_eq!(acq2.conflicts[0].path, "src/shared/*");
        assert!(
            acq2.conflicts[0].holders.iter().any(|h| h == "BlueLake"),
            "holder must be A: {:?}",
            acq2.conflicts[0].holders
        );

        // A third party's conflict check sees A's hold.
        register(&mailbox, "demo", "MistyDesert").await;
        let report = mailbox
            .check_file_reservation_conflicts("demo", "MistyDesert", &["src/shared/*".into()])
            .await
            .expect("third-party conflicts");
        assert!(!report.conflict_free);
        assert_eq!(report.conflicts.len(), 1);
        assert!(
            report.conflicts[0].holders.iter().any(|h| h == "BlueLake"),
            "{:?}",
            report.conflicts[0].holders
        );

        // A's OWN re-check stays clear (own lease never self-conflicts).
        let own = mailbox
            .check_file_reservation_conflicts("demo", "BlueLake", &["src/shared/*".into()])
            .await
            .expect("own conflicts");
        assert!(own.conflict_free);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Regression (bug hunt 2026-08-17): an acquire whose free-text `reason`
    /// contains the substring "released" was classified Released by
    /// `lease_status`'s bare `content.contains("released")` — the lease
    /// vanished from the held view and the P0 exclusive-path conflict floor
    /// silently stopped blocking the second agent. Only canonical marker
    /// forms may classify (mirror of is_revocation's plain/JSON/tag forms).
    #[tokio::test]
    async fn test_acquire_with_released_in_reason_still_blocks() {
        let (mailbox, db_path, data_dir) = test_mailbox("released-reason").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // A acquires with a reason that mentions "released".
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/shared/*".into()],
                Some(3600),
                true,
                Some("cleanup after v2 released"),
            )
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1, "A's acquire must grant: {acq:?}");

        // B must still hard-block on the same exclusive path.
        let acq2 = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/shared/*".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("B acquire");
        assert!(
            acq2.granted.is_empty(),
            "reason text must NOT hide a held lease: {acq2:?}"
        );
        assert_eq!(acq2.conflicts.len(), 1, "{acq2:?}");

        // The held view still lists A's lease.
        register(&mailbox, "demo", "MistyPalms").await;
        let report = mailbox
            .check_file_reservation_conflicts("demo", "MistyPalms", &["src/shared/*".into()])
            .await
            .expect("held view");
        assert!(
            !report.conflict_free,
            "held lease must be visible: {report:?}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-mbh0 acceptance gate (the TanFern repro): a live lease whose
    /// remaining TTL already exceeds extend_secs must STILL count as
    /// renewed — the operation converges every live lease forward and
    /// `renewed` equals the holder's live lease set. Under the old ratchet
    /// this returned `renewed:0`, which callers read as failure while the
    /// leases later lapsed un-renewed mid-VERIFY.
    #[tokio::test]
    async fn test_renew_counts_healthy_leases_bd_mbh0() {
        let (mailbox, db_path, data_dir) = test_mailbox("renew-mbh0").await;
        register(&mailbox, "demo", "BlueLake").await;

        // Long-TTL lease: remaining (~7200s) >> extend (3600s) — the old
        // ratchet returned renewed:0 exactly here.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(7200),
                true,
                None,
            )
            .await
            .expect("acquire long");
        assert_eq!(acq.granted.len(), 1);

        let renew = mailbox
            .renew_file_reservations("demo", "BlueLake", &[], 3600)
            .await
            .expect("renew");
        assert_eq!(
            renew.renewed, 1,
            "healthy lease counts as renewed (bd-mbh0)"
        );
        assert_eq!(renew.reservations.len(), 1);
        assert_eq!(
            renew.reservations[0].expires_ts_us, acq.granted[0].expires_ts_us,
            "already-healthy expiry stays put (converged), never double-extended"
        );

        // Near-lapse lease on another path takes the republish path:
        // ABSOLUTE extension (old + extend), expiry visibly moves forward.
        let short = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["docs/**".into()],
                Some(60),
                true,
                None,
            )
            .await
            .expect("acquire short");
        let old_short = short.granted[0].expires_ts_us.expect("short expiry");

        let renew2 = mailbox
            .renew_file_reservations("demo", "BlueLake", &[], 3600)
            .await
            .expect("renew 2");
        assert_eq!(
            renew2.renewed, 2,
            "renewed equals the live lease set (both paths)"
        );
        assert_eq!(renew2.reservations.len(), 2);
        let moved = renew2
            .reservations
            .iter()
            .find(|r| r.path_pattern == "docs/**")
            .expect("short lease reported");
        let new_exp = moved.expires_ts_us.expect("moved expiry");
        assert!(new_exp > old_short, "near-lapse expiry moved forward");
        assert!(
            (new_exp - old_short) / 1_000_000 >= 3600,
            "absolute extension: new = old + extend (got +{}s)",
            (new_exp - old_short) / 1_000_000
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn renew_extends_from_absolute_expiry_not_double_counting() {
        // Regression pin (parity bug bd-XXX): renewal must set the new expiry
        // to the OLD absolute expiry PLUS the extension — treating
        // `expires_ts_us` as a relative duration and re-adding to `now`
        // double-counts elapsed time (2x expiry). Acquire TTL 3600, renew
        // +7200: the renewed lease must expire at old+7200, NOT ~2x-old.
        let (mailbox, db_path, data_dir) = test_mailbox("renew-abs").await;
        register(&mailbox, "demo", "BlueLake").await;

        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        let old_expiry_us = acq.granted[0]
            .expires_ts_us
            .expect("acquired lease has expiry");

        let renew = mailbox
            .renew_file_reservations("demo", "BlueLake", &["src/**".into()], 7200)
            .await
            .expect("renew");
        assert_eq!(renew.renewed, 1);
        assert_eq!(renew.reservations.len(), 1);
        let expected_us = old_expiry_us + 7_200_000_000;
        assert_eq!(
            renew.reservations[0].expires_ts_us,
            Some(expected_us),
            "renewed expiry must be OLD absolute expiry + extend, not now+extend or 2x (bd-XXX regression); old={old_expiry_us} got={:?}",
            renew.reservations[0].expires_ts_us,
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_renew_and_release() {
        let (mailbox, db_path, data_dir) = test_mailbox("renew").await;
        register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");

        let renew = mailbox
            .renew_file_reservations("demo", "BlueLake", &["src/**".into()], 7200)
            .await
            .expect("renew");
        assert_eq!(renew.renewed, 1);
        assert_eq!(renew.reservations.len(), 1);

        let release = mailbox
            .release_file_reservations("demo", "BlueLake", &["src/**".into()])
            .await
            .expect("release");
        assert_eq!(release.released, 1);

        // After release: no active reservation for that path.
        let res = mailbox.active_reservations("demo").await.expect("active");
        assert!(
            res.iter().all(|r| r.path_pattern != "src/**"),
            "released lease must not appear active, got {res:?}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// S6 (bd-release-breakdown-xom3): the release response lists each
    /// released lease — path, mode, holder — so callers need not re-run
    /// `check_file_reservation_conflicts` to see what was released.
    #[tokio::test]
    async fn test_release_reports_per_path_breakdown() {
        let (mailbox, db_path, data_dir) = test_mailbox("release-breakdown").await;
        register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into(), "tests/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire exclusive");
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["docs/**".into()],
                Some(3600),
                false,
                None,
            )
            .await
            .expect("acquire shared");

        let release = mailbox
            .release_file_reservations("demo", "BlueLake", &[])
            .await
            .expect("release all");
        assert_eq!(release.released, 3);
        assert_eq!(release.leases.len(), 3);
        let by_path: std::collections::BTreeMap<&str, &ReleasedLease> = release
            .leases
            .iter()
            .map(|l| (l.path.as_str(), l))
            .collect();
        assert_eq!(by_path["src/**"].mode, "exclusive");
        assert_eq!(by_path["tests/**"].mode, "exclusive");
        assert_eq!(by_path["docs/**"].mode, "shared");
        assert_eq!(by_path["src/**"].holder, "BlueLake");
        assert_eq!(by_path["docs/**"].holder, "BlueLake");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_force_release_notifies_holder_and_audits_git() {
        let (mailbox, db_path, data_dir) = test_mailbox("notify").await;
        register(&mailbox, "demo", "RoseDune").await;

        // A lease held by an agent with NO profile → is_holder_stale = true
        // (unknown agent), so the full force_release path runs end-to-end:
        // operator revocation + NIP-17 notify + Git audit transition.
        let holder = crate::keys::generate();
        let d = "demo:src/**";
        let holder_tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![Tag::parse(["d", d]).expect("d")],
            Some("demo"),
        );
        let lease = build_event(
            &holder,
            toron_kind(FILE_RESERVATION),
            r#"{"exclusive":true}"#,
            holder_tags,
            Timestamp::now(),
        )
        .expect("lease");
        mailbox
            .bus
            .publish(lease.clone())
            .await
            .expect("publish lease");

        mailbox
            .force_release_file_reservation("demo", "RoseDune", "src/**", None, Some("gone"), true)
            .await
            .expect("force release stale holder");

        // Projection resolves to the operator revocation (both events present).
        let events = collect_replay(
            mailbox.bus.as_ref(),
            kind_project_filter(toron_kind(FILE_RESERVATION), "demo"),
        )
        .await
        .expect("collect");
        let operator = mailbox.operator().unwrap();
        let resolved =
            resolve_coordinate(events.iter(), Some(&operator.public_key())).expect("resolved");
        assert!(is_revocation(&resolved), "projection shows revoked");
        assert!(
            events.iter().any(|e| e.id == lease.id),
            "agent lease still present"
        );

        // Git archive has the revoked transition record (acquired→revoked).
        let audit_dir = data_dir.join("demo").join(".audit");
        let files: Vec<_> = std::fs::read_dir(&audit_dir)
            .map(|rd| {
                rd.flatten()
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            files.iter().any(|f| f.contains("revoked")),
            "audit transition record present, got {files:?}"
        );

        // Previous holder received a NIP-17 notify (a wrap addressed to them).
        let notify_filter = ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(holder.public_key())
            .build();
        let wraps = collect_replay(mailbox.bus.as_ref(), notify_filter)
            .await
            .expect("notify wraps");
        assert!(
            wraps.iter().any(|w| {
                crate::nostr::gift_wrap::unwrap(&holder, w)
                    .map(|g| {
                        g.rumor.tags.iter().any(|t| {
                            t.as_slice()
                                == ["toron-kind".to_string(), "reservation-revoked".to_string()]
                        })
                    })
                    .unwrap_or(false)
            }),
            "holder notified via NIP-17 reservation-revoked"
        );

        // No cross-key NIP-09 anywhere.
        assert!(events.iter().all(|e| e.kind.as_u16() != 5));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ttl_expiry_clears_active_lease() {
        // Publish a lease with a past expiration → not active.
        let (mailbox, db_path, data_dir) = test_mailbox("ttl").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;

        let d = "demo:src/**";
        let tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", d]).expect("d"),
                Tag::expiration(Timestamp::from(1_500_000_000)),
            ],
            Some("demo"),
        );
        let ev = build_event(
            &crate::nostr::Keys::new(
                crate::nostr::SecretKey::from_hex(
                    "6b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e",
                )
                .expect("sk"),
            ),
            toron_kind(FILE_RESERVATION),
            r#"{"exclusive":true}"#,
            tags,
            Timestamp::from(1_400_000_000),
        )
        .expect("build");
        mailbox.bus.publish(ev).await.expect("publish");

        let res = mailbox.active_reservations("demo").await.expect("active");
        assert!(res.is_empty(), "expired lease must not be active");
        let _ = (blue,);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_declare_intent_publishes_status_intent_and_boards() {
        let (mailbox, db_path, data_dir) = test_mailbox("intent-declare").await;
        register(&mailbox, "demo", "BlueLake").await;

        let intent = mailbox
            .declare_intent(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some("auth rewrite"),
                None,
            )
            .await
            .expect("declare");
        assert_eq!(intent.declared.len(), 1);
        let ev_id = intent.declared[0].id.clone();

        // Bus event carries the status:intent tag.
        let events = collect_replay(
            mailbox.bus.as_ref(),
            kind_project_filter(toron_kind(FILE_RESERVATION), "demo"),
        )
        .await
        .expect("collect");
        let ev = events
            .iter()
            .find(|e| e.id.to_hex() == ev_id)
            .expect("intent event on bus");
        assert_eq!(
            lease_status(ev),
            ReservationStatus::Intent,
            "status tag classifies as intent"
        );

        // Intent board shows it (in-TTL), agent named.
        let intents = mailbox.active_intents("demo").await.expect("intents");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].agent_name, "BlueLake");
        assert_eq!(intents[0].path_pattern, "src/**");
        assert_eq!(intents[0].ttl_secs, INTENT_TTL_SECS);

        // The intent is NOT a held reservation.
        let held = mailbox.active_reservations("demo").await.expect("held");
        assert!(held.is_empty(), "intent must not appear as held");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_acquire_against_intent_is_soft_not_hard() {
        let (mailbox, db_path, data_dir) = test_mailbox("soft-acquire").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // BlueLake declares an intent on src/**.
        mailbox
            .declare_intent("demo", "BlueLake", &["src/**".into()], None, None)
            .await
            .expect("declare");

        // RoseDune acquires src/lib.rs → SOFT conflict (frozen), NOT a hard block.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/lib.rs".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        assert!(acq.granted.is_empty(), "soft conflict freezes the acquire");
        assert!(acq.conflicts.is_empty(), "NOT a hard block");
        assert_eq!(acq.soft_conflicts.len(), 1, "soft conflict reported");
        assert_eq!(acq.soft_conflicts[0].path, "src/lib.rs");
        assert!(
            acq.soft_conflicts[0]
                .holders
                .iter()
                .any(|h| h == "BlueLake")
        );
        assert_eq!(
            acq.soft_conflicts[0].reservation_ids.len(),
            1,
            "soft conflict carries the intent's reservation id"
        );

        // No lease was published for RoseDune (freeze, not grant).
        let held = mailbox.active_reservations("demo").await.expect("held");
        assert!(
            held.iter().all(|r| r.agent_name != "RoseDune"),
            "frozen acquirer holds nothing"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_acquire_against_held_reservation_hard_blocks_unchanged() {
        let (mailbox, db_path, data_dir) = test_mailbox("hard-acquire").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        let held_id = held.granted[0].id.clone();

        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/lib.rs".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        assert!(acq.granted.is_empty());
        assert_eq!(acq.conflicts.len(), 1, "held lease hard-blocks");
        assert!(acq.soft_conflicts.is_empty(), "no soft tier involved");
        assert!(
            acq.conflicts[0]
                .reservation_ids
                .iter()
                .any(|i| i == &held_id),
            "hard conflict carries the held lease's reservation id: {:?}",
            acq.conflicts[0].reservation_ids
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-dsu1: `check_file_reservation_conflicts` must surface the
    /// reservation ids behind every conflict — the operator lock board needs
    /// the event id for diagnostics (force-release, request-release, replay).
    #[tokio::test]
    async fn test_conflict_report_carries_reservation_ids() {
        let (mailbox, db_path, data_dir) = test_mailbox("conflict-ids").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        assert_eq!(held.granted.len(), 1);
        let held_id = held.granted[0].id.clone();

        // Hard tier: RoseDune checks a path BlueLake holds.
        let report = mailbox
            .check_file_reservation_conflicts("demo", "RoseDune", &["src/lib.rs".into()])
            .await
            .expect("report");
        assert_eq!(report.conflicts.len(), 1, "hard conflict reported");
        assert_eq!(report.conflicts[0].path, "src/lib.rs");
        assert!(report.conflicts[0].holders.iter().any(|h| h == "BlueLake"));
        assert!(
            report.conflicts[0]
                .reservation_ids
                .iter()
                .any(|i| i == &held_id),
            "hard conflict carries the lease's id: {:?}",
            report.conflicts[0].reservation_ids
        );
        assert!(report.soft_conflicts.is_empty(), "no soft tier involved");

        // Soft tier: BlueLake checks a path RoseDune holds an intent on.
        mailbox
            .declare_intent("demo", "RoseDune", &["docs/**".into()], None, None)
            .await
            .expect("intent");
        let report2 = mailbox
            .check_file_reservation_conflicts("demo", "BlueLake", &["docs/guide.md".into()])
            .await
            .expect("report2");
        assert_eq!(report2.soft_conflicts.len(), 1, "soft conflict reported");
        assert_eq!(
            report2.soft_conflicts[0].reservation_ids.len(),
            1,
            "soft conflict carries the intent id"
        );
        assert!(
            report2.soft_conflicts[0]
                .holders
                .iter()
                .any(|h| h == "RoseDune")
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_clear_acquire_succeeds_immediately() {
        let (mailbox, db_path, data_dir) = test_mailbox("clear-acquire").await;
        register(&mailbox, "demo", "BlueLake").await;

        // No leases, no intents → immediate grant.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["docs/x.md".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1);
        assert!(acq.conflicts.is_empty());
        assert!(acq.soft_conflicts.is_empty());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_intent_expires_after_ttl_and_unblocks_acquire() {
        let (mailbox, db_path, data_dir) = test_mailbox("intent-ttl").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // Publish an intent DECLARED past the TTL (created_at old enough that
        // declared + INTENT_TTL_SECS < now).
        let d = "demo:src/**";
        let now = Timestamp::now().as_secs() as i64;
        let tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", d]).expect("d"),
                Tag::parse(["status", "intent"]).expect("status"),
            ],
            Some("demo"),
        );
        let old_intent = build_event(
            &crate::nostr::Keys::new(
                crate::nostr::SecretKey::from_hex(
                    "6b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e",
                )
                .expect("sk"),
            ),
            toron_kind(FILE_RESERVATION),
            r#"{"status":"intent","exclusive":true}"#,
            tags,
            Timestamp::from((now - INTENT_TTL_SECS as i64 - 60) as u64),
        )
        .expect("build");
        mailbox.bus.publish(old_intent).await.expect("publish");
        let _ = blue;

        // Expired intent is not on the board...
        let intents = mailbox.active_intents("demo").await.expect("intents");
        assert!(intents.is_empty(), "expired intent must auto-expire");

        // ...and no longer soft-conflicts: RoseDune's acquire succeeds.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/lib.rs".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1, "expired intent does not freeze");
        assert!(acq.soft_conflicts.is_empty());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_own_acquire_converts_intent_to_held() {
        // Must-convert (Spec 2): the declarer's own acquire at the same
        // coordinate supersedes the intent (NIP-33) and holds.
        let (mailbox, db_path, data_dir) = test_mailbox("must-convert").await;
        register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .declare_intent("demo", "BlueLake", &["src/**".into()], None, None)
            .await
            .expect("declare");
        assert_eq!(
            mailbox.active_intents("demo").await.expect("intents").len(),
            1
        );

        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                Some("converting"),
            )
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1, "own intent never freezes own acquire");

        let intents = mailbox.active_intents("demo").await.expect("intents");
        assert!(intents.is_empty(), "intent converted to held");
        let held = mailbox.active_reservations("demo").await.expect("held");
        assert_eq!(held.len(), 1, "held view shows the converted lease");
        assert_eq!(held[0].agent_name, "BlueLake");
        assert_eq!(held[0].path_pattern, "src/**");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_check_report_classifies_soft_and_hard_tiers() {
        let (mailbox, db_path, data_dir) = test_mailbox("tier-report").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;
        register(&mailbox, "demo", "GaleSummit").await;

        // RoseDune holds held/**, BlueLake intents intent/**.
        mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["held/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        mailbox
            .declare_intent("demo", "BlueLake", &["intent/**".into()], None, None)
            .await
            .expect("intent");

        let report = mailbox
            .check_file_reservation_conflicts(
                "demo",
                "GaleSummit",
                &[
                    "held/a.rs".into(),
                    "intent/b.rs".into(),
                    "clear/c.md".into(),
                ],
            )
            .await
            .expect("check");
        assert!(!report.conflict_free, "hard conflict present");
        assert_eq!(report.conflicts.len(), 1, "held tier hard");
        assert_eq!(report.conflicts[0].path, "held/a.rs");
        assert!(report.conflicts[0].holders.iter().any(|h| h == "RoseDune"));
        assert!(report.has_soft_conflicts, "soft tier flagged");
        assert_eq!(report.soft_conflicts.len(), 1, "intent tier soft");
        assert_eq!(report.soft_conflicts[0].path, "intent/b.rs");
        assert!(
            report.soft_conflicts[0]
                .holders
                .iter()
                .any(|h| h == "BlueLake")
        );
        assert!(report.clear_paths.contains(&"clear/c.md".to_string()));
        assert_eq!(report.total_conflicting_reservations, 1, "hard count only");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_request_release_roundtrip_and_validation() {
        let (mailbox, db_path, data_dir) = test_mailbox("request-release").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;
        register(&mailbox, "demo", "GaleSummit").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        let lease_id = held.granted[0].id.clone();

        // Validation: self-request is a no-op.
        let err = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "BlueLake".into(),
                target_agent: "BlueLake".into(),
                reservation_id: lease_id.clone(),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect_err("self-request");
        assert!(err.to_string().contains("self"), "{err}");

        // Validation: unknown reservation id.
        let err = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: "f".repeat(64),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect_err("unknown id");
        assert!(
            err.to_string().contains("no active held reservation"),
            "{err}"
        );

        // Validation: GaleSummit (not the holder) asks for BlueLake's lease
        // targeting RoseDune — holder mismatch.
        let err = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "GaleSummit".into(),
                target_agent: "RoseDune".into(),
                reservation_id: lease_id.clone(),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect_err("wrong holder");
        assert!(err.to_string().contains("held by BlueLake"), "{err}");

        // Happy path: RoseDune asks BlueLake to defer src/** by 600s.
        let req = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: lease_id.clone(),
                ask: ReleaseAsk::Defer { defer_secs: 600 },
                note: Some("need the dir".to_string()),
            })
            .await
            .expect("request");
        assert!(req.delivered);
        assert_eq!(req.holder, "BlueLake");
        assert_eq!(req.path_pattern, "src/**");
        assert!(!req.message_id.is_empty());

        // The typed rumor is readable by the holder (toron-kind + refs).
        let rumors = mailbox.project_rumors("demo").await;
        let rumor = rumors
            .iter()
            .find(|r| (**r).clone().id().to_hex() == req.message_id)
            .expect("request rumor");
        assert!(
            rumor
                .tags
                .iter()
                .any(|t| t.as_slice() == ["toron-kind".to_string(), "request-release".to_string()]),
            "typed toron-kind tag"
        );
        assert!(
            rumor
                .tags
                .iter()
                .any(|t| t.as_slice() == ["ask".to_string(), "defer:600".to_string()]),
            "ask encoded"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_request_release_status_and_escalation_to_captain() {
        let (mailbox, db_path, data_dir) = test_mailbox("request-escalate").await;
        register(&mailbox, "demo", "BlueLake").await;
        let rose = register(&mailbox, "demo", "RoseDune").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        let lease_id = held.granted[0].id.clone();

        // Fresh request → Pending (window not elapsed).
        let req = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: lease_id.clone(),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect("request");
        let status = mailbox
            .request_release_status("demo", "RoseDune", &req.message_id, None)
            .await;
        assert!(
            matches!(status, RequestReleaseStatus::Pending { .. }),
            "fresh request pending, got {status:?}"
        );

        // Backdated request (declared past the tier window) → EscalationDue.
        let old_ts = Timestamp::from(
            (Timestamp::now().as_secs() as i64 - REQUEST_RELEASE_TIER_WINDOW_SECS as i64 - 60)
                as u64,
        );
        let rose_keys = mailbox
            .agent_keys_for("demo", "RoseDune")
            .expect("rose keys");
        let blue_pk = mailbox
            .agent_keys_for("demo", "BlueLake")
            .expect("blue keys")
            .public_key();
        let backdated = gift_wrap::build_rumor(
            rose_keys.public_key(),
            old_ts,
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["p", &blue_pk.to_hex()]).expect("p"),
                Tag::parse(["toron-kind", "request-release"]).expect("kind"),
                Tag::parse(["ask", "release"]).expect("ask"),
                Tag::parse(["reservation-id", &lease_id]).expect("res"),
                Tag::parse(["subject", "[request-release] src/**"]).expect("subj"),
                Tag::parse(["importance", "high"]).expect("imp"),
            ],
            "backdated request".to_string(),
        );
        let backdated_id = backdated.clone().id().to_hex();
        let wrap = gift_wrap::gift_wrap(&rose_keys, &blue_pk, backdated).expect("wrap");
        mailbox.bus.publish(wrap).await.expect("publish");

        let status = mailbox
            .request_release_status("demo", "RoseDune", &backdated_id, None)
            .await;
        assert!(
            matches!(status, RequestReleaseStatus::EscalationDue { .. }),
            "unanswered past window escalates, got {status:?}"
        );

        // Escalate → kind-14 DM to the captain (operator key).
        mailbox
            .escalate_request_release("demo", "RoseDune", &backdated_id, Some("need it now"))
            .await
            .expect("escalate");
        let operator = mailbox.operator().expect("operator").public_key();
        let wraps = collect_replay(
            mailbox.bus.as_ref(),
            ToronFilter::new()
                .kinds([Kind::GiftWrap])
                .pubkey(operator)
                .build(),
        )
        .await
        .expect("wraps");
        assert!(
            wraps.iter().any(|w| {
                crate::nostr::gift_wrap::unwrap(mailbox.operator().expect("op"), w)
                    .map(|g| {
                        g.rumor.tags.iter().any(|t| {
                            t.as_slice()
                                == [
                                    "toron-kind".to_string(),
                                    "request-release-escalation".to_string(),
                                ]
                        }) && g.rumor.tags.iter().any(|t| {
                            t.as_slice().first().map(|s| s == "e").unwrap_or(false)
                                && t.as_slice()
                                    .get(1)
                                    .map(|s| s == &backdated_id)
                                    .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false)
            }),
            "escalation DM delivered to captain with request e-ref"
        );
        let _ = rose;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-wake-id-contract-1jz1: a DECIDED request-release can never re-fire
    /// a wake for the responder. Contract: BEFORE responding, the request is
    /// an unread urgent row in the holder's projection (wake-citable); the
    /// response records the holder's read receipt on the request rumor, so
    /// AFTER, no wake-citing row remains (bd-dbre: read rows do not wake).
    #[tokio::test]
    async fn test_decided_request_release_never_refires_wake() {
        let (mailbox, db_path, data_dir) = test_mailbox("wake-refire").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");

        let req = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: held.granted[0].id.clone(),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect("request");

        // A wake candidate = UNREAD + urgent (flywheel wake_urgent drops
        // read rows, bd-dbre) — so the query mirrors the loop's poll.
        let wake_rows = crate::mailbox::messaging::FetchInbox {
            project_slug: "demo".into(),
            agent_name: "BlueLake".into(),
            unread_only: true,
            limit: Some(10),
            include_bodies: true,
            urgent_only: true,
            since_ts: None,
            thread_id: None,
            ack_overdue_only: false,
        };
        let before = mailbox
            .fetch_inbox(&wake_rows)
            .await
            .expect("pre-response inbox");

        assert!(
            before
                .iter()
                .any(|r| r.id == req.message_id && r.read_ts_us.is_none()),
            "the request must be a live wake candidate pre-response"
        );

        mailbox
            .respond_request_release(&ReleaseResponse {
                project_slug: "demo".into(),
                sender_name: "BlueLake".into(),
                request_message_id: req.message_id.clone(),
                decision: ReleaseDecision::Agreed,
                note: None,
            })
            .await
            .expect("respond");

        let after = mailbox
            .fetch_inbox(&wake_rows)
            .await
            .expect("post-response inbox");
        assert!(
            !after.iter().any(|r| r.id == req.message_id),
            "a decided request-release must never re-fire a wake"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_request_release_response_resolves_status() {
        let (mailbox, db_path, data_dir) = test_mailbox("request-respond").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        let lease_id = held.granted[0].id.clone();

        let req = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: lease_id.clone(),
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect("request");

        // A third party cannot respond (cannot read the request).
        register(&mailbox, "demo", "GaleSummit").await;
        let err = mailbox
            .respond_request_release(&ReleaseResponse {
                project_slug: "demo".into(),
                sender_name: "GaleSummit".into(),
                request_message_id: req.message_id.clone(),
                decision: ReleaseDecision::Agreed,
                note: None,
            })
            .await
            .expect_err("non-target cannot respond");
        assert!(
            err.to_string().contains("no request-release message"),
            "{err}"
        );

        // The holder responds: Agreed → status resolves.
        let send = mailbox
            .respond_request_release(&ReleaseResponse {
                project_slug: "demo".into(),
                sender_name: "BlueLake".into(),
                request_message_id: req.message_id.clone(),
                decision: ReleaseDecision::Agreed,
                note: Some("on it".to_string()),
            })
            .await
            .expect("respond");
        assert_eq!(send.count, 1);
        let status = mailbox
            .request_release_status("demo", "RoseDune", &req.message_id, None)
            .await;
        assert_eq!(
            status,
            RequestReleaseStatus::Responded {
                decision: ReleaseDecision::Agreed
            },
            "response resolves the negotiation"
        );

        // A deferred decision round-trips through the wire encoding too.
        // "LATEST decision wins" resolves on second-granularity rumor time;
        // two responses in the same second are order-dependent. Sleep across
        // a boundary so the Deferred response is deterministically later.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let send2 = mailbox
            .respond_request_release(&ReleaseResponse {
                project_slug: "demo".into(),
                sender_name: "BlueLake".into(),
                request_message_id: req.message_id.clone(),
                decision: ReleaseDecision::Deferred { defer_secs: 300 },
                note: None,
            })
            .await
            .expect("respond 2");
        assert_eq!(send2.count, 1);
        let status2 = mailbox
            .request_release_status("demo", "RoseDune", &req.message_id, None)
            .await;
        assert_eq!(
            status2,
            RequestReleaseStatus::Responded {
                decision: ReleaseDecision::Deferred { defer_secs: 300 }
            },
            "deferred decision round-trips"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_escalation_requires_operator_key() {
        // No operator key → escalation (captain DM) must fail loudly.
        let db_path =
            std::env::temp_dir().join(format!("toron-res-noop-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-res-data-noop-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), None)
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        register(&mailbox, "demo", "BlueLake").await;

        let held = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("hold");
        let lease_id = held.granted[0].id.clone();
        register(&mailbox, "demo", "RoseDune").await;
        let req = mailbox
            .request_release(&RequestRelease {
                project_slug: "demo".into(),
                sender_name: "RoseDune".into(),
                target_agent: "BlueLake".into(),
                reservation_id: lease_id,
                ask: ReleaseAsk::Release,
                note: None,
            })
            .await
            .expect("request");
        let err = mailbox
            .escalate_request_release("demo", "RoseDune", &req.message_id, None)
            .await
            .expect_err("no operator");
        assert!(
            err.to_string().contains("no operator key"),
            "escalation gated on operator: {err}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_force_release_operator_revocation() {
        let (mailbox, db_path, data_dir) = test_mailbox("force").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        // A lease from BlueLake.
        let acq = mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/**".into()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("acquire");
        let lease_id = acq.granted[0].id.clone();

        // Fresh holder → not stale → force-release denied.
        let err = mailbox
            .force_release_file_reservation("demo", "RoseDune", "src/**", None, Some("gone"), true)
            .await
            .expect_err("not stale");
        assert!(err.to_string().contains("not stale"), "{err}");

        // Simulate staleness by publishing an OLD profile for the holder,
        // then force-release must succeed via operator revocation.
        // (Write an old kind-30013 for BlueLake.)
        let blue_profile = crate::nostr::identity::create_agent_identity(
            "demo",
            &profile("BlueLake"),
            Timestamp::from(1_500_000_000),
        )
        .expect("old profile");
        // Publishing a different-author profile won't help staleness (the
        // mailbox's own register already created a fresh one). Force the check
        // by issuing the revocation directly through the operator path below.
        let _ = blue_profile;

        // Operator revocation: publish a kind-30031 status:revoked at the same
        // coordinate. This is what force_release does after the heuristic.
        let operator = mailbox.operator().unwrap();
        let d = "demo:src/**";
        let lease_ev = EventId::from_hex(&lease_id).expect("lease id");
        let tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", d]).expect("d"),
                Tag::parse(["status", "revoked"]).expect("status"),
                Tag::event(lease_ev),
            ],
            Some("demo"),
        );
        let revoke = build_event(
            operator,
            toron_kind(FILE_RESERVATION),
            r#"{"status":"revoked","heuristic":"stale-holder"}"#,
            tags,
            Timestamp::now(),
        )
        .expect("revoke");
        assert!(is_revocation(&revoke));
        mailbox
            .bus
            .publish(revoke.clone())
            .await
            .expect("publish revoke");

        // Projection: with BOTH agent lease + operator revoke present, the
        // resolved view is the operator revocation (key-priority operator>agent).
        let events = collect_replay(
            mailbox.bus.as_ref(),
            kind_project_filter(toron_kind(FILE_RESERVATION), "demo"),
        )
        .await
        .expect("collect");
        let resolved =
            resolve_coordinate(events.iter(), Some(&operator.public_key())).expect("resolved");
        assert!(is_revocation(&resolved), "projection shows revoked");
        // Both events present (agent lease + operator revoke).
        let revokes = events.iter().filter(|e| is_revocation(e)).count();
        assert!(revokes >= 1, "operator revoke event present");
        // No NIP-09 (kind-5) events anywhere.
        assert!(
            events.iter().all(|e| e.kind.as_u16() != 5),
            "no cross-key NIP-09 deletes"
        );
        // Active view excludes the lease.
        let active = mailbox.active_reservations("demo").await.expect("active");
        assert!(active.is_empty(), "revoked lease not active");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-h3oj: release_by_reason revokes ONLY the leases whose reason
    /// equals the bead id — other reasons stay live, and the released path
    /// is immediately conflict-free (no TTL wait).
    #[tokio::test]
    async fn test_release_by_reason_revokes_only_matching_reason() {
        let (mailbox, db_path, data_dir) = test_mailbox("h3oj-reason").await;
        register(&mailbox, "demo", "BlueLake").await;

        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/x.rs".into()],
                Some(3600),
                true,
                Some("bd-XX"),
            )
            .await
            .expect("acquire x");
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/y.rs".into()],
                Some(3600),
                true,
                Some("bd-YY"),
            )
            .await
            .expect("acquire y");

        let r = mailbox
            .release_by_reason("demo", "operator", "bd-XX", false)
            .await
            .expect("release by reason");
        assert_eq!(r.released, 1, "exactly the bd-XX lease released");

        let active = mailbox.active_reservations("demo").await.expect("active");
        assert_eq!(active.len(), 1, "only the bd-YY lease stays");
        assert_eq!(active[0].path_pattern, "src/y.rs");

        // The released path is conflict-free immediately (no TTL wait).
        let rep = mailbox
            .check_file_reservation_conflicts("demo", "BlueLake", &["src/x.rs".into()])
            .await
            .expect("conflict check");
        assert!(rep.conflict_free, "src/x.rs released without waiting TTL");
        assert!(rep.conflicts.is_empty(), "{:?}", rep.conflicts);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// A lease taken with no explicit TTL must expire on the HEARTBEAT clock,
    /// not the 90-day envelope clock: a crashed holder frees its paths in one
    /// reservation TTL, and `with-lease` renews while it runs.
    #[tokio::test]
    async fn test_default_lease_ttl_is_the_reservation_heartbeat_not_the_envelope() {
        let (mailbox, _db, data_dir) = test_mailbox("locks-default-ttl").await;
        register(&mailbox, "demo", "BlueLake").await;
        let before = Timestamp::now().as_secs() as i64;
        mailbox
            .file_reservation_paths("demo", "BlueLake", &["src/a.rs".into()], None, true, Some("bd-ttl"))
            .await
            .expect("acquire with the default TTL");
        let row = mailbox
            .active_reservations("demo")
            .await
            .expect("board")
            .into_iter()
            .find(|r| r.path_pattern == "src/a.rs")
            .expect("the lease is on the board");
        let expires_secs = row.expires_ts_us.expect("default TTL stamps an expiry") / 1_000_000;
        let window = expires_secs - before;
        assert!(
            (DEFAULT_RESERVATION_TTL_SECS as i64 - 5..=DEFAULT_RESERVATION_TTL_SECS as i64 + 5)
                .contains(&window),
            "default TTL must be {}s, saw {window}s",
            DEFAULT_RESERVATION_TTL_SECS
        );
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// The shared lease-board filter (`list_file_reservations`): exact holder
    /// and symmetric-glob path overlap. One implementation backs both the MCP
    /// tool and `toron reserve list`, so the CLI and MCP boards cannot drift
    /// (F14, docs/learnings/2026-09-12-swarm-stack-friction.md).
    #[tokio::test]
    async fn test_list_file_reservations_filters_holder_and_path() {
        let (mailbox, db_path, data_dir) = test_mailbox("locks-list").await;
        register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RedStone").await;
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/a.rs".into()],
                Some(3600),
                true,
                Some("bd-a"),
            )
            .await
            .expect("acquire a");
        mailbox
            .file_reservation_paths(
                "demo",
                "RedStone",
                &["docs/*.md".into()],
                Some(3600),
                false,
                Some("bd-b"),
            )
            .await
            .expect("acquire b");

        let all = mailbox
            .list_file_reservations("demo", None, None)
            .await
            .expect("all");
        assert_eq!(all.len(), 2, "board shows both held leases");

        let by_holder = mailbox
            .list_file_reservations("demo", Some("BlueLake"), None)
            .await
            .expect("holder");
        assert_eq!(by_holder.len(), 1);
        assert_eq!(by_holder[0].path_pattern, "src/a.rs");

        let by_path = mailbox
            .list_file_reservations("demo", None, Some("docs/readme.md"))
            .await
            .expect("path");
        assert_eq!(by_path.len(), 1, "glob overlap on the requested path");
        assert_eq!(by_path[0].agent_name, "RedStone");

        let none = mailbox
            .list_file_reservations("demo", Some("NobodyHere"), None)
            .await
            .expect("none");
        assert!(none.is_empty(), "unknown holder matches nothing");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-h3oj: a bead id with no matching leases is a clean no-op
    /// (released = 0, never an error — the loop hook is idempotent).
    #[tokio::test]
    async fn test_release_by_reason_no_match_is_noop() {
        let (mailbox, db_path, data_dir) = test_mailbox("h3oj-nomatch").await;
        register(&mailbox, "demo", "BlueLake").await;
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/x.rs".into()],
                Some(3600),
                true,
                Some("bd-XX"),
            )
            .await
            .expect("acquire");

        let r = mailbox
            .release_by_reason("demo", "operator", "bd-ZZ", false)
            .await
            .expect("no-match is not an error");
        assert_eq!(r.released, 0);
        assert_eq!(
            mailbox
                .active_reservations("demo")
                .await
                .expect("active")
                .len(),
            1,
            "bd-XX lease untouched"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Regression (bd-reacquire): a lease acquired AFTER an operator
    /// revocation at the same coordinate must be visible again — the
    /// revocation released the OLD lease (bead-closed), it does not
    /// permanently shadow the path. Without the fix, key-priority
    /// resolution lets the revocation (no TTL, operator class) outrank a
    /// newer agent lease forever.
    #[tokio::test]
    async fn test_reacquire_after_revoke_becomes_visible() {
        let (mailbox, db_path, data_dir) = test_mailbox("revoke-reacquire").await;
        let rose = register(&mailbox, "demo", "RoseDune").await;
        register(&mailbox, "demo", "BlueLake").await;

        // BlueLake acquires src/x.rs under reason bd-X.
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/x.rs".into()],
                Some(3600),
                true,
                Some("bd-X"),
            )
            .await
            .expect("acquire");
        assert_eq!(
            mailbox
                .active_reservations("demo")
                .await
                .expect("active")
                .len(),
            1,
            "lease active before release"
        );

        // Operator release-by-reason revokes it (bead-closed).
        let r = mailbox
            .release_by_reason("demo", "operator", "bd-X", false)
            .await
            .expect("release");
        assert_eq!(r.released, 1);
        assert!(
            mailbox
                .active_reservations("demo")
                .await
                .expect("active")
                .is_empty(),
            "revoked lease not active"
        );

        // RoseDune re-acquires the same path immediately (release unlocked it).
        mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["src/x.rs".into()],
                Some(3600),
                true,
                Some("bd-Y"),
            )
            .await
            .expect("re-acquire");

        let active = mailbox.active_reservations("demo").await.expect("active");
        assert_eq!(active.len(), 1, "re-acquired lease visible");
        assert_eq!(active[0].path_pattern, "src/x.rs");
        assert_eq!(active[0].agent_pubkey, rose.to_hex(), "new holder wins");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-h3oj: the operator signs the revocation — no operator key, no
    /// release (mirrors force_release's gate).
    #[tokio::test]
    async fn test_release_by_reason_gated_on_operator() {
        let db_path =
            std::env::temp_dir().join(format!("toron-res-h3oj-op-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-res-data-h3oj-op-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), None)
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        register(&mailbox, "demo", "BlueLake").await;
        mailbox
            .file_reservation_paths(
                "demo",
                "BlueLake",
                &["src/x.rs".into()],
                Some(3600),
                true,
                Some("bd-XX"),
            )
            .await
            .expect("acquire");

        let err = mailbox
            .release_by_reason("demo", "operator", "bd-XX", false)
            .await
            .expect_err("operator required");
        assert!(
            err.to_string().contains("no operator key"),
            "release_by_reason gated on operator: {err}"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// A hand-built board row for the pure close-event/audit tests.
    fn lease_with_reason(reason: &str, path: &str) -> Reservation {
        Reservation {
            id: format!("lease-{path}"),
            agent_name: "BlueLake".into(),
            agent_pubkey: "ab".repeat(32),
            path_pattern: path.into(),
            exclusive: true,
            reason: reason.into(),
            expires_ts_us: None,
            status: ReservationStatus::Held,
            epoch: 1,
        }
    }

    /// bd-close-releases-leases-qkbo: a close event for one bead releases
    /// ONLY the leases whose reason is exactly that bead — other reasons stay
    /// on the board, a longer id with the same prefix is not a match, and an
    /// empty id releases nothing (it would otherwise sweep every reasonless
    /// lease). Calls are `super::`-qualified: this test's own name shadows
    /// the imported function inside the module.
    #[test]
    fn release_on_close_event() {
        let board = vec![
            lease_with_reason("bd-1", "src/one.rs"),
            lease_with_reason("bd-10", "src/ten.rs"),
            lease_with_reason("bd-2", "src/two.rs"),
            lease_with_reason("", "src/none.rs"),
        ];

        let released = super::release_on_close_event(&board, "bd-1");
        assert_eq!(
            released
                .iter()
                .map(|l| l.path_pattern.as_str())
                .collect::<Vec<_>>(),
            vec!["src/one.rs"],
            "exactly the bd-1 lease"
        );
        assert_eq!(board.len(), 4, "the selector never mutates the board");

        assert!(
            super::release_on_close_event(&board, "").is_empty(),
            "an empty close id releases nothing"
        );
        assert!(
            super::release_on_close_event(&board, "bd-nope").is_empty(),
            "an unknown close id releases nothing"
        );
        assert_eq!(
            super::release_on_close_event(&board, "bd-10")
                .iter()
                .map(|l| l.reason.as_str())
                .collect::<Vec<_>>(),
            vec!["bd-10"],
            "prefix sharing must not leak (bd-1 vs bd-10)"
        );
    }

    /// The stale audit is the union of close events over the closed set, so
    /// `reserve list`'s marker and `reserve sweep` agree with the release
    /// predicate by construction.
    #[test]
    fn stale_closed_leases_folds_close_events() {
        let board = vec![
            lease_with_reason("bd-1", "src/a.rs"),
            lease_with_reason("bd-2", "src/b.rs"),
            lease_with_reason("bd-3", "src/c.rs"),
            lease_with_reason("", "src/d.rs"),
        ];
        let closed: std::collections::BTreeSet<String> =
            ["bd-1", "bd-3"].into_iter().map(String::from).collect();
        let stale: Vec<&str> = stale_closed_leases(&board, &closed)
            .iter()
            .map(|l| l.path_pattern.as_str())
            .collect();
        assert_eq!(stale, vec!["src/a.rs", "src/c.rs"]);
        assert!(
            stale_closed_leases(&board, &std::collections::BTreeSet::new()).is_empty(),
            "no closed beads, no stale leases"
        );
    }

    // --- bd-adr-0028-implementation-zfnx.11: claim fencing epochs ---

    /// Publish a raw legacy (epoch-less) lease for keys with NO profile on
    /// the bus — the stale-pane false negative: the holder is alive, the bus
    /// considers it stale, so `force_release` runs end-to-end.
    async fn publish_raw_stale_lease(mailbox: &Mailbox, holder: &Keys, project: &str, path: &str) {
        use crate::nostr::event::build_event;
        let d = format!("{project}:{path}");
        let tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![Tag::parse(["d", &d]).expect("d")],
            Some(project),
        );
        let lease = build_event(
            holder,
            toron_kind(FILE_RESERVATION),
            r#"{"exclusive":true,"reason":"legacy"}"#,
            tags,
            Timestamp::now(),
        )
        .expect("raw lease");
        mailbox.bus.publish(lease).await.expect("publish");
    }

    /// Publish a raw `status:intent` 30031 for keys with NO profile on the
    /// bus — the intent-board half of the roster-miss case: neither the live
    /// profiles nor the roster can name the holder.
    async fn publish_raw_intent(mailbox: &Mailbox, holder: &Keys, project: &str, path: &str) {
        use crate::nostr::event::build_event;
        let d = format!("{project}:{path}");
        let tags = with_project_scoping(
            toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["status", "intent"]).expect("status"),
            ],
            Some(project),
        );
        let intent = build_event(
            holder,
            toron_kind(FILE_RESERVATION),
            r#"{"status":"intent","exclusive":true,"reason":""}"#,
            tags,
            Timestamp::now(),
        )
        .expect("raw intent");
        mailbox.bus.publish(intent).await.expect("publish");
    }

    /// Order 1 — hostile reclaim: bus-stale A holds (epoch 0), operator
    /// force-releases, registered B reclaims. A presents 0 → exactly one
    /// loud loser; B holds epoch 1; zero double-own.
    #[tokio::test]
    async fn test_fencing_epoch_force_reclaim_loser_fails_loud() {
        let (mailbox, db_path, data_dir) = test_mailbox("fence-11a").await;
        register(&mailbox, "demo", "RoseDune").await;
        let holder_a = crate::keys::generate();
        publish_raw_stale_lease(&mailbox, &holder_a, "demo", "epoch-fence.txt").await;

        mailbox
            .force_release_file_reservation(
                "demo",
                "RoseDune",
                "epoch-fence.txt",
                None,
                Some("stale"),
                false,
            )
            .await
            .expect("force release the bus-stale lease");

        let reclaim = mailbox
            .file_reservation_paths(
                "demo",
                "RoseDune",
                &["epoch-fence.txt".into()],
                Some(3600),
                true,
                Some("bd-adr-0028-implementation-zfnx.11"),
            )
            .await
            .expect("reclaim grants");
        assert_eq!(reclaim.granted.len(), 1, "{reclaim:?}");
        assert_eq!(
            reclaim.granted[0].epoch, 1,
            "reclaim bumps the epoch past the revoked marker: {reclaim:?}"
        );

        // A's epoch-0 belief is stale: the ONE loud loser.
        let err = mailbox
            .check_claim_epoch("demo", "epoch-fence.txt", 0)
            .await
            .expect_err("stale epoch must fail loud");
        assert!(
            err.to_string().contains("stale claim epoch"),
            "loud stale-pointer, got: {err:?}"
        );

        // B presents the current epoch: the single winner.
        let current = mailbox
            .check_claim_epoch("demo", "epoch-fence.txt", 1)
            .await
            .expect("winner reads current");
        assert_eq!(current.agent_name, "RoseDune");

        // Zero double-own: exactly one live lease at the coordinate.
        let live: Vec<_> = mailbox
            .active_reservations("demo")
            .await
            .expect("active")
            .into_iter()
            .filter(|r| r.path_pattern == "epoch-fence.txt")
            .collect();
        assert_eq!(live.len(), 1, "one owner: {live:?}");
        assert_eq!(live[0].epoch, 1);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Order 2 — cooperative reclaim: registered A holds genesis, transfers
    /// to registered B (bump on reclaim). A's epoch-0 belief then fails
    /// loud; B holds epoch 1; zero double-own.
    #[tokio::test]
    async fn test_fencing_epoch_transfer_reclaim_loser_fails_loud() {
        let (mailbox, db_path, data_dir) = test_mailbox("fence-11b").await;
        register(&mailbox, "demo", "CoralFinch").await;
        register(&mailbox, "demo", "TealDune").await;

        let genesis = mailbox
            .file_reservation_paths(
                "demo",
                "CoralFinch",
                &["epoch-fence.txt".into()],
                Some(3600),
                true,
                Some("bd-adr-0028-implementation-zfnx.11"),
            )
            .await
            .expect("genesis claim");
        assert_eq!(genesis.granted.len(), 1);
        assert_eq!(
            genesis.granted[0].epoch, 0,
            "first claim on a fresh coordinate is genesis: {genesis:?}"
        );

        mailbox
            .transfer_reservation("demo", "CoralFinch", "TealDune", "epoch-fence.txt", None)
            .await
            .expect("transfer reclaims");

        // A's epoch-0 belief is stale: the ONE loud loser.
        let err = mailbox
            .check_claim_epoch("demo", "epoch-fence.txt", 0)
            .await
            .expect_err("stale epoch must fail loud");
        assert!(
            err.to_string().contains("stale claim epoch"),
            "loud stale-pointer, got: {err:?}"
        );

        // B presents the current epoch: the single winner.
        let current = mailbox
            .check_claim_epoch("demo", "epoch-fence.txt", 1)
            .await
            .expect("winner reads current");
        assert_eq!(current.agent_name, "TealDune");

        // Zero double-own.
        let live: Vec<_> = mailbox
            .active_reservations("demo")
            .await
            .expect("active")
            .into_iter()
            .filter(|r| r.path_pattern == "epoch-fence.txt")
            .collect();
        assert_eq!(live.len(), 1, "one owner: {live:?}");
        assert_eq!(live[0].epoch, 1);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
