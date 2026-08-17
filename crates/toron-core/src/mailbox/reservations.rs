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

use nostr_sdk::nostr::{Event, EventId, Keys, Kind, PublicKey, Tag, Timestamp};

use crate::bus::addressable::is_revocation;
use crate::bus::filter::{ToronFilter, kind_project_filter};
use crate::bus::scoping::with_project_scoping;
use crate::bus::{EnvelopeTtl, resolve_coordinate};
use crate::mailbox::guard::pattern_matches;
use crate::nostr::event::build_event;
use crate::nostr::gift_wrap;
use crate::nostr::kinds::{FILE_RESERVATION, toron_kind};

use super::Mailbox;
use super::collect_replay;
use super::messaging::{SendMessage, SendResult};

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

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GrantedReservation {
    pub id: String,
    pub path_pattern: String,
    pub exclusive: bool,
    pub reason: String,
    pub expires_ts_us: Option<i64>,
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
    /// Soft conflicts (other agents' in-TTL intents) — ADR-0011 Spec 4.
    pub soft_conflicts: Vec<PathConflict>,
    /// True when any path has a soft conflict.
    pub has_soft_conflicts: bool,
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
            let d = format!("{project_slug}:{path}");
            let hits = self
                .conflict_view(&agent_keys.public_key(), project_slug, path)
                .await;
            let hard: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Held)
                .map(|h| h.agent_name.clone())
                .collect();
            let hard_ids: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Held)
                .map(|h| h.reservation_id.clone())
                .collect();
            let soft: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Intent)
                .map(|h| h.agent_name.clone())
                .collect();
            let soft_ids: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Intent)
                .map(|h| h.reservation_id.clone())
                .collect();
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
            let base = vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["reason", reason.unwrap_or("")]).expect("reason"),
            ];
            let mut tags =
                with_project_scoping(toron_kind(FILE_RESERVATION), base, Some(project_slug));
            let ttl = EnvelopeTtl {
                secs: Some(ttl_secs.unwrap_or(crate::bus::DEFAULT_ENVELOPE_TTL_SECS)),
            };
            let created = Timestamp::now();
            if let Some(exp) = ttl.expiration_tag(created) {
                tags.push(exp);
            }
            let content = format!(
                r#"{{"exclusive":{},"reason":{}}}"#,
                exclusive,
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
                anyhow::bail!("reservation event rejected by bus");
            }
            result.granted.push(GrantedReservation {
                id: event.id.to_hex(),
                path_pattern: path.clone(),
                exclusive,
                reason: reason.unwrap_or("").to_string(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
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
            let hits = self
                .conflict_view(&agent_keys.public_key(), project_slug, path)
                .await;
            let hard: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Held)
                .map(|h| h.agent_name.clone())
                .collect();
            let hard_ids: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Held)
                .map(|h| h.reservation_id.clone())
                .collect();
            let soft: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Intent)
                .map(|h| h.agent_name.clone())
                .collect();
            let soft_ids: Vec<String> = hits
                .iter()
                .filter(|h| h.status == ReservationStatus::Intent)
                .map(|h| h.reservation_id.clone())
                .collect();
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
        Ok(report)
    }

    /// `renew_file_reservations`: republish the holder's active leases with an
    /// extended TTL (same `(kind,d)` coordinate → supersedes per author).
    ///
    /// bd-evqs renewal cap: a lease whose remaining TTL is ALREADY ≥
    /// `extend_secs` is NOT republished. The durable loop heartbeat renews
    /// every tick; without the cap it re-publishes every lease each tick —
    /// a single d-tag accumulated 1218 renewal events over ~10h (the
    /// reservation renewal storm, holder inactive, owning bead closed).
    /// Renewing a lease that will outlive the extension adds nothing (the
    /// TTL ratchets: renew only when remaining < extend_secs, keeping the
    /// lease healthy in [extend, 2×extend) forever) — the cap collapses the
    /// churn to ~1 event per extend window per lease, and lets a concurrent
    /// release/revocation win the coordinate instead of racing a same-second
    /// renew. Permanent leases (no expiry) are never renewed (they never
    /// lapse).
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
            // bd-evqs cap: skip when the lease still has ≥ extend_secs of TTL
            // left (or is permanent). No event is published — the loop's
            // every-tick heartbeat stops churning the bus. The lease is still
            // REPORTED (unrenewed) so the embedding's closed-bead sweep can
            // see + release it: a reservation whose owning bead closed is
            // stale even while its TTL is healthy.
            let old_expiry_secs = res.expires_ts_us.map(|t| t / 1_000_000);
            let skip = match old_expiry_secs {
                None => true, // permanent lease — nothing to renew
                Some(abs) => abs.saturating_sub(now_secs) >= extend_secs as i64,
            };
            if skip {
                result.reservations.push(GrantedReservation {
                    id: res.id.clone(),
                    path_pattern: res.path_pattern.clone(),
                    exclusive: res.exclusive,
                    reason: res.reason.clone(),
                    expires_ts_us: res.expires_ts_us,
                });
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
                format!(r#"{{"exclusive":{}}}"#, res.exclusive),
                tags,
                created,
            )?;
            self.bus.publish(event.clone()).await?;
            result.reservations.push(GrantedReservation {
                id: event.id.to_hex(),
                path_pattern: res.path_pattern.clone(),
                exclusive: res.exclusive,
                reason: res.reason.clone(),
                expires_ts_us: ttl
                    .expiration_at(created)
                    .map(|t| t.as_secs() as i64 * 1_000_000),
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
                r#"{"status":"released"}"#,
                tags,
                Timestamp::now(),
            )?;
            self.bus.publish(event.clone()).await?;
            self.record_transition(project_slug, &d, "released", &event.id)
                .await;
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
            .find(|r| match holder {
                Some(h) => r.agent_name == h,
                None => true,
            })
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
            r#"{"status":"revoked","heuristic":"stale-holder"}"#,
            tags,
            Timestamp::now(),
        )?;
        assert!(is_revocation(&revoke), "revocation must be detectable");
        self.bus.publish(revoke.clone()).await?;
        self.record_transition(project_slug, &d, "revoked", &revoke.id)
            .await;

        // 5. Notify previous holder via NIP-17 (heuristic summary).
        if notify_previous {
            let holder_keys = Keys::generate(); // sender key for the wrap
            let holder_npub = PublicKey::from_hex(&lease.agent_pubkey)
                .map_err(|_| anyhow::anyhow!("bad holder npub"))?;
            let rumor = crate::nostr::gift_wrap::build_rumor(
                holder_keys.public_key(),
                Timestamp::now(),
                nostr_sdk::nostr::Kind::PrivateDirectMessage,
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
                .await
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
                r#"{"status":"revoked","heuristic":"bead-closed"}"#,
                tags,
                Timestamp::now(),
            )?;
            assert!(is_revocation(&revoke), "revocation must be detectable");
            self.bus.publish(revoke.clone()).await?;
            self.record_transition(project_slug, &d, "revoked", &revoke.id)
                .await;
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
                    nostr_sdk::nostr::Kind::PrivateDirectMessage,
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
                    .await
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
        let operator = self.operator.as_ref().map(|k| k.public_key());
        let now_secs = Timestamp::now().as_secs() as i64;

        // Group by (kind, d) coordinate → resolve latest per priority.
        let mut by_d: std::collections::HashMap<String, Vec<&Event>> =
            std::collections::HashMap::new();
        for ev in &events {
            if let Some(d) = ev
                .tags
                .iter()
                .find_map(|t| (t.kind() == nostr_sdk::nostr::TagKind::d()).then(|| t.content()))
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
            // TTL expiry clears the lease (NIP-40, held and intent alike).
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
                    (t.as_slice().first().map(|s| s.as_str()) == Some("reason"))
                        .then(|| t.as_slice().get(1).map(|s| s.to_string()))
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
            });
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
            })
            .collect();
        // Fill agent names from profiles (best-effort).
        self.fill_agent_names(project_slug, &mut out).await;
        Ok(out)
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
        for i in intents.iter_mut() {
            i.agent_name = names
                .get(&i.agent_pubkey)
                .cloned()
                .unwrap_or_else(|| i.agent_pubkey.chars().take(8).collect());
        }
        Ok(intents)
    }

    /// Symmetric-glob conflict view against OTHER agents' active leases:
    /// held exclusive leases = HARD (`Held` hits), in-TTL intents = SOFT
    /// (`Intent` hits). Own leases never conflict; shared leases never block.
    /// One replay serves both tiers (ADR-0011 Spec 4).
    async fn conflict_view(
        &self,
        own_npub: &PublicKey,
        project_slug: &str,
        path: &str,
    ) -> Vec<ConflictHit> {
        let leases = match self.resolved_leases(project_slug).await {
            Ok(l) => l,
            Err(_) => return Vec::new(),
        };
        let now_secs = Timestamp::now().as_secs() as i64;
        let names = self.agent_name_map(project_slug).await;
        leases
            .into_iter()
            .filter(|l| l.agent_pubkey != own_npub.to_hex())
            .filter(|l| l.exclusive)
            .filter(|l| overlaps(&l.path_pattern, path))
            .filter(|l| match l.status {
                ReservationStatus::Held => true,
                ReservationStatus::Intent => {
                    l.created_at_secs.saturating_add(INTENT_TTL_SECS as i64) >= now_secs
                }
                _ => false,
            })
            .map(|l| ConflictHit {
                agent_name: names
                    .get(&l.agent_pubkey)
                    .cloned()
                    .unwrap_or_else(|| l.agent_pubkey.chars().take(8).collect()),
                reservation_id: l.id,
                path_pattern: l.path_pattern,
                status: l.status,
            })
            .collect()
    }

    /// Staleness heuristic: holder has no signed profile activity within
    /// `STALE_AFTER_SECS`.
    async fn is_holder_stale(&self, project_slug: &str, res: &Reservation) -> bool {
        let filter =
            kind_project_filter(toron_kind(crate::nostr::kinds::AGENT_PROFILE), project_slug);
        match collect_replay(self.bus.as_ref(), filter).await {
            Ok(events) => {
                let now = Timestamp::now().as_secs() as i64;
                let holder_pubkey = res.agent_pubkey.clone();
                let last_active = events
                    .iter()
                    .filter(|e| e.pubkey.to_hex() == holder_pubkey)
                    .map(|e| e.created_at.as_secs() as i64)
                    .max();
                match last_active {
                    Some(ts) => now - ts > STALE_AFTER_SECS as i64,
                    // No profile at all → treat as stale (unknown agent).
                    None => true,
                }
            }
            Err(_) => false,
        }
    }

    /// Best-effort pubkey → agent-name map from kind-30013 profiles.
    async fn agent_name_map(
        &self,
        project_slug: &str,
    ) -> std::collections::HashMap<String, String> {
        let filter =
            kind_project_filter(toron_kind(crate::nostr::kinds::AGENT_PROFILE), project_slug);
        let Ok(events) = collect_replay(self.bus.as_ref(), filter).await else {
            return std::collections::HashMap::new();
        };
        events
            .iter()
            .map(|e| {
                (
                    e.pubkey.to_hex(),
                    crate::nostr::identity::parse_profile(e)
                        .map(|p| p.name)
                        .unwrap_or_default(),
                )
            })
            .collect()
    }

    /// Best-effort: fill `agent_name` from kind-30013 profiles.
    async fn fill_agent_names(&self, project_slug: &str, reservations: &mut [Reservation]) {
        let name_by_pubkey = self.agent_name_map(project_slug).await;
        for r in reservations.iter_mut() {
            if let Some(name) = name_by_pubkey.get(&r.agent_pubkey) {
                r.agent_name = name.clone();
            } else {
                r.agent_name = r.agent_pubkey.chars().take(8).collect();
            }
        }
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
        self.send_message(&send).await
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
        let wrap = gift_wrap::gift_wrap(&keys, &operator_npub, rumor).await?;
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
    async fn project_rumors(&self, project_slug: &str) -> Vec<nostr_sdk::nostr::UnsignedEvent> {
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
    async fn record_transition(
        &self,
        project_slug: &str,
        d: &str,
        status: &str,
        event_id: &EventId,
    ) {
        let root = crate::mailbox::aliases::project_root(&self.data_dir, project_slug);
        if let Ok(archive) = crate::archive::git::GitArchive::open_or_init(&root) {
            let rel = std::path::Path::new(".audit").join(format!("{}.{status}.json", event_id));
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
}

/// One conflicting other-agent lease hit for a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConflictHit {
    pub agent_name: String,
    pub reservation_id: String,
    pub path_pattern: String,
    pub status: ReservationStatus,
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
        .and_then(|t| t.as_slice().get(1).map(|s| s.to_string()));
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
fn lease_tag_value(tags: &nostr_sdk::nostr::Tags, name: &str) -> Option<String> {
    tags.iter().find_map(|t| {
        (t.as_slice().first().map(|s| s == name).unwrap_or(false))
            .then(|| t.as_slice().get(1).map(|s| s.to_string()))
            .flatten()
    })
}

/// True when a rumor's `p` tags address `npub_hex` (kind-14 recipient list).
fn rumor_addresses(rumor: &nostr_sdk::nostr::UnsignedEvent, npub_hex: &str) -> bool {
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
fn decision_from_tags(tags: &nostr_sdk::nostr::Tags) -> Option<ReleaseDecision> {
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

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> PublicKey {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        PublicKey::from_hex(&agent.npub).expect("npub")
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
            .kinds([nostr_sdk::nostr::Kind::GiftWrap])
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
            &nostr_sdk::nostr::Keys::new(
                nostr_sdk::nostr::SecretKey::from_hex(
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
            &nostr_sdk::nostr::Keys::new(
                nostr_sdk::nostr::SecretKey::from_hex(
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
        let wrap = gift_wrap::gift_wrap(&rose_keys, &blue_pk, backdated)
            .await
            .expect("wrap");
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
}
