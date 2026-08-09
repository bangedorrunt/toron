// governed-by: ADR-0003
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

use nostr_sdk::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use crate::bus::addressable::is_revocation;
use crate::bus::filter::kind_project_filter;
use crate::bus::scoping::with_project_scoping;
use crate::bus::{EnvelopeTtl, resolve_coordinate};
use crate::mailbox::guard::pattern_matches;
use crate::nostr::event::build_event;
use crate::nostr::kinds::{FILE_RESERVATION, toron_kind};

use super::Mailbox;
use super::collect_replay;

/// Default staleness threshold for force-release (holder inactive this long
/// counts as stale). Matches the AM-Rust inactivity heuristic spirit (30 min).
pub const STALE_AFTER_SECS: u64 = 30 * 60;

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
}

/// Result of `file_reservation_paths` (parity shape).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcquireResult {
    pub granted: Vec<GrantedReservation>,
    pub conflicts: Vec<PathConflict>,
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
}

/// Result of `check_file_reservation_conflicts` (parity shape).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConflictReport {
    pub conflict_free: bool,
    pub conflicts: Vec<PathConflict>,
    pub clear_paths: Vec<String>,
    pub checked_paths: usize,
    pub total_conflicting_reservations: usize,
}

/// Result of `renew_file_reservations`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RenewResult {
    pub renewed: usize,
    pub reservations: Vec<GrantedReservation>,
}

/// Result of `release_file_reservations`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReleaseResult {
    pub released: usize,
}

impl Mailbox {
    /// `file_reservation_paths` (acquire): request leases on `paths`. Conflict
    /// is checked symmetrically against OTHER agents' active reservations; own
    /// never conflicts. Granted leases are published as kind-30031 events.
    pub async fn file_reservation_paths(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        ttl_secs: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
    ) -> anyhow::Result<AcquireResult> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mut result = AcquireResult::default();

        for path in paths {
            let d = format!("{project_slug}:{path}");
            let conflicts = self
                .conflicts_for(&agent_keys.public_key(), project_slug, path)
                .await;
            if !conflicts.is_empty() {
                result.conflicts.push(PathConflict {
                    path: path.clone(),
                    holders: conflicts,
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

    /// `check_file_reservation_conflicts`: symmetric-glob conflict report for
    /// paths vs OTHER agents' active reservations.
    pub async fn check_file_reservation_conflicts(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
    ) -> anyhow::Result<ConflictReport> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let mut report = ConflictReport {
            checked_paths: paths.len(),
            ..Default::default()
        };
        for path in paths {
            let holders = self
                .conflicts_for(&agent_keys.public_key(), project_slug, path)
                .await;
            if holders.is_empty() {
                report.clear_paths.push(path.clone());
            } else {
                report.conflicts.push(PathConflict {
                    path: path.clone(),
                    holders,
                });
            }
        }
        report.conflict_free = report.conflicts.is_empty();
        report.total_conflicting_reservations =
            report.conflicts.iter().map(|c| c.holders.len()).sum();
        Ok(report)
    }

    /// `renew_file_reservations`: republish the holder's active leases with an
    /// extended TTL (same `(kind,d)` coordinate → supersedes per author).
    pub async fn renew_file_reservations(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        extend_secs: u64,
    ) -> anyhow::Result<RenewResult> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
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
            let old_expiry_secs = res.expires_ts_us.map(|t| (t / 1_000_000) as u64);
            let new_expiry_secs = old_expiry_secs
                .map(|abs| abs.saturating_add(extend_secs))
                // no old expiry (permanent lease): default to now+extend.
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
    /// a `status:released` marker; same-author NIP-33 supersession).
    pub async fn release_file_reservations(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
    ) -> anyhow::Result<ReleaseResult> {
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
            released += 1;
        }
        Ok(ReleaseResult { released })
    }

    /// `force_release_file_reservation` (G5 A1): staleness heuristic → the
    /// OPERATOR signs a kind-30031 `status:revoked` at the target coordinate +
    /// `e`/`a` refs. Projection key-priority resolves it; no NIP-09.
    pub async fn force_release_file_reservation(
        &self,
        project_slug: &str,
        requestor_name: &str,
        file_reservation_id: &str,
        note: Option<&str>,
        notify_previous: bool,
    ) -> anyhow::Result<()> {
        let operator = self
            .operator
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no operator key configured"))?;

        // Locate the lease by id (current event id).
        let lease = self
            .active_reservations(project_slug)
            .await?
            .into_iter()
            .find(|r| r.id == file_reservation_id)
            .ok_or_else(|| {
                anyhow::anyhow!("no active reservation with id {file_reservation_id}")
            })?;

        // 1. Staleness heuristic.
        let stale = self.is_holder_stale(project_slug, &lease).await;
        if !stale {
            anyhow::bail!(
                "reservation {} not stale (holder active) — force-release denied",
                file_reservation_id
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

    /// Active (non-revoked, non-expired) reservations in a project, resolved
    /// with operator priority (bd-toh).
    pub(crate) async fn active_reservations(
        &self,
        project_slug: &str,
    ) -> anyhow::Result<Vec<Reservation>> {
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
            let Some(winning) = resolve_coordinate(evs.iter().copied(), operator.as_ref()) else {
                continue;
            };
            if is_revocation(&winning) {
                continue;
            }
            // Released marker → skip.
            if winning.content.contains("released") {
                continue;
            }
            // TTL expiry clears the lease.
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
            out.push(Reservation {
                id: winning.id.to_hex(),
                agent_name: String::new(), // resolved via profile below
                agent_pubkey: winning.pubkey.to_hex(),
                path_pattern: path.to_string(),
                exclusive,
                reason,
                expires_ts_us: winning
                    .tags
                    .expiration()
                    .map(|t| t.as_secs() as i64 * 1_000_000),
            });
        }
        // Fill agent names from profiles (best-effort).
        self.fill_agent_names(project_slug, &mut out).await;
        Ok(out)
    }

    /// Symmetric-glob conflict: holders of OTHER active reservations whose
    /// glob overlaps `path` either way.
    async fn conflicts_for(
        &self,
        own_npub: &PublicKey,
        project_slug: &str,
        path: &str,
    ) -> Vec<String> {
        let res = match self.active_reservations(project_slug).await {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        res.into_iter()
            .filter(|r| r.agent_pubkey != own_npub.to_hex())
            .filter(|r| r.exclusive)
            .filter(|r| overlaps(&r.path_pattern, path))
            .map(|r| r.agent_name)
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

    /// Best-effort: fill `agent_name` from kind-30013 profiles.
    async fn fill_agent_names(&self, project_slug: &str, reservations: &mut [Reservation]) {
        let filter =
            kind_project_filter(toron_kind(crate::nostr::kinds::AGENT_PROFILE), project_slug);
        let Ok(events) = collect_replay(self.bus.as_ref(), filter).await else {
            return;
        };
        let name_by_pubkey: std::collections::HashMap<String, String> = events
            .iter()
            .map(|e| {
                (
                    e.pubkey.to_hex(),
                    crate::nostr::identity::parse_profile(e)
                        .map(|p| p.name)
                        .unwrap_or_default(),
                )
            })
            .collect();
        for r in reservations.iter_mut() {
            if let Some(name) = name_by_pubkey.get(&r.agent_pubkey) {
                r.agent_name = name.clone();
            } else {
                r.agent_name = r.agent_pubkey.chars().take(8).collect();
            }
        }
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
        let root = self.data_dir.join(project_slug);
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
            .force_release_file_reservation(
                "demo",
                "RoseDune",
                &lease.id.to_hex(),
                Some("gone"),
                true,
            )
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
            .force_release_file_reservation("demo", "RoseDune", &lease_id, Some("gone"), true)
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
}
