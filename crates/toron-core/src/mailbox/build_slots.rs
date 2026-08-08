// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Build-slots cluster (bd-led.5.9): acquire_build_slot, renew_build_slot,
//! release_build_slot — the tool layer over oc-b's `index::slots` core.
//!
//! Wire (ADR-0003): kind-30032 addressable; `d`=`project:slot`; NIP-40
//! `expiration`=TTL; dual-emit scoping (`#t` + NIP-32 — kind-30032 is not
//! metadata-hiding). The functional core (conflict rules, TTL expiry,
//! release/revoke transitions) lives in `index::slots`; this module resolves
//! agent keys, publishes the signed events, and mirrors into the projection.

use nostr_sdk::nostr::{Tag, Timestamp};

use crate::bus::scoping::with_project_scoping;
use crate::index::slots;
use crate::mailbox::Mailbox;
use crate::nostr::event::build_event;
use crate::nostr::kinds::{BUILD_SLOT, toron_kind};

/// Acquire result (parity shape).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AcquireBuildSlotResult {
    pub granted: Vec<slots::GrantedSlot>,
    pub conflicts: Vec<slots::SlotConflict>,
}

impl Mailbox {
    /// `acquire_build_slot`: acquire an advisory slot lease, optionally
    /// exclusive. Exclusive serializes (conflicts with any holder); shared
    /// conflicts only with an exclusive holder. Own lease never conflicts.
    pub async fn acquire_build_slot(
        &self,
        project_slug: &str,
        agent_name: &str,
        slot: &str,
        ttl_secs: Option<u64>,
        exclusive: bool,
    ) -> anyhow::Result<AcquireBuildSlotResult> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let npub = agent_keys.public_key().to_hex();

        let pool = self.index_pool_required()?;
        let active = slots::active_slots(pool, project_slug).await?;
        let conflicts = slots::conflict_for(&active, slot, &npub, exclusive);
        if !conflicts.is_empty() {
            return Ok(AcquireBuildSlotResult {
                granted: Vec::new(),
                conflicts: vec![slots::SlotConflict {
                    slot: slot.to_string(),
                    holders: conflicts,
                }],
            });
        }

        // Publish the kind-30032 lease (NIP-40 expiration + dual-emit).
        let d = format!("{project_slug}:{slot}");
        let created = Timestamp::now();
        let ttl_secs = ttl_secs.unwrap_or(crate::bus::DEFAULT_ENVELOPE_TTL_SECS);
        let expires = created.as_secs() + ttl_secs;
        let base = vec![
            Tag::parse(["d", &d]).expect("d"),
            Tag::expiration(Timestamp::from(expires)),
        ];
        let tags = with_project_scoping(toron_kind(BUILD_SLOT), base, Some(project_slug));
        let event = build_event(
            &agent_keys,
            toron_kind(BUILD_SLOT),
            format!(r#"{{"exclusive":{exclusive}}}"#),
            tags,
            created,
        )?;
        let ok = self.bus.publish(event.clone()).await?;
        if !ok.stored {
            anyhow::bail!("build-slot event rejected by bus");
        }

        // Mirror into the projection.
        slots::insert_slot(
            pool,
            &slots::SlotLease {
                project_slug: project_slug.to_string(),
                slot: slot.to_string(),
                agent_name: agent_name.to_string(),
                agent_pubkey: npub,
                exclusive,
                event_id: event.id.to_hex(),
                expires_ts_us: Some(expires as i64 * 1_000_000),
            },
        )
        .await?;

        Ok(AcquireBuildSlotResult {
            granted: vec![slots::GrantedSlot {
                slot: slot.to_string(),
                exclusive,
                expires_ts_us: Some(expires as i64 * 1_000_000),
            }],
            conflicts: Vec::new(),
        })
    }

    /// `renew_build_slot`: extend an active lease's TTL. No-op if missing.
    pub async fn renew_build_slot(
        &self,
        project_slug: &str,
        agent_name: &str,
        slot: &str,
        extend_secs: u64,
    ) -> anyhow::Result<Option<slots::BuildSlot>> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let npub = agent_keys.public_key().to_hex();
        let pool = self.index_pool_required()?;

        // Extension must be relative to the EXISTING lease expiry, not `now`.
        // Recomputing from `now` collapses a long-lived lease to now+extend
        // (parity bug bd-XXX: reported under-extension to ~10s).
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let existing = active_slot(pool, project_slug, slot).await;
        let base_abs_us = existing.and_then(|s| s.expires_ts_us).unwrap_or(now_us);
        let new_expiry = base_abs_us.saturating_add(extend_secs as i64 * 1_000_000);
        let renewed = slots::renew_slot(pool, project_slug, slot, &npub, new_expiry).await?;
        if !renewed {
            return Ok(None);
        }

        // Re-publish the lease with the extended TTL (operator supersedes per
        // author isolation).
        let d = format!("{project_slug}:{slot}");
        let created = Timestamp::now();
        let expires_secs = (new_expiry / 1_000_000) as u64;
        let base = vec![
            Tag::parse(["d", &d]).expect("d"),
            Tag::expiration(Timestamp::from(expires_secs)),
        ];
        let tags = with_project_scoping(toron_kind(BUILD_SLOT), base, Some(project_slug));
        let event = build_event(
            &agent_keys,
            toron_kind(BUILD_SLOT),
            r#"{"exclusive":true}"#,
            tags,
            created,
        )?;
        self.bus.publish(event.clone()).await?;

        Ok(active_slot(pool, project_slug, slot).await)
    }

    /// `release_build_slot`: mark an active lease released (non-destructive).
    pub async fn release_build_slot(
        &self,
        project_slug: &str,
        agent_name: &str,
        slot: &str,
    ) -> anyhow::Result<bool> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let npub = agent_keys.public_key().to_hex();
        let pool = self.index_pool_required()?;

        let released = slots::release_slot(pool, project_slug, slot, &npub).await?;
        if released {
            // Publish the status:released marker (own-event supersession).
            let d = format!("{project_slug}:{slot}");
            let base = vec![
                Tag::parse(["d", &d]).expect("d"),
                Tag::parse(["status", "released"]).expect("status"),
            ];
            let tags = with_project_scoping(toron_kind(BUILD_SLOT), base, Some(project_slug));
            let event = build_event(
                &agent_keys,
                toron_kind(BUILD_SLOT),
                r#"{"status":"released"}"#,
                tags,
                Timestamp::now(),
            )?;
            self.bus.publish(event.clone()).await?;
        }
        Ok(released)
    }
}

/// Fetch a single active slot from the projection.
async fn active_slot(
    pool: &sqlx::SqlitePool,
    project_slug: &str,
    slot: &str,
) -> Option<slots::BuildSlot> {
    slots::active_slots(pool, project_slug)
        .await
        .ok()?
        .into_iter()
        .find(|s| s.slot == slot)
}

#[cfg(test)]
mod tests {
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use std::sync::Arc;

    const PASSPHRASE: &str = "slots-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-slots-mb-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-slots-mb-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    async fn register(mailbox: &Mailbox, name: &str) {
        mailbox
            .register_agent("demo", &crate::nostr::identity::AgentProfile::new(name))
            .await
            .expect("register");
    }

    #[tokio::test]
    async fn exclusive_acquire_serializes_and_conflicts() {
        let (mailbox, db_path, data_dir) = test_mailbox("excl").await;
        register(&mailbox, "BlueLake").await;
        register(&mailbox, "RoseDune").await;

        // A acquires slot ci exclusively.
        let a = mailbox
            .acquire_build_slot("demo", "BlueLake", "ci", Some(3600), true)
            .await
            .expect("acquire a");
        assert_eq!(a.granted.len(), 1);
        assert_eq!(a.granted[0].slot, "ci");
        assert!(a.conflicts.is_empty());

        // B exclusive acquire conflicts.
        let b = mailbox
            .acquire_build_slot("demo", "RoseDune", "ci", Some(3600), true)
            .await
            .expect("acquire b");
        assert!(b.granted.is_empty());
        assert_eq!(b.conflicts.len(), 1);
        assert_eq!(b.conflicts[0].slot, "ci");
        assert!(!b.conflicts[0].holders.is_empty(), "holder identified");

        // A re-acquiring own slot never conflicts.
        let a2 = mailbox
            .acquire_build_slot("demo", "BlueLake", "ci", Some(3600), true)
            .await
            .expect("re-acquire a");
        assert_eq!(a2.granted.len(), 1, "own lease never conflicts");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn renew_and_release_lifecycle() {
        let (mailbox, db_path, data_dir) = test_mailbox("lifecycle").await;
        register(&mailbox, "BlueLake").await;

        mailbox
            .acquire_build_slot("demo", "BlueLake", "ci", Some(3600), true)
            .await
            .expect("acquire");

        let renewed = mailbox
            .renew_build_slot("demo", "BlueLake", "ci", 7200)
            .await
            .expect("renew");
        assert!(renewed.is_some(), "renew extends active lease");

        let released = mailbox
            .release_build_slot("demo", "BlueLake", "ci")
            .await
            .expect("release");
        assert!(released, "release marks lease released");

        // After release, slot is free for another agent.
        register(&mailbox, "RoseDune").await;
        let b = mailbox
            .acquire_build_slot("demo", "RoseDune", "ci", Some(3600), true)
            .await
            .expect("acquire after release");
        assert_eq!(b.granted.len(), 1, "freed slot acquirable");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn renew_missing_slot_is_noop() {
        let (mailbox, db_path, data_dir) = test_mailbox("noop").await;
        register(&mailbox, "BlueLake").await;
        let renewed = mailbox
            .renew_build_slot("demo", "BlueLake", "nonexistent", 7200)
            .await
            .expect("renew missing");
        assert!(renewed.is_none(), "renew missing slot is a no-op");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
