// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Build-slot store (bd-led.5.9): pure functional core for the build-slots
//! cluster (acquire/renew/release/force-revoke). The mailbox layer wraps this
//! with key resolution + kind-30032 publishing; this module owns the
//! SQLite-backed projection + conflict/TTL/revocation RULES so both the tools
//! and the e2e scenarios share one source of truth.
//!
//! Wire (ADR-0003): kind-30032 addressable; `d`=`project:slot`; NIP-40
//! expiration=TTL. Force-release = operator revocation (status:revoked,
//! projection key-priority operator>agent, G5 A1).

use sqlx::Row;
use sqlx::SqlitePool;

/// A build slot lease (projection view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildSlot {
    pub slot: String,
    pub agent_name: String,
    pub agent_pubkey: String,
    pub exclusive: bool,
    pub status: SlotStatus,
    pub expires_ts_us: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    Active,
    Released,
    Revoked,
}

impl SlotStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SlotStatus::Active => "active",
            SlotStatus::Released => "released",
            SlotStatus::Revoked => "revoked",
        }
    }
}

/// Outcome of an acquire attempt.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AcquireSlotResult {
    pub granted: Vec<GrantedSlot>,
    pub conflicts: Vec<SlotConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantedSlot {
    pub slot: String,
    pub exclusive: bool,
    pub expires_ts_us: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotConflict {
    pub slot: String,
    pub holders: Vec<String>,
}

/// A slot lease to insert (request shape).
#[derive(Debug, Clone)]
pub struct SlotLease {
    pub project_slug: String,
    pub slot: String,
    pub agent_name: String,
    pub agent_pubkey: String,
    pub exclusive: bool,
    pub event_id: String,
    pub expires_ts_us: Option<i64>,
}

/// Insert a slot lease (idempotent on (project_slug, slot)).
pub async fn insert_slot(pool: &SqlitePool, lease: &SlotLease) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO build_slots \
         (project_slug, slot, agent_name, agent_pubkey, exclusive, status, \
          event_id, expires_ts_us, created_ts_us) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?7, ?8) \
         ON CONFLICT (project_slug, slot) DO UPDATE SET \
           agent_name = excluded.agent_name, \
           agent_pubkey = excluded.agent_pubkey, \
           exclusive = excluded.exclusive, \
           status = 'active', \
           event_id = excluded.event_id, \
           expires_ts_us = excluded.expires_ts_us",
    )
    .bind(&lease.project_slug)
    .bind(&lease.slot)
    .bind(&lease.agent_name)
    .bind(&lease.agent_pubkey)
    .bind(lease.exclusive)
    .bind(&lease.event_id)
    .bind(lease.expires_ts_us)
    .bind(now_us())
    .execute(pool)
    .await?;
    Ok(())
}

/// Active (non-revoked, non-expired) leases for a project. TTL expiry is
/// applied here (an expired lease is not active).
pub async fn active_slots(pool: &SqlitePool, project_slug: &str) -> anyhow::Result<Vec<BuildSlot>> {
    let now = now_us();
    let rows = sqlx::query(
        "SELECT slot, agent_name, agent_pubkey, exclusive, status, expires_ts_us \
         FROM build_slots WHERE project_slug = ?1 AND status = 'active'",
    )
    .bind(project_slug)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let expires: Option<i64> = r.get("expires_ts_us");
            // Expired → not active.
            if let Some(exp) = expires
                && exp <= now
            {
                return None;
            }
            Some(BuildSlot {
                slot: r.get("slot"),
                agent_name: r.get("agent_name"),
                agent_pubkey: r.get("agent_pubkey"),
                exclusive: r.get("exclusive"),
                status: SlotStatus::Active,
                expires_ts_us: expires,
            })
        })
        .collect())
}

/// Exclusive acquire rule: an exclusive lease conflicts with ANY other holder
/// of the slot (exclusive serializes); a shared lease conflicts only with an
/// exclusive holder. Own leases never conflict.
pub fn conflict_for(
    active: &[BuildSlot],
    slot: &str,
    requester_pubkey: &str,
    exclusive: bool,
) -> Vec<String> {
    active
        .iter()
        .filter(|s| s.slot == slot && s.agent_pubkey != requester_pubkey)
        .filter(|s| exclusive || s.exclusive)
        .map(|s| s.agent_pubkey.clone())
        .collect()
}

/// Release a slot (holder-initiated).
pub async fn release_slot(
    pool: &SqlitePool,
    project_slug: &str,
    slot: &str,
    agent_pubkey: &str,
) -> anyhow::Result<bool> {
    let rows = sqlx::query(
        "UPDATE build_slots SET status = 'released' \
         WHERE project_slug = ?1 AND slot = ?2 AND agent_pubkey = ?3 AND status = 'active'",
    )
    .bind(project_slug)
    .bind(slot)
    .bind(agent_pubkey)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Force-release via operator revocation (G5 A1): an operator-signed
/// revocation wins by projection key-priority `operator > agent`. This marks
/// the lease revoked. The operator's signed kind-30032 event is the audit
/// record (Git archive); this is the projection's resolution.
pub async fn revoke_slot(
    pool: &SqlitePool,
    project_slug: &str,
    slot: &str,
) -> anyhow::Result<bool> {
    let rows = sqlx::query(
        "UPDATE build_slots SET status = 'revoked' \
         WHERE project_slug = ?1 AND slot = ?2 AND status = 'active'",
    )
    .bind(project_slug)
    .bind(slot)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Renew: extend an active lease's expiration.
pub async fn renew_slot(
    pool: &SqlitePool,
    project_slug: &str,
    slot: &str,
    agent_pubkey: &str,
    expires_ts_us: i64,
) -> anyhow::Result<bool> {
    let rows = sqlx::query(
        "UPDATE build_slots SET expires_ts_us = ?4 \
         WHERE project_slug = ?1 AND slot = ?2 AND agent_pubkey = ?3 AND status = 'active'",
    )
    .bind(project_slug)
    .bind(slot)
    .bind(agent_pubkey)
    .bind(expires_ts_us)
    .execute(pool)
    .await?;
    Ok(rows.rows_affected() > 0)
}

/// Microsecond clock (i64-µs, ADR G7).
fn now_us() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema;

    async fn pool(tag: &str) -> (SqlitePool, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("toron-slots-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("slots.sqlite");
        let p = schema::connect(db.to_str().unwrap()).await.unwrap();
        schema::migrate(&p).await.unwrap();
        (p, dir)
    }

    fn lease(slot: &str, agent: &str, npub: &str, exclusive: bool) -> SlotLease {
        SlotLease {
            project_slug: "demo".into(),
            slot: slot.into(),
            agent_name: agent.into(),
            agent_pubkey: npub.into(),
            exclusive,
            event_id: format!("e-{slot}-{agent}"),
            expires_ts_us: None,
        }
    }

    #[tokio::test]
    async fn exclusive_acquire_conflicts_with_others() {
        let (p, dir) = pool("excl").await;
        insert_slot(&p, &lease("ci", "A", "aaa", true))
            .await
            .unwrap();
        let active = active_slots(&p, "demo").await.unwrap();

        // B (exclusive) conflicts with A's exclusive.
        let c1 = conflict_for(&active, "ci", "bbb", true);
        assert_eq!(c1, vec!["aaa".to_string()], "exclusive vs exclusive");
        // B (shared) also conflicts with A's exclusive.
        let c2 = conflict_for(&active, "ci", "bbb", false);
        assert_eq!(c2, vec!["aaa".to_string()], "shared vs exclusive");
        // A re-acquiring its own slot never conflicts.
        let c3 = conflict_for(&active, "ci", "aaa", true);
        assert!(c3.is_empty(), "own lease never conflicts");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn shared_does_not_conflict_with_shared() {
        let (p, dir) = pool("shared").await;
        insert_slot(&p, &lease("ci", "A", "aaa", false))
            .await
            .unwrap();
        let active = active_slots(&p, "demo").await.unwrap();
        let c = conflict_for(&active, "ci", "bbb", false);
        assert!(c.is_empty(), "shared holders coexist");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn expired_lease_not_active() {
        let (p, dir) = pool("expiry").await;
        // Lease expires in the past.
        insert_slot(
            &p,
            &SlotLease {
                expires_ts_us: Some(now_us() - 1000),
                ..lease("ci", "A", "aaa", true)
            },
        )
        .await
        .unwrap();
        let active = active_slots(&p, "demo").await.unwrap();
        assert!(active.is_empty(), "expired lease is not active");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn release_and_revoke_flows() {
        let (p, dir) = pool("lifecycle").await;
        insert_slot(&p, &lease("ci", "A", "aaa", true))
            .await
            .unwrap();

        // Release by holder works; second release no-ops.
        assert!(release_slot(&p, "demo", "ci", "aaa").await.unwrap());
        assert!(!release_slot(&p, "demo", "ci", "aaa").await.unwrap());

        // Re-acquire, then force-revoke via operator (G5 A1).
        insert_slot(&p, &lease("ci", "B", "bbb", true))
            .await
            .unwrap();
        assert!(revoke_slot(&p, "demo", "ci").await.unwrap());
        assert!(
            active_slots(&p, "demo").await.unwrap().is_empty(),
            "revoked not active"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn renew_extends_expiry() {
        let (p, dir) = pool("renew").await;
        let t0 = now_us();
        insert_slot(
            &p,
            &SlotLease {
                expires_ts_us: Some(t0 + 60_000_000),
                ..lease("ci", "A", "aaa", true)
            },
        )
        .await
        .unwrap();
        assert!(
            renew_slot(&p, "demo", "ci", "aaa", t0 + 600_000_000)
                .await
                .unwrap()
        );
        let active = active_slots(&p, "demo").await.unwrap();
        assert_eq!(active[0].expires_ts_us, Some(t0 + 600_000_000), "renewed");
        let _ = std::fs::remove_dir_all(dir);
    }
}
