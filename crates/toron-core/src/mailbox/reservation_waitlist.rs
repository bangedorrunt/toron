// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-ll7bs: FIFO waiters for a held path.
//!
//! A blocked `reserve acquire --queue` records one row here and sends the
//! holder ONE typed request-release. Every release path reads the head waiter
//! from this table and grants through `transfer_reservation` — one handoff
//! helper, never a second grant path. A waiter is removed once by the grant,
//! so a release never strands the queue.

use serde::{Deserialize, Serialize};

use super::{AcquireResult, Mailbox, ReleaseAsk, RequestRelease, Reservation, ReservationStatus};

/// One queued waiter, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Waiter {
    pub seq: i64,
    pub project_slug: String,
    pub path_pattern: String,
    pub agent_name: String,
    pub reason: Option<String>,
    pub created_us: i64,
}

/// The pure head-of-line rule: `head` is the first arrival not already served.
/// No clock, socket or SQLite here — the host passes the ordered rows.
#[must_use]
pub fn head_of_line<'a>(waiters: &'a [Waiter], served: &[String]) -> Option<&'a Waiter> {
    waiters.iter().find(|w| !served.contains(&w.agent_name))
}

impl Mailbox {
    /// Acquire with `--queue`: a hard conflict leaves the lease board alone,
    /// records ONE waiter per path, and sends each holder one typed
    /// request-release. The lease is granted later by the release handoff.
    #[allow(clippy::too_many_arguments)]
    pub async fn file_reservation_paths_queued(
        &self,
        project_slug: &str,
        agent_name: &str,
        paths: &[String],
        ttl_seconds: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
        repo_root: Option<&str>,
    ) -> anyhow::Result<AcquireResult> {
        let out = self
            .file_reservation_paths_with_root(
                project_slug,
                agent_name,
                paths,
                ttl_seconds,
                exclusive,
                reason,
                repo_root,
            )
            .await?;
        for conflict in &out.conflicts {
            if !self
                .enqueue_waiter(project_slug, &conflict.path, agent_name, reason)
                .await?
            {
                continue; // one row and one ask per (path, agent)
            }
            for (holder, id) in conflict.holders.iter().zip(conflict.reservation_ids.iter()) {
                let _ = self
                    .request_release(&RequestRelease {
                        project_slug: project_slug.to_string(),
                        sender_name: agent_name.to_string(),
                        target_agent: holder.clone(),
                        reservation_id: id.clone(),
                        ask: ReleaseAsk::Release,
                        note: reason.map(str::to_string),
                    })
                    .await;
            }
        }
        Ok(out)
    }

    /// Enqueue one waiter. Idempotent per `(project, path, agent)`: a repeat
    /// acquire does not stack a second row or send a second request-release.
    /// Returns true when this call created the row.
    pub async fn enqueue_waiter(
        &self,
        project_slug: &str,
        path_pattern: &str,
        agent_name: &str,
        reason: Option<&str>,
    ) -> anyhow::Result<bool> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let created_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let result = sqlx::query(
            "INSERT OR IGNORE INTO reservation_waitlist \
             (project_slug, path_pattern, agent_name, reason, created_us) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(&project_slug)
        .bind(path_pattern)
        .bind(agent_name)
        .bind(reason)
        .bind(created_us)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Every waiter for a path, oldest first.
    pub async fn list_waiters(
        &self,
        project_slug: &str,
        path_pattern: &str,
    ) -> anyhow::Result<Vec<Waiter>> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let rows: Vec<(i64, String, String, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT seq, project_slug, path_pattern, agent_name, reason, created_us \
             FROM reservation_waitlist WHERE project_slug = ? AND path_pattern = ? \
             ORDER BY seq",
        )
        .bind(&project_slug)
        .bind(path_pattern)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(seq, project_slug, path_pattern, agent_name, reason, created_us)| Waiter {
                    seq,
                    project_slug,
                    path_pattern,
                    agent_name,
                    reason,
                    created_us,
                },
            )
            .collect())
    }

    /// Every queued waiter in a project, oldest first.
    pub async fn list_project_waiters(&self, project_slug: &str) -> anyhow::Result<Vec<Waiter>> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let rows: Vec<(i64, String, String, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT seq, project_slug, path_pattern, agent_name, reason, created_us \
             FROM reservation_waitlist WHERE project_slug = ? ORDER BY seq",
        )
        .bind(&project_slug)
        .fetch_all(pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(
                |(seq, project_slug, path_pattern, agent_name, reason, created_us)| Waiter {
                    seq,
                    project_slug,
                    path_pattern,
                    agent_name,
                    reason,
                    created_us,
                },
            )
            .collect())
    }

    /// Hand waitlisted paths to their head waiter when no live holder remains.
    /// Called from the lease-board read that discovers an expired lease; the
    /// caller's inflight guard keeps this from recursing through that read.
    pub async fn sweep_expired_waitlists(&self, project_slug: &str) -> anyhow::Result<usize> {
        use std::collections::HashSet;

        let waiters = self.list_project_waiters(project_slug).await?;
        if waiters.is_empty() {
            return Ok(0);
        }
        let leases = self.resolved_leases(project_slug).await?;
        let mut seen = HashSet::new();
        let mut handed = 0;
        for w in waiters {
            if !seen.insert(w.path_pattern.clone()) {
                continue;
            }
            let on_path: Vec<_> = leases
                .iter()
                .filter(|l| l.path_pattern == w.path_pattern && l.status == ReservationStatus::Held)
                .collect();
            if on_path.iter().any(|l| !l.expired) {
                continue; // a live holder still owns the path
            }
            if let Some(expired) = on_path.first() {
                let mut lease = Reservation {
                    id: expired.id.clone(),
                    agent_name: String::new(),
                    agent_pubkey: expired.agent_pubkey.clone(),
                    path_pattern: expired.path_pattern.clone(),
                    exclusive: expired.exclusive,
                    reason: expired.reason.clone(),
                    expires_ts_us: expired.expires_ts_us,
                    status: ReservationStatus::Held,
                    epoch: expired.epoch,
                    repo_root: expired.repo_root.clone(),
                };
                self.fill_agent_names(project_slug, std::slice::from_mut(&mut lease))
                    .await;
                if self.handoff_waiter(project_slug, &lease).await {
                    handed += 1;
                }
            } else {
                // The expired lease has left the board entirely: grant the
                // head waiter through the ordinary acquire path.
                let out = self
                    .file_reservation_paths(
                        project_slug,
                        &w.agent_name,
                        std::slice::from_ref(&w.path_pattern),
                        Some(super::reservations::DEFAULT_RESERVATION_TTL_SECS),
                        true,
                        w.reason.as_deref(),
                    )
                    .await?;
                if !out.granted.is_empty() {
                    self.remove_waiter(w.seq).await?;
                    handed += 1;
                }
            }
        }
        Ok(handed)
    }

    /// The next agent to receive a freed path, or None when the queue is empty.
    pub async fn next_waiter(
        &self,
        project_slug: &str,
        path_pattern: &str,
    ) -> anyhow::Result<Option<Waiter>> {
        let pool = self.index_pool_required()?;
        let project_slug = self.canonical_project(project_slug).await;
        let row: Option<(i64, String, String, String, Option<String>, i64)> = sqlx::query_as(
            "SELECT seq, project_slug, path_pattern, agent_name, reason, created_us \
             FROM reservation_waitlist WHERE project_slug = ? AND path_pattern = ? \
             ORDER BY seq LIMIT 1",
        )
        .bind(&project_slug)
        .bind(path_pattern)
        .fetch_optional(pool)
        .await?;
        Ok(row.map(
            |(seq, project_slug, path_pattern, agent_name, reason, created_us)| Waiter {
                seq,
                project_slug,
                path_pattern,
                agent_name,
                reason,
                created_us,
            },
        ))
    }

    /// Remove exactly one waiter after it was granted the path.
    pub async fn remove_waiter(&self, seq: i64) -> anyhow::Result<()> {
        let pool = self.index_pool_required()?;
        sqlx::query("DELETE FROM reservation_waitlist WHERE seq = ?")
            .bind(seq)
            .execute(pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn waiter(agent: &str) -> Waiter {
        Waiter {
            seq: 1,
            project_slug: "p".into(),
            path_pattern: "src/lib.rs".into(),
            agent_name: agent.into(),
            reason: None,
            created_us: 1,
        }
    }

    #[tokio::test]
    async fn waitlist_rows_are_idempotent_and_fifo() {
        use std::sync::Arc;

        use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
        use crate::index;

        let db_path =
            std::env::temp_dir().join(format!("toron-waitlist-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = index::schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        index::schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-waitlist-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(
            Arc::new(bus),
            data_dir.clone(),
            Some(crate::keys::generate()),
        )
        .with_index_pool(pool);

        assert!(
            mailbox
                .enqueue_waiter("p", "src/lib.rs", "A", Some("bd-ll7bs"))
                .await
                .expect("enqueue A")
        );
        assert!(
            !mailbox
                .enqueue_waiter("p", "src/lib.rs", "A", Some("bd-ll7bs"))
                .await
                .expect("dedupe A"),
            "a repeat acquire must not stack a second row"
        );
        assert!(
            mailbox
                .enqueue_waiter("p", "src/lib.rs", "B", None)
                .await
                .expect("enqueue B")
        );
        let rows = mailbox.list_waiters("p", "src/lib.rs").await.expect("list");
        assert_eq!(
            rows.iter()
                .map(|w| w.agent_name.as_str())
                .collect::<Vec<_>>(),
            vec!["A", "B"]
        );
        assert_eq!(
            head_of_line(&rows, &[]).map(|w| w.agent_name.as_str()),
            Some("A")
        );
        let head = mailbox
            .next_waiter("p", "src/lib.rs")
            .await
            .expect("head")
            .expect("a head exists");
        assert_eq!(head.agent_name, "A");
        mailbox.remove_waiter(head.seq).await.expect("remove");
        assert_eq!(
            mailbox
                .next_waiter("p", "src/lib.rs")
                .await
                .expect("next")
                .map(|w| w.agent_name),
            Some("B".to_string())
        );
    }

    #[test]
    fn head_of_line_is_fifo_and_skips_served() {
        let rows = vec![waiter("A"), waiter("B")];
        assert_eq!(
            head_of_line(&rows, &[]).map(|w| w.agent_name.as_str()),
            Some("A")
        );
        assert_eq!(
            head_of_line(&rows, &["A".to_string()]).map(|w| w.agent_name.as_str()),
            Some("B")
        );
        assert!(head_of_line(&rows, &["A".to_string(), "B".to_string()]).is_none());
    }
}
