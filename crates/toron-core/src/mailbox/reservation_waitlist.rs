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

use super::Mailbox;

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
