// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Macros cluster (bd-led.5.7): 4 AM-Rust parity macros that batch common
//! multi-step mailbox ops. **Tool-layer only** — no new Nostr kinds; each
//! macro composes the identity/messaging/reservation/contact/search
//! primitives (G1–G14) and preserves AM-Rust return shapes.

use crate::mailbox::contacts::{RequestContact, RespondContact};
use crate::mailbox::messaging::{FetchInbox, SendMessage};
use crate::mailbox::{ContactLink, ThreadSummary};
use crate::nostr::identity::AgentProfile;

use super::Mailbox;

/// Result of `macro_start_session`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartSessionResult {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub granted_reservations: Vec<crate::mailbox::GrantedReservation>,
    pub inbox_count: usize,
}

/// Result of `macro_prepare_thread`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrepareThreadResult {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub thread_id: String,
    pub summary: ThreadSummary,
    pub inbox_count: usize,
}

/// Result of `macro_contact_handshake`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContactHandshakeResult {
    pub requester: String,
    pub target: String,
    pub link: Option<ContactLink>,
    pub welcome_sent: bool,
}

/// Result of `macro_file_reservation_cycle`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReservationCycleResult {
    pub project_key: String,
    pub agent_name: String,
    pub granted: Vec<crate::mailbox::GrantedReservation>,
    pub released: usize,
}

impl Mailbox {
    /// `macro_start_session`: ensure_project + register/create agent +
    /// optional file reservation + inbox snapshot.
    #[allow(clippy::too_many_arguments)]
    pub async fn macro_start_session(
        &self,
        project_key: &str,
        agent_name: &str,
        program: &str,
        model: &str,
        task_description: Option<&str>,
        reservation_paths: Option<&[String]>,
        reservation_ttl_secs: Option<u64>,
        reservation_reason: Option<&str>,
        inbox_limit: Option<usize>,
    ) -> anyhow::Result<StartSessionResult> {
        // 1. Ensure the project manifest exists (idempotent).
        self.ensure_project(project_key).await?;

        // 2. Register/create the agent (idempotent; create_agent_identity
        //    would mint a fresh key each time, so use register for sessions).
        let profile = AgentProfile {
            name: agent_name.to_string(),
            program: Some(program.to_string()),
            model: Some(model.to_string()),
            task_description: task_description.map(str::to_string),
            persona: None,
        };
        let agent = self.register_agent(project_key, &profile).await?;

        // 3. Optional reservation cycle.
        let mut granted = Vec::new();
        if let Some(paths) = reservation_paths {
            let res = self
                .file_reservation_paths(
                    project_key,
                    agent_name,
                    paths,
                    reservation_ttl_secs,
                    true,
                    reservation_reason,
                )
                .await?;
            granted = res.granted;
        }

        // 4. Inbox snapshot.
        let inbox = self
            .fetch_inbox(&FetchInbox {
                project_slug: project_key.to_string(),
                agent_name: agent_name.to_string(),
                limit: inbox_limit,
                include_bodies: true,
                ..Default::default()
            })
            .await?;

        Ok(StartSessionResult {
            project_key: project_key.to_string(),
            agent_name: agent_name.to_string(),
            npub: agent.npub,
            granted_reservations: granted,
            inbox_count: inbox.len(),
        })
    }

    /// `macro_prepare_thread`: ensure registration + summarize the thread +
    /// fetch recent inbox context.
    #[allow(clippy::too_many_arguments)]
    pub async fn macro_prepare_thread(
        &self,
        project_key: &str,
        thread_id: &str,
        agent_name: &str,
        program: &str,
        model: &str,
        task_description: Option<&str>,
        register_if_missing: bool,
        include_examples: bool,
        inbox_limit: Option<usize>,
    ) -> anyhow::Result<PrepareThreadResult> {
        // Ensure registration (upsert if absent; no-op if the agent exists).
        if register_if_missing {
            let profile = AgentProfile {
                name: agent_name.to_string(),
                program: Some(program.to_string()),
                model: Some(model.to_string()),
                task_description: task_description.map(str::to_string),
                persona: None,
            };
            let agent = self.register_agent(project_key, &profile).await?;
            let _ = agent;
        }
        let who = self.whois(project_key, agent_name).await?;

        // Summarize the thread.
        let summary = self
            .summarize_thread(project_key, thread_id, include_examples, false, None)
            .await?;

        // Recent inbox context.
        let inbox = self
            .fetch_inbox(&FetchInbox {
                project_slug: project_key.to_string(),
                agent_name: agent_name.to_string(),
                limit: inbox_limit,
                include_bodies: true,
                ..Default::default()
            })
            .await?;

        Ok(PrepareThreadResult {
            project_key: project_key.to_string(),
            agent_name: agent_name.to_string(),
            npub: who.npub,
            thread_id: thread_id.to_string(),
            summary,
            inbox_count: inbox.len(),
        })
    }

    /// `macro_contact_handshake`: request contact + optionally auto-approve +
    /// send a welcome message.
    #[allow(clippy::too_many_arguments)]
    pub async fn macro_contact_handshake(
        &self,
        project_key: &str,
        requester: &str,
        target: &str,
        reason: Option<&str>,
        auto_accept: bool,
        welcome_subject: Option<&str>,
        welcome_body: Option<&str>,
    ) -> anyhow::Result<ContactHandshakeResult> {
        let link = self
            .request_contact(&RequestContact {
                project_slug: project_key.to_string(),
                from_agent: requester.to_string(),
                to_agent: target.to_string(),
                to_project: None,
                reason: reason.map(str::to_string),
            })
            .await?;

        let mut welcome_sent = false;
        let final_link = if auto_accept {
            let approved = self
                .respond_contact(&RespondContact {
                    project_slug: project_key.to_string(),
                    to_agent: target.to_string(),
                    from_agent: requester.to_string(),
                    accept: true,
                    from_project: None,
                })
                .await?;

            if let (Some(subject), Some(body)) = (welcome_subject, welcome_body) {
                self.send_message(&SendMessage {
                    project_slug: project_key.to_string(),
                    sender_name: target.to_string(),
                    to: vec![requester.to_string()],
                    subject: subject.to_string(),
                    body_md: body.to_string(),
                    ..Default::default()
                })
                .await?;
                welcome_sent = true;
            }
            approved
        } else {
            link
        };

        Ok(ContactHandshakeResult {
            requester: requester.to_string(),
            target: target.to_string(),
            link: Some(final_link),
            welcome_sent,
        })
    }

    /// `macro_file_reservation_cycle`: reserve a set of paths, optionally
    /// release them at the end.
    #[allow(clippy::too_many_arguments)]
    pub async fn macro_file_reservation_cycle(
        &self,
        project_key: &str,
        agent_name: &str,
        paths: &[String],
        ttl_seconds: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
        auto_release: bool,
    ) -> anyhow::Result<ReservationCycleResult> {
        let res = self
            .file_reservation_paths(
                project_key,
                agent_name,
                paths,
                ttl_seconds,
                exclusive,
                reason,
            )
            .await?;
        let granted = res.granted;

        let released = if auto_release {
            let r = self
                .release_file_reservations(project_key, agent_name, paths)
                .await?;
            r.released
        } else {
            0
        };

        Ok(ReservationCycleResult {
            project_key: project_key.to_string(),
            agent_name: agent_name.to_string(),
            granted,
            released,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use std::sync::Arc;

    const PASSPHRASE: &str = "macros-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-macro-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-macro-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    #[tokio::test]
    async fn test_macro_start_session_composes() {
        let (mailbox, db_path, data_dir) = test_mailbox("start").await;
        let result = mailbox
            .macro_start_session(
                "demo",
                "GreenCastle",
                "opencode",
                "deepseek-v4",
                Some("session agent"),
                Some(&["src/**".to_string()]),
                Some(3600),
                Some("onboarding"),
                None,
            )
            .await
            .expect("macro start session");
        assert_eq!(result.project_key, "demo");
        assert_eq!(result.agent_name, "GreenCastle");
        assert_eq!(result.npub.len(), 64);
        assert_eq!(result.granted_reservations.len(), 1, "reserved src/**");
        assert_eq!(result.granted_reservations[0].path_pattern, "src/**");
        assert_eq!(result.inbox_count, 0, "empty inbox snapshot");

        // whois confirms the agent registered.
        let w = mailbox.whois("demo", "GreenCastle").await.expect("whois");
        assert_eq!(w.npub, result.npub);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_macro_file_reservation_cycle_auto_release() {
        let (mailbox, db_path, data_dir) = test_mailbox("cycle").await;
        mailbox
            .register_agent("demo", &AgentProfile::new("BlueLake"))
            .await
            .expect("register");

        let result = mailbox
            .macro_file_reservation_cycle(
                "demo",
                "BlueLake",
                &["src/**".to_string()],
                Some(3600),
                true,
                Some("cycle"),
                true,
            )
            .await
            .expect("cycle");
        assert_eq!(result.granted.len(), 1);
        assert_eq!(result.released, 1, "auto_release released the lease");

        // No active reservation remains.
        let active = mailbox.active_reservations("demo").await.expect("active");
        assert!(
            active.iter().all(|r| r.path_pattern != "src/**"),
            "released after cycle"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_macro_contact_handshake_auto_accept_welcome() {
        let (mailbox, db_path, data_dir) = test_mailbox("handshake").await;
        mailbox
            .register_agent("demo", &AgentProfile::new("BlueLake"))
            .await
            .expect("register blue");
        mailbox
            .register_agent("demo", &AgentProfile::new("RoseDune"))
            .await
            .expect("register rose");

        let result = mailbox
            .macro_contact_handshake(
                "demo",
                "BlueLake",
                "RoseDune",
                Some("peer"),
                true,
                Some("welcome"),
                Some("hi"),
            )
            .await
            .expect("handshake");
        assert_eq!(result.requester, "BlueLake");
        assert_eq!(result.target, "RoseDune");
        assert_eq!(
            result.link.as_ref().map(|l| l.status.as_str()),
            Some("approved")
        );
        assert!(
            result.welcome_sent,
            "welcome message sent after auto-accept"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_macro_prepare_thread_composes() {
        let (mailbox, db_path, data_dir) = test_mailbox("thread").await;
        // Seed a thread in the index so summarize has content.
        let pool = mailbox.index_pool().unwrap();
        sqlx::query(
            "INSERT INTO messages \
             (event_id, project_slug, sender, sender_pubkey, subject, body, \
              importance, ack_required, thread_id, created_ts_us, local_seq) \
             VALUES ('m1', 'demo', 'PiAlpha', 'aaaa', 'Fix flaky', 'body', \
                     'normal', 1, 'T1', 1000000, 1) ON CONFLICT (event_id) DO NOTHING",
        )
        .execute(pool)
        .await
        .expect("seed");

        let result = mailbox
            .macro_prepare_thread(
                "demo",
                "T1",
                "GreenCastle",
                "opencode",
                "deepseek-v4",
                Some("thread prep"),
                true,
                true,
                None,
            )
            .await
            .expect("prepare thread");
        assert_eq!(result.thread_id, "T1");
        assert_eq!(result.summary.message_count, 1);
        assert_eq!(result.summary.key_points, vec!["Fix flaky".to_string()]);
        assert_eq!(
            result.summary.action_items,
            vec!["Fix flaky".to_string()],
            "ack-required"
        );
        assert!(result.npub.len() == 64);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
