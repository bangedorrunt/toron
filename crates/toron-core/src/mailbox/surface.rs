// governed-by: ADR-0007
// R1 / D2 (ADR-0007): `MailSurface` — the coordination-fabric trait living
// next to `Mailbox`. Operation-shaped (send → inbox → ack → reply, contacts,
// agents, infra) — NOT the raw 38-tool surface. `&self` receivers, string ids
// ([`MessageId`]) end-to-end, machine-coded [`MailError`]. Flywheel can
// path-dep and re-export this later (ADR-0007 R1.5 deletes its local
// `MailBackend` in favor of this trait).
//
// Shared request/response types defined here carry NO u64 (ADR-0007 D1).

use async_trait::async_trait;

use super::contacts::{ContactLink, RequestContact, RespondContact};
use super::error::{MailError, MailSurfaceResult};
use super::ids::MessageId;
use super::identity::{ListAgentsOptions, WhoIs};
use super::infra::{HealthReport, ProjectState};
use super::messaging::{FetchInbox, InboxMessage, SendMessage};
use super::search::{SearchMessage, SearchMessages};
use super::unwrap::ContactPolicy;
use super::Mailbox;

/// `send` request (operation-shaped; `thread_id` only on replies).
#[derive(Debug, Clone, Default)]
pub struct SendMail {
    pub project: String,
    pub sender: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub importance: Option<String>,
    pub ack_required: bool,
    /// ADR-0004 Ship 5: operator Overseer override (signs with the operator
    /// key, skips contact-policy enforcement).
    pub overseer: bool,
}

impl From<&SendMail> for SendMessage {
    fn from(r: &SendMail) -> Self {
        SendMessage {
            project_slug: r.project.clone(),
            sender_name: r.sender.clone(),
            to: r.to.clone(),
            subject: r.subject.clone(),
            body_md: r.body_md.clone(),
            cc: r.cc.clone(),
            bcc: r.bcc.clone(),
            importance: r.importance.clone(),
            ack_required: r.ack_required,
            thread_id: None,
            sender_token: None,
            overseer: r.overseer,
            extra_tags: Vec::new(),
        }
    }
}

/// `reply` request — the original message's id threads the reply.
#[derive(Debug, Clone)]
pub struct ReplyMail {
    pub project: String,
    pub sender: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub ack_required: bool,
    pub thread_id: MessageId,
}

impl From<&ReplyMail> for SendMessage {
    fn from(r: &ReplyMail) -> Self {
        SendMessage {
            project_slug: r.project.clone(),
            sender_name: r.sender.clone(),
            to: r.to.clone(),
            subject: r.subject.clone(),
            body_md: r.body_md.clone(),
            cc: Vec::new(),
            bcc: Vec::new(),
            importance: None,
            ack_required: r.ack_required,
            thread_id: Some(r.thread_id.as_str().to_string()),
            sender_token: None,
            overseer: false,
            extra_tags: Vec::new(),
        }
    }
}

/// One recipient's delivery record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryLine {
    pub recipient: String,
    pub event_id: MessageId,
}

/// `send`/`reply` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReceipt {
    /// The first (primary) delivered event id.
    pub id: MessageId,
    pub deliveries: Vec<DeliveryLine>,
    pub count: usize,
}

/// `inbox` request.
#[derive(Debug, Clone, Default)]
pub struct InboxQuery {
    pub project: String,
    pub agent: String,
    pub unread_only: bool,
    pub limit: Option<usize>,
    pub include_bodies: bool,
}

impl From<&InboxQuery> for FetchInbox {
    fn from(q: &InboxQuery) -> Self {
        FetchInbox {
            project_slug: q.project.clone(),
            agent_name: q.agent.clone(),
            unread_only: q.unread_only,
            limit: q.limit,
            include_bodies: q.include_bodies,
        }
    }
}

/// One projected inbox/search result message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailItem {
    pub id: MessageId,
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<MessageId>,
    pub created_ts_us: i64,
}

impl From<InboxMessage> for MailItem {
    fn from(m: InboxMessage) -> Self {
        MailItem {
            id: MessageId::from(m.id),
            sender: m.sender,
            subject: m.subject,
            body_md: m.body_md,
            importance: m.importance,
            ack_required: m.ack_required,
            thread_id: m.thread_id.map(MessageId::from),
            created_ts_us: m.created_ts_us,
        }
    }
}

impl From<SearchMessage> for MailItem {
    fn from(m: SearchMessage) -> Self {
        MailItem {
            id: MessageId::from(m.id),
            sender: m.from,
            subject: m.subject,
            body_md: m.body_md.unwrap_or_default(),
            importance: m.importance,
            ack_required: m.ack_required,
            thread_id: m.thread_id.map(MessageId::from),
            created_ts_us: m.created_ts_us,
        }
    }
}

/// A read/ack receipt operation (idempotent).
#[derive(Debug, Clone)]
pub struct ReceiptOp {
    pub project: String,
    pub agent: String,
    pub message_id: MessageId,
}

/// The receipt outcome (idempotent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptOutcome {
    pub message_id: MessageId,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
}

/// `search` request.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    pub project: String,
    pub query: String,
    pub limit: Option<usize>,
    pub ranking: Option<String>,
    pub sender: Option<String>,
    pub include_body_md: bool,
}

impl From<&SearchQuery> for SearchMessages {
    fn from(q: &SearchQuery) -> Self {
        SearchMessages {
            project_key: q.project.clone(),
            query: q.query.clone(),
            limit: q.limit,
            ranking: q.ranking.clone(),
            sender_name: q.sender.clone(),
            importance: None,
            thread_id: None,
            since_us: None,
            until_us: None,
            include_body_md: q.include_body_md,
        }
    }
}

/// A contact handshake request.
#[derive(Debug, Clone)]
pub struct ContactRequest {
    pub project: String,
    pub from_agent: String,
    pub to_agent: String,
    pub reason: Option<String>,
}

impl From<&ContactRequest> for RequestContact {
    fn from(r: &ContactRequest) -> Self {
        RequestContact {
            project_slug: r.project.clone(),
            from_agent: r.from_agent.clone(),
            to_agent: r.to_agent.clone(),
            to_project: None,
            reason: r.reason.clone(),
        }
    }
}

/// A contact handshake response.
#[derive(Debug, Clone)]
pub struct ContactResponse {
    pub project: String,
    pub to_agent: String,
    pub from_agent: String,
    pub accept: bool,
}

impl From<&ContactResponse> for RespondContact {
    fn from(r: &ContactResponse) -> Self {
        RespondContact {
            project_slug: r.project.clone(),
            to_agent: r.to_agent.clone(),
            from_agent: r.from_agent.clone(),
            accept: r.accept,
            from_project: None,
        }
    }
}

/// The coordination-fabric trait (ADR-0007 D2): `&self` receivers, string
/// ids, operation-shaped. Implemented natively by toron ([`ToronMailSurface`])
/// and, at the AM edge, by an agent-mail adapter (u64 ↔ String conversion
/// happens ONLY there, at the MCP JSON boundary, never in shared types).
#[async_trait]
pub trait MailSurface: Send + Sync {
    // --- messaging ---
    /// Send a message; returns the primary delivered event id.
    async fn send(&self, req: &SendMail) -> Result<SendReceipt, MailError>;
    /// Reply inside an existing thread.
    async fn reply(&self, req: &ReplyMail) -> Result<SendReceipt, MailError>;
    /// Project the agent's inbox (rumor-time order).
    async fn inbox(&self, req: &InboxQuery) -> Result<Vec<MailItem>, MailError>;
    /// Acknowledge a message (explicit receipt, idempotent).
    async fn acknowledge(&self, req: &ReceiptOp) -> Result<ReceiptOutcome, MailError>;
    /// Mark a message read (idempotent).
    async fn mark_read(&self, req: &ReceiptOp) -> Result<ReceiptOutcome, MailError>;

    // --- search ---
    /// FTS5 search over a project's indexed messages.
    async fn search(&self, req: &SearchQuery) -> Result<Vec<MailItem>, MailError>;

    // --- contacts ---
    /// Request contact with another agent.
    async fn request_contact(&self, req: &ContactRequest) -> Result<ContactLink, MailError>;
    /// Approve or deny a pending contact request.
    async fn respond_contact(&self, req: &ContactResponse) -> Result<ContactLink, MailError>;
    /// List an agent's contact links.
    async fn list_contacts(&self, project: &str, agent: &str)
        -> Result<Vec<ContactLink>, MailError>;
    /// Set an agent's inbound contact policy.
    async fn set_contact_policy(
        &self,
        project: &str,
        agent: &str,
        policy: ContactPolicy,
    ) -> Result<(), MailError>;

    // --- agents ---
    /// List a project's registered agents (most-recently-active first).
    async fn list_agents(
        &self,
        project: &str,
        active_within_days: Option<u64>,
    ) -> Result<Vec<WhoIs>, MailError>;
    /// Resolve an agent name to its profile.
    async fn whois(&self, project: &str, agent: &str) -> Result<WhoIs, MailError>;

    // --- infra ---
    /// Health report (never fails; machine codes for unhealthy components).
    async fn health(&self) -> Result<HealthReport, MailError>;
    /// Idempotently ensure a project exists.
    async fn ensure_project(&self, human_key: &str) -> Result<ProjectState, MailError>;
}

/// Thin native implementation over the durable [`Mailbox`] (ADR-0007 R1.3).
/// Construction may still use demo/temp roots until R2 lands the durable
/// XDG data dir; the trait surface itself is backend-agnostic.
#[derive(Debug, Clone)]
pub struct ToronMailSurface {
    mailbox: Mailbox,
}

impl ToronMailSurface {
    pub fn new(mailbox: Mailbox) -> Self {
        Self { mailbox }
    }

    pub fn mailbox(&self) -> &Mailbox {
        &self.mailbox
    }
}

#[async_trait]
impl MailSurface for ToronMailSurface {
    async fn send(&self, req: &SendMail) -> Result<SendReceipt, MailError> {
        let result = self
            .mailbox
            .send_message(&SendMessage::from(req))
            .await
            .map_mail_error()?;
        let deliveries: Vec<DeliveryLine> = result
            .deliveries
            .into_iter()
            .map(|d| DeliveryLine {
                recipient: d.payload.recipient,
                event_id: MessageId::from(d.payload.event_id),
            })
            .collect();
        let id = deliveries
            .first()
            .map(|d| d.event_id.clone())
            .unwrap_or_else(|| MessageId::from(String::new()));
        Ok(SendReceipt {
            id,
            deliveries,
            count: result.count,
        })
    }

    async fn reply(&self, req: &ReplyMail) -> Result<SendReceipt, MailError> {
        let result = self
            .mailbox
            .reply_message(&SendMessage::from(req))
            .await
            .map_mail_error()?;
        let deliveries: Vec<DeliveryLine> = result
            .deliveries
            .into_iter()
            .map(|d| DeliveryLine {
                recipient: d.payload.recipient,
                event_id: MessageId::from(d.payload.event_id),
            })
            .collect();
        let id = deliveries
            .first()
            .map(|d| d.event_id.clone())
            .unwrap_or_else(|| MessageId::from(String::new()));
        Ok(SendReceipt {
            id,
            deliveries,
            count: result.count,
        })
    }

    async fn inbox(&self, req: &InboxQuery) -> Result<Vec<MailItem>, MailError> {
        Ok(self
            .mailbox
            .fetch_inbox(&FetchInbox::from(req))
            .await
            .map_mail_error()?
            .into_iter()
            .map(MailItem::from)
            .collect())
    }

    async fn acknowledge(&self, req: &ReceiptOp) -> Result<ReceiptOutcome, MailError> {
        let r = self
            .mailbox
            .acknowledge_message(&req.project, &req.agent, req.message_id.as_str())
            .await
            .map_mail_error()?;
        Ok(ReceiptOutcome {
            message_id: MessageId::from(r.message_id),
            acknowledged: r.acknowledged,
            acknowledged_at_us: r.acknowledged_at_us,
        })
    }

    async fn mark_read(&self, req: &ReceiptOp) -> Result<ReceiptOutcome, MailError> {
        let r = self
            .mailbox
            .mark_message_read(&req.project, &req.agent, req.message_id.as_str())
            .await
            .map_mail_error()?;
        Ok(ReceiptOutcome {
            message_id: MessageId::from(r.message_id),
            acknowledged: r.acknowledged,
            acknowledged_at_us: r.acknowledged_at_us,
        })
    }

    async fn search(&self, req: &SearchQuery) -> Result<Vec<MailItem>, MailError> {
        Ok(self
            .mailbox
            .search_messages(&SearchMessages::from(req))
            .await
            .map_mail_error()?
            .into_iter()
            .map(MailItem::from)
            .collect())
    }

    async fn request_contact(&self, req: &ContactRequest) -> Result<ContactLink, MailError> {
        self.mailbox
            .request_contact(&RequestContact::from(req))
            .await
            .map_mail_error()
    }

    async fn respond_contact(&self, req: &ContactResponse) -> Result<ContactLink, MailError> {
        self.mailbox
            .respond_contact(&RespondContact::from(req))
            .await
            .map_mail_error()
    }

    async fn list_contacts(
        &self,
        project: &str,
        agent: &str,
    ) -> Result<Vec<ContactLink>, MailError> {
        self.mailbox
            .list_contacts(project, agent)
            .await
            .map_mail_error()
    }

    async fn set_contact_policy(
        &self,
        project: &str,
        agent: &str,
        policy: ContactPolicy,
    ) -> Result<(), MailError> {
        self.mailbox
            .set_contact_policy(project, agent, policy)
            .await
            .map(|_| ())
            .map_mail_error()
    }

    async fn list_agents(
        &self,
        project: &str,
        active_within_days: Option<u64>,
    ) -> Result<Vec<WhoIs>, MailError> {
        self.mailbox
            .list_agents(project, ListAgentsOptions { active_within_days })
            .await
            .map_mail_error()
    }

    async fn whois(&self, project: &str, agent: &str) -> Result<WhoIs, MailError> {
        self.mailbox
            .whois(project, agent)
            .await
            .map_mail_error()
    }

    async fn health(&self) -> Result<HealthReport, MailError> {
        Ok(self.mailbox.health_check().await)
    }

    async fn ensure_project(&self, human_key: &str) -> Result<ProjectState, MailError> {
        self.mailbox
            .ensure_project(human_key)
            .await
            .map_mail_error()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::nostr::identity::AgentProfile;

    use super::*;

    const PASSPHRASE: &str = "surface-test-pass";

    async fn test_surface(tag: &str) -> (ToronMailSurface, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-surface-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir()
            .join(format!("toron-surface-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE);
        (ToronMailSurface::new(mailbox), db_path, data_dir)
    }

    async fn register_agent(surface: &ToronMailSurface, project: &str, name: &str) -> String {
        surface
            .mailbox
            .register_agent(
                project,
                &AgentProfile {
                    name: name.into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("register")
            .npub
    }

    #[tokio::test]
    async fn send_inbox_ack_reply_round_trip() {
        let (surface, db_path, data_dir) = test_surface("roundtrip").await;
        let green_npub = register_agent(&surface, "demo", "GreenCastle").await;
        let _ = register_agent(&surface, "demo", "BlueLake").await;

        let sent = surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "review".into(),
                body_md: "please review".into(),
                ack_required: true,
                ..Default::default()
            })
            .await
            .expect("send");
        assert_eq!(sent.count, 1, "one recipient");
        assert_eq!(sent.deliveries[0].recipient, "BlueLake");

        // Inbox: the recipient sees the message with a typed MessageId.
        let inbox = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("inbox");
        assert_eq!(inbox.len(), 1);
        let item = &inbox[0];
        assert_eq!(item.sender, green_npub, "sender field is the npub hex");
        assert_eq!(item.body_md, "please review");
        assert_eq!(item.ack_required, true);
        assert!(!item.id.as_str().is_empty(), "typed message id present");

        // Ack (idempotent receipt).
        let ack = surface
            .acknowledge(&ReceiptOp {
                project: "demo".into(),
                agent: "BlueLake".into(),
                message_id: item.id.clone(),
            })
            .await
            .expect("ack");
        assert!(ack.acknowledged);
        assert_eq!(ack.message_id, item.id);
        let ack_again = surface
            .acknowledge(&ReceiptOp {
                project: "demo".into(),
                agent: "BlueLake".into(),
                message_id: item.id.clone(),
            })
            .await
            .expect("ack again");
        assert!(ack_again.acknowledged, "ack is idempotent");

        // Reply threads to the original.
        let reply = surface
            .reply(&ReplyMail {
                project: "demo".into(),
                sender: "BlueLake".into(),
                to: vec!["GreenCastle".into()],
                subject: "Re: review".into(),
                body_md: "done".into(),
                ack_required: false,
                thread_id: item.id.clone(),
            })
            .await
            .expect("reply");
        assert_eq!(reply.count, 1);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn ensure_project_and_health_are_operation_shaped() {
        let (surface, db_path, data_dir) = test_surface("infra").await;
        let state = surface
            .ensure_project("some/human/key")
            .await
            .expect("ensure project");
        assert_eq!(state.slug, "some/human/key");
        let report = surface.health().await.expect("health");
        assert!(
            report.bus.ok && report.archive.ok,
            "in-process bus is healthy: {report:?}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
