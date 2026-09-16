// governed-by: ADR-0007
// governed-by: ADR-0017 (project-root canonicalization)
// R1 / D2 (ADR-0007): `MailSurface` — the coordination-fabric trait living
// next to `Mailbox`. Operation-shaped (send → inbox → ack → reply, contacts,
// agents, infra) — NOT the raw 38-tool surface. `&self` receivers, string ids
// ([`MessageId`]) end-to-end, machine-coded [`MailError`]. Flywheel can
// path-dep and re-export this later (ADR-0007 R1.5 deletes its local
// `MailBackend` in favor of this trait).
//
// Shared request/response types defined here carry NO u64 (ADR-0007 D1).

use async_trait::async_trait;

use super::Mailbox;
use super::build_slots::AcquireBuildSlotResult;
use super::contacts::{ContactLink, RequestContact, RespondContact};
use super::error::{MailError, MailSurfaceResult};
use super::identity::{ListAgentsOptions, RegisteredAgent, WhoIs};
use super::ids::MessageId;
use super::infra::{HealthReport, ProjectState};
use super::messaging::{FetchInbox, InboxMessage, SendMessage};
use super::product::ProductInfo;
use super::reservations::{AcquireResult, ConflictReport, ReleaseResult, RenewResult};
use super::search::{SearchMessage, SearchMessages, ThreadSummary};
use super::unwrap::ContactPolicy;
use crate::index::slots::BuildSlot;
use crate::nostr::identity::AgentProfile;

/// `send` request. `thread_id` opens a message INSIDE an existing thread
/// (flywheel's debate/plan-review patterns seed a thread via send); replies
/// use [`ReplyMail`].
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
    /// Seed into an existing thread (free-form AM-style id or hex event id).
    pub thread_id: Option<String>,
    /// ADR-0004 Ship 5: operator Overseer override (signs with the operator
    /// key, skips contact-policy enforcement).
    pub overseer: bool,
    /// ADR-0010 D9 (bd-l0fz): stamp the `bead=<id>` structural done-hold on
    /// the rumor. CLI-only via `--resolve-bead`; the loop reporter-of-record
    /// `[{id}] done` keeps its subject and gains this tag.
    pub bead: Option<String>,
}

impl From<&SendMail> for SendMessage {
    fn from(r: &SendMail) -> Self {
        let mut extra_tags = Vec::new();
        if let Some(bd) = &r.bead
            && let Ok(t) = crate::nostr::Tag::parse(["bead", bd])
        {
            extra_tags.push(t);
        }
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
            thread_id: r.thread_id.clone(),
            sender_token: None,
            overseer: r.overseer,
            extra_tags,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Keep only messages with importance ∈ {high, urgent} (applied by the
    /// surface after projection — the row model carries `importance`).
    pub urgent_only: bool,
    /// Keep only messages with `created_ts_us` strictly newer than this
    /// RFC3339 timestamp. `None` = no time bound. A malformed timestamp is a
    /// `MailError::Malformed` — never a silently ignored filter.
    pub since_ts: Option<String>,
    /// Keep only messages whose projected `thread_id` equals this value
    /// (bd-harvest-thread-filter-yp9r). Post-unwrap filter — the thread tag
    /// lives inside the encrypted gift wrap. `None` = no thread bound.
    pub thread_id: Option<String>,
}

impl From<&InboxQuery> for FetchInbox {
    fn from(q: &InboxQuery) -> Self {
        FetchInbox {
            project_slug: q.project.clone(),
            agent_name: q.agent.clone(),
            unread_only: q.unread_only,
            limit: q.limit,
            include_bodies: q.include_bodies,
            urgent_only: q.urgent_only,
            since_ts: q.since_ts.clone(),
            thread_id: q.thread_id.clone(),
        }
    }
}

/// One projected inbox/search result message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MailItem {
    pub id: MessageId,
    /// AM-parity: the wire row may name the sender `from` (agent-mail MCP)
    /// or `sender` (toron MCP) — both deserialize onto this field.
    #[serde(alias = "from")]
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<MessageId>,
    pub created_ts_us: i64,
    /// S5 (bd-dup-assign-collapse-ws21): the first id of the duplicate group
    /// this row survives for; `None` = standalone (see `collapse_duplicates`).
    pub duplicate_of: Option<MessageId>,
    /// ADR-0010 D9 (bd-l0fz): the structural done-hold — `bead=<id>` projected
    /// from the rumor. Wire-safe: optional on both ends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
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
            duplicate_of: m.duplicate_of.map(MessageId::from),
            bead: m.bead,
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
            duplicate_of: None,
            bead: None,
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Restrict to a message importance level (exact match; AM-parity).
    pub importance: Option<String>,
    /// Restrict to a thread id.
    pub thread_id: Option<String>,
    /// Inclusive lower bound for `created_ts_us` (RFC3339). A malformed
    /// timestamp is a `MailError::Malformed` — never silently ignored.
    pub date_start: Option<String>,
    /// Inclusive upper bound for `created_ts_us` (RFC3339).
    pub date_end: Option<String>,
}

impl From<&SearchQuery> for SearchMessages {
    fn from(q: &SearchQuery) -> Self {
        SearchMessages {
            project_key: q.project.clone(),
            query: q.query.clone(),
            limit: q.limit,
            ranking: q.ranking.clone(),
            sender_name: q.sender.clone(),
            importance: q.importance.clone(),
            thread_id: q.thread_id.clone(),
            since_us: None,
            until_us: None,
            include_body_md: q.include_body_md,
        }
    }
}

/// One cross-project product hit (search/inbox fan-out).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProductHit {
    /// The member project the hit landed in.
    pub project: String,
    /// The projected message.
    pub item: MailItem,
}

/// A contact handshake request.
#[derive(Debug, Clone)]
pub struct ContactRequest {
    pub project: String,
    pub from_agent: String,
    pub to_agent: String,
    pub reason: Option<String>,
    /// Cross-project target (AM parity: request contact in another project).
    pub to_project: Option<String>,
}

impl From<&ContactRequest> for RequestContact {
    fn from(r: &ContactRequest) -> Self {
        RequestContact {
            project_slug: r.project.clone(),
            from_agent: r.from_agent.clone(),
            to_agent: r.to_agent.clone(),
            to_project: r.to_project.clone(),
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
    /// Origin project of the request (cross-project replies).
    pub from_project: Option<String>,
}

impl From<&ContactResponse> for RespondContact {
    fn from(r: &ContactResponse) -> Self {
        RespondContact {
            project_slug: r.project.clone(),
            to_agent: r.to_agent.clone(),
            from_agent: r.from_agent.clone(),
            accept: r.accept,
            from_project: r.from_project.clone(),
        }
    }
}

/// The coordination-fabric trait (ADR-0007 D2): `&self` receivers, string
/// ids, operation-shaped. Implemented natively by toron ([`ToronMailSurface`])
/// and, at the AM edge, by an agent-mail adapter (u64 ↔ String conversion
/// happens ONLY there, at the MCP JSON boundary, never in shared types).
///
/// The full surface is the AM-intent superset (ADR-0007 D8.1): messaging,
/// search, contacts, agents, reservations, build slots, products, pane
/// identity helpers — every op flywheel's coordination loop needs.
#[async_trait]
pub trait MailSurface: Send + Sync {
    // --- meta ---
    /// Stable backend identifier ("toron" | "agentmail" | …).
    fn name(&self) -> &'static str;
    /// The agent identity this surface sends AS / reads the inbox for.
    /// REQUIRED (no default): a `""` default would let a backend silently act
    /// as an empty identity.
    fn agent_name(&self) -> &str;

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
    async fn list_contacts(
        &self,
        project: &str,
        agent: &str,
    ) -> Result<Vec<ContactLink>, MailError>;
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

    // --- agents ---
    /// Register (upsert) an agent identity; mint a fresh keypair when absent.
    async fn register_agent(
        &self,
        project: &str,
        profile: &AgentProfile,
    ) -> Result<RegisteredAgent, MailError>;

    // --- reservations (ADR-0007 D8.1) ---
    /// Acquire advisory file-reservation leases on `paths` (kind-30031).
    async fn reserve(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
        ttl_secs: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
    ) -> Result<AcquireResult, MailError>;
    /// Symmetric-glob conflict report for `paths` vs other agents' leases.
    async fn check_conflicts(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
    ) -> Result<ConflictReport, MailError>;
    /// Extend the holder's active leases' TTLs. `paths` empty = all own
    /// leases. (The AM-side `file_reservation_ids` selector is NOT part of
    /// the shared surface — it was silently ignored on the toron path, an
    /// F11 honesty gap; paths-only is the truthful superset.)
    async fn renew_reservations(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
        extend_secs: u64,
    ) -> Result<RenewResult, MailError>;
    /// Release the holder's own leases (`paths` empty = all).
    async fn release_reservations(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
    ) -> Result<ReleaseResult, MailError>;
    /// Operator force-release of a stale lease (staleness heuristic +
    /// signed revocation; NIP-09 is never the mechanism).
    async fn force_release(
        &self,
        project: &str,
        requestor: &str,
        path: &str,
        holder: Option<&str>,
        note: Option<&str>,
        notify_previous: bool,
    ) -> Result<(), MailError>;
    /// Operator-signed revocation of every live lease whose reason is the
    /// bead id (bd-h3oj release-on-bead-close). No staleness heuristic — the
    /// bead closing is the evidence; `released == 0` is a clean no-op.
    async fn release_by_reason(
        &self,
        project: &str,
        requestor: &str,
        bead_id: &str,
        notify_previous: bool,
    ) -> Result<ReleaseResult, MailError>;

    // --- build slots ---
    /// Acquire an advisory build-slot lease (kind-30032).
    async fn slot_acquire(
        &self,
        project: &str,
        agent: &str,
        slot: &str,
        ttl_secs: Option<u64>,
        exclusive: bool,
    ) -> Result<AcquireBuildSlotResult, MailError>;
    /// Extend an active lease's TTL (no-op `None` when no active lease).
    async fn slot_renew(
        &self,
        project: &str,
        agent: &str,
        slot: &str,
        extend_secs: u64,
    ) -> Result<Option<BuildSlot>, MailError>;
    /// Mark an active lease released (non-destructive). `Ok(false)` when the
    /// caller held no active lease.
    async fn slot_release(&self, project: &str, agent: &str, slot: &str)
    -> Result<bool, MailError>;

    // --- products (G6) ---
    /// Idempotent product manifest (kind-30011).
    async fn ensure_product(&self, product_key: &str, name: &str)
    -> Result<ProductInfo, MailError>;
    /// Link a project into a product (idempotent).
    async fn products_link(
        &self,
        product_key: &str,
        project: &str,
    ) -> Result<ProductInfo, MailError>;
    /// Fan the search across a product's member projects (newest first).
    async fn search_product(
        &self,
        product_key: &str,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProductHit>, MailError>;
    /// Fan the inbox across a product's member projects.
    async fn inbox_product(
        &self,
        product_key: &str,
        agent: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProductHit>, MailError>;
    /// Summarize a thread across member projects (first project containing
    /// the thread wins the digest).
    async fn summarize_thread_product(
        &self,
        product_key: &str,
        thread_id: &str,
        include_examples: bool,
    ) -> Result<ThreadSummary, MailError>;

    // --- threads ---
    /// Deterministic digest of one thread.
    async fn summarize_thread(
        &self,
        project: &str,
        thread_id: &str,
        include_examples: bool,
    ) -> Result<ThreadSummary, MailError>;

    // --- pane identity helpers (D8.1) ---
    /// Resolve a herdr pane_id to its agent name (G9). `Ok(None)` = no
    /// mapping.
    async fn resolve_pane_identity(
        &self,
        project: &str,
        pane_id: &str,
    ) -> Result<Option<String>, MailError>;
    /// Prune identities whose panes are absent from herdr's live pane list;
    /// returns the pruned agent names.
    async fn cleanup_pane_identities(&self, project: &str) -> Result<Vec<String>, MailError>;
    /// Write the per-pane identity file (agent-mail server layout). toron
    /// holds no per-pane files (live herdr mapping) → `Unsupported` there.
    async fn write_pane_identity(
        &self,
        project: &str,
        pane_id: &str,
        agent: &str,
    ) -> Result<(), MailError>;
    /// Remove the per-pane identity file. toron → `Unsupported` (see above).
    async fn clear_pane_identity(&self, project: &str, pane_id: &str) -> Result<(), MailError>;
}

/// Thin native implementation over the durable [`Mailbox`] (ADR-0007 R1.3).
/// Construction may still use demo/temp roots until R2 lands the durable
/// XDG data dir; the trait surface itself is backend-agnostic.
#[derive(Debug, Clone)]
pub struct ToronMailSurface {
    mailbox: Mailbox,
    /// The agent identity this surface acts as (ADR-0007 se2.3: flywheel
    /// builds the surface around the durable shared mailbox and rebinds this
    /// to the loop's own identity; `""` only when the embedding host never
    /// sets it — send/inbox then take explicit agent names anyway).
    agent_name: String,
}

impl ToronMailSurface {
    pub fn new(mailbox: Mailbox) -> Self {
        Self {
            mailbox,
            agent_name: String::new(),
        }
    }

    /// Rebind the acting identity (construction-time only — `&self` ops never
    /// mutate it; c3b.2 style).
    pub fn with_agent_name(mut self, agent_name: impl Into<String>) -> Self {
        self.agent_name = agent_name.into();
        self
    }

    pub fn agent_name_str(&self) -> &str {
        &self.agent_name
    }

    pub fn mailbox(&self) -> &Mailbox {
        &self.mailbox
    }
}

#[async_trait]
impl MailSurface for ToronMailSurface {
    fn name(&self) -> &'static str {
        "toron"
    }

    fn agent_name(&self) -> &str {
        &self.agent_name
    }

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
        // F11 / c3b.3 (bd-mcp-inbox-filter-qyyq): urgent/since filtering now
        // lives INSIDE `Mailbox::fetch_inbox` (the shared core inbox query) —
        // this surface path and the MCP tool path inherit the same filter.
        let fetched = self
            .mailbox
            .fetch_inbox(&FetchInbox::from(req))
            .await
            .map_mail_error()?;
        Ok(fetched.into_iter().map(MailItem::from).collect())
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
        let mut inner = SearchMessages::from(req);
        inner.since_us = req
            .date_start
            .as_deref()
            .map(super::messaging::parse_rfc3339_us)
            .transpose()?;
        inner.until_us = req
            .date_end
            .as_deref()
            .map(super::messaging::parse_rfc3339_us)
            .transpose()?;
        Ok(self
            .mailbox
            .search_messages(&inner)
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
        self.mailbox.whois(project, agent).await.map_mail_error()
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

    async fn register_agent(
        &self,
        project: &str,
        profile: &AgentProfile,
    ) -> Result<RegisteredAgent, MailError> {
        self.mailbox
            .register_agent(project, profile)
            .await
            .map_mail_error()
    }

    async fn reserve(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
        ttl_secs: Option<u64>,
        exclusive: bool,
        reason: Option<&str>,
    ) -> Result<AcquireResult, MailError> {
        self.mailbox
            .file_reservation_paths(project, agent, paths, ttl_secs, exclusive, reason)
            .await
            .map_mail_error()
    }

    async fn check_conflicts(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
    ) -> Result<ConflictReport, MailError> {
        self.mailbox
            .check_file_reservation_conflicts(project, agent, paths)
            .await
            .map_mail_error()
    }

    async fn renew_reservations(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
        extend_secs: u64,
    ) -> Result<RenewResult, MailError> {
        self.mailbox
            .renew_file_reservations(project, agent, paths, extend_secs)
            .await
            .map_mail_error()
    }

    async fn release_reservations(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
    ) -> Result<ReleaseResult, MailError> {
        self.mailbox
            .release_file_reservations(project, agent, paths)
            .await
            .map_mail_error()
    }

    async fn force_release(
        &self,
        project: &str,
        requestor: &str,
        path: &str,
        holder: Option<&str>,
        note: Option<&str>,
        notify_previous: bool,
    ) -> Result<(), MailError> {
        self.mailbox
            .force_release_file_reservation(project, requestor, path, holder, note, notify_previous)
            .await
            .map_mail_error()
    }

    async fn release_by_reason(
        &self,
        project: &str,
        requestor: &str,
        bead_id: &str,
        notify_previous: bool,
    ) -> Result<ReleaseResult, MailError> {
        self.mailbox
            .release_by_reason(project, requestor, bead_id, notify_previous)
            .await
            .map_mail_error()
    }

    async fn slot_acquire(
        &self,
        project: &str,
        agent: &str,
        slot: &str,
        ttl_secs: Option<u64>,
        exclusive: bool,
    ) -> Result<AcquireBuildSlotResult, MailError> {
        self.mailbox
            .acquire_build_slot(project, agent, slot, ttl_secs, exclusive)
            .await
            .map_mail_error()
    }

    async fn slot_renew(
        &self,
        project: &str,
        agent: &str,
        slot: &str,
        extend_secs: u64,
    ) -> Result<Option<BuildSlot>, MailError> {
        self.mailbox
            .renew_build_slot(project, agent, slot, extend_secs)
            .await
            .map_mail_error()
    }

    async fn slot_release(
        &self,
        project: &str,
        agent: &str,
        slot: &str,
    ) -> Result<bool, MailError> {
        self.mailbox
            .release_build_slot(project, agent, slot)
            .await
            .map_mail_error()
    }

    async fn ensure_product(
        &self,
        product_key: &str,
        name: &str,
    ) -> Result<ProductInfo, MailError> {
        self.mailbox
            .ensure_product(product_key, name)
            .await
            .map_mail_error()
    }

    async fn products_link(
        &self,
        product_key: &str,
        project: &str,
    ) -> Result<ProductInfo, MailError> {
        self.mailbox
            .products_link(product_key, project)
            .await
            .map_mail_error()
    }

    async fn search_product(
        &self,
        product_key: &str,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProductHit>, MailError> {
        Ok(self
            .mailbox
            .search_messages_product(product_key, query, limit, true)
            .await
            .map_mail_error()?
            .into_iter()
            .map(|(project, m)| ProductHit {
                project,
                item: MailItem::from(m),
            })
            .collect())
    }

    async fn inbox_product(
        &self,
        product_key: &str,
        agent: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProductHit>, MailError> {
        Ok(self
            .mailbox
            .fetch_inbox_product(product_key, agent)
            .await
            .map_mail_error()?
            .into_iter()
            .take(limit.unwrap_or(usize::MAX))
            .map(|(project, m)| ProductHit {
                project,
                item: MailItem::from(m),
            })
            .collect())
    }

    async fn summarize_thread_product(
        &self,
        product_key: &str,
        thread_id: &str,
        include_examples: bool,
    ) -> Result<ThreadSummary, MailError> {
        self.mailbox
            .summarize_thread_product(product_key, thread_id, include_examples)
            .await
            .map_mail_error()
    }

    async fn summarize_thread(
        &self,
        project: &str,
        thread_id: &str,
        include_examples: bool,
    ) -> Result<ThreadSummary, MailError> {
        self.mailbox
            .summarize_thread(project, thread_id, include_examples, false, None)
            .await
            .map_mail_error()
    }

    async fn resolve_pane_identity(
        &self,
        _project: &str,
        pane_id: &str,
    ) -> Result<Option<String>, MailError> {
        let backend = crate::backend::herdr::HerdrBackend::connect(None)
            .map_err(|e| anyhow::anyhow!("herdr backend unavailable: {e}"))
            .map_mail_error()?;
        self.mailbox
            .resolve_pane_identity(&backend, pane_id)
            .map_mail_error()
    }

    async fn cleanup_pane_identities(&self, _project: &str) -> Result<Vec<String>, MailError> {
        let backend = crate::backend::herdr::HerdrBackend::connect(None)
            .map_err(|e| anyhow::anyhow!("herdr backend unavailable: {e}"))
            .map_mail_error()?;
        self.mailbox
            .cleanup_pane_identities(&backend)
            .map_mail_error()
    }

    async fn write_pane_identity(
        &self,
        _project: &str,
        _pane_id: &str,
        _agent: &str,
    ) -> Result<(), MailError> {
        Err(MailError::unsupported(
            "write_pane_identity: toron holds no per-pane identity files (live herdr mapping; G9)",
        ))
    }

    async fn clear_pane_identity(&self, _project: &str, _pane_id: &str) -> Result<(), MailError> {
        Err(MailError::unsupported(
            "clear_pane_identity: toron holds no per-pane identity files (live herdr mapping; G9)",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::mailbox::error::MailErrorCode;
    use crate::nostr::identity::AgentProfile;

    use super::*;

    const PASSPHRASE: &str = "surface-test-pass";

    async fn test_surface(tag: &str) -> (ToronMailSurface, std::path::PathBuf, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-surface-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-surface-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        // The unwrap loop feeds the projection (index sink) — without it the
        // index-backed ops (search/summarize/slots/product fan) see nothing.
        mailbox.spawn_unwrap_loop();
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

    /// The unwrap loop sinks into the index asynchronously — poll `probe`
    /// until `check` passes (bounded ~2.5s; never hangs the suite).
    async fn wait_indexed<T, F, Fut>(mut probe: F, check: impl Fn(&T) -> bool) -> T
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        for _ in 0..100 {
            let value = probe().await;
            if check(&value) {
                return value;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        probe().await
    }

    #[tokio::test]
    async fn send_inbox_ack_reply_round_trip() {
        let (surface, db_path, data_dir) = test_surface("roundtrip").await;
        let _green_npub = register_agent(&surface, "demo", "GreenCastle").await;
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
        // G2 / D8.5: the projection resolves the sender NAME, not the npub —
        // agents never need to know npubs.
        assert_eq!(item.sender, "GreenCastle");
        assert_eq!(item.body_md, "please review");
        assert!(item.ack_required, "ack_required flag projected");
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

    /// S3 conformance (bd-ts-unit-audit-pmn5): `since_ts` is an RFC3339
    /// string compared against `created_ts_us` (i64-µs, ADR G7). A future
    /// bound yields an empty inbox; a past bound yields the message. A
    /// malformed bound is a hard `MailError::Malformed` — never silent.
    #[tokio::test]
    async fn inbox_since_ts_rfc3339_conformance() {
        let (surface, db_path, data_dir) = test_surface("since").await;
        let _green_npub = register_agent(&surface, "demo", "GreenCastle").await;
        let _ = register_agent(&surface, "demo", "BlueLake").await;

        let now = chrono::Utc::now();
        let sent = surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "since-conformance".into(),
                body_md: "bound me".into(),
                ..Default::default()
            })
            .await
            .expect("send");
        assert_eq!(sent.count, 1);

        let query = |since: Option<String>| InboxQuery {
            project: "demo".into(),
            agent: "BlueLake".into(),
            include_bodies: true,
            since_ts: since,
            ..Default::default()
        };

        // Future bound (now + 1h): the just-sent message is older → empty.
        let future = (now + chrono::Duration::hours(1)).to_rfc3339();
        let future_inbox = surface
            .inbox(&query(Some(future)))
            .await
            .expect("future inbox");
        assert_eq!(
            future_inbox.len(),
            0,
            "future since_ts must exclude the message"
        );

        // Past bound (now − 1h): the message is newer → present.
        let past = (now - chrono::Duration::hours(1)).to_rfc3339();
        let past_inbox = surface.inbox(&query(Some(past))).await.expect("past inbox");
        assert_eq!(
            past_inbox.len(),
            1,
            "past since_ts must include the message"
        );
        assert_eq!(past_inbox[0].subject, "since-conformance");

        // Malformed bound → Malformed error, never a silent ignore.
        let err = surface.inbox(&query(Some("not-a-timestamp".into()))).await;
        assert!(matches!(
            err,
            Err(MailError {
                code: MailErrorCode::Malformed,
                ..
            })
        ));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn ensure_project_and_health_are_operation_shaped() {
        let (surface, db_path, data_dir) = test_surface("infra").await;
        // ADR-0017: a path-form key resolves to its canonical short slug.
        let state = surface
            .ensure_project("some/human/key")
            .await
            .expect("ensure project");
        assert_eq!(state.slug, "key", "canonical short slug");
        let report = surface.health().await.expect("health");
        assert!(
            report.bus.ok && report.archive.ok,
            "in-process bus is healthy: {report:?}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn meta_name_and_agent_name_are_truthful() {
        let (surface, db_path, data_dir) = test_surface("meta").await;
        assert_eq!(surface.name(), "toron");
        assert_eq!(surface.agent_name(), "", "unset until with_agent_name");
        let rebound = surface.with_agent_name("GreenCastle");
        assert_eq!(rebound.agent_name(), "GreenCastle");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn register_agent_via_surface_returns_typed_registered_agent() {
        let (surface, db_path, data_dir) = test_surface("reg").await;
        let agent = surface
            .register_agent(
                "demo",
                &AgentProfile {
                    name: "GreenCastle".into(),
                    program: Some("opencode".into()),
                    model: None,
                    persona: None,
                    task_description: Some("surface reg".into()),
                },
            )
            .await
            .expect("register");
        assert_eq!(agent.name, "GreenCastle");
        assert!(!agent.npub.is_empty());
        assert!(!agent.key_path.is_empty(), "ncryptsec persisted");
        // Idempotent upsert: same npub, `created` flips to false.
        let again = surface
            .register_agent(
                "demo",
                &AgentProfile {
                    name: "GreenCastle".into(),
                    program: None,
                    model: None,
                    persona: None,
                    task_description: None,
                },
            )
            .await
            .expect("re-register");
        assert_eq!(again.npub, agent.npub);
        assert!(!again.created);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn reservations_acquire_conflict_renew_release_round_trip() {
        let (surface, db_path, data_dir) = test_surface("resv").await;
        for n in ["GreenCastle", "BlueLake"] {
            register_agent(&surface, "demo", n).await;
        }
        let ok = surface
            .reserve(
                "demo",
                "GreenCastle",
                &["src/**".to_string(), "docs/*.md".to_string()],
                Some(3600),
                true,
                Some("se2.3 test"),
            )
            .await
            .expect("reserve");
        assert_eq!(ok.granted.len(), 2, "both paths granted");
        assert!(ok.conflicts.is_empty());

        // Conflicts: BlueLake collides on the src/** glob.
        let report = surface
            .check_conflicts("demo", "BlueLake", &["src/lib.rs".to_string()])
            .await
            .expect("conflicts");
        assert!(!report.conflict_free);
        assert_eq!(report.conflicts[0].holders, vec!["GreenCastle".to_string()]);

        // Renew: only the docs lease.
        let renewed = surface
            .renew_reservations("demo", "GreenCastle", &["docs/*.md".to_string()], 60)
            .await
            .expect("renew");
        // bd-evqs renewal cap + bd-mbh0 contract: a fresh lease (remaining
        // TTL ≈ 3600s) is NOT republished for a 60s extension — zero events,
        // no churn (the 1218-event storm). But it IS counted as renewed:
        // its expiry already guarantees more runway than requested, so
        // `renewed` equals the live lease set instead of reading as failure.
        assert_eq!(
            renewed.renewed, 1,
            "healthy lease counts as renewed without a republish (bd-mbh0)"
        );
        let still_active = surface
            .check_conflicts("demo", "BlueLake", &["src/lib.rs".to_string()])
            .await
            .expect("conflicts after capped renew");
        assert!(
            !still_active.conflict_free,
            "capped renewal leaves the lease active (not released)"
        );

        // Release all.
        let released = surface
            .release_reservations("demo", "GreenCastle", &[])
            .await
            .expect("release");
        assert_eq!(released.released, 2);
        let report = surface
            .check_conflicts("demo", "BlueLake", &["src/lib.rs".to_string()])
            .await
            .expect("conflicts after release");
        assert!(report.conflict_free, "released leases no longer conflict");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-evqs: the renewal cap renews a lease ONLY when it is near expiry
    /// (remaining TTL < the extension) — the durable-loop heartbeat then
    /// re-publishes at most ~1 event per extend window per lease instead of
    /// every tick.
    #[tokio::test]
    async fn renew_near_expiry_lease_is_extended() {
        let (surface, db_path, data_dir) = test_surface("resv-near").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        let ok = surface
            .reserve(
                "demo",
                "GreenCastle",
                &["src/**".to_string()],
                Some(60), // near expiry relative to the 300s extension
                true,
                Some("near-expiry"),
            )
            .await
            .expect("reserve");
        assert_eq!(ok.granted.len(), 1);

        let renewed = surface
            .renew_reservations("demo", "GreenCastle", &[], 300)
            .await
            .expect("renew");
        assert_eq!(renewed.renewed, 1, "near-expiry lease IS renewed");
        // The extension ratchets from the OLD absolute expiry + extend.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let new_exp = renewed.reservations[0]
            .expires_ts_us
            .map(|t| t / 1_000_000)
            .unwrap_or(0);
        assert!(
            new_exp > now + 250,
            "expiry extended past now+250s (got {new_exp} vs now {now})"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn force_release_is_denied_while_holder_active() {
        let (surface, db_path, data_dir) = test_surface("frc").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        let _ok = surface
            .reserve(
                "demo",
                "GreenCastle",
                &["src/**".to_string()],
                Some(3600),
                true,
                None,
            )
            .await
            .expect("reserve");
        let err = surface
            .force_release("demo", "BlueLake", "src/**", None, Some("nope"), false)
            .await
            .expect_err("active holder → denied");
        assert!(
            err.to_string().contains("not stale"),
            "names the staleness denial: {err}"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn build_slots_acquire_renew_release_round_trip() {
        let (surface, db_path, data_dir) = test_surface("slot").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        let ok = surface
            .slot_acquire("demo", "GreenCastle", "build-1", Some(600), true)
            .await
            .expect("acquire");
        assert_eq!(ok.granted.len(), 1);
        assert_eq!(ok.granted[0].slot, "build-1");

        let renewed = surface
            .slot_renew("demo", "GreenCastle", "build-1", 120)
            .await
            .expect("renew");
        assert!(renewed.is_some(), "active lease renewed");

        let released = surface
            .slot_release("demo", "GreenCastle", "build-1")
            .await
            .expect("release");
        assert!(released);
        let none = surface
            .slot_release("demo", "GreenCastle", "build-1")
            .await
            .expect("release again");
        assert!(!none, "no active lease → false");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn products_link_search_and_inbox_fan_out_across_members() {
        let (surface, db_path, data_dir) = test_surface("prod").await;
        let green_npub = register_agent(&surface, "alpha", "GreenCastle").await;
        register_agent(&surface, "beta", "GreenCastle").await;

        let info = surface
            .ensure_product("acme", "Acme")
            .await
            .expect("ensure product");
        assert_eq!(info.product_key, "acme");
        let linked = surface
            .products_link("acme", "alpha")
            .await
            .expect("link alpha");
        let _ = surface
            .products_link("acme", "beta")
            .await
            .expect("link beta");
        assert_eq!(linked.member_projects.len(), 1);

        surface
            .send(&SendMail {
                project: "alpha".into(),
                sender: "GreenCastle".into(),
                to: vec!["GreenCastle".into()],
                subject: "acme topic".into(),
                body_md: "product fan body".into(),
                ..Default::default()
            })
            .await
            .expect("send alpha");
        surface
            .send(&SendMail {
                project: "beta".into(),
                sender: "GreenCastle".into(),
                to: vec!["GreenCastle".into()],
                subject: "beta topic".into(),
                body_md: "beta fan body".into(),
                ..Default::default()
            })
            .await
            .expect("send beta");

        let hits = wait_indexed(
            || surface.search_product("acme", "fan", None),
            |r| matches!(r, Ok(v) if v.len() == 2),
        )
        .await
        .expect("product search");
        assert_eq!(hits.len(), 2, "one hit per member project");
        assert!(hits.iter().any(|h| h.project == "alpha"));
        assert!(hits.iter().any(|h| h.project == "beta"));

        let inbox = surface
            .inbox_product("acme", "GreenCastle", None)
            .await
            .expect("product inbox");
        assert_eq!(inbox.len(), 2);
        let _ = green_npub;
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn summarize_thread_returns_digest() {
        let (surface, db_path, data_dir) = test_surface("summ").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        register_agent(&surface, "demo", "BlueLake").await;
        let sent = surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "digest me".into(),
                body_md: "body one".into(),
                ..Default::default()
            })
            .await
            .expect("send");
        let id = sent.id;
        let summary = wait_indexed(
            || surface.summarize_thread("demo", id.as_str(), true),
            |s| matches!(s, Ok(v) if v.message_count > 0),
        )
        .await
        .expect("summarize");
        assert_eq!(summary.message_count, 1);
        assert_eq!(summary.thread_id, id.as_str());
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn inbox_applies_urgent_and_since_filters() {
        let (surface, db_path, data_dir) = test_surface("filt").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        register_agent(&surface, "demo", "BlueLake").await;
        surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "low".into(),
                body_md: "low prio".into(),
                importance: Some("low".into()),
                ..Default::default()
            })
            .await
            .expect("send low");
        // Rumor timestamps are second-granularity (nostr Timestamp), so a
        // `since` bound can only separate sends in different seconds. Sleep
        // across a boundary so the urgent send is strictly newer.
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "urgent".into(),
                body_md: "urgent prio".into(),
                importance: Some("urgent".into()),
                ..Default::default()
            })
            .await
            .expect("send urgent");

        let all = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("inbox");
        assert_eq!(all.len(), 2);

        let urgent = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                urgent_only: true,
                ..Default::default()
            })
            .await
            .expect("urgent inbox");
        assert_eq!(urgent.len(), 1);
        assert_eq!(urgent[0].subject, "urgent");

        // Anchor `since` on the low row's own timestamp (second-granularity
        // rumor time — the sleep above guarantees the urgent send is a later
        // second).
        let low_ts = all
            .iter()
            .find(|m| m.subject == "low")
            .expect("low row")
            .created_ts_us;
        let since = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                since_ts: Some(
                    chrono::DateTime::from_timestamp_micros(low_ts)
                        .expect("low ts")
                        .to_rfc3339(),
                ),
                ..Default::default()
            })
            .await
            .expect("since inbox");
        assert_eq!(
            since.len(),
            1,
            "only the urgent send is newer than the low row"
        );
        assert_eq!(since[0].subject, "urgent");

        let bad = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                since_ts: Some("not-a-timestamp".into()),
                ..Default::default()
            })
            .await
            .expect_err("malformed since_ts is an error, never silent");
        assert_eq!(bad.code(), &MailErrorCode::Malformed);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-harvest-thread-filter-yp9r: `InboxQuery.thread_id` keeps ONLY the
    /// target thread's rows; `None` stays byte-identical to unfiltered.
    #[tokio::test]
    async fn inbox_applies_thread_filter() {
        let (surface, db_path, data_dir) = test_surface("thrd").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        register_agent(&surface, "demo", "BlueLake").await;
        for (subject, thread) in [
            ("t1-a", "debate-1"),
            ("t1-b", "debate-1"),
            ("t2-a", "debate-2"),
        ] {
            surface
                .send(&SendMail {
                    project: "demo".into(),
                    sender: "GreenCastle".into(),
                    to: vec!["BlueLake".into()],
                    subject: subject.into(),
                    body_md: "threaded".into(),
                    thread_id: Some(thread.into()),
                    ..Default::default()
                })
                .await
                .expect("send");
        }

        let all = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("inbox");
        assert_eq!(all.len(), 3);

        let t1 = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                thread_id: Some("debate-1".into()),
                ..Default::default()
            })
            .await
            .expect("thread inbox");
        assert_eq!(t1.len(), 2, "only debate-1 rows");
        assert!(
            t1.iter()
                .all(|m| m.thread_id.as_ref().map(|t| t.as_str()) == Some("debate-1")),
            "every row carries the requested thread_id"
        );

        let t2 = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                thread_id: Some("debate-2".into()),
                ..Default::default()
            })
            .await
            .expect("thread inbox 2");
        assert_eq!(t2.len(), 1);
        assert_eq!(t2[0].subject, "t2-a");

        let missing = surface
            .inbox(&InboxQuery {
                project: "demo".into(),
                agent: "BlueLake".into(),
                include_bodies: true,
                thread_id: Some("no-such-thread".into()),
                ..Default::default()
            })
            .await
            .expect("empty thread inbox");
        assert!(missing.is_empty(), "unknown thread yields no rows");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn search_applies_importance_thread_and_date_window() {
        let (surface, db_path, data_dir) = test_surface("sfil").await;
        register_agent(&surface, "demo", "GreenCastle").await;
        register_agent(&surface, "demo", "BlueLake").await;
        let sent = surface
            .send(&SendMail {
                project: "demo".into(),
                sender: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "windowed".into(),
                body_md: "in window".into(),
                importance: Some("high".into()),
                ..Default::default()
            })
            .await
            .expect("send");
        let thread = sent.deliveries[0].event_id.clone();

        let importance = wait_indexed(
            || async {
                surface
                    .search(&SearchQuery {
                        project: "demo".into(),
                        query: "windowed".into(),
                        importance: Some("high".into()),
                        ..Default::default()
                    })
                    .await
            },
            |r| matches!(r, Ok(v) if !v.is_empty()),
        )
        .await
        .expect("search by importance");
        assert_eq!(importance.len(), 1);

        let thread_only = surface
            .search(&SearchQuery {
                project: "demo".into(),
                query: "windowed".into(),
                thread_id: Some(thread.as_str().into()),
                ..Default::default()
            })
            .await
            .expect("search by thread");
        assert_eq!(thread_only.len(), 1);

        let start = chrono::Utc::now().to_rfc3339();
        let empty = surface
            .search(&SearchQuery {
                project: "demo".into(),
                query: "windowed".into(),
                date_start: Some(start),
                ..Default::default()
            })
            .await
            .expect("search by date window");
        assert_eq!(empty.len(), 0, "strictly newer than the message's ts");
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn pane_identity_ops_are_unsupported_on_toron() {
        let (surface, db_path, data_dir) = test_surface("pane").await;
        let err = surface
            .write_pane_identity("demo", "%0", "GreenCastle")
            .await
            .expect_err("toron has no per-pane files");
        assert_eq!(err.code(), &MailErrorCode::Unsupported);
        let err = surface
            .clear_pane_identity("demo", "%0")
            .await
            .expect_err("toron has no per-pane files");
        assert_eq!(err.code(), &MailErrorCode::Unsupported);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
