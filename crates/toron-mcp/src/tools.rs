// governed-by: ADR-0003

//! AM-Rust parity tool surface (bd-led.7.2): the 38 mailbox tools as rmcp
//! `#[tool]` methods on [`ToolServer`].
//!
//! Parity-by-construction: every WireArgs field uses the exact AM-Rust arg
//! name and every `#[tool(name = ...)]` is the exact AM-Rust tool name.
//!
//! Wire: [`ToolServer`] wraps `Arc<toron_core::mailbox::Mailbox>` and exposes
//! the router via `#[tool_router]` + a hand-written `#[tool_handler]` impl so
//! `get_info` can mirror `ToronServer` (name "toron", 2026-07-28).

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::IntoCallToolResult;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResponse, CallToolResult, ContentBlock, Implementation, InitializeResult,
    ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::{schemars, tool, tool_handler, tool_router, ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use toron_core::backend::herdr::HerdrBackend;
use toron_core::index::slots::{BuildSlot, GrantedSlot, SlotConflict, SlotStatus};
use toron_core::mailbox::{
    AcquireBuildSlotResult, AcquireResult, ConflictReport, ContactHandshakeResult, ContactLink,
    ContactPolicy, Delivery, DeliveryPayload, FetchInbox, GrantedReservation, HealthComponent,
    HealthReport, InboxMessage, ListAgentsOptions, Mailbox, PathConflict, PrepareThreadResult,
    ProductInfo, ProjectState, ReceiptResult, RegisteredAgent, ReleaseResult, RenewResult,
    RequestContact, ReservationCycleResult, RespondContact, SearchMessage, SearchMessages,
    SendMessage, SendResult, StartSessionResult, ThreadSummary, WhoIs,
};
use toron_core::nostr::identity::AgentProfile;

/// Tool return value: serialized JSON on `Ok`, a tool error on `Err`.
///
/// rmcp's `IntoCallToolResult` has no blanket impl for `anyhow::Result` /
/// `serde_json::Value` (only `String`/`ContentBlock`/`Json<T>`/`ErrorData`),
/// so the 38 tool bodies return this local wrapper and keep the AM-Rust JSON
/// contract on the wire.
pub struct ToolOutput(pub anyhow::Result<serde_json::Value>);

impl IntoCallToolResult for ToolOutput {
    fn into_call_tool_result(self) -> Result<CallToolResponse, rmcp::ErrorData> {
        match self.0 {
            Ok(value) => {
                let text = serde_json::to_string(&value)
                    .unwrap_or_else(|e| format!("serialize tool result: {e}"));
                Ok(CallToolResult::success(vec![ContentBlock::text(text)]).into())
            }
            Err(e) => Ok(CallToolResult::error(vec![ContentBlock::text(format!("{e:#}"))]).into()),
        }
    }
}

impl ToolOutput {
    fn ok(value: Value) -> Self {
        Self(Ok(value))
    }

    fn err(e: impl std::fmt::Display) -> Self {
        Self(Err(anyhow::anyhow!("{e}")))
    }

    fn result<T: ToJson>(r: anyhow::Result<T>) -> Self {
        Self(r.map(|value| value.to_json()))
    }
}

/// Serialize a toron-core result into its AM-Rust parity JSON shape.
///
/// The toron-core result structs do not derive `Serialize`, so the JSON shape
/// is hand-mirrored field-by-field (parity-by-construction).
pub trait ToJson {
    fn to_json(&self) -> Value;
}

impl ToJson for bool {
    fn to_json(&self) -> Value {
        json!(self)
    }
}

impl ToJson for String {
    fn to_json(&self) -> Value {
        json!(self)
    }
}

impl ToJson for () {
    fn to_json(&self) -> Value {
        json!({ "ok": true })
    }
}

impl<T: ToJson> ToJson for Vec<T> {
    fn to_json(&self) -> Value {
        Value::Array(self.iter().map(ToJson::to_json).collect())
    }
}

impl<T: ToJson> ToJson for Option<T> {
    fn to_json(&self) -> Value {
        self.as_ref().map(ToJson::to_json).unwrap_or(Value::Null)
    }
}

impl ToJson for HealthComponent {
    fn to_json(&self) -> Value {
        json!({ "ok": self.ok, "message": self.message })
    }
}

impl ToJson for HealthReport {
    fn to_json(&self) -> Value {
        json!({ "bus": self.bus.to_json(), "archive": self.archive.to_json() })
    }
}

impl ToJson for ProjectState {
    fn to_json(&self) -> Value {
        json!({
            "slug": self.slug,
            "manifest_event_id": self.manifest_event_id,
            "created": self.created,
        })
    }
}

impl ToJson for RegisteredAgent {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "project_slug": self.project_slug,
            "npub": self.npub,
            "profile_event_id": self.profile_event_id,
            "key_path": self.key_path,
            "created": self.created,
        })
    }
}

impl ToJson for WhoIs {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "npub": self.npub,
            "program": self.program,
            "model": self.model,
            "task_description": self.task_description,
            "last_active_ts_us": self.last_active_ts_us,
        })
    }
}

impl ToJson for ContactLink {
    fn to_json(&self) -> Value {
        json!({
            "project_slug": self.project_slug,
            "from_name": self.from_name,
            "from_pubkey": self.from_pubkey,
            "to_name": self.to_name,
            "to_pubkey": self.to_pubkey,
            "to_project": self.to_project,
            "status": self.status.as_str(),
            "created_ts_us": self.created_ts_us,
        })
    }
}

impl ToJson for DeliveryPayload {
    fn to_json(&self) -> Value {
        json!({ "recipient": self.recipient, "event_id": self.event_id })
    }
}

impl ToJson for Delivery {
    fn to_json(&self) -> Value {
        json!({ "project": self.project, "payload": self.payload.to_json() })
    }
}

impl ToJson for SendResult {
    fn to_json(&self) -> Value {
        json!({
            "deliveries": self.deliveries.to_json(),
            "count": self.count,
            "verified_sender": self.verified_sender,
        })
    }
}

impl ToJson for InboxMessage {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "sender": self.sender,
            "subject": self.subject,
            "body_md": self.body_md,
            "importance": self.importance,
            "ack_required": self.ack_required,
            "thread_id": self.thread_id,
            "created_ts_us": self.created_ts_us,
        })
    }
}

impl ToJson for ReceiptResult {
    fn to_json(&self) -> Value {
        json!({
            "message_id": self.message_id,
            "acknowledged": self.acknowledged,
            "acknowledged_at_us": self.acknowledged_at_us,
        })
    }
}

impl ToJson for GrantedReservation {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "path_pattern": self.path_pattern,
            "exclusive": self.exclusive,
            "reason": self.reason,
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for PathConflict {
    fn to_json(&self) -> Value {
        json!({ "path": self.path, "holders": self.holders })
    }
}

impl ToJson for AcquireResult {
    fn to_json(&self) -> Value {
        json!({
            "granted": self.granted.to_json(),
            "conflicts": self.conflicts.to_json(),
        })
    }
}

impl ToJson for ConflictReport {
    fn to_json(&self) -> Value {
        json!({
            "conflict_free": self.conflict_free,
            "conflicts": self.conflicts.to_json(),
            "clear_paths": self.clear_paths,
            "checked_paths": self.checked_paths,
            "total_conflicting_reservations": self.total_conflicting_reservations,
        })
    }
}

impl ToJson for RenewResult {
    fn to_json(&self) -> Value {
        json!({
            "renewed": self.renewed,
            "reservations": self.reservations.to_json(),
        })
    }
}

impl ToJson for ReleaseResult {
    fn to_json(&self) -> Value {
        json!({ "released": self.released })
    }
}

impl ToJson for SearchMessage {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "subject": self.subject,
            "from": self.from,
            "created_ts_us": self.created_ts_us,
            "importance": self.importance,
            "ack_required": self.ack_required,
            "thread_id": self.thread_id,
            "body_md": self.body_md,
        })
    }
}

impl ToJson for (String, SearchMessage) {
    fn to_json(&self) -> Value {
        json!({
            "project": self.0,
            "id": self.1.id,
            "subject": self.1.subject,
            "from": self.1.from,
            "created_ts_us": self.1.created_ts_us,
            "importance": self.1.importance,
            "ack_required": self.1.ack_required,
            "thread_id": self.1.thread_id,
            "body_md": self.1.body_md,
        })
    }
}

impl ToJson for (String, InboxMessage) {
    fn to_json(&self) -> Value {
        json!({
            "project": self.0,
            "id": self.1.id,
            "sender": self.1.sender,
            "subject": self.1.subject,
            "body_md": self.1.body_md,
            "importance": self.1.importance,
            "ack_required": self.1.ack_required,
            "thread_id": self.1.thread_id,
            "created_ts_us": self.1.created_ts_us,
        })
    }
}

impl ToJson for ThreadSummary {
    fn to_json(&self) -> Value {
        json!({
            "thread_id": self.thread_id,
            "participants": self.participants,
            "key_points": self.key_points,
            "action_items": self.action_items,
            "message_count": self.message_count,
        })
    }
}

impl ToJson for ProductInfo {
    fn to_json(&self) -> Value {
        json!({
            "product_key": self.product_key,
            "name": self.name,
            "member_projects": self.member_projects,
            "created": self.created,
        })
    }
}

impl ToJson for GrantedSlot {
    fn to_json(&self) -> Value {
        json!({
            "slot": self.slot,
            "exclusive": self.exclusive,
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for SlotConflict {
    fn to_json(&self) -> Value {
        json!({ "slot": self.slot, "holders": self.holders })
    }
}

impl ToJson for SlotStatus {
    fn to_json(&self) -> Value {
        json!(self.as_str())
    }
}

impl ToJson for BuildSlot {
    fn to_json(&self) -> Value {
        json!({
            "slot": self.slot,
            "agent_name": self.agent_name,
            "agent_pubkey": self.agent_pubkey,
            "exclusive": self.exclusive,
            "status": self.status.to_json(),
            "expires_ts_us": self.expires_ts_us,
        })
    }
}

impl ToJson for AcquireBuildSlotResult {
    fn to_json(&self) -> Value {
        json!({
            "granted": self.granted.to_json(),
            "conflicts": self.conflicts.to_json(),
        })
    }
}

impl ToJson for StartSessionResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "npub": self.npub,
            "granted_reservations": self.granted_reservations.to_json(),
            "inbox_count": self.inbox_count,
        })
    }
}

impl ToJson for PrepareThreadResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "npub": self.npub,
            "thread_id": self.thread_id,
            "summary": self.summary.to_json(),
            "inbox_count": self.inbox_count,
        })
    }
}

impl ToJson for ContactHandshakeResult {
    fn to_json(&self) -> Value {
        json!({
            "requester": self.requester,
            "target": self.target,
            "link": self.link.to_json(),
            "welcome_sent": self.welcome_sent,
        })
    }
}

impl ToJson for ReservationCycleResult {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "agent_name": self.agent_name,
            "granted": self.granted.to_json(),
            "released": self.released,
        })
    }
}

/// The MCP server exposing the 38 AM-Rust parity tools over a mailbox.
#[derive(Clone)]
pub struct ToolServer {
    mailbox: Arc<Mailbox>,
    tool_router: ToolRouter<Self>,
}

impl ToolServer {
    /// Wrap a mailbox in the tool surface.
    pub fn new(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
            tool_router: Self::tool_router(),
        }
    }

    /// The wrapped mailbox.
    pub fn mailbox(&self) -> &Arc<Mailbox> {
        &self.mailbox
    }

    /// Server capabilities advertised via `initialize`/`discover` (mirrors
    /// `ToronServer::capabilities`).
    #[allow(deprecated)]
    pub fn capabilities() -> ServerCapabilities {
        ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_tool_list_changed()
            .enable_logging()
            .build()
    }
}

#[tool_router]
impl ToolServer {
    // ------------------------------------------------------------------
    // INFRASTRUCTURE (4)
    // ------------------------------------------------------------------

    /// `health_check` (read-only): bus liveness + archive health.
    #[tool(
        name = "health_check",
        description = "Bus liveness + archive health for a project (AM-Rust parity).",
        annotations(title = "Health check", read_only_hint = true)
    )]
    async fn health_check(&self, Parameters(_args): Parameters<HealthCheckArgs>) -> ToolOutput {
        ToolOutput::ok(self.mailbox.health_check().await.to_json())
    }

    /// `ensure_project`: idempotent project initialization.
    #[tool(
        name = "ensure_project",
        description = "Idempotent project initialization (kind-30010 manifest)."
    )]
    async fn ensure_project(&self, Parameters(args): Parameters<EnsureProjectArgs>) -> ToolOutput {
        ToolOutput::result(self.mailbox.ensure_project(&args.human_key).await)
    }

    /// `install_precommit_guard` (destructive): install the toron pre-commit hook.
    #[tool(
        name = "install_precommit_guard",
        description = "Install the toron pre-commit hook in a git repository.",
        annotations(title = "Install pre-commit guard", destructive_hint = true)
    )]
    async fn install_precommit_guard(
        &self,
        Parameters(args): Parameters<InstallPrecommitGuardArgs>,
    ) -> ToolOutput {
        match self
            .mailbox
            .install_precommit_guard(Path::new(&args.code_repo_path))
        {
            Ok(path) => ToolOutput::ok(json!({ "hook_path": path.display().to_string() })),
            Err(e) => ToolOutput::err(e),
        }
    }

    /// `uninstall_precommit_guard`: remove the toron hook (never clobbers a
    /// user hook).
    #[tool(
        name = "uninstall_precommit_guard",
        description = "Remove the toron pre-commit hook (never clobbers a user hook)."
    )]
    async fn uninstall_precommit_guard(
        &self,
        Parameters(args): Parameters<UninstallPrecommitGuardArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .uninstall_precommit_guard(Path::new(&args.code_repo_path)),
        )
    }

    // ------------------------------------------------------------------
    // IDENTITY (6)
    // ------------------------------------------------------------------

    /// `register_agent`: upsert on the existing agent key (or create).
    #[tool(
        name = "register_agent",
        description = "Upsert an agent identity on its existing key (or create if absent)."
    )]
    async fn register_agent(&self, Parameters(args): Parameters<RegisterAgentArgs>) -> ToolOutput {
        let name = args.name.clone().unwrap_or_else(generate_agent_name);
        let profile = AgentProfile {
            name: name.clone(),
            program: Some(args.program.clone()),
            model: Some(args.model.clone()),
            task_description: args.task_description.clone(),
            persona: None,
        };
        ToolOutput::result(
            self.mailbox
                .register_agent(&args.project_key, &profile)
                .await,
        )
    }

    /// `create_agent_identity`: ALWAYS mint a fresh keypair.
    #[tool(
        name = "create_agent_identity",
        description = "Mint a fresh agent keypair (ncryptsec) and publish the kind-30013 profile."
    )]
    async fn create_agent_identity(
        &self,
        Parameters(args): Parameters<CreateAgentIdentityArgs>,
    ) -> ToolOutput {
        let name = args.name_hint.clone().unwrap_or_else(generate_agent_name);
        let profile = AgentProfile {
            name: name.clone(),
            program: Some(args.program.clone()),
            model: Some(args.model.clone()),
            task_description: args.task_description.clone(),
            persona: None,
        };
        ToolOutput::result(
            self.mailbox
                .create_agent_identity(&args.project_key, &profile)
                .await,
        )
    }

    /// `whois` (read-only): name → npub + profile fields.
    #[tool(
        name = "whois",
        description = "Resolve an agent name to npub + profile fields (fail-fast on unknown).",
        annotations(title = "Whois", read_only_hint = true)
    )]
    async fn whois(&self, Parameters(args): Parameters<WhoisArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .whois(&args.project_key, &args.agent_name)
                .await,
        )
    }

    /// `resolve_pane_identity`: herdr pane_id → agent name (G9).
    #[tool(
        name = "resolve_pane_identity",
        description = "Resolve a herdr pane_id to the agent name (G9)."
    )]
    async fn resolve_pane_identity(
        &self,
        Parameters(args): Parameters<ResolvePaneIdentityArgs>,
    ) -> ToolOutput {
        let pane_id = args.pane_id.clone().unwrap_or_default();
        match HerdrBackend::connect(None) {
            Ok(backend) => match self.mailbox.resolve_pane_identity(&backend, &pane_id) {
                Ok(Some(name)) => ToolOutput::ok(json!({ "agent_name": name, "pane_id": pane_id })),
                Ok(None) => ToolOutput::ok(json!({ "agent_name": null, "pane_id": pane_id })),
                Err(e) => ToolOutput::err(e),
            },
            Err(e) => ToolOutput::ok(json!({
                "note": format!("pane identity requires a herdr backend; not wired: {e}"),
                "pane_id": pane_id,
            })),
        }
    }

    /// `cleanup_pane_identities`: prune identities whose herdr panes are gone.
    #[tool(
        name = "cleanup_pane_identities",
        description = "Prune identities whose herdr panes are absent from the live pane list."
    )]
    async fn cleanup_pane_identities(
        &self,
        Parameters(_args): Parameters<CleanupPaneIdentitiesArgs>,
    ) -> ToolOutput {
        match HerdrBackend::connect(None) {
            Ok(backend) => ToolOutput::result(self.mailbox.cleanup_pane_identities(&backend)),
            Err(e) => ToolOutput::ok(json!({
                "note": format!("pane cleanup requires a herdr backend; not wired: {e}"),
            })),
        }
    }

    /// `list_agents` (read-only): project agents with npub + last-active.
    #[tool(
        name = "list_agents",
        description = "List a project's agents with npub + last-active provenance.",
        annotations(title = "List agents", read_only_hint = true)
    )]
    async fn list_agents(&self, Parameters(args): Parameters<ListAgentsArgs>) -> ToolOutput {
        let opts = ListAgentsOptions {
            active_within_days: args.active_within_days.map(|d| d as u64),
        };
        ToolOutput::result(self.mailbox.list_agents(&args.project_key, opts).await)
    }

    // ------------------------------------------------------------------
    // MESSAGING (5)
    // ------------------------------------------------------------------

    /// `send_message`: NIP-17 gift-wrapped fan-out.
    #[tool(
        name = "send_message",
        description = "Send a NIP-17 gift-wrapped message to one or more recipients."
    )]
    async fn send_message(&self, Parameters(args): Parameters<SendMessageArgs>) -> ToolOutput {
        if args.broadcast == Some(true) {
            return ToolOutput::err("broadcast=true is rejected (AM-Rust invariant)");
        }
        let msg = SendMessage {
            project_slug: args.project_key.clone(),
            sender_name: args.sender_name.clone(),
            to: args.to.clone(),
            subject: args.subject.clone(),
            body_md: args.body_md.clone(),
            cc: args.cc.clone().unwrap_or_default(),
            bcc: args.bcc.clone().unwrap_or_default(),
            importance: args.importance.clone(),
            ack_required: args.ack_required.unwrap_or(false),
            thread_id: args.thread_id.clone(),
            sender_token: args.sender_token.clone(),
        };
        ToolOutput::result(self.mailbox.send_message(&msg).await)
    }

    /// `reply_message`: reply within a thread (NIP-10 `e` reference).
    #[tool(
        name = "reply_message",
        description = "Reply to a message within its thread (NIP-10 reply reference)."
    )]
    async fn reply_message(&self, Parameters(args): Parameters<ReplyMessageArgs>) -> ToolOutput {
        let msg = SendMessage {
            project_slug: args.project_key.clone(),
            sender_name: args.sender_name.clone(),
            to: args.to.clone(),
            subject: args.subject.clone(),
            body_md: args.body_md.clone(),
            cc: args.cc.clone().unwrap_or_default(),
            bcc: args.bcc.clone().unwrap_or_default(),
            importance: args.importance.clone(),
            ack_required: args.ack_required.unwrap_or(false),
            thread_id: args.thread_id.clone(),
            sender_token: args.sender_token.clone(),
        };
        ToolOutput::result(self.mailbox.reply_message(&msg).await)
    }

    /// `fetch_inbox` (read-only): the agent's inbox projection (rumor-time).
    #[tool(
        name = "fetch_inbox",
        description = "Project the agent's inbox (rumor-time order, G7).",
        annotations(title = "Fetch inbox", read_only_hint = true)
    )]
    async fn fetch_inbox(&self, Parameters(args): Parameters<FetchInboxArgs>) -> ToolOutput {
        let fi = FetchInbox {
            project_slug: args.project_key.clone(),
            agent_name: args.agent_name.clone(),
            unread_only: args.unread_only.unwrap_or(false),
            limit: args.limit.map(|l| l as usize),
            include_bodies: args.include_bodies.unwrap_or(false),
        };
        ToolOutput::result(self.mailbox.fetch_inbox(&fi).await)
    }

    /// `acknowledge_message`: private idempotent ack to the original sender.
    #[tool(
        name = "acknowledge_message",
        description = "Acknowledge a message (private NIP-17 ack, idempotent)."
    )]
    async fn acknowledge_message(
        &self,
        Parameters(args): Parameters<AcknowledgeMessageArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .acknowledge_message(
                    &args.project_key,
                    &args.agent_name,
                    &args.message_id.to_string(),
                )
                .await,
        )
    }

    /// `mark_message_read`: private idempotent read receipt.
    #[tool(
        name = "mark_message_read",
        description = "Mark a message read (private NIP-17 read receipt, idempotent)."
    )]
    async fn mark_message_read(
        &self,
        Parameters(args): Parameters<MarkMessageReadArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .mark_message_read(
                    &args.project_key,
                    &args.agent_name,
                    &args.message_id.to_string(),
                )
                .await,
        )
    }

    // ------------------------------------------------------------------
    // CONTACTS (4)
    // ------------------------------------------------------------------

    /// `request_contact`: request a contact link (AgentLink, G14).
    #[tool(
        name = "request_contact",
        description = "Request contact with another agent (AgentLink + NIP-17 control message)."
    )]
    async fn request_contact(
        &self,
        Parameters(args): Parameters<RequestContactArgs>,
    ) -> ToolOutput {
        let rc = RequestContact {
            project_slug: args.project_key.clone(),
            from_agent: args.from_agent.clone(),
            to_agent: args.to_agent.clone(),
            to_project: args.to_project.clone(),
            reason: args.reason.clone(),
        };
        ToolOutput::result(self.mailbox.request_contact(&rc).await)
    }

    /// `respond_contact`: approve or deny a contact request.
    #[tool(
        name = "respond_contact",
        description = "Approve or deny a pending contact request."
    )]
    async fn respond_contact(
        &self,
        Parameters(args): Parameters<RespondContactArgs>,
    ) -> ToolOutput {
        let rc = RespondContact {
            project_slug: args.project_key.clone(),
            to_agent: args.to_agent.clone(),
            from_agent: args.from_agent.clone(),
            accept: args.accept,
            from_project: args.from_project.clone(),
        };
        ToolOutput::result(self.mailbox.respond_contact(&rc).await)
    }

    /// `list_contacts`: an agent's contact links.
    #[tool(
        name = "list_contacts",
        description = "List an agent's contact links (AgentLink rows)."
    )]
    async fn list_contacts(&self, Parameters(args): Parameters<ListContactsArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .list_contacts(&args.project_key, &args.agent_name)
                .await,
        )
    }

    /// `set_contact_policy`: the agent's inbound contact policy (kind-30014).
    #[tool(
        name = "set_contact_policy",
        description = "Set the agent's inbound contact policy (open|auto|contacts_only|block_all)."
    )]
    async fn set_contact_policy(
        &self,
        Parameters(args): Parameters<SetContactPolicyArgs>,
    ) -> ToolOutput {
        let policy = ContactPolicy::parse(&args.policy);
        ToolOutput::result(
            self.mailbox
                .set_contact_policy(&args.project_key, &args.agent_name, policy)
                .await,
        )
    }

    // ------------------------------------------------------------------
    // FILE RESERVATIONS (5)
    // ------------------------------------------------------------------

    /// `check_file_reservation_conflicts` (read-only): symmetric-glob report.
    #[tool(
        name = "check_file_reservation_conflicts",
        description = "Symmetric-glob conflict report for paths vs other agents' reservations.",
        annotations(title = "Check file reservation conflicts", read_only_hint = true)
    )]
    async fn check_file_reservation_conflicts(
        &self,
        Parameters(args): Parameters<CheckFileReservationConflictsArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .check_file_reservation_conflicts(&args.project_key, &args.agent_name, &args.paths)
                .await,
        )
    }

    /// `file_reservation_paths` (destructive): acquire leases on path globs.
    #[tool(
        name = "file_reservation_paths",
        description = "Acquire exclusive/shared leases on path globs (kind-30031).",
        annotations(title = "File reservation paths", destructive_hint = true)
    )]
    async fn file_reservation_paths(
        &self,
        Parameters(args): Parameters<FileReservationPathsArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .file_reservation_paths(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths,
                    args.ttl_seconds.map(|t| t as u64),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                )
                .await,
        )
    }

    /// `renew_file_reservations`: extend the holder's active lease TTLs.
    #[tool(
        name = "renew_file_reservations",
        description = "Extend the TTLs of the holder's active file reservations."
    )]
    async fn renew_file_reservations(
        &self,
        Parameters(args): Parameters<RenewFileReservationsArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .renew_file_reservations(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths.clone().unwrap_or_default(),
                    args.extend_seconds.unwrap_or(0) as u64,
                )
                .await,
        )
    }

    /// `release_file_reservations`: release the holder's leases.
    #[tool(
        name = "release_file_reservations",
        description = "Release the holder's active file reservations (idempotent)."
    )]
    async fn release_file_reservations(
        &self,
        Parameters(args): Parameters<ReleaseFileReservationsArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .release_file_reservations(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths.clone().unwrap_or_default(),
                )
                .await,
        )
    }

    /// `force_release_file_reservation` (destructive): operator revocation.
    #[tool(
        name = "force_release_file_reservation",
        description = "Operator force-release of a stale reservation (staleness heuristic + revocation).",
        annotations(title = "Force release file reservation", destructive_hint = true)
    )]
    async fn force_release_file_reservation(
        &self,
        Parameters(args): Parameters<ForceReleaseFileReservationArgs>,
    ) -> ToolOutput {
        match self
            .mailbox
            .force_release_file_reservation(
                &args.project_key,
                &args.agent_name,
                &args.file_reservation_id.to_string(),
                args.note.as_deref(),
                args.notify_previous.unwrap_or(false),
            )
            .await
        {
            Ok(()) => ToolOutput::ok(json!({ "ok": true })),
            Err(e) => ToolOutput::err(e),
        }
    }

    // ------------------------------------------------------------------
    // SEARCH (2)
    // ------------------------------------------------------------------

    /// `search_messages` (read-only): FTS5 search over indexed messages.
    #[tool(
        name = "search_messages",
        description = "FTS5 search over a project's indexed messages (porter/unicode61).",
        annotations(title = "Search messages", read_only_hint = true)
    )]
    async fn search_messages(
        &self,
        Parameters(args): Parameters<SearchMessagesArgs>,
    ) -> ToolOutput {
        let sm = SearchMessages {
            project_key: args.project_key.clone(),
            query: args.query.clone(),
            limit: args.limit.map(|l| l as usize),
            ranking: args.ranking.clone(),
            sender_name: args.sender.clone(),
            importance: args.importance.clone(),
            thread_id: args.thread_id.clone(),
            since_us: args.since.as_deref().and_then(|s| s.parse::<i64>().ok()),
            until_us: args.until.as_deref().and_then(|s| s.parse::<i64>().ok()),
            include_body_md: args.include_body_md.unwrap_or(false),
        };
        ToolOutput::result(self.mailbox.search_messages(&sm).await)
    }

    /// `summarize_thread` (read-only): deterministic thread digest.
    #[tool(
        name = "summarize_thread",
        description = "Deterministic digest of a thread (participants, key points, actions).",
        annotations(title = "Summarize thread", read_only_hint = true)
    )]
    async fn summarize_thread(
        &self,
        Parameters(args): Parameters<SummarizeThreadArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .summarize_thread(
                    &args.project_key,
                    &args.thread_id,
                    args.include_examples.unwrap_or(false),
                    args.llm_mode.unwrap_or(false),
                    args.llm_model.as_deref(),
                )
                .await,
        )
    }

    // ------------------------------------------------------------------
    // MACROS (4)
    // ------------------------------------------------------------------

    /// `macro_start_session`: ensure_project + register + reserve + inbox.
    #[tool(
        name = "macro_start_session",
        description = "Batch: ensure_project + register agent + optional file reservation + inbox snapshot."
    )]
    async fn macro_start_session(
        &self,
        Parameters(args): Parameters<MacroStartSessionArgs>,
    ) -> ToolOutput {
        let agent_name = args.agent_name.clone().unwrap_or_else(generate_agent_name);
        ToolOutput::result(
            self.mailbox
                .macro_start_session(
                    &args.human_key,
                    &agent_name,
                    &args.program,
                    &args.model,
                    args.task_description.as_deref(),
                    args.file_reservation_paths.as_deref(),
                    args.file_reservation_ttl_seconds.map(|t| t as u64),
                    args.file_reservation_reason.as_deref(),
                    args.inbox_limit.map(|l| l as usize),
                )
                .await,
        )
    }

    /// `macro_prepare_thread`: registration + thread summary + inbox context.
    #[tool(
        name = "macro_prepare_thread",
        description = "Batch: ensure registration + thread summary + recent inbox context."
    )]
    async fn macro_prepare_thread(
        &self,
        Parameters(args): Parameters<MacroPrepareThreadArgs>,
    ) -> ToolOutput {
        let agent_name = args.agent_name.clone().unwrap_or_else(generate_agent_name);
        ToolOutput::result(
            self.mailbox
                .macro_prepare_thread(
                    &args.project_key,
                    &args.thread_id,
                    &agent_name,
                    &args.program,
                    &args.model,
                    args.task_description.as_deref(),
                    args.register_if_missing.unwrap_or(false),
                    args.include_examples.unwrap_or(false),
                    args.inbox_limit.map(|l| l as usize),
                )
                .await,
        )
    }

    /// `macro_contact_handshake`: request + auto-accept + welcome.
    #[tool(
        name = "macro_contact_handshake",
        description = "Batch: request contact + optional auto-accept + welcome message."
    )]
    async fn macro_contact_handshake(
        &self,
        Parameters(args): Parameters<MacroContactHandshakeArgs>,
    ) -> ToolOutput {
        let requester = args
            .requester
            .clone()
            .or_else(|| args.agent_name.clone())
            .unwrap_or_else(generate_agent_name);
        let target = args
            .target
            .clone()
            .or_else(|| args.to_agent.clone())
            .unwrap_or_else(|| "Peer".to_string());
        ToolOutput::result(
            self.mailbox
                .macro_contact_handshake(
                    &args.project_key,
                    &requester,
                    &target,
                    args.reason.as_deref(),
                    args.auto_accept.unwrap_or(false),
                    args.welcome_subject.as_deref(),
                    args.welcome_body.as_deref(),
                )
                .await,
        )
    }

    /// `macro_file_reservation_cycle`: reserve paths, optionally release.
    #[tool(
        name = "macro_file_reservation_cycle",
        description = "Batch: reserve a set of path globs, optionally release them at the end."
    )]
    async fn macro_file_reservation_cycle(
        &self,
        Parameters(args): Parameters<MacroFileReservationCycleArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .macro_file_reservation_cycle(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths,
                    args.ttl_seconds.map(|t| t as u64),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                    args.auto_release.unwrap_or(false),
                )
                .await,
        )
    }

    // ------------------------------------------------------------------
    // PRODUCT BUS (5)
    // ------------------------------------------------------------------

    /// `ensure_product`: idempotent product manifest (kind-30011).
    #[tool(
        name = "ensure_product",
        description = "Idempotent product manifest (kind-30011) for a product key."
    )]
    async fn ensure_product(&self, Parameters(args): Parameters<EnsureProductArgs>) -> ToolOutput {
        let key = args
            .product_key
            .clone()
            .or_else(|| args.name.clone())
            .unwrap_or_else(|| "product".to_string());
        let name = args.name.clone().unwrap_or_else(|| key.clone());
        ToolOutput::result(self.mailbox.ensure_product(&key, &name).await)
    }

    /// `products_link`: link a project into a product (idempotent).
    #[tool(
        name = "products_link",
        description = "Link a project into a product (idempotent; kind-30011 a-tag)."
    )]
    async fn products_link(&self, Parameters(args): Parameters<ProductsLinkArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .products_link(&args.product_key, &args.project_key)
                .await,
        )
    }

    /// `search_messages_product`: fan search across member projects.
    #[tool(
        name = "search_messages_product",
        description = "Fan an FTS5 search across a product's member projects."
    )]
    async fn search_messages_product(
        &self,
        Parameters(args): Parameters<SearchMessagesProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .search_messages_product(
                    &args.product_key,
                    &args.query,
                    args.limit.map(|l| l as usize),
                    args.include_body_md.unwrap_or(false),
                )
                .await,
        )
    }

    /// `fetch_inbox_product`: fan inbox fetch across member projects.
    #[tool(
        name = "fetch_inbox_product",
        description = "Fan an inbox fetch across a product's member projects."
    )]
    async fn fetch_inbox_product(
        &self,
        Parameters(args): Parameters<FetchInboxProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .fetch_inbox_product(&args.product_key, &args.agent_name)
                .await,
        )
    }

    /// `summarize_thread_product`: summarize across member projects.
    #[tool(
        name = "summarize_thread_product",
        description = "Summarize a thread across a product's member projects."
    )]
    async fn summarize_thread_product(
        &self,
        Parameters(args): Parameters<SummarizeThreadProductArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .summarize_thread_product(
                    &args.product_key,
                    &args.thread_id,
                    args.include_examples.unwrap_or(false),
                )
                .await,
        )
    }

    // ------------------------------------------------------------------
    // BUILD SLOTS (3)
    // ------------------------------------------------------------------

    /// `acquire_build_slot` (destructive): advisory slot lease.
    #[tool(
        name = "acquire_build_slot",
        description = "Acquire an advisory build-slot lease (kind-30032), optionally exclusive.",
        annotations(title = "Acquire build slot", destructive_hint = true)
    )]
    async fn acquire_build_slot(
        &self,
        Parameters(args): Parameters<AcquireBuildSlotArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .acquire_build_slot(
                    &args.project_key,
                    &args.agent_name,
                    &args.slot,
                    args.ttl_seconds.map(|t| t as u64),
                    args.exclusive.unwrap_or(false),
                )
                .await,
        )
    }

    /// `renew_build_slot`: extend an active slot lease's TTL.
    #[tool(
        name = "renew_build_slot",
        description = "Extend an active build-slot lease's TTL (no-op when missing)."
    )]
    async fn renew_build_slot(
        &self,
        Parameters(args): Parameters<RenewBuildSlotArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .renew_build_slot(
                    &args.project_key,
                    &args.agent_name,
                    &args.slot,
                    args.extend_seconds.unwrap_or(0) as u64,
                )
                .await,
        )
    }

    /// `release_build_slot`: release an active slot lease.
    #[tool(
        name = "release_build_slot",
        description = "Release an active build-slot lease (non-destructive)."
    )]
    async fn release_build_slot(
        &self,
        Parameters(args): Parameters<ReleaseBuildSlotArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .release_build_slot(&args.project_key, &args.agent_name, &args.slot)
                .await,
        )
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ToolServer {
    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(Self::capabilities())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_server_info(Implementation::new("toron", env!("CARGO_PKG_VERSION")))
    }
}

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct HealthCheckArgs {
    pub project_key: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct EnsureProjectArgs {
    pub human_key: String,
    pub identity_mode: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct InstallPrecommitGuardArgs {
    pub project_key: String,
    pub code_repo_path: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct UninstallPrecommitGuardArgs {
    pub code_repo_path: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct RegisterAgentArgs {
    pub project_key: String,
    pub program: String,
    pub model: String,
    pub name: Option<String>,
    pub task_description: Option<String>,
    pub attachments_policy: Option<String>,
    pub reaper_exempt: Option<bool>,
    pub pane_id: Option<String>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct CreateAgentIdentityArgs {
    pub project_key: String,
    pub program: String,
    pub model: String,
    pub name_hint: Option<String>,
    pub task_description: Option<String>,
    pub attachments_policy: Option<String>,
    pub pane_id: Option<String>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct WhoisArgs {
    pub project_key: String,
    pub agent_name: String,
    pub include_recent_commits: Option<bool>,
    pub commit_limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ResolvePaneIdentityArgs {
    pub project_key: String,
    pub pane_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct CleanupPaneIdentitiesArgs {
    pub project_key: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ListAgentsArgs {
    pub project_key: String,
    pub limit: Option<u32>,
    pub active_within_days: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SendMessageArgs {
    pub project_key: String,
    pub sender_name: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub cc: Option<Vec<String>>,
    pub bcc: Option<Vec<String>>,
    pub attachment_paths: Option<Vec<String>>,
    pub convert_images: Option<bool>,
    pub importance: Option<String>,
    pub ack_required: Option<bool>,
    pub thread_id: Option<String>,
    pub topic: Option<String>,
    pub broadcast: Option<bool>,
    pub auto_contact_if_blocked: Option<bool>,
    pub sender_token: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ReplyMessageArgs {
    pub project_key: String,
    pub sender_name: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub cc: Option<Vec<String>>,
    pub bcc: Option<Vec<String>>,
    pub attachment_paths: Option<Vec<String>>,
    pub convert_images: Option<bool>,
    pub importance: Option<String>,
    pub ack_required: Option<bool>,
    pub thread_id: Option<String>,
    pub topic: Option<String>,
    pub broadcast: Option<bool>,
    pub auto_contact_if_blocked: Option<bool>,
    pub sender_token: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct FetchInboxArgs {
    pub project_key: String,
    pub agent_name: String,
    pub urgent_only: Option<bool>,
    pub since_ts: Option<String>,
    pub limit: Option<i32>,
    pub include_bodies: Option<bool>,
    pub unread_only: Option<bool>,
    pub ack_overdue_only: Option<bool>,
    pub topic: Option<String>,
    pub mark_read: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct AcknowledgeMessageArgs {
    pub project_key: String,
    pub agent_name: String,
    pub message_id: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MarkMessageReadArgs {
    pub project_key: String,
    pub agent_name: String,
    pub message_id: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct RequestContactArgs {
    pub project_key: String,
    pub from_agent: String,
    pub to_agent: String,
    pub to_project: Option<String>,
    pub reason: Option<String>,
    pub ttl_seconds: Option<i64>,
    pub register_if_missing: Option<bool>,
    pub program: Option<String>,
    pub model: Option<String>,
    pub task_description: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct RespondContactArgs {
    pub project_key: String,
    pub to_agent: String,
    pub from_agent: String,
    pub from_project: Option<String>,
    pub accept: bool,
    pub ttl_seconds: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ListContactsArgs {
    pub project_key: String,
    pub agent_name: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SetContactPolicyArgs {
    pub project_key: String,
    pub agent_name: String,
    pub policy: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct CheckFileReservationConflictsArgs {
    pub project_key: String,
    pub agent_name: String,
    pub paths: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct FileReservationPathsArgs {
    pub project_key: String,
    pub agent_name: String,
    pub paths: Vec<String>,
    pub ttl_seconds: Option<i64>,
    pub exclusive: Option<bool>,
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct RenewFileReservationsArgs {
    pub project_key: String,
    pub agent_name: String,
    pub extend_seconds: Option<i64>,
    pub paths: Option<Vec<String>>,
    pub file_reservation_ids: Option<Vec<i64>>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ReleaseFileReservationsArgs {
    pub project_key: String,
    pub agent_name: String,
    pub paths: Option<Vec<String>>,
    pub file_reservation_ids: Option<Vec<i64>>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ForceReleaseFileReservationArgs {
    pub project_key: String,
    pub agent_name: String,
    pub file_reservation_id: i64,
    pub note: Option<String>,
    pub notify_previous: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SearchMessagesArgs {
    pub project_key: String,
    pub query: String,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
    pub cursor: Option<String>,
    pub ranking: Option<String>,
    pub sender: Option<String>,
    pub importance: Option<String>,
    pub thread_id: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub explain: Option<bool>,
    pub include_body_md: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SummarizeThreadArgs {
    pub project_key: String,
    pub thread_id: String,
    pub include_examples: Option<bool>,
    pub llm_mode: Option<bool>,
    pub llm_model: Option<String>,
    pub per_thread_limit: Option<i32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MacroStartSessionArgs {
    pub human_key: String,
    pub program: String,
    pub model: String,
    pub agent_name: Option<String>,
    pub task_description: Option<String>,
    pub file_reservation_paths: Option<Vec<String>>,
    pub file_reservation_reason: Option<String>,
    pub file_reservation_ttl_seconds: Option<i64>,
    pub inbox_limit: Option<i32>,
    pub reaper_exempt: Option<bool>,
    pub pane_id: Option<String>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MacroPrepareThreadArgs {
    pub project_key: String,
    pub thread_id: String,
    pub program: String,
    pub model: String,
    pub agent_name: Option<String>,
    pub task_description: Option<String>,
    pub register_if_missing: Option<bool>,
    pub include_examples: Option<bool>,
    pub include_inbox_bodies: Option<bool>,
    pub llm_mode: Option<bool>,
    pub llm_model: Option<String>,
    pub inbox_limit: Option<i32>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MacroContactHandshakeArgs {
    pub project_key: String,
    pub requester: Option<String>,
    pub target: Option<String>,
    pub agent_name: Option<String>,
    pub to_agent: Option<String>,
    pub to_project: Option<String>,
    pub reason: Option<String>,
    pub auto_accept: Option<bool>,
    pub ttl_seconds: Option<i64>,
    pub welcome_subject: Option<String>,
    pub welcome_body: Option<String>,
    pub thread_id: Option<String>,
    pub register_if_missing: Option<bool>,
    pub program: Option<String>,
    pub model: Option<String>,
    pub task_description: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MacroFileReservationCycleArgs {
    pub project_key: String,
    pub agent_name: String,
    pub paths: Vec<String>,
    pub ttl_seconds: Option<i64>,
    pub exclusive: Option<bool>,
    pub reason: Option<String>,
    pub auto_release: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct EnsureProductArgs {
    pub product_key: Option<String>,
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ProductsLinkArgs {
    pub product_key: String,
    pub project_key: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SearchMessagesProductArgs {
    pub product_key: String,
    pub query: String,
    pub limit: Option<i32>,
    pub cursor: Option<String>,
    pub project: Option<String>,
    pub sender: Option<String>,
    pub importance: Option<String>,
    pub thread_id: Option<String>,
    pub ranking: Option<String>,
    pub include_body_md: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct FetchInboxProductArgs {
    pub product_key: String,
    pub agent_name: String,
    pub limit: Option<i32>,
    pub urgent_only: Option<bool>,
    pub include_bodies: Option<bool>,
    pub since_ts: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct SummarizeThreadProductArgs {
    pub product_key: String,
    pub thread_id: String,
    pub include_examples: Option<bool>,
    pub llm_mode: Option<bool>,
    pub llm_model: Option<String>,
    pub per_thread_limit: Option<i32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct AcquireBuildSlotArgs {
    pub project_key: String,
    pub agent_name: String,
    pub slot: String,
    pub ttl_seconds: Option<i64>,
    pub exclusive: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct RenewBuildSlotArgs {
    pub project_key: String,
    pub agent_name: String,
    pub slot: String,
    pub extend_seconds: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ReleaseBuildSlotArgs {
    pub project_key: String,
    pub agent_name: String,
    pub slot: String,
}

/// Deterministic adjective+noun fallback for tools that do not require an
/// explicit agent name (AM-Rust naming rule: never descriptive-role names).
fn generate_agent_name() -> String {
    const ADJECTIVES: [&str; 12] = [
        "Blue", "Green", "Red", "Pale", "Coral", "Dusty", "Crimson", "Azure", "Sable", "Frost",
        "Ivory", "Olive",
    ];
    const NOUNS: [&str; 12] = [
        "Lake", "Castle", "Finch", "Sparrow", "Dune", "Stone", "Willow", "Fox", "Hawk", "Otter",
        "Pine", "Creek",
    ];
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed) as usize;
    format!(
        "{}{}",
        ADJECTIVES[n % ADJECTIVES.len()],
        NOUNS[(n / ADJECTIVES.len()) % NOUNS.len()]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_agent_name_is_adjective_noun() {
        let a = generate_agent_name();
        let b = generate_agent_name();
        assert_ne!(a, b, "sequential names differ");
        assert!(a.chars().count() > 3);
    }
}
