// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! Messaging + search tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: messaging (5) + search
//! (2) tools. `#[tool_router(router = mail_router)]` emits
//! `pub(crate) fn mail_router()`; `tools/mod.rs` merges it into the single
//! 38-tool surface via `ToolRouter`'s `Add`. Frozen names/args/returns:
//! ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use toron_core::mailbox::{FetchInbox, SearchMessages, SendMessage};

use super::{ToJson, ToolOutput, ToolServer};

#[tool_router(router = mail_router, vis = "pub(crate)")]
impl ToolServer {
    // ------------------------------------------------------------------
    // MESSAGING (5)
    // ------------------------------------------------------------------

    /// `send_message`: NIP-17 gift-wrapped fan-out.
    #[tool(
        name = "send_message",
        description = "Send a NIP-17 gift-wrapped message to one or more recipients. REQUIRED: project_key, sender_name (your adjective+noun pin), to[], subject, body_md. Shell available? Prefer: flywheel mail send <proj> --to <names> --subject S --body B.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::SendResultOut>(),
        annotations(title = "Send message", destructive_hint = true)
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
            overseer: false,
            extra_tags: Vec::new(),
        };
        ToolOutput::result(self.mailbox.send_message(&msg).await)
    }

    /// `reply_message`: reply within a thread (NIP-10 `e` reference).
    #[tool(
        name = "reply_message",
        description = "Reply to a message within its thread (NIP-10 reply reference). REQUIRED: project_key, sender_name (your adjective+noun pin), to[], subject, body_md. Thread context comes from args (thread_id) - no auto-parent resolution on toron; the flywheel facade resolves it.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::SendResultOut>(),
        annotations(title = "Reply message", destructive_hint = true)
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
            overseer: false,
            extra_tags: Vec::new(),
        };
        ToolOutput::result(self.mailbox.reply_message(&msg).await)
    }

    /// `fetch_inbox` (read-only): the agent's inbox projection (rumor-time).
    #[tool(
        name = "fetch_inbox",
        description = "Project the agent's inbox (rumor-time order, G7).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Vec<crate::responses::InboxMessageOut>>(),
        annotations(title = "Fetch inbox", read_only_hint = true)
    )]
    async fn fetch_inbox(&self, Parameters(args): Parameters<FetchInboxArgs>) -> ToolOutput {
        let fi = FetchInbox {
            project_slug: args.project_key.clone(),
            agent_name: args.agent_name.clone(),
            unread_only: args.unread_only.unwrap_or(false),
            limit: args.limit.map(|l| l as usize),
            include_bodies: args.include_bodies.unwrap_or(false),
            urgent_only: args.urgent_only.unwrap_or(false),
            since_ts: args.since_ts.clone(),
        };
        ToolOutput::result(self.mailbox.fetch_inbox(&fi).await)
    }

    /// `acknowledge_message`: private idempotent ack to the original sender.
    #[tool(
        name = "acknowledge_message",
        description = "Acknowledge a message (private NIP-17 ack, idempotent).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ReceiptResultOut>(),
        annotations(title = "Acknowledge message", read_only_hint = true)
    )]
    async fn acknowledge_message(
        &self,
        Parameters(args): Parameters<AcknowledgeMessageArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .acknowledge_message(&args.project_key, &args.agent_name, &args.message_id)
                .await,
        )
    }

    /// `mark_message_read`: private idempotent read receipt.
    #[tool(
        name = "mark_message_read",
        description = "Mark a message read (private NIP-17 read receipt, idempotent).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ReceiptResultOut>(),
        annotations(title = "Mark message read", read_only_hint = true)
    )]
    async fn mark_message_read(
        &self,
        Parameters(args): Parameters<MarkMessageReadArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .mark_message_read(&args.project_key, &args.agent_name, &args.message_id)
                .await,
        )
    }

    // ------------------------------------------------------------------
    // SEARCH (2)
    // ------------------------------------------------------------------

    /// `search_messages` (read-only): FTS5 search over indexed messages.
    ///
    /// AM-parity envelope (bd-led.7.2 / live parity 2026-08-08): AM's
    /// `search_messages` wraps rows in `{result, next_cursor}` where
    /// `next_cursor` (an opaque `s<sort>:i<last-id>` resume token) is
    /// present exactly when results are non-empty. Emit the same shape so
    /// the flywheel parity harness sees identical JSON key sets.
    #[tool(
        name = "search_messages",
        description = "FTS5 search over a project's indexed messages (porter/unicode61).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Vec<crate::responses::SearchMessageOut>>(),
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
        match self.mailbox.search_messages(&sm).await {
            Ok(rows) => {
                let cursor = rows.last().map(|r| format!("s0000000000000000:i{}", r.id));
                let result = rows.to_json();
                let mut map = serde_json::Map::new();
                if let Some(c) = cursor {
                    map.insert("next_cursor".to_string(), Value::String(c));
                }
                map.insert("result".to_string(), result);
                ToolOutput::ok(Value::Object(map))
            }
            Err(e) => ToolOutput::err(format!("{e:#}")),
        }
    }

    /// `summarize_thread` (read-only): deterministic thread digest.
    #[tool(
        name = "summarize_thread",
        description = "Deterministic digest of a thread (participants, key points, actions).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ThreadSummaryOut>(),
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
}

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

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
    /// toron message id = the rumor's content-addressed hex EventId (NOT an
    /// AM-style integer sequence). The arg is a string to match toron's own
    /// id model — `fetch_inbox` returns `id` as hex; this accepts it verbatim.
    /// (known-divergences §1: id-type differs by design per ADR-0003.)
    pub message_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct MarkMessageReadArgs {
    pub project_key: String,
    pub agent_name: String,
    /// See [`AcknowledgeMessageArgs::message_id`] — hex event id, not an int.
    pub message_id: String,
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
