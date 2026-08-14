// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! Contact tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: contacts (4) tools.
//! `#[tool_router(router = contact_router)]` emits
//! `pub(crate) fn contact_router()`; `tools/mod.rs` merges it into the single
//! 38-tool surface via `ToolRouter`'s `Add`. Frozen names/args/returns:
//! ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};

use toron_core::mailbox::{ContactPolicy, RequestContact, RespondContact};

use super::{ToolOutput, ToolServer};

#[tool_router(router = contact_router, vis = "pub(crate)")]
impl ToolServer {
    // ------------------------------------------------------------------
    // CONTACTS (4)
    // ------------------------------------------------------------------

    /// `request_contact`: request a contact link (AgentLink, G14).
    #[tool(
        name = "request_contact",
        description = "Request contact with another agent (AgentLink + NIP-17 control message).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ContactLinkOut>(),
        annotations(title = "Request contact", destructive_hint = true)
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
        description = "Approve or deny a pending contact request.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ContactLinkOut>(),
        annotations(title = "Respond contact", destructive_hint = true)
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
        description = "List an agent's contact links (AgentLink rows). Link status is the social graph (pending|approved|denied) — it does NOT gate mail. Delivery is gated by the RECIPIENT's contact policy (kind-30014, see set_contact_policy, default auto): a pending link still receives mail unless the recipient's policy blocks it.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Vec<crate::responses::ContactLinkOut>>(),
        annotations(title = "List contacts", read_only_hint = true)
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
        description = "Set the agent's inbound contact policy (open|auto|contacts_only|block_all).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<String>(),
        annotations(title = "Set contact policy", destructive_hint = true)
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
}

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

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
