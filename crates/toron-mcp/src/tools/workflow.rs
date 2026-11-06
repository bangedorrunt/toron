// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! Workflow-automation (macro) tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: the 4 `macro_*` session
//! tools (macro = workflow-automation plane per ADR-0005; the module is named
//! `workflow.rs` to avoid confusion with Rust macro definitions).
//! `#[tool_router(router = workflow_router)]` emits
//! `pub(crate) fn workflow_router()`; `tools/mod.rs` merges it into the single
//! 38-tool surface via `ToolRouter`'s `Add`. Frozen names/args/returns:
//! ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};

use super::{ToolOutput, ToolServer, generate_agent_name, nonneg_secs};

#[tool_router(router = workflow_router, vis = "pub(crate)")]
impl ToolServer {
    /// `macro_start_session`: ensure_project + register + reserve + inbox.
    #[tool(
        name = "macro_start_session",
        description = "Batch: ensure_project + register agent + optional file reservation + inbox snapshot.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::StartSessionResultOut>(),
        annotations(title = "Macro start session", destructive_hint = true)
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
                    nonneg_secs(args.file_reservation_ttl_seconds),
                    args.file_reservation_reason.as_deref(),
                    args.inbox_limit.map(|l| l as usize),
                )
                .await,
        )
    }

    /// `macro_prepare_thread`: registration + thread summary + inbox context.
    #[tool(
        name = "macro_prepare_thread",
        description = "Batch: ensure registration + thread summary + recent inbox context.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::PrepareThreadResultOut>(),
        annotations(title = "Macro prepare thread", read_only_hint = true)
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
        description = "Batch: request contact + optional auto-accept + welcome message.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ContactHandshakeResultOut>(),
        annotations(title = "Macro contact handshake", destructive_hint = true)
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
        description = "Batch: reserve a set of path globs, optionally release them at the end.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ReservationCycleResultOut>(),
        annotations(title = "Macro file reservation cycle", destructive_hint = true)
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
                    nonneg_secs(args.ttl_seconds),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                    args.auto_release.unwrap_or(false),
                )
                .await,
        )
    }
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
