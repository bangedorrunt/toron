// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)
// governed-by: ADR-0017 (project-root canonicalization)

//! System + identity tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: infrastructure (4) +
//! identity (6) tools. The `#[tool_router(router = system_router)]` macro
//! emits `pub(crate) fn system_router()`; `tools/mod.rs` merges it with the
//! other domain routers via `ToolRouter`'s `Add` (rmcp-3.2.0) into the single
//! 38-tool surface. Frozen tool names/args/returns: ZERO change (ADR-0007 C3).

use std::path::Path;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::json;

use toron_core::mailbox::ListAgentsOptions;
use toron_core::nostr::identity::AgentProfile;

use super::{ToJson, ToolOutput, ToolServer, generate_agent_name};

#[tool_router(router = system_router, vis = "pub(crate)")]
impl ToolServer {
    /// `health_check` (read-only): bus liveness + archive health.
    #[tool(
        name = "health_check",
        description = "Bus liveness + archive health for a project (AM-Rust parity).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::HealthReportOut>(),
        annotations(title = "Health check", read_only_hint = true)
    )]
    async fn health_check(&self, Parameters(_args): Parameters<HealthCheckArgs>) -> ToolOutput {
        ToolOutput::ok(self.mailbox.health_check().await.to_json())
    }

    /// `ensure_project`: idempotent project initialization.
    #[tool(
        name = "ensure_project",
        description = "Idempotent project initialization (kind-30010 manifest).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ProjectStateOut>(),
        annotations(title = "Ensure project", destructive_hint = true)
    )]
    async fn ensure_project(&self, Parameters(args): Parameters<EnsureProjectArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .ensure_project_with_mode(&args.human_key, args.identity_mode.as_deref())
                .await,
        )
    }

    /// `install_precommit_guard` (destructive): install the toron pre-commit hook.
    #[tool(
        name = "install_precommit_guard",
        description = "Install the toron pre-commit hook in a git repository.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::HookPathOut>(),
        annotations(title = "Install pre-commit guard", destructive_hint = true)
    )]
    async fn install_precommit_guard(
        &self,
        Parameters(args): Parameters<InstallPrecommitGuardArgs>,
    ) -> ToolOutput {
        let project = self.mailbox.canonical_project(&args.project_key).await;
        match self
            .mailbox
            .install_precommit_guard(Path::new(&args.code_repo_path), &project)
        {
            Ok(path) => ToolOutput::ok(json!({ "hook_path": path.display().to_string() })),
            Err(e) => ToolOutput::err(e),
        }
    }

    /// `uninstall_precommit_guard`: remove the toron hook (never clobbers a
    /// user hook).
    #[tool(
        name = "uninstall_precommit_guard",
        description = "Remove the toron pre-commit hook (never clobbers a user hook).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::UninstallHookWrapper>(),
        annotations(title = "Uninstall pre-commit guard", destructive_hint = true)
    )]
    async fn uninstall_precommit_guard(
        &self,
        Parameters(args): Parameters<UninstallPrecommitGuardArgs>,
    ) -> ToolOutput {
        match self
            .mailbox
            .uninstall_precommit_guard(Path::new(&args.code_repo_path))
        {
            Ok(ok) => ToolOutput::ok(json!({ "ok": ok })),
            Err(e) => ToolOutput::err(e),
        }
    }

    /// `register_agent`: upsert on the existing agent key (or create).
    #[tool(
        name = "register_agent",
        description = "Upsert an agent identity on its existing key (or create if absent).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::RegisteredAgentOut>(),
        annotations(title = "Register agent", destructive_hint = true)
    )]
    async fn register_agent(&self, Parameters(args): Parameters<RegisterAgentArgs>) -> ToolOutput {
        let name = args.name.clone().unwrap_or_else(generate_agent_name);
        let profile = AgentProfile {
            name: name.clone(),
            program: args.program.clone(),
            model: args.model.clone(),
            task_description: args.task_description.clone(),
            persona: None,
        };
        let registered = self
            .mailbox
            .register_agent(&args.project_key, &profile)
            .await;
        if registered.is_ok()
            && let Some(alias) = args.alias.as_deref().filter(|s| !s.is_empty())
        {
            self.mailbox
                .fold_agent_name_alias(&args.project_key, alias, &name)
                .await;
        }
        ToolOutput::result(registered)
    }

    /// `create_agent_identity`: ALWAYS mint a fresh keypair.
    #[tool(
        name = "create_agent_identity",
        description = "Mint a fresh agent keypair (ncryptsec) and publish the kind-30013 profile.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::RegisteredAgentOut>(),
        annotations(title = "Create agent identity", destructive_hint = true)
    )]
    async fn create_agent_identity(
        &self,
        Parameters(args): Parameters<CreateAgentIdentityArgs>,
    ) -> ToolOutput {
        let name = args.name_hint.clone().unwrap_or_else(generate_agent_name);
        let profile = AgentProfile {
            name: name.clone(),
            program: args.program.clone(),
            model: args.model.clone(),
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
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::WhoIsOut>(),
        annotations(title = "Whois", read_only_hint = true)
    )]
    async fn whois(&self, Parameters(args): Parameters<WhoisArgs>) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .whois(&args.project_key, &args.agent_name)
                .await,
        )
    }

    /// `resolve_pane_identity`: pane id → agent name (ADR-0045).
    #[tool(
        name = "resolve_pane_identity",
        description = "Resolve a pane/terminal id to the agent name (G9).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ResolvePaneWrapper>(),
        annotations(title = "Resolve pane identity", read_only_hint = true)
    )]
    async fn resolve_pane_identity(
        &self,
        Parameters(args): Parameters<ResolvePaneIdentityArgs>,
    ) -> ToolOutput {
        let pane_id = args.pane_id.clone().unwrap_or_default();
        match toron_core::backend::plane::connect_default() {
            Ok(backend) => match self
                .mailbox
                .resolve_pane_identity(backend.as_ref(), &pane_id)
            {
                Ok(Some(name)) => ToolOutput::ok(json!({ "agent_name": name, "pane_id": pane_id })),
                Ok(None) => ToolOutput::ok(json!({ "agent_name": null, "pane_id": pane_id })),
                Err(e) => ToolOutput::err(e),
            },
            Err(e) => ToolOutput::ok(json!({
                "note": format!("pane identity requires a pane plane; not wired: {e}"),
                "pane_id": pane_id,
            })),
        }
    }

    /// `cleanup_pane_identities`: prune identities whose panes are gone.
    #[tool(
        name = "cleanup_pane_identities",
        description = "Prune identities whose hc panes are absent from the live pane list.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::CleanupWrapper>(),
        annotations(title = "Cleanup pane identities", destructive_hint = true)
    )]
    async fn cleanup_pane_identities(
        &self,
        Parameters(_args): Parameters<CleanupPaneIdentitiesArgs>,
    ) -> ToolOutput {
        match toron_core::backend::plane::connect_default() {
            Ok(backend) => {
                let pruned = self.mailbox.cleanup_pane_identities(backend.as_ref());
                match pruned {
                    Ok(names) => {
                        let reaped = self
                            .mailbox
                            .reap_ghost_leases(&names, Some("pane-pruned"))
                            .await;
                        match reaped {
                            Ok(releases) if !releases.is_empty() => ToolOutput::ok(json!({
                                "removed": names,
                                "released": releases
                                    .into_iter()
                                    .map(|r| json!({
                                        "project_slug": r.project_slug,
                                        "path_pattern": r.path_pattern,
                                        "holder": r.holder,
                                    }))
                                    .collect::<Vec<_>>(),
                            })),
                            Ok(_) => ToolOutput::ok(json!({ "removed": names })),
                            Err(e) => ToolOutput::ok(json!({
                                "removed": names,
                                "note": format!("ghost-lease reaper failed: {e:#}"),
                            })),
                        }
                    }
                    Err(e) => ToolOutput::result::<Vec<String>>(Err(e)),
                }
            }
            Err(e) => ToolOutput::ok(json!({
                "note": format!("pane cleanup requires a pane plane; not wired: {e}"),
            })),
        }
    }

    /// `list_agents` (read-only): project agents with npub + last-active.
    #[tool(
        name = "list_agents",
        description = "List a project's agents with npub + last-active provenance.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ListAgentsWrapper>(),
        annotations(title = "List agents", read_only_hint = true)
    )]
    async fn list_agents(&self, Parameters(args): Parameters<ListAgentsArgs>) -> ToolOutput {
        let opts = ListAgentsOptions {
            active_within_days: args.active_within_days.and_then(|d| u64::try_from(d).ok()),
        };
        match self.mailbox.list_agents(&args.project_key, opts).await {
            Ok(v) => ToolOutput::ok(json!({ "agents": v.to_json() })),
            Err(e) => ToolOutput::err(format!("{e:#}")),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct HealthCheckArgs {
    /// AM parity: `health_check` takes no required args. The field is kept
    /// optional (default empty) so clients that mirror the project-scoped
    /// convention still parse, but it is NOT required and NOT used — the
    /// daemon reports process-global bus + archive health.
    #[serde(default)]
    pub project_key: Option<String>,
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
    /// Harness / program (maps to 30013 content `program`). Optional for
    /// backward compat — old callers omit it (ADR-0005 P2.2 content-map).
    pub program: Option<String>,
    /// Model (maps to 30013 content `model`). Optional for backward compat
    /// (ADR-0005 P2.2 content-map).
    pub model: Option<String>,
    pub name: Option<String>,
    /// Herdr slug (or other alias) to fold onto `name` (the pin).
    pub alias: Option<String>,
    pub task_description: Option<String>,
    pub attachments_policy: Option<String>,
    pub reaper_exempt: Option<bool>,
    pub pane_id: Option<String>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct CreateAgentIdentityArgs {
    pub project_key: String,
    /// Harness / program (maps to 30013 content `program`). Optional for
    /// backward compat — old callers omit it (ADR-0005 P2.2 content-map).
    pub program: Option<String>,
    /// Model (maps to 30013 content `model`). Optional for backward compat
    /// (ADR-0005 P2.2 content-map).
    pub model: Option<String>,
    pub name_hint: Option<String>,
    pub task_description: Option<String>,
    pub attachments_policy: Option<String>,
    pub pane_id: Option<String>,
    pub registration_proof: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct WhoisArgs {
    pub project_key: String,
    /// The identity to look up: a REGISTERED adjective+noun pin, canonical
    /// name, folded alias, or npub. `name` is accepted as a serde alias
    /// (grok/other wrappers pass `name`); the field stays REQUIRED (both
    /// missing fails — never defaults to $USER or a generated pin).
    #[serde(alias = "name")]
    pub agent_name: String,
    pub include_recent_commits: Option<bool>,
    pub commit_limit: Option<i32>,
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
    pub limit: Option<i32>,
    pub active_within_days: Option<i32>,
}
