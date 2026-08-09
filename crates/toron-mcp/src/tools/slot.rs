// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! Build-slot tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: build slots (3) tools.
//! `#[tool_router(router = slot_router)]` emits `pub(crate) fn slot_router()`;
//! `tools/mod.rs` merges it into the single 38-tool surface via
//! `ToolRouter`'s `Add`. Frozen names/args/returns: ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};


use super::{ToolOutput, ToolServer, nonneg_secs};

#[tool_router(router = slot_router, vis = "pub(crate)")]
impl ToolServer {
    // ------------------------------------------------------------------
    // BUILD SLOTS (3)
    // ------------------------------------------------------------------

    /// `acquire_build_slot` (destructive): advisory slot lease.
    #[tool(
        name = "acquire_build_slot",
        description = "Acquire an advisory build-slot lease (kind-30032), optionally exclusive.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::AcquireBuildSlotResultOut>(),
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
                    nonneg_secs(args.ttl_seconds),
                    args.exclusive.unwrap_or(false),
                )
                .await,
        )
    }

    /// `renew_build_slot`: extend an active slot lease's TTL.
    #[tool(
        name = "renew_build_slot",
        description = "Extend an active build-slot lease's TTL (no-op when missing).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<Option<crate::responses::BuildSlotOut>>(),
        annotations(title = "Renew build slot", destructive_hint = true)
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
                    nonneg_secs(args.extend_seconds).unwrap_or(0),
                )
                .await,
        )
    }

    /// `release_build_slot`: release an active slot lease.
    #[tool(
        name = "release_build_slot",
        description = "Release an active build-slot lease (non-destructive).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<bool>(),
        annotations(title = "Release build slot", destructive_hint = true)
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

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

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
