// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! File-reservation tools (F21.TOOLS-SPLIT domain module).
//!
//! Move-only split of the former `tools.rs` monolith: file reservations (5)
//! tools. `#[tool_router(router = reservation_router)]` emits
//! `pub(crate) fn reservation_router()`; `tools/mod.rs` merges it into the
//! single 38-tool surface via `ToolRouter`'s `Add`. Frozen names/args/returns:
//! ZERO change (ADR-0007 C3).

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::json;


use super::{ToolOutput, ToolServer, nonneg_secs};

#[tool_router(router = reservation_router, vis = "pub(crate)")]
impl ToolServer {
    // ------------------------------------------------------------------
    // FILE RESERVATIONS (5)
    // ------------------------------------------------------------------

    /// `check_file_reservation_conflicts` (read-only): symmetric-glob report.
    #[tool(
        name = "check_file_reservation_conflicts",
        description = "Symmetric-glob conflict report for paths vs other agents' reservations.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ConflictReportOut>(),
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
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::AcquireResultOut>(),
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
                    nonneg_secs(args.ttl_seconds),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                )
                .await,
        )
    }

    /// `renew_file_reservations`: extend the holder's active lease TTLs.
    #[tool(
        name = "renew_file_reservations",
        description = "Extend the TTLs of the holder's active file reservations.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::RenewResultOut>(),
        annotations(title = "Renew file reservations", destructive_hint = true)
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
                    nonneg_secs(args.extend_seconds).unwrap_or(0),
                )
                .await,
        )
    }

    /// `release_file_reservations`: release the holder's leases.
    #[tool(
        name = "release_file_reservations",
        description = "Release the holder's active file reservations (idempotent).",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ReleaseResultOut>(),
        annotations(title = "Release file reservations", destructive_hint = true)
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
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::OkOut>(),
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
                &args.path,
                args.holder.as_deref(),
                args.note.as_deref(),
                args.notify_previous.unwrap_or(false),
            )
            .await
        {
            Ok(()) => ToolOutput::ok(json!({ "ok": true })),
            Err(e) => ToolOutput::err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// WireArgs — AM-Rust arg parity (field names + Option-ness), one per tool.
// ---------------------------------------------------------------------------

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
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ReleaseFileReservationsArgs {
    pub project_key: String,
    pub agent_name: String,
    pub paths: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ForceReleaseFileReservationArgs {
    pub project_key: String,
    pub agent_name: String,
    /// Path (or glob) of the stale reservation to revoke. The daemon resolves
    /// the active lease on this path itself (bd-cumj) — the caller does NOT
    /// need the holder's event id.
    pub path: String,
    /// Optional holder name to disambiguate when multiple holders match the
    /// path. Omit to revoke the (single) matching lease.
    pub holder: Option<String>,
    pub note: Option<String>,
    pub notify_previous: Option<bool>,
}
