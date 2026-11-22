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
use rmcp::schemars::{self, JsonSchema};
use rmcp::{tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ToolOutput, ToolServer, nonneg_secs};

#[tool_router(router = reservation_router, vis = "pub(crate)")]
impl ToolServer {
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
        if let Some(from) = args.transfer_from.as_deref().filter(|s| !s.is_empty()) {
            let Some(path) = args.paths.first() else {
                return ToolOutput::err("transfer via file_reservation_paths needs paths[0]");
            };
            return ToolOutput::result(
                self.mailbox
                    .transfer_reservation(
                        &args.project_key,
                        from,
                        &args.agent_name,
                        path,
                        args.reason.as_deref(),
                    )
                    .await,
            );
        }
        ToolOutput::result(if args.queue {
            self.mailbox
                .file_reservation_paths_queued(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths,
                    nonneg_secs(args.ttl_seconds),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                    args.repo_root.as_deref(),
                )
                .await
        } else {
            self.mailbox
                .file_reservation_paths_with_root(
                    &args.project_key,
                    &args.agent_name,
                    &args.paths,
                    nonneg_secs(args.ttl_seconds),
                    args.exclusive.unwrap_or(false),
                    args.reason.as_deref(),
                    args.repo_root.as_deref(),
                )
                .await
        })
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

    /// `release_file_reservations_by_reason` (additive, bd-h3oj — NOT part
    /// of the AM-Rust parity 38): operator-signed revocation of every live
    /// lease whose reason tag is the bead id. The flywheel after-close hook
    /// calls this so a closed bead's locks never linger to TTL. `released
    /// == 0` is a clean no-op.
    #[tool(
        name = "release_file_reservations_by_reason",
        description = "Release all file reservations whose reason is the given bead id (operator-signed; release-on-bead-close). REQUIRED: project_key, agent_name, bead_id. Operator/loop after-close hook; crews release their own locks via release_file_reservations (by agent_name) or CLI: toron reserve release --project <p> --as <pin>. Operator CLI door: toron reserve release-by-reason <bead> --project <p> --as <operator>.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<crate::responses::ReleaseResultOut>(),
        annotations(title = "Release reservations by reason", destructive_hint = true)
    )]
    async fn release_file_reservations_by_reason(
        &self,
        Parameters(args): Parameters<ReleaseFileReservationsByReasonArgs>,
    ) -> ToolOutput {
        ToolOutput::result(
            self.mailbox
                .release_by_reason(
                    &args.project_key,
                    &args.agent_name,
                    &args.bead_id,
                    args.notify_previous.unwrap_or(true),
                )
                .await,
        )
    }

    /// `list_file_reservations` (additive read, bd-locks-list-daemon-xetz —
    /// NOT part of the AM-Rust parity 38): the daemon-side lease board. The
    /// strict guard already evaluates this state server-side; without a read
    /// tool the remote seam was blind (flywheel `mail locks list` errored).
    /// Read-only: never registers or publishes.
    #[tool(
        name = "list_file_reservations",
        description = "List every active (held) file reservation in the project — the lock board the strict guard evaluates: id, path glob, holder, exclusive, reason, expiry. Optional filters: holder (exact agent name), path (glob overlap). REQUIRED: project_key.",
        output_schema = rmcp::handler::server::tool::schema_for_output::<LeaseBoardOut>(),
        annotations(title = "List file reservations", read_only_hint = true)
    )]
    async fn list_file_reservations(
        &self,
        Parameters(args): Parameters<ListFileReservationsArgs>,
    ) -> ToolOutput {
        let rows = match self
            .mailbox
            .list_file_reservations(
                &args.project_key,
                args.holder.as_deref(),
                args.path.as_deref(),
            )
            .await
        {
            Ok(rows) => rows,
            Err(e) => return ToolOutput::result::<()>(Err(e)),
        };
        let mut reservations: Vec<LeaseRowOut> = Vec::with_capacity(rows.len());
        for r in rows {
            let holder_live = self
                .mailbox
                .holder_live(&args.project_key, &r.agent_pubkey)
                .await;
            reservations.push(LeaseRowOut {
                id: r.id,
                path: r.path_pattern,
                holder: r.agent_name,
                exclusive: r.exclusive,
                reason: r.reason,
                expires_ts_us: r.expires_ts_us,
                holder_live,
            });
        }
        let waiters = self
            .mailbox
            .list_project_waiters(&args.project_key)
            .await
            .unwrap_or_default();
        ToolOutput::result(Ok(LeaseBoardOut {
            project_key: args.project_key,
            count: reservations.len() as i64,
            reservations,
            waitlists: waitlist_rows(&waiters),
        }))
    }
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
    /// When set, this call is a `transfer_reservation` (`agent_name` = to).
    pub transfer_from: Option<String>,
    /// On a hard conflict, join the FIFO waitlist and ask the holder once.
    #[serde(default)]
    pub queue: bool,
    /// Canonical git toplevel of the repo these paths belong to.
    pub repo_root: Option<String>,
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
    /// the active lease on this path itself — the caller does NOT need the
    /// holder's event id.
    pub path: String,
    /// Optional holder name to disambiguate when multiple holders match the
    /// path. Omit to revoke the (single) matching lease.
    pub holder: Option<String>,
    pub note: Option<String>,
    pub notify_previous: Option<bool>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone)]
pub struct ReleaseFileReservationsByReasonArgs {
    pub project_key: String,
    /// Requestor (operator-facing name; used in the holder notification DM).
    pub agent_name: String,
    /// Bead id whose matching (reason == bead id) leases get revoked.
    pub bead_id: String,
    pub notify_previous: Option<bool>,
}

/// `list_file_reservations` args (additive read).
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ListFileReservationsArgs {
    pub project_key: String,
    /// exact holder filter (agent name)
    pub holder: Option<String>,
    /// glob-overlap filter (show holders of one path)
    pub path: Option<String>,
}

/// One lease row (`list_file_reservations`).
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct LeaseRowOut {
    pub id: String,
    pub path: String,
    pub holder: String,
    pub exclusive: bool,
    pub reason: String,
    pub expires_ts_us: Option<i64>,
    /// False when the holder has no live signed presence.
    pub holder_live: bool,
}

/// `list_file_reservations`.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct LeaseBoardOut {
    pub project_key: String,
    pub count: i64,
    pub reservations: Vec<LeaseRowOut>,
    /// FIFO waiters by path, shown by `reserve list`.
    #[serde(default)]
    pub waitlists: Vec<ReservationWaitlistOut>,
}

/// One path's FIFO waiters.
#[derive(Debug, Clone, JsonSchema, Serialize, Deserialize)]
pub struct ReservationWaitlistOut {
    pub path: String,
    pub waiters: Vec<String>,
}

/// Group the project's waiters by path, preserving arrival order within each.
#[must_use]
pub fn waitlist_rows(
    waiters: &[toron_core::mailbox::reservation_waitlist::Waiter],
) -> Vec<ReservationWaitlistOut> {
    use std::collections::BTreeMap;
    let mut rows: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for w in waiters {
        rows.entry(&w.path_pattern)
            .or_default()
            .push(w.agent_name.clone());
    }
    rows.into_iter()
        .map(|(path, waiters)| ReservationWaitlistOut {
            path: path.to_string(),
            waiters,
        })
        .collect()
}

impl crate::tools::ToJson for LeaseBoardOut {
    fn to_json(&self) -> Value {
        json!({
            "project_key": self.project_key,
            "count": self.count,
            "reservations": self.reservations.iter().map(|r| json!({
                "id": r.id,
                "path": r.path,
                "holder": r.holder,
                "exclusive": r.exclusive,
                "reason": r.reason,
                "expires_ts_us": r.expires_ts_us,
                "holder_live": r.holder_live,
            })).collect::<Vec<_>>(),
            "waitlists": self.waitlists.iter().map(|w| json!({
                "path": w.path,
                "waiters": w.waiters,
            })).collect::<Vec<_>>(),
        })
    }
}
