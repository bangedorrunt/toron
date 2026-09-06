// governed-by: ADR-0003 (rev7 L31, L33 — SEP-2106 structured outputs)
#![forbid(unsafe_code)]

//! Schemars-derived output schemas for the 38 AM-Rust parity tools.
//!
//! Each struct mirrors the exact wire shape the tool returns (the `to_json`
//! impls in `tools.rs`). They exist ONLY to feed `outputSchema` on the tool
//! definitions — clients (incl. pi's adapter) parse tool results into typed
//! shapes without heuristics, closing the bd-96a probe-divergence gap at the
//! schema layer. None of these are used for actual (de)serialization.
//
//! Counts are `i64` (never `usize`): schemars maps unsigned ints to
//! `uint*` formats unknown to MCP clients, while `i64` emits `int64`.

// All schemas derive from the schemars lineage rmcp re-exports (1.x) —
// there is deliberately NO direct `schemars` dependency (a split lineage
// is how `uint*` formats leaked onto the wire before). The module alias
// stays: the derive macro expands to code referencing `schemars::...`.
use rmcp::schemars;
use rmcp::schemars::JsonSchema;

/// `health_check`: `{bus, archive}` each `{ok, message}`.
#[derive(Debug, Clone, JsonSchema)]
pub struct HealthComponentOut {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct HealthReportOut {
    pub bus: HealthComponentOut,
    pub archive: HealthComponentOut,
}

/// `ensure_project`: `{slug, manifest_event_id, created}`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ProjectStateOut {
    pub slug: String,
    pub manifest_event_id: String,
    pub created: bool,
}

/// `install_precommit_guard`: `{hook_path}`.
#[derive(Debug, Clone, JsonSchema)]
pub struct HookPathOut {
    pub hook_path: String,
}

/// `force_release_file_reservation`: `{ok: true}`.
#[derive(Debug, Clone, JsonSchema)]
pub struct OkOut {
    pub ok: bool,
}

/// `cleanup_pane_identities`: bare `Vec<String>` of pruned pane identities
/// (happy path) or `{note}` (no herdr backend wired). With the bd-gc7k
/// cascade, pruned pins also report the ghost leases released on their
/// behalf (`removed` + `released`).
#[derive(Debug, Clone, JsonSchema)]
#[schemars(untagged)]
pub enum CleanupPaneIdentitiesOut {
    Removed(Vec<String>),
    RemovedWithReleases {
        removed: Vec<String>,
        released: Vec<ReleasedGhostOut>,
    },
    Note {
        note: String,
    },
}

/// One exclusive lease revoked because its holder pin was pruned
/// (bd-gc7k: a dead pin must not own the ledger).
#[derive(Debug, Clone, JsonSchema)]
pub struct ReleasedGhostOut {
    pub project_slug: String,
    pub path_pattern: String,
    pub holder: String,
}

/// `register_agent` / `create_agent_identity` / `list_agents` rows.
#[derive(Debug, Clone, JsonSchema)]
pub struct RegisteredAgentOut {
    pub name: String,
    pub project_slug: String,
    pub npub: String,
    pub profile_event_id: String,
    pub key_path: String,
    pub created: bool,
}

/// `whois`: `{name, npub, program, model, task_description, last_active_ts_us}`.
#[derive(Debug, Clone, JsonSchema)]
pub struct WhoIsOut {
    pub name: String,
    pub npub: String,
    pub program: Option<String>,
    pub model: Option<String>,
    pub task_description: Option<String>,
    pub last_active_ts_us: i64,
}

/// `resolve_pane_identity`: `{agent_name, pane_id}` (name found or not) or
/// `{note, pane_id}` (no herdr backend).
#[derive(Debug, Clone, JsonSchema)]
#[schemars(untagged)]
pub enum PaneIdentityOut {
    Resolved {
        agent_name: Option<String>,
        pane_id: String,
    },
    Note {
        note: String,
        pane_id: String,
    },
}

/// `send_message` / `reply_message`.
#[derive(Debug, Clone, JsonSchema)]
pub struct DeliveryPayloadOut {
    pub recipient: String,
    pub event_id: String,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct DeliveryOut {
    pub project: String,
    pub payload: DeliveryPayloadOut,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct SendResultOut {
    pub deliveries: Vec<DeliveryOut>,
    pub count: i64,
    pub verified_sender: bool,
}

/// `fetch_inbox` rows.
#[derive(Debug, Clone, JsonSchema)]
pub struct InboxMessageOut {
    pub id: String,
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub created_ts_us: i64,
    /// Read receipt timestamp (i64-µs) from the `message_reads` projection;
    /// `null` = unread (bd-5icg).
    pub read_ts_us: Option<i64>,
    /// S5 (bd-dup-assign-collapse-ws21): the first id of the duplicate group
    /// this row survives for; `null` = standalone message.
    pub duplicate_of: Option<String>,
    /// ADR-0027 E2 assignment tags projected from the rumor (never 1059);
    /// mirror of `ToJson for InboxMessage` wire fields
    /// (bd-mcp-inbox-assignment-tags-sdi1). All optional, `null` = untasked.
    pub corr: Option<String>,
    pub wake: Option<String>,
    pub priority: Option<String>,
    pub audience: Option<String>,
    pub result_path: Option<String>,
    /// HumanDecision live producer: `verdict=` extra_tag on the rumor.
    pub verdict: Option<String>,
}

/// `acknowledge_message` / `mark_message_read`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ReceiptResultOut {
    pub message_id: String,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
}

/// `request_contact` / `respond_contact` / `list_contacts` rows.
///
/// Link rows describe the SOCIAL GRAPH — they do not gate mail. Inbound
/// delivery is gated by the recipient's contact policy (kind-30014, default
/// `auto`): a `pending` link still receives mail unless the recipient's
/// policy blocks it.
#[derive(Debug, Clone, JsonSchema)]
pub struct ContactLinkOut {
    pub project_slug: String,
    pub from_name: String,
    pub from_pubkey: String,
    pub to_name: String,
    pub to_pubkey: String,
    pub to_project: Option<String>,
    /// pending | approved | denied — social graph state, not a delivery gate.
    pub status: String,
    pub created_ts_us: i64,
}

/// `check_file_reservation_conflicts`.
#[derive(Debug, Clone, JsonSchema)]
pub struct PathConflictOut {
    pub path: String,
    pub holders: Vec<String>,
    /// Kind-30031 event id of each conflicting lease (bd-dsu1) — parallel to
    /// `holders`, one id per holder. The operator lock board surfaces these
    /// for diagnostics (force-release, request-release, replay).
    pub reservation_ids: Vec<String>,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct ConflictReportOut {
    pub conflict_free: bool,
    pub conflicts: Vec<PathConflictOut>,
    pub clear_paths: Vec<String>,
    pub checked_paths: i64,
    pub total_conflicting_reservations: i64,
    /// ADR-0011 Spec 4: additive soft tier (in-TTL intents).
    pub soft_conflicts: Vec<PathConflictOut>,
    pub has_soft_conflicts: bool,
    /// bd-slot-visibility-gv17: ACTIVE build-slot leases held by others —
    /// "is the cargo fence free?" in the same one-call pre-write check.
    pub slot_holders: Vec<SlotHolderOut>,
}

/// One active build-slot lease (bd-slot-visibility-gv17).
#[derive(Debug, Clone, JsonSchema)]
pub struct SlotHolderOut {
    pub slot: String,
    pub agent_name: String,
    pub exclusive: bool,
    pub expires_ts_us: Option<i64>,
}

/// `file_reservation_paths`.
#[derive(Debug, Clone, JsonSchema)]
pub struct GrantedReservationOut {
    pub id: String,
    pub path_pattern: String,
    pub exclusive: bool,
    pub reason: String,
    pub expires_ts_us: Option<i64>,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct AcquireResultOut {
    pub granted: Vec<GrantedReservationOut>,
    pub conflicts: Vec<PathConflictOut>,
    /// ADR-0011 Spec 4: additive soft tier (frozen-acquire paths).
    pub soft_conflicts: Vec<PathConflictOut>,
}

/// `renew_file_reservations`.
#[derive(Debug, Clone, JsonSchema)]
pub struct RenewResultOut {
    pub renewed: i64,
    pub reservations: Vec<GrantedReservationOut>,
}

/// `release_file_reservations` per-lease breakdown (S6).
#[derive(Debug, Clone, JsonSchema)]
pub struct ReleasedLeaseOut {
    pub path: String,
    pub mode: String,
    pub holder: String,
}

/// `release_file_reservations`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ReleaseResultOut {
    pub released: i64,
    pub leases: Vec<ReleasedLeaseOut>,
}

/// `search_messages` rows.
#[derive(Debug, Clone, JsonSchema)]
pub struct SearchMessageOut {
    pub id: String,
    pub subject: String,
    pub from: String,
    pub created_ts_us: i64,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub body_md: Option<String>,
}

/// `search_messages_product` rows (`(project, SearchMessage)` flatten).
#[derive(Debug, Clone, JsonSchema)]
pub struct ProjectSearchMessageOut {
    pub project: String,
    pub id: String,
    pub subject: String,
    pub from: String,
    pub created_ts_us: i64,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub body_md: Option<String>,
}

/// `fetch_inbox_product` rows (`(project, InboxMessage)` flatten).
#[derive(Debug, Clone, JsonSchema)]
pub struct ProjectInboxMessageOut {
    pub project: String,
    pub id: String,
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub created_ts_us: i64,
    /// ADR-0027 E2 assignment tags (bd-mcp-inbox-assignment-tags-sdi1);
    /// flattened mirror of `InboxMessageOut` tags. Optional, `null` = untasked.
    pub corr: Option<String>,
    pub wake: Option<String>,
    pub priority: Option<String>,
    pub audience: Option<String>,
    pub result_path: Option<String>,
    pub verdict: Option<String>,
}

/// `summarize_thread` / `summarize_thread_product`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ThreadSummaryOut {
    pub thread_id: String,
    pub participants: Vec<String>,
    pub key_points: Vec<String>,
    pub action_items: Vec<String>,
    pub message_count: i64,
}

/// `macro_start_session`.
#[derive(Debug, Clone, JsonSchema)]
pub struct StartSessionResultOut {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub granted_reservations: Vec<GrantedReservationOut>,
    pub inbox_count: i64,
}

/// `macro_prepare_thread`.
#[derive(Debug, Clone, JsonSchema)]
pub struct PrepareThreadResultOut {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub thread_id: String,
    pub summary: ThreadSummaryOut,
    pub inbox_count: i64,
}

/// `macro_contact_handshake`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ContactHandshakeResultOut {
    pub requester: String,
    pub target: String,
    pub link: Option<ContactLinkOut>,
    pub welcome_sent: bool,
}

/// `macro_file_reservation_cycle`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ReservationCycleResultOut {
    pub project_key: String,
    pub agent_name: String,
    pub granted: Vec<GrantedReservationOut>,
    pub released: i64,
}

/// `ensure_product` / `products_link`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ProductInfoOut {
    pub product_key: String,
    pub name: String,
    pub member_projects: Vec<String>,
    pub created: bool,
}

/// `acquire_build_slot`.
#[derive(Debug, Clone, JsonSchema)]
pub struct GrantedSlotOut {
    pub slot: String,
    pub exclusive: bool,
    pub expires_ts_us: Option<i64>,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct SlotConflictOut {
    pub slot: String,
    pub holders: Vec<String>,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct AcquireBuildSlotResultOut {
    pub granted: Vec<GrantedSlotOut>,
    pub conflicts: Vec<SlotConflictOut>,
}

/// MCP object-type wrappers — pi's tools/list validator requires every
/// outputSchema to have top-level `type: "object"` (2026-08-08 regression
/// bd-jgd). Bare arrays / primitives / anyOf used before fail that gate.
/// Wrappers produce the compliant schema via `schema_for_output::<Wrapper>()`
/// AND the wire payload (`ToolOutput` JSON text) is wrapped to match —
/// honest structured output (SEP-2106).
#[derive(Debug, Clone, JsonSchema)]
pub struct FetchInboxWrapper {
    pub messages: Vec<InboxMessageOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct FetchInboxProductWrapper {
    pub messages: Vec<ProjectInboxMessageOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct ListAgentsWrapper {
    pub agents: Vec<RegisteredAgentOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct ListContactsWrapper {
    pub contacts: Vec<ContactLinkOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct SearchMessagesWrapper {
    pub result: Vec<SearchMessageOut>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct SearchMessagesProductWrapper {
    pub result: Vec<ProjectSearchMessageOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct UninstallHookWrapper {
    pub ok: bool,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct ReleaseBuildSlotWrapper {
    pub released: bool,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct RenewBuildSlotWrapper {
    pub slot: Option<BuildSlotOut>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct ResolvePaneWrapper {
    pub agent_name: Option<String>,
    pub pane_id: String,
    pub note: Option<String>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct CleanupWrapper {
    pub removed: Option<Vec<String>>,
    pub released: Option<Vec<ReleasedGhostOut>>,
    pub note: Option<String>,
}
#[derive(Debug, Clone, JsonSchema)]
pub struct SetContactPolicyWrapper {
    pub event_id: String,
}

/// `renew_build_slot` (null when no lease).
#[derive(Debug, Clone, JsonSchema)]
pub struct BuildSlotOut {
    pub slot: String,
    pub agent_name: String,
    pub agent_pubkey: String,
    pub exclusive: bool,
    pub status: String,
    pub expires_ts_us: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bd-mcp-inbox-assignment-tags-sdi1 (RED): schema must list the optional
    /// assignment tags the wire already emits via `ToJson for InboxMessage`.
    #[test]
    fn inbox_message_out_schema_lists_assignment_tags() {
        let schema = rmcp::schemars::schema_for!(InboxMessageOut);
        let json = serde_json::to_value(&schema).expect("schema serializes");
        let props = json
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("InboxMessageOut schema has properties");
        for tag in [
            "corr",
            "wake",
            "priority",
            "audience",
            "result_path",
            "verdict",
        ] {
            assert!(
                props.contains_key(tag),
                "InboxMessageOut schema missing optional assignment tag `{tag}`"
            );
        }
        let project_schema = rmcp::schemars::schema_for!(ProjectInboxMessageOut);
        let project_json =
            serde_json::to_value(&project_schema).expect("project schema serializes");
        let project_props = project_json
            .get("properties")
            .and_then(|v| v.as_object())
            .expect("ProjectInboxMessageOut schema has properties");
        for tag in [
            "corr",
            "wake",
            "priority",
            "audience",
            "result_path",
            "verdict",
        ] {
            assert!(
                project_props.contains_key(tag),
                "ProjectInboxMessageOut schema missing optional assignment tag `{tag}`"
            );
        }
    }
}
