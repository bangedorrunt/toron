// governed-by: ADR-0003 (rev7 L31, L33 — SEP-2106 structured outputs)
#![forbid(unsafe_code)]

//! Schemars-derived output schemas for the 38 AM-Rust parity tools.
//!
//! Each struct mirrors the exact wire shape the tool returns (the `to_json`
//! impls in `tools.rs`). They exist ONLY to feed `outputSchema` on the tool
//! definitions — clients (incl. pi's adapter) parse tool results into typed
//! shapes without heuristics, closing the bd-96a probe-divergence gap at the
//! schema layer. None of these are used for actual (de)serialization.

use rmcp::schemars;
use schemars::JsonSchema;

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
/// (happy path) or `{note}` (no herdr backend wired).
#[derive(Debug, Clone, JsonSchema)]
#[schemars(untagged)]
pub enum CleanupPaneIdentitiesOut {
    Removed(Vec<String>),
    Note { note: String },
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
    pub count: usize,
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
}

/// `acknowledge_message` / `mark_message_read`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ReceiptResultOut {
    pub message_id: String,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
}

/// `request_contact` / `respond_contact` / `list_contacts` rows.
#[derive(Debug, Clone, JsonSchema)]
pub struct ContactLinkOut {
    pub project_slug: String,
    pub from_name: String,
    pub from_pubkey: String,
    pub to_name: String,
    pub to_pubkey: String,
    pub to_project: Option<String>,
    pub status: String,
    pub created_ts_us: i64,
}

/// `check_file_reservation_conflicts`.
#[derive(Debug, Clone, JsonSchema)]
pub struct PathConflictOut {
    pub path: String,
    pub holders: Vec<String>,
}

#[derive(Debug, Clone, JsonSchema)]
pub struct ConflictReportOut {
    pub conflict_free: bool,
    pub conflicts: Vec<PathConflictOut>,
    pub clear_paths: Vec<String>,
    pub checked_paths: usize,
    pub total_conflicting_reservations: usize,
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
}

/// `renew_file_reservations`.
#[derive(Debug, Clone, JsonSchema)]
pub struct RenewResultOut {
    pub renewed: usize,
    pub reservations: Vec<GrantedReservationOut>,
}

/// `release_file_reservations`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ReleaseResultOut {
    pub released: usize,
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
}

/// `summarize_thread` / `summarize_thread_product`.
#[derive(Debug, Clone, JsonSchema)]
pub struct ThreadSummaryOut {
    pub thread_id: String,
    pub participants: Vec<String>,
    pub key_points: Vec<String>,
    pub action_items: Vec<String>,
    pub message_count: usize,
}

/// `macro_start_session`.
#[derive(Debug, Clone, JsonSchema)]
pub struct StartSessionResultOut {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub granted_reservations: Vec<GrantedReservationOut>,
    pub inbox_count: usize,
}

/// `macro_prepare_thread`.
#[derive(Debug, Clone, JsonSchema)]
pub struct PrepareThreadResultOut {
    pub project_key: String,
    pub agent_name: String,
    pub npub: String,
    pub thread_id: String,
    pub summary: ThreadSummaryOut,
    pub inbox_count: usize,
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
    pub released: usize,
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
