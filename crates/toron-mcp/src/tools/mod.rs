// governed-by: ADR-0003
// governed-by: ADR-0007 (C3 frozen tool names — byte-identical surface)

//! AM-Rust parity tool surface (bd-led.7.2): the 38 mailbox tools as rmcp
//! `#[tool]` methods on [`ToolServer`].
//!
//! Parity-by-construction: every WireArgs field uses the exact AM-Rust arg
//! name and every `#[tool(name = ...)]` is the exact AM-Rust tool name.
//!
//! Wire: [`ToolServer`] wraps `Arc<toron_core::mailbox::Mailbox>` and exposes
//! the router via `#[tool_router]` + a hand-written `#[tool_handler]` impl so
//! `get_info` can mirror `ToronServer` (name "toron", 2026-07-28).
//!
//! **Layout (F21.TOOLS-SPLIT, bd-p4-refactor-has5.2):** the 38 tool bodies +
//! their `*Args` structs live in per-domain modules (`system`, `mail`,
//! `reservation`, `product`, `contact`, `slot`, `workflow`); each exposes its
//! own `#[tool_router]`-generated `*_router()` and this module merges them via
//! `ToolRouter`'s `Add` (rmcp-3.1.1). Move-only — tool names/args/returns are
//! byte-identical (ADR-0007 C3).

pub mod contact;
pub mod mail;
pub mod product;
pub mod reservation;
pub mod slot;
pub mod system;
pub mod workflow;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::IntoCallToolResult;
use rmcp::model::{
    CallToolResponse, CallToolResult, ContentBlock, Implementation, InitializeResult,
    ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::{ServerHandler, tool_handler};
use serde_json::{Value, json};

use toron_core::index::slots::{BuildSlot, GrantedSlot, SlotConflict, SlotStatus};
use toron_core::mailbox::{
    AcquireBuildSlotResult, AcquireResult, ConflictReport, ContactHandshakeResult, ContactLink,
    Delivery, DeliveryPayload, GrantedReservation, HealthComponent, HealthReport, InboxMessage,
    Mailbox, PathConflict, PrepareThreadResult, ProductInfo, ProjectState, ReceiptResult,
    RegisteredAgent, ReleaseResult, RenewResult, ReservationCycleResult, SearchMessage, SendResult,
    StartSessionResult, ThreadSummary, WhoIs,
};

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
            "read_ts_us": self.read_ts_us,
            // S5 (bd-dup-assign-collapse-ws21): duplicate-group first id.
            "duplicate_of": self.duplicate_of,
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
        json!({
            "path": self.path,
            "holders": self.holders,
            // bd-dsu1: the reservation ids ride through every conflict
            // surface (acquire + report) for operator diagnostics.
            "reservation_ids": self.reservation_ids,
        })
    }
}

impl ToJson for AcquireResult {
    fn to_json(&self) -> Value {
        json!({
            "granted": self.granted.to_json(),
            "conflicts": self.conflicts.to_json(),
            // ADR-0011 Spec 4: additive soft tier (frozen-acquire paths).
            "soft_conflicts": self.soft_conflicts.to_json(),
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
            // ADR-0011 Spec 4: additive soft tier (in-TTL intents).
            "soft_conflicts": self.soft_conflicts.to_json(),
            "has_soft_conflicts": self.has_soft_conflicts,
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
        json!({
            "released": self.released,
            "leases": self.leases.iter().map(|l| json!({
                "path": l.path,
                "mode": l.mode,
                "holder": l.holder,
            })).collect::<Vec<_>>(),
        })
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

impl ToolServer {
    /// The assembled 38-tool router (F21.TOOLS-SPLIT): each domain module
    /// contributes its `#[tool_router]`-generated router; `Add` merges them
    /// into the single surface. Tool names/args/returns are byte-identical
    /// to the pre-split monolith (ADR-0007 C3).
    pub fn tool_router() -> ToolRouter<Self> {
        Self::system_router()
            + Self::mail_router()
            + Self::reservation_router()
            + Self::product_router()
            + Self::contact_router()
            + Self::slot_router()
            + Self::workflow_router()
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

/// Deterministic adjective+noun fallback for tools that do not require an
/// explicit agent name (AM-Rust naming rule: never descriptive-role names).
pub(crate) fn generate_agent_name() -> String {
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

/// Coerce an i64 AM-parity arg to u64 WITHOUT negative wrap (P2
/// bd-p2-unwrap-or-zero-coercion-5ac3): `-1 as u64` = ~1.8e19 seconds — an
/// astronomical lease/extend. Negative → `None` (default TTL / no extension).
pub(crate) fn nonneg_secs(v: Option<i64>) -> Option<u64> {
    v.and_then(|s| u64::try_from(s).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonneg_secs_rejects_negative_wrap() {
        // P2 (bd-p2-unwrap-or-zero-coercion-5ac3): a negative i64 TTL/extend
        // coerced with `as u64` wraps to ~1.8e19 seconds — an astronomical
        // lease. Negative must become None (default TTL / no extension).
        assert_eq!(nonneg_secs(Some(-1)), None);
        assert_eq!(nonneg_secs(Some(-86_400)), None);
        assert_eq!(nonneg_secs(None), None);
        assert_eq!(nonneg_secs(Some(0)), Some(0));
        assert_eq!(nonneg_secs(Some(3_600)), Some(3_600));
    }

    #[test]
    fn generated_agent_name_is_adjective_noun() {
        let a = generate_agent_name();
        let b = generate_agent_name();
        assert_ne!(a, b, "sequential names differ");
        assert!(a.chars().count() > 3);
    }

    /// Every tool must declare an outputSchema (ADR-0003 rev7 SEP-2106).
    /// This is the acceptance gate for bd-jgd: 38/38 parity tools carry a
    /// schemars-derived `output_schema` on their tool definition. The count
    /// is 39 since bd-h3oj: 38 AM-Rust parity tools + the additive
    /// `release_file_reservations_by_reason` (release-on-bead-close) — the
    /// parity names/args/returns stay byte-identical (ADR-0007 C3).
    #[test]
    fn all_tools_declare_output_schema() {
        let tools = ToolServer::tool_router().list_all();
        assert!(!tools.is_empty(), "router must enumerate the tools");
        let missing: Vec<String> = tools
            .iter()
            .filter(|t| t.output_schema.is_none())
            .map(|t| t.name.to_string())
            .collect();
        assert!(
            missing.is_empty(),
            "tools missing outputSchema ({}): {}",
            missing.len(),
            missing.join(", ")
        );
        assert_eq!(
            tools.len(),
            39,
            "38 AM-Rust parity tools + release_file_reservations_by_reason (bd-h3oj)"
        );
    }

    /// bd-u612: the three crew-facing mail/release tools must carry REQUIRED
    /// arg shapes in their descriptions. SEP-2567 stateless HTTP has no
    /// per-session identity, so `sender_name`-required is BY DESIGN
    /// (AM parity) — agents read descriptions BEFORE calling and were
    /// guessing arg shapes (live schema-validation failures 2026-08-16).
    /// Guard the tokens so a future description edit cannot silently drop
    /// them again.
    #[test]
    fn crew_facing_tools_carry_required_arg_shapes() {
        let tools = ToolServer::tool_router().list_all();
        let desc = |name: &str| {
            tools
                .iter()
                .find(|t| t.name.as_ref() == name)
                .unwrap_or_else(|| panic!("tool {name} missing from surface"))
                .description
                .clone()
                .unwrap_or_default()
                .to_string()
        };
        // send_message: REQUIRED token lists the mandatory args + the shell
        // escape hatch (agents with a shell should prefer `flywheel mail`).
        let send = desc("send_message");
        for token in ["REQUIRED: project_key, sender_name", "flywheel mail send"] {
            assert!(
                send.contains(token),
                "send_message description missing {token:?}: {send}"
            );
        }
        // reply_message: same REQUIRED + thread context comes from args
        // (no auto-parent resolution on toron; the flywheel facade resolves).
        let reply = desc("reply_message");
        for token in [
            "REQUIRED: project_key, sender_name",
            "thread_id",
            "auto-parent",
        ] {
            assert!(
                reply.contains(token),
                "reply_message description missing {token:?}: {reply}"
            );
        }
        // release_file_reservations_by_reason: REQUIRED + the crew-visible
        // release path (their own locks go via release_file_reservations /
        // the CLI, not this operator hook).
        let release = desc("release_file_reservations_by_reason");
        for token in [
            "REQUIRED: project_key, agent_name, bead_id",
            "release_file_reservations",
            "toron reserve release",
        ] {
            assert!(
                release.contains(token),
                "release_file_reservations_by_reason description missing {token:?}: {release}"
            );
        }
    }

    /// S4 / bd-p2-id-type-end-to-end-v9dy: the toron MCP surface must be
    /// all-String for id-bearing fields (hex event ids; ADR-0007 D1 — never
    /// u64 in shared request/response types). Walk every tool's live
    /// inputSchema and reject any property whose name mentions `id` that is
    /// integer/number typed (directly or as array items). RED today:
    /// `renew_file_reservations` / `release_file_reservations` still carry
    /// the dead AM-parity `file_reservation_ids: Option<Vec<i64>>` selector.
    #[test]
    fn no_integer_id_fields_in_tool_surface() {
        let tools = ToolServer::tool_router().list_all();
        let mut violations: Vec<String> = Vec::new();
        for tool in tools.iter() {
            let Some(props) = tool
                .input_schema
                .get("properties")
                .and_then(|v| v.as_object())
            else {
                continue;
            };
            for (name, prop) in props {
                if !name.contains("id") {
                    continue;
                }
                if let Some(t) = prop.get("type").and_then(|v| v.as_str())
                    && (t == "integer" || t == "number")
                {
                    violations.push(format!("{}.{}: {t}", tool.name, name));
                }
                if let Some(items) = prop
                    .get("items")
                    .and_then(|v| v.get("type"))
                    .and_then(|v| v.as_str())
                    && (items == "integer" || items == "number")
                {
                    violations.push(format!("{}.{}: array of {items}", tool.name, name));
                }
            }
        }
        assert!(
            violations.is_empty(),
            "id-bearing tool surface fields must be String (ADR-0007 D1); found {}: {}",
            violations.len(),
            violations.join(", ")
        );
    }

    /// Spot-check the schemas encode the toron-side shapes (bd-96a section 2),
    /// not a generic `{}` — the schema must name the divergence-critical fields.
    #[test]
    fn output_schemas_encode_toron_shapes() {
        let tools = ToolServer::tool_router().list_all();
        let by = |n: &str| {
            tools
                .iter()
                .find(|t| t.name == n)
                .unwrap_or_else(|| panic!("tool {n}"))
        };
        let props = |n: &str| {
            by(n)
                .output_schema
                .as_ref()
                .expect("outputSchema")
                .get("properties")
                .and_then(|v| v.as_object())
                .expect("schema properties")
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        // whois → npub (not integer id), *_ts_us microsecond timestamp.
        assert!(
            props("whois").contains(&"npub".to_string()),
            "whois schema must expose npub"
        );
        assert!(
            props("whois").contains(&"last_active_ts_us".to_string()),
            "whois schema must expose *_ts_us timestamp"
        );
        // send_message → deliveries[].payload {recipient, event_id}.
        // schemars emits nested types as $ref into $defs — resolve through it.
        let schema_obj = by("send_message")
            .output_schema
            .as_ref()
            .expect("outputSchema");
        let deliveries = schema_obj
            .get("properties")
            .and_then(|v| v.get("deliveries"))
            .and_then(|v| v.get("items"))
            .and_then(|v| v.get("$ref"))
            .and_then(|v| v.as_str())
            .and_then(|r| r.strip_prefix("#/$defs/"))
            .and_then(|name| schema_obj.get("$defs").and_then(|d| d.get(name)))
            .and_then(|d| d.get("properties"))
            .and_then(|v| v.get("payload"))
            .and_then(|v| v.get("$ref"))
            .and_then(|v| v.as_str())
            .and_then(|r| r.strip_prefix("#/$defs/"))
            .and_then(|name| schema_obj.get("$defs").and_then(|d| d.get(name)))
            .and_then(|d| d.get("properties"))
            .and_then(|v| v.as_object())
            .expect("deliveries payload props");
        assert!(
            deliveries.contains_key("recipient"),
            "send payload recipient"
        );
        assert!(deliveries.contains_key("event_id"), "send payload event_id");
        // release_file_reservations → {released, leases} (bd-release-breakdown-xom3
        // added the per-path breakdown; released count unchanged).
        assert_eq!(
            props("release_file_reservations"),
            vec!["leases".to_string(), "released".to_string()],
            "release schema = {{released, leases}}"
        );
        // set_contact_policy → bare kind-30014 event-id string.
        assert_eq!(
            by("set_contact_policy").output_schema.as_ref().unwrap()["type"],
            "string",
            "set_contact_policy is a bare string schema"
        );
    }
}
