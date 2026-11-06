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
//! `ToolRouter`'s `Add` (rmcp-3.2.0). Move-only — tool names/args/returns are
//! byte-identical (ADR-0007 C3).

pub mod contact;
pub mod judgment;
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
    ProtocolVersion, ServerCapabilities, ServerConfig,
};
use rmcp::{ServerHandler, tool_handler};
use serde_json::Value;

use toron_core::mailbox::Mailbox;

/// Tool return value: serialized JSON on `Ok`, a tool error on `Err`.
///
/// rmcp's `IntoCallToolResult` has no blanket impl for `anyhow::Result` /
/// `serde_json::Value` (only `String`/`ContentBlock`/`Json<T>`/`ErrorData`),
/// so the 38 tool bodies return this local wrapper and keep the AM-Rust JSON
/// contract on the wire.
pub struct ToolOutput(pub anyhow::Result<serde_json::Value>);

/// MCP error envelope (ADR-0034 G4): machine `code` + conservative
/// `retryable` + one-line `hint` alongside the human message. Additive on
/// the error path only: the first content block keeps today's exact text,
/// the second carries the envelope JSON. Success shapes, tool names, and
/// counts untouched (ADR-0033). Codes reuse `MailErrorCode` (ADR-0007 R1).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ToolError {
    pub code: String,
    pub retryable: bool,
    pub hint: String,
    pub message: String,
}

impl ToolError {
    pub fn classify(e: anyhow::Error) -> Self {
        use toron_core::mailbox::error::{MailErrorCode, mail_error_from_anyhow};
        let message = format!("{e:#}");
        let code = mail_error_from_anyhow(anyhow::anyhow!("{message}")).code;
        let retryable = matches!(code, MailErrorCode::Timeout | MailErrorCode::Io);
        let hint = match &code {
            MailErrorCode::NotFound => {
                "check the agent/project/message id and retry with the correct one"
            }
            MailErrorCode::PolicyDenied => {
                "request contact or use an authorized pin; never retry blind"
            }
            MailErrorCode::Unsupported => "use a supported backend or operation",
            MailErrorCode::Timeout | MailErrorCode::Io => "safe to retry with backoff",
            MailErrorCode::Malformed | MailErrorCode::Serialize => "fix the input shape and retry",
            MailErrorCode::Other(_) => "unclassified failure; do not retry blindly",
        };
        Self {
            code: code.as_str().to_string(),
            retryable,
            hint: hint.to_string(),
            message,
        }
    }
}

impl IntoCallToolResult for ToolOutput {
    fn into_call_tool_result(self) -> Result<CallToolResponse, rmcp::ErrorData> {
        match self.0 {
            Ok(value) => {
                let text = serde_json::to_string(&value)
                    .unwrap_or_else(|e| format!("serialize tool result: {e}"));
                let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                if value.is_object() {
                    result.structured_content = Some(value);
                }
                Ok(result.into())
            }
            Err(e) => {
                let env = ToolError::classify(e);
                let envelope = serde_json::to_string(&env)
                    .unwrap_or_else(|_| "{\"code\":\"serialize\"}".to_string());
                Ok(CallToolResult::error(vec![
                    ContentBlock::text(env.message),
                    ContentBlock::text(envelope),
                ])
                .into())
            }
        }
    }
}

impl ToolOutput {
    const fn ok(value: Value) -> Self {
        Self(Ok(value))
    }

    fn err(e: impl std::fmt::Display) -> Self {
        Self(Err(anyhow::anyhow!("{e}")))
    }

    fn result<T: ToJson>(r: anyhow::Result<T>) -> Self {
        Self(r.map(|value| value.to_json()))
    }
}

/// `ToJson` lives in `toron_core::json` (moved wave5-c: the trait lives where the
/// types live); re-exported here so existing `super::ToJson` / `crate::tools::ToJson`
/// paths — and toron-mcp's own `pub use tools::{ToJson, ToolServer}` — keep working.
pub use toron_core::json::ToJson;

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
    pub const fn mailbox(&self) -> &Arc<Mailbox> {
        &self.mailbox
    }

    /// Server capabilities advertised via `initialize`/`discover` (mirrors
    /// `ToronServer::capabilities`; no logging per SEP-2577).
    pub fn capabilities() -> ServerCapabilities {
        ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .enable_resources_subscribe()
            .enable_tool_list_changed()
            .build()
    }
}

impl ToolServer {
    /// The assembled router (38 parity + additive domain modules): each domain
    /// module contributes its `#[tool_router]`-generated router; `Add` merges
    /// them into the single surface. Parity tool names/args/returns are
    /// byte-identical to the pre-split monolith (ADR-0007 C3); additive tools
    /// ride under ADR-0033 discipline.
    pub fn tool_router() -> ToolRouter<Self> {
        Self::system_router()
            + Self::mail_router()
            + Self::reservation_router()
            + Self::judgment_router()
            + Self::product_router()
            + Self::contact_router()
            + Self::slot_router()
            + Self::workflow_router()
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ToolServer {
    fn get_info(&self) -> ServerConfig {
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
    use crate::tools::system::WhoisArgs;
    use serde_json::json;

    #[test]
    fn tool_error_envelope_is_conservative_and_parseable() {
        for (msg, code, retryable) in [
            ("agent X not found", "not_found", false),
            ("policy denied: block_all", "policy_denied", false),
            ("fetch timed out", "timeout", true),
            ("boom", "boom", false),
        ] {
            let env = ToolError::classify(anyhow::anyhow!(msg));
            assert_eq!(env.code, code, "code for {msg}");
            assert_eq!(env.retryable, retryable, "retryable for {msg}");
            assert!(!env.hint.is_empty(), "hint for {msg}");
            let v = serde_json::to_value(&env).expect("envelope serializes");
            assert_eq!(v["code"], code);
            assert_eq!(v["retryable"], retryable);
        }
    }

    #[test]
    fn nonneg_secs_rejects_negative_wrap() {
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

    /// bd-pin-public-plane-s-easy.8: `whois` accepts the identity as
    /// `agent_name` (canonical) OR `name` (serde alias — grok/other wrappers
    /// pass `name`). Both missing still fails (required identity field).
    #[test]
    fn whois_args_accepts_name_alias_and_stays_required() {
        let by_agent_name: WhoisArgs = serde_json::from_value(json!({
            "project_key": "toron",
            "agent_name": "BlueRidge",
        }))
        .expect("agent_name accepted");
        assert_eq!(by_agent_name.agent_name, "BlueRidge");

        let by_name: WhoisArgs = serde_json::from_value(json!({
            "project_key": "toron",
            "name": "BlueRidge",
        }))
        .expect("name alias accepted");
        assert_eq!(by_name.agent_name, "BlueRidge");

        let err = serde_json::from_value::<WhoisArgs>(json!({
            "project_key": "toron",
        }))
        .unwrap_err();
        assert!(
            err.to_string().contains("agent_name"),
            "missing identity field fails: {err}"
        );
    }

    /// Every tool must declare an outputSchema (ADR-0003 rev7 SEP-2106).
    /// Parity tools carry a schemars-derived `output_schema` on their tool
    /// definition. The count is 41: 38 AM-Rust parity tools + the additives
    /// `release_file_reservations_by_reason` +
    /// `list_file_reservations` + `ask_judgment` — the parity names/args/returns
    /// stay byte-identical (ADR-0007 C3).
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
            41,
            "38 AM-Rust parity tools + release_file_reservations_by_reason \
             + list_file_reservations + ask_judgment"
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
        let send = desc("send_message");
        for token in ["REQUIRED: project_key, sender_name", "flywheel mail send"] {
            assert!(
                send.contains(token),
                "send_message description missing {token:?}: {send}"
            );
        }
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
        for tool in &tools {
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

    /// MCP clients validate tool schemas against JSON Schema's known
    /// `format` vocabulary (`int32`/`int64`/`date-time`/etc.). schemars emits
    /// `uint`/`uint32`/`uint64` for Rust unsigned ints (`usize` → `uint`),
    /// which strict clients report as "unknown format uint ignored in schema
    /// at path #/properties/..." — so the surface must be all-signed
    /// (`i32`/`i64`; wire-identical JSON numbers). Walk every tool's live
    /// inputSchema + outputSchema recursively and reject any `uint*` format.
    #[test]
    fn no_uint_formats_in_tool_schemas() {
        fn walk(tool: &str, path: String, v: &Value, out: &mut Vec<String>) {
            if let Some(o) = v.as_object() {
                if let Some(f) = o.get("format").and_then(|f| f.as_str())
                    && f.starts_with("uint")
                {
                    out.push(format!("{tool} {path}: format {f:?}"));
                }
                for (k, child) in o {
                    walk(tool, format!("{path}/{k}"), child, out);
                }
            } else if let Some(a) = v.as_array() {
                for (i, child) in a.iter().enumerate() {
                    walk(tool, format!("{path}[{i}]"), child, out);
                }
            }
        }
        let tools = ToolServer::tool_router().list_all();
        let mut violations: Vec<String> = Vec::new();
        for tool in &tools {
            let name = tool.name.as_ref();
            walk(
                name,
                "#".to_string(),
                &Value::Object((*tool.input_schema).clone()),
                &mut violations,
            );
            if let Some(output) = tool.output_schema.as_ref() {
                walk(
                    name,
                    "#".to_string(),
                    &Value::Object((**output).clone()),
                    &mut violations,
                );
            }
        }
        assert!(
            violations.is_empty(),
            "unsigned `uint*` schema formats (unknown to MCP clients) at {}: {}",
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
        assert!(
            props("whois").contains(&"npub".to_string()),
            "whois schema must expose npub"
        );
        assert!(
            props("whois").contains(&"last_active_ts_us".to_string()),
            "whois schema must expose *_ts_us timestamp"
        );
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
        assert_eq!(
            props("release_file_reservations"),
            vec!["leases".to_string(), "released".to_string()],
            "release schema = {{released, leases}}"
        );
        assert_eq!(
            by("set_contact_policy").output_schema.as_ref().unwrap()["type"],
            "object",
            "set_contact_policy wrapper must be object"
        );
        assert!(
            props("set_contact_policy").contains(&"event_id".to_string()),
            "set_contact_policy schema must expose event_id"
        );
    }
}
