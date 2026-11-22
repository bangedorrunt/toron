// governed-by: ADR-0003

//! MCP surface catalog extractor (toron.dev ADR-0002 D3).
//!
//! Builds the machine-readable catalog of the 38 AM-Rust parity tools
//! (name, group, description, input/output JSON schemas verbatim from the
//! rmcp `#[tool]` definitions via the assembled [`ToolServer`] router) and
//! the 25 resources. Additive tools (ADR-0033) are excluded — the catalog
//! contract is exactly the frozen 38.

use serde_json::{Map, Value, json};
use std::collections::HashSet;

use rmcp::model::Tool;

use crate::resources::RESOURCES;
use crate::tools::ToolServer;

/// The first non-parity additive tool (bd-h3oj) — NOT part of the 38.
/// (Singular for history; the full list is [`ADDITIVE_TOOLS`].)
pub const ADDITIVE_TOOL: &str = "release_file_reservations_by_reason";

/// Every non-parity additive tool (ADR-0033 discipline): live on the MCP
/// surface, excluded from the frozen 38-parity catalog.
pub const ADDITIVE_TOOLS: &[&str] = &[ADDITIVE_TOOL, "list_file_reservations", "ask_judgment"];

/// Expected parity tool count (frozen: toron ADR-0007 C3).
pub const PARITY_TOOL_COUNT: usize = 38;

/// The 9 catalog groups (toron.dev catalog contract / ADR-0001 D7), each with
/// its parity tool names in canonical order.
pub const TOOL_GROUPS: &[(&str, &[&str])] = &[
    (
        "Identity",
        &[
            "register_agent",
            "create_agent_identity",
            "whois",
            "resolve_pane_identity",
            "cleanup_pane_identities",
            "list_agents",
        ],
    ),
    (
        "Messaging",
        &[
            "send_message",
            "reply_message",
            "fetch_inbox",
            "acknowledge_message",
            "mark_message_read",
        ],
    ),
    (
        "Contacts",
        &[
            "request_contact",
            "respond_contact",
            "list_contacts",
            "set_contact_policy",
        ],
    ),
    (
        "File-reservations",
        &[
            "check_file_reservation_conflicts",
            "file_reservation_paths",
            "renew_file_reservations",
            "release_file_reservations",
            "force_release_file_reservation",
        ],
    ),
    ("Search", &["search_messages", "summarize_thread"]),
    (
        "Macros",
        &[
            "macro_start_session",
            "macro_prepare_thread",
            "macro_contact_handshake",
            "macro_file_reservation_cycle",
        ],
    ),
    (
        "Product-bus",
        &[
            "ensure_product",
            "products_link",
            "search_messages_product",
            "fetch_inbox_product",
            "summarize_thread_product",
        ],
    ),
    (
        "Build-slots",
        &[
            "acquire_build_slot",
            "renew_build_slot",
            "release_build_slot",
        ],
    ),
    (
        "Infrastructure",
        &[
            "health_check",
            "ensure_project",
            "install_precommit_guard",
            "uninstall_precommit_guard",
        ],
    ),
];

/// Extract the 38 parity tools, schemas verbatim from the rmcp router.
///
/// Emits entries in `TOOL_GROUPS` order. Fails on drift: a parity name
/// missing from the live surface, or any tool outside the frozen 38 + the
/// ADR-0033 additives, is a catalog error (ADR-0007 C3).
pub fn tool_catalog() -> anyhow::Result<Vec<Value>> {
    let tools = ToolServer::tool_router().list_all();
    let mut seen: HashSet<&str> = HashSet::with_capacity(PARITY_TOOL_COUNT);
    let mut out = Vec::with_capacity(PARITY_TOOL_COUNT);
    for (group, names) in TOOL_GROUPS {
        for name in *names {
            let tool = tools
                .iter()
                .find(|t| t.name.as_ref() == *name)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "catalog drift: tool `{name}` (group `{group}`) missing from the MCP surface"
                    )
                })?;
            seen.insert(*name);
            out.push(tool_entry(tool, group));
        }
    }
    let extras: Vec<&str> = tools
        .iter()
        .map(|t| t.name.as_ref())
        .filter(|n| !seen.contains(*n))
        .collect();
    let unexpected: Vec<&str> = extras
        .iter()
        .copied()
        .filter(|n| !ADDITIVE_TOOLS.contains(n))
        .collect();
    if !unexpected.is_empty() {
        anyhow::bail!(
            "catalog drift: tools outside the frozen 38 + additive: {unexpected:?} — \
             a new tool needs an ADR (ADR-0007 C3) and a catalog group"
        );
    }
    Ok(out)
}

/// The 25 resources as catalog entries (name, uri, description).
pub fn resource_catalog() -> Vec<Value> {
    RESOURCES
        .iter()
        .map(|(name, uri, description)| {
            json!({ "name": name, "uri": uri, "description": description })
        })
        .collect()
}

/// Every live MCP tool and the CLI command that covers it (bd-5jeyj).
/// The parity test reads the live router and fails naming any tool with no
/// row here, or whose CLI path does not exist — so a future tool cannot
/// ship without a CLI. Paths are `toron` subcommand words (`mail send`).
pub const TOOL_CLI_PARITY: &[(&str, &str)] = &[
    ("register_agent", "agent bootstrap"),
    ("create_agent_identity", "agent bootstrap"),
    ("whois", "mail whois"),
    ("resolve_pane_identity", "pane resolve"),
    ("cleanup_pane_identities", "pane cleanup"),
    ("list_agents", "agent list"),
    ("send_message", "mail send"),
    ("reply_message", "mail reply"),
    ("fetch_inbox", "mail inbox"),
    ("acknowledge_message", "mail ack"),
    ("mark_message_read", "mail read"),
    ("request_contact", "contact request"),
    ("respond_contact", "contact respond"),
    ("list_contacts", "contact list"),
    ("set_contact_policy", "contact policy"),
    ("check_file_reservation_conflicts", "reserve conflicts"),
    ("file_reservation_paths", "reserve acquire"),
    ("renew_file_reservations", "reserve renew"),
    ("release_file_reservations", "reserve release"),
    ("force_release_file_reservation", "reserve force-release"),
    ("list_file_reservations", "reserve list"),
    (
        "release_file_reservations_by_reason",
        "reserve release-by-reason",
    ),
    ("search_messages", "mail search"),
    ("summarize_thread", "mail thread-summarize"),
    ("macro_start_session", "agent bootstrap"),
    ("macro_prepare_thread", "mail prepare-thread"),
    ("macro_contact_handshake", "contact handshake"),
    ("macro_file_reservation_cycle", "reserve cycle"),
    ("ensure_product", "product ensure"),
    ("products_link", "product link"),
    ("search_messages_product", "product search"),
    ("fetch_inbox_product", "product inbox"),
    ("summarize_thread_product", "product summarize"),
    ("acquire_build_slot", "slot acquire"),
    ("renew_build_slot", "slot renew"),
    ("release_build_slot", "slot release"),
    ("health_check", "status"),
    ("ensure_project", "project ensure"),
    ("install_precommit_guard", "guard install"),
    ("uninstall_precommit_guard", "guard uninstall"),
    ("ask_judgment", "judgment ask"),
];

/// The live additive tools as catalog entries (bd-5jeyj): same shape as
/// the parity rows, plus the CLI command that covers each one. The frozen
/// 38-tool table stays byte-identical (ADR-0007 C3); this section is how
/// the catalog lists ALL live tools.
pub fn additive_tool_catalog() -> anyhow::Result<Vec<Value>> {
    let tools = ToolServer::tool_router().list_all();
    let mut out = Vec::with_capacity(ADDITIVE_TOOLS.len());
    for name in ADDITIVE_TOOLS {
        let tool = tools
            .iter()
            .find(|t| t.name.as_ref() == *name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "catalog drift: additive tool `{name}` missing from the MCP surface"
                )
            })?;
        let cli = TOOL_CLI_PARITY
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, c)| *c)
            .ok_or_else(|| {
                anyhow::anyhow!("catalog drift: additive tool `{name}` has no CLI mapping")
            })?;
        let mut entry = tool_entry(tool, "Additive");
        entry["cli"] = Value::String(cli.to_string());
        out.push(entry);
    }
    Ok(out)
}

fn tool_entry(tool: &Tool, group: &str) -> Value {
    let input_schema = Value::Object((*tool.input_schema).clone());
    let output_schema = tool
        .output_schema
        .as_ref()
        .map(|s| Value::Object((**s).clone()));
    json!({
        "name": tool.name.to_string(),
        "group": group,
        "description": tool.description.clone().unwrap_or_default().to_string(),
        "input_schema": input_schema,
        "output_schema": output_schema,
        "parity": "AM-Rust 38-tool surface",
        "example": sample_schema(&input_schema),
    })
}

/// Deterministic schema-derived example payload (one sample value per
/// `properties` entry, in schema order).
fn sample_schema(schema: &Value) -> Value {
    let mut obj = Map::new();
    if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
        for (k, v) in props {
            obj.insert(k.clone(), sample_value(v));
        }
    }
    Value::Object(obj)
}

fn sample_value(schema: &Value) -> Value {
    match schema.get("type").and_then(|t| t.as_str()) {
        Some("string") => schema
            .get("enum")
            .and_then(|e| e.as_array())
            .and_then(|e| e.first().cloned())
            .unwrap_or_else(|| Value::String("<string>".to_string())),
        Some("integer") | Some("number") => Value::from(0),
        Some("boolean") => Value::Bool(true),
        Some("array") => Value::Array(vec![
            schema.get("items").map(sample_value).unwrap_or(Value::Null),
        ]),
        Some("object") => sample_schema(schema),
        _ => Value::Null,
    }
}
