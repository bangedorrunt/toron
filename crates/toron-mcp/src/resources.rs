// governed-by: ADR-0003

//! MCP resources data: the 25 AM-Rust parity resource templates and the
//! value resolution helpers used by the `ServerHandler` in `server.rs`.

/// All 25 parity resources as (name, uri-template, description).
pub const RESOURCES: &[(&str, &str, &str)] = &[
    (
        "projects",
        "toron://projects/{slug}",
        "List projects known to toron.",
    ),
    (
        "project",
        "toron://project/{slug}",
        "Project manifest (kind-30010).",
    ),
    (
        "product",
        "toron://products/{key}",
        "Product + linked projects (kind-30011).",
    ),
    (
        "agents",
        "toron://agents/{project}",
        "Registered agents in a project.",
    ),
    (
        "agent_profile",
        "toron://agents/{name}/profile",
        "Agent profile (kind-30013).",
    ),
    (
        "agent_inbox",
        "toron://agents/{name}/inbox",
        "Agent inbox (unwrapped mail).",
    ),
    (
        "agent_outbox",
        "toron://agents/{name}/outbox",
        "Messages sent by an agent.",
    ),
    (
        "mailbox",
        "toron://mailbox/{name}",
        "Agent mailbox + commit metadata.",
    ),
    (
        "message",
        "toron://messages/{id}",
        "Single message body/metadata.",
    ),
    (
        "thread",
        "toron://threads/{id}",
        "Thread DAG (NIP-10 e tags).",
    ),
    (
        "reservations",
        "toron://reservations/{project}",
        "File reservations.",
    ),
    (
        "reservation",
        "toron://reservations/{id}",
        "Single file reservation.",
    ),
    ("slots", "toron://slots/{project}", "Build slots."),
    ("slot", "toron://slots/{slot}", "Single build slot."),
    (
        "identity",
        "toron://identity/{project}",
        "Identity resolution for a project.",
    ),
    (
        "config_environment",
        "toron://config/environment",
        "Server environment + HTTP settings.",
    ),
    (
        "metrics",
        "toron://tooling/metrics",
        "Aggregated tool call/error counts.",
    ),
    (
        "metrics_core",
        "toron://tooling/metrics_core",
        "Core system metrics (HTTP/DB/storage).",
    ),
    (
        "directory",
        "toron://tooling/directory",
        "Clustered tool directory.",
    ),
    (
        "schemas",
        "toron://tooling/schemas",
        "Parameter schemas for tools/macros.",
    ),
    (
        "recent",
        "toron://tooling/recent/{window_seconds}",
        "Recent tool activity.",
    ),
    (
        "locks",
        "toron://tooling/locks",
        "Lock metadata from archive storage.",
    ),
    (
        "diagnostics",
        "toron://tooling/diagnostics",
        "Diagnostic report + recommendations.",
    ),
    (
        "capabilities",
        "toron://tooling/capabilities/{agent}",
        "Tooling capabilities for an agent.",
    ),
    (
        "ack_overdue",
        "toron://views/ack-overdue/{agent}",
        "Ack-required messages past TTL.",
    ),
];

/// True if a concrete URI matches a resource template (prefix match on the
/// static part before the first `{`).
pub fn template_matches(template: &str, uri: &str) -> bool {
    let prefix = template.split('{').next().unwrap_or(template);
    uri.starts_with(prefix)
}

/// Resolve a resource URI to its JSON value. Static metadata is served
/// directly; data-backed templates return a structured stub that names the
/// live tool to call (ADR-0007 R4.4 — honest projection, not empty success).
/// `None` = unknown resource.
pub fn read_resource_value(uri: &str) -> Option<serde_json::Value> {
    let base = uri.split('?').next().unwrap_or(uri);
    if let Some(v) = static_resource(base) {
        return Some(v);
    }
    if let Some((name, template, desc)) = RESOURCES
        .iter()
        .find(|(_, t, _)| template_matches(t, base))
    {
        return Some(serde_json::json!({
            "uri": base,
            "resource": name,
            "template": template,
            "description": desc,
            "status": "live_via_tools",
            "note": "Use the matching MCP tool for full projection (list_agents, fetch_inbox, search_messages, health_check, …). Resource URI is a discovery handle."
        }));
    }
    None
}

fn static_resource(uri: &str) -> Option<serde_json::Value> {
    match uri {
        "toron://config/environment" => Some(serde_json::json!({
            "host": "127.0.0.1",
            "mcp_path": "/mcp",
            "protocol": "2026-07-28",
        })),
        "toron://tooling/directory" => Some(serde_json::json!({
            "clusters": ["Infrastructure", "Identity", "Messaging", "Contacts",
                         "File Reservations", "Search", "Macros", "Product Bus", "Build Slots"],
            "tools_count": 38,
        })),
        "toron://tooling/metrics" => Some(serde_json::json!({"tools_called": 0, "errors": 0})),
        "toron://tooling/metrics_core" => Some(serde_json::json!({
            "http": "not started", "database": "not wired", "storage": "not wired",
        })),
        "toron://tooling/schemas" => {
            Some(serde_json::json!({"note": "see tools.schema.json fixtures"}))
        }
        "toron://tooling/locks" => Some(serde_json::json!({"locks": []})),
        "toron://tooling/diagnostics" => Some(serde_json::json!({"health": "ok", "checks": []})),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exactly_25_resources() {
        assert_eq!(RESOURCES.len(), 25, "AM-Rust parity requires 25 resources");
    }

    #[test]
    fn unique_uris() {
        let mut uris: Vec<&str> = RESOURCES.iter().map(|(_, u, _)| *u).collect();
        uris.sort();
        uris.dedup();
        assert_eq!(uris.len(), 25, "resource URIs must be unique");
    }

    #[test]
    fn templates_are_a_subset() {
        let templates: Vec<&str> = RESOURCES
            .iter()
            .filter(|(_, u, _)| u.contains('{'))
            .map(|(_, u, _)| *u)
            .collect();
        assert!(!templates.is_empty());
        assert!(templates.len() < RESOURCES.len());
    }

    #[test]
    fn template_matching_works() {
        assert!(template_matches(
            "toron://agents/{name}/inbox",
            "toron://agents/BlueLake/inbox"
        ));
        assert!(!template_matches(
            "toron://agents/{name}/inbox",
            "toron://config/environment"
        ));
    }

    #[test]
    fn static_resources_serve_json() {
        assert!(read_resource_value("toron://config/environment").is_some());
        assert!(read_resource_value("toron://bogus/uri").is_none());
    }
}
