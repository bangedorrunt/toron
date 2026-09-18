// governed-by: ADR-0003

//! MCP resources data: the 25 AM-Rust parity resource templates and the
//! value resolution helpers used by the `ServerHandler` in `server.rs`.

use sqlx::Row;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::Event;
use toron_core::nostr::kinds::{
    AGENT_PROFILE, BUILD_SLOT, FILE_RESERVATION, PRODUCT_MANIFEST, PROJECT_MANIFEST, toron_kind,
};

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

/// Return the concrete resources that can be listed and read without
/// substituting template parameters.
pub fn concrete_resources()
-> impl Iterator<Item = &'static (&'static str, &'static str, &'static str)> {
    RESOURCES.iter().filter(|(_, uri, _)| !uri.contains('{'))
}

/// Return the parameterized resources for `resources/templates/list`.
pub fn resource_templates()
-> impl Iterator<Item = &'static (&'static str, &'static str, &'static str)> {
    RESOURCES.iter().filter(|(_, uri, _)| uri.contains('{'))
}

/// Return whether a URI is a supported resource handle or a concrete
/// expansion of one of the advertised templates.
pub fn resource_uri_supported(uri: &str) -> bool {
    let base = uri.split('?').next().unwrap_or(uri);
    concrete_resources().any(|(_, known, _)| *known == base)
        || resource_templates().any(|(_, template, _)| template_matches(template, base))
}

/// Select a concrete, readable resource URI for a bus update.
///
/// Resource-update notifications carry handles, not arbitrary event IDs. Use
/// the most specific readable projection when the event has enough identity;
/// otherwise use the concrete metrics resource as a safe refresh signal.
pub fn notification_resource_uri(event: &Event) -> String {
    let project = event
        .tags
        .iter()
        .find_map(|tag| (tag.kind() == "t").then(|| tag.content()).flatten());
    let d_tag = event
        .tags
        .iter()
        .find_map(|tag| (tag.kind() == "d").then(|| tag.content()).flatten());

    match event.kind.as_u16() {
        PROJECT_MANIFEST => project
            .map(|slug| format!("toron://project/{slug}"))
            .unwrap_or_else(|| "toron://tooling/metrics".to_string()),
        PRODUCT_MANIFEST => d_tag
            .map(|key| format!("toron://products/{key}"))
            .unwrap_or_else(|| "toron://tooling/metrics".to_string()),
        AGENT_PROFILE => d_tag
            .and_then(|identity| identity.split_once(':').map(|(_, name)| name))
            .map(|name| format!("toron://agents/{name}/profile"))
            .unwrap_or_else(|| "toron://tooling/metrics".to_string()),
        FILE_RESERVATION => project
            .map(|slug| format!("toron://reservations/{slug}"))
            .unwrap_or_else(|| "toron://tooling/metrics".to_string()),
        BUILD_SLOT => project
            .map(|slug| format!("toron://slots/{slug}"))
            .unwrap_or_else(|| "toron://tooling/metrics".to_string()),
        _ => "toron://tooling/metrics".to_string(),
    }
}

/// True if a concrete URI matches a resource template.
///
/// Matching is segment-aware: literal segments must be equal, variable
/// segments must be non-empty, and extra or missing path segments reject the
/// URI. Query parameters are ignored for matching but remain available to the
/// projection layer through the original URI.
pub fn template_matches(template: &str, uri: &str) -> bool {
    let concrete = uri.split('?').next().unwrap_or(uri);
    let template_segments: Vec<&str> = template.split('/').collect();
    let concrete_segments: Vec<&str> = concrete.split('/').collect();
    template_segments.len() == concrete_segments.len()
        && template_segments.iter().zip(concrete_segments).all(
            |(template_segment, concrete_segment)| {
                if template_segment.starts_with('{') && template_segment.ends_with('}') {
                    !concrete_segment.is_empty()
                } else {
                    template_segment == &concrete_segment
                }
            },
        )
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
    if let Some((name, template, desc)) =
        resource_templates().find(|(_, t, _)| template_matches(t, base))
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
            // S8 honest-stub gate (Rev 1): placeholder fields MUST name the
            // live tool that serves the real value (bd-p0-s8-resources-stub-gate-rrfz).
            "status": "live via health_check / diagnostics tools",
        })),
        "toron://tooling/schemas" => Some(serde_json::json!({
            "note": "see tools.schema.json fixtures",
            "status": "live via MCP tools/list schema introspection",
        })),
        "toron://tooling/locks" => Some(serde_json::json!({"locks": []})),
        "toron://tooling/diagnostics" => Some(serde_json::json!({"health": "ok", "checks": []})),
        _ => None,
    }
}

/// ADR-0007 F10 / R4.4: project a data-backed resource URI through the
/// mailbox (index + bus + data_dir). Returns `None` for static tooling URIs
/// and unknown URIs — the caller falls back to [`read_resource_value`].
///
/// Projections:
/// - `toron://projects/{slug}` / `toron://project/{slug}` → manifest state.
/// - `toron://products/{key}` → kind-30011 manifest events.
/// - `toron://agents/{project}` → registered agents; `{name}/profile` →
///   whois; `{name}/inbox` → unwrapped mail; `{name}/outbox` → sent mail.
/// - `toron://mailbox/{name}` → health + inbox summary for the agent.
/// - `toron://messages/{id}` → exact message; `toron://threads/{id}` →
///   thread members.
/// - `toron://reservations/{project|id}` / `toron://slots/{project|slot}`
///   → kind-30031 / 30032 events (project-scoped or by event id).
/// - `toron://identity/{project}` → agents of the project.
/// - `toron://views/ack-overdue/{agent}` → ack-required mail.
pub async fn project_resource_value(mailbox: &Mailbox, uri: &str) -> Option<serde_json::Value> {
    let base = uri.split('?').next().unwrap_or(uri);
    if let Some(slug) = base.strip_prefix("toron://projects/") {
        return mailbox
            .ensure_project(slug)
            .await
            .ok()
            .and_then(|s| serde_json::to_value(s).ok());
    }
    if let Some(slug) = base.strip_prefix("toron://project/") {
        return mailbox
            .ensure_project(slug)
            .await
            .ok()
            .and_then(|s| serde_json::to_value(s).ok());
    }
    if let Some(key) = base.strip_prefix("toron://products/") {
        return manifest_view(mailbox, PRODUCT_MANIFEST, Some(key)).await;
    }
    if let Some(rest) = base.strip_prefix("toron://agents/") {
        if let Some(name) = rest.strip_suffix("/profile") {
            let profile = find_profile(mailbox, name).await?;
            return Some(whois_json(&profile));
        }
        if let Some(name) = rest.strip_suffix("/inbox") {
            return inbox_view(mailbox, name).await;
        }
        if let Some(name) = rest.strip_suffix("/outbox") {
            return outbox_view(mailbox, name).await;
        }
        return Some(agents_json(mailbox, rest).await);
    }
    if let Some(name) = base.strip_prefix("toron://mailbox/") {
        return mailbox_view(mailbox, name).await;
    }
    if let Some(id) = base.strip_prefix("toron://messages/") {
        return message_view(mailbox, id).await;
    }
    if let Some(id) = base.strip_prefix("toron://threads/") {
        return thread_view(mailbox, id).await;
    }
    if let Some(rest) = base.strip_prefix("toron://reservations/") {
        return reservation_view(mailbox, rest).await;
    }
    if let Some(rest) = base.strip_prefix("toron://slots/") {
        return slot_view(mailbox, rest).await;
    }
    if let Some(project) = base.strip_prefix("toron://identity/") {
        return Some(agents_json(mailbox, project).await);
    }
    if let Some(agent) = base.strip_prefix("toron://views/ack-overdue/") {
        return overdue_view(mailbox, agent).await;
    }
    None
}

/// Project slugs present under the mailbox data_dir (top-level dirs).
fn project_slugs(mailbox: &Mailbox) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(mailbox.data_dir()) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
            && let Some(name) = entry.file_name().to_str()
        {
            out.push(name.to_string());
        }
    }
    out
}

/// Find the project that holds an agent named `name` (whois probe per slug).
async fn find_profile(mailbox: &Mailbox, name: &str) -> Option<toron_core::mailbox::WhoIs> {
    for slug in project_slugs(mailbox) {
        if let Ok(w) = mailbox.whois(&slug, name).await {
            return Some(w);
        }
    }
    None
}

/// Registered agents of a project as a JSON array.
async fn agents_json(mailbox: &Mailbox, project: &str) -> serde_json::Value {
    let opts = toron_core::mailbox::ListAgentsOptions {
        active_within_days: None,
    };
    match mailbox.list_agents(project, opts).await {
        Ok(agents) => serde_json::Value::Array(agents.iter().map(whois_json).collect()),
        Err(e) => serde_json::json!({ "error": e.to_string(), "project": project }),
    }
}

fn whois_json(w: &toron_core::mailbox::WhoIs) -> serde_json::Value {
    serde_json::json!({
        "name": w.name,
        "npub": w.npub,
        "program": w.program,
        "model": w.model,
        "task_description": w.task_description,
        "last_active_ts_us": w.last_active_ts_us,
        "pane_id": w.pane_id,
        "workspace_id": w.workspace_id,
        "tab_id": w.tab_id,
    })
}

/// Unwrapped inbox mail for an agent (project located by profile probe).
async fn inbox_view(mailbox: &Mailbox, name: &str) -> Option<serde_json::Value> {
    let profile = find_profile(mailbox, name).await?;
    let args = toron_core::mailbox::FetchInbox {
        project_slug: project_of(mailbox, &profile).await,
        agent_name: name.to_string(),
        unread_only: false,
        limit: Some(25),
        include_bodies: false,
        urgent_only: false,
        since_ts: None,
        thread_id: None,
    };
    let messages = mailbox.fetch_inbox(&args).await.ok()?;
    Some(serde_json::json!({
        "agent": name,
        "project": args.project_slug,
        "messages": messages.iter().map(inbox_json).collect::<Vec<_>>(),
    }))
}

/// The project slug holding a profile (first data_dir hit — matches the
/// whois probe order).
async fn project_of(mailbox: &Mailbox, profile: &toron_core::mailbox::WhoIs) -> String {
    for slug in project_slugs(mailbox) {
        if mailbox
            .whois(&slug, &profile.name)
            .await
            .map(|w| w.npub == profile.npub)
            .unwrap_or(false)
        {
            return slug;
        }
    }
    "unknown".to_string()
}

fn inbox_json(m: &toron_core::mailbox::InboxMessage) -> serde_json::Value {
    serde_json::json!({
        "id": m.id,
        "sender": m.sender,
        "subject": m.subject,
        "body_md": m.body_md,
        "body": m.body_md,
        "importance": m.importance,
        "ack_required": m.ack_required,
        "thread_id": m.thread_id,
        "created_ts_us": m.created_ts_us,
        "read_ts_us": m.read_ts_us,
        "duplicate_of": m.duplicate_of,
        "corr": m.corr,
        "wake": m.wake,
        "priority": m.priority,
        "audience": m.audience,
        "result_path": m.result_path,
        "verdict": m.verdict,
    })
}

/// Messages sent by an agent (index scan by sender display name).
async fn outbox_view(mailbox: &Mailbox, name: &str) -> Option<serde_json::Value> {
    let hits = match index_scan(
        mailbox,
        "SELECT event_id, subject, sender, created_ts_us, importance, ack_required, thread_id, body \
         FROM messages WHERE sender = ?1 ORDER BY created_ts_us DESC, event_id ASC",
        &[name],
        50,
        false,
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "outbox_view index scan failed; resource not served");
            return None;
        }
    };
    Some(
        serde_json::json!({ "agent": name, "messages": hits.into_iter().map(search_json).collect::<Vec<_>>() }),
    )
}

fn search_json(m: toron_core::mailbox::SearchMessage) -> serde_json::Value {
    serde_json::json!({
        "id": m.id,
        "from": m.from,
        "subject": m.subject,
        "body_md": m.body_md,
        "importance": m.importance,
        "ack_required": m.ack_required,
        "thread_id": m.thread_id,
        "created_ts_us": m.created_ts_us,
    })
}

/// Map one `messages`-table row to the search-result shape (AM-parity field
/// names). Shared by the index-scan projections below.
fn indexed_row(
    row: &sqlx::sqlite::SqliteRow,
    include_body: bool,
) -> toron_core::mailbox::SearchMessage {
    toron_core::mailbox::SearchMessage {
        id: row.get("event_id"),
        subject: row.get("subject"),
        from: row.get("sender"),
        created_ts_us: row.get("created_ts_us"),
        importance: row.get("importance"),
        ack_required: row.get::<i64, _>("ack_required") != 0,
        thread_id: row.get("thread_id"),
        body_md: include_body.then_some(row.get("body")),
    }
}

/// Direct `messages`-table scan (FTS5 MATCH has no match-all query, so the
/// id/sender/thread/across-project projections bypass the FTS join).
///
/// Returns `Err` on a DB failure — a broken index must surface, not mask as
/// an empty result set (P2 bd-p2-unwrap-or-zero-coercion-5ac3).
async fn index_scan(
    mailbox: &Mailbox,
    sql: &str,
    params: &[&str],
    limit: usize,
    include_body: bool,
) -> Result<Vec<toron_core::mailbox::SearchMessage>, sqlx::Error> {
    let Some(pool) = mailbox.index_pool() else {
        return Ok(Vec::new());
    };
    let sql = format!("{sql} LIMIT {limit}");
    let mut q = sqlx::query(&sql);
    for p in params {
        q = q.bind(*p);
    }
    let rows = q.fetch_all(pool).await?;
    Ok(rows.iter().map(|r| indexed_row(r, include_body)).collect())
}

/// One message by exact event id (index lookup; ids are globally unique).
async fn message_view(mailbox: &Mailbox, id: &str) -> Option<serde_json::Value> {
    let hits = match index_scan(
        mailbox,
        "SELECT event_id, subject, sender, created_ts_us, importance, ack_required, thread_id, body \
         FROM messages WHERE event_id = ?1 ORDER BY created_ts_us DESC",
        &[id],
        1,
        true,
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "message_view index scan failed; resource not served");
            return None;
        }
    };
    hits.into_iter().next().map(search_json)
}

/// Thread members by thread id (index lookup, chronological).
async fn thread_view(mailbox: &Mailbox, id: &str) -> Option<serde_json::Value> {
    let hits = match index_scan(
        mailbox,
        "SELECT event_id, subject, sender, created_ts_us, importance, ack_required, thread_id, body \
         FROM messages WHERE thread_id = ?1 ORDER BY created_ts_us ASC, local_seq ASC",
        &[id],
        200,
        false,
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "thread_view index scan failed; resource not served");
            return None;
        }
    };
    if hits.is_empty() {
        return None;
    }
    Some(
        serde_json::json!({ "thread_id": id, "messages": hits.into_iter().map(search_json).collect::<Vec<_>>() }),
    )
}

/// Ack-required mail sent by an agent (index scan on `ack_required` +
/// sender display name — the index stores senders, not recipients).
async fn overdue_view(mailbox: &Mailbox, agent: &str) -> Option<serde_json::Value> {
    let hits = match index_scan(
        mailbox,
        "SELECT event_id, subject, sender, created_ts_us, importance, ack_required, thread_id, body \
         FROM messages WHERE ack_required = 1 AND sender = ?1 ORDER BY created_ts_us DESC, event_id ASC",
        &[agent],
        500,
        false,
    )
    .await
    {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "overdue_view index scan failed; resource not served");
            return None;
        }
    };
    Some(
        serde_json::json!({ "agent": agent, "messages": hits.into_iter().map(search_json).collect::<Vec<_>>() }),
    )
}

/// Mailbox summary for an agent: health + recent inbox.
async fn mailbox_view(mailbox: &Mailbox, name: &str) -> Option<serde_json::Value> {
    let profile = find_profile(mailbox, name).await?;
    let project = project_of(mailbox, &profile).await;
    let args = toron_core::mailbox::FetchInbox {
        project_slug: project.clone(),
        agent_name: name.to_string(),
        unread_only: false,
        limit: Some(10),
        include_bodies: false,
        urgent_only: false,
        since_ts: None,
        thread_id: None,
    };
    let messages = mailbox.fetch_inbox(&args).await.ok()?;
    let health = mailbox.health_check().await;
    Some(serde_json::json!({
        "agent": name,
        "project": project,
        "npub": profile.npub,
        "inbox_count": messages.len(),
        "recent": messages.iter().map(inbox_json).collect::<Vec<_>>(),
        "health": serde_json::to_value(&health).ok(),
    }))
}

/// Kind-addressable manifest events (products/reservations/slots) as a JSON
/// array of signed events, newest first. `d` filters on the addressable tag.
async fn manifest_view(mailbox: &Mailbox, kind: u16, d: Option<&str>) -> Option<serde_json::Value> {
    let filter =
        toron_core::nostr::Filter::new().kinds([toron_core::nostr::kinds::toron_kind(kind)]);
    let events = toron_core::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
        .await
        .ok()?;
    let mut matches: Vec<_> = events
        .into_iter()
        .filter(|ev| {
            d.map(|want| {
                ev.tags
                    .iter()
                    .any(|t| t.kind() == "d" && t.content() == Some(want))
            })
            .unwrap_or(true)
        })
        .collect();
    matches.sort_by_key(|ev| ev.created_at.as_secs());
    matches.reverse();
    let docs: Vec<serde_json::Value> = matches
        .into_iter()
        .filter_map(|ev| serde_json::to_value(&ev).ok())
        .collect();
    if docs.is_empty() && d.is_some() {
        return None;
    }
    Some(serde_json::json!({ "kind": kind, "count": docs.len(), "events": docs }))
}

/// Reservations view: `toron://reservations/{project}` lists project-scoped
/// kind-30031 events; a 64-hex param is treated as a reservation event id.
async fn reservation_view(mailbox: &Mailbox, param: &str) -> Option<serde_json::Value> {
    if param.len() == 64 && param.chars().all(|c| c.is_ascii_hexdigit()) {
        let events = manifest_view(mailbox, FILE_RESERVATION, None).await?;
        return events["events"]
            .as_array()
            .and_then(|docs| {
                docs.iter()
                    .find(|d| d["id"].as_str() == Some(param))
                    .cloned()
            })
            .map(|doc| serde_json::json!({ "reservation": doc }));
    }
    project_scoped_events(mailbox, FILE_RESERVATION, param).await
}

/// Slots view: `toron://slots/{project}` lists project-scoped kind-30032
/// events; a 64-hex param is treated as a slot event id.
async fn slot_view(mailbox: &Mailbox, param: &str) -> Option<serde_json::Value> {
    if param.len() == 64 && param.chars().all(|c| c.is_ascii_hexdigit()) {
        let events = manifest_view(mailbox, BUILD_SLOT, None).await?;
        return events["events"]
            .as_array()
            .and_then(|docs| {
                docs.iter()
                    .find(|d| d["id"].as_str() == Some(param))
                    .cloned()
            })
            .map(|doc| serde_json::json!({ "slot": doc }));
    }
    project_scoped_events(mailbox, BUILD_SLOT, param).await
}

/// Events of `kind` whose `t`-tags include `project` (project scoping).
///
/// bd-pin-public-plane-s-easy.14: replays with the project-scoped filter
/// (`kind_project_filter`), NOT a bare kind filter — the bus access gate
/// (A3) rejects project-scoped events for global (no-`#t`) subscriptions, so
/// a bare-kind replay returned ZERO reservation events even when leases were
/// held (the daemon's `toron://reservations/{project}` looked empty; the CLI
/// guard saw false Unreserved). This is the same filter `resolved_leases`
/// uses in toron-core.
async fn project_scoped_events(
    mailbox: &Mailbox,
    kind: u16,
    project: &str,
) -> Option<serde_json::Value> {
    let filter = toron_core::bus::filter::kind_project_filter(toron_kind(kind), project);
    let events = toron_core::mailbox::collect_replay(mailbox.bus().as_ref(), filter)
        .await
        .ok()?;
    let mut docs: Vec<serde_json::Value> = events
        .into_iter()
        .filter_map(|ev| serde_json::to_value(&ev).ok())
        .collect();
    docs.sort_by_key(|d| d["created_at"].as_i64().unwrap_or(0));
    docs.reverse();
    Some(
        serde_json::json!({ "project": project, "kind": kind, "count": docs.len(), "events": docs }),
    )
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
        let templates: Vec<&str> = resource_templates().map(|(_, u, _)| *u).collect();
        assert!(!templates.is_empty());
        assert!(templates.len() < RESOURCES.len());
        assert_eq!(
            concrete_resources().count() + templates.len(),
            RESOURCES.len()
        );
    }

    #[test]
    fn concrete_resources_are_literal_uris() {
        assert!(concrete_resources().all(|(_, uri, _)| !uri.contains('{')));
        assert!(concrete_resources().all(|(_, uri, _)| read_resource_value(uri).is_some()));
        assert!(resource_uri_supported("toron://tooling/metrics"));
        assert!(resource_uri_supported("toron://agents/BlueLake/inbox"));
        assert!(!resource_uri_supported(
            "toron://agents/BlueLake/inbox/extra"
        ));
        assert!(!resource_uri_supported("toron://events/14/abc"));
    }

    #[test]
    fn notification_uris_are_concrete_and_readable() {
        use nostr::prelude::{EventBuilder, FinalizeEvent, Kind, Tag};

        let fallback = EventBuilder::new(Kind::from_u16(14), "update")
            .finalize(&toron_core::nostr::Keys::generate())
            .expect("sign test event");
        let fallback_uri = notification_resource_uri(&fallback);
        assert_eq!(fallback_uri, "toron://tooling/metrics");
        assert!(read_resource_value(&fallback_uri).is_some());

        let reservation = EventBuilder::new(Kind::from_u16(FILE_RESERVATION), "lease")
            .tags(vec![Tag::parse(["t", "demo"]).expect("project tag")])
            .finalize(&toron_core::nostr::Keys::generate())
            .expect("sign reservation event");
        let reservation_uri = notification_resource_uri(&reservation);
        assert_eq!(reservation_uri, "toron://reservations/demo");
        assert!(template_matches(
            "toron://reservations/{project}",
            &reservation_uri
        ));

        let profile = EventBuilder::new(Kind::from_u16(AGENT_PROFILE), "profile")
            .tags(vec![
                Tag::parse(["d", "demo:BlueLake"]).expect("profile tag"),
            ])
            .finalize(&toron_core::nostr::Keys::generate())
            .expect("sign profile event");
        let profile_uri = notification_resource_uri(&profile);
        assert_eq!(profile_uri, "toron://agents/BlueLake/profile");
        assert!(template_matches(
            "toron://agents/{name}/profile",
            &profile_uri
        ));
    }

    #[test]
    fn template_matching_works() {
        assert!(template_matches(
            "toron://agents/{name}/inbox",
            "toron://agents/BlueLake/inbox?unread=true"
        ));
        assert!(!template_matches(
            "toron://agents/{name}/inbox",
            "toron://agents/BlueLake/inbox/extra"
        ));
        assert!(!template_matches(
            "toron://agents/{name}/inbox",
            "toron://agents//inbox"
        ));
        assert!(!template_matches(
            "toron://agents/{name}/inbox",
            "toron://config/environment"
        ));
    }

    /// The 7 static (non-template) tooling URIs served by [`static_resource`].
    const STATIC_RESOURCE_URIS: &[&str] = &[
        "toron://config/environment",
        "toron://tooling/directory",
        "toron://tooling/metrics",
        "toron://tooling/metrics_core",
        "toron://tooling/schemas",
        "toron://tooling/locks",
        "toron://tooling/diagnostics",
    ];

    #[test]
    fn static_resources_serve_json() {
        for uri in STATIC_RESOURCE_URIS {
            assert!(
                read_resource_value(uri).is_some(),
                "static URI must serve non-None JSON: {uri}"
            );
        }
        assert!(read_resource_value("toron://bogus/uri").is_none());
    }

    /// S8 honest-stub gate (Rev 1 wording, bd-p0-s8-resources-stub-gate-rrfz):
    /// EVERY static tooling stub whose fields are placeholders ("not wired",
    /// "not started", "see …", "…") MUST carry a `status` field naming the
    /// live tool that serves the real value. Placeholder strings without a
    /// `status` field = RED.
    #[test]
    fn static_stubs_name_live_tool() {
        let placeholder_marker = |s: &str| {
            let l = s.to_ascii_lowercase();
            l.contains("not wired")
                || l.contains("not started")
                || l.contains("see ")
                || l.contains("tbd")
                || l.contains("todo")
                || l.contains("placeholder")
        };
        for uri in STATIC_RESOURCE_URIS {
            let v = read_resource_value(uri)
                .unwrap_or_else(|| panic!("static URI must resolve: {uri}"));
            let obj = v.as_object().expect("static value is an object");
            let has_placeholder = obj
                .values()
                .any(|val| val.as_str().map(placeholder_marker).unwrap_or(false));
            if has_placeholder {
                let status = obj
                    .get("status")
                    .and_then(|s| s.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty());
                assert!(
                    status.is_some(),
                    "S8 gate RED: {uri} has placeholder fields but no `status` naming the live tool: {obj:?}"
                );
            }
        }
    }

    /// S8 gate (Rev 1): all 25 resource templates resolve to NON-EMPTY JSON —
    /// either real static data or an honest discovery stub that names the
    /// live tool. `{param}` slots are substituted with a sample value.
    #[test]
    fn all_25_resources_resolve_to_non_empty_json() {
        fn concrete(template: &str) -> String {
            // Substitute the first {param} with a sample value; remaining
            // params get "demo" (all templates have ≤2 params).
            let mut out = template.to_string();
            for p in [
                "slug",
                "project",
                "key",
                "name",
                "id",
                "slot",
                "agent",
                "window_seconds",
            ] {
                out = out.replace(&format!("{{{p}}}"), "demo");
            }
            out.replace("{", "demo").replace("}", "")
        }
        assert_eq!(RESOURCES.len(), 25);
        for (name, template, _) in RESOURCES {
            let uri = concrete(template);
            let v =
                read_resource_value(&uri).unwrap_or_else(|| panic!("{name}: {uri} must resolve"));
            let non_empty = match &v {
                serde_json::Value::Object(o) => !o.is_empty(),
                serde_json::Value::Array(a) => !a.is_empty(),
                _ => true,
            };
            assert!(non_empty, "{name}: {uri} resolves to empty JSON");
            // Stub entries (not real static data) must name the live tool.
            let obj = v.as_object().expect("object");
            let is_stub = obj.get("status").is_some();
            if is_stub {
                let status = obj["status"].as_str().unwrap_or("").trim();
                assert!(
                    !status.is_empty(),
                    "{name}: stub `status` must name the live tool"
                );
            }
        }
    }

    /// ADR-0007 F10 / R4.4: data-backed resource URIs project LIVE mailbox +
    /// index data (agents, inbox, messages, threads, projects, ack-overdue),
    /// not the "live_via_tools" discovery stub.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn data_backed_resources_project_mailbox_data() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::mailbox::{Mailbox, SendMessage};
        use toron_core::nostr::identity::AgentProfile;

        const PASSPHRASE: &str = "res-pass";
        let db = std::env::temp_dir().join(format!("res-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: std::sync::Arc<dyn toron_core::bus::Bus> = std::sync::Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir = std::env::temp_dir().join(format!("res-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(
            bus.clone(),
            data_dir.clone(),
            Some(toron_core::keys::generate()),
        )
        .with_agent_passphrase(PASSPHRASE)
        .with_index_pool(pool);
        mailbox
            .register_agent("demo", &AgentProfile::new("workflow-demo"))
            .await
            .expect("register sender");
        mailbox
            .register_agent("demo", &AgentProfile::new("BlueLake"))
            .await
            .expect("register recipient");
        let handle = mailbox.spawn_unwrap_loop();
        let sent = mailbox
            .send_message(&SendMessage {
                project_slug: "demo".into(),
                sender_name: "workflow-demo".into(),
                to: vec!["BlueLake".into()],
                subject: "res subject".into(),
                body_md: "res body".into(),
                cc: vec![],
                bcc: vec![],
                importance: None,
                ack_required: true,
                thread_id: Some("res-thread-1".into()),
                sender_token: None,
                overseer: false,
                extra_tags: vec![],
            })
            .await
            .expect("send");
        assert_eq!(sent.count, 1, "delivered");
        // Poll the projection to completion. The signal is the messages
        // TABLE (toron://messages/{mid}), NOT the inbox resource: inbox
        // replays the bus and sees the wrap BEFORE the unwrap loop's argon2
        // KDF (log_n=16, ~4.4s/key) lands the SQLite row. A fixed sleep is
        // never enough under parallel load (bd-projection-flake-gyox).
        async {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
            loop {
                if let Some(inbox) =
                    project_resource_value(&mailbox, "toron://agents/BlueLake/inbox").await
                    && let Some(id) = inbox["messages"]
                        .as_array()
                        .and_then(|a| a.first())
                        .and_then(|m| m["id"].as_str())
                    && let Some(by_id) =
                        project_resource_value(&mailbox, &format!("toron://messages/{id}")).await
                    && by_id["id"].as_str() == Some(id)
                {
                    return;
                }
                if std::time::Instant::now() >= deadline {
                    panic!("mail never projected into the messages table (90s poll deadline)");
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        }
        .await;

        // agents/{project} → live registration list.
        let agents = project_resource_value(&mailbox, "toron://agents/demo")
            .await
            .expect("agents projection");
        let names: Vec<&str> = agents
            .as_array()
            .expect("array")
            .iter()
            .filter_map(|a| a["name"].as_str())
            .collect();
        assert!(
            names.contains(&"BlueLake") && names.contains(&"workflow-demo"),
            "agents/demo lists registered agents: {names:?}"
        );

        // agents/{name}/inbox → the projected message (exercised by the poll
        // above; mid feeds the remaining index URIs).

        // threads/{id} → thread members.
        let thread = project_resource_value(&mailbox, "toron://threads/res-thread-1")
            .await
            .expect("thread projection");
        assert!(
            thread["messages"].as_array().map(Vec::len) == Some(1),
            "thread has the one message"
        );

        // views/ack-overdue/{agent} → the ack-required message (sender-side:
        // the index stores senders, not recipients).
        let overdue = project_resource_value(&mailbox, "toron://views/ack-overdue/workflow-demo")
            .await
            .expect("ack-overdue projection");
        assert!(
            overdue["messages"].as_array().map(|a| !a.is_empty()) == Some(true),
            "ack-required message surfaces as overdue"
        );

        // project/{slug} → live manifest state.
        let project = project_resource_value(&mailbox, "toron://project/demo")
            .await
            .expect("project projection");
        assert_eq!(project["slug"].as_str(), Some("demo"));

        // reservations/{project} → project-scoped kind-30031 events (empty ok).
        let reservations = project_resource_value(&mailbox, "toron://reservations/demo")
            .await
            .expect("reservations projection");
        assert_eq!(
            reservations["count"].as_u64(),
            Some(0),
            "no reservations registered"
        );

        // slots/{project} → project-scoped kind-30032 events (empty ok).
        let slots = project_resource_value(&mailbox, "toron://slots/demo")
            .await
            .expect("slots projection");
        assert_eq!(slots["count"].as_u64(), Some(0), "no slots leased");

        // identity/{project} → the registered agents.
        let identity = project_resource_value(&mailbox, "toron://identity/demo")
            .await
            .expect("identity projection");
        assert_eq!(
            identity.as_array().map(Vec::len),
            Some(2),
            "identity lists both registered agents"
        );

        // agents/{name}/outbox → the sent message (sender-side index scan).
        let outbox = project_resource_value(&mailbox, "toron://agents/workflow-demo/outbox")
            .await
            .expect("outbox projection");
        assert!(
            outbox["messages"].as_array().map(|a| !a.is_empty()) == Some(true),
            "outbox has the sent message"
        );

        // mailbox/{name} → health + inbox summary.
        let mbx = project_resource_value(&mailbox, "toron://mailbox/BlueLake")
            .await
            .expect("mailbox projection");
        assert_eq!(mbx["agent"].as_str(), Some("BlueLake"));
        assert!(mbx["health"].is_object(), "health object present");

        // Static tooling URIs keep serving (no mailbox needed).
        assert!(
            project_resource_value(&mailbox, "toron://tooling/metrics")
                .await
                .is_none(),
            "static tooling URI is not data-projected (falls back to static)"
        );
        assert!(read_resource_value("toron://tooling/metrics").is_some());

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// P2 (bd-p2-unwrap-or-zero-coercion-5ac3): a DB error in the index scan
    /// must PROPAGATE, not mask as an empty result set (an operator seeing
    /// "0 messages" while the DB is down is a silent-failure mask).
    #[tokio::test]
    async fn index_scan_db_error_propagates_not_masked() {
        use std::sync::Arc;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db =
            std::env::temp_dir().join(format!("res-index-scan-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
        schema::migrate(&pool).await.expect("migrate");
        let bus = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("res-index-scan-data-{}", uuid::Uuid::new_v4()));
        let mailbox =
            toron_core::mailbox::Mailbox::new(bus, data_dir, None).with_index_pool(pool.clone());

        pool.close().await; // break the index pool

        let res = index_scan(
            &mailbox,
            "SELECT event_id, subject, sender, created_ts_us, importance, ack_required, thread_id, body \
             FROM messages WHERE sender = ?1 ORDER BY created_ts_us DESC",
            &["nobody"],
            10,
            false,
        )
        .await;
        assert!(
            res.is_err(),
            "index scan DB error must propagate (not mask as empty): {res:?}"
        );
        let _ = std::fs::remove_file(&db);
    }
}
