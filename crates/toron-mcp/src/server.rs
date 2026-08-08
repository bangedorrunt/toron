// governed-by: ADR-0003
#![forbid(unsafe_code)]

use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ErrorData, Implementation, InitializeResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse, ReadResourceResult, Resource,
    ResourceContents, ResourceTemplate, ResourceUpdatedNotification,
    ResourceUpdatedNotificationParam, ServerCapabilities, ServerInfo, SubscriptionFilter, Tool,
};
use rmcp::service::{RequestContext, RoleServer, SubscriptionContext};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, ServiceExt};
use toron_core::bus::Bus;
use toron_core::mailbox::Mailbox;

use crate::resources::{RESOURCES, read_resource_value};
use crate::tools::ToolServer;

/// MCP server for toron.
///
/// Holds an `Arc<dyn Bus>` (toron-core) that backs data resources and the
/// SEP-2575 subscription push, plus the [`ToolServer`] exposing the 38
/// AM-Rust parity tools (bd-led.7.2).
#[derive(Clone, Default)]
pub struct ToronServer {
    bus: Option<Arc<dyn Bus>>,
    tools: Option<ToolServer>,
}

impl std::fmt::Debug for ToronServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToronServer")
            .field("bus", &self.bus.is_some())
            .field("tools", &self.tools.is_some())
            .finish()
    }
}

impl ToronServer {
    /// Create a new toron MCP server.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the in-process bus used to back data resources + subscriptions.
    /// Also wires a mailbox over the same bus so the 38 tools are served.
    pub fn with_bus(mut self, bus: Arc<dyn Bus>) -> Self {
        let data_dir = std::env::temp_dir().join("toron-mcp-demo");
        let mailbox = Mailbox::new(bus.clone(), data_dir, None);
        self.bus = Some(bus);
        self.tools = Some(ToolServer::new(Arc::new(mailbox)));
        self
    }

    /// Attach a fully-wired mailbox (bus + index pool) backing the tool
    /// surface. The bus is shared so resource/subscription features work too.
    pub fn with_mailbox(mut self, mailbox: Mailbox) -> Self {
        self.bus = Some(mailbox.bus().clone());
        self.tools = Some(ToolServer::new(Arc::new(mailbox)));
        self
    }

    /// The attached bus, if any.
    pub fn bus(&self) -> Option<&Arc<dyn Bus>> {
        self.bus.as_ref()
    }

    /// The attached tool surface, if any.
    pub fn tools(&self) -> Option<&ToolServer> {
        self.tools.as_ref()
    }

    /// Server capabilities advertised via `initialize`/`discover`.
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

impl ServerHandler for ToronServer {
    fn get_info(&self) -> ServerInfo {
        // rmcp auto-negotiates: prefers V_2026_07_28, falls back to V_2025_11_25.
        InitializeResult::new(Self::capabilities())
            .with_protocol_version(ProtocolVersion::V_2026_07_28)
            .with_server_info(Implementation::new("toron", env!("CARGO_PKG_VERSION")))
    }

    /// The 38 AM-Rust parity tools (bd-led.7.2), delegated to `ToolServer`.
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        match &self.tools {
            Some(tools) => tools.list_tools(request, context).await,
            None => Ok(ListToolsResult::default()),
        }
    }

    /// Tool dispatch, delegated to `ToolServer`.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        match &self.tools {
            Some(tools) => tools.call_tool(request, context).await,
            None => Err(ErrorData::method_not_found::<
                rmcp::model::CallToolRequestMethod,
            >()),
        }
    }

    /// Tool definition lookup, delegated to `ToolServer`.
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tools.as_ref().and_then(|tools| tools.get_tool(name))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let resources: Vec<Resource> = RESOURCES
            .iter()
            .map(|(name, uri, desc)| {
                Resource::new(uri.to_string(), name.to_string())
                    .with_title(name.to_string())
                    .with_description(desc.to_string())
                    .with_mime_type("application/json")
            })
            .collect();
        Ok(ListResourcesResult::with_all_items(resources))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, ErrorData> {
        let templates: Vec<ResourceTemplate> = RESOURCES
            .iter()
            .filter(|(_, uri, _)| uri.contains('{'))
            .map(|(name, uri, desc)| {
                ResourceTemplate::new(uri.to_string(), name.to_string())
                    .with_title(name.to_string())
                    .with_description(desc.to_string())
                    .with_mime_type("application/json")
            })
            .collect();
        Ok(ListResourceTemplatesResult::with_all_items(templates))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let uri = request.uri.as_str();
        match read_resource_value(uri) {
            Some(value) => {
                let text = serde_json::to_string_pretty(&value).map_err(|e| {
                    ErrorData::internal_error(format!("serialize resource: {e}"), None)
                })?;
                let contents = ResourceContents::text(text, uri).with_mime_type("application/json");
                Ok(ReadResourceResponse::Complete(ReadResourceResult::new(
                    vec![contents],
                )))
            }
            None => Err(ErrorData::invalid_params(
                format!("resource not found: {uri}"),
                None,
            )),
        }
    }

    /// Accept subscriptions for resource updates + tool_list_changed.
    fn accepted_subscription_filter(
        &self,
        requested: &SubscriptionFilter,
    ) -> Option<SubscriptionFilter> {
        let mut accepted = SubscriptionFilter::builder().tools_list_changed();
        if let Some(uris) = &requested.resource_subscriptions {
            let keep: Vec<String> = uris
                .iter()
                .filter(|u| {
                    RESOURCES
                        .iter()
                        .any(|(_, t, _)| crate::resources::template_matches(t, u))
                })
                .cloned()
                .collect();
            if !keep.is_empty() {
                accepted = accepted.resource_subscriptions(keep);
            }
        }
        Some(accepted.build())
    }

    /// Wire the bus to the subscription sink: push matching events <50ms.
    async fn listen(&self, context: SubscriptionContext) -> Result<(), ErrorData> {
        let Some(bus) = self.bus() else {
            context.cancelled().await;
            return Ok(());
        };
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .map_err(|e| ErrorData::internal_error(format!("bus subscribe: {e}"), None))?;
        loop {
            tokio::select! {
                _ = context.cancelled() => return Ok(()),
                ev = sub.next() => {
                    let Some(ev) = ev else { return Ok(()) };
                    if let toron_core::bus::SubscriptionEvent::Event(event) = ev {
                        let uri = format!("toron://events/{}/{}", event.kind.as_u16(), event.id);
                        let notification =
                            ResourceUpdatedNotification::new(ResourceUpdatedNotificationParam::new(uri));
                        if context.sink().send(notification.into()).await.is_err() {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Build the demo mailbox backing the tool surface for `router`/`serve_stdio`
/// and the robot CLI: an in-process bus over a per-call SQLite index in the
/// system temp dir. Public so `toron-cli` robot commands construct the exact
/// same mailbox + `ToJson` shapes the MCP tools use (JSON parity, bd-led.8.3).
pub async fn demo_mailbox() -> anyhow::Result<Mailbox> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let stamp = format!("{}-{}", std::process::id(), seq);
    let db = std::env::temp_dir().join(format!("toron-mcp-{stamp}.sqlite"));
    let pool = toron_core::index::schema::connect(db.to_str().unwrap()).await?;
    toron_core::index::schema::migrate(&pool).await?;
    let bus = toron_core::bus::InProcessBus::open(pool.clone(), Default::default()).await?;
    // Per-invocation data_dir by default: the unwrap loop scans this dir for
    // agent keys and runs the argon2 KDF (~1s/key) on every rebuild. A fixed
    // dir accumulates keys across runs until the loop stalls for minutes on
    // dozens of keys. A unique dir isolates each daemon and bounds the
    // rebuild cost to just that daemon's own agents.
    //
    // `TORON_MCP_DATA_DIR` overrides: tests that need a data_dir to persist
    // ACROSS processes (cli_robot registers in-process then shells out to the
    // built binary, which constructs its own demo_mailbox) set it to a shared
    // per-run dir so both sides see the same agent keys.
    let data_dir = match std::env::var_os("TORON_MCP_DATA_DIR") {
        Some(dir) => {
            let d = std::path::PathBuf::from(dir);
            std::fs::create_dir_all(&d)?;
            d
        }
        None => std::env::temp_dir().join(format!("toron-mcp-demo-{stamp}")),
    };
    let mailbox = Mailbox::new(Arc::new(bus), data_dir, operator_keys()?).with_index_pool(pool);
    // ADR-0004 inbound unwrap loop (bd-pkc): make the G14 contact-policy gate,
    // the operator Overseer bypass, and the idempotent projection LIVE in the
    // daemon. Agent keys are created on-disk at register/create_agent_identity,
    // so the loop discovers them at runtime and rebuilds on change.
    mailbox.spawn_unwrap_loop();
    Ok(mailbox)
}

/// Load the operator key for the MCP server from env, if configured.
///
/// Honors `TORON_OPERATOR_KEY` (a **path**, never a secret on the
/// command line — CLI design) + `TORON_KEY_PASSPHRASE` (the SAME env var
/// `toron key generate` uses via `key_password()` in toron-cli/src/operator.rs,
/// default "toron") — they MUST agree, or a freshly-generated operator key
/// will not decrypt at serve time (aead::Error on passphrase mismatch).
/// Without an operator the parity-privileged ops (`ensure_project`, `macro_start_session`,
/// product ops, force-release) fail with "no operator key configured", which
/// the real-e2e parity test (bd-e2e / opencode agent probe) surfaced as a
/// blocking defect. daemon hosts MUST set these so the full 38-tool surface
/// is exercisable.*/
pub(crate) fn operator_keys() -> anyhow::Result<Option<toron_core::keys::Keys>> {
    let Some(key_os) = std::env::var_os("TORON_OPERATOR_KEY") else {
        return Ok(None);
    };
    let path = std::path::PathBuf::from(key_os);
    if !path.is_file() {
        anyhow::bail!(
            "TORON_OPERATOR_KEY={} is not a file (operator ncryptsec)",
            path.display()
        );
    }
    let pass = std::env::var("TORON_KEY_PASSPHRASE").unwrap_or_else(|_| "toron".to_string());
    toron_core::keys::load_encrypted_key(&path, &pass)
        .map(Some)
        .map_err(|e| anyhow::anyhow!("failed to load operator key from {}: {e}", path.display()))
}

/// Serve the toron MCP server over stdio (transport `transport-io`).
///
/// Blocks until the stdio connection closes.
pub async fn serve_stdio() -> anyhow::Result<()> {
    let server = ToronServer::new().with_mailbox(demo_mailbox().await?);
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

/// Build the axum router with the Streamable HTTP transport mounted at `/mcp`.
///
/// SEP-2567 stateless mode is enabled (`legacy_session_mode(false)`) so the
/// daemon/federation path is horizontally scalable with no session affinity.
/// The served handler carries the 38 AM-Rust parity tools (bd-led.7.2).
pub async fn router() -> anyhow::Result<axum::Router> {
    let server = ToronServer::new().with_mailbox(demo_mailbox().await?);
    let service: StreamableHttpService<ToronServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(server.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                // 2026-07-28 clients are served statelessly per SEP-2567
                // regardless of this flag; keep legacy mode enabled so
                // pre-2026-07-28 clients still complete a session lifecycle.
                .with_json_response(true),
        );
    // ADR-0006 (bd-esh.5): mount the Blossom blob-serving surface at
    // `/.blossom` (BUD-01 retrieval + BUD-02 upload) over a per-daemon blob
    // store rooted at the demo data dir's `blobs/`.
    let blob_root = std::env::temp_dir().join("toron-mcp-demo").join("blobs");
    let blossom = crate::blob_serve::blossom_router(
        toron_core::archive::blob::BlobStore::new(blob_root),
    );
    Ok(axum::Router::new()
        .nest_service("/mcp", service)
        .merge(blossom))
}

/// Serve the toron MCP server over Streamable HTTP on `bind_addr`
/// (e.g. `127.0.0.1:8000`).
pub async fn serve_http(bind_addr: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "toron MCP Streamable HTTP listening at /mcp");
    axum::serve(listener, router().await?).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_advertise_tools_and_resources() {
        let caps = ToronServer::capabilities();
        assert!(caps.tools.is_some(), "tools capability must be enabled");
        assert!(
            caps.resources.is_some(),
            "resources capability must be enabled"
        );
    }

    #[test]
    fn resource_surface_matches_fixture_count() {
        assert_eq!(RESOURCES.len(), 25);
    }
}
