// governed-by: ADR-0003
#![forbid(unsafe_code)]

use std::sync::Arc;

use rmcp::model::{
    ErrorData, Implementation, InitializeResult, ListResourceTemplatesResult, ListResourcesResult,
    PaginatedRequestParams, ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse,
    ReadResourceResult, Resource, ResourceContents, ResourceTemplate, ResourceUpdatedNotification,
    ResourceUpdatedNotificationParam, ServerCapabilities, ServerInfo, SubscriptionFilter,
};
use rmcp::service::{RequestContext, RoleServer, SubscriptionContext};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ServerHandler, ServiceExt};
use toron_core::bus::Bus;

use crate::resources::{read_resource_value, RESOURCES};

/// MCP server for toron.
///
/// Holds an `Arc<dyn Bus>` (toron-core) that backs data resources and the
/// SEP-2575 subscription push. `bd-led.7.2` adds `#[tool]` methods.
#[derive(Clone, Default)]
pub struct ToronServer {
    bus: Option<Arc<dyn Bus>>,
}

impl std::fmt::Debug for ToronServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToronServer")
            .field("bus", &self.bus.is_some())
            .finish()
    }
}

impl ToronServer {
    /// Create a new toron MCP server.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the in-process bus used to back data resources + subscriptions.
    pub fn with_bus(mut self, bus: Arc<dyn Bus>) -> Self {
        self.bus = Some(bus);
        self
    }

    /// The attached bus, if any.
    pub fn bus(&self) -> Option<&Arc<dyn Bus>> {
        self.bus.as_ref()
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

/// Serve the toron MCP server over stdio (transport `transport-io`).
///
/// Blocks until the stdio connection closes.
pub async fn serve_stdio() -> anyhow::Result<()> {
    let server = ToronServer::new();
    let running = server.serve(rmcp::transport::stdio()).await?;
    running.waiting().await?;
    Ok(())
}

/// Build the axum router with the Streamable HTTP transport mounted at `/mcp`.
///
/// SEP-2567 stateless mode is enabled (`legacy_session_mode(false)`) so the
/// daemon/federation path is horizontally scalable with no session affinity.
pub fn router() -> axum::Router {
    let service: StreamableHttpService<ToronServer, LocalSessionManager> =
        StreamableHttpService::new(
            || Ok(ToronServer::new()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                // 2026-07-28 clients are served statelessly per SEP-2567
                // regardless of this flag; keep legacy mode enabled so
                // pre-2026-07-28 clients still complete a session lifecycle.
                .with_json_response(true),
        );
    axum::Router::new().nest_service("/mcp", service)
}

/// Serve the toron MCP server over Streamable HTTP on `bind_addr`
/// (e.g. `127.0.0.1:8000`).
pub async fn serve_http(bind_addr: &str) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    tracing::info!(%bind_addr, "toron MCP Streamable HTTP listening at /mcp");
    axum::serve(listener, router()).await?;
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
