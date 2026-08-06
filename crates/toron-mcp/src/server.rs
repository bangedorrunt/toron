// governed-by: ADR-0003
#![forbid(unsafe_code)]

use std::sync::Arc;

use rmcp::model::{
    Implementation, InitializeResult, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::ServerHandler;
use rmcp::ServiceExt;

/// MCP server for toron.
///
/// Holds the `Arc<Mailbox>` (toron-core) once the mailbox lands; `bd-led.7.2`
/// adds `#[tool]` methods, `bd-led.7.3` adds `#[resource]` + subscriptions.
#[derive(Debug, Clone, Default)]
pub struct ToronServer {
    // mailbox: Arc<Mailbox>, // wired by bd-led.7.2
}

impl ToronServer {
    /// Create a new toron MCP server.
    pub fn new() -> Self {
        Self::default()
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
}
