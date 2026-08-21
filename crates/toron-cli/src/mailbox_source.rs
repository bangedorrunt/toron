// governed-by: ADR-0003
// governed-by: ADR-0017 (project-root canonicalization — the daemon is the
// canonical writer; the CLI robot path is a client of it)

//! One-mailbox daemon client (bd-pin-public-plane-s-easy.14).
//!
//! The robot path (`mail`, `reserve`, `slot`, `contact`, `guard check`) must
//! NOT always build a second in-process `demo_mailbox()` — flywheel reserves
//! through the daemon at `127.0.0.1:18080`, so a CLI-side second store sees
//! an empty reservation roster and the guard false-positives every path as
//! Unreserved. This module is the SAME mailbox selector flywheel uses:
//!
//! 1. `TORON_MCP_URL` env — explicit daemon URL.
//! 2. Default `http://127.0.0.1:18080/mcp` when that port serves.
//! 3. flywheel `[mail.toron] url` (best-effort config read).
//! 4. Otherwise: embed once against the durable XDG root the daemon would
//!    use — never a third dir.
//!
//! The daemon is the writer; the CLI is a client. No second SQLite.

use std::sync::Arc;

use anyhow::Context;
use rmcp::ClientServiceExt;
use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, ProtocolVersion,
    ReadResourceRequestParams, ResourceContents,
};
use rmcp::service::{ClientLifecycleMode, RoleClient, RunningService};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use serde_json::Value;

/// The default toron daemon MCP endpoint (flywheel `[mail.toron]` default).
pub const DEFAULT_DAEMON_URL: &str = "http://127.0.0.1:18080/mcp";

/// Hard ceiling for one daemon tool call (bd-gtqe). A wedged daemon used to
/// park robot CLIs silently for 120s+ with zero output; now the call fails
/// loudly instead. `TORON_TOOL_TIMEOUT_MS` overrides for slow operations.
pub const TOOL_CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Ceiling for the daemon `initialize` handshake (bd-gtqe).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

fn tool_call_timeout() -> std::time::Duration {
    std::env::var("TORON_TOOL_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or(TOOL_CALL_TIMEOUT)
}

/// One live toron daemon connection (rmcp client over Streamable HTTP).
///
/// `call_tool` returns the parsed JSON of the first text content block —
/// the MCP tool result IS the robot path's parity JSON. `read_resource`
/// returns the parsed JSON of the first text resource content block.
#[derive(Clone)]
pub struct DaemonClient {
    client: Arc<RunningService<RoleClient, ClientInfo>>,
}

impl DaemonClient {
    /// Connect to a daemon at `url` (e.g. `http://127.0.0.1:18080/mcp`).
    /// `url` may be a bare base (`http://127.0.0.1:18080`); `/mcp` is
    /// appended when absent (flywheel `mcp_http` parity).
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let base = url.trim_end_matches('/');
        let endpoint = if base.ends_with("/mcp") {
            base.to_string()
        } else {
            format!("{base}/mcp")
        };
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint),
        );
        let client_info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::from_build_env(),
        );
        let client = tokio::time::timeout(
            CONNECT_TIMEOUT,
            client_info.serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![
                        ProtocolVersion::V_2026_07_28,
                        ProtocolVersion::V_2025_11_25,
                    ],
                },
            ),
        )
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "daemon connect timed out after {}s (is `toron serve --mcp-http` running and responsive?)",
                CONNECT_TIMEOUT.as_secs()
            )
        })?
        .context("daemon connect (is `toron serve --mcp-http` running?)")?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Call an MCP tool by name, returning the parsed JSON payload of the
    /// first text content block. Tool errors surface as anyhow errors.
    ///
    /// Bounded by [`tool_call_timeout`] (bd-gtqe): a daemon that accepts the
    /// request but never answers produces a loud timeout error instead of an
    /// unbounded silent park. Use [`Self::call_tool_within`] for paths that
    /// need a tighter deadline.
    pub async fn call_tool(&self, tool: &str, args: Value) -> anyhow::Result<Value> {
        self.call_tool_within(tool, args, tool_call_timeout()).await
    }

    /// [`Self::call_tool`] with an explicit deadline (bd-gtqe).
    pub async fn call_tool_within(
        &self,
        tool: &str,
        args: Value,
        deadline: std::time::Duration,
    ) -> anyhow::Result<Value> {
        let params = CallToolRequestParams::new(tool.to_string())
            .with_arguments(args.as_object().cloned().unwrap_or_default());
        let result = tokio::time::timeout(deadline, self.client.call_tool(params))
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "call_tool({tool}) timed out after {}s — daemon unresponsive (bd-gtqe loud-fail; was a silent hang)",
                    deadline.as_secs()
                )
            })?
            .with_context(|| format!("call_tool({tool}) transport error"))?;
        if result.is_error.unwrap_or(false) {
            anyhow::bail!("tool {tool} returned an error result: {:?}", result.content);
        }
        Self::text_json(tool, &result.content)
    }

    /// Read an MCP resource by URI (`toron://...`), returning the parsed
    /// JSON of the first text content block.
    pub async fn read_resource(&self, uri: &str) -> anyhow::Result<Value> {
        let result = self
            .client
            .read_resource(ReadResourceRequestParams::new(uri))
            .await
            .with_context(|| format!("read_resource({uri}) transport error"))?;
        let text = result
            .contents
            .iter()
            .find_map(|c| match c {
                ResourceContents::TextResourceContents { text, .. } => Some(text.clone()),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("resource {uri} returned no text content"))?;
        serde_json::from_str::<Value>(&text)
            .with_context(|| format!("resource {uri} text is not JSON"))
    }

    /// Extract + parse the first text content block of a CallToolResult.
    fn text_json(tool: &str, content: &[rmcp::model::ContentBlock]) -> anyhow::Result<Value> {
        let text = content
            .iter()
            .find_map(|c| c.as_text().map(|t| t.text.clone()))
            .ok_or_else(|| anyhow::anyhow!("tool {tool} returned no text content"))?;
        serde_json::from_str::<Value>(&text)
            .with_context(|| format!("tool {tool} text is not JSON: {text}"))
    }
}

/// The mailbox source for the robot path (bd-pin-public-plane-s-easy.14).
///
/// `Daemon(url)` when a toron MCP HTTP endpoint is configured or serving
/// (env `TORON_MCP_URL` → default port probe → flywheel `[mail.toron] url`);
/// `Embedded` otherwise — the durable XDG root the daemon would use.
#[derive(Clone)]
pub enum MailboxSource {
    /// The daemon at this MCP URL (the writer; the CLI is a client).
    Daemon(DaemonClient),
    /// In-process mailbox against the durable XDG root (`demo_mailbox`
    /// production mode — never a third dir).
    Embedded(Arc<toron_core::mailbox::Mailbox>),
}

impl MailboxSource {
    /// Resolve the robot path's mailbox source with the flywheel selector.
    ///
    /// Precedence (explicit wins over implicit):
    /// 1. `TORON_MCP_URL` — explicit daemon URL.
    /// 2. `TORON_MCP_DATA_DIR` — explicit durable-root pin (test isolation;
    ///    the daemon honors the same env when launched with it). Pinned
    ///    roots beat the implicit default-port probe: a caller that sets
    ///    one has said "use THIS root", and the probe would silently route
    ///    to an unrelated live daemon.
    /// 3. Default `127.0.0.1:18080` when that port serves.
    /// 4. flywheel `[mail.toron] url`.
    /// 5. Embedded against the durable XDG root.
    pub async fn resolve() -> anyhow::Result<Self> {
        if let Some(url) = daemon_url_from_env() {
            return Ok(Self::Daemon(DaemonClient::connect(&url).await?));
        }
        if std::env::var_os("TORON_MCP_DATA_DIR").is_some() {
            return Ok(Self::Embedded(Arc::new(toron_mcp::demo_mailbox().await?)));
        }
        if let Some(url) = default_daemon_if_serving().or_else(flywheel_toron_url) {
            return Ok(Self::Daemon(DaemonClient::connect(&url).await?));
        }
        Ok(Self::Embedded(Arc::new(toron_mcp::demo_mailbox().await?)))
    }
}

/// `TORON_MCP_URL` env override.
fn daemon_url_from_env() -> Option<String> {
    std::env::var("TORON_MCP_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

/// The default daemon endpoint when the port serves (TCP probe, short
/// timeout — fail-soft: a down daemon just falls through to embedded).
fn default_daemon_if_serving() -> Option<String> {
    let host = DEFAULT_DAEMON_URL
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:18080");
    let (addr, port) = host.split_once(':').unwrap_or((host, "18080"));
    let probe = format!("{addr}:{port}");
    std::net::TcpStream::connect_timeout(
        &probe.parse().ok()?,
        std::time::Duration::from_millis(200),
    )
    .ok()
    .map(|_| DEFAULT_DAEMON_URL.to_string())
}

/// flywheel `[mail.toron] url` (best-effort: `~/.config/flywheel/config.toml`).
fn flywheel_toron_url() -> Option<String> {
    let home = std::env::var_os("HOME")?;
    let path = std::path::Path::new(&home)
        .join(".config")
        .join("flywheel")
        .join("config.toml");
    let raw = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&raw).ok()?;
    value
        .get("mail")?
        .get("toron")?
        .get("url")?
        .as_str()
        .map(|s| s.to_string())
}
