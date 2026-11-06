// governed-by: ADR-0003
// governed-by: ADR-0017 (project-root canonicalization — the daemon is the

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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use rmcp::ClientServiceExt;
use rmcp::model::{
    CallToolRequestParams, ClientCapabilities, ClientConfig, Implementation, ProtocolVersion,
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

/// Ceiling for the daemon `server/discover` handshake (bd-gtqe).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Why the daemon could not be reached (wave5-b): connection refused or a
/// timeout, typed at the origin so callers match the type instead of
/// grepping remediation prose. `Display` keeps today's remediation text
/// byte-for-byte (flywheel matches on it cross-repo).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonUnreachable {
    /// Normalized `http://host:port/mcp` endpoint.
    pub endpoint: String,
    /// Which reachability failure this was.
    pub kind: UnreachableKind,
}

/// The two ways a daemon is unreachable: refused up front, or silent past
/// the deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnreachableKind {
    /// TCP-level refusal (nothing listening).
    Refused,
    /// Accepted (or unanswered) but silent past the deadline.
    Timeout {
        /// The tool call that hung, if this was a tool call.
        tool: Option<String>,
        secs: u64,
    },
}

impl std::fmt::Display for DaemonUnreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            UnreachableKind::Refused => write!(
                f,
                "daemon unreachable at {} (connection refused — is `toron serve --mcp-http` running?)",
                self.endpoint
            ),
            UnreachableKind::Timeout { tool: None, secs } => write!(
                f,
                "daemon connect timed out after {secs}s (is `toron serve --mcp-http` running and responsive?)"
            ),
            UnreachableKind::Timeout {
                tool: Some(tool),
                secs,
            } => write!(
                f,
                "call_tool({tool}) timed out after {secs}s — daemon unresponsive (bd-gtqe loud-fail; was a silent hang)"
            ),
        }
    }
}

impl std::error::Error for DaemonUnreachable {}

/// Classify a failed daemon `connect` as a typed refusal when the anyhow
/// chain carries an `io::Error` with `ConnectionRefused` (type match, never
/// prose grep). Returns false for failures that are not a refusal — those
/// keep their existing context.
fn is_refused(err: &anyhow::Error) -> bool {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
        .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
}

/// Parse-or-refuse form: a set-but-garbage `TORON_TOOL_TIMEOUT_MS` is a
/// [`ConfigError`](toron_core::config::ConfigError), never a silent default.
fn try_tool_call_timeout() -> Result<std::time::Duration, toron_core::config::ConfigError> {
    Ok(toron_core::config::parse_env_u64("TORON_TOOL_TIMEOUT_MS")?
        .map(std::time::Duration::from_millis)
        .unwrap_or(TOOL_CALL_TIMEOUT))
}

/// One live toron daemon connection (rmcp client over Streamable HTTP).
///
/// `call_tool` returns the parsed JSON of the first text content block —
/// the MCP tool result IS the robot path's parity JSON. `read_resource`
/// returns the parsed JSON of the first text resource content block.
#[derive(Clone)]
pub struct DaemonClient {
    client: Arc<RunningService<RoleClient, ClientConfig>>,
    endpoint: String,
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
        // wave8-b: first-hand refusal check. rmcp wraps the transport error
        // message-only, so the chain walk in `is_refused` never sees the
        // RST; a bounded TCP open classifies refusal at the origin instead.
        // Only a definite refusal short-circuits — anything else (timeout,
        // DNS, unreachable) falls through to the normal handshake path.
        if refused_now(&endpoint) {
            return Err(anyhow::Error::new(DaemonUnreachable {
                endpoint: endpoint.clone(),
                kind: UnreachableKind::Refused,
            }));
        }
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(endpoint.clone()),
        );
        let client_info = ClientConfig::new(
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
            anyhow::Error::new(DaemonUnreachable {
                endpoint: endpoint.clone(),
                kind: UnreachableKind::Timeout {
                    tool: None,
                    secs: CONNECT_TIMEOUT.as_secs(),
                },
            })
            .context(format!(
                "daemon connect timed out after {}s (is `toron serve --mcp-http` running and responsive?)",
                CONNECT_TIMEOUT.as_secs()
            ))
        })?
        .context("daemon connect (is `toron serve --mcp-http` running?)")
        .map_err(|e| {
            if is_refused(&e) {
                anyhow::Error::new(DaemonUnreachable {
                    endpoint: endpoint.clone(),
                    kind: UnreachableKind::Refused,
                })
                .context(format!("{e:#}"))
            } else {
                e
            }
        })?;
        Ok(Self {
            client: Arc::new(client),
            endpoint: endpoint.to_string(),
        })
    }

    /// The normalized MCP endpoint URL this client talks to
    /// (`http://host:port/mcp`). Used by the plane path to decide whether
    /// the daemon's store is the local durable root (loopback) or a
    /// remote one the CLI must not shadow-write (bd-d41q).
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Call an MCP tool by name, returning the parsed JSON payload of the
    /// first text content block. Tool errors surface as anyhow errors.
    ///
    /// Bounded by [`try_tool_call_timeout`] (bd-gtqe): a daemon that accepts the
    /// request but never answers produces a loud timeout error instead of an
    /// unbounded silent park. Use [`Self::call_tool_within`] for paths that
    /// need a tighter deadline.
    pub async fn call_tool(&self, tool: &str, args: Value) -> anyhow::Result<Value> {
        let timeout = try_tool_call_timeout()?;
        self.call_tool_within(tool, args, timeout).await
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
                anyhow::Error::new(DaemonUnreachable {
                    endpoint: self.endpoint.clone(),
                    kind: UnreachableKind::Timeout {
                        tool: Some(tool.to_string()),
                        secs: deadline.as_secs(),
                    },
                })
                .context(format!(
                    "call_tool({tool}) timed out after {}s — daemon unresponsive (bd-gtqe loud-fail; was a silent hang)",
                    deadline.as_secs()
                ))
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
/// `Daemon(url)` when a toron MCP HTTP endpoint is explicitly configured
/// (`TORON_MCP_URL`, flywheel `[mail.toron] url`); `Embedded` otherwise —
/// the durable XDG root the daemon would use. A live default-port daemon
/// with no configured URL refuses resolution loudly (one store).
#[derive(Clone)]
pub enum MailboxSource {
    /// The daemon at this MCP URL (the writer; the CLI is a client).
    Daemon(DaemonClient),
    /// In-process mailbox against the durable XDG root (`demo_mailbox`
    /// production mode — never a third dir).
    Embedded(Arc<toron_core::mailbox::Mailbox>),
}

/// Pure selector decision (bd-nsr7: testable without sockets or env).
/// wave8-b: a live default port DISCOVERS the daemon (`DaemonDefault`) —
/// the old `RefuseSecondStore` arm is gone; refusing a healthy local daemon
/// was the bug. `DaemonExplicit` (env/config) connects or fails loud;
/// `DaemonDefault` connects and falls back to embedded with a loud note on
/// the probe-to-connect race.
/// wave13-b: the default probe is store-scoped — an explicitly pinned store
/// root that does not resolve onto the default durable root never discovers
/// (`EmbeddedIsolated`), so a temp-dir test can never talk to the live
/// daemon's store. Explicit `TORON_MCP_URL` still wins unconditionally.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceDecision {
    DaemonExplicit(String),
    DaemonDefault,
    EmbeddedIsolated,
    EmbeddedXdg,
}

/// What store the embedded mailbox would open, and whether it was
/// explicitly pinned (wave13-b). Mirrors `build_mailbox` root precedence
/// (`TORON_MCP_DATA_DIR` > `TORON_DATA_DIR` > durable default); demo mode
/// always counts as pinned (unique temp root per call, never the daemon's).
///
/// The test harness injects `TORON_DATA_DIR=<sandbox root>` into every
/// spawned child: a short pin that merely names the sandbox root is the
/// harness default, not an operator pin, so it does NOT count as explicit
/// (otherwise no test could ever discover). Any other short pin does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreIdentity {
    pub root: PathBuf,
    pub explicit: bool,
}

/// The CLI's store identity from the ambient environment.
pub fn embedded_store_identity() -> StoreIdentity {
    use std::path::PathBuf;
    if toron_core::config::ToronPaths::demo_mode() {
        return StoreIdentity {
            root: std::env::temp_dir(),
            explicit: true,
        };
    }
    if let Some(dir) = std::env::var_os("TORON_MCP_DATA_DIR") {
        return StoreIdentity {
            root: PathBuf::from(dir),
            explicit: true,
        };
    }
    if let Some(dir) = toron_core::config::parse_env_path("TORON_DATA_DIR").unwrap_or(None) {
        let harness_default = toron_core::config::ambient_sandbox()
            .is_some_and(|sandbox| same_store_root(&dir, &sandbox.root));
        if !harness_default {
            return StoreIdentity {
                root: dir,
                explicit: true,
            };
        }
    }
    StoreIdentity {
        root: toron_core::config::durable_data_dir(),
        explicit: false,
    }
}

/// The durable default root: what [`embedded_store_identity`] resolves
/// when nothing is pinned. The default daemon serves this root, so only
/// an explicit root that resolves here may discover it.
pub fn default_store_root() -> PathBuf {
    fn var_non_empty(name: &str) -> Option<PathBuf> {
        std::env::var_os(name)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
    }
    toron_core::config::resolve_data_root(&toron_core::config::DataRootEnv {
        toron_data_dir: None,
        xdg_data_home: var_non_empty("XDG_DATA_HOME"),
        home: var_non_empty("HOME"),
        sandbox: toron_core::config::ambient_sandbox(),
    })
}

/// Do two store roots name the same directory? Canonicalizes (follows the
/// symlinks a scratch dir may hide behind); unresolvable paths fall back
/// to a lexical absolute comparison, which still separates a temp pin
/// from the durable root.
pub fn same_store_root(a: &Path, b: &Path) -> bool {
    match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => absolutize(a) == absolutize(b),
    }
}

/// Absolute + lexically cleaned path for comparison when canonicalization
/// fails (missing dir). Never touches the fs beyond `current_dir`.
fn absolutize(p: &Path) -> PathBuf {
    use std::path::Component;
    let abs = if p.is_absolute() {
        p.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(p)
    };
    let mut out = PathBuf::new();
    for c in abs.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            _ => out.push(c.as_os_str()),
        }
    }
    out
}

/// bd-qpm0: the refusal text, as a CONSTANT so a fence can read it.
///
/// It lives here rather than inline in `resolve()` because the only way to reach
/// that arm is through process env, and this crate is
/// `#![forbid(unsafe_code)]` — `std::env::set_var` is `unsafe` in edition 2024.
/// A test that cannot set up its own condition is a test that cannot falsify
/// the condition, so the message is pure data and the arm just interpolates it.
pub const CONTRADICTION_MESSAGE: &str = "NOTE: TORON_MCP_URL and TORON_MCP_DATA_DIR are both \
set, and the DAEMON wins — the data-dir pin is being IGNORED. If that is not what you \
meant, unset TORON_MCP_URL. (If the data dir is the root the daemon itself owns, the \
two agree and there is nothing to fix.)";

fn decide_source(
    env_url: Option<String>,
    cli_root: &Path,
    root_explicit: bool,
    default_root: &Path,
    flywheel_url: Option<String>,
    default_port_live: bool,
) -> SourceDecision {
    if let Some(url) = env_url {
        return SourceDecision::DaemonExplicit(url);
    }
    // wave13-b: an explicitly pinned store root that does not resolve onto
    // the default durable root can never be the default daemon's store —
    // skip discovery (never cross-store), even when the port is live.
    if root_explicit && !same_store_root(cli_root, default_root) {
        return SourceDecision::EmbeddedIsolated;
    }
    if let Some(url) = flywheel_url {
        return SourceDecision::DaemonExplicit(url);
    }
    if default_port_live {
        return SourceDecision::DaemonDefault;
    }
    SourceDecision::EmbeddedXdg
}

/// Injectable inputs for [`MailboxSource::resolve_with`] (wave8-b): the
/// fake-prober seam. Production builds this from process env plus a bounded
/// TCP probe of the default port; tests pass fake values and never touch
/// a live daemon.
///
/// wave13-b: the store roots ride along so the fake-prober matrix covers
/// same-root/different-root/explicit-env without sockets.
#[derive(Debug, Clone)]
pub struct ResolveInputs {
    /// `TORON_MCP_URL` (already parse-or-refused by the caller).
    pub env_url: Option<String>,
    /// The store root the embedded mailbox would open.
    pub cli_store_root: PathBuf,
    /// Whether that root was explicitly pinned (env/demo), as opposed to
    /// the durable default.
    pub store_root_explicit: bool,
    /// The durable default root (no pins); the default daemon serves this.
    pub default_store_root: PathBuf,
    /// flywheel `[mail.toron] url`.
    pub config_url: Option<String>,
    /// Default-port probe: the URL probed plus whether it answered.
    pub default_probe: (String, bool),
}

impl MailboxSource {
    /// Resolve the robot path's mailbox source with the flywheel selector.
    ///
    /// One selector (ADR-0035 Spec 4.5, bd-nsr7; wave8-b discovery; wave13-b
    /// store identity): an explicit daemon URL connects or errors loud —
    /// never a silent second store. With no configured URL, a healthy
    /// default-port daemon is USED (discovered, not refused) — but ONLY
    /// when the CLI's resolved store root matches the durable default the
    /// daemon serves. An explicitly pinned root elsewhere never discovers
    /// (never cross-store); only a quiet port falls back to embedded.
    /// A discovered daemon that dies between probe and connect falls back
    /// to embedded with a loud note.
    ///
    /// Precedence:
    /// 0. BOTH `TORON_MCP_URL` and `TORON_MCP_DATA_DIR` — a contradiction,
    ///    WARNED loud (bd-qpm0); the daemon still wins.
    /// 1. `TORON_MCP_URL` — explicit daemon URL (connect failure is loud,
    ///    wins over everything including a store mismatch).
    /// 2. Explicit store root (`TORON_MCP_DATA_DIR` / `TORON_DATA_DIR` /
    ///    `TORON_DEMO`) that resolves away from the durable default —
    ///    embedded with a loud note, never discovered, never cross-store.
    /// 3. flywheel `[mail.toron] url` — configured daemon (connect loud).
    /// 4. Default `http://127.0.0.1:18080/mcp` — used when its health probe
    ///    answers; connect-race failure falls back loud, never refuses.
    /// 5. Embedded against the durable root (no daemon anywhere, or an
    ///    isolated pin).
    pub async fn resolve() -> anyhow::Result<Self> {
        // bd-qpm0: report the ambiguity LOUDLY, but do not change the decision.
        if try_daemon_url_from_env()?.is_some() && data_dir_pinned_from_env() {
            eprintln!("{CONTRADICTION_MESSAGE}");
        }
        let identity = embedded_store_identity();
        Self::resolve_with(ResolveInputs {
            env_url: try_daemon_url_from_env()?,
            cli_store_root: identity.root,
            store_root_explicit: identity.explicit,
            default_store_root: default_store_root(),
            config_url: flywheel_toron_url(),
            default_probe: (DEFAULT_DAEMON_URL.to_string(), default_port_serving()),
        })
        .await
    }

    /// [`Self::resolve`] with injected inputs (wave8-b fake-prober seam):
    /// the endpoint precedence without process env or live sockets, except
    /// the connect itself. Tests pass a dead loopback URL for the failure
    /// arms (refused instantly, no daemon) and a dead probe for the
    /// embedded arms (no socket touched at all). wave13-b adds the store
    /// roots so temp-root + healthy-probe resolves embedded without ever
    /// connecting.
    pub async fn resolve_with(inputs: ResolveInputs) -> anyhow::Result<Self> {
        let decision = decide_source(
            inputs.env_url,
            &inputs.cli_store_root,
            inputs.store_root_explicit,
            &inputs.default_store_root,
            inputs.config_url,
            inputs.default_probe.1,
        );
        match decision {
            SourceDecision::DaemonExplicit(url) => {
                Ok(Self::Daemon(DaemonClient::connect(&url).await?))
            }
            SourceDecision::DaemonDefault => {
                let url = inputs.default_probe.0;
                match DaemonClient::connect(&url).await {
                    Ok(dc) => Ok(Self::Daemon(dc)),
                    Err(e) => {
                        eprintln!(
                            "toron: default daemon at {url} probed live but the connect failed ({e:#}); falling back to embedded for this command (one store per project — start the daemon or set TORON_MCP_URL to use it)."
                        );
                        Ok(Self::Embedded(Arc::new(toron_mcp::demo_mailbox().await?)))
                    }
                }
            }
            SourceDecision::EmbeddedIsolated => {
                eprintln!(
                    "toron: explicit store root {} differs from the default daemon store {}; using embedded (never cross-store — set TORON_MCP_URL to use a daemon deliberately).",
                    inputs.cli_store_root.display(),
                    inputs.default_store_root.display()
                );
                Ok(Self::Embedded(Arc::new(toron_mcp::demo_mailbox().await?)))
            }
            SourceDecision::EmbeddedXdg => {
                Ok(Self::Embedded(Arc::new(toron_mcp::demo_mailbox().await?)))
            }
        }
    }
}

/// True when a daemon endpoint URL is loopback (same machine): the
/// daemon's store IS the local durable root, so an embedded mailbox
/// writes where the daemon reads. A non-loopback daemon owns a remote
/// store — plane commands must refuse rather than shadow-write a local
/// second store the daemon never sees (bd-d41q).
pub fn daemon_url_is_loopback(url: &str) -> bool {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let host_port = after_scheme.split('/').next().unwrap_or(after_scheme);
    let host = host_port
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(host_port)
        .trim_matches(|c| c == '[' || c == ']');
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}
/// Parse-or-refuse form: a set-but-garbage `TORON_MCP_URL` (not http/https)
/// is a [`ConfigError`](toron_core::config::ConfigError) naming the accepted
/// forms, instead of a confusing connect failure later.
fn try_daemon_url_from_env() -> Result<Option<String>, toron_core::config::ConfigError> {
    toron_core::config::parse_env_url("TORON_MCP_URL")
}

/// bd-qpm0: is the durable-root pin set? Its own reader, so the warning cannot
/// disagree with the selector about what counts as "pinned".
fn data_dir_pinned_from_env() -> bool {
    std::env::var_os("TORON_MCP_DATA_DIR").is_some()
}

/// Whether an addr serves (TCP probe, short timeout).
fn port_serving(addr: &str) -> bool {
    let Ok(sock) = addr.parse() else {
        return false;
    };
    std::net::TcpStream::connect_timeout(&sock, std::time::Duration::from_millis(200)).is_ok()
}

/// First-hand refusal probe for [`DaemonClient::connect`] (wave8-b): open a
/// bounded TCP connection to the endpoint's host:port. `true` only on a
/// definite RST — timeouts, DNS failures, and unparseable endpoints return
/// `false` so the normal handshake path (with its own typed timeout) decides.
fn refused_now(endpoint: &str) -> bool {
    use std::net::ToSocketAddrs;
    let host_port = endpoint
        .split("://")
        .nth(1)
        .unwrap_or(endpoint)
        .split('/')
        .next()
        .unwrap_or("");
    let mut addrs = match host_port.to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(_) => return false,
    };
    let Some(addr) = addrs.next() else {
        return false;
    };
    match std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(200)) {
        Ok(_) => false,
        Err(e) => e.kind() == std::io::ErrorKind::ConnectionRefused,
    }
}

/// Whether the default daemon port serves (TCP probe, short timeout).
/// Used ONLY as a second-store guard: a live default daemon with no
/// configured URL refuses embedded fallback loudly in `resolve`.
fn default_port_serving() -> bool {
    let host = DEFAULT_DAEMON_URL
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("127.0.0.1:18080");
    let (addr, port) = host.split_once(':').unwrap_or((host, "18080"));
    port_serving(&format!("{addr}:{port}"))
}

/// flywheel `[mail.toron] url` (best-effort: `~/.config/flywheel/config.toml`).
fn flywheel_toron_url() -> Option<String> {
    let home = toron_core::config::home_env("HOME")?;
    let path = home.join(".config").join("flywheel").join("config.toml");
    let raw = std::fs::read_to_string(path).ok()?;
    let value: toml::Value = toml::from_str(&raw).ok()?;
    value
        .get("mail")?
        .get("toron")?
        .get("url")?
        .as_str()
        .map(ToString::to_string)
}

#[cfg(test)]
mod selector_tests {
    use super::*;

    #[test]
    fn decide_source_precedence() {
        use std::path::Path;
        let def = Path::new("/xdg/toron");
        let temp = Path::new("/tmp/cli-temp");
        // (env_url, cli_root, explicit, default_root, config_url, live)
        type EnvRow = (
            Option<String>,
            &'static Path,
            bool,
            &'static Path,
            Option<String>,
            bool,
        );
        let cases: Vec<(&str, EnvRow, SourceDecision)> = vec![
            (
                "url wins over everything, even an isolated pin",
                (
                    Some("http://x/mcp".to_string()),
                    temp,
                    true,
                    def,
                    Some("http://y/mcp".to_string()),
                    true,
                ),
                SourceDecision::DaemonExplicit("http://x/mcp".to_string()),
            ),
            (
                "env url when nothing is pinned",
                (
                    Some("http://x/mcp".to_string()),
                    def,
                    false,
                    def,
                    Some("http://y/mcp".to_string()),
                    true,
                ),
                SourceDecision::DaemonExplicit("http://x/mcp".to_string()),
            ),
            (
                "isolated temp pin beats config url and live probe",
                (
                    None,
                    temp,
                    true,
                    def,
                    Some("http://y/mcp".to_string()),
                    true,
                ),
                SourceDecision::EmbeddedIsolated,
            ),
            (
                "explicit root that matches the default still discovers",
                (None, def, true, def, None, true),
                SourceDecision::DaemonDefault,
            ),
            (
                "config url when the store is default",
                (
                    None,
                    def,
                    false,
                    def,
                    Some("http://y/mcp".to_string()),
                    false,
                ),
                SourceDecision::DaemonExplicit("http://y/mcp".to_string()),
            ),
            (
                "live default discovers the daemon",
                (None, def, false, def, None, true),
                SourceDecision::DaemonDefault,
            ),
            (
                "quiet embeds",
                (None, def, false, def, None, false),
                SourceDecision::EmbeddedXdg,
            ),
        ];
        for (name, (a0, a1, a2, a3, a4, a5), want) in cases {
            assert_eq!(decide_source(a0, a1, a2, a3, a4, a5), want, "{name}");
        }
    }

    #[test]
    fn daemon_url_loopback_means_shared_local_store() {
        assert!(daemon_url_is_loopback("http://127.0.0.1:18080/mcp"));
        assert!(daemon_url_is_loopback("http://localhost:18080/mcp"));
        assert!(daemon_url_is_loopback("http://[::1]:18080/mcp"));
        assert!(daemon_url_is_loopback("http://127.0.0.1:18080"));
        assert!(!daemon_url_is_loopback("http://100.64.0.5:18080/mcp"));
        assert!(!daemon_url_is_loopback("http://toron.tailnet:18080/mcp"));
        assert!(!daemon_url_is_loopback(""));
    }

    #[test]
    fn port_probe_sees_bound_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
        let addr = listener.local_addr().expect("addr").to_string();
        assert!(port_serving(&addr));
        drop(listener);
        assert!(!port_serving(&addr));
    }

    /// bd-qpm0: the warning must be ACTIONABLE. A notice that says "ambiguous
    /// configuration" without naming the two variables leaves the operator
    /// unable to act, which is the same unactionable-signal defect as an audit
    /// row with a null detail. It must also say which one WON, because that is
    /// the thing the operator cannot observe otherwise.
    ///
    /// Read from the constant rather than by driving `resolve()`: reaching that
    /// arm needs process env, and `set_var` is `unsafe` under this crate's
    /// `forbid(unsafe_code)`. A fence that cannot set up its own condition is a
    /// fence that cannot falsify it, so the condition under test is pure data.
    #[test]
    fn the_ambiguity_warning_names_both_variables_and_the_winner() {
        let m = CONTRADICTION_MESSAGE;
        assert!(
            m.contains("TORON_MCP_URL") && m.contains("TORON_MCP_DATA_DIR"),
            "the refusal must name BOTH variables or the operator cannot act: {m}"
        );
        let lower = m.to_ascii_lowercase();
        assert!(
            lower.contains("wins"),
            "it must say which signal WON, since that is what the operator cannot observe: {m}"
        );
        assert!(
            lower.contains("unset toron_mcp_url"),
            "and it must name the remedy: {m}"
        );
        assert!(
            !lower.contains("refus"),
            "it must NOT claim to refuse: naming a daemon's own data root alongside its URL is \
             legitimate, and a refusal broke four conformance tests to fix a silence: {m}"
        );
    }

    /// wave8-b: the injectable seam resolves end-to-end without env or a
    /// live daemon. Loopback port 9 is refused (never a daemon); a dead
    /// default probe touches no socket at all.
    /// wave13-b: the seam also carries store identity — a temp root plus a
    /// live probe resolves embedded without ever connecting.
    const DEAD_URL: &str = "http://127.0.0.1:9/mcp";

    fn dead_default(live: bool) -> (String, bool) {
        (DEAD_URL.to_string(), live)
    }

    fn roots(cli: &str, explicit: bool, default: &str) -> (PathBuf, bool, PathBuf) {
        (PathBuf::from(cli), explicit, PathBuf::from(default))
    }

    fn inputs_with_roots(
        env_url: Option<String>,
        cli: &str,
        explicit: bool,
        default: &str,
        config_url: Option<String>,
        live: bool,
    ) -> ResolveInputs {
        let (cli_store_root, store_root_explicit, default_store_root) =
            roots(cli, explicit, default);
        ResolveInputs {
            env_url,
            cli_store_root,
            store_root_explicit,
            default_store_root,
            config_url,
            default_probe: dead_default(live),
        }
    }

    #[tokio::test]
    async fn explicit_dead_url_fails_typed_with_remedy() {
        for inputs in [
            inputs_with_roots(
                Some(DEAD_URL.to_string()),
                "/tmp/cli-temp",
                true,
                "/xdg/toron",
                Some(DEAD_URL.to_string()),
                false,
            ),
            inputs_with_roots(
                None,
                "/xdg/toron",
                false,
                "/xdg/toron",
                Some(DEAD_URL.to_string()),
                true,
            ),
        ] {
            let err = match MailboxSource::resolve_with(inputs).await {
                Ok(_) => {
                    panic!("a configured-but-dead daemon must fail loud, never embed silently")
                }
                Err(e) => e,
            };
            let typed = err
                .downcast_ref::<DaemonUnreachable>()
                .expect("the failure must be the typed DaemonUnreachable");
            assert_eq!(typed.endpoint, DEAD_URL);
            assert_eq!(typed.kind, UnreachableKind::Refused);
            assert!(
                format!("{typed}").contains("toron serve --mcp-http"),
                "the typed error must carry the one-line remedy"
            );
        }
    }

    #[tokio::test]
    async fn quiet_port_embeds_without_touching_the_net() {
        let source = MailboxSource::resolve_with(inputs_with_roots(
            None,
            "/xdg/toron",
            false,
            "/xdg/toron",
            None,
            false,
        ))
        .await
        .expect("no daemon anywhere must embed");
        assert!(
            matches!(source, MailboxSource::Embedded(_)),
            "quiet default port embeds"
        );
    }

    #[tokio::test]
    async fn isolated_temp_root_skips_a_live_default_probe() {
        // The wave13-b core: a temp store plus a *live* probe still embeds.
        // With a live operator daemon this proves no connect was attempted
        // (it would have succeeded); without one the probe flag alone
        // proves the decision skipped the daemon arm.
        let source = MailboxSource::resolve_with(inputs_with_roots(
            None,
            "/tmp/cli-temp-isolated",
            true,
            "/xdg/toron",
            None,
            true,
        ))
        .await
        .expect("an isolated temp root must never discover");
        assert!(
            matches!(source, MailboxSource::Embedded(_)),
            "isolated temp embeds without attempting the probed daemon"
        );
    }

    #[tokio::test]
    async fn explicit_env_wins_over_store_isolation() {
        // Explicit TORON_MCP_URL still wins unconditionally, even with an
        // isolated store: the operator asked for that daemon.
        let err = match MailboxSource::resolve_with(inputs_with_roots(
            Some(DEAD_URL.to_string()),
            "/tmp/cli-temp",
            true,
            "/xdg/toron",
            None,
            true,
        ))
        .await
        {
            Ok(_) => panic!("explicit env must attempt the daemon even when isolated"),
            Err(e) => e,
        };
        assert!(
            err.downcast_ref::<DaemonUnreachable>().is_some(),
            "explicit env failure stays typed: {err:?}"
        );
    }

    #[tokio::test]
    async fn discovered_default_that_dies_falls_back_loud() {
        let source = MailboxSource::resolve_with(inputs_with_roots(
            None,
            "/xdg/toron",
            false,
            "/xdg/toron",
            None,
            true,
        ))
        .await
        .expect("the probe-to-connect race must fall back, never refuse");
        assert!(
            matches!(source, MailboxSource::Embedded(_)),
            "dead-on-arrival default daemon falls back to embedded"
        );
    }

    #[test]
    fn same_store_root_matches_identical_and_symlinked_dirs() {
        let dir = std::env::temp_dir().join(format!("toron-root-eq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(same_store_root(&dir, &dir));
        assert!(same_store_root(&dir.join("sub").join(".."), &dir.join(".")));
        assert!(!same_store_root(&dir, &dir.join("other-missing")));
        assert!(!same_store_root(
            &PathBuf::from("/tmp/toron-a-absent"),
            &PathBuf::from("/tmp/toron-b-absent")
        ));
        assert!(same_store_root(
            &PathBuf::from("/tmp/toron-same-absent"),
            &PathBuf::from("/tmp/toron-same-absent")
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod daemon_unreachable_tests {
    use super::*;

    fn refused_chain() -> anyhow::Error {
        let io = std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connect: connection refused",
        );
        anyhow::Error::new(io).context("daemon connect (is `toron serve --mcp-http` running?)")
    }

    #[test]
    fn refused_connection_classifies_by_type_not_prose() {
        assert!(is_refused(&refused_chain()));
        let other: anyhow::Error = anyhow::anyhow!("daemon connect timed out");
        assert!(!is_refused(&other));
        // the words alone never classify — only the io::Error kind does
        let prose_only =
            anyhow::anyhow!("connection refused (is `toron serve --mcp-http` running?)");
        assert!(!is_refused(&prose_only));
    }

    #[test]
    fn display_keeps_todays_remediation_text() {
        let ep = "http://127.0.0.1:18080/mcp";
        let refused = DaemonUnreachable {
            endpoint: ep.into(),
            kind: UnreachableKind::Refused,
        };
        assert!(
            refused
                .to_string()
                .contains("is `toron serve --mcp-http` running?")
        );
        let timeout = DaemonUnreachable {
            endpoint: ep.into(),
            kind: UnreachableKind::Timeout {
                tool: None,
                secs: 10,
            },
        };
        assert_eq!(
            timeout.to_string(),
            "daemon connect timed out after 10s (is `toron serve --mcp-http` running and responsive?)"
        );
        let call_timeout = DaemonUnreachable {
            endpoint: ep.into(),
            kind: UnreachableKind::Timeout {
                tool: Some("fetch_inbox".into()),
                secs: 30,
            },
        };
        assert_eq!(
            call_timeout.to_string(),
            "call_tool(fetch_inbox) timed out after 30s — daemon unresponsive (bd-gtqe loud-fail; was a silent hang)"
        );
    }
}
