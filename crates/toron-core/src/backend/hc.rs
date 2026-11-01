// governed-by: ADR-0003, ADR-0005
#![forbid(unsafe_code)]

//! hc (herdr-core) OpenRPC backend.
//!
//! Replaces the herdr TUI socket client. Status is OSC 7501 only, folded
//! through [`crate::osc7501::fold`]. No per-harness status, no
//! `HERDR_SOCKET_PATH` / `herdr.sock`, no HDR_PROTOCOL.
//!
//! # Pane / terminal ids
//!
//! Wire addresses are hc terminal ids: `t7`, a name, or `machine/t7`.
//! Values returned through the [`super::Backend`] trait carry the stable
//! prefix [`ID_PREFIX`] (`hc:`), so flywheel can parse them without
//! guessing. Examples: `hc:t7`, `hc:review`, `hc:box/t3`. Strip the
//! prefix with [`strip_id_prefix`] before an hc RPC.
//!
//! # Connect
//!
//! JSON-RPC 2.0, one message per line on the hc unix socket (protocol
//! generation 1). Resolution order for [`Client::connect`]:
//! 1. explicit `socket_path` argument
//! 2. `HC_SOCKET_PATH`
//! 3. `$XDG_STATE_HOME/herdr-core/$HC_SESSION/$HC_SESSION.sock`
//!    (`XDG_STATE_HOME` defaults to `$HOME/.local/state`, `HC_SESSION`
//!    defaults to `default`)
//!
//! Connect probes `hello` / method list and returns a typed error when a
//! required method is missing. Unknown JSON fields are ignored.
//!
//! # Mail identity (labels-only)
//!
//! hc has no `pane.report_metadata`. Mail identity rides on a **stable,
//! unique terminal `name`** (and optional `toron_*` labels). Rules:
//! - A pin / display name may address a terminal only when that name is
//!   present and unique among the live terminals considered.
//! - A missing name never invents an address from argv or cwd: mail falls
//!   back to the opaque [`ID_PREFIX`] id (`hc:t7`), or refuses the name
//!   lookup.
//! - A **duplicate** name among live terminals is refused for name-based
//!   mail — never resolved to an arbitrary match. Callers must use
//!   `hc:<address>` or rename until unique.
//!
//! [`mail_pane_id_for_name`] encodes that policy as a pure function.

use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::osc7501::{Board, FoldPolicy, HcStatusView, fold};

use super::error::{BackendError, BackendErrorCode};
use super::types::{
    AgentInfo, AgentStatus, BackendEvent, PaneId, PaneInfo, PromptLanding, Subscription,
    WaitOptions, normalize_herdr_status,
};
use super::{Backend, EventStream};

/// Stable prefix on every pane/agent id this backend returns.
pub const ID_PREFIX: &str = "hc:";

/// Protocol generation this client speaks (hc OpenRPC `hello.generation`).
pub const HC_GENERATION: u32 = 1;

/// Default unary RPC budget when params omit a timeout.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 30_000;
/// Margin above the call budget so the socket deadline loses the race cleanly.
pub const RPC_TIMEOUT_MARGIN_MS: u64 = 5_000;

/// Methods required for the Backend surface we implement.
const REQUIRED_METHODS: &[&str] = &[
    "hello",
    "terminal.list",
    "terminal.get",
    "terminal.run",
    "terminal.send",
    "terminal.keys",
    "terminal.wait",
    "terminal.read",
    "terminal.kill",
    "terminal.rm",
    "events.subscribe",
    "rpc.discover",
];

/// Strip [`ID_PREFIX`] when present; otherwise return `id` unchanged
/// (already a bare hc address).
pub fn strip_id_prefix(id: &str) -> &str {
    id.strip_prefix(ID_PREFIX).unwrap_or(id)
}

/// Prefix a bare hc address for Backend trait returns.
pub fn prefix_id(address: &str) -> String {
    if address.starts_with(ID_PREFIX) {
        address.to_string()
    } else {
        format!("{ID_PREFIX}{address}")
    }
}

/// Why a name-based mail lookup could not address a terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MailNameError {
    /// No live terminal carries this name (and it is not an `hc:` id).
    Missing,
    /// Two or more live terminals share the name — never pick one.
    Duplicate { count: usize },
}

/// Resolve a mail target to a pane id under the labels-only hc rule.
///
/// `named` is `(optional_terminal_name, bare_or_prefixed_address)` for each
/// live terminal. Policy:
/// - If `query` is already an `hc:` id (or bare address matching one row),
///   return that unique id — ids never collide.
/// - If `query` matches exactly one non-empty terminal name, return that
///   terminal's prefixed id.
/// - If it matches zero names → [`MailNameError::Missing`].
/// - If it matches two or more → [`MailNameError::Duplicate`] (refuse;
///   never return an arbitrary pane).
pub fn mail_pane_id_for_name(
    named: &[(Option<&str>, &str)],
    query: &str,
) -> Result<String, MailNameError> {
    let q = query.trim();
    if q.is_empty() {
        return Err(MailNameError::Missing);
    }

    // Prefer an explicit id query (`hc:t7` or bare address equal to a row).
    let want_addr = strip_id_prefix(q);
    let id_query = q.starts_with(ID_PREFIX)
        || named
            .iter()
            .any(|(_, addr)| strip_id_prefix(addr) == q);
    if id_query {
        let mut hits = named
            .iter()
            .filter(|(_, addr)| strip_id_prefix(addr) == want_addr)
            .map(|(_, addr)| prefix_id(addr));
        return match (hits.next(), hits.next()) {
            (Some(id), None) => Ok(id),
            (Some(_), Some(_)) => Err(MailNameError::Duplicate { count: 2 }),
            (None, _) => Err(MailNameError::Missing),
        };
    }

    let hits: Vec<String> = named
        .iter()
        .filter_map(|(name, addr)| {
            let name = name.filter(|n| !n.is_empty())?;
            (name == q).then(|| prefix_id(addr))
        })
        .collect();
    match hits.len() {
        0 => Err(MailNameError::Missing),
        1 => Ok(hits.into_iter().next().expect("len 1")),
        n => Err(MailNameError::Duplicate { count: n }),
    }
}

/// Display name for mail maps: keep the terminal name only when it is
/// non-empty and unique in `all_names`. Otherwise `None` so callers fall
/// back to the `hc:` id and never publish a colliding name.
pub fn unique_mail_name<'a>(name: Option<&'a str>, all_names: &[Option<&str>]) -> Option<&'a str> {
    let name = name.filter(|n| !n.is_empty())?;
    let count = all_names
        .iter()
        .filter(|n| n.as_deref() == Some(name))
        .count();
    (count == 1).then_some(name)
}

/// Resolve the hc socket path (pure — testable).
pub fn resolve_socket_path(
    explicit: Option<&str>,
    hc_socket_path: Option<&str>,
    xdg_state_home: Option<&Path>,
    home: Option<&Path>,
    hc_session: Option<&str>,
) -> PathBuf {
    if let Some(p) = explicit {
        return PathBuf::from(p);
    }
    if let Some(p) = hc_socket_path {
        return PathBuf::from(p);
    }
    let session = hc_session.unwrap_or("default");
    let state = if let Some(p) = xdg_state_home {
        p.to_path_buf()
    } else if let Some(home) = home {
        home.join(".local/state")
    } else {
        PathBuf::from(".local/state")
    };
    state
        .join("herdr-core")
        .join(session)
        .join(format!("{session}.sock"))
}

fn default_socket_path() -> PathBuf {
    resolve_socket_path(
        None,
        std::env::var("HC_SOCKET_PATH").ok().as_deref(),
        std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .as_deref(),
        std::env::var_os("HOME").map(PathBuf::from).as_deref(),
        std::env::var("HC_SESSION").ok().as_deref(),
    )
}

/// Wire Status object from hc (`Terminal.status` / `records` values).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct HcStatus {
    pub state: String,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub progress: Option<u8>,
    #[serde(default)]
    pub app: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub msg: Option<String>,
}

impl HcStatus {
    fn view(&self) -> HcStatusView<'_> {
        HcStatusView {
            state: self.state.as_str(),
            kind: self.kind.as_deref(),
            progress: self.progress,
            app: self.app.as_deref(),
            title: self.title.as_deref(),
            msg: self.msg.as_deref(),
        }
    }
}

/// Wire Terminal object (unknown fields ignored).
#[derive(Debug, Clone, Deserialize)]
pub struct Terminal {
    pub address: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub status: Option<HcStatus>,
    #[serde(default)]
    pub records: BTreeMap<String, HcStatus>,
    #[serde(default)]
    pub reported: bool,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub parent: Option<String>,
}

impl Terminal {
    /// Fold OSC root + records through [`fold`].
    pub fn agent_status(&self, policy: FoldPolicy) -> AgentStatus {
        let mut board = Board::default();
        if let Some(root) = self.status.as_ref() {
            board.insert_hc("", &root.view());
        }
        for (id, status) in &self.records {
            board.insert_hc(id, &status.view());
        }
        fold(&board, policy)
    }

    fn to_agent_info(&self, policy: FoldPolicy) -> AgentInfo {
        self.to_agent_info_with_mail_name(policy, self.name.clone())
    }

    fn to_agent_info_with_mail_name(
        &self,
        policy: FoldPolicy,
        mail_name: Option<String>,
    ) -> AgentInfo {
        let id = prefix_id(&self.address);
        AgentInfo {
            agent: mail_name.clone().or_else(|| self.argv.first().cloned()),
            agent_status: self.agent_status(policy),
            cwd: self.cwd.clone(),
            display_agent: mail_name.clone(),
            focused: false,
            foreground_cwd: self.cwd.clone(),
            interactive_ready: self.reported,
            launch_pending: false,
            name: mail_name,
            pane_id: id.clone(),
            revision: 0,
            state_change_seq: 0,
            completion_seq: None,
            state_labels: serde_json::Map::new(),
            tab_id: String::new(),
            terminal_id: id.clone(),
            terminal_title: self.name.clone(),
            workspace_id: String::new(),
        }
    }

    #[allow(dead_code)]
    fn to_pane_info(&self, policy: FoldPolicy) -> PaneInfo {
        self.to_pane_info_with_mail_name(policy, self.name.clone())
    }

    fn to_pane_info_with_mail_name(
        &self,
        policy: FoldPolicy,
        mail_name: Option<String>,
    ) -> PaneInfo {
        let id = prefix_id(&self.address);
        let mut tokens = serde_json::Map::new();
        for (k, v) in &self.labels {
            tokens.insert(k.clone(), Value::String(v.clone()));
        }
        PaneInfo {
            pane_id: id.clone(),
            terminal_id: id,
            workspace_id: String::new(),
            tab_id: String::new(),
            focused: false,
            agent_status: self.agent_status(policy),
            revision: 0,
            title: mail_name.clone(),
            cwd: self.cwd.clone(),
            agent: mail_name.clone().or_else(|| self.argv.first().cloned()),
            agent_session: None,
            display_agent: mail_name.clone(),
            foreground_cwd: self.cwd.clone(),
            label: mail_name.clone(),
            scroll: None,
            state_labels: serde_json::Map::new(),
            terminal_title: mail_name.clone(),
            terminal_title_stripped: mail_name,
            tokens,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct HelloResult {
    generation: u32,
    version: String,
    #[serde(default)]
    methods: Vec<String>,
    #[serde(default)]
    features: Vec<String>,
}

/// Capabilities learned at connect from `hello` / `rpc.discover`.
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub generation: u32,
    pub version: String,
    pub methods: Vec<String>,
    pub features: Vec<String>,
}

impl Capabilities {
    fn ensure_method(&self, method: &str) -> Result<(), BackendError> {
        if self.methods.iter().any(|m| m == method) {
            Ok(())
        } else {
            Err(BackendError::new(
                BackendErrorCode::Other(format!("missing_method:{method}")),
                format!("hc server lacks required method {method}"),
            ))
        }
    }
}

/// Blocking JSON-RPC client over the hc unix socket.
#[derive(Debug)]
pub struct Client {
    socket_path: String,
    next_id: AtomicU64,
    caps: Capabilities,
    fold_policy: FoldPolicy,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
            caps: self.caps.clone(),
            fold_policy: self.fold_policy,
        }
    }
}

impl Client {
    /// Connect and probe `hello`. Fails when a required method is missing.
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        Self::connect_with_policy(socket_path, FoldPolicy::Attention)
    }

    pub fn connect_with_policy(
        socket_path: Option<&str>,
        fold_policy: FoldPolicy,
    ) -> Result<Self, BackendError> {
        let path = socket_path
            .map(ToString::to_string)
            .unwrap_or_else(|| default_socket_path().to_string_lossy().into_owned());
        let mut client = Client {
            socket_path: path,
            next_id: AtomicU64::new(1),
            caps: Capabilities {
                generation: 0,
                version: String::new(),
                methods: Vec::new(),
                features: Vec::new(),
            },
            fold_policy,
        };
        let hello = client.hello()?;
        for method in REQUIRED_METHODS {
            if *method == "hello" {
                continue;
            }
            if !hello.methods.iter().any(|m| m == method) {
                return Err(BackendError::new(
                    BackendErrorCode::Other(format!("missing_method:{method}")),
                    format!("hc hello omitted required method {method}"),
                ));
            }
        }
        if hello.generation != HC_GENERATION {
            return Err(BackendError::new(
                BackendErrorCode::Other("unsupported_generation".into()),
                format!(
                    "hc generation {} unsupported (want {HC_GENERATION})",
                    hello.generation
                ),
            ));
        }
        client.caps = Capabilities {
            generation: hello.generation,
            version: hello.version,
            methods: hello.methods,
            features: hello.features,
        };
        Ok(client)
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.caps
    }

    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    pub fn fold_policy(&self) -> FoldPolicy {
        self.fold_policy
    }

    fn alloc_id(&self) -> String {
        self.next_id.fetch_add(1, Ordering::Relaxed).to_string()
    }

    fn open(&self, timeout: Duration) -> Result<UnixStream, BackendError> {
        let stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Connect,
                format!("cannot connect to hc socket at {}: {e}", self.socket_path),
            )
        })?;
        stream.set_read_timeout(Some(timeout)).map_err(|e| {
            BackendError::new(BackendErrorCode::Io, format!("set_read_timeout: {e}"))
        })?;
        stream.set_write_timeout(Some(timeout)).map_err(|e| {
            BackendError::new(BackendErrorCode::Io, format!("set_write_timeout: {e}"))
        })?;
        Ok(stream)
    }

    pub fn call_coded(&self, method: &str, params: Value) -> Result<Value, BackendError> {
        if !self.caps.methods.is_empty() {
            self.caps.ensure_method(method)?;
        }
        let id = self.alloc_id();
        let timeout = socket_timeout_from_params(&params);
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');

        let mut reader = BufReader::new(self.open(timeout)?);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| map_io_err("write to hc socket", e))?;

        loop {
            let mut buf = String::new();
            let n = reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read from hc socket", e))?;
            if n == 0 {
                return Err(BackendError::new(
                    BackendErrorCode::Io,
                    String::from("hc socket closed while waiting for response"),
                ));
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("malformed hc response: {buf}: {e}"),
                )
            })?;
            let got_id = val.get("id").and_then(|v| {
                v.as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            });
            if got_id.as_deref() != Some(id.as_str()) {
                continue;
            }
            if let Some(err) = val.get("error") {
                return Err(map_wire_error(err));
            }
            return val.get("result").cloned().ok_or_else(|| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("hc response missing result: {val}"),
                )
            });
        }
    }

    pub fn call(&self, method: &str, params: Value) -> Result<Value, BackendError> {
        self.call_coded(method, params)
    }

    fn hello(&self) -> Result<HelloResult, BackendError> {
        let result = self.call_coded(
            "hello",
            json!({ "generation": HC_GENERATION, "framed": false }),
        )?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Malformed,
                format!("hello result: {e}"),
            )
        })
    }

    pub fn terminal_list(&self, all: bool) -> Result<Vec<Terminal>, BackendError> {
        let result = self.call_coded("terminal.list", json!({ "all": all }))?;
        let terminals = result
            .get("terminals")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        serde_json::from_value(terminals).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Malformed,
                format!("terminal.list: {e}"),
            )
        })
    }

    pub fn terminal_get(&self, terminal: &str) -> Result<Terminal, BackendError> {
        let result = self.call_coded(
            "terminal.get",
            json!({ "terminal": strip_id_prefix(terminal) }),
        )?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.get: {e}"))
        })
    }

    pub fn terminal_run(
        &self,
        argv: &[String],
        name: Option<&str>,
        cwd: Option<&str>,
    ) -> Result<Terminal, BackendError> {
        let mut params = json!({ "argv": argv });
        if let Some(name) = name {
            params["name"] = json!(name);
        }
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd);
        }
        // Background by default so the RPC returns the terminal object.
        params["wait"] = json!(false);
        let result = self.call_coded("terminal.run", params)?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.run: {e}"))
        })
    }

    pub fn terminal_send(
        &self,
        terminal: &str,
        text: &str,
        wait_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({
            "terminal": strip_id_prefix(terminal),
            "text": text,
        });
        if let Some(ms) = wait_ms {
            params["wait"] = json!({ "timeout_ms": ms });
            params["timeout_ms"] = json!(ms);
        }
        self.call_coded("terminal.send", params)
    }

    pub fn terminal_keys(&self, terminal: &str, keys: &[&str]) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.keys",
            json!({
                "terminal": strip_id_prefix(terminal),
                "keys": keys,
            }),
        )
    }

    pub fn terminal_wait(
        &self,
        terminal: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "terminal": strip_id_prefix(terminal) });
        if let Some(ms) = timeout_ms {
            params["timeout_ms"] = json!(ms);
        }
        self.call_coded("terminal.wait", params)
    }

    pub fn terminal_read(&self, terminal: &str, lines: u32) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.read",
            json!({
                "terminal": strip_id_prefix(terminal),
                "lines": lines,
            }),
        )
    }

    pub fn terminal_kill(&self, terminal: &str) -> Result<Terminal, BackendError> {
        let result = self.call_coded(
            "terminal.kill",
            json!({ "terminal": strip_id_prefix(terminal) }),
        )?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.kill: {e}"))
        })
    }

    pub fn terminal_rm(&self, terminal: &str) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.rm",
            json!({ "terminal": strip_id_prefix(terminal) }),
        )
    }

    /// hc has no `pane.report_metadata` (labels-only mail identity). Callers
    /// such as CLI bootstrap stamp must treat this as unsupported and skip —
    /// never invent a stamp from argv/cwd.
    pub fn pane_report_metadata(
        &self,
        _pane_id: &str,
        _pin: &str,
        _project: &str,
        _npub: &str,
    ) -> Result<Value, BackendError> {
        Err(BackendError::unsupported(
            "hc has no pane.report_metadata; mail identity is labels-only",
        ))
    }

    pub fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        let terminals = self.terminal_list(false)?;
        let names: Vec<Option<String>> = terminals.iter().map(|t| t.name.clone()).collect();
        let name_refs: Vec<Option<&str>> = names.iter().map(|n| n.as_deref()).collect();
        Ok(terminals
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let mail_name = unique_mail_name(name_refs[i], &name_refs).map(str::to_string);
                t.to_agent_info_with_mail_name(self.fold_policy, mail_name)
            })
            .collect())
    }

    pub fn agent_get(&self, target: &str) -> Result<AgentInfo, BackendError> {
        Ok(self
            .terminal_get(target)?
            .to_agent_info(self.fold_policy))
    }

    pub fn pane_list(&self) -> Result<Vec<PaneInfo>, BackendError> {
        let terminals = self.terminal_list(false)?;
        let names: Vec<Option<String>> = terminals.iter().map(|t| t.name.clone()).collect();
        let name_refs: Vec<Option<&str>> = names.iter().map(|n| n.as_deref()).collect();
        Ok(terminals
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let mail_name = unique_mail_name(name_refs[i], &name_refs).map(str::to_string);
                t.to_pane_info_with_mail_name(self.fold_policy, mail_name)
            })
            .collect())
    }

    /// Subscribe; returns snapshot terminals as status events, then a
    /// long-lived push stream of terminal updates.
    pub fn subscribe_terminals(
        &self,
        terminal: Option<&str>,
    ) -> Result<(Vec<Terminal>, HcEventStream), BackendError> {
        let mut params = json!({});
        if let Some(t) = terminal {
            params["terminal"] = json!(strip_id_prefix(t));
        }
        let id = self.alloc_id();
        let timeout =
            Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS.saturating_add(RPC_TIMEOUT_MARGIN_MS));
        let req = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "events.subscribe",
            "params": params,
        });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');
        let stream = self.open(timeout)?;
        let mut reader = BufReader::new(stream);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| map_io_err("write subscribe", e))?;
        // First matching response is the subscribe result.
        let snapshot = loop {
            let mut buf = String::new();
            let n = reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read subscribe ack", e))?;
            if n == 0 {
                return Err(BackendError::new(
                    BackendErrorCode::Io,
                    String::from("hc subscribe closed before ack"),
                ));
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("subscribe ack: {buf}: {e}"),
                )
            })?;
            let got_id = val.get("id").and_then(|v| {
                v.as_str()
                    .map(str::to_string)
                    .or_else(|| v.as_u64().map(|n| n.to_string()))
            });
            if got_id.as_deref() != Some(id.as_str()) {
                continue;
            }
            if let Some(err) = val.get("error") {
                return Err(map_wire_error(err));
            }
            let result = val.get("result").cloned().ok_or_else(|| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    String::from("subscribe missing result"),
                )
            })?;
            let terminals = result
                .get("terminals")
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            break serde_json::from_value::<Vec<Terminal>>(terminals).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("subscribe terminals: {e}"),
                )
            })?;
        };
        Ok((
            snapshot,
            HcEventStream {
                reader,
                fold_policy: self.fold_policy,
            },
        ))
    }
}

fn socket_timeout_from_params(params: &Value) -> Duration {
    let ms = params
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .or_else(|| {
            params
                .get("wait")
                .and_then(|w| w.get("timeout_ms"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(DEFAULT_RPC_TIMEOUT_MS);
    Duration::from_millis(ms.saturating_add(RPC_TIMEOUT_MARGIN_MS))
}

fn map_io_err(what: &str, e: std::io::Error) -> BackendError {
    let code = match e.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => BackendErrorCode::Timeout,
        _ => BackendErrorCode::Io,
    };
    BackendError::new(code, format!("{what}: {e}"))
}

fn map_wire_error(err: &Value) -> BackendError {
    let kind = err
        .get("data")
        .and_then(|d| d.get("kind"))
        .and_then(Value::as_str)
        .or_else(|| err.get("message").and_then(Value::as_str))
        .unwrap_or("error");
    let message = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or(kind)
        .to_string();
    BackendError::new(parse_code(kind), message)
}

fn parse_code(code: &str) -> BackendErrorCode {
    match code {
        "timeout" => BackendErrorCode::Timeout,
        "unsupported_generation" | "missing_method" => {
            BackendErrorCode::Other(code.to_string())
        }
        other => BackendErrorCode::Other(other.to_string()),
    }
}

/// Long-lived hc event stream (plain mode: notifications after subscribe).
pub struct HcEventStream {
    reader: BufReader<UnixStream>,
    fold_policy: FoldPolicy,
}

impl HcEventStream {
    fn decode_line(&self, line: &str) -> Result<Option<BackendEvent>, BackendError> {
        let val: Value = serde_json::from_str(line.trim()).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Malformed,
                format!("hc event: {line}: {e}"),
            )
        })?;
        // Notifications: { method, params } or { result: Terminal } variants.
        if let Some(method) = val.get("method").and_then(Value::as_str) {
            if method.contains("missed") {
                return Ok(Some(BackendEvent::EventsLost));
            }
            let params = val.get("params").cloned().unwrap_or(Value::Null);
            if let Some(event) = terminal_event_from_value(&params, self.fold_policy) {
                return Ok(Some(event));
            }
            return Ok(Some(BackendEvent::Raw(line.to_string())));
        }
        if let Some(event) = terminal_event_from_value(&val, self.fold_policy) {
            return Ok(Some(event));
        }
        Ok(Some(BackendEvent::Raw(line.to_string())))
    }
}

impl EventStream for HcEventStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        let mut buf = String::new();
        let n = self
            .reader
            .read_line(&mut buf)
            .map_err(|e| map_io_err("read hc event", e))?;
        if n == 0 {
            return Ok(None);
        }
        self.decode_line(&buf)
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), BackendError> {
        self.reader
            .get_mut()
            .set_read_timeout(timeout)
            .map_err(|e| BackendError::new(BackendErrorCode::Io, e.to_string()))
    }
}

fn terminal_event_from_value(val: &Value, policy: FoldPolicy) -> Option<BackendEvent> {
    // Prefer an embedded terminal object.
    let term_val = if val.get("address").is_some() {
        val.clone()
    } else {
        val.get("terminal")?.clone()
    };
    let term: Terminal = serde_json::from_value(term_val).ok()?;
    let status = term.agent_status(policy);
    let pane = PaneId::new(prefix_id(&term.address));
    if term.state.as_deref() == Some("finished") {
        return Some(BackendEvent::PaneExited { pane });
    }
    Some(BackendEvent::AgentStatusChanged {
        pane,
        agent: term.name.clone(),
        status,
    })
}

/// Sync [`Backend`] over hc OpenRPC.
#[derive(Debug, Clone)]
pub struct HcBackend {
    client: Client,
}

impl HcBackend {
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        Ok(Self {
            client: Client::connect(socket_path)?,
        })
    }

    pub fn connect_with_policy(
        socket_path: Option<&str>,
        policy: FoldPolicy,
    ) -> Result<Self, BackendError> {
        Ok(Self {
            client: Client::connect_with_policy(socket_path, policy)?,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl Backend for HcBackend {
    fn name(&self) -> &'static str {
        "hc"
    }

    fn pane_list(&self, _workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        self.client.pane_list()
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), BackendError> {
        let _ = self.client.terminal_kill(pane_id)?;
        let _ = self.client.terminal_rm(pane_id);
        Ok(())
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        self.client.agent_list()
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        _pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        let mut argv = Vec::with_capacity(1 + args.len());
        argv.push(kind.to_string());
        argv.extend(args.iter().cloned());
        Ok(self
            .client
            .terminal_run(&argv, Some(name), None)?
            .to_agent_info(self.client.fold_policy))
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        let Some(name) = new_name else {
            return Ok(());
        };
        self.client.call_coded(
            "terminal.label",
            json!({
                "terminal": strip_id_prefix(target),
                "labels": { "name": name },
            }),
        )?;
        Ok(())
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<Value, BackendError> {
        let wait_ms = wait.and_then(|w| w.timeout_ms);
        self.client.terminal_send(target, text, wait_ms)
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        match self.prompt(target, text, wait) {
            Ok(val) => {
                let status = val
                    .get("status")
                    .and_then(Value::as_str)
                    .map(normalize_herdr_status);
                Ok(PromptLanding {
                    status,
                    code: None,
                })
            }
            Err(err) => Ok(PromptLanding {
                status: None,
                code: Some(err.code().as_str().to_string()),
            }),
        }
    }

    fn agent_wait(
        &self,
        target: &str,
        _until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        let _ = self.client.terminal_wait(target, timeout_ms)?;
        self.client.agent_get(target)
    }

    fn agent_read(&self, target: &str, _source: &str, lines: u32) -> Result<Value, BackendError> {
        self.client.terminal_read(target, lines)
    }

    fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<(), BackendError> {
        let _ = self.client.terminal_keys(target, keys)?;
        Ok(())
    }

    fn subscribe(
        &self,
        filters: &[Subscription],
    ) -> Result<Box<dyn EventStream>, BackendError> {
        let terminal = filters.first().map(|f| strip_id_prefix(&f.pane_id));
        let (snapshot, stream) = self.client.subscribe_terminals(terminal)?;
        // Emit snapshot as status-changed events before returning the live stream.
        Ok(Box::new(SnapshotThenStream {
            pending: snapshot
                .into_iter()
                .map(|t| {
                    let status = t.agent_status(self.client.fold_policy);
                    BackendEvent::AgentStatusChanged {
                        pane: PaneId::new(prefix_id(&t.address)),
                        agent: t.name.clone(),
                        status,
                    }
                })
                .collect(),
            inner: stream,
        }))
    }
}

struct SnapshotThenStream {
    pending: VecDeque<BackendEvent>,
    inner: HcEventStream,
}

impl EventStream for SnapshotThenStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        if let Some(ev) = self.pending.pop_front() {
            return Ok(Some(ev));
        }
        self.inner.read_event()
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), BackendError> {
        self.inner.set_read_timeout(timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::sync::{Arc, Mutex};
    use std::thread;

    struct FakeHc {
        socket: PathBuf,
        _dir: tempfile::TempDir,
        _methods: Arc<Mutex<Vec<String>>>,
        _terminals: Arc<Mutex<Vec<Value>>>,
    }

    impl FakeHc {
        fn spawn(methods: Vec<String>, terminals: Vec<Value>) -> Self {
            let dir = tempfile::tempdir().expect("tmpdir");
            let socket = dir.path().join("test.sock");
            let methods = Arc::new(Mutex::new(methods));
            let terminals = Arc::new(Mutex::new(terminals));
            let listener = UnixListener::bind(&socket).expect("bind");
            let methods_c = Arc::clone(&methods);
            let terminals_c = Arc::clone(&terminals);
            thread::spawn(move || {
                while let Ok((stream, _)) = listener.accept() {
                    let methods = Arc::clone(&methods_c);
                    let terminals = Arc::clone(&terminals_c);
                    thread::spawn(move || serve_conn(stream, methods, terminals));
                }
            });
            // Give the listener a tick.
            thread::sleep(Duration::from_millis(20));
            Self {
                socket,
                _dir: dir,
                _methods: methods,
                _terminals: terminals,
            }
        }
    }

    fn serve_conn(
        stream: UnixStream,
        methods: Arc<Mutex<Vec<String>>>,
        terminals: Arc<Mutex<Vec<Value>>>,
    ) {
        let mut reader = BufReader::new(stream.try_clone().expect("clone"));
        let mut writer = stream;
        loop {
            let mut buf = String::new();
            if reader.read_line(&mut buf).ok().unwrap_or(0) == 0 {
                break;
            }
            let req: Value = match serde_json::from_str(buf.trim()) {
                Ok(v) => v,
                Err(_) => break,
            };
            let id = req.get("id").cloned().unwrap_or(json!(null));
            let method = req.get("method").and_then(Value::as_str).unwrap_or("");
            let result = match method {
                "hello" => {
                    let m = methods.lock().expect("lock").clone();
                    json!({
                        "generation": 1,
                        "version": "0.1.16-fake",
                        "core": { "id": "fake", "name": "fake" },
                        "framed": false,
                        "methods": m,
                        "features": ["events", "status"],
                    })
                }
                "terminal.list" => json!({
                    "terminals": terminals.lock().expect("lock").clone(),
                    "hidden_finished": 0,
                    "machines": [],
                }),
                "terminal.get" => {
                    let term = req
                        .pointer("/params/terminal")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    terminals
                        .lock()
                        .expect("lock")
                        .iter()
                        .find(|t| t.get("address").and_then(Value::as_str) == Some(term))
                        .cloned()
                        .unwrap_or(json!({"address": term}))
                }
                "terminal.send" | "terminal.keys" | "terminal.read" | "terminal.wait"
                | "terminal.rm" | "terminal.label" => json!({ "ok": true, "turn": 1 }),
                "terminal.kill" => {
                    let term = req
                        .pointer("/params/terminal")
                        .and_then(Value::as_str)
                        .unwrap_or("t1");
                    json!({ "address": term, "state": "finished" })
                }
                "terminal.run" => {
                    let name = req.pointer("/params/name").cloned();
                    let t = json!({
                        "address": "t9",
                        "name": name,
                        "status": { "state": "working" },
                        "records": {},
                        "state": "live",
                    });
                    terminals.lock().expect("lock").push(t.clone());
                    t
                }
                "events.subscribe" => {
                    // Snapshot then hold the connection open without further lines.
                    let snap = json!({
                        "seq": 1,
                        "terminals": terminals.lock().expect("lock").clone(),
                        "channel": null,
                    });
                    let resp = json!({"jsonrpc":"2.0","id": id, "result": snap});
                    let mut line = serde_json::to_string(&resp).unwrap();
                    line.push('\n');
                    let _ = writer.write_all(line.as_bytes());
                    let _ = writer.flush();
                    // Block until peer closes.
                    let mut sink = Vec::new();
                    let _ = reader.read_to_end(&mut sink);
                    break;
                }
                "rpc.discover" => json!({"openrpc": "1.3.2"}),
                other => {
                    let resp = json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {
                            "code": -32000,
                            "message": other,
                            "data": { "kind": "missing_method" },
                        }
                    });
                    let mut line = serde_json::to_string(&resp).unwrap();
                    line.push('\n');
                    let _ = writer.write_all(line.as_bytes());
                    let _ = writer.flush();
                    continue;
                }
            };
            let resp = json!({"jsonrpc":"2.0","id": id, "result": result});
            let mut line = serde_json::to_string(&resp).unwrap();
            line.push('\n');
            if writer.write_all(line.as_bytes()).is_err() {
                break;
            }
            let _ = writer.flush();
        }
    }

    fn full_methods() -> Vec<String> {
        REQUIRED_METHODS.iter().map(|s| (*s).to_string()).collect()
    }

    fn sample_terminal() -> Value {
        json!({
            "address": "t7",
            "name": "review",
            "state": "live",
            "cwd": "/tmp",
            "status": { "state": "working", "app": "deploy" },
            "records": {
                "child": { "state": "blocked", "kind": "permission" }
            },
            "reported": true,
            "labels": { "toron_pin": "DustySparrow" },
            "argv": ["claude"],
        })
    }

    #[test]
    fn resolve_socket_uses_session_under_xdg_state() {
        let p = resolve_socket_path(
            None,
            None,
            Some(Path::new("/state")),
            Some(Path::new("/home/x")),
            Some("s1"),
        );
        assert_eq!(p, PathBuf::from("/state/herdr-core/s1/s1.sock"));
    }

    #[test]
    fn connect_and_fold_attention_from_terminal_get() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        let term = client.terminal_get("t7").expect("get");
        assert_eq!(term.agent_status(FoldPolicy::Attention), AgentStatus::Blocked);
        assert_eq!(term.agent_status(FoldPolicy::RootWins), AgentStatus::Working);
        let info = client.agent_get("hc:t7").expect("agent");
        assert_eq!(info.pane_id, "hc:t7");
        assert_eq!(info.agent_status, AgentStatus::Blocked);
    }

    #[test]
    fn missing_method_at_hello_is_typed_error() {
        let mut methods = full_methods();
        methods.retain(|m| m != "events.subscribe");
        let fake = FakeHc::spawn(methods, vec![]);
        let err = Client::connect(Some(fake.socket.to_str().unwrap())).expect_err("skew");
        assert!(
            matches!(err.code(), BackendErrorCode::Other(s) if s.contains("missing_method")),
            "{err:?}"
        );
    }

    #[test]
    fn unsupported_generation_is_typed_error() {
        // Serve hello with generation 99 by using a custom listener.
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("g.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut writer = stream;
                let mut buf = String::new();
                let _ = reader.read_line(&mut buf);
                let req: Value = serde_json::from_str(buf.trim()).unwrap();
                let id = req.get("id").cloned().unwrap();
                let resp = json!({
                    "jsonrpc":"2.0","id": id,
                    "result": {
                        "generation": 99,
                        "version": "x",
                        "core": {"id":"c","name":"n"},
                        "framed": false,
                        "methods": full_methods(),
                        "features": [],
                    }
                });
                let mut line = serde_json::to_string(&resp).unwrap();
                line.push('\n');
                let _ = writer.write_all(line.as_bytes());
            }
        });
        thread::sleep(Duration::from_millis(20));
        let err = Client::connect(Some(socket.to_str().unwrap())).expect_err("gen");
        assert!(
            matches!(err.code(), BackendErrorCode::Other(s) if s == "unsupported_generation"),
            "{err:?}"
        );
    }

    #[test]
    fn backend_subscribe_emits_snapshot_status() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let mut stream = backend
            .subscribe(&[Subscription::agent_status_changed("hc:t7".into())])
            .expect("sub");
        let ev = stream.read_event().expect("ev").expect("some");
        match ev {
            BackendEvent::AgentStatusChanged { pane, status, .. } => {
                assert_eq!(pane.as_str(), "hc:t7");
                assert_eq!(status, AgentStatus::Blocked);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn workspace_methods_are_unsupported() {
        let fake = FakeHc::spawn(full_methods(), vec![]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let err = backend.workspace_list().expect_err("unsupported");
        assert_eq!(err.code(), &BackendErrorCode::Unsupported);
    }

    #[test]
    fn mail_name_unique_resolves_and_duplicate_is_refused() {
        let named = [
            (Some("review"), "t1"),
            (Some("fix"), "t2"),
            (None, "t3"),
        ];
        assert_eq!(
            mail_pane_id_for_name(&named, "review").unwrap(),
            "hc:t1"
        );
        assert_eq!(
            mail_pane_id_for_name(&named, "hc:t3").unwrap(),
            "hc:t3"
        );
        assert_eq!(
            mail_pane_id_for_name(&named, "missing"),
            Err(MailNameError::Missing)
        );
        // Unnamed terminal is not reachable by a made-up name; id still works.
        assert_eq!(
            mail_pane_id_for_name(&named, "t3").unwrap(),
            "hc:t3"
        );

        let dup = [(Some("twin"), "t1"), (Some("twin"), "t2"), (None, "t3")];
        assert_eq!(
            mail_pane_id_for_name(&dup, "twin"),
            Err(MailNameError::Duplicate { count: 2 })
        );
        // Id path still unique under a duplicate-name set.
        assert_eq!(mail_pane_id_for_name(&dup, "hc:t2").unwrap(), "hc:t2");

        let names = [Some("twin"), Some("twin"), None];
        assert_eq!(unique_mail_name(Some("twin"), &names), None);
        assert_eq!(unique_mail_name(None, &names), None);
        assert_eq!(unique_mail_name(Some("solo"), &[Some("solo"), None]), Some("solo"));
    }

    #[test]
    fn id_prefix_round_trip() {
        assert_eq!(strip_id_prefix("hc:t7"), "t7");
        assert_eq!(strip_id_prefix("t7"), "t7");
        assert_eq!(prefix_id("t7"), "hc:t7");
        assert_eq!(prefix_id("hc:t7"), "hc:t7");
    }

    /// Throwaway live hc server. Never touches ~/.config/herdr-core or the
    /// user's default session. Run with:
    /// `cargo test -p toron-core --lib live_hc_attention_roundtrip -- --ignored --nocapture`
    #[test]
    #[ignore = "live hc; needs ~/.local/bin/hc"]
    fn live_hc_attention_roundtrip() {
        use std::process::Command;
        use std::thread;

        let hc = PathBuf::from(
            std::env::var_os("HC_BIN").unwrap_or_else(|| "/Users/bangedorrunt/.local/bin/hc".into()),
        );
        assert!(hc.is_file(), "hc binary missing at {}", hc.display());

        let dir = tempfile::tempdir().expect("tmpdir");
        let home = dir.path().join("home");
        let config = dir.path().join("config");
        let data = dir.path().join("data");
        let state = dir.path().join("state");
        let cache = dir.path().join("cache");
        for p in [&home, &config, &data, &state, &cache] {
            std::fs::create_dir_all(p).unwrap();
        }
        let session = format!("toron-hc-live-{}", std::process::id());
        let sock = state
            .join("herdr-core")
            .join(&session)
            .join(format!("{session}.sock"));

        let log_dir = PathBuf::from(std::env::var_os("HC_LIVE_LOG_DIR").unwrap_or_else(|| {
            "/Users/bangedorrunt/workspace/stack-comms/logs/hc-backend".into()
        }));
        let _ = std::fs::create_dir_all(&log_dir);
        let transcript = log_dir.join(format!("live-hc-{session}.log"));
        let mut log = std::fs::File::create(&transcript).expect("transcript");
        let _ = writeln!(log, "session={session}");
        let _ = writeln!(log, "sock={}", sock.display());

        let apply_env = |cmd: &mut Command| {
            cmd.env("HOME", &home)
                .env("XDG_CONFIG_HOME", &config)
                .env("XDG_DATA_HOME", &data)
                .env("XDG_STATE_HOME", &state)
                .env("XDG_CACHE_HOME", &cache)
                .env("HC_SESSION", &session)
                .env_remove("HC_SOCKET_PATH")
                .env_remove("HERDR_SOCKET_PATH");
        };

        let stop_server = |hc: &Path, home: &Path, config: &Path, data: &Path, state: &Path, cache: &Path, session: &str| {
            let mut stop = Command::new(hc);
            stop.env("HOME", home)
                .env("XDG_CONFIG_HOME", config)
                .env("XDG_DATA_HOME", data)
                .env("XDG_STATE_HOME", state)
                .env("XDG_CACHE_HOME", cache)
                .env("HC_SESSION", session)
                .args(["server", "stop", "--json"]);
            let _ = stop.output();
        };

        let mut start = Command::new(&hc);
        apply_env(&mut start);
        let out = start.args(["server", "start", "--json"]).output().expect("start");
        let _ = writeln!(log, "start_status={}", out.status);
        let _ = writeln!(log, "start_stdout={}", String::from_utf8_lossy(&out.stdout));
        let _ = writeln!(log, "start_stderr={}", String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success(), "server start failed");
        assert!(sock.exists(), "socket missing at {}", sock.display());

        let script = "printf '\\033]7501;state=working:app=live\\033\\\\'; sleep 0.4; printf '\\033]7501;state=blocked:id=child:kind=permission\\033\\\\'; sleep 3; printf '\\033]7501;state=done\\033\\\\'; sleep 0.4; printf '\\033]7501;state=error:app=live\\033\\\\'; sleep 2";
        let mut run = Command::new(&hc);
        apply_env(&mut run);
        let run_out = run
            .args(["run", "--bg", "--name", "osc-live", "--", "sh", "-c", script])
            .output()
            .expect("hc run");
        let _ = writeln!(log, "run_status={}", run_out.status);
        let _ = writeln!(log, "run_stdout={}", String::from_utf8_lossy(&run_out.stdout));
        let _ = writeln!(log, "run_stderr={}", String::from_utf8_lossy(&run_out.stderr));
        if !run_out.status.success() {
            stop_server(&hc, &home, &config, &data, &state, &cache, &session);
            panic!("hc run failed; see {}", transcript.display());
        }

        let client = match Client::connect(Some(sock.to_str().unwrap())) {
            Ok(c) => c,
            Err(e) => {
                stop_server(&hc, &home, &config, &data, &state, &cache, &session);
                panic!("connect failed: {e}");
            }
        };
        let mut saw_blocked = false;
        let mut address = String::new();
        for i in 0..50 {
            thread::sleep(Duration::from_millis(150));
            let term = match client.terminal_get("osc-live") {
                Ok(t) => t,
                Err(e) => {
                    let _ = writeln!(log, "get_err[{i}]={e}");
                    continue;
                }
            };
            address = term.address.clone();
            let attn = term.agent_status(FoldPolicy::Attention);
            let root = term.agent_status(FoldPolicy::RootWins);
            let _ = writeln!(
                log,
                "poll[{i}] address={} attn={attn:?} root={root:?} status={:?} records={}",
                term.address,
                term.status.as_ref().map(|s| s.state.as_str()),
                term.records.len()
            );
            if attn == AgentStatus::Blocked && root == AgentStatus::Working {
                saw_blocked = true;
                break;
            }
        }
        if !saw_blocked {
            stop_server(&hc, &home, &config, &data, &state, &cache, &session);
            panic!(
                "never observed Attention=Blocked with RootWins=Working; see {}",
                transcript.display()
            );
        }

        let backend = HcBackend::connect(Some(sock.to_str().unwrap())).expect("backend");
        let mut stream = backend
            .subscribe(&[Subscription::agent_status_changed(prefix_id(&address))])
            .expect("subscribe");
        let ev = stream.read_event().expect("ev").expect("snapshot");
        let _ = writeln!(log, "subscribe_ev={ev:?}");
        match &ev {
            BackendEvent::AgentStatusChanged { status, .. } => {
                assert_eq!(*status, AgentStatus::Blocked);
            }
            other => {
                stop_server(&hc, &home, &config, &data, &state, &cache, &session);
                panic!("unexpected snapshot event {other:?}");
            }
        }

        let _ = client
            .terminal_send("osc-live", "ping-from-toron-live-test", None)
            .expect("send");
        let _ = writeln!(log, "send_ok");
        let _ = client.terminal_kill("osc-live").expect("kill");
        let _ = client.terminal_rm("osc-live");
        let _ = writeln!(log, "kill_rm_ok");

        stop_server(&hc, &home, &config, &data, &state, &cache, &session);
        let _ = writeln!(log, "server_stopped");
        thread::sleep(Duration::from_millis(300));
        assert!(!sock.exists(), "socket still present after stop");
        let ps = Command::new("ps").args(["aux"]).output().expect("ps");
        let ps_txt = String::from_utf8_lossy(&ps.stdout);
        let leaked: Vec<_> = ps_txt.lines().filter(|l| l.contains(&session)).collect();
        let _ = writeln!(log, "leaked_lines={leaked:?}");
        assert!(
            leaked.is_empty(),
            "hc processes still mention session {session}: {leaked:?}"
        );
        let _ = writeln!(log, "transcript_path={}", transcript.display());
        eprintln!("live hc transcript: {}", transcript.display());
    }

}
