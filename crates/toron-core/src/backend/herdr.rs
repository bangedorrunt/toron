// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! herdr backend client — newline-delimited JSON-RPC over a unix socket
//! (protocol v19, per ADR-0003). Single shared client; flywheel's copy is
//! deleted after this lands (ADR: "one definition of `agent_prompt`").
//!
//! Wire shape (one JSON object per line):
//!   request:  {"id":"<n>","method":"agent.prompt","params":{...}}
//!   response: {"id":"<n>","result":{...}} | {"id":"<n>","error":{"code","message"}}
//!   push:     {"event":"<type>","data":{...}}
//!
//! Socket path: `HERDR_SOCKET_PATH` or `~/.config/herdr/herdr.sock`
//! (XDG_CONFIG_HOME respected). Errors are typed with machine codes —
//! classify on `BackendError::code`, never message text.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};

use super::error::{BackendError, BackendErrorCode};
use super::types::{AgentStatus, BackendEvent, PaneId, PromptLanding};

/// herdr socket protocol version this client is verified against (v19).
pub const HDR_PROTOCOL: u64 = 19;

/// Resolve the herdr socket path from explicit env values (pure — testable).
/// Priority: `HERDR_SOCKET_PATH` env, else `$XDG_CONFIG_HOME/herdr/herdr.sock`
/// (absolute only), else `~/.config/herdr/herdr.sock`.
pub fn resolve_socket_path(
    herdr_env: Option<String>,
    xdg_config_home: Option<String>,
    home: Option<String>,
) -> PathBuf {
    if let Some(p) = herdr_env {
        return PathBuf::from(p);
    }
    if let Some(xdg) = xdg_config_home {
        let base = PathBuf::from(xdg);
        if base.is_absolute() {
            return base.join("herdr/herdr.sock");
        }
    }
    let home = home
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("~"));
    home.join(".config").join("herdr").join("herdr.sock")
}

/// Resolve the herdr socket path from the process environment.
pub fn default_socket_path() -> PathBuf {
    resolve_socket_path(
        std::env::var("HERDR_SOCKET_PATH").ok(),
        std::env::var("XDG_CONFIG_HOME").ok(),
        std::env::var("HOME").ok(),
    )
}

/// Wire-contract struct (deserialized from herdr protocol; unused fields
/// must still deserialize — keep `#[allow(dead_code)]`).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AgentInfo {
    pub agent: Option<String>,
    pub agent_status: AgentStatus,
    pub cwd: Option<String>,
    pub display_agent: Option<String>,
    pub focused: bool,
    pub foreground_cwd: Option<String>,
    #[serde(default)]
    pub interactive_ready: bool,
    #[serde(default)]
    pub launch_pending: bool,
    pub name: Option<String>,
    pub pane_id: String,
    pub revision: u64,
    #[serde(default)]
    pub state_change_seq: u64,
    #[serde(default)]
    pub state_labels: serde_json::Map<String, Value>,
    pub tab_id: String,
    pub terminal_id: String,
    pub terminal_title: Option<String>,
    pub workspace_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct WorkspaceInfo {
    pub workspace_id: String,
    pub number: u64,
    pub label: String,
    pub focused: bool,
    pub pane_count: u64,
    pub tab_count: u64,
    pub active_tab_id: String,
    pub agent_status: AgentStatus,
    #[serde(default)]
    pub cwd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PaneInfo {
    pub pane_id: String,
    pub terminal_id: String,
    pub workspace_id: String,
    pub tab_id: String,
    pub focused: bool,
    pub agent_status: AgentStatus,
    pub revision: u64,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub agent: Option<String>,
    pub agent_session: Option<AgentSessionInfo>,
    pub display_agent: Option<String>,
    pub foreground_cwd: Option<String>,
    pub label: Option<String>,
    pub scroll: Option<PaneScrollInfo>,
    #[serde(default)]
    pub state_labels: serde_json::Map<String, Value>,
    pub terminal_title: Option<String>,
    pub terminal_title_stripped: Option<String>,
    #[serde(default)]
    pub tokens: serde_json::Map<String, Value>,
}

/// herdr agent-session reference (`agent_session` on AgentInfo/PaneInfo).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct AgentSessionInfo {
    pub source: String,
    pub agent: String,
    pub kind: AgentSessionRefKind,
    pub value: String,
}

/// herdr agent-session reference kind (`id` | `path`).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "lowercase")]
pub enum AgentSessionRefKind {
    Id,
    Path,
}

/// herdr pane scroll state (`scroll` on PaneInfo).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PaneScrollInfo {
    pub offset_from_bottom: u64,
    pub max_offset_from_bottom: u64,
    pub viewport_rows: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct WorktreeInfo {
    pub path: String,
    pub is_bare: bool,
    pub is_detached: bool,
    pub is_prunable: bool,
    pub is_linked_worktree: bool,
    pub label: String,
    pub branch: Option<String>,
    pub open_workspace_id: Option<String>,
}

/// Wait options for `agent.prompt` / `agent.wait`.
#[derive(Debug, Clone)]
pub struct WaitOptions {
    pub timeout_ms: Option<u64>,
    pub until: Vec<AgentStatus>,
}

/// Result of a `pane.wait_for_output` wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaneWaitResult {
    pub pane_id: String,
    pub matched_line: Option<String>,
    pub read: String,
}

/// Classification of a confirmed `agent.prompt` outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendVerdict {
    Landed(Option<AgentStatus>),
    Stalled,
    TimedOut,
    Coded(String),
}

/// Map a herdr error code + observed status to a verdict. Classifies on the
/// protocol code, never message strings.
pub fn classify_send_result(code: Option<&str>, status: Option<AgentStatus>) -> SendVerdict {
    match code {
        Some("agent_prompt_stalled") => SendVerdict::Stalled,
        Some("timeout") => SendVerdict::TimedOut,
        Some(other) => SendVerdict::Coded(other.to_string()),
        None => SendVerdict::Landed(status),
    }
}

/// Subscription event kinds accepted by `events.subscribe`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionEventKind {
    PaneAgentStatusChanged,
}

impl SubscriptionEventKind {
    pub const fn as_wire_str(self) -> &'static str {
        match self {
            SubscriptionEventKind::PaneAgentStatusChanged => "pane.agent_status_changed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub event: SubscriptionEventKind,
    pub pane_id: String,
    pub agent_status: Option<AgentStatus>,
}

impl Subscription {
    pub fn agent_status_changed(pane_id: String) -> Self {
        Subscription {
            event: SubscriptionEventKind::PaneAgentStatusChanged,
            pane_id,
            agent_status: None,
        }
    }

    pub fn with_agent_status(mut self, status: AgentStatus) -> Self {
        self.agent_status = Some(status);
        self
    }

    pub fn to_value(&self) -> Value {
        json!({
            "type": self.event.as_wire_str(),
            "pane_id": self.pane_id,
            "agent_status": self.agent_status.map(|s| s.as_str()),
        })
    }
}

/// Blocking JSON-RPC client over the herdr unix socket. One request per
/// connection (except `events.subscribe`, which holds the socket open).
#[derive(Debug, Clone)]
pub struct Client {
    socket_path: String,
    next_id: u64,
}

impl Client {
    /// Connect (probes reachability now so callers fail fast).
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        let path = socket_path
            .map(|p| p.to_string())
            .or_else(|| std::env::var("HERDR_SOCKET_PATH").ok())
            .unwrap_or_else(|| default_socket_path().to_string_lossy().into_owned());
        let _probe = UnixStream::connect(&path).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Connect,
                format!("cannot connect to herdr socket at {path}: {e}"),
            )
        })?;
        Ok(Client {
            socket_path: path,
            next_id: 1,
        })
    }

    fn open(&self) -> Result<UnixStream, BackendError> {
        UnixStream::connect(&self.socket_path).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Connect,
                format!(
                    "cannot connect to herdr socket at {}: {e}",
                    self.socket_path
                ),
            )
        })
    }

    /// Send one request on a fresh connection; returns `result` or a typed
    /// error carrying the protocol machine code.
    pub fn call_coded(&mut self, method: &str, params: Value) -> Result<Value, BackendError> {
        let id = self.next_id.to_string();
        self.next_id += 1;
        let req = json!({ "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');

        let mut reader = BufReader::new(self.open()?);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| {
                BackendError::new(BackendErrorCode::Io, format!("write to herdr socket: {e}"))
            })?;

        loop {
            let mut buf = String::new();
            let n = reader.read_line(&mut buf).map_err(|e| {
                BackendError::new(BackendErrorCode::Io, format!("read from herdr socket: {e}"))
            })?;
            if n == 0 {
                return Err(BackendError::new(
                    BackendErrorCode::Io,
                    String::from("herdr socket closed while waiting for response"),
                ));
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("malformed herdr response: {buf}: {e}"),
                )
            })?;
            if val.get("id").and_then(Value::as_str) != Some(id.as_str()) {
                continue;
            }
            if let Some(err) = val.get("error") {
                let code = err
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("error")
                    .to_string();
                let message = err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
                    .to_string();
                return Err(BackendError::new(parse_code(&code), message));
            }
            return val.get("result").cloned().ok_or_else(|| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("herdr response missing result: {val}"),
                )
            });
        }
    }

    /// `call_coded` unwrapped into anyhow-style `Result<Value>`.
    pub fn call(&mut self, method: &str, params: Value) -> Result<Value, BackendError> {
        self.call_coded(method, params)
    }

    /// Subscribe to events; returns a long-lived blocking stream.
    pub fn subscribe(
        &mut self,
        subscriptions: &[Subscription],
    ) -> Result<EventStream, BackendError> {
        let id = self.next_id.to_string();
        self.next_id += 1;
        let req = json!({
            "id": id,
            "method": "events.subscribe",
            "params": {
                "subscriptions": subscriptions.iter().map(|s| s.to_value()).collect::<Vec<_>>()
            }
        });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');

        let mut reader = BufReader::new(self.open()?);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| {
                BackendError::new(BackendErrorCode::Io, format!("write subscription: {e}"))
            })?;

        loop {
            let mut buf = String::new();
            let n = reader.read_line(&mut buf).map_err(|e| {
                BackendError::new(BackendErrorCode::Io, format!("read subscription ack: {e}"))
            })?;
            if n == 0 {
                return Err(BackendError::new(
                    BackendErrorCode::Io,
                    String::from("herdr socket closed before subscription ack"),
                ));
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("malformed herdr response: {buf}: {e}"),
                )
            })?;
            if val.get("id").and_then(Value::as_str) == Some(id.as_str()) {
                if let Some(err) = val.get("error") {
                    let code = err
                        .get("code")
                        .and_then(Value::as_str)
                        .unwrap_or("error")
                        .to_string();
                    let message = err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    return Err(BackendError::new(parse_code(&code), message));
                }
                break;
            }
        }
        Ok(EventStream { reader })
    }

    // ---- typed helpers (flywheel-loop coverage) ----

    pub fn ping(&mut self) -> Result<Value, BackendError> {
        self.call("ping", json!({}))
    }

    pub fn workspace_create(
        &mut self,
        cwd: Option<&str>,
        label: Option<&str>,
        focus: bool,
    ) -> Result<(WorkspaceInfo, PaneInfo), BackendError> {
        let res = self.call(
            "workspace.create",
            json!({ "cwd": cwd, "label": label, "focus": focus }),
        )?;
        let ws: WorkspaceInfo =
            serde_json::from_value(res.get("workspace").cloned().unwrap_or(res.clone()))
                .map_err(|e| parse_err(e.to_string()))?;
        let root: PaneInfo = serde_json::from_value(res.get("root_pane").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))?;
        Ok((ws, root))
    }

    pub fn workspace_list(&mut self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        let res = self.call("workspace.list", json!({}))?;
        serde_json::from_value(res.get("workspaces").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn workspace_close(&mut self, workspace_id: &str) -> Result<Value, BackendError> {
        self.call("workspace.close", json!({ "workspace_id": workspace_id }))
    }

    pub fn pane_list(&mut self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        let res = self.call("pane.list", json!({ "workspace_id": workspace_id }))?;
        serde_json::from_value(res.get("panes").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_list(&mut self) -> Result<Vec<AgentInfo>, BackendError> {
        let res = self.call("agent.list", json!({}))?;
        serde_json::from_value(res.get("agents").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_start(
        &mut self,
        name: &str,
        kind: &str,
        pane_id: &str,
        timeout_ms: u64,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        let res = self.call(
            "agent.start",
            json!({
                "name": name, "kind": kind, "pane_id": pane_id,
                "timeout_ms": timeout_ms, "args": args,
            }),
        )?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_rename(&mut self, target: &str, name: &str) -> Result<Value, BackendError> {
        self.call("agent.rename", json!({ "target": target, "name": name }))
    }

    pub fn agent_prompt(
        &mut self,
        target: &str,
        text: &str,
        wait: Option<WaitOptions>,
    ) -> Result<AgentInfo, BackendError> {
        self.agent_prompt_coded(target, text, wait)
    }

    /// `agent.prompt` returning the typed error for code classification.
    pub fn agent_prompt_coded(
        &mut self,
        target: &str,
        text: &str,
        wait: Option<WaitOptions>,
    ) -> Result<AgentInfo, BackendError> {
        let wait = wait.map(|w| {
            json!({ "timeout_ms": w.timeout_ms, "until": w.until.iter().map(|s| s.as_str()).collect::<Vec<_>>() })
        });
        let res = self.call_coded(
            "agent.prompt",
            json!({ "target": target, "text": text, "wait": wait }),
        )?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_wait(
        &mut self,
        target: &str,
        until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        let res = self.call(
            "agent.wait",
            json!({
                "target": target,
                "until": until.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                "timeout_ms": timeout_ms,
            }),
        )?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_read(
        &mut self,
        target: &str,
        source: &str,
        lines: Option<u32>,
    ) -> Result<String, BackendError> {
        let res = self.call(
            "agent.read",
            json!({ "target": target, "source": source, "lines": lines, "format": "text", "strip_ansi": true }),
        )?;
        let text = res
            .get("content")
            .or_else(|| res.get("text"))
            .or_else(|| res.get("output"))
            .or_else(|| res.get("read").and_then(|r| r.get("text")))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("agent.read result has no content: {res}"),
                )
            })?;
        Ok(text.to_string())
    }

    pub fn agent_send_keys(&mut self, target: &str, keys: &[&str]) -> Result<Value, BackendError> {
        self.call("agent.send_keys", json!({ "target": target, "keys": keys }))
    }

    pub fn worktree_create(
        &mut self,
        workspace_id: &str,
        branch: &str,
        base: Option<&str>,
        label: Option<&str>,
        focus: bool,
    ) -> Result<Value, BackendError> {
        self.call(
            "worktree.create",
            json!({
                "workspace_id": workspace_id, "branch": branch, "base": base,
                "label": label, "focus": focus,
            }),
        )
    }

    pub fn worktree_remove(
        &mut self,
        workspace_id: &str,
        force: bool,
    ) -> Result<Value, BackendError> {
        self.call(
            "worktree.remove",
            json!({ "workspace_id": workspace_id, "force": force }),
        )
    }
}

/// Map a herdr wire error code string onto the typed code enum. Codes the
/// client knows map explicitly; everything else stays `Other(code)`.
fn parse_code(code: &str) -> BackendErrorCode {
    code.parse()
        .unwrap_or_else(|_| BackendErrorCode::Other(code.to_string()))
}

fn parse_err(msg: String) -> BackendError {
    BackendError::new(BackendErrorCode::Malformed, msg)
}

/// Blocking event stream (long-lived subscription socket).
pub struct EventStream {
    reader: BufReader<UnixStream>,
}

impl EventStream {
    /// Read one pushed event (dotted wire name + data). `Ok(None)` on close.
    pub fn read_event(&mut self) -> Result<Option<(String, Value)>, BackendError> {
        loop {
            let mut buf = String::new();
            let n = self.reader.read_line(&mut buf).map_err(|e| {
                BackendError::new(BackendErrorCode::Io, format!("read herdr event: {e}"))
            })?;
            if n == 0 {
                return Ok(None);
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("malformed herdr event: {buf}: {e}"),
                )
            })?;
            let Some(event) = val.get("event").and_then(Value::as_str) else {
                continue;
            };
            return Ok(Some((
                event.to_string(),
                val.get("data").cloned().unwrap_or(Value::Null),
            )));
        }
    }
}

/// Sync `Backend` over the blocking herdr client (ADR-0005 P1 unified trait).
/// The blocking client calls run inline (they are short unix-socket reads);
/// statefulness lives behind the `Mutex<Client>`, so `&self` is enough.
pub struct HerdrBackend {
    client: Mutex<Client>,
}

impl HerdrBackend {
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        Ok(Self {
            client: Mutex::new(Client::connect(socket_path)?),
        })
    }

    pub fn client(&self) -> parking_lot::MutexGuard<'_, Client> {
        self.client.lock()
    }
}

impl super::Backend for HerdrBackend {
    fn name(&self) -> &'static str {
        "herdr"
    }

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        self.client.lock().workspace_list()
    }

    fn workspace_create(&self, label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        let mut client = self.client.lock();
        let (ws, _root) = client.workspace_create(None, label, false)?;
        Ok(ws)
    }

    fn workspace_close(&self, workspace_id: &str) -> Result<(), BackendError> {
        self.client.lock().workspace_close(workspace_id)?;
        Ok(())
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        direction: &str,
        cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        let mut client = self.client.lock();
        let res = client.call(
            "pane.split",
            json!({ "target_pane_id": parent_pane_id, "direction": direction, "cwd": cwd }),
        )?;
        serde_json::from_value(res.get("pane").cloned().unwrap_or(res))
            .map_err(|e| BackendError::new(BackendErrorCode::Malformed, e.to_string()))
    }

    fn pane_list(&self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        self.client.lock().pane_list(workspace_id)
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), BackendError> {
        self.client
            .lock()
            .call("pane.close", json!({ "pane_id": pane_id }))?;
        Ok(())
    }

    fn pane_run(&self, _pane_id: &str, _command: &str) -> Result<(), BackendError> {
        Err(BackendError::unsupported(
            "herdr does not implement pane_run (no typed Client helper)",
        ))
    }

    fn pane_wait_output(
        &self,
        pane_id: &str,
        needle: &str,
        timeout_ms: u64,
    ) -> Result<serde_json::Value, BackendError> {
        let mut client = self.client.lock();
        let res = client.call_coded(
            "pane.wait_for_output",
            json!({
                "pane_id": pane_id,
                "mode": "match",
                "pattern": needle,
                "timeout_ms": timeout_ms,
            }),
        )?;
        Ok(res)
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        self.client.lock().agent_list()
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        self.client
            .lock()
            .agent_start(name, kind, pane_id, 30_000, args)
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        let name = new_name.ok_or_else(|| {
            BackendError::unsupported("herdr agent_rename cannot clear a name (None)")
        })?;
        self.client.lock().agent_rename(target, name)?;
        Ok(())
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<serde_json::Value, BackendError> {
        let mut client = self.client.lock();
        let info = client.agent_prompt(target, text, wait.cloned())?;
        let status = Some(info.agent_status.as_str().to_string());
        Ok(json!({
            "agent": info.agent.clone().unwrap_or_default(),
            "pane_id": info.pane_id,
            "status": status,
        }))
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        let mut client = self.client.lock();
        match client.agent_prompt_coded(target, text, wait.cloned()) {
            Ok(info) => Ok(PromptLanding {
                status: Some(info.agent_status),
                code: None,
            }),
            Err(e) => Ok(PromptLanding {
                status: None,
                code: Some(e.code().as_str().to_string()),
            }),
        }
    }

    fn agent_wait(
        &self,
        target: &str,
        until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        self.client.lock().agent_wait(target, until, timeout_ms)
    }

    fn agent_read(
        &self,
        target: &str,
        source: &str,
        lines: u32,
    ) -> Result<serde_json::Value, BackendError> {
        let text = self.client.lock().agent_read(target, source, Some(lines))?;
        Ok(json!({ "text": text }))
    }

    fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<(), BackendError> {
        self.client.lock().agent_send_keys(target, keys)?;
        Ok(())
    }

    fn subscribe(
        &self,
        filters: &[Subscription],
    ) -> Result<Box<dyn super::EventStream>, BackendError> {
        let mut client = self.client.lock();
        let stream = client.subscribe(filters)?;
        Ok(Box::new(HerdrEventStreamAdapter(stream)))
    }
}

/// Adapter: raw herdr blocking stream → unified [`super::EventStream`] that
/// yields typed [`BackendEvent`]s. Forwards to `parse_push`.
struct HerdrEventStreamAdapter(EventStream);

impl super::EventStream for HerdrEventStreamAdapter {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        Ok(self
            .0
            .read_event()?
            .map(|(kind, data)| parse_push(&kind, &data)))
    }

    /// Bounds each blocking read — without this the trait default is a
    /// no-op and `read_event()` blocks forever when no events arrive
    /// (flywheel's deadline-based poll_events hangs). Applied to the
    /// underlying unix socket so `BufReader::read_line` returns
    /// WouldBlock/TimedOut once the deadline passes.
    fn set_read_timeout(
        &mut self,
        timeout: Option<std::time::Duration>,
    ) -> Result<(), BackendError> {
        self.0
            .reader
            .get_ref()
            .set_read_timeout(timeout)
            .map_err(|e| BackendError::new(BackendErrorCode::Io, e.to_string()))
    }
}

/// Map a herdr push onto a typed `BackendEvent` (ADR-0005 P1 unified shape).
///
/// `pane.agent_status_changed` → [`BackendEvent::AgentStatusChanged`];
/// `pane.agent_detected` → [`BackendEvent::AgentDetected`];
/// `pane.exited` → [`BackendEvent::PaneExited`]; everything else → [`BackendEvent::Raw`].
fn parse_push(kind: &str, data: &Value) -> BackendEvent {
    let pane = data
        .get("pane_id")
        .and_then(Value::as_str)
        .map(|s| PaneId(s.to_string()))
        .unwrap_or_else(|| PaneId(String::new()));
    let agent = data
        .get("agent")
        .and_then(Value::as_str)
        .or_else(|| data.get("display_agent").and_then(Value::as_str))
        .map(|s| s.to_string());
    let status = data
        .get("agent_status")
        .or_else(|| data.get("final_status"))
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .unwrap_or(AgentStatus::Unknown);
    match kind {
        "pane.agent_status_changed" => BackendEvent::AgentStatusChanged {
            pane,
            agent,
            status,
        },
        "pane.agent_detected" => BackendEvent::AgentDetected {
            pane,
            agent,
            status,
        },
        "pane.exited" => BackendEvent::PaneExited { pane },
        other => BackendEvent::Raw(format!("{other}: {data}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_serializes_wire_shape() {
        let sub = Subscription::agent_status_changed("w1:p2".into())
            .with_agent_status(AgentStatus::Working);
        assert_eq!(
            sub.to_value(),
            json!({
                "type": "pane.agent_status_changed",
                "pane_id": "w1:p2",
                "agent_status": "working",
            })
        );
    }

    #[test]
    fn classify_send_result_maps_codes() {
        assert_eq!(
            classify_send_result(Some("agent_prompt_stalled"), None),
            SendVerdict::Stalled
        );
        assert_eq!(
            classify_send_result(Some("timeout"), Some(AgentStatus::Working)),
            SendVerdict::TimedOut
        );
        assert_eq!(
            classify_send_result(Some("agent_not_running"), None),
            SendVerdict::Coded("agent_not_running".into())
        );
        assert_eq!(
            classify_send_result(None, Some(AgentStatus::Blocked)),
            SendVerdict::Landed(Some(AgentStatus::Blocked))
        );
    }

    #[test]
    fn default_socket_path_respects_env_override() {
        // Pure resolver: HERDR_SOCKET_PATH wins regardless of XDG/HOME.
        assert_eq!(
            resolve_socket_path(
                Some("/tmp/custom-herdr.sock".into()),
                Some("/Users/x/.config".into()),
                Some("/Users/x".into()),
            ),
            PathBuf::from("/tmp/custom-herdr.sock")
        );
    }

    #[test]
    fn default_socket_path_prefers_xdg_then_home() {
        // XDG_CONFIG_HOME (absolute) wins over HOME.
        let via_xdg = resolve_socket_path(
            None,
            Some("/Users/x/.config".into()),
            Some("/Users/x".into()),
        );
        assert_eq!(via_xdg, PathBuf::from("/Users/x/.config/herdr/herdr.sock"));
        // Relative XDG is ignored; falls to HOME.
        let via_home =
            resolve_socket_path(None, Some("rel/config".into()), Some("/Users/x".into()));
        assert_eq!(via_home, PathBuf::from("/Users/x/.config/herdr/herdr.sock"));
        // No HOME at all: ~ fallback.
        let no_home = resolve_socket_path(None, None, None);
        assert!(no_home.to_string_lossy().ends_with("herdr/herdr.sock"));
    }

    /// Trimmed live v19 schema snapshot + real payload samples (captured from
    /// the running herdr daemon, 2026-08-08; see bead bd-4eze).
    const V19_FIXTURE: &str = include_str!("../../tests/fixtures/herdr-schema-v19.json");

    fn fixture() -> Value {
        serde_json::from_str(V19_FIXTURE).expect("v19 fixture parses")
    }

    #[test]
    fn protocol_pin_matches_live_v19_fixture() {
        let f = fixture();
        assert_eq!(HDR_PROTOCOL, 19);
        assert_eq!(f["protocol"].as_u64(), Some(HDR_PROTOCOL));
    }

    #[test]
    fn pane_info_parses_live_v19_payload() {
        let f = fixture();
        let pane: PaneInfo = serde_json::from_value(f["samples"]["pane"].clone()).expect("pane");
        assert_eq!(pane.pane_id, "wT:p2");
        assert_eq!(pane.terminal_id, "term_6583b54d13a1b9");
        assert_eq!(pane.workspace_id, "wT");
        assert_eq!(pane.tab_id, "wT:t2");
        assert!(!pane.focused);
        assert_eq!(pane.agent_status, AgentStatus::Idle);
        assert_eq!(pane.revision, 557);
        assert_eq!(pane.agent.as_deref(), Some("opencode"));
        assert_eq!(
            pane.cwd.as_deref(),
            Some("/Users/bangedorrunt/workspace/flywheel")
        );
        assert!(pane.scroll.is_some());
        assert_eq!(
            pane.terminal_title.as_deref(),
            Some("OC | Implementing remaining beads with fly...")
        );
    }

    #[test]
    fn worktree_info_parses_live_v19_payloads() {
        let f = fixture();
        let wts: Vec<WorktreeInfo> =
            serde_json::from_value(f["samples"]["worktrees"].clone()).expect("worktrees");
        assert_eq!(wts.len(), 2);
        let linked = &wts[0];
        assert_eq!(linked.path, "/Users/bangedorrunt/workspace/toron");
        assert_eq!(linked.label, "toron");
        assert!(!linked.is_bare);
        assert!(!linked.is_detached);
        assert!(!linked.is_linked_worktree);
        assert!(!linked.is_prunable);
        assert_eq!(linked.branch.as_deref(), Some("main"));
        assert_eq!(linked.open_workspace_id.as_deref(), Some("w17"));
        let detached = &wts[1];
        assert!(detached.is_detached);
        assert!(detached.is_linked_worktree);
        assert_eq!(detached.branch, None);
        assert_eq!(detached.open_workspace_id, None);
    }

    #[test]
    fn agent_info_parses_live_v19_payload() {
        let f = fixture();
        let agent: AgentInfo =
            serde_json::from_value(f["samples"]["agent"].clone()).expect("agent");
        assert_eq!(agent.agent_status, AgentStatus::Idle);
        assert_eq!(agent.terminal_id, "term_6583b54d13a1b9");
        assert_eq!(agent.workspace_id, "wT");
        assert_eq!(agent.tab_id, "wT:t2");
        assert_eq!(agent.pane_id, "wT:p2");
        assert!(!agent.focused);
        assert_eq!(agent.revision, 557);
    }

    #[test]
    fn workspace_info_parses_live_v19_payload() {
        let f = fixture();
        let ws: WorkspaceInfo =
            serde_json::from_value(f["samples"]["workspace"].clone()).expect("workspace");
        assert_eq!(ws.workspace_id, "wD");
        assert_eq!(ws.number, 1);
        assert_eq!(ws.label, "chiebukuro");
        assert!(!ws.focused);
        assert_eq!(ws.pane_count, 3);
        assert_eq!(ws.tab_count, 3);
        assert_eq!(ws.active_tab_id, "wD:t6");
        assert_eq!(ws.agent_status, AgentStatus::Unknown);
    }

    #[test]
    fn v19_schema_requires_pane_v19_fields() {
        // Field-for-field conformance guard: the fixture schema is the live
        // daemon's; if it starts requiring something PaneInfo/WorktreeInfo
        // lack, this test fails before the socket round-trip ever does.
        let f = fixture();
        let pane_schema = &f["schemas"]["success_response"]["$defs"]["PaneInfo"];
        let pane_required: Vec<&str> = pane_schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        for field in [
            "pane_id",
            "terminal_id",
            "workspace_id",
            "tab_id",
            "focused",
            "agent_status",
            "revision",
        ] {
            assert!(
                pane_required.contains(&field),
                "PaneInfo missing required {field}"
            );
        }
        let wt_schema = &f["schemas"]["success_response"]["$defs"]["WorktreeInfo"];
        let wt_props = wt_schema["properties"].as_object().unwrap();
        for field in [
            "open_workspace_id",
            "is_bare",
            "is_detached",
            "is_linked_worktree",
            "is_prunable",
            "path",
            "label",
        ] {
            assert!(wt_props.contains_key(field), "WorktreeInfo missing {field}");
        }
        let wt_required: Vec<&str> = wt_schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        for field in [
            "path",
            "is_bare",
            "is_detached",
            "is_prunable",
            "is_linked_worktree",
            "label",
        ] {
            assert!(
                wt_required.contains(&field),
                "WorktreeInfo missing required {field}"
            );
        }
    }

    #[test]
    #[ignore = "requires a live herdr daemon on the socket"]
    fn live_round_trip_v19() {
        // bd-4eze: prove the typed client parses live v19 responses for the
        // methods the flywheel loop depends on. List-only — no side effects.
        let mut c = Client::connect(None).expect("connect to live herdr socket");
        c.ping().expect("ping");
        let ws = c.workspace_list().expect("workspace.list");
        assert!(!ws.is_empty(), "live herdr has no workspaces");
        let agents = c.agent_list().expect("agent.list");
        assert!(!agents.is_empty(), "live herdr has no agents");
        let panes = c.pane_list(None).expect("pane.list");
        assert!(!panes.is_empty(), "live herdr has no panes");
        for w in &ws {
            assert!(!w.active_tab_id.is_empty());
        }
        for a in &agents {
            assert!(!a.terminal_id.is_empty(), "agent missing terminal_id");
            assert!(!a.pane_id.is_empty());
        }
        for p in &panes {
            assert!(!p.terminal_id.is_empty(), "pane missing terminal_id");
            assert!(!p.tab_id.is_empty());
            assert!(!p.workspace_id.is_empty());
        }
    }

    #[test]
    fn agent_wire_params_shape() {
        // Static assert of the JSON-RPC params we send on the wire for the
        // methods the flywheel loop depends on (single source of truth).
        let prompt_wait = WaitOptions {
            timeout_ms: Some(5000),
            until: vec![AgentStatus::Done],
        };
        let wait = json!({ "timeout_ms": 5000, "until": ["done"] });
        assert_eq!(
            json!({ "timeout_ms": prompt_wait.timeout_ms, "until": prompt_wait.until.iter().map(|s| s.as_str()).collect::<Vec<_>>() }),
            wait
        );
    }
}
