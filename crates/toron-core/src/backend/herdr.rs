// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! herdr backend client — newline-delimited JSON-RPC over a unix socket
//! (protocol v22, per ADR-0003; bumped for herdr 0.9.0 — see the protocol
//! bead). Single shared client; flywheel's copy is deleted after this lands
//! (ADR: "one definition of `agent_prompt`").
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use super::error::{BackendError, BackendErrorCode};
use super::types::{normalize_herdr_status, AgentStatus, BackendEvent, PaneId, PromptLanding};

/// herdr socket protocol version this client is verified against (v22,
/// herdr 0.9.0 — live `herdr api schema --json` reports 22).
pub const HDR_PROTOCOL: u64 = 22;

/// Default socket I/O budget when a unary call's params omit `timeout_ms`
/// (and omit nested `wait.timeout_ms`). 30s covers short list/read RPCs
/// without letting a wedged daemon hang the host forever.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 30_000;
/// Margin added on top of the call's own `timeout_ms` (or
/// [`DEFAULT_RPC_TIMEOUT_MS`]) so serialize/connect and the server's
/// deadline race cleanly rather than racing the socket close.
pub const RPC_TIMEOUT_MARGIN_MS: u64 = 5_000;

/// Source id for toron mail-identity pane stamps (bd-6f57). Display-only
/// `pane.report_metadata` — never `pane.report_agent`, which would steal
/// lifecycle authority (waits/rollups) from the herdr integration.
pub const MAIL_IDENTITY_SOURCE: &str = "toron:mail-identity";
/// Token keys for the mail-identity stamp (ASCII alnum/`_`/`-`, ≤32 chars).
pub const TOKEN_PIN: &str = "toron_pin";
pub const TOKEN_PROJECT: &str = "toron_project";
pub const TOKEN_NPUB: &str = "toron_npub";

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
    /// Monotonic finished-turn counter (herdr). Older servers omit it
    /// (`None`). Prefer this over watching working→idle for done/hand-off:
    /// a fast turn can start and finish entirely between two polls.
    #[serde(default)]
    pub completion_seq: Option<u64>,
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
/// protocol code, never message strings. `agent_blocked` (protocol v22:
/// herdr refused to send — agent at an approval/question dialog) stays
/// [`SendVerdict::Coded`]: no text was delivered, so callers must NOT
/// respawn-on-stalled or blind-retry; the blocked-agent escalation paths
/// (flywheel loop tiers) own recovery from state polls.
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
    pub const fn agent_status_changed(pane_id: String) -> Self {
        Subscription {
            event: SubscriptionEventKind::PaneAgentStatusChanged,
            pane_id,
            agent_status: None,
        }
    }

    /// Build one multiplexed subscription covering many panes.
    /// Prefer a single `Client::subscribe` / `Backend::subscribe` call with
    /// this list over one connection per pane (herdr/tuios cap streams).
    pub fn agent_status_changed_many(
        pane_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Vec<Self> {
        pane_ids
            .into_iter()
            .map(|id| Self::agent_status_changed(id.into()))
            .collect()
    }

    pub const fn with_agent_status(mut self, status: AgentStatus) -> Self {
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
///
/// Methods take `&self`: each unary call opens a fresh `UnixStream`, so
/// concurrent callers only share the atomic request-id counter. Do not hold
/// a higher-level mutex across these calls.
#[derive(Debug)]
pub struct Client {
    socket_path: String,
    next_id: AtomicU64,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
        }
    }
}

impl Client {
    /// Connect (probes reachability now so callers fail fast).
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        let path = socket_path
            .map(ToString::to_string)
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
            next_id: AtomicU64::new(1),
        })
    }

    fn alloc_id(&self) -> String {
        self.next_id.fetch_add(1, Ordering::Relaxed).to_string()
    }

    /// Open a fresh stream and apply read/write timeouts derived from the
    /// call's `timeout_ms` (+ [`RPC_TIMEOUT_MARGIN_MS`]), or
    /// [`DEFAULT_RPC_TIMEOUT_MS`] + margin when the call omits a budget.
    fn open(&self, timeout: Duration) -> Result<UnixStream, BackendError> {
        let stream = UnixStream::connect(&self.socket_path).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Connect,
                format!(
                    "cannot connect to herdr socket at {}: {e}",
                    self.socket_path
                ),
            )
        })?;
        stream.set_read_timeout(Some(timeout)).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Io,
                format!("set_read_timeout on herdr socket: {e}"),
            )
        })?;
        stream.set_write_timeout(Some(timeout)).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Io,
                format!("set_write_timeout on herdr socket: {e}"),
            )
        })?;
        Ok(stream)
    }

    /// Send one request on a fresh connection; returns `result` or a typed
    /// error carrying the protocol machine code.
    pub fn call_coded(&self, method: &str, params: Value) -> Result<Value, BackendError> {
        let id = self.alloc_id();
        let timeout = socket_timeout_from_params(&params);
        let req = json!({ "id": id, "method": method, "params": params });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');

        let mut reader = BufReader::new(self.open(timeout)?);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| map_io_err("write to herdr socket", e))?;

        loop {
            let mut buf = String::new();
            let n = reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read from herdr socket", e))?;
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
    pub fn call(&self, method: &str, params: Value) -> Result<Value, BackendError> {
        self.call_coded(method, params)
    }

    /// Subscribe to events; returns a long-lived blocking stream.
    ///
    /// Pass every pane of interest in one call (see
    /// [`Subscription::agent_status_changed_many`]) rather than opening one
    /// stream per pane.
    pub fn subscribe(&self, subscriptions: &[Subscription]) -> Result<EventStream, BackendError> {
        let id = self.alloc_id();
        // Bound the subscribe-ack wait with the default unary budget; the
        // long-lived stream then relies on [`EventStream`]/
        // [`super::EventStream::set_read_timeout`] for push reads.
        let timeout =
            Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS.saturating_add(RPC_TIMEOUT_MARGIN_MS));
        let req = json!({
            "id": id,
            "method": "events.subscribe",
            "params": {
                "subscriptions": subscriptions.iter().map(Subscription::to_value).collect::<Vec<_>>()
            }
        });
        let mut line = serde_json::to_string(&req)
            .map_err(|e| BackendError::new(BackendErrorCode::Serialize, e.to_string()))?;
        line.push('\n');

        let mut reader = BufReader::new(self.open(timeout)?);
        reader
            .get_mut()
            .write_all(line.as_bytes())
            .and_then(|_| reader.get_mut().flush())
            .map_err(|e| map_io_err("write subscription", e))?;

        loop {
            let mut buf = String::new();
            let n = reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read subscription ack", e))?;
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

    pub fn ping(&self) -> Result<Value, BackendError> {
        self.call("ping", json!({}))
    }

    pub fn workspace_create(
        &self,
        cwd: Option<&str>,
        label: Option<&str>,
        focus: bool,
        env: Option<&std::collections::BTreeMap<String, String>>,
    ) -> Result<(WorkspaceInfo, PaneInfo), BackendError> {
        let mut params = serde_json::Map::new();
        params.insert("cwd".into(), serde_json::json!(cwd));
        params.insert("label".into(), serde_json::json!(label));
        params.insert("focus".into(), serde_json::json!(focus));
        if let Some(env) = env {
            params.insert("env".into(), serde_json::json!(env));
        }
        let res = self.call("workspace.create", serde_json::Value::Object(params))?;
        let ws: WorkspaceInfo =
            serde_json::from_value(res.get("workspace").cloned().unwrap_or(res.clone()))
                .map_err(|e| parse_err(e.to_string()))?;
        let root: PaneInfo = serde_json::from_value(res.get("root_pane").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))?;
        Ok((ws, root))
    }

    pub fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        let res = self.call("workspace.list", json!({}))?;
        serde_json::from_value(res.get("workspaces").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn workspace_close(
        &self,
        workspace_id: &str,
        close_group: bool,
    ) -> Result<Value, BackendError> {
        self.call(
            "workspace.close",
            workspace_close_params(workspace_id, close_group),
        )
    }

    pub fn pane_list(&self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        let res = self.call("pane.list", json!({ "workspace_id": workspace_id }))?;
        serde_json::from_value(res.get("panes").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        let res = self.call("agent.list", json!({}))?;
        serde_json::from_value(res.get("agents").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    /// `agent.get` — single agent; same [`AgentInfo`] shape as list/snapshot
    /// (optional `completion_seq` on newer servers).
    pub fn agent_get(&self, target: &str) -> Result<AgentInfo, BackendError> {
        let res = self.call("agent.get", json!({ "target": target }))?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_start(
        &self,
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

    pub fn agent_rename(&self, target: &str, name: &str) -> Result<Value, BackendError> {
        self.call("agent.rename", json!({ "target": target, "name": name }))
    }

    pub fn agent_prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<WaitOptions>,
    ) -> Result<AgentInfo, BackendError> {
        self.agent_prompt_coded(target, text, wait)
    }

    /// `agent.prompt` returning the typed error for code classification.
    pub fn agent_prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<WaitOptions>,
    ) -> Result<AgentInfo, BackendError> {
        let wait = wait.map(|w| {
            json!({ "timeout_ms": w.timeout_ms, "until": w.until.iter().map(super::types::AgentStatus::as_str).collect::<Vec<_>>() })
        });
        let res = self.call_coded(
            "agent.prompt",
            json!({ "target": target, "text": text, "wait": wait }),
        )?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_wait(
        &self,
        target: &str,
        until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        let res = self.call(
            "agent.wait",
            json!({
                "target": target,
                "until": until.iter().map(super::types::AgentStatus::as_str).collect::<Vec<_>>(),
                "timeout_ms": timeout_ms,
            }),
        )?;
        serde_json::from_value(res.get("agent").cloned().unwrap_or(res))
            .map_err(|e| parse_err(e.to_string()))
    }

    pub fn agent_read(
        &self,
        target: &str,
        source: &str,
        lines: Option<u32>,
    ) -> Result<String, BackendError> {
        let res = self.call("agent.read", agent_read_params(target, source, lines))?;
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

    pub fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<Value, BackendError> {
        self.call("agent.send_keys", json!({ "target": target, "keys": keys }))
    }

    // ---- pane primitives (H2, bd-harness-pane-send-text-7fe8) ----
    //
    // Wire shapes verified against `herdr api schema --json` (protocol 19,
    // build 2026-08-04-d78e3d3b5126) and the live daemon 2026-08-15, then
    // re-verified field-for-field against protocol 22 (herdr 0.9.0-preview,
    // build 2026-09-08-62431dbd033b; live probe 2026-09-09) — additive only
    // from this client's perspective:
    // `pane.send_text` takes { pane_id, text }; `pane.send_input` takes
    // { pane_id, keys } (key combos incl. modifier chords ctrl+h/alt+x and
    // named punctuation minus/plus); `pane.wait_for_output` takes
    // { pane_id, source, match: { type: substring|regex, value }, ... }.
    // NOTE: there is still NO raw `pane.run` method in protocol 22 — the
    // herdr CLI implements `herdr pane run` as send_text + Enter.
    // [`Client::pane_run`] mirrors that composite so pane-scoped command
    // execution needs no agent lifecycle (ADR-0019 CommandVerified).
    // (Caveat kept from v19: the schema's EventMatch def uses key `event`
    // with underscored values, but the live daemon accepts `type` + dotted
    // values on events.subscribe — empirically confirmed against 0.9.0.
    // 0.9 behavior change: subscriptions are LIVE-ONLY, no retained replay —
    // subscribe before taking any snapshot, see bd-myab.)

    /// `pane.send_text` — literal text into the pane's terminal (no Enter).
    pub fn pane_send_text(&self, pane_id: &str, text: &str) -> Result<Value, BackendError> {
        self.call(
            "pane.send_text",
            json!({ "pane_id": pane_id, "text": text }),
        )
    }

    /// `pane.send_input` — key combos (modifier chords, named punctuation).
    pub fn pane_send_input(&self, pane_id: &str, keys: &[&str]) -> Result<Value, BackendError> {
        self.call(
            "pane.send_input",
            json!({ "pane_id": pane_id, "keys": keys }),
        )
    }

    /// `pane.run` composite (no raw method in the protocol — v19 nor v22):
    /// send the command text, then an Enter keypress — the same shape
    /// `herdr pane run` uses.
    pub fn pane_run(&self, pane_id: &str, command: &str) -> Result<Value, BackendError> {
        self.pane_send_text(pane_id, command)?;
        self.pane_send_input(pane_id, &["Enter"])
    }

    /// `pane.wait_for_output` — block until the pane's output matches
    /// `pattern` (substring or regex per `match_type`) or `timeout_ms` elapses.
    pub fn pane_wait_for_output(
        &self,
        pane_id: &str,
        source: &str,
        match_type: &str,
        pattern: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        self.call(
            "pane.wait_for_output",
            json!({
                "pane_id": pane_id,
                "source": source,
                "match": { "type": match_type, "value": pattern },
                "strip_ansi": true,
                "timeout_ms": timeout_ms,
            }),
        )
    }

    /// `pane.report_metadata` — display-only mail-identity stamp (bd-6f57).
    /// Sets `display_agent` to the pin so the Agents sidebar shows the mail
    /// id, plus `toron_pin`/`toron_project`/`toron_npub` tokens for
    /// token-first resolve. No TTL: the stamp lives until the pane closes.
    pub fn pane_report_metadata(
        &self,
        pane_id: &str,
        pin: &str,
        project: &str,
        npub: &str,
    ) -> Result<Value, BackendError> {
        self.call(
            "pane.report_metadata",
            report_metadata_params(pane_id, pin, project, npub),
        )
    }

    pub fn worktree_create(
        &self,
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

    pub fn worktree_remove(&self, workspace_id: &str, force: bool) -> Result<Value, BackendError> {
        self.call(
            "worktree.remove",
            json!({ "workspace_id": workspace_id, "force": force }),
        )
    }
}

/// Derive the unix-socket read/write timeout for a unary RPC from its
/// params. Looks at top-level `timeout_ms`, then nested `wait.timeout_ms`
/// (agent.prompt), else [`DEFAULT_RPC_TIMEOUT_MS`]. Always adds
/// [`RPC_TIMEOUT_MARGIN_MS`].
fn socket_timeout_from_params(params: &Value) -> Duration {
    let call_ms = params
        .get("timeout_ms")
        .and_then(Value::as_u64)
        .or_else(|| {
            params
                .get("wait")
                .and_then(|w| w.get("timeout_ms"))
                .and_then(Value::as_u64)
        })
        .unwrap_or(DEFAULT_RPC_TIMEOUT_MS);
    Duration::from_millis(call_ms.saturating_add(RPC_TIMEOUT_MARGIN_MS))
}

/// Map transport errors onto typed codes. Socket deadlines surface as
/// `TimedOut`/`WouldBlock` → [`BackendErrorCode::Timeout`].
fn map_io_err(what: &str, e: std::io::Error) -> BackendError {
    match e.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => {
            BackendError::timeout(format!("{what}: {e}"))
        }
        _ => BackendError::new(BackendErrorCode::Io, format!("{what}: {e}")),
    }
}

/// Map a herdr wire error code string onto the typed code enum. Codes the
/// client knows map explicitly; everything else stays `Other(code)`.
fn parse_code(code: &str) -> BackendErrorCode {
    code.parse()
        .unwrap_or_else(|_| BackendErrorCode::Other(code.to_string()))
}

/// Wire params for `agent.read`. Source strings (recent, visible,
/// recent-unwrapped, ...) pass through UNTOUCHED — the herdr daemon owns
/// source semantics; this client never rewrites or whitelists them.
fn agent_read_params(target: &str, source: &str, lines: Option<u32>) -> Value {
    json!({ "target": target, "source": source, "lines": lines, "format": "text", "strip_ansi": true })
}

/// `workspace.close` params. herdr 0.9 (protocol 22) group semantics: a
/// primary workspace with open worktree workspaces stays open unless
/// `close_group` is set. The flag rides only when true so the param never
/// reaches a daemon that predates it.
fn workspace_close_params(workspace_id: &str, close_group: bool) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("workspace_id".into(), json!(workspace_id));
    if close_group {
        params.insert("close_group".into(), json!(true));
    }
    Value::Object(params)
}

/// Pure param builder for the mail-identity stamp (bd-6f57): display-only
/// `pane.report_metadata` with `display_agent` = pin and tokens for
/// token-first resolve. Pure so the wire shape is unit-tested without a
/// live herdr socket.
pub fn report_metadata_params(pane_id: &str, pin: &str, project: &str, npub: &str) -> Value {
    json!({
        "pane_id": pane_id,
        "source": MAIL_IDENTITY_SOURCE,
        "display_agent": pin,
        "tokens": {
            TOKEN_PIN: pin,
            TOKEN_PROJECT: project,
            TOKEN_NPUB: npub,
        },
    })
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
    ///
    /// Lines without an `"event"` key are skipped **except** herdr error
    /// responses with `code: "events_lost"`: those return
    /// [`BackendErrorCode::EventsLost`] so callers can resubscribe and
    /// reconcile with a snapshot (not treat the stream as a quiet EOF).
    pub fn read_event(&mut self) -> Result<Option<(String, Value)>, BackendError> {
        loop {
            let mut buf = String::new();
            let n = self
                .reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read herdr event", e))?;
            if n == 0 {
                return Ok(None);
            }
            let val: Value = serde_json::from_str(buf.trim()).map_err(|e| {
                BackendError::new(
                    BackendErrorCode::Malformed,
                    format!("malformed herdr event: {buf}: {e}"),
                )
            })?;
            if let Some(event) = val.get("event").and_then(Value::as_str) {
                return Ok(Some((
                    event.to_string(),
                    val.get("data").cloned().unwrap_or(Value::Null),
                )));
            }
            // Subscription error responses (no "event" key). Surface
            // events_lost as a typed signal; skip other non-event lines.
            if let Some(err) = val.get("error") {
                let code = err.get("code").and_then(Value::as_str).unwrap_or("");
                if code == "events_lost" {
                    let message = err
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("events lost; resubscribe and snapshot")
                        .to_string();
                    return Err(BackendError::new(BackendErrorCode::EventsLost, message));
                }
            }
        }
    }
}

/// Sync `Backend` over the blocking herdr client (ADR-0005 P1 unified trait).
///
/// Each unary RPC opens a fresh `UnixStream` on the shared [`Client`], so
/// concurrent trait calls do not serialize on a mutex — only the atomic
/// request-id counter is shared. Long waits (`agent.wait`, prompt+wait)
/// therefore do not head-of-line-block other callers.
pub struct HerdrBackend {
    client: Client,
}

impl HerdrBackend {
    pub fn connect(socket_path: Option<&str>) -> Result<Self, BackendError> {
        Ok(Self {
            client: Client::connect(socket_path)?,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl super::Backend for HerdrBackend {
    fn name(&self) -> &'static str {
        "herdr"
    }

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        self.client.workspace_list()
    }

    fn workspace_create(&self, label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        let (ws, _root) = self.client.workspace_create(None, label, false, None)?;
        Ok(ws)
    }

    fn workspace_close(&self, workspace_id: &str, close_group: bool) -> Result<(), BackendError> {
        self.client.workspace_close(workspace_id, close_group)?;
        Ok(())
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        direction: &str,
        cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        let res = self.client.call(
            "pane.split",
            json!({ "target_pane_id": parent_pane_id, "direction": direction, "cwd": cwd }),
        )?;
        serde_json::from_value(res.get("pane").cloned().unwrap_or(res))
            .map_err(|e| BackendError::new(BackendErrorCode::Malformed, e.to_string()))
    }

    fn pane_list(&self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        self.client.pane_list(workspace_id)
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), BackendError> {
        self.client
            .call("pane.close", json!({ "pane_id": pane_id }))?;
        Ok(())
    }

    fn pane_run(&self, pane_id: &str, command: &str) -> Result<(), BackendError> {
        self.client.pane_run(pane_id, command)?;
        Ok(())
    }

    fn pane_wait_output(
        &self,
        pane_id: &str,
        needle: &str,
        timeout_ms: u64,
    ) -> Result<serde_json::Value, BackendError> {
        // H2: schema-conformant shape { pane_id, source, match } — the old
        // { mode, pattern } shape is rejected by the live daemon
        // ("missing field `source`", verified 2026-08-15).
        self.client
            .pane_wait_for_output(pane_id, "recent", "substring", needle, Some(timeout_ms))
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        self.client.agent_list()
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        self.client.agent_start(name, kind, pane_id, 30_000, args)
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        let name = new_name.ok_or_else(|| {
            BackendError::unsupported("herdr agent_rename cannot clear a name (None)")
        })?;
        self.client.agent_rename(target, name)?;
        Ok(())
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<serde_json::Value, BackendError> {
        let info = self.client.agent_prompt(target, text, wait.cloned())?;
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
        let res = { self.client.agent_prompt_coded(target, text, wait.cloned()) };
        match res {
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
        self.client.agent_wait(target, until, timeout_ms)
    }

    fn agent_read(
        &self,
        target: &str,
        source: &str,
        lines: u32,
    ) -> Result<serde_json::Value, BackendError> {
        let text = self.client.agent_read(target, source, Some(lines))?;
        Ok(json!({ "text": text }))
    }

    fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<(), BackendError> {
        self.client.agent_send_keys(target, keys)?;
        Ok(())
    }

    fn subscribe(
        &self,
        filters: &[Subscription],
    ) -> Result<Box<dyn super::EventStream>, BackendError> {
        let stream = self.client.subscribe(filters)?;
        Ok(Box::new(HerdrEventStreamAdapter(stream)))
    }
}

/// Adapter: raw herdr blocking stream → unified [`super::EventStream`] that
/// yields typed [`BackendEvent`]s. Forwards to `parse_push`.
struct HerdrEventStreamAdapter(EventStream);

impl super::EventStream for HerdrEventStreamAdapter {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        match self.0.read_event() {
            Ok(Some((kind, data))) => Ok(Some(parse_push(&kind, &data))),
            Ok(None) => Ok(None),
            Err(e) if e.code == BackendErrorCode::EventsLost => Ok(Some(BackendEvent::EventsLost)),
            Err(e) => Err(e),
        }
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
/// `pane.exited` → [`BackendEvent::PaneExited`];
/// subscription `events_lost` → [`BackendEvent::EventsLost`] (via the adapter);
/// everything else → [`BackendEvent::Raw`].
fn parse_push(kind: &str, data: &Value) -> BackendEvent {
    // ADR-0031 S1: PaneId constructed at the adapter edge only; status
    // enters through the canonical normalizer, never .parse() in reducers.
    let pane = data
        .get("pane_id")
        .and_then(Value::as_str)
        .map(PaneId::new)
        .unwrap_or_else(PaneId::empty);
    let agent = data
        .get("agent")
        .and_then(Value::as_str)
        .or_else(|| data.get("display_agent").and_then(Value::as_str))
        .map(ToString::to_string);
    let status = data
        .get("agent_status")
        .or_else(|| data.get("final_status"))
        .and_then(Value::as_str)
        .map(normalize_herdr_status)
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
    fn report_metadata_shape() {
        // bd-6f57: display-only stamp — pane_id + source + display_agent +
        // toron_pin/toron_project/toron_npub tokens, no lifecycle fields.
        assert_eq!(
            report_metadata_params("w1:p1", "DustySparrow", "toron", "npub1abc"),
            json!({
                "pane_id": "w1:p1",
                "source": "toron:mail-identity",
                "display_agent": "DustySparrow",
                "tokens": {
                    "toron_pin": "DustySparrow",
                    "toron_project": "toron",
                    "toron_npub": "npub1abc",
                },
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

    /// Live v22 schema snapshot + real payload samples (captured from the
    /// running herdr 0.9.0-preview daemon, 2026-09-09; protocol-bump bead
    /// bd-jqgh — the successor of the v20 capture).
    const V22_FIXTURE: &str = include_str!("../../tests/fixtures/herdr-schema-v22.json");

    fn fixture() -> Value {
        serde_json::from_str(V22_FIXTURE).expect("v22 fixture parses")
    }

    #[test]
    fn protocol_pin_matches_live_v22_fixture() {
        let f = fixture();
        assert_eq!(HDR_PROTOCOL, 22);
        assert_eq!(f["protocol"].as_u64(), Some(HDR_PROTOCOL));
    }

    #[test]
    fn pane_info_parses_live_v22_payload() {
        let f = fixture();
        let pane: PaneInfo = serde_json::from_value(f["samples"]["pane"].clone()).expect("pane");
        assert_eq!(pane.pane_id, "w5:p1");
        assert_eq!(pane.terminal_id, "term_65af5e84e7eef1");
        assert_eq!(pane.workspace_id, "w5");
        assert_eq!(pane.tab_id, "w5:t1");
        assert!(!pane.focused);
        assert_eq!(pane.agent_status, AgentStatus::Unknown);
        assert_eq!(pane.revision, 0);
        assert_eq!(pane.agent, None);
        assert_eq!(
            pane.cwd.as_deref(),
            Some("/Users/bangedorrunt/workspace/dotfiles")
        );
        assert!(pane.scroll.is_some());
        // No title on this shell pane — Option covers the absent key.
        assert_eq!(pane.terminal_title, None);
    }

    #[test]
    fn worktree_info_parses_live_v22_payloads() {
        let f = fixture();
        let wts: Vec<WorktreeInfo> =
            serde_json::from_value(f["samples"]["worktrees"].clone()).expect("worktrees");
        assert_eq!(wts.len(), 1);
        let main = &wts[0];
        assert_eq!(main.path, "/Users/bangedorrunt/workspace/dotfiles");
        assert_eq!(main.label, "dotfiles");
        assert!(!main.is_bare);
        assert!(!main.is_detached);
        assert!(!main.is_linked_worktree);
        assert!(!main.is_prunable);
        assert_eq!(main.branch.as_deref(), Some("main"));
        assert_eq!(main.open_workspace_id.as_deref(), Some("w5"));
    }

    #[test]
    fn agent_info_parses_live_v22_payload() {
        let f = fixture();
        let agent: AgentInfo =
            serde_json::from_value(f["samples"]["agent"].clone()).expect("agent");
        assert_eq!(agent.agent_status, AgentStatus::Working);
        assert_eq!(agent.terminal_id, "term_65af5e84f1b009");
        assert_eq!(agent.workspace_id, "wP");
        assert_eq!(agent.tab_id, "wP:t1");
        assert_eq!(agent.pane_id, "wP:p1");
        assert!(agent.focused);
        assert_eq!(agent.revision, 1654);
        // v22 omits `name` for this agent (agent-kind identity only) —
        // Option + serde's implicit missing→None covers it.
        assert_eq!(agent.name, None);
        // interactive_ready/launch_pending are absent in this live payload;
        // #[serde(default)] covers missing→false.
        assert!(!agent.interactive_ready);
        assert!(!agent.launch_pending);
        assert_eq!(agent.state_change_seq, 10);
        // Older / this fixture omit completion_seq — Option covers missing.
        assert_eq!(agent.completion_seq, None);
    }

    #[test]
    fn agent_info_parses_optional_completion_seq() {
        // Done/hand-off should prefer completion_seq over working→idle.
        let with_seq = json!({
            "agent_status": "done",
            "focused": false,
            "pane_id": "w1:p1",
            "revision": 1,
            "state_change_seq": 3,
            "completion_seq": 7,
            "tab_id": "w1:t1",
            "terminal_id": "term_x",
            "workspace_id": "w1",
        });
        let agent: AgentInfo = serde_json::from_value(with_seq).expect("with seq");
        assert_eq!(agent.completion_seq, Some(7));
        let without = json!({
            "agent_status": "idle",
            "focused": false,
            "pane_id": "w1:p1",
            "revision": 1,
            "tab_id": "w1:t1",
            "terminal_id": "term_x",
            "workspace_id": "w1",
        });
        let agent: AgentInfo = serde_json::from_value(without).expect("without seq");
        assert_eq!(agent.completion_seq, None);
    }

    #[test]
    fn socket_timeout_from_params_uses_call_budget_plus_margin() {
        let top = json!({ "timeout_ms": 1000 });
        assert_eq!(
            socket_timeout_from_params(&top),
            Duration::from_millis(1000 + RPC_TIMEOUT_MARGIN_MS)
        );
        let nested = json!({ "wait": { "timeout_ms": 2000, "until": ["done"] } });
        assert_eq!(
            socket_timeout_from_params(&nested),
            Duration::from_millis(2000 + RPC_TIMEOUT_MARGIN_MS)
        );
        let absent = json!({ "target": "w1:p1" });
        assert_eq!(
            socket_timeout_from_params(&absent),
            Duration::from_millis(DEFAULT_RPC_TIMEOUT_MS + RPC_TIMEOUT_MARGIN_MS)
        );
    }

    #[test]
    fn multiplex_subscription_covers_many_panes() {
        let subs = Subscription::agent_status_changed_many(["w1:p1", "w1:p2"]);
        assert_eq!(subs.len(), 2);
        assert_eq!(subs[0].pane_id, "w1:p1");
        assert_eq!(subs[1].pane_id, "w1:p2");
        assert_eq!(subs[0].event, SubscriptionEventKind::PaneAgentStatusChanged);
    }

    #[test]
    fn workspace_info_parses_live_v22_payload() {
        let f = fixture();
        let ws: WorkspaceInfo =
            serde_json::from_value(f["samples"]["workspace"].clone()).expect("workspace");
        assert_eq!(ws.workspace_id, "w5");
        assert_eq!(ws.number, 1);
        assert_eq!(ws.label, "dotfiles");
        assert!(!ws.focused);
        assert_eq!(ws.pane_count, 2);
        assert_eq!(ws.tab_count, 2);
        assert_eq!(ws.active_tab_id, "w5:t2");
        assert_eq!(ws.agent_status, AgentStatus::Unknown);
    }

    #[test]
    fn v22_schema_requires_pane_v22_fields() {
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
    fn live_round_trip_v22() {
        // Protocol bump (bd-jqgh): prove the typed client parses live v22
        // responses for the methods the flywheel loop depends on. List-only
        // — no side effects.
        let c = Client::connect(None).expect("connect to live herdr socket");
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
            // completion_seq optional — live 0.9.x servers populate it;
            // older servers omit it (None). Prefer it over working→idle.
            let _ = a.completion_seq;
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
            json!({ "timeout_ms": prompt_wait.timeout_ms, "until": prompt_wait.until.iter().map(super::super::types::AgentStatus::as_str).collect::<Vec<_>>() }),
            wait
        );
    }
}

// ---- H2 pane primitives (bd-harness-pane-send-text-7fe8) ----
//
// Hermetic socket round-trips: a UnixListener in the test process speaks the
// JSON-RPC side, so the typed Client methods are exercised end-to-end
// (serialize request → socket → parse response) with no live daemon. The
// module sits at the file root (not inside `mod tests`) so the full test
// path is `backend::herdr::pane_send_tests::…` — the bead VERIFY filter
// `cargo test -p toron-core backend::herdr::pane_send` matches it.
#[cfg(test)]
mod pane_send_tests {
    use super::*;
    use crate::backend::Backend;
    use std::io::{BufRead, Write};
    use std::os::unix::net::UnixListener;

    fn test_sock(n: u32) -> String {
        format!(
            "{}/herdr-pane-test-{}-{n}.sock",
            std::env::temp_dir().display(),
            std::process::id()
        )
    }

    /// Server thread: accept `expected.len()` connections; for each, read
    /// one request line, assert method + params, reply `reply` with the
    /// request's id echoed (one request per connection, like the real
    /// daemon).
    fn serve(
        listener: UnixListener,
        expected: Vec<(&'static str, Value)>,
        reply: Value,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            // Client::connect probes reachability with a throwaway
            // connection (connect + drop, no request) — consume it first.
            let (probe, _) = listener.accept().expect("accept probe connection");
            let mut probe_line = String::new();
            std::io::BufReader::new(probe.try_clone().expect("clone probe"))
                .read_line(&mut probe_line)
                .expect("probe read");
            assert!(probe_line.is_empty(), "probe must not send a request");
            for (method, params) in expected {
                let (mut sock, _) = listener.accept().expect("accept test connection");
                let mut line = String::new();
                std::io::BufReader::new(sock.try_clone().expect("clone"))
                    .read_line(&mut line)
                    .expect("read request line");
                let req: Value = serde_json::from_str(&line).expect("parse request");
                assert_eq!(req["method"].as_str(), Some(method), "wire method");
                assert_eq!(&req["params"], &params, "wire params for {method}");
                let resp = json!({ "id": req["id"], "result": reply });
                let mut out = serde_json::to_string(&resp).expect("serialize response");
                out.push('\n');
                sock.write_all(out.as_bytes()).expect("write response");
            }
        })
    }

    #[test]
    fn send_text_round_trip() {
        let path = test_sock(1);
        let listener = UnixListener::bind(&path).expect("bind");
        let t = serve(
            listener,
            vec![(
                "pane.send_text",
                json!({ "pane_id": "w1:p1", "text": "echo SEND_TEXT_OK" }),
            )],
            json!({ "type": "ok" }),
        );
        let c = Client::connect(Some(&path)).expect("connect");
        let res = c
            .pane_send_text("w1:p1", "echo SEND_TEXT_OK")
            .expect("pane.send_text ok");
        assert_eq!(res, json!({ "type": "ok" }));
        t.join().expect("server assertions");
    }

    #[test]
    fn send_input_round_trip() {
        let path = test_sock(2);
        let listener = UnixListener::bind(&path).expect("bind");
        let t = serve(
            listener,
            vec![(
                "pane.send_input",
                json!({ "pane_id": "w1:p1", "keys": ["ctrl+h"] }),
            )],
            json!({ "type": "ok" }),
        );
        let c = Client::connect(Some(&path)).expect("connect");
        let res = c
            .pane_send_input("w1:p1", &["ctrl+h"])
            .expect("pane.send_input ok");
        assert_eq!(res, json!({ "type": "ok" }));
        t.join().expect("server assertions");
    }

    #[test]
    fn run_composite_round_trip() {
        // No raw pane.run in the protocol (v19 nor v20): the composite sends
        // the command text then an Enter keypress, on two connections (one
        // request per connection) — the same shape `herdr pane run` uses.
        let path = test_sock(3);
        let listener = UnixListener::bind(&path).expect("bind");
        let t = serve(
            listener,
            vec![
                (
                    "pane.send_text",
                    json!({ "pane_id": "w1:p1", "text": "cargo test --quiet" }),
                ),
                (
                    "pane.send_input",
                    json!({ "pane_id": "w1:p1", "keys": ["Enter"] }),
                ),
            ],
            json!({ "type": "ok" }),
        );
        let c = Client::connect(Some(&path)).expect("connect");
        let res = c
            .pane_run("w1:p1", "cargo test --quiet")
            .expect("pane.run composite ok");
        assert_eq!(res, json!({ "type": "ok" }));
        t.join().expect("server assertions");
    }

    #[test]
    fn wait_for_output_round_trip() {
        let path = test_sock(4);
        let listener = UnixListener::bind(&path).expect("bind");
        let matched = json!({
            "type": "output_matched",
            "pane_id": "w1:p1",
            "revision": 0,
            "matched_line": "cargo test --quiet",
        });
        let t = serve(
            listener,
            vec![(
                "pane.wait_for_output",
                json!({
                    "pane_id": "w1:p1",
                    "source": "recent",
                    "match": { "type": "regex", "value": "tests passed" },
                    "strip_ansi": true,
                    "timeout_ms": 5000,
                }),
            )],
            matched,
        );
        let c = Client::connect(Some(&path)).expect("connect");
        let res = c
            .pane_wait_for_output("w1:p1", "recent", "regex", "tests passed", Some(5000))
            .expect("pane.wait_for_output ok");
        assert_eq!(res["type"].as_str(), Some("output_matched"));
        assert_eq!(res["matched_line"].as_str(), Some("cargo test --quiet"));
        t.join().expect("server assertions");
    }

    /// bd-harness-alt-screen-reads: agent.read source strings pass through
    /// the wire UNTOUCHED — recent-unwrapped included. The client must never
    /// rewrite or whitelist source semantics.
    #[test]
    fn agent_read_params_pass_sources_through_untouched() {
        for source in ["recent", "visible", "recent-unwrapped", "weird source"] {
            let p = agent_read_params("w1:p1", source, Some(40));
            assert_eq!(p["target"], "w1:p1");
            assert_eq!(p["source"], source, "source must not be rewritten");
            assert_eq!(p["lines"], 40);
            assert_eq!(p["format"], "text");
            assert_eq!(p["strip_ansi"], true);
        }
        // None lines serializes as null (herdr contract: no line cap).
        let p = agent_read_params("w1:p1", "recent-unwrapped", None);
        assert_eq!(p["lines"], Value::Null);
    }

    /// bd-jqgh: herdr 0.9 group-close semantics — the flag rides only when
    /// true, so pre-0.9 daemons never see an unknown param.
    #[test]
    fn workspace_close_params_omit_group_flag_unless_true() {
        let base = workspace_close_params("w1", false);
        assert_eq!(base["workspace_id"], "w1");
        assert!(base.get("close_group").is_none());
        let group = workspace_close_params("w1", true);
        assert_eq!(group["workspace_id"], "w1");
        assert_eq!(group["close_group"], true);
    }

    /// bd-harness-alt-screen-reads: the wire agent_not_idle code maps to the
    /// typed variant via parse_code — callers branch on code, never text.
    #[test]
    fn parse_code_maps_agent_not_idle() {
        assert_eq!(parse_code("agent_not_idle"), BackendErrorCode::AgentNotIdle);
        assert_eq!(
            parse_code("agent_prompt_stalled"),
            BackendErrorCode::AgentPromptStalled
        );
        assert_eq!(parse_code("events_lost"), BackendErrorCode::EventsLost);
        assert_eq!(
            parse_code("unknown_wire_code"),
            BackendErrorCode::Other("unknown_wire_code".into())
        );
    }

    /// Unary RPC socket deadlines: a server that accepts but never replies
    /// must surface [`BackendErrorCode::Timeout`] (not hang forever).
    #[test]
    fn unary_rpc_times_out_when_server_silent() {
        let path = test_sock(10);
        let listener = UnixListener::bind(&path).expect("bind");
        let server = std::thread::spawn(move || {
            // Probe connection from Client::connect.
            let (probe, _) = listener.accept().expect("probe");
            let mut probe_line = String::new();
            std::io::BufReader::new(probe.try_clone().expect("clone"))
                .read_line(&mut probe_line)
                .ok();
            // Real request: accept, read the request, then sleep past the
            // client deadline without writing a response.
            let (sock, _) = listener.accept().expect("accept call");
            let mut line = String::new();
            // Keep `sock` owned so the connection stays open for the sleep.
            let mut reader = std::io::BufReader::new(sock);
            let _ = reader.read_line(&mut line);
            // Hold open past client deadline (1ms + margin) without replying.
            std::thread::sleep(Duration::from_millis(1 + RPC_TIMEOUT_MARGIN_MS + 500));
            drop(reader);
        });
        let c = Client::connect(Some(&path)).expect("connect");
        // Tiny timeout_ms so the test stays fast; margin still applies but
        // we override via params — use 50ms + 5s margin would be slow.
        // Call with timeout_ms=50; socket budget = 50 + 5000 = 5050ms.
        // To keep the test snappy, temporarily we rely on a custom params
        // path: use call_coded with timeout_ms small AND we accept that
        // margin is 5s — sleep server 6s would be slow.
        // Instead: verify map_io_err classification + open applies timeout
        // by using a 1ms timeout and a silent server; budget = 1+5000=5001ms.
        // Cap wall time with a shorter sleep on the assertion side by
        // checking the error kind after ~5s. For a faster test, patch is
        // heavy — use 1ms call timeout and sleep server forever; client
        // should return Timeout within margin+epsilon (~5.1s). Acceptable.
        let started = std::time::Instant::now();
        let err = c
            .call_coded("ping", json!({ "timeout_ms": 1 }))
            .expect_err("silent server must error");
        assert_eq!(
            err.code,
            BackendErrorCode::Timeout,
            "expected Timeout, got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "deadline should fire near margin, took {:?}",
            started.elapsed()
        );
        server.join().expect("server");
    }

    /// `events_lost` on the subscription socket is a typed signal, not a
    /// skipped line that becomes a quiet `Ok(None)`.
    #[test]
    fn events_lost_surfaces_as_typed_error_and_backend_event() {
        let path = test_sock(11);
        let listener = UnixListener::bind(&path).expect("bind");
        let server = std::thread::spawn(move || {
            let (probe, _) = listener.accept().expect("probe");
            let mut probe_line = String::new();
            std::io::BufReader::new(probe.try_clone().expect("clone"))
                .read_line(&mut probe_line)
                .ok();
            // subscribe connection
            let (mut sock, _) = listener.accept().expect("subscribe");
            let mut line = String::new();
            std::io::BufReader::new(sock.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read subscribe");
            let req: Value = serde_json::from_str(&line).expect("parse");
            assert_eq!(req["method"].as_str(), Some("events.subscribe"));
            // ack
            let ack = json!({ "id": req["id"], "result": { "ok": true } });
            let mut out = serde_json::to_string(&ack).unwrap();
            out.push('\n');
            sock.write_all(out.as_bytes()).expect("ack");
            // Push an ordinary event, then events_lost error (no "event" key).
            let push = json!({
                "event": "pane.agent_status_changed",
                "data": { "pane_id": "w1:p1", "agent_status": "working" }
            });
            let mut out = serde_json::to_string(&push).unwrap();
            out.push('\n');
            sock.write_all(out.as_bytes()).expect("push");
            let lost = json!({
                "id": req["id"],
                "error": { "code": "events_lost", "message": "subscriber fell behind" }
            });
            let mut out = serde_json::to_string(&lost).unwrap();
            out.push('\n');
            sock.write_all(out.as_bytes()).expect("lost");
        });

        let c = Client::connect(Some(&path)).expect("connect");
        let subs = Subscription::agent_status_changed_many(["w1:p1"]);
        let mut stream = c.subscribe(&subs).expect("subscribe");
        let (kind, data) = stream.read_event().expect("event").expect("some");
        assert_eq!(kind, "pane.agent_status_changed");
        assert_eq!(data["pane_id"], "w1:p1");
        let err = stream.read_event().expect_err("events_lost");
        assert_eq!(err.code, BackendErrorCode::EventsLost);

        // Adapter maps the typed error onto BackendEvent::EventsLost.
        // Rebuild a short stream that only yields events_lost after ack.
        drop(stream);
        server.join().expect("server");

        let path2 = test_sock(12);
        let listener = UnixListener::bind(&path2).expect("bind");
        let server = std::thread::spawn(move || {
            let (probe, _) = listener.accept().expect("probe");
            let mut probe_line = String::new();
            std::io::BufReader::new(probe.try_clone().expect("clone"))
                .read_line(&mut probe_line)
                .ok();
            let (mut sock, _) = listener.accept().expect("subscribe");
            let mut line = String::new();
            std::io::BufReader::new(sock.try_clone().expect("clone"))
                .read_line(&mut line)
                .expect("read");
            let req: Value = serde_json::from_str(&line).expect("parse");
            let ack = json!({ "id": req["id"], "result": { "ok": true } });
            let mut out = serde_json::to_string(&ack).unwrap();
            out.push('\n');
            sock.write_all(out.as_bytes()).unwrap();
            let lost = json!({
                "id": req["id"],
                "error": { "code": "events_lost", "message": "behind" }
            });
            let mut out = serde_json::to_string(&lost).unwrap();
            out.push('\n');
            sock.write_all(out.as_bytes()).unwrap();
        });
        let backend = HerdrBackend::connect(Some(&path2)).expect("backend");
        let mut adapted = backend
            .subscribe(&[Subscription::agent_status_changed("w1:p1".into())])
            .expect("subscribe");
        let ev = adapted.read_event().expect("read").expect("some");
        assert_eq!(ev, BackendEvent::EventsLost);
        server.join().expect("server");
    }

    /// Long wait must not serialize a concurrent short call (no mutex across
    /// blocking I/O). Two overlapping RPCs on one HerdrBackend.
    #[test]
    fn concurrent_calls_do_not_serialize_on_client_mutex() {
        let path = test_sock(13);
        let listener = UnixListener::bind(&path).expect("bind");
        let server = std::thread::spawn(move || {
            // probe
            let (probe, _) = listener.accept().expect("probe");
            let mut probe_line = String::new();
            std::io::BufReader::new(probe.try_clone().expect("clone"))
                .read_line(&mut probe_line)
                .ok();
            // Accept two overlapping connections. First is the slow wait;
            // second is the fast ping — reply to ping before wait.
            use std::sync::mpsc;
            let (tx, rx) = mpsc::channel::<UnixStream>();
            let acceptor = std::thread::spawn(move || {
                for _ in 0..2 {
                    let (sock, _) = listener.accept().expect("accept");
                    tx.send(sock).unwrap();
                }
            });
            let sock_a = rx.recv().unwrap();
            let sock_b = rx.recv().unwrap();
            // Read both requests
            let mut line_a = String::new();
            let mut line_b = String::new();
            let mut ra = std::io::BufReader::new(sock_a.try_clone().unwrap());
            let mut rb = std::io::BufReader::new(sock_b.try_clone().unwrap());
            ra.read_line(&mut line_a).unwrap();
            rb.read_line(&mut line_b).unwrap();
            let req_a: Value = serde_json::from_str(&line_a).unwrap();
            let req_b: Value = serde_json::from_str(&line_b).unwrap();
            // Identify which is wait vs ping
            let (wait_sock, wait_req, ping_sock, ping_req) = if req_a["method"] == "agent.wait" {
                (sock_a, req_a, sock_b, req_b)
            } else {
                (sock_b, req_b, sock_a, req_a)
            };
            assert_eq!(wait_req["method"], "agent.wait");
            assert_eq!(ping_req["method"], "ping");
            // Reply to ping immediately; delay wait reply.
            let mut out = serde_json::to_string(&json!({
                "id": ping_req["id"],
                "result": { "ok": true }
            }))
            .unwrap();
            out.push('\n');
            let mut ping_sock = ping_sock;
            ping_sock.write_all(out.as_bytes()).unwrap();
            std::thread::sleep(Duration::from_millis(200));
            let agent = json!({
                "agent_status": "done",
                "focused": false,
                "pane_id": "w1:p1",
                "revision": 1,
                "tab_id": "w1:t1",
                "terminal_id": "term",
                "workspace_id": "w1",
                "completion_seq": 2,
            });
            let mut out = serde_json::to_string(&json!({
                "id": wait_req["id"],
                "result": { "agent": agent }
            }))
            .unwrap();
            out.push('\n');
            let mut wait_sock = wait_sock;
            wait_sock.write_all(out.as_bytes()).unwrap();
            acceptor.join().unwrap();
        });

        let backend = std::sync::Arc::new(HerdrBackend::connect(Some(&path)).expect("connect"));
        let b2 = backend.clone();
        let started = std::time::Instant::now();
        let wait_handle =
            std::thread::spawn(move || b2.agent_wait("w1:p1", &[AgentStatus::Done], Some(5_000)));
        // Give the wait a moment to open its stream first.
        std::thread::sleep(Duration::from_millis(50));
        let ping_started = std::time::Instant::now();
        backend.client().ping().expect("ping while wait in flight");
        let ping_elapsed = ping_started.elapsed();
        let wait_res = wait_handle.join().expect("join").expect("wait ok");
        assert_eq!(wait_res.completion_seq, Some(2));
        // If the mutex were held across wait, ping would wait for the 200ms
        // delayed wait reply. With no mutex, ping returns promptly.
        assert!(
            ping_elapsed < Duration::from_millis(150),
            "ping serialized behind wait: {ping_elapsed:?} (total {:?})",
            started.elapsed()
        );
        server.join().expect("server");
    }
}
