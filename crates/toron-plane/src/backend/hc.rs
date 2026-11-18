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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};

use crate::osc7501::{Board, FoldPolicy, HcStatusView, fold};

use super::error::{BackendError, BackendErrorCode};
use super::types::{BackendEvent, PaneId, PromptLanding, Subscription, WaitOptions, WorkspaceInfo};
use super::{Backend, EventStream};
use toron_core::pane_types::{AgentInfo, AgentStatus, PaneInfo, TOKEN_PIN, normalize_agent_status};

/// Stable prefix on every pane/agent id this backend returns.
pub const ID_PREFIX: &str = "hc:";

/// Protocol generation this client speaks (hc OpenRPC `hello.generation`).
pub const HC_GENERATION: u32 = 1;

/// Minimum hc patch version this client is tested against. Connect fails
/// (typed `hc_too_old`) when the server reports an older version, so an
/// upgrade gap surfaces at connect time instead of as `Malformed` later.
/// Bump together with the tested-max note below.
pub const MIN_HC_VERSION: &str = "0.1.16";
/// Highest hc version exercised by the test suite. Newer servers still
/// connect (additive-only), but emit a one-time stderr note so drift is
/// visible without bricking production.
pub const TESTED_HC_VERSION: &str = "0.1.18";

/// Default unary RPC budget when params omit a timeout.
pub const DEFAULT_RPC_TIMEOUT_MS: u64 = 30_000;
/// hc's documented default wait (config `wait_timeout`, else 180 s).
const DEFAULT_WAIT_MS: u64 = 180_000;
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
    "terminal.label",
    "events.subscribe",
    "rpc.discover",
];

/// POSIX shell-quote one word: bare when every byte is shell-safe,
/// single-quoted otherwise. Keeps a typed launch line one line.
fn shell_quote(word: &str) -> String {
    let safe = !word.is_empty()
        && word.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'-' | b'.' | b'/' | b',' | b':' | b'=' | b'+' | b'@'
                )
        });
    if safe {
        return word.to_string();
    }
    format!("'{}'", word.replace('\'', "'\\''"))
}

/// Join words into one shell line, quoting only what needs it.
fn shell_join(words: &[String]) -> String {
    words
        .iter()
        .map(|w| shell_quote(w))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Env forwarded when this process creates a shell terminal: connection
/// facts (which hc to talk to), never identity. `AGENT_NAME` is
/// deliberately NOT inherited — the pane is for a new agent, and stamping
/// the creator's pin onto it would mislabel every later lookup.
fn spawn_env() -> Option<BTreeMap<String, String>> {
    let mut env = BTreeMap::new();
    for key in ["HC_SOCKET_PATH", "HC_SESSION"] {
        if let Ok(v) = std::env::var(key)
            && !v.is_empty()
        {
            env.insert(key.to_string(), v);
        }
    }
    (!env.is_empty()).then_some(env)
}

/// Compare two `X.Y.Z` (or `X.Y`) version strings. Returns true when
/// `have` >= `want`. Non-numeric suffixes are ignored (e.g. "0.1.18-rc1"
/// compares as 0.1.18).
pub fn version_at_least(have: &str, want: &str) -> bool {
    let parse = |s: &str| -> (u64, u64, u64) {
        let core = s.split('-').next().unwrap_or(s);
        let mut parts = core.split('.');
        let a = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let b = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let c = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        (a, b, c)
    };
    parse(have) >= parse(want)
}

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

    let want_addr = strip_id_prefix(q);
    let id_query =
        q.starts_with(ID_PREFIX) || named.iter().any(|(_, addr)| strip_id_prefix(addr) == q);
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

/// Walk `address` to its root ancestor through the parent map (flywheel
/// maps a workspace to a root shell terminal and filters agents by
/// `workspace_id`). Unknown or missing parents stop the walk — the
/// terminal is its own root. Bounded by map size, so cycles break.
fn root_address(
    parents: &BTreeMap<String, String>,
    known: &BTreeSet<String>,
    address: &str,
) -> String {
    let mut addr = address.to_string();
    for _ in 0..known.len().saturating_add(1) {
        match parents.get(&addr) {
            Some(p) if known.contains(p) => addr = p.clone(),
            _ => break,
        }
    }
    addr
}

/// Parent map for one `terminal.list` snapshot: bare address -> bare
/// parent address.
fn parent_map(terminals: &[Terminal]) -> BTreeMap<String, String> {
    terminals
        .iter()
        .filter_map(|t| {
            t.parent
                .as_deref()
                .map(|p| (t.address.clone(), strip_id_prefix(p).to_string()))
        })
        .collect()
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

/// Per-terminal mail identity for one snapshot: a unique `toron_pin` label
/// wins (the labels-only stamp), else a unique terminal name, else `None` so
/// the caller falls back to the `hc:` id and never publishes a collision.
fn mail_names(terminals: &[Terminal]) -> Vec<Option<String>> {
    let pins: Vec<Option<&str>> = terminals
        .iter()
        .map(|t| t.labels.get(TOKEN_PIN).map(String::as_str))
        .collect();
    let names: Vec<Option<&str>> = terminals.iter().map(|t| t.name.as_deref()).collect();
    (0..terminals.len())
        .map(|i| {
            unique_mail_name(pins[i], &pins)
                .or_else(|| unique_mail_name(names[i], &names))
                .map(str::to_string)
        })
        .collect()
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

/// Stable per-client caller id. hc uses `(caller, terminal)` to remember the
/// turn a send started, so a bare wait from the same caller waits for that
/// turn instead of returning at once while the agent is still idle.
static CALLER_SEQ: AtomicU64 = AtomicU64::new(1);

/// Blocking JSON-RPC client over the hc unix socket.
#[derive(Debug)]
pub struct Client {
    socket_path: String,
    next_id: AtomicU64,
    caps: Capabilities,
    fold_policy: FoldPolicy,
    caller: String,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            next_id: AtomicU64::new(self.next_id.load(Ordering::Relaxed)),
            caps: self.caps.clone(),
            fold_policy: self.fold_policy,
            caller: self.caller.clone(),
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
            caller: format!(
                "toron-{}-{}",
                std::process::id(),
                CALLER_SEQ.fetch_add(1, Ordering::Relaxed)
            ),
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
        // Version floor: refuse older servers with a typed, actionable code.
        if !version_at_least(&hello.version, MIN_HC_VERSION) {
            return Err(BackendError::new(
                BackendErrorCode::Other("hc_too_old".into()),
                format!(
                    "hc {} is below the supported minimum {MIN_HC_VERSION}",
                    hello.version
                ),
            ));
        }
        if !version_at_least(&hello.version, TESTED_HC_VERSION) {
            // Newer than tested: additive-only, but make drift visible.
            static NOTED: std::sync::Once = std::sync::Once::new();
            NOTED.call_once(|| {
                eprintln!(
                    "toron/hc: server {} is newer than tested {TESTED_HC_VERSION}; \
                     additive changes only — verify before relying on new features",
                    hello.version
                );
            });
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
            BackendError::new(BackendErrorCode::Malformed, format!("hello result: {e}"))
        })
    }

    pub fn terminal_list(&self, all: bool) -> Result<Vec<Terminal>, BackendError> {
        let result = self.call_coded("terminal.list", json!({ "all": all }))?;
        let terminals = result
            .get("terminals")
            .cloned()
            .unwrap_or(Value::Array(vec![]));
        serde_json::from_value(terminals).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.list: {e}"))
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
        self.terminal_run_with_env(argv, name, cwd, None, None)
    }

    /// Run a terminal, optionally under a parent and with extra env.
    /// Empty `argv` runs that machine's own shell. Env carries connection
    /// facts (`HC_SOCKET_PATH`/`HC_SESSION`); identity rides on labels.
    pub fn terminal_run_with_env(
        &self,
        argv: &[String],
        name: Option<&str>,
        cwd: Option<&str>,
        parent: Option<&str>,
        env: Option<&BTreeMap<String, String>>,
    ) -> Result<Terminal, BackendError> {
        let mut params = json!({ "argv": argv });
        if let Some(name) = name {
            params["name"] = json!(name);
        }
        if let Some(cwd) = cwd {
            params["cwd"] = json!(cwd);
        }
        if let Some(parent) = parent {
            params["parent"] = json!(strip_id_prefix(parent));
        }
        if let Some(env) = env {
            params["env"] = json!(env);
        }
        params["no_parent"] = json!(true);
        params["wait"] = json!(false);
        let result = self.call_coded("terminal.run", params)?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.run: {e}"))
        })
    }

    /// Send without waiting for the turn.
    pub fn terminal_send(
        &self,
        terminal: &str,
        text: &str,
        wait_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        match wait_ms {
            Some(ms) => self.terminal_send_wait(terminal, text, Some(ms)),
            None => self.call_coded(
                "terminal.send",
                json!({
                    "terminal": strip_id_prefix(terminal),
                    "text": text,
                    "caller": self.caller,
                }),
            ),
        }
    }

    /// Send, then wait for the turn it started. `timeout_ms: None` means the
    /// hc default (180 s), sent explicitly so the socket deadline covers it.
    pub fn terminal_send_wait(
        &self,
        terminal: &str,
        text: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let secs = timeout_ms.unwrap_or(DEFAULT_WAIT_MS) as f64 / 1000.0;
        self.call_coded(
            "terminal.send",
            json!({
                "terminal": strip_id_prefix(terminal),
                "text": text,
                "caller": self.caller,
                "wait": true,
                "timeout": secs,
            }),
        )
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
        let secs = timeout_ms.unwrap_or(DEFAULT_WAIT_MS) as f64 / 1000.0;
        self.call_coded(
            "terminal.wait",
            json!({
                "terminal": strip_id_prefix(terminal),
                "caller": self.caller,
                "timeout": secs,
            }),
        )
    }

    /// Wait until post-call output contains `output` (plain text), or the
    /// terminal blocks, errors, or exits (`terminal.wait` `output` needle).
    pub fn terminal_wait_output(
        &self,
        terminal: &str,
        output: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let secs = timeout_ms.unwrap_or(DEFAULT_WAIT_MS) as f64 / 1000.0;
        self.call_coded(
            "terminal.wait",
            json!({
                "terminal": strip_id_prefix(terminal),
                "output": output,
                "timeout": secs,
            }),
        )
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

    /// Keep a live terminal across loss: replace its program with a shell
    /// instead of dropping it. Exit with a code / explicit kill stay final.
    pub fn terminal_keep(&self, terminal: &str, keep: bool) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.keep",
            json!({ "terminal": strip_id_prefix(terminal), "keep": keep }),
        )
    }

    /// Press the stop key (esc if it reports status, else ctrl+c) and report
    /// what was observed. Never escalates.
    pub fn terminal_interrupt(
        &self,
        terminal: &str,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "terminal": strip_id_prefix(terminal) });
        if let Some(ms) = timeout_ms {
            params["timeout"] = json!(ms as f64 / 1000.0);
        }
        self.call_coded("terminal.interrupt", params)
    }

    /// Write bytes to a file beside a live terminal, on its machine.
    pub fn terminal_put(
        &self,
        terminal: &str,
        name: &str,
        data: &str,
    ) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.put",
            json!({ "terminal": strip_id_prefix(terminal), "name": name, "data": data }),
        )
    }

    /// Open a raw (framed) stream to a live terminal. Requires framed mode.
    pub fn terminal_attach(&self, terminal: &str) -> Result<Value, BackendError> {
        self.call_coded(
            "terminal.attach",
            json!({ "terminal": strip_id_prefix(terminal) }),
        )
    }

    /// List templates by name and description.
    pub fn templates_list(&self) -> Result<Value, BackendError> {
        self.call_coded("templates.list", json!({}))
    }

    /// This core's identity, version, pid, uptime and terminal counts.
    pub fn server_status(&self) -> Result<Value, BackendError> {
        self.call_coded("server.status", json!({}))
    }

    /// Stop the core (kept terminals stay pending, get shells on next start).
    pub fn server_stop(&self) -> Result<Value, BackendError> {
        self.call_coded("server.stop", json!({}))
    }

    /// Planned restart: screens saved, programs keep running, next core adopts.
    pub fn server_restart(&self) -> Result<Value, BackendError> {
        self.call_coded("server.restart", json!({}))
    }

    /// Run one program on a machine without a terminal; returns exit code + output.
    pub fn machine_exec(
        &self,
        argv: &[String],
        line: Option<&str>,
        on: Option<&str>,
        cwd: Option<&str>,
        timeout_ms: Option<u64>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "argv": argv });
        if let Some(l) = line {
            params["line"] = json!(l);
        }
        if let Some(m) = on {
            params["on"] = json!(m);
        }
        if let Some(c) = cwd {
            params["cwd"] = json!(c);
        }
        if let Some(ms) = timeout_ms {
            params["timeout"] = json!(ms as f64 / 1000.0);
        }
        self.call_coded("machine.exec", params)
    }

    /// Read a text file here or on a machine.
    pub fn file_read(
        &self,
        path: &str,
        on: Option<&str>,
        offset: Option<u64>,
        limit: Option<u64>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "path": path });
        if let Some(m) = on {
            params["on"] = json!(m);
        }
        if let Some(o) = offset {
            params["offset"] = json!(o);
        }
        if let Some(l) = limit {
            params["limit"] = json!(l);
        }
        self.call_coded("file.read", params)
    }

    /// Write a file (atomic swap).
    pub fn file_write(
        &self,
        path: &str,
        data: &str,
        on: Option<&str>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "path": path, "data": data });
        if let Some(m) = on {
            params["on"] = json!(m);
        }
        self.call_coded("file.write", params)
    }

    /// Read an opaque JSON document + version.
    pub fn doc_get(&self, name: &str, on: Option<&str>) -> Result<Value, BackendError> {
        let mut params = json!({ "name": name });
        if let Some(m) = on {
            params["on"] = json!(m);
        }
        self.call_coded("doc.get", params)
    }

    /// Write an opaque JSON document if version still matches.
    pub fn doc_set(
        &self,
        name: &str,
        value: Value,
        version: u64,
        on: Option<&str>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "name": name, "value": value, "version": version });
        if let Some(m) = on {
            params["on"] = json!(m);
        }
        self.call_coded("doc.set", params)
    }

    /// Machines added here.
    pub fn machine_list(&self) -> Result<Value, BackendError> {
        self.call_coded("machine.list", json!({}))
    }

    /// Add another machine over ssh/command.
    pub fn machine_add(
        &self,
        name: &str,
        target: &str,
        via: Option<&str>,
    ) -> Result<Value, BackendError> {
        let mut params = json!({ "name": name, "target": target });
        if let Some(v) = via {
            params["via"] = json!(v);
        }
        self.call_coded("machine.add", params)
    }

    /// Close a workspace. hc has no workspace object: a workspace is a root
    /// shell terminal plus its descendants. Flywheel creates every child
    /// `outlive_parent`, so the root's death does not end the tree — kill and
    /// remove each pane instead. A workspace with no panes is a no-op.
    pub fn close_workspace(&self, workspace_id: &str) -> Result<(), BackendError> {
        let want = strip_id_prefix(workspace_id);
        let panes = self.pane_rows(self.terminal_list(true)?);
        let mut targets: Vec<String> = panes
            .into_iter()
            .filter(|p| strip_id_prefix(&p.workspace_id) == want)
            .map(|p| p.pane_id)
            .collect();
        targets.reverse();
        for pane in targets {
            match self.terminal_kill(&pane) {
                Ok(_) => {}
                Err(e) if e.code() == &BackendErrorCode::Other("not_found".into()) => continue,
                Err(e) => return Err(e),
            }
            let _ = self.terminal_rm(&pane);
        }
        Ok(())
    }

    /// Set labels on a terminal and return the updated Terminal
    /// (mail-identity stamp: `toron_pin` / `toron_project` / `toron_npub`).
    pub fn terminal_label(
        &self,
        terminal: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<Terminal, BackendError> {
        let result = self.call_coded(
            "terminal.label",
            json!({
                "terminal": strip_id_prefix(terminal),
                "labels": labels,
            }),
        )?;
        serde_json::from_value(result).map_err(|e| {
            BackendError::new(BackendErrorCode::Malformed, format!("terminal.label: {e}"))
        })
    }

    pub fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        let terminals = self.terminal_list(false)?;
        let parents = parent_map(&terminals);
        let known: BTreeSet<String> = terminals.iter().map(|t| t.address.clone()).collect();
        let mail_names = mail_names(&terminals);
        Ok(terminals
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let mut info =
                    t.to_agent_info_with_mail_name(self.fold_policy, mail_names[i].clone());
                info.workspace_id = prefix_id(&root_address(&parents, &known, &t.address));
                info
            })
            .collect())
    }

    pub fn agent_get(&self, target: &str) -> Result<AgentInfo, BackendError> {
        let term = self.terminal_get(target)?;
        let mail_name = term
            .labels
            .get(TOKEN_PIN)
            .cloned()
            .or_else(|| term.name.clone());
        Ok(term.to_agent_info_with_mail_name(self.fold_policy, mail_name))
    }

    pub fn pane_list(&self) -> Result<Vec<PaneInfo>, BackendError> {
        Ok(self.pane_rows(self.terminal_list(false)?))
    }

    /// Build pane rows from one `terminal.list` snapshot: unique mail
    /// names plus each terminal's root-ancestor workspace id.
    fn pane_rows(&self, terminals: Vec<Terminal>) -> Vec<PaneInfo> {
        let parents = parent_map(&terminals);
        let known: BTreeSet<String> = terminals.iter().map(|t| t.address.clone()).collect();
        let mail_names = mail_names(&terminals);
        terminals
            .into_iter()
            .enumerate()
            .map(|(i, t)| {
                let ws = prefix_id(&root_address(&parents, &known, &t.address));
                let mut info =
                    t.to_pane_info_with_mail_name(self.fold_policy, mail_names[i].clone());
                info.workspace_id = ws;
                info
            })
            .collect()
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
                pending: VecDeque::new(),
            },
        ))
    }
}

fn socket_timeout_from_params(params: &Value) -> Duration {
    let ms = params
        .get("timeout")
        .and_then(Value::as_f64)
        .map(|s| (s * 1000.0) as u64)
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
        "blocked" => BackendErrorCode::AgentBlocked,
        "status_mismatch" => BackendErrorCode::AgentNotIdle,
        other => BackendErrorCode::Other(other.to_string()),
    }
}

/// Map one hc `SendResult` onto the coded landing the old prompt path used.
/// `started: false` means the text went in but the program did not go
/// `working` within hc's start window — the old `agent_prompt_stalled`.
fn landing_from_send(val: &Value) -> PromptLanding {
    let status = val
        .get("status")
        .and_then(Value::as_str)
        .map(normalize_agent_status);
    let code = (val.get("started").and_then(Value::as_bool) == Some(false))
        .then(|| "agent_prompt_stalled".to_string());
    PromptLanding { status, code }
}

/// Long-lived hc event stream (plain mode: notifications after subscribe).
pub struct HcEventStream {
    reader: BufReader<UnixStream>,
    fold_policy: FoldPolicy,
    /// A `missed` notification carries a fresh snapshot; its terminal records
    /// are queued here so the consumer sees the gap notice first, then the
    /// current status of every terminal the snapshot named.
    pending: VecDeque<BackendEvent>,
}

impl HcEventStream {
    fn decode_line(&mut self, line: &str) -> Result<Option<BackendEvent>, BackendError> {
        let val: Value = serde_json::from_str(line.trim()).map_err(|e| {
            BackendError::new(
                BackendErrorCode::Malformed,
                format!("hc event: {line}: {e}"),
            )
        })?;
        let Some(method) = val.get("method").and_then(Value::as_str) else {
            if let Some(event) = terminal_event_from_value(&val, self.fold_policy) {
                return Ok(Some(event));
            }
            return Ok(Some(BackendEvent::Raw(line.to_string())));
        };
        let params = val.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "missed" => {
                if let Some(terminals) = params.get("terminals").and_then(Value::as_array) {
                    for record in terminals {
                        let Ok(term) = serde_json::from_value::<Terminal>(record.clone()) else {
                            continue;
                        };
                        self.pending
                            .push_back(status_event(&term, self.fold_policy));
                    }
                }
                Ok(Some(BackendEvent::EventsLost))
            }
            "event" => Ok(Some(event_from_params(&params, self.fold_policy))),
            "heartbeat" => Ok(Some(BackendEvent::Raw(line.to_string()))),
            _ => {
                if let Some(event) = terminal_event_from_value(&params, self.fold_policy) {
                    return Ok(Some(event));
                }
                Ok(Some(BackendEvent::Raw(line.to_string())))
            }
        }
    }
}

impl EventStream for HcEventStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }
        loop {
            let mut buf = String::new();
            let n = self
                .reader
                .read_line(&mut buf)
                .map_err(|e| map_io_err("read hc event", e))?;
            if n == 0 {
                return Ok(None);
            }
            let event = self.decode_line(&buf)?;
            if event.is_some() {
                return Ok(event);
            }
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), BackendError> {
        self.reader
            .get_mut()
            .set_read_timeout(timeout)
            .map_err(|e| BackendError::new(BackendErrorCode::Io, e.to_string()))
    }
}

/// One `event` notification: `{"type": …, "terminal": "t7", "data": {…}}`.
fn event_from_params(params: &Value, policy: FoldPolicy) -> BackendEvent {
    let event_type = params.get("type").and_then(Value::as_str).unwrap_or("");
    let data = params.get("data").cloned().unwrap_or(Value::Null);
    let pane = params
        .get("terminal")
        .and_then(Value::as_str)
        .or_else(|| data.get("address").and_then(Value::as_str))
        .map(|a| PaneId::new(prefix_id(a)));
    match event_type {
        "status" => match pane {
            Some(pane) => BackendEvent::AgentStatusChanged {
                pane,
                agent: None,
                status: fold_status_data(&data, policy),
            },
            None => BackendEvent::Raw(params.to_string()),
        },
        "turn" => match pane {
            Some(pane) => BackendEvent::AgentStatusChanged {
                pane,
                agent: None,
                status: data
                    .get("state")
                    .and_then(Value::as_str)
                    .map(normalize_agent_status)
                    .unwrap_or(AgentStatus::Unknown),
            },
            None => BackendEvent::Raw(params.to_string()),
        },
        "finished" | "lost" | "removed" => match pane {
            Some(pane) => BackendEvent::PaneExited { pane },
            None => BackendEvent::Raw(params.to_string()),
        },
        "created" | "restarted" => match serde_json::from_value::<Terminal>(data) {
            Ok(term) => status_event(&term, policy),
            Err(_) => BackendEvent::Raw(params.to_string()),
        },
        // labels/title/size/foreground/keep/doc do not change the status fold.
        _ => BackendEvent::Raw(params.to_string()),
    }
}

/// Fold the `status` event's root + child records through the same policy the
/// snapshot uses. A null root with no records is `Unknown`, never `Idle`.
fn fold_status_data(data: &Value, policy: FoldPolicy) -> AgentStatus {
    let mut board = Board::default();
    if let Some(root) = data.get("status").filter(|v| !v.is_null())
        && let Ok(status) = serde_json::from_value::<HcStatus>(root.clone())
    {
        board.insert_hc("", &status.view());
    }
    if let Some(records) = data.get("records").and_then(Value::as_object) {
        for (id, record) in records {
            if let Ok(status) = serde_json::from_value::<HcStatus>(record.clone()) {
                board.insert_hc(id, &status.view());
            }
        }
    }
    fold(&board, policy)
}

fn status_event(term: &Terminal, policy: FoldPolicy) -> BackendEvent {
    let pane = PaneId::new(prefix_id(&term.address));
    if term.state.as_deref() == Some("finished") {
        return BackendEvent::PaneExited { pane };
    }
    BackendEvent::AgentStatusChanged {
        pane,
        agent: term.name.clone(),
        status: term.agent_status(policy),
    }
}

fn terminal_event_from_value(val: &Value, policy: FoldPolicy) -> Option<BackendEvent> {
    let term_val = if val.get("address").is_some() {
        val.clone()
    } else {
        val.get("terminal")?.clone()
    };
    let term: Terminal = serde_json::from_value(term_val).ok()?;
    Some(status_event(&term, policy))
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

/// bd-1hkt0: byte-identical port of flywheel's hc liveness fold.
///
/// `terminal.get` carries the shell pid and the foreground group leader;
/// a binary match is ground truth, a shell-owned foreground means the agent
/// exited, and an unknown kind with no other signal stays inconclusive.
fn agent_alive_from_terminal(res: &Value, kind: &str) -> Option<bool> {
    let shell_pid = res.get("pid").and_then(Value::as_u64);
    let (foreground_pgid, foreground): (Option<u64>, Vec<(String, Option<String>)>) =
        match res.get("foreground") {
            Some(fg) if !fg.is_null() => {
                let pid = fg.get("pid").and_then(Value::as_u64);
                let argv0 = fg
                    .get("argv")
                    .and_then(Value::as_array)
                    .and_then(|a| a.first())
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let procs = match fg.get("name").and_then(Value::as_str) {
                    Some(name) => vec![(name.to_string(), argv0)],
                    None => Vec::new(),
                };
                (pid, procs)
            }
            _ => (None, Vec::new()),
        };
    let names = kind_process_names(kind);
    let binary_hit = names.iter().any(|n| {
        foreground
            .iter()
            .any(|(name, argv0)| process_name_matches(name, n) || argv0.as_deref() == Some(*n))
    });
    if binary_hit {
        return Some(true);
    }
    if matches!((foreground_pgid, shell_pid), (Some(fg), Some(sh)) if fg == sh) {
        return Some(false);
    }
    if names.is_empty() {
        return None;
    }
    Some(false)
}

/// Known agent-kind → executable names (flywheel's `kind_process_names`).
fn kind_process_names(kind: &str) -> &'static [&'static str] {
    match kind {
        "opencode" | "oc" => &["opencode"],
        "pi" => &["pi", "gemini"],
        "grok" | "gk" => &["grok", "grok-build"],
        _ => &[],
    }
}

/// Exact match, or the same name with an npm/mise `.exe` suffix.
fn process_name_matches(candidate: &str, expected: &str) -> bool {
    candidate == expected
        || candidate
            .strip_suffix(".exe")
            .is_some_and(|stem| stem == expected)
}

#[cfg(test)]
mod agent_alive_tests {
    use super::agent_alive_from_terminal;
    use serde_json::json;

    #[test]
    fn hc_liveness_sees_the_agent_binary() {
        let res =
            json!({"pid": 10, "foreground": {"pid": 11, "name": "opencode", "argv": ["opencode"]}});
        assert_eq!(agent_alive_from_terminal(&res, "opencode"), Some(true));
    }

    #[test]
    fn hc_liveness_reads_a_shell_owned_foreground_as_dead() {
        let res = json!({"pid": 10, "foreground": {"pid": 10, "name": "zsh", "argv": ["-zsh"]}});
        assert_eq!(agent_alive_from_terminal(&res, "opencode"), Some(false));
    }

    #[test]
    fn hc_liveness_is_inconclusive_for_an_unknown_kind() {
        let res = json!({"pid": 10, "foreground": {"pid": 11, "name": "mystery"}});
        assert_eq!(agent_alive_from_terminal(&res, "mystery-kind"), None);
    }
}

impl Backend for HcBackend {
    fn name(&self) -> &'static str {
        "hc"
    }

    /// Group live terminals by root address (the same root `agent_list`
    /// calls a workspace). hc itself has no workspace object; returning
    /// `[]` made every workspace-label lookup fail ("no flywheel swarm")
    /// even for a live swarm. The `flywheel_workspace` label is the swarm
    /// label, so label matching works again.
    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        let terminals = self.client.terminal_list(false)?;
        let parents = parent_map(&terminals);
        let known: BTreeSet<String> = terminals.iter().map(|t| t.address.clone()).collect();
        let mut groups: BTreeMap<String, Vec<&Terminal>> = BTreeMap::new();
        for t in &terminals {
            groups
                .entry(root_address(&parents, &known, &t.address))
                .or_default()
                .push(t);
        }
        Ok(groups
            .into_iter()
            .map(|(root, rows)| {
                let label = rows
                    .iter()
                    .find_map(|t| {
                        t.labels
                            .get("flywheel_workspace")
                            .filter(|v| !v.is_empty())
                            .cloned()
                    })
                    .unwrap_or_else(|| root.clone());
                let first = rows.first().expect("group is non-empty");
                WorkspaceInfo {
                    workspace_id: prefix_id(&root),
                    number: 0,
                    label,
                    focused: false,
                    pane_count: u64::try_from(rows.len()).unwrap_or(u64::MAX),
                    tab_count: 0,
                    active_tab_id: String::new(),
                    agent_status: first.agent_status(self.client.fold_policy),
                    cwd: first.cwd.clone(),
                }
            })
            .collect())
    }

    fn workspace_close(&self, workspace_id: &str, _close_group: bool) -> Result<(), BackendError> {
        self.client.close_workspace(workspace_id)
    }

    fn workspace_create(&self, label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        let term =
            self.client
                .terminal_run_with_env(&[], None, None, None, spawn_env().as_ref())?;
        let workspace_id = prefix_id(&term.address);
        Ok(WorkspaceInfo {
            workspace_id: workspace_id.clone(),
            number: 0,
            label: label.unwrap_or(workspace_id.as_str()).to_string(),
            focused: false,
            pane_count: 1,
            tab_count: 0,
            active_tab_id: String::new(),
            agent_status: term.agent_status(self.client.fold_policy),
            cwd: term.cwd.clone(),
        })
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        _direction: &str,
        cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        Ok(self
            .client
            .terminal_run_with_env(&[], None, cwd, Some(parent_pane_id), spawn_env().as_ref())?
            .to_pane_info(self.client.fold_policy))
    }

    fn pane_list(&self, workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        if let Some(ws) = workspace_id {
            let want = strip_id_prefix(ws);
            let hits: Vec<PaneInfo> = self
                .client
                .pane_rows(self.client.terminal_list(false)?)
                .into_iter()
                .filter(|p| strip_id_prefix(&p.workspace_id) == want)
                .collect();
            if !hits.is_empty() {
                return Ok(hits);
            }
            return Ok(vec![
                self.client
                    .terminal_get(ws)?
                    .to_pane_info(self.client.fold_policy),
            ]);
        }
        self.client.pane_list()
    }

    fn pane_run(&self, pane_id: &str, command: &str) -> Result<(), BackendError> {
        let _ = self.client.terminal_send(pane_id, command, None)?;
        Ok(())
    }

    fn pane_wait_output(
        &self,
        pane_id: &str,
        needle: &str,
        timeout_ms: u64,
    ) -> Result<Value, BackendError> {
        self.client
            .terminal_wait_output(pane_id, needle, Some(timeout_ms))
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), BackendError> {
        let _ = self.client.terminal_kill(pane_id)?;
        let _ = self.client.terminal_rm(pane_id);
        Ok(())
    }

    /// bd-1hkt0: hc's liveness is unchanged — port of flywheel's process fold.
    fn agent_alive(&self, pane_id: &str, kind: &str) -> Option<bool> {
        let res = self
            .client
            .call(
                "terminal.get",
                json!({ "terminal": strip_id_prefix(pane_id) }),
            )
            .ok()?;
        agent_alive_from_terminal(&res, kind)
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        self.client.agent_list()
    }

    fn agent_start(
        &self,
        _name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        let mut words = Vec::with_capacity(1 + args.len());
        words.push(kind.to_string());
        words.extend(args.iter().cloned());
        let line = shell_join(&words);
        self.client.terminal_send(pane_id, &line, None)?;
        self.client.agent_get(pane_id)
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        let Some(name) = new_name else {
            return Ok(());
        };
        self.client.call_coded(
            "terminal.label",
            json!({
                "terminal": strip_id_prefix(target),
                "labels": { "name": name, TOKEN_PIN: name },
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
        match wait {
            Some(w) => self.client.terminal_send_wait(target, text, w.timeout_ms),
            None => self.client.terminal_send(target, text, None),
        }
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        match self.prompt(target, text, wait) {
            Ok(val) => Ok(landing_from_send(&val)),
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

    fn subscribe(&self, filters: &[Subscription]) -> Result<Box<dyn EventStream>, BackendError> {
        let wanted: Option<BTreeSet<String>> = (!filters.is_empty()).then(|| {
            filters
                .iter()
                .map(|f| strip_id_prefix(&f.pane_id).to_string())
                .collect()
        });
        let (snapshot, stream) = self.client.subscribe_terminals(None)?;
        let pending: VecDeque<BackendEvent> = snapshot
            .into_iter()
            .filter(|t| {
                wanted
                    .as_ref()
                    .is_none_or(|set| set.contains(strip_id_prefix(&t.address)))
            })
            .map(|t| {
                let status = t.agent_status(self.client.fold_policy);
                BackendEvent::AgentStatusChanged {
                    pane: PaneId::new(prefix_id(&t.address)),
                    agent: t.name.clone(),
                    status,
                }
            })
            .collect();
        Ok(Box::new(SnapshotThenStream {
            pending,
            inner: FilteredStream {
                inner: Box::new(stream),
                wanted,
            },
        }))
    }
}

/// The pane an event belongs to, if it is pane-scoped. `EventsLost` and
/// `Raw` are stream-level: they always pass the filter.
fn event_pane(ev: &BackendEvent) -> Option<&str> {
    match ev {
        BackendEvent::AgentStatusChanged { pane, .. } => Some(pane.as_str()),
        BackendEvent::AgentDetected { pane, .. } => Some(pane.as_str()),
        BackendEvent::PaneExited { pane } => Some(pane.as_str()),
        BackendEvent::EventsLost | BackendEvent::Raw(_) => None,
    }
}

/// Drops pane-scoped events outside the subscribed set (`None` = all).
/// hc subscribes per-connection, so one connection fans out to many panes
/// and the filter restores per-pane routing.
struct FilteredStream {
    inner: Box<dyn EventStream>,
    wanted: Option<BTreeSet<String>>,
}

impl EventStream for FilteredStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        loop {
            match self.inner.read_event()? {
                None => return Ok(None),
                Some(ev) => {
                    let keep = match event_pane(&ev) {
                        None => true,
                        Some(pane) => self
                            .wanted
                            .as_ref()
                            .is_none_or(|set| set.contains(strip_id_prefix(pane))),
                    };
                    if keep {
                        return Ok(Some(ev));
                    }
                }
            }
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), BackendError> {
        self.inner.set_read_timeout(timeout)
    }
}

struct SnapshotThenStream {
    pending: VecDeque<BackendEvent>,
    inner: FilteredStream,
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
        requests: Arc<Mutex<Vec<(String, Value)>>>,
    }

    impl FakeHc {
        /// Requests the fake received, in order (method + params).
        fn requests(&self) -> Vec<(String, Value)> {
            self.requests.lock().expect("lock").clone()
        }

        /// Params of every call to one method, in order.
        fn requests_for(&self, method: &str) -> Vec<Value> {
            self.requests()
                .into_iter()
                .filter(|(m, _)| m == method)
                .map(|(_, p)| p)
                .collect()
        }

        fn spawn(methods: Vec<String>, terminals: Vec<Value>) -> Self {
            let dir = tempfile::tempdir().expect("tmpdir");
            let socket = dir.path().join("test.sock");
            let methods = Arc::new(Mutex::new(methods));
            let terminals = Arc::new(Mutex::new(terminals));
            let requests = Arc::new(Mutex::new(Vec::new()));
            let listener = UnixListener::bind(&socket).expect("bind");
            let methods_c = Arc::clone(&methods);
            let terminals_c = Arc::clone(&terminals);
            let requests_c = Arc::clone(&requests);
            thread::spawn(move || {
                while let Ok((stream, _)) = listener.accept() {
                    let methods = Arc::clone(&methods_c);
                    let terminals = Arc::clone(&terminals_c);
                    let requests = Arc::clone(&requests_c);
                    thread::spawn(move || serve_conn(stream, methods, terminals, requests));
                }
            });
            thread::sleep(Duration::from_millis(20));
            Self {
                socket,
                _dir: dir,
                _methods: methods,
                _terminals: terminals,
                requests,
            }
        }
    }

    fn serve_conn(
        stream: UnixStream,
        methods: Arc<Mutex<Vec<String>>>,
        terminals: Arc<Mutex<Vec<Value>>>,
        requests: Arc<Mutex<Vec<(String, Value)>>>,
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
            let method = method.to_string();
            requests.lock().expect("lock").push((
                method.clone(),
                req.get("params").cloned().unwrap_or(Value::Null),
            ));
            let result = match method.as_str() {
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
                | "terminal.rm" => json!({ "ok": true, "turn": 1 }),
                "terminal.label" => {
                    let term = req
                        .pointer("/params/terminal")
                        .and_then(Value::as_str)
                        .unwrap_or("t1");
                    let labels = req.pointer("/params/labels").cloned().unwrap_or(json!({}));
                    json!({ "address": term, "labels": labels })
                }
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
        assert_eq!(
            term.agent_status(FoldPolicy::Attention),
            AgentStatus::Blocked
        );
        assert_eq!(
            term.agent_status(FoldPolicy::RootWins),
            AgentStatus::Working
        );
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

    /// In-memory [`EventStream`] for filter tests: replays a fixed queue.
    struct StubStream {
        events: VecDeque<BackendEvent>,
    }

    impl EventStream for StubStream {
        fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
            Ok(self.events.pop_front())
        }
    }

    fn changed(pane: &str) -> BackendEvent {
        BackendEvent::AgentStatusChanged {
            pane: PaneId::new(pane.to_string()),
            agent: None,
            status: AgentStatus::Working,
        }
    }

    #[test]
    fn subscribe_filters_snapshot_and_stream_to_the_requested_panes() {
        let mut stream = FilteredStream {
            inner: Box::new(StubStream {
                events: VecDeque::from([
                    changed("hc:t1"),
                    changed("hc:t2"),
                    BackendEvent::PaneExited {
                        pane: PaneId::new("hc:t2".to_string()),
                    },
                    BackendEvent::EventsLost,
                    BackendEvent::Raw("noise".into()),
                ]),
            }),
            wanted: Some(BTreeSet::from(["t1".to_string()])),
        };
        let mut got = Vec::new();
        while let Some(ev) = stream.read_event().expect("read") {
            got.push(ev);
        }
        assert_eq!(
            got,
            [
                changed("hc:t1"),
                BackendEvent::EventsLost,
                BackendEvent::Raw("noise".into())
            ]
        );
        let mut stream = FilteredStream {
            inner: Box::new(StubStream {
                events: VecDeque::from([changed("hc:t2")]),
            }),
            wanted: None,
        };
        assert_eq!(stream.read_event().expect("read"), Some(changed("hc:t2")));
    }

    #[test]
    fn backend_subscribe_emits_only_the_subscribed_pane_snapshot() {
        let other = json!({ "address": "t8", "status": { "state": "working" } });
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal(), other]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let mut stream = backend
            .subscribe(&[Subscription::agent_status_changed("hc:t7".into())])
            .expect("sub");
        let subs = fake.requests_for("events.subscribe");
        assert_eq!(subs.len(), 1);
        assert!(subs[0].get("terminal").is_none());
        let ev = stream.read_event().expect("ev").expect("some");
        match ev {
            BackendEvent::AgentStatusChanged { pane, status, .. } => {
                assert_eq!(pane.as_str(), "hc:t7");
                assert_eq!(status, AgentStatus::Blocked);
            }
            other => panic!("unexpected {other:?}"),
        }
        stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .expect("timeout");
        let err = stream.read_event().expect_err("no more events");
        assert_eq!(err.code(), &BackendErrorCode::Timeout);
    }

    #[test]
    fn workspace_list_is_empty_and_create_names_a_shell_by_its_address() {
        let fake = FakeHc::spawn(full_methods(), vec![]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let list = backend.workspace_list().expect("list");
        assert!(list.is_empty());
    }

    #[test]
    fn mail_name_unique_resolves_and_duplicate_is_refused() {
        let named = [(Some("review"), "t1"), (Some("fix"), "t2"), (None, "t3")];
        assert_eq!(mail_pane_id_for_name(&named, "review").unwrap(), "hc:t1");
        assert_eq!(mail_pane_id_for_name(&named, "hc:t3").unwrap(), "hc:t3");
        assert_eq!(
            mail_pane_id_for_name(&named, "missing"),
            Err(MailNameError::Missing)
        );
        assert_eq!(mail_pane_id_for_name(&named, "t3").unwrap(), "hc:t3");

        let dup = [(Some("twin"), "t1"), (Some("twin"), "t2"), (None, "t3")];
        assert_eq!(
            mail_pane_id_for_name(&dup, "twin"),
            Err(MailNameError::Duplicate { count: 2 })
        );
        assert_eq!(mail_pane_id_for_name(&dup, "hc:t2").unwrap(), "hc:t2");

        let names = [Some("twin"), Some("twin"), None];
        assert_eq!(unique_mail_name(Some("twin"), &names), None);
        assert_eq!(unique_mail_name(None, &names), None);
        assert_eq!(
            unique_mail_name(Some("solo"), &[Some("solo"), None]),
            Some("solo")
        );
    }

    #[test]
    fn id_prefix_round_trip() {
        assert_eq!(strip_id_prefix("hc:t7"), "t7");
        assert_eq!(strip_id_prefix("t7"), "t7");
        assert_eq!(prefix_id("t7"), "hc:t7");
        assert_eq!(prefix_id("hc:t7"), "hc:t7");
    }

    #[test]
    fn shell_quote_leaves_safe_words_bare_and_quotes_the_rest() {
        assert_eq!(shell_quote("pi"), "pi");
        assert_eq!(shell_quote("--project"), "--project");
        assert_eq!(shell_quote("a b"), "'a b'");
        assert_eq!(shell_quote("it's"), "'it'\\''s'");
        assert_eq!(
            shell_join(&["pi".into(), "--as".into(), "a b".into()]),
            "pi --as 'a b'"
        );
    }

    #[test]
    fn a_unique_toron_pin_label_is_the_mail_name() {
        let fake = FakeHc::spawn(
            full_methods(),
            vec![
                json!({"address": "t7", "name": "lane", "labels": {"toron_pin": "DustySparrow"}}),
                json!({"address": "t8", "name": "review"}),
            ],
        );
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        let agents = client.agent_list().expect("agents");
        assert_eq!(agents[0].name.as_deref(), Some("DustySparrow"));
        assert_eq!(agents[1].name.as_deref(), Some("review"));
        let panes = client.pane_list().expect("panes");
        assert_eq!(panes[0].display_agent.as_deref(), Some("DustySparrow"));
        assert_eq!(panes[1].display_agent.as_deref(), Some("review"));
    }

    #[test]
    fn send_and_wait_use_schema_params() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        client.terminal_send("t7", "hi", Some(2000)).expect("send");
        client.terminal_wait("t7", Some(3000)).expect("wait");
        client
            .terminal_wait_output("t7", "needle", Some(1500))
            .expect("wait-output");
        let sends = fake.requests_for("terminal.send");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0]["terminal"], "t7");
        assert_eq!(sends[0]["wait"], true);
        assert_eq!(sends[0]["timeout"], 2.0);
        assert!(sends[0].get("timeout_ms").is_none());
        let waits = fake.requests_for("terminal.wait");
        assert_eq!(waits.len(), 2);
        assert_eq!(waits[0]["timeout"], 3.0);
        assert!(waits[0].get("timeout_ms").is_none());
        assert!(waits[0].get("output").is_none());
        assert_eq!(waits[1]["output"], "needle");
        assert_eq!(waits[1]["timeout"], 1.5);
    }

    #[test]
    fn agent_start_types_the_launch_line_into_the_given_pane() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let info = backend
            .agent_start(
                "DustySparrow",
                "pi",
                "hc:t7",
                &["--as".into(), "a b".into()],
            )
            .expect("start");
        assert!(fake.requests_for("terminal.run").is_empty());
        let sends = fake.requests_for("terminal.send");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0]["terminal"], "t7");
        assert_eq!(sends[0]["text"], "pi --as 'a b'");
        assert_eq!(info.pane_id, "hc:t7");
    }

    #[test]
    fn terminal_label_carries_the_three_mail_identity_keys() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        let mut labels = BTreeMap::new();
        labels.insert(
            toron_core::pane_types::TOKEN_PIN.to_string(),
            "DustySparrow".to_string(),
        );
        labels.insert(
            toron_core::pane_types::TOKEN_PROJECT.to_string(),
            "demo".to_string(),
        );
        labels.insert(
            toron_core::pane_types::TOKEN_NPUB.to_string(),
            "npub1x".to_string(),
        );
        let term = client.terminal_label("hc:t7", &labels).expect("label");
        let reqs = fake.requests_for("terminal.label");
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["terminal"], "t7");
        assert_eq!(reqs[0]["labels"]["toron_pin"], "DustySparrow");
        assert_eq!(reqs[0]["labels"]["toron_project"], "demo");
        assert_eq!(reqs[0]["labels"]["toron_npub"], "npub1x");
        assert_eq!(term.address, "t7");
        assert_eq!(
            term.labels.get("toron_pin").map(String::as_str),
            Some("DustySparrow")
        );
    }

    #[test]
    fn rename_sets_toron_pin_and_a_stamped_pane_resolves() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        backend
            .agent_rename("hc:t7", Some("DustySparrow"))
            .expect("rename");
        let reqs = fake.requests_for("terminal.label");
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0]["labels"]["toron_pin"], "DustySparrow");
        assert_eq!(reqs[0]["labels"]["name"], "DustySparrow");
        let term: Terminal = serde_json::from_value(json!({
            "address": "t7",
            "labels": { "toron_pin": "DustySparrow" },
        }))
        .expect("terminal");
        let pane = term.to_pane_info(FoldPolicy::Attention);
        assert_eq!(
            toron_core::mailbox::identity::pane_token_pin(&pane),
            Some("DustySparrow")
        );
    }

    #[test]
    fn workspace_create_runs_a_shell_named_by_its_address() {
        let fake = FakeHc::spawn(full_methods(), vec![]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let ws = backend.workspace_create(Some("squad")).expect("create");
        let runs = fake.requests_for("terminal.run");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0]["argv"], json!([]));
        assert_eq!(runs[0]["no_parent"], true);
        assert_eq!(ws.workspace_id, "hc:t9");
        assert_eq!(ws.label, "squad");
        let pane = backend
            .pane_split("hc:t9", "right", Some("/tmp"))
            .expect("split");
        let runs = fake.requests_for("terminal.run");
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[1]["parent"], "t9");
        assert_eq!(runs[1]["cwd"], "/tmp");
        assert_eq!(runs[1]["no_parent"], true);
        assert_eq!(pane.pane_id, "hc:t9");
    }

    #[test]
    fn every_terminal_carries_its_root_ancestor_workspace_id() {
        let chain = vec![
            json!({ "address": "t1" }),
            json!({ "address": "t2", "parent": "t1" }),
            json!({ "address": "t3", "parent": "t2" }),
        ];
        let fake = FakeHc::spawn(full_methods(), chain);
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        for info in client.agent_list().expect("agents") {
            assert_eq!(info.workspace_id, "hc:t1", "{info:?}");
        }
        for pane in client.pane_list().expect("panes") {
            assert_eq!(pane.workspace_id, "hc:t1", "{pane:?}");
        }
        let solo = vec![json!({ "address": "t9" })];
        let fake = FakeHc::spawn(full_methods(), solo);
        let client = Client::connect(Some(fake.socket.to_str().unwrap())).expect("connect");
        let panes = client.pane_list().expect("panes");
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].workspace_id, "hc:t9");
    }

    #[test]
    fn workspace_close_kills_the_root_and_its_descendants() {
        let chain = vec![
            json!({ "address": "t1" }),
            json!({ "address": "t2", "parent": "t1" }),
            json!({ "address": "t3", "parent": "t2" }),
        ];
        let fake = FakeHc::spawn(full_methods(), chain);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        backend.workspace_close("hc:t1", true).expect("close");
        let lists = fake.requests_for("terminal.list");
        assert_eq!(lists[0]["all"], true, "close must see finished roots too");
        let kills = fake.requests_for("terminal.kill");
        let killed: Vec<&str> = kills
            .iter()
            .filter_map(|k| k["terminal"].as_str())
            .collect();
        assert_eq!(killed, ["t3", "t2", "t1"], "{kills:?}");
        assert_eq!(fake.requests_for("terminal.rm").len(), 3, "records removed");

        let empty = FakeHc::spawn(full_methods(), vec![]);
        let backend = HcBackend::connect(Some(empty.socket.to_str().unwrap())).expect("backend");
        backend
            .workspace_close("hc:t1", false)
            .expect("no-op close");
        assert!(empty.requests_for("terminal.kill").is_empty());
    }

    #[test]
    fn pane_list_scoped_returns_the_root_plus_its_descendants() {
        let chain = vec![
            json!({ "address": "t1" }),
            json!({ "address": "t2", "parent": "t1" }),
            json!({ "address": "t3", "parent": "t2" }),
        ];
        let fake = FakeHc::spawn(full_methods(), chain);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let panes = backend.pane_list(Some("hc:t1")).expect("list");
        assert_eq!(panes.len(), 3);
        let mut ids: Vec<&str> = panes.iter().map(|p| p.pane_id.as_str()).collect();
        ids.sort();
        assert_eq!(ids, ["hc:t1", "hc:t2", "hc:t3"]);
        for pane in &panes {
            assert_eq!(pane.workspace_id, "hc:t1");
        }
    }

    #[test]
    fn pane_list_scoped_returns_that_terminal_and_run_types() {
        let fake = FakeHc::spawn(full_methods(), vec![sample_terminal()]);
        let backend = HcBackend::connect(Some(fake.socket.to_str().unwrap())).expect("backend");
        let panes = backend.pane_list(Some("hc:t7")).expect("list");
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].pane_id, "hc:t7");
        assert_eq!(
            toron_core::mailbox::identity::pane_token_pin(&panes[0]),
            Some("DustySparrow")
        );
        backend.pane_run("hc:t7", "echo hi").expect("run");
        let sends = fake.requests_for("terminal.send");
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0]["text"], "echo hi");
        let _ = backend
            .pane_wait_output("hc:t7", "done", 1000)
            .expect("wait");
        let waits = fake.requests_for("terminal.wait");
        assert_eq!(waits[0]["output"], "done");
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
            std::env::var_os("HC_BIN")
                .unwrap_or_else(|| "/Users/bangedorrunt/.local/bin/hc".into()),
        );
        assert!(hc.is_file(), "hc binary missing at {}", hc.display());

        // the system tempdir is long, /tmp/thcXXXX is not.
        let dir = tempfile::Builder::new()
            .prefix("thc")
            .tempdir_in("/tmp")
            .expect("tmpdir");
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

        let log_dir =
            PathBuf::from(std::env::var_os("HC_LIVE_LOG_DIR").unwrap_or_else(|| {
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

        let stop_server = |hc: &Path,
                           home: &Path,
                           config: &Path,
                           data: &Path,
                           state: &Path,
                           cache: &Path,
                           session: &str| {
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
        let out = start
            .args(["server", "start", "--json"])
            .output()
            .expect("start");
        let _ = writeln!(log, "start_status={}", out.status);
        let _ = writeln!(log, "start_stdout={}", String::from_utf8_lossy(&out.stdout));
        let _ = writeln!(log, "start_stderr={}", String::from_utf8_lossy(&out.stderr));
        assert!(out.status.success(), "server start failed");
        assert!(sock.exists(), "socket missing at {}", sock.display());

        let script = "printf '\\033]7501;state=working:app=live\\033\\\\'; sleep 0.4; printf '\\033]7501;state=blocked:id=child:kind=permission\\033\\\\'; sleep 3; printf '\\033]7501;state=done\\033\\\\'; sleep 0.4; printf '\\033]7501;state=error:app=live\\033\\\\'; sleep 2";
        let mut run = Command::new(&hc);
        apply_env(&mut run);
        let run_out = run
            .args([
                "run", "--bg", "--name", "osc-live", "--", "sh", "-c", script,
            ])
            .output()
            .expect("hc run");
        let _ = writeln!(log, "run_status={}", run_out.status);
        let _ = writeln!(
            log,
            "run_stdout={}",
            String::from_utf8_lossy(&run_out.stdout)
        );
        let _ = writeln!(
            log,
            "run_stderr={}",
            String::from_utf8_lossy(&run_out.stderr)
        );
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

/// The live push shape is `{"method":"event","params":{"type":…,"terminal":…,
/// "data":…}}`; a `missed` notification carries `{"seq":…,"terminals":[…]}`.
/// These decode tests pin that shape against the 0.1.16 source.
#[cfg(test)]
mod event_decode_tests {
    use super::*;
    use std::os::unix::net::UnixStream;

    fn stream(policy: FoldPolicy) -> HcEventStream {
        let (reader, _peer) = UnixStream::pair().expect("socketpair");
        HcEventStream {
            reader: BufReader::new(reader),
            fold_policy: policy,
            pending: VecDeque::new(),
        }
    }

    fn decode(stream: &mut HcEventStream, params: Value) -> BackendEvent {
        let line = json!({"jsonrpc": "2.0", "method": "event", "params": params}).to_string();
        stream.decode_line(&line).expect("decode").expect("event")
    }

    #[test]
    fn status_event_folds_root_and_records() {
        let mut s = stream(FoldPolicy::Attention);
        let event = decode(
            &mut s,
            json!({
                "type": "status",
                "terminal": "t7",
                "data": {
                    "status": {"state": "working"},
                    "records": {"child": {"state": "blocked", "kind": "permission"}},
                    "reported": true
                },
                "seq": 3
            }),
        );
        match event {
            BackendEvent::AgentStatusChanged { pane, status, .. } => {
                assert_eq!(pane.as_str(), "hc:t7");
                assert_eq!(status, AgentStatus::Blocked);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_cleared_status_is_unknown_not_idle() {
        let mut s = stream(FoldPolicy::Attention);
        let event = decode(
            &mut s,
            json!({"type": "status", "terminal": "t7", "data": {"status": null, "records": {}}}),
        );
        match event {
            BackendEvent::AgentStatusChanged { status, .. } => {
                assert_eq!(status, AgentStatus::Unknown);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn turn_event_uses_the_end_state() {
        let mut s = stream(FoldPolicy::Attention);
        let event = decode(
            &mut s,
            json!({"type": "turn", "terminal": "t7", "data": {"turn": 4, "state": "idle"}}),
        );
        match event {
            BackendEvent::AgentStatusChanged { pane, status, .. } => {
                assert_eq!(pane.as_str(), "hc:t7");
                assert_eq!(status, AgentStatus::Idle);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn finished_event_is_a_pane_exit() {
        let mut s = stream(FoldPolicy::Attention);
        let event = decode(
            &mut s,
            json!({"type": "finished", "terminal": "t7", "data": {"exit_code": 0}}),
        );
        assert!(matches!(event, BackendEvent::PaneExited { pane } if pane.as_str() == "hc:t7"));
    }

    #[test]
    fn missed_snapshot_is_queued_after_the_gap_notice() {
        let mut s = stream(FoldPolicy::Attention);
        let line = json!({
            "jsonrpc": "2.0",
            "method": "missed",
            "params": {
                "seq": 9,
                "terminals": [
                    {"address": "t7", "status": {"state": "working"}, "state": "live"},
                    {"address": "t8", "status": {"state": "blocked"}, "state": "live"},
                    {"address": "t9", "state": "finished"}
                ]
            }
        })
        .to_string();
        assert!(matches!(
            s.decode_line(&line).expect("decode"),
            Some(BackendEvent::EventsLost)
        ));
        let first = s.pending.pop_front().expect("first snapshot row");
        assert!(matches!(
            first,
            BackendEvent::AgentStatusChanged { ref pane, status: AgentStatus::Working, .. }
                if pane.as_str() == "hc:t7"
        ));
        let second = s.pending.pop_front().expect("second snapshot row");
        assert!(matches!(
            second,
            BackendEvent::AgentStatusChanged { ref pane, status: AgentStatus::Blocked, .. }
                if pane.as_str() == "hc:t8"
        ));
        let third = s.pending.pop_front().expect("finished row");
        assert!(matches!(third, BackendEvent::PaneExited { ref pane } if pane.as_str() == "hc:t9"));
        assert!(s.pending.is_empty());
    }

    #[test]
    fn labels_and_heartbeat_do_not_change_the_fold() {
        let mut s = stream(FoldPolicy::Attention);
        let labels = decode(
            &mut s,
            json!({"type": "labels", "terminal": "t7", "data": {"labels": {"toron_pin": "X"}}}),
        );
        assert!(matches!(labels, BackendEvent::Raw(_)));
        let line =
            json!({"jsonrpc": "2.0", "method": "heartbeat", "params": {"seq": 4}}).to_string();
        assert!(matches!(
            s.decode_line(&line).expect("decode"),
            Some(BackendEvent::Raw(_))
        ));
    }

    /// Live witness: the real daemon's push shape decodes to a status event.
    /// Ignored by default; run with `-- --ignored` on a host with hc.
    #[test]
    #[ignore = "live hc daemon: needs ~/.local/bin/hc and the default session"]
    fn live_events_decode_a_status_report() {
        let client = Client::connect(None).expect("live hc");
        let (_, mut stream) = client.subscribe_terminals(None).expect("subscribe");
        let script = "printf '\\033]7501;state=blocked:kind=question:app=probe\\033\\\\'; sleep 5";
        let term = client
            .terminal_run(
                &["sh".into(), "-c".into(), script.into()],
                Some("toron-event-probe"),
                None,
            )
            .expect("run probe");
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .expect("timeout");
        let want = prefix_id(&term.address);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut saw = false;
        while std::time::Instant::now() < deadline {
            match stream.read_event() {
                Ok(Some(BackendEvent::AgentStatusChanged { pane, status, .. }))
                    if pane.as_str() == want && status == AgentStatus::Blocked =>
                {
                    saw = true;
                    break;
                }
                Ok(_) => {}
                Err(e) if e.code() == &BackendErrorCode::Timeout => {}
                Err(e) => panic!("live event read: {e}"),
            }
        }
        let _ = client.terminal_kill(&term.address);
        let _ = client.terminal_rm(&term.address);
        assert!(saw, "live status event for {want} was not decoded");
    }

    #[test]
    fn a_send_that_did_not_start_is_stalled() {
        let landing = landing_from_send(&json!({"status": "idle", "turn": 1, "started": false}));
        assert_eq!(landing.status, Some(AgentStatus::Idle));
        assert_eq!(landing.code.as_deref(), Some("agent_prompt_stalled"));

        let landed = landing_from_send(&json!({"status": "working", "turn": 2, "started": true}));
        assert_eq!(landed.status, Some(AgentStatus::Working));
        assert_eq!(landed.code, None);

        let silent = landing_from_send(&json!({"status": null, "turn": 1, "started": null}));
        assert_eq!(silent.status, None);
        assert_eq!(silent.code, None);
    }
}
