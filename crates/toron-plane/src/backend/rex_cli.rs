//! Rex CLI edge. The binary and the wait live here. The status fold does not.
//!
//! `REX_BIN_DIR` is read only in [`Rex::resolve`]. A set directory that has
//! no `rex` file is "rex CLI not installed" and does not fall through to
//! PATH, so a test cannot reach the live server by accident.
//!
//! Four outcomes stay apart: success, [`RexFail::Exited`] (rex returned a
//! status), [`RexFail::Interrupted`] (we killed the child at the bound),
//! and a spawn or JSON failure. A hang is not a rex failure.

use std::io::BufRead;
use std::path::PathBuf;
use std::process::Output;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::backend::rex_schema;
use crate::backend::{BackendError, BackendErrorCode, BackendEvent};
use serde_json::Value;

use toron_core::proc::{ProcError, run_bounded};

/// Wall bound around rex's own `--timeout 10s`, so a rex-side timeout can
/// exit before we kill the process group.
pub const REX_WAIT: Duration = Duration::from_secs(12);

/// One resolved rex binary and the wait this call will allow.
pub struct Rex {
    bin: PathBuf,
    wait: Duration,
}

/// What a rex invocation did besides succeed.
#[derive(Debug)]
pub enum RexFail {
    /// The binary is not there. The backend message contains
    /// "rex CLI not installed".
    NotInstalled { detail: String },
    /// The child outlived the bound and was killed. Not a rex exit code.
    Interrupted { waited: Duration },
    /// Rex ran and returned a non-success status.
    Exited { code: Option<i32>, stderr: String },
    /// Stdout was not the JSON the command claims to print.
    BadOutput { message: String },
    /// The process could not be started, and the binary does exist.
    Spawn { message: String },
    /// We refused before spawn (unknown direction, missing block).
    Rejected { message: String },
    /// The server predates the client: `rex api list -q` omits a method we
    /// depend on. Surfaces loud as `rex_version_skew`, never `rex_failed`.
    VersionSkew { method: &'static str },
}

impl RexFail {
    pub fn into_backend(self) -> BackendError {
        match self {
            Self::NotInstalled { detail } => BackendError::new(
                BackendErrorCode::Io,
                format!("rex CLI not installed ({detail})"),
            ),
            Self::Interrupted { waited } => BackendError::timeout(format!(
                "rex wait interrupted after {}",
                bound_label(waited)
            )),
            Self::Exited { code, stderr } => BackendError::new(
                BackendErrorCode::Other("rex_failed".into()),
                format!("rex failed ({code:?}): {stderr}"),
            ),
            Self::BadOutput { message } => BackendError::new(BackendErrorCode::Malformed, message),
            Self::Spawn { message } => BackendError::new(BackendErrorCode::Io, message),
            Self::Rejected { message } => {
                BackendError::new(BackendErrorCode::Other("rejected".into()), message)
            }
            Self::VersionSkew { method } => BackendError::new(
                BackendErrorCode::Other("rex_version_skew".into()),
                format!(
                    "rex server predates the client: method {method} is missing (rex api list -q)"
                ),
            ),
        }
    }
}

/// One row of `rex ls --json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexSession {
    pub session_id: String,
    pub label: String,
    pub pane_count: u64,
    pub tab_count: u64,
    pub focused: bool,
    pub active_tab_id: String,
    pub cwd: Option<String>,
}

/// One row of `session.list_blocks`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexBlock {
    pub block_id: String,
    pub label: String,
    pub creator_name: String,
    pub window_id: String,
}

/// One block resolved by `target.resolve`. Only the ids and label are needed;
/// the adapter fetches the full block from that session for creator metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexTarget {
    pub session_id: String,
    pub block_id: String,
    pub window_id: String,
    pub label: String,
}

/// First part of a `format` reply. `id` means more remains at `blob_read`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexFormatChunk {
    pub content: String,
    pub id: Option<u64>,
    pub total: Option<u64>,
}

/// One `blob_read` continuation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RexBlobChunk {
    pub id: u64,
    pub data: String,
    pub total: u64,
    pub eof: bool,
}

impl Rex {
    pub fn new(bin: PathBuf, wait: Duration) -> Self {
        Self { bin, wait }
    }

    /// The same resolved binary with a tighter per-call bound. Used by the
    /// roster sweep so one status probe cannot spend the whole command's
    /// wait while other blocks wait behind it.
    pub fn with_wait(&self, wait: Duration) -> Self {
        Self {
            bin: self.bin.clone(),
            wait,
        }
    }

    /// `$REX_BIN_DIR/rex` when that variable is set, otherwise `rex` on PATH.
    pub fn resolve() -> Result<Self, RexFail> {
        match std::env::var("REX_BIN_DIR") {
            Ok(dir) if !dir.trim().is_empty() => {
                let path = PathBuf::from(dir.trim()).join("rex");
                if !path.is_file() {
                    return Err(RexFail::NotInstalled {
                        detail: path.display().to_string(),
                    });
                }
                Ok(Self::new(path, REX_WAIT))
            }
            Ok(_) => Err(RexFail::NotInstalled {
                detail: "REX_BIN_DIR is empty".into(),
            }),
            Err(_) => Ok(Self::new(PathBuf::from("rex"), REX_WAIT)),
        }
    }

    pub fn run(&self, args: &[String]) -> Result<Output, RexFail> {
        match self.run_raw(args) {
            Err(RexFail::Exited { code, stderr }) => match self.skew_method() {
                Some(method) => Err(RexFail::VersionSkew { method }),
                None => Err(RexFail::Exited { code, stderr }),
            },
            other => other,
        }
    }

    fn run_raw(&self, args: &[String]) -> Result<Output, RexFail> {
        let mut cmd = std::process::Command::new(&self.bin);
        cmd.arg("--color")
            .arg("never")
            .arg("--timeout")
            .arg("10s")
            .args(args);
        match run_bounded(&mut cmd, self.wait) {
            Ok(out) if out.status.success() => Ok(out),
            Ok(out) => Err(RexFail::Exited {
                code: out.status.code(),
                stderr: clip(&String::from_utf8_lossy(&out.stderr)),
            }),
            Err(ProcError::Timeout { .. }) => Err(RexFail::Interrupted { waited: self.wait }),
            Err(ProcError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(RexFail::NotInstalled {
                    detail: self.bin.display().to_string(),
                })
            }
            Err(ProcError::Io(e)) => Err(RexFail::Spawn {
                message: format!("rex spawn: {e}"),
            }),
        }
    }

    pub fn json(&self, args: &[String]) -> Result<Value, RexFail> {
        let out = self.run(args)?;
        parse_stdout_json(&out.stdout).map_err(|bad| match self.skew_method() {
            Some(method) => RexFail::VersionSkew { method },
            None => bad,
        })
    }

    pub fn text(&self, args: &[String]) -> Result<String, RexFail> {
        let out = self.run(args)?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }

    /// A long-lived `rex events` child. Drop kills it. The 12s call bound
    /// does not apply: the stream ends when the caller drops it.
    pub fn spawn(&self, args: &[String]) -> Result<std::process::Child, RexFail> {
        let mut cmd = std::process::Command::new(&self.bin);
        cmd.arg("--color")
            .arg("never")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .args(args);
        cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                RexFail::NotInstalled {
                    detail: self.bin.display().to_string(),
                }
            } else {
                RexFail::Spawn {
                    message: format!("rex spawn: {e}"),
                }
            }
        })
    }

    /// E: version-skew probe on the failure path. One `rex api list -q`
    /// per process (raw run: the probe itself must never probe).
    fn skew_method(&self) -> Option<&'static str> {
        let cached = {
            let cache = api_cache();
            let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(text) = guard.as_ref() {
                text.clone()
            } else {
                let text = self
                    .run_raw(&api_list_args())
                    .ok()
                    .filter(|out| out.status.success())
                    .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
                    .unwrap_or_default();
                *guard = Some(text.clone());
                text
            }
        };
        missing_api_method(&cached)
    }
}

/// Newline-delimited `rex events --json`. Dropping the stream kills rex.
pub struct RexEventStream {
    child: Option<std::process::Child>,
    rx: std::sync::mpsc::Receiver<std::io::Result<String>>,
    timeout: Option<Duration>,
    panes: Vec<String>,
}

impl RexEventStream {
    pub fn start(mut child: std::process::Child, panes: Vec<String>) -> Result<Self, RexFail> {
        let stdout = child.stdout.take().ok_or_else(|| RexFail::Spawn {
            message: "rex events has no stdout".into(),
        })?;
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if tx.send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        break;
                    }
                }
            }
        });
        Ok(Self {
            child: Some(child),
            rx,
            timeout: None,
            panes,
        })
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    pub fn next_line(&self) -> Result<Option<String>, RexFail> {
        let received = match self.timeout {
            Some(timeout) => self.rx.recv_timeout(timeout),
            None => self
                .rx
                .recv()
                .map_err(|_| std::sync::mpsc::RecvTimeoutError::Disconnected),
        };
        match received {
            Ok(Ok(line)) => Ok(Some(line)),
            Ok(Err(e)) => Err(RexFail::Spawn {
                message: format!("rex events read: {e}"),
            }),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(RexFail::Interrupted {
                waited: self.timeout.unwrap_or(Duration::ZERO),
            }),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(None),
        }
    }
}

impl Drop for RexEventStream {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl crate::backend::EventStream for RexEventStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        loop {
            let line = match self.next_line() {
                Ok(Some(line)) => line,
                Ok(None) => return Ok(None),
                Err(err) => return Err(err.into_backend()),
            };
            if line.trim().is_empty() {
                continue;
            }
            let event = rex_map::event_from_line(&line);
            if self.panes.is_empty() || pane_wanted(&event, &self.panes) {
                return Ok(Some(event));
            }
        }
    }

    fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<(), BackendError> {
        self.set_timeout(timeout);
        Ok(())
    }
}

fn pane_wanted(event: &BackendEvent, panes: &[String]) -> bool {
    let pane = match event {
        BackendEvent::AgentStatusChanged { pane, .. }
        | BackendEvent::AgentDetected { pane, .. }
        | BackendEvent::PaneExited { pane } => pane.as_str(),
        BackendEvent::EventsLost | BackendEvent::Raw(_) => return true,
    };
    panes.iter().any(|want| want == pane)
}

pub fn ls_args() -> Vec<String> {
    vec!["ls".into(), "--json".into()]
}

pub fn new_args(label: Option<&str>) -> Vec<String> {
    let mut args = vec!["new".into()];
    if let Some(label) = label {
        args.push(label.to_string());
    }
    args.push("--json".into());
    args
}

pub fn list_blocks_args(session: &str) -> Vec<String> {
    vec![
        "api".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "session.list_blocks".into(),
    ]
}

pub fn program_status_args(session: &str, block: &str) -> Vec<String> {
    vec![
        "block".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
        "program_status".into(),
    ]
}

/// `process` block method: the processes running in the terminal and how
/// its program exited. Same call shape as `program_status`.
pub fn process_args(session: &str, block: &str) -> Vec<String> {
    vec![
        "block".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
        "process".into(),
    ]
}

pub fn write_args(session: &str, block: &str, text: &str) -> Vec<String> {
    vec![
        "block".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
        "write".into(),
        serde_json::json!({ "data": text }).to_string(),
    ]
}

/// `block.close` names one block. It does not name `session.close`.
pub fn block_close_args(session: &str, block: &str) -> Vec<String> {
    vec![
        "api".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "block.close".into(),
        serde_json::json!({ "block_id": block }).to_string(),
    ]
}

pub fn capture_args(session: &str, block: &str, format: &str) -> Vec<String> {
    vec![
        "capture".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
        "--format".into(),
        format.into(),
        "--trim".into(),
    ]
}

/// One all-session block lookup. No `-s`: with no caller session the server
/// searches every session (the probe with `-s` is session-scoped).
pub fn target_resolve_args(query: &str) -> Vec<String> {
    vec![
        "api".into(),
        "call".into(),
        "target.resolve".into(),
        serde_json::json!({ "kinds": ["block"], "queries": [query] }).to_string(),
    ]
}

/// Bounded `format`: the reply is one chunk, and `id` names the blob holding
/// the rest. The request body nests the method's own arguments under `args`.
pub fn format_args(session: &str, block: &str, format: &str, max_bytes: u64) -> Vec<String> {
    vec![
        "api".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "com.superlogical.terminal.format".into(),
        serde_json::json!({
            "block_id": block,
            "args": { "format": format, "trim": true, "max_bytes": max_bytes }
        })
        .to_string(),
    ]
}

/// Continue a `format` reply. `offset` is in bytes of the returned content.
pub fn blob_read_args(
    session: &str,
    block: &str,
    id: u64,
    offset: u64,
    max_bytes: u64,
) -> Vec<String> {
    vec![
        "api".into(),
        "call".into(),
        "-s".into(),
        session.into(),
        "com.superlogical.terminal.blob_read".into(),
        serde_json::json!({
            "block_id": block,
            "args": { "id": id, "offset": offset, "max_bytes": max_bytes }
        })
        .to_string(),
    ]
}

pub fn split_args(session: &str, block: &str, direction: &str, cwd: Option<&str>) -> Vec<String> {
    let mut args = vec![
        "split".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
        format!("--split={direction}"),
        "--json".into(),
    ];
    if let Some(cwd) = cwd {
        args.push("-c".into());
        args.push(cwd.to_string());
    }
    args
}

pub fn session_kill_args(session: &str) -> Vec<String> {
    vec!["session".into(), "kill".into(), session.into()]
}

/// `rex run`: one labelled keep-open block in a new window. Label at creation
/// (no create-then-rename window) and `--keep-open` so the block survives
/// the agent exit for `block capture`. Consume `.block_ids[0]` as binding.
/// `run` takes no anchor: later blocks ride `split_labelled_args` anchored
/// to the previous block instead of the focused-block race.
pub fn run_args(session: &str, label: &str, cwd: Option<&str>, command: &[String]) -> Vec<String> {
    let mut args = vec!["run".into(), "-s".into(), session.into()];
    if !label.is_empty() {
        args.push("--label".into());
        args.push(label.into());
    }
    args.push("--keep-open".into());
    args.push("--json".into());
    if let Some(dir) = cwd.filter(|s| !s.is_empty()) {
        args.push("-c".into());
        args.push(dir.into());
    }
    args.push("--".into());
    args.extend(command.iter().cloned());
    args
}

/// `rex split`: one labelled keep-open block anchored to `anchor` (never the
/// focused block). `direction` is already a rex word (`below`, not `down`).
/// Same label/keep-open/command contract as `run_args`.
pub fn split_labelled_args(
    session: &str,
    anchor: &str,
    direction: &str,
    label: &str,
    cwd: Option<&str>,
    command: &[String],
) -> Vec<String> {
    let mut args = vec![
        "split".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        anchor.into(),
        format!("--split={direction}"),
    ];
    if !label.is_empty() {
        args.push("--label".into());
        args.push(label.into());
    }
    args.push("--keep-open".into());
    args.push("--json".into());
    if let Some(dir) = cwd.filter(|s| !s.is_empty()) {
        args.push("-c".into());
        args.push(dir.into());
    }
    args.push("--".into());
    args.extend(command.iter().cloned());
    args
}

pub fn rename_args(session: &str, block: &str, label: &str) -> Vec<String> {
    vec![
        "block".into(),
        "rename".into(),
        "-s".into(),
        session.into(),
        block.into(),
        label.into(),
    ]
}

pub fn send_key_args(session: &str, block: &str, keys: &[&str]) -> Vec<String> {
    let mut args = vec![
        "block".into(),
        "send-key".into(),
        "-s".into(),
        session.into(),
        "-b".into(),
        block.into(),
    ];
    args.extend(keys.iter().map(|key| (*key).to_string()));
    args
}

pub fn events_args(session: Option<&str>) -> Vec<String> {
    let mut args = vec!["events".into(), "--json".into()];
    if let Some(session) = session {
        args.push("-s".into());
        args.push(session.into());
    }
    args
}

/// E: one cached `rex api list -q` per process. The first failure path pays
/// for it; every later failure reads the cache.
pub fn api_list_args() -> Vec<String> {
    vec!["api".into(), "list".into(), "-q".into()]
}

/// E: every method a rex call in this file may depend on. Each group passes
/// when ANY member is present (`run` predates `session.new_split`).
///
/// bd-ne16: the first members were CLI VERBS
/// (`send-key`, `events`) that are not API methods — api_revision 7 exposes
/// `client.key` and `block.event` — so the probe reported every ordinary
/// rex failure as "server predates the client". Historical names stay in the
/// group for older servers, but each new name must be the REAL method the
/// verb calls.
const REQUIRED_API_GROUPS: &[&[&str]] = &[
    &["run", "session.new_split"],
    &["session.set_block_label"],
    &["block.close"],
    &["write", "com.superlogical.terminal.write"],
    &["send-key", "client.key"],
    &["events", "block.event"],
    &["session.list"],
    &["session.kill"],
    &["target.resolve"],
    &["com.superlogical.terminal.format"],
    &["com.superlogical.terminal.blob_read"],
];

/// E: first required method missing from `rex api list -q` output, if any.
/// Empty output is unknown (a dead server), never skew.
pub fn missing_api_method(output: &str) -> Option<&'static str> {
    if output.trim().is_empty() {
        return None;
    }
    REQUIRED_API_GROUPS
        .iter()
        .find(|group| !group.iter().any(|m| output.contains(m)))
        .map(|group| group[0])
}

static API_LIST_CACHE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

fn api_cache() -> &'static Mutex<Option<String>> {
    API_LIST_CACHE.get_or_init(|| Mutex::new(None))
}

/// `rex ls --json` is either `{"sessions":[...]}` or a bare array.
pub fn parse_sessions(value: &Value) -> Result<Vec<RexSession>, RexFail> {
    let rows = match value {
        Value::Array(rows) => rows,
        Value::Object(map) => map
            .get("sessions")
            .and_then(Value::as_array)
            .ok_or_else(|| RexFail::BadOutput {
                message: "rex ls JSON missing sessions".into(),
            })?,
        _ => {
            return Err(RexFail::BadOutput {
                message: "rex ls JSON is not a session list".into(),
            });
        }
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(session_row(row)?);
    }
    Ok(out)
}

/// `rex new --json`. Accepts a bare session object or `{"session": ...}`.
pub fn parse_created_session(
    value: &Value,
    label_arg: Option<&str>,
) -> Result<RexSession, RexFail> {
    let row = value.get("session").unwrap_or(value);
    if row.get("session_id").is_none() && row.get("id").is_none() {
        return Err(RexFail::BadOutput {
            message: "rex new JSON missing session_id".into(),
        });
    }
    let mut session = session_row(row)?;
    if session.label == session.session_id
        && let Some(label) = label_arg.filter(|s| !s.is_empty())
    {
        session.label = label.to_string();
    }
    let windows = value
        .get("initial_windows")
        .or_else(|| row.get("initial_windows"))
        .and_then(Value::as_array);
    if let Some(windows) = windows {
        session.tab_count = windows.len() as u64;
        session.pane_count = windows
            .iter()
            .map(|w| {
                w.get("block_ids")
                    .and_then(Value::as_array)
                    .map(|ids| ids.len() as u64)
                    .unwrap_or(0)
            })
            .sum();
        if session.active_tab_id.is_empty() {
            session.active_tab_id = windows
                .first()
                .and_then(|w| w.get("window_id"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
        }
    }
    Ok(session)
}

pub fn parse_blocks(value: &Value) -> Result<Vec<RexBlock>, RexFail> {
    let rows = value
        .get("blocks")
        .and_then(Value::as_array)
        .ok_or_else(|| RexFail::BadOutput {
            message: "rex list_blocks JSON missing blocks".into(),
        })?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let block_id = field(row, &["block_id", "id"]).ok_or_else(|| RexFail::BadOutput {
            message: "rex block missing block_id".into(),
        })?;
        out.push(RexBlock {
            block_id,
            label: field(row, &["label", "title"]).unwrap_or_default(),
            creator_name: field(row, &["creator_name", "creator"]).unwrap_or_default(),
            window_id: field(row, &["window_id"]).unwrap_or_default(),
        });
    }
    Ok(out)
}

pub fn parse_created_block_id(value: &Value) -> Result<String, RexFail> {
    if let Some(id) = field(value, &["block_id"]) {
        return Ok(id);
    }
    if let Some(block) = value.get("block")
        && let Some(id) = field(block, &["block_id", "id"])
    {
        return Ok(id);
    }
    if let Some(id) = value
        .get("block_ids")
        .and_then(Value::as_array)
        .and_then(|ids| ids.first())
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    {
        return Ok(id.to_string());
    }
    Err(RexFail::BadOutput {
        message: "rex split JSON missing block_id".into(),
    })
}

fn schema_check(method: &str, value: &Value) -> Result<(), RexFail> {
    rex_schema::validate_response(method, value).map_err(|err| RexFail::BadOutput {
        message: err.to_string(),
    })
}

/// Parse one `target.resolve` reply. Only `resolved` yields a target; both
/// `not_found` and unknown statuses become the same typed refusal.
pub fn parse_target_resolve(value: &Value, query: &str) -> Result<RexTarget, RexFail> {
    schema_check("target.resolve", value)?;
    let results = value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| RexFail::BadOutput {
            message: "rex target.resolve JSON missing results".into(),
        })?;
    let result = results.first().ok_or_else(|| RexFail::BadOutput {
        message: "rex target.resolve returned no result".into(),
    })?;
    match result.get("status").and_then(Value::as_str) {
        Some("resolved") => {
            let target = result.get("target").ok_or_else(|| RexFail::BadOutput {
                message: "rex target.resolve resolved without a target".into(),
            })?;
            if target.get("kind").and_then(Value::as_str) != Some("block") {
                return Err(RexFail::Rejected {
                    message: format!("rex query {query} did not resolve to a block"),
                });
            }
            let session_id = field(target, &["session_id"]).ok_or_else(|| RexFail::BadOutput {
                message: "rex target.resolve target missing session_id".into(),
            })?;
            let block_id = field(target, &["block_id"]).ok_or_else(|| RexFail::BadOutput {
                message: "rex target.resolve target missing block_id".into(),
            })?;
            Ok(RexTarget {
                session_id,
                block_id,
                window_id: field(target, &["window_id"]).unwrap_or_default(),
                label: target
                    .get("label")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
            })
        }
        Some("ambiguous") => {
            let count = result
                .get("candidates")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            Err(RexFail::Rejected {
                message: format!("rex block {query} is ambiguous ({count} candidates)"),
            })
        }
        _ => Err(RexFail::Rejected {
            message: format!("rex block {query} not found"),
        }),
    }
}

pub fn parse_format_chunk(value: &Value) -> Result<RexFormatChunk, RexFail> {
    schema_check("com.superlogical.terminal.format", value)?;
    let content = value
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| RexFail::BadOutput {
            message: "rex format JSON missing content".into(),
        })?
        .to_string();
    let id = match value.get("id") {
        Some(raw) => Some(raw.as_u64().ok_or_else(|| RexFail::BadOutput {
            message: "rex format id is not an integer".into(),
        })?),
        None => None,
    };
    let total = match value.get("total") {
        Some(raw) => Some(raw.as_u64().ok_or_else(|| RexFail::BadOutput {
            message: "rex format total is not an integer".into(),
        })?),
        None => None,
    };
    Ok(RexFormatChunk { content, id, total })
}

pub fn parse_blob_chunk(value: &Value) -> Result<RexBlobChunk, RexFail> {
    schema_check("com.superlogical.terminal.blob_read", value)?;
    let number = |key: &str| -> Result<u64, RexFail> {
        value
            .get(key)
            .and_then(Value::as_u64)
            .ok_or_else(|| RexFail::BadOutput {
                message: format!("rex blob_read missing integer {key}"),
            })
    };
    Ok(RexBlobChunk {
        id: number("id")?,
        data: value
            .get("data")
            .and_then(Value::as_str)
            .ok_or_else(|| RexFail::BadOutput {
                message: "rex blob_read missing data".into(),
            })?
            .to_string(),
        total: number("total")?,
        eof: value
            .get("eof")
            .and_then(Value::as_bool)
            .ok_or_else(|| RexFail::BadOutput {
                message: "rex blob_read missing eof".into(),
            })?,
    })
}

/// Largest single `format`/`blob_read` response the server accepts.
pub const FORMAT_MAX_BYTES: u64 = 960 * 1024;
/// Enough lines for a tail read without fetching the whole scrollback.
pub const TAIL_READ_BYTES: u64 = 64 * 1024;

pub fn read_format(
    rex: &Rex,
    session: &str,
    block: &str,
    format: &str,
    max_bytes: u64,
) -> Result<RexFormatChunk, RexFail> {
    let value = rex.json(&format_args(session, block, format, max_bytes))?;
    parse_format_chunk(&value)
}

pub fn read_blob(
    rex: &Rex,
    session: &str,
    block: &str,
    id: u64,
    offset: u64,
    max_bytes: u64,
) -> Result<RexBlobChunk, RexFail> {
    let value = rex.json(&blob_read_args(session, block, id, offset, max_bytes))?;
    parse_blob_chunk(&value)
}

/// Read through every blob chunk. Used when the caller asked for the whole
/// screen (`lines == 0`).
pub fn read_whole_format(
    rex: &Rex,
    session: &str,
    block: &str,
    format: &str,
) -> Result<String, RexFail> {
    let first = read_format(rex, session, block, format, FORMAT_MAX_BYTES)?;
    let mut out = first.content;
    let mut next = first.id;
    while let Some(id) = next {
        let chunk = read_blob(rex, session, block, id, out.len() as u64, FORMAT_MAX_BYTES)?;
        if chunk.data.is_empty() {
            return Err(RexFail::BadOutput {
                message: "rex blob_read made no progress".into(),
            });
        }
        out.push_str(&chunk.data);
        next = if chunk.eof { None } else { Some(chunk.id) };
    }
    Ok(out)
}

/// Read only the end of a large output. The first reply tells us `total`; seek
/// there with `blob_read`, then continue until `eof`. Falls back to the first
/// reply when it was whole.
pub fn read_tail_format(
    rex: &Rex,
    session: &str,
    block: &str,
    format: &str,
    lines: u32,
) -> Result<String, RexFail> {
    let first = read_format(rex, session, block, format, TAIL_READ_BYTES)?;
    let Some(id) = first.id else {
        return Ok(rex_map::tail_lines(&first.content, lines));
    };
    let total = first.total.ok_or_else(|| RexFail::BadOutput {
        message: "rex format returned id without total".into(),
    })?;
    let start = total.saturating_sub(TAIL_READ_BYTES);
    let mut chunk = read_blob(rex, session, block, id, start, TAIL_READ_BYTES)?;
    let mut out = chunk.data.clone();
    while !chunk.eof {
        let next = read_blob(
            rex,
            session,
            block,
            chunk.id,
            start.saturating_add(out.len() as u64),
            TAIL_READ_BYTES,
        )?;
        if next.data.is_empty() {
            return Err(RexFail::BadOutput {
                message: "rex blob_read made no progress".into(),
            });
        }
        out.push_str(&next.data);
        chunk = next;
    }
    Ok(rex_map::tail_lines(&out, lines))
}

fn session_row(row: &Value) -> Result<RexSession, RexFail> {
    let session_id = field(row, &["session_id", "id"]).ok_or_else(|| RexFail::BadOutput {
        message: "rex session missing session_id".into(),
    })?;
    let label = field(row, &["label"]).unwrap_or_else(|| session_id.clone());
    Ok(RexSession {
        session_id,
        label,
        pane_count: row.get("pane_count").and_then(Value::as_u64).unwrap_or(0),
        tab_count: row
            .get("tab_count")
            .or_else(|| row.get("window_count"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        focused: row.get("focused").and_then(Value::as_bool).unwrap_or(false),
        active_tab_id: row
            .get("active_window_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        cwd: row.get("cwd").and_then(Value::as_str).map(str::to_string),
    })
}

fn field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        value
            .get(*key)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn parse_stdout_json(bytes: &[u8]) -> Result<Value, RexFail> {
    let text = String::from_utf8(bytes.to_vec()).map_err(|e| RexFail::BadOutput {
        message: format!("rex stdout is not utf-8: {e}"),
    })?;
    let text = text.trim();
    if text.is_empty() {
        return Err(RexFail::BadOutput {
            message: "rex returned empty stdout".into(),
        });
    }
    serde_json::from_str(text).map_err(|e| RexFail::BadOutput {
        message: format!("rex JSON: {e}"),
    })
}

fn clip(text: &str) -> String {
    let mut clipped: String = text.chars().take(400).collect();
    if text.chars().count() > 400 {
        clipped.push('…');
    }
    clipped
}

fn bound_label(waited: Duration) -> String {
    if waited.as_secs() >= 1 && waited.subsec_nanos() == 0 {
        format!("{}s", waited.as_secs())
    } else {
        format!("{}ms", waited.as_millis())
    }
}

/// Pure status fold. It does not read the environment or spawn rex.
///
/// The status fold lives inside `rex_cli.rs` so the CLI edge owns parsing and
/// folding together; there is no second copy of this module.
pub mod rex_map {
    use crate::backend::{AgentStatus, BackendEvent, PaneId};
    use crate::osc7501::{BlockKind, Board, HcStatusView, ProgramState, Report, parse_report};
    use serde_json::Value;

    use crate::backend::rex::fold_status;

    /// Map one `program_status` payload to the trait status.
    ///
    /// OSC `idle` is at a prompt. OSC `error` is Unknown, never Done.
    /// The root record wins. With no root, any blocked child is Blocked.
    ///
    /// ponytail: no-root, non-blocked children fold newest-first.
    pub fn status_from_program_status(value: &Value) -> AgentStatus {
        let (board, order) = board_of(value);
        fold_status(&board, &order)
    }

    /// The board behind one payload, shared by the records fold and the
    /// process fallback so both read one parse.
    pub fn board_of(value: &Value) -> (Board, Vec<String>) {
        let mut board = Board::default();
        let mut order: Vec<String> = Vec::new();
        for record in record_slice(value) {
            apply_record(&mut board, &mut order, record);
        }
        (board, order)
    }

    /// Labels for the report that decided the Attention fold: blocked child,
    /// else working, error, done, idle. `program_status` is an object, not a
    /// flat status word, so optional kind/progress/app/title/msg survive
    /// without inventing empty keys. Empty board means no label.
    pub fn state_labels_from_board(board: &Board) -> serde_json::Map<String, Value> {
        let mut labels = serde_json::Map::new();
        let Some((id, report)) = deciding_report(board) else {
            return labels;
        };
        let mut row = serde_json::Map::new();
        row.insert("id".into(), Value::String(id.to_string()));
        row.insert(
            "state".into(),
            Value::String(state_word(report.state).to_string()),
        );
        if let Some(kind) = report.kind {
            row.insert("kind".into(), Value::String(kind_word(kind).to_string()));
        }
        if let Some(progress) = report.progress {
            row.insert("progress".into(), Value::from(progress));
        }
        if let Some(app) = report.app.as_ref() {
            row.insert("app".into(), Value::String(app.clone()));
        }
        if let Some(title) = report.title.as_ref() {
            row.insert("title".into(), Value::String(title.clone()));
        }
        if let Some(msg) = report.msg.as_ref() {
            row.insert("msg".into(), Value::String(msg.clone()));
        }
        labels.insert("program_status".into(), Value::Object(row));
        labels
    }

    fn deciding_report(board: &Board) -> Option<(&str, &Report)> {
        let mut working: Option<(&str, &Report)> = None;
        let mut error: Option<(&str, &Report)> = None;
        let mut done: Option<(&str, &Report)> = None;
        let mut idle: Option<(&str, &Report)> = None;
        for (id, report) in board.iter() {
            match report.state {
                ProgramState::Blocked => return Some((id, report)),
                ProgramState::Working => working = working.or(Some((id, report))),
                ProgramState::Error => error = error.or(Some((id, report))),
                ProgramState::Done => done = done.or(Some((id, report))),
                ProgramState::Idle => idle = idle.or(Some((id, report))),
                ProgramState::Clear => {}
            }
        }
        working.or(error).or(done).or(idle)
    }

    fn state_word(state: ProgramState) -> &'static str {
        match state {
            ProgramState::Idle => "idle",
            ProgramState::Working => "working",
            ProgramState::Done => "done",
            ProgramState::Blocked => "blocked",
            ProgramState::Error => "error",
            ProgramState::Clear => "clear",
        }
    }

    fn kind_word(kind: BlockKind) -> &'static str {
        match kind {
            BlockKind::Permission => "permission",
            BlockKind::Question => "question",
            BlockKind::Auth => "auth",
        }
    }

    /// What the terminal's foreground process looks like. Empty when the
    /// shape is missing or odd — the fold reads that as Unknown.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub struct ProcessView {
        pub foreground: String,
        pub cwd: Option<String>,
    }

    /// Shells: a foreground shell is at a prompt (Idle). Anything else that
    /// is not a known agent is Unknown, never inferred.
    const SHELLS: [&str; 5] = ["sh", "bash", "zsh", "fish", "dash"];

    /// Parse one `process` block-method payload into the foreground view.
    pub fn parse_process_view(value: &Value) -> ProcessView {
        let foreground = value.get("foreground");
        ProcessView {
            foreground: foreground
                .and_then(|f| f.get("name"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            cwd: foreground
                .and_then(|f| f.get("cwd"))
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }

    /// Liveness without cooperation: a known agent binary in front is
    /// Working, a shell is at a prompt. Explicit records always win over
    /// this — callers only reach it on an empty board.
    pub fn process_status(view: &ProcessView) -> AgentStatus {
        match toron_core::harness::Harness::from_kind(&view.foreground) {
            toron_core::harness::Harness::Other => {
                if SHELLS.contains(&view.foreground.as_str()) {
                    AgentStatus::Idle
                } else {
                    AgentStatus::Unknown
                }
            }
            _ => AgentStatus::Working,
        }
    }

    /// How long a `working` record must sit unchanged before the screen is
    /// allowed to overrule it (bd-0uzrq.10). One turn's tail can look idle
    /// between two tool calls; two minutes of silence cannot be a turn.
    pub const STALE_WORKING_SECS: u64 = 120;

    /// How long the idle footer must stay IDENTICAL before it counts as a turn
    /// that ended (bd-0uzrq.14). opencode re-renders its idle footer in the gap
    /// between two tool calls, so one capture that looks like a prompt is not
    /// evidence — the live 11:12:37Z ring was typed into a running turn whose
    /// commits bracket the drain.
    pub const PROMPT_STABLE_SECS: i64 = 10;

    /// The ONE idle detector, shared by every consumer of a rex status (the
    /// captain watch and the dispatcher both reach it through `block_state`).
    ///
    /// OSC first: `idle` is at a prompt, `blocked` is a blocked child, `error`
    /// is Unknown. But `working` is not trustworthy on its own — opencode
    /// emits `state=working` at turn start and does NOT emit `idle` after a
    /// turn that ran a background shell, so the record sits at `working`
    /// forever while the pane is at its prompt. The second signal is the
    /// screen: when a stale `working` record is contradicted by opencode's
    /// idle footer, believe the screen — but only once that footer has been
    /// STABLE, because a single capture cannot tell a tool-call gap from a
    /// finished turn (bd-0uzrq.14).
    ///
    /// `prev` is the last sample this detector returned for the pin. The
    /// sample carries a digest of the capture and the instant it was first
    /// seen; an identical digest on a later capture keeps that instant, so the
    /// caller only has to remember the last returned sample.
    #[must_use]
    pub fn status_with_stable_screen(
        status: AgentStatus,
        record_age_secs: Option<u64>,
        prev: Option<PromptSample>,
        screen: &str,
        now_unix: i64,
        min_stable_secs: i64,
    ) -> (AgentStatus, Option<PromptSample>) {
        let stale = record_age_secs.is_some_and(|age| age >= STALE_WORKING_SECS);
        if status != AgentStatus::Working || !stale || !screen_shows_a_prompt(screen) {
            return (status, None);
        }
        let digest = screen_prompt_digest(screen);
        let sample = match prev {
            Some(p) if p.digest == digest => p,
            _ => PromptSample {
                digest,
                first_seen_unix: now_unix,
            },
        };
        let stable = now_unix.saturating_sub(sample.first_seen_unix) >= min_stable_secs;
        let verdict = if stable {
            AgentStatus::Idle
        } else {
            AgentStatus::Working
        };
        (verdict, Some(sample))
    }

    /// One retained sample of an at-prompt capture: what the screen said and
    /// when this detector first saw it say that.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PromptSample {
        pub digest: u64,
        pub first_seen_unix: i64,
    }

    /// Digest of a capture, for "is this the same screen?". FNV-1a: stable
    /// across runs and cheap on a pane-sized string.
    #[must_use]
    pub fn screen_prompt_digest(screen: &str) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in screen.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }

    /// Does this screen show a pane AT A PROMPT?
    ///
    /// The footer's SHAPE decides, never its first character: opencode elides
    /// BOTH ends of the context row on a narrow pane, so the live capture reads
    /// `…/flywheel:adr-0021-stage-…  767.9K (77%)` and any check on the leading
    /// `/` misses it (bd-0uzrq.10 live leg, 09:28).
    ///
    /// At a prompt the footer's lower line is the context row: it ends in the
    /// window percentage `(NN%)`. During a turn that row is replaced by the
    /// progress row, which carries `N shell` and the truncated interrupt hint
    /// (`es...upt   1 shell  770.3K (77%)`) or the `↓` jump marker. So: a
    /// percentage ending, minus the progress markers.
    #[must_use]
    pub fn screen_shows_a_prompt(screen: &str) -> bool {
        screen
            .lines()
            .rev()
            .filter(|line| !line.trim().is_empty())
            .take(3)
            .any(is_context_row)
    }

    /// One footer line: a context row ends in `(NN%)`; a progress row carries
    /// the shell counter, the interrupt hint, or the jump marker.
    fn is_context_row(line: &str) -> bool {
        let row = line.trim_end();
        let Some(open) = row.rfind('(') else {
            return false;
        };
        let tail = &row[open + 1..];
        let Some(pct) = tail.strip_suffix(')') else {
            return false;
        };
        let Some(digits) = pct.strip_suffix('%') else {
            return false;
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        let lower = row.to_ascii_lowercase();
        !(lower.contains("shell") || lower.contains("esc") || row.contains('\u{2193}'))
    }

    /// Age of the newest `program_status` record, in seconds, from its
    /// `updated_at` (RFC3339 with an offset, as rex emits it). `None` when no
    /// record carries a parseable stamp — an unreadable age never overrules
    /// the fold.
    #[must_use]
    pub fn newest_record_age_secs(records: &Value, now_epoch_secs: i64) -> Option<u64> {
        let mut newest: Option<i64> = None;
        for record in record_slice(records) {
            let Some(stamp) = record.get("updated_at").and_then(Value::as_str) else {
                continue;
            };
            let Some(epoch) = parse_rfc3339_epoch(stamp) else {
                continue;
            };
            newest = Some(newest.map_or(epoch, |cur: i64| cur.max(epoch)));
        }
        newest.map(|epoch| now_epoch_secs.saturating_sub(epoch).max(0) as u64)
    }

    /// Records first, process fallback: an empty board (no program ever
    /// reported — the opencode steady state) folds the foreground process;
    /// any record at all keeps the records fold, including explicit Unknown.
    pub fn status_for_block(board: &Board, order: &[String], view: &ProcessView) -> AgentStatus {
        if board.is_empty() {
            process_status(view)
        } else {
            fold_status(board, order)
        }
    }

    /// Rex `--split` directions, plus the hc words the trait already passes.
    pub fn split_direction(raw: &str) -> Option<&'static str> {
        Some(match raw {
            "left" => "left",
            "right" | "horizontal" => "right",
            "above" | "up" => "above",
            "below" | "down" | "vertical" => "below",
            _ => return None,
        })
    }

    /// `rex capture` has no line cap. `0` means the whole screen.
    pub fn tail_lines(text: &str, lines: u32) -> String {
        if lines == 0 {
            return text.to_string();
        }
        let all: Vec<&str> = text.lines().collect();
        let start = all.len().saturating_sub(lines as usize);
        let mut out = all[start..].join("\n");
        if text.ends_with('\n') && !out.is_empty() {
            out.push('\n');
        }
        out
    }

    /// `rex capture --format`. Unknown sources stay text. No tmux flags.
    pub fn capture_format(source: &str) -> &'static str {
        match source {
            "html" => "html",
            "vt" => "vt",
            _ => "text",
        }
    }

    /// Epoch seconds from an RFC3339 stamp with a numeric offset, which is the
    /// shape rex emits (`2026-11-15T18:56:36.144457+11:00`). Fractional seconds
    /// are dropped: the caller compares whole seconds.
    fn parse_rfc3339_epoch(stamp: &str) -> Option<i64> {
        let bytes = stamp.as_bytes();
        if bytes.len() < 19 {
            return None;
        }
        let num = |range: std::ops::Range<usize>| -> Option<i64> {
            stamp.get(range)?.parse::<i64>().ok()
        };
        let (year, month, day) = (num(0..4)?, num(5..7)?, num(8..10)?);
        let (hour, minute, second) = (num(11..13)?, num(14..16)?, num(17..19)?);
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        let rest = stamp.get(19..)?;
        let offset_secs = if rest.starts_with('Z') || rest.starts_with('z') {
            0
        } else {
            let plus = rest.find(['+', '-'])?;
            let sign: i64 = if rest.as_bytes()[plus] == b'-' { -1 } else { 1 };
            let (oh, om) = rest.get(plus + 1..)?.split_once(':')?;
            sign * (oh.parse::<i64>().ok()? * 3600 + om.parse::<i64>().ok()? * 60)
        };
        // Days since the Unix epoch (Howard Hinnant's civil_from_days, reversed).
        let y = year - i64::from(month <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = y - era * 400;
        let mp = (month + 9) % 12;
        let doy = (153 * mp + 2) / 5 + day - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let days = era * 146_097 + doe - 719_468;
        Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset_secs)
    }

    fn record_slice(value: &Value) -> Vec<&Value> {
        if let Some(rows) = value.get("records").and_then(Value::as_array) {
            return rows.iter().collect();
        }
        match value {
            Value::Array(rows) => rows.iter().collect(),
            Value::String(_) => vec![value],
            Value::Object(map) if map.contains_key("state") || map.contains_key("osc") => {
                vec![value]
            }
            _ => Vec::new(),
        }
    }

    /// Apply one `program_status` record to the board. Raw strings go to
    /// the parser as-is; objects naming a raw report key (`osc`, `raw`,
    /// `report`, `sequence`) apply the first report the parser accepts;
    /// otherwise a `state` word renders through the same grammar, so an id
    /// outside the OSC id grammar drops the record. Anything `parse_report`
    /// refuses is not a record.
    fn apply_record(board: &mut Board, order: &mut Vec<String>, value: &Value) {
        match value {
            Value::String(raw) => {
                if let Some(report) = parse_report(raw) {
                    order.push(report.id.clone().unwrap_or_default());
                    board.apply(raw);
                }
            }
            Value::Object(map) => {
                for key in ["osc", "raw", "report", "sequence"] {
                    if let Some(Value::String(raw)) = map.get(key)
                        && let Some(report) = parse_report(raw)
                    {
                        order.push(report.id.clone().unwrap_or_default());
                        board.apply(raw);
                        return;
                    }
                }
                let Some(state) = map.get("state").and_then(Value::as_str) else {
                    return;
                };
                if word_state(state).is_none() {
                    return;
                }
                let id = map.get("id").and_then(Value::as_str).unwrap_or("");
                if state == "clear" {
                    let raw = if id.is_empty() {
                        format!("\u{1b}]7501;state={state}\u{1b}\\")
                    } else {
                        format!("\u{1b}]7501;state={state}:id={id}\u{1b}\\")
                    };
                    if let Some(report) = parse_report(&raw) {
                        order.push(report.id.clone().unwrap_or_default());
                        board.apply(&raw);
                    }
                    return;
                }
                // Object records carry the same fields as an OSC report.
                // Rebuilding a raw sequence here would drop them (the old
                // path kept only state/id); insert the structured view so a
                // blocked child's kind/msg reach state_labels.
                let field = |key: &str| map.get(key).and_then(Value::as_str);
                let progress = map
                    .get("progress")
                    .and_then(Value::as_u64)
                    .and_then(|n| u8::try_from(n).ok());
                board.insert_hc(
                    id,
                    &HcStatusView {
                        state,
                        kind: field("kind"),
                        progress,
                        app: field("app"),
                        title: field("title"),
                        msg: field("msg"),
                    },
                );
                order.push(id.to_string());
            }
            _ => {}
        }
    }

    fn word_state(raw: &str) -> Option<ProgramState> {
        Some(match raw {
            "idle" => ProgramState::Idle,
            "working" => ProgramState::Working,
            "done" => ProgramState::Done,
            "blocked" => ProgramState::Blocked,
            "error" => ProgramState::Error,
            "clear" => ProgramState::Clear,
            _ => return None,
        })
    }

    /// One `rex events --json` line. Unknown shapes stay `Raw`.
    pub fn event_from_line(line: &str) -> BackendEvent {
        let line = line.trim();
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            return BackendEvent::Raw(line.to_string());
        };
        let inner = value.pointer("/payload/event").unwrap_or(&value);
        let kind = inner.get("type").and_then(Value::as_str).unwrap_or("");
        let payload = inner.get("payload").cloned().unwrap_or(Value::Null);
        let pane = payload
            .get("block_id")
            .or_else(|| payload.get("id"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if kind.ends_with("program_status_changed") {
            let record = payload.get("record").cloned().unwrap_or(Value::Null);
            let status = status_from_program_status(&json_records(&record));
            return BackendEvent::AgentStatusChanged {
                pane: PaneId::new(pane),
                agent: payload
                    .get("label")
                    .and_then(Value::as_str)
                    .map(str::to_string),
                status,
            };
        }
        if kind.ends_with("block_closed") {
            return BackendEvent::PaneExited {
                pane: PaneId::new(pane),
            };
        }
        BackendEvent::Raw(line.to_string())
    }

    /// D: the headless one-shot outcome. `terminal.child_exited` carries the
    /// block and its exit code; anything else is not an exit (`None` is
    /// unknown, never success).
    ///
    /// Two shapes are in the wild, and both must parse — a real
    /// `rex events --json` line nests the name and the fields one level deeper
    /// than the synthetic fixture does:
    ///
    /// ```text
    /// real:  {"type":"session_event","payload":{"event":{"type":"block_event",
    ///         "payload":{"block_id":…,"name":"child_exited",
    ///         "payload":{"exit_code":7}}}}}
    /// test:  {"type":"terminal.child_exited","payload":{"block_id":…,"exit_code":0}}
    /// ```
    ///
    /// Reading only `type` matched the fixture and never the stream, so the
    /// live one-shot probe degraded to `unknown` while the tests stayed green.
    pub fn child_exit_from_line(line: &str) -> Option<(String, i32)> {
        let value: Value = serde_json::from_str(line.trim()).ok()?;
        let inner = value.pointer("/payload/event").unwrap_or(&value);
        let name = inner
            .pointer("/payload/name")
            .and_then(Value::as_str)
            .or_else(|| inner.get("name").and_then(Value::as_str))
            .or_else(|| inner.get("type").and_then(Value::as_str))
            .unwrap_or("");
        if !name.ends_with("child_exited") {
            return None;
        }
        let payload = inner.get("payload").unwrap_or(&Value::Null);
        let fields = payload.get("payload").unwrap_or(payload);
        let field = |key: &str| -> Option<&Value> { fields.get(key).or_else(|| payload.get(key)) };
        let block = field("block_id")
            .or_else(|| field("id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())?
            .to_string();
        let code = field("exit_code")
            .or_else(|| field("code"))
            .and_then(Value::as_i64)? as i32;
        Some((block, code))
    }

    fn json_records(record: &Value) -> Value {
        if record.get("records").is_some() {
            return record.clone();
        }
        serde_json::json!({ "records": [record] })
    }
}

#[cfg(test)]
mod wire_contract_tests {
    use super::rex_map::{
        PROMPT_STABLE_SECS, STALE_WORKING_SECS, newest_record_age_secs, screen_shows_a_prompt,
        status_from_program_status, status_with_stable_screen,
    };
    use super::*;
    use crate::backend::AgentStatus;
    use serde_json::json;

    fn body(args: &[String]) -> Value {
        serde_json::from_str(args.last().expect("call carries a JSON body"))
            .expect("call body is JSON")
    }

    #[test]
    fn target_resolve_searches_all_sessions_and_asks_for_blocks() {
        let args = target_resolve_args("DustySparrow");
        assert_eq!(args[0], "api");
        assert_eq!(args[2], "target.resolve");
        assert!(
            !args.iter().any(|arg| arg == "-s"),
            "a caller session would scope the search and miss a cross-session block"
        );
        let body = body(&args);
        assert_eq!(body["kinds"], json!(["block"]));
        assert_eq!(body["queries"], json!(["DustySparrow"]));
    }

    #[test]
    fn format_args_bound_the_reply_and_nest_method_arguments() {
        let args = format_args("s1", "b1", "text", 4096);
        assert_eq!(args[2], "-s");
        assert_eq!(args[3], "s1");
        assert_eq!(args[4], "com.superlogical.terminal.format");
        let body = body(&args);
        assert_eq!(body["block_id"], "b1");
        assert_eq!(body["args"]["format"], "text");
        assert_eq!(body["args"]["trim"], true);
        assert_eq!(body["args"]["max_bytes"], 4096);
    }

    #[test]
    fn blob_read_carries_the_blob_id_and_byte_offset() {
        let args = blob_read_args("s1", "b1", 7, 4096, 8192);
        assert_eq!(args[4], "com.superlogical.terminal.blob_read");
        let body = body(&args);
        assert_eq!(body["block_id"], "b1");
        assert_eq!(body["args"]["id"], 7);
        assert_eq!(body["args"]["offset"], 4096);
        assert_eq!(body["args"]["max_bytes"], 8192);
    }

    #[test]
    fn target_resolve_parses_a_resolved_block() {
        let value = json!({
            "results": [{
                "query": "DustySparrow",
                "status": "resolved",
                "target": {
                    "kind": "block",
                    "session_id": "session:1",
                    "window_id": "window:2",
                    "block_id": "block:3",
                    "label": "DustySparrow"
                }
            }]
        });
        let hit = parse_target_resolve(&value, "DustySparrow").expect("resolved block");
        assert_eq!(hit.session_id, "session:1");
        assert_eq!(hit.window_id, "window:2");
        assert_eq!(hit.block_id, "block:3");
        assert_eq!(hit.label, "DustySparrow");
    }

    #[test]
    fn target_resolve_refuses_ambiguous_and_missing_blocks() {
        let ambiguous = json!({"results": [{
            "query": "shell",
            "status": "ambiguous",
            "candidates": [{"kind":"block","label":"a"},{"kind":"block","label":"b"}]
        }]});
        match parse_target_resolve(&ambiguous, "shell") {
            Err(RexFail::Rejected { message }) => {
                assert!(message.contains("ambiguous"), "{message}");
                assert!(message.contains('2'), "{message}");
            }
            other => panic!("ambiguous must be a typed refusal, got {other:?}"),
        }

        let missing = json!({"results": [{"query": "nope", "status": "not_found"}]});
        match parse_target_resolve(&missing, "nope") {
            Err(RexFail::Rejected { message }) => {
                assert!(message.contains("not found"), "{message}")
            }
            other => panic!("missing must be a typed refusal, got {other:?}"),
        }
    }

    #[test]
    fn target_resolve_missing_results_is_malformed_not_not_found() {
        let err = parse_target_resolve(&json!({}), "x").expect_err("missing results");
        assert!(matches!(err, RexFail::BadOutput { .. }), "{err:?}");
    }

    #[test]
    fn state_labels_name_the_deciding_report() {
        let blocked = json!({"records": [
            {"state": "working", "id": "", "app": "pi"},
            {"state": "blocked", "id": "child", "kind": "permission", "progress": 42,
             "app": "terraform", "title": "Apply plan", "msg": "approve 3 changes"}
        ]});
        let (board, _) = rex_map::board_of(&blocked);
        assert_eq!(
            rex_map::status_from_program_status(&blocked),
            AgentStatus::Blocked
        );
        let labels = rex_map::state_labels_from_board(&board);
        let report = &labels["program_status"];
        assert_eq!(report["id"], "child");
        assert_eq!(report["state"], "blocked");
        assert_eq!(report["kind"], "permission");
        assert_eq!(report["progress"], 42);
        assert_eq!(report["app"], "terraform");
        assert_eq!(report["title"], "Apply plan");
        assert_eq!(report["msg"], "approve 3 changes");

        let root = json!({"records": [{"state": "working", "app": "claude"}]});
        let (board, _) = rex_map::board_of(&root);
        let labels = rex_map::state_labels_from_board(&board);
        let report = &labels["program_status"];
        assert_eq!(report["id"], "");
        assert_eq!(report["state"], "working");
        assert_eq!(report["app"], "claude");
        assert!(report.get("msg").is_none());

        let (empty, _) = rex_map::board_of(&json!({"records": []}));
        assert!(rex_map::state_labels_from_board(&empty).is_empty());
    }

    #[test]
    fn format_chunk_preserves_the_blob_continuation() {
        let chunk = parse_format_chunk(&json!({"content": "abc", "id": 9, "total": 100}))
            .expect("bounded format");
        assert_eq!(chunk.content, "abc");
        assert_eq!(chunk.id, Some(9));
        assert_eq!(chunk.total, Some(100));
        let whole = parse_format_chunk(&json!({"content": "abc"})).expect("whole format");
        assert_eq!(whole.id, None);
        assert_eq!(whole.total, None);
    }

    #[test]
    fn format_and_blob_require_their_declared_fields() {
        assert!(matches!(
            parse_format_chunk(&json!({})),
            Err(RexFail::BadOutput { .. })
        ));
        assert!(matches!(
            parse_blob_chunk(&json!({"id": 1, "data": "x", "total": 1})),
            Err(RexFail::BadOutput { .. })
        ));
        let chunk = parse_blob_chunk(&json!({"id": 1, "data": "x", "total": 1, "eof": true}))
            .expect("complete blob");
        assert_eq!(chunk.data, "x");
        assert!(chunk.eof);
    }

    // --- bd-0uzrq.10: the ONE idle detector --------------------------------

    /// Captured live 2026-11-15T18:56:36+11:00 from block 01a1d6a1 (StormyDesert,
    /// opencode): the pane was at its prompt with NO turn running, and the
    /// records still said `working` — opencode does not emit `idle` after a turn
    /// that ran a background shell. This is the pair that made the dispatcher
    /// never drain a queued wake.
    const STUCK_WORKING_RECORDS: &str = r#"{"records":[
        {"state":"working","app":"opencode","effective_app":"opencode","title":"Toron new work wm 352","updated_at":"2026-11-15T18:56:36.144457+11:00","owner":{"pid":88709,"name":"opencode.exe"}},
        {"id":"ses_e24d69cd7ffeC1B6BlnKWvUz6z","state":"working","app":"opencode","effective_app":"opencode","title":"Toron new work wm 352","updated_at":"2026-11-15T18:56:36.144927+11:00","owner":{"pid":88709,"name":"opencode.exe"}}
    ]}"#;

    /// The idle footer at three widths. opencode elides BOTH ends of the
    /// context row when the pane is narrow, so the live 09:28 capture (narrow)
    /// starts with `…`, not `/`. The wide one is block 77a480's real capture;
    /// the default width sits between them (same row, no elision).
    const IDLE_SCREEN_NARROW: &str = "    Nothing installed all cycle; every\n    proof built its own binary in the\n    export.\n\n    Build  DeepSeek V4.1 Flash 1017\n    59m 45s  40.6 tok/s\n\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n \u{2026}/flywheel:adr-0021-stage-\u{2026}  767.9K (77%)\n";
    const IDLE_SCREEN_DEFAULT: &str = "    Nothing installed all cycle.\n\n    Build  DeepSeek V4.1 Flash 1017\n    59m 45s  40.6 tok/s\n\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n /flywheel:adr-0021-stage-30412  767.9K (77%)\n";
    const IDLE_SCREEN_WIDE: &str = "    identity.\n\n    Build  DeepSeek V4.1 Flash 1017\n    43.6s  58.2 tok/s\n\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n /flywheel:adr-0021-stage-  577.5K (58%)\n";

    /// The progress row at three widths: the shell counter and the truncated
    /// interrupt hint survive every width. Narrow is this pane's live capture
    /// (block 01a1d6a1, 770.3K); wide is the same pane mid-turn at 721.4K.
    const WORKING_SCREEN_NARROW: &str = "    $ cd /Users/bangedorrunt/workspace/flywheel && for b in\n\n                          Jump to latest\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n  es...upt   1 shell  770.3K (77%)\n";
    const WORKING_SCREEN_DEFAULT: &str = "    $ cd /Users/bangedorrunt/workspace/flywheel && for b in 77a48\n\n                          Jump to latest\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n  es...upt   1 shell  750.1K (75%)\n";
    const WORKING_SCREEN_WIDE: &str = "    $ cd /Users/bangedorrunt/workspace/flywheel && for b in 77a480 34b981; do\n\n                          Jump to latest\n\n\n\n  DeepSeek V4.1 Flash 1017  high\n\n  es...upt   1 shell  721.4K (72%)\n";

    /// An idle capture, from block 77a480 at the same instant: records say
    /// `idle` and the screen agrees. Both signals on the same side.
    const IDLE_RECORDS: &str = r#"{"records":[{"state":"idle","app":"opencode","effective_app":"opencode","updated_at":"2026-11-15T18:53:22.738181+11:00","owner":{"pid":67198,"name":"opencode.exe"}}]}"#;

    #[test]
    fn a_stale_working_record_is_overruled_by_the_idle_footer() {
        let records: Value = serde_json::from_str(STUCK_WORKING_RECORDS).unwrap();
        assert_eq!(
            status_from_program_status(&records),
            AgentStatus::Working,
            "the records alone say working: this is the bug"
        );
        assert_eq!(
            super::rex_map::status_from_program_status(&records),
            AgentStatus::Working
        );
        for (width, screen) in [
            ("narrow", IDLE_SCREEN_NARROW),
            ("default", IDLE_SCREEN_DEFAULT),
            ("wide", IDLE_SCREEN_WIDE),
        ] {
            assert!(
                screen_shows_a_prompt(screen),
                "the idle footer is recognised at {width} width"
            );
        }
        for (width, screen) in [
            ("narrow", WORKING_SCREEN_NARROW),
            ("default", WORKING_SCREEN_DEFAULT),
            ("wide", WORKING_SCREEN_WIDE),
        ] {
            assert!(
                !screen_shows_a_prompt(screen),
                "a mid-turn footer is not a prompt at {width} width"
            );
        }

        // Stale past the threshold, and the footer has now been stable for
        // longer than the minimum: the screen wins.
        let first = status_with_stable_screen(
            AgentStatus::Working,
            Some(STALE_WORKING_SECS),
            None,
            IDLE_SCREEN_NARROW,
            1_000,
            PROMPT_STABLE_SECS,
        );
        assert_eq!(
            first.0,
            AgentStatus::Working,
            "ONE capture never overrules — it could be a tool-call gap"
        );
        let second = status_with_stable_screen(
            AgentStatus::Working,
            Some(STALE_WORKING_SECS),
            first.1,
            IDLE_SCREEN_NARROW,
            1_000 + PROMPT_STABLE_SECS,
            PROMPT_STABLE_SECS,
        );
        assert_eq!(
            second.0,
            AgentStatus::Idle,
            "a stable idle footer over a silent working record is idle"
        );
        // Fresh: the record wins, and no screen evidence is even considered.
        let fresh = status_with_stable_screen(
            AgentStatus::Working,
            Some(5),
            None,
            IDLE_SCREEN_NARROW,
            1_000,
            PROMPT_STABLE_SECS,
        );
        assert_eq!(
            fresh.0,
            AgentStatus::Working,
            "a fresh record is never overruled"
        );
        assert!(
            fresh.1.is_none(),
            "a fresh record needs no screen evidence at all"
        );
        // Stale but the screen shows a turn: the record wins.
        assert_eq!(
            status_with_stable_screen(
                AgentStatus::Working,
                Some(600),
                None,
                WORKING_SCREEN_NARROW,
                1_000,
                PROMPT_STABLE_SECS
            )
            .0,
            AgentStatus::Working,
            "a working screen confirms the record"
        );
        // An unknown age never overrules.
        assert_eq!(
            status_with_stable_screen(
                AgentStatus::Working,
                None,
                None,
                IDLE_SCREEN_NARROW,
                1_000,
                PROMPT_STABLE_SECS
            )
            .0,
            AgentStatus::Working,
            "no age, no overrule"
        );
        // Other statuses are not touched.
        assert_eq!(
            status_with_stable_screen(
                AgentStatus::Idle,
                Some(600),
                None,
                WORKING_SCREEN_NARROW,
                1_000,
                PROMPT_STABLE_SECS
            )
            .0,
            AgentStatus::Idle
        );
    }

    /// bd-0uzrq.14: opencode re-renders its idle footer in the gap between two
    /// tool calls, so ONE capture that looks like a prompt is not a finished
    /// turn. The live proof is the 11:12:37Z ring: the turn's commits bracket
    /// the drain (11:01:00Z .. 11:14:04Z), so the pane was at a tool-call gap.
    #[test]
    fn a_tool_call_gap_is_not_a_finished_turn() {
        let fold = |prev, screen: &str, now: i64| {
            status_with_stable_screen(
                AgentStatus::Working,
                Some(STALE_WORKING_SECS),
                prev,
                screen,
                now,
                PROMPT_STABLE_SECS,
            )
        };

        // One capture: still working, however perfect the footer looks.
        let (verdict, sample) = fold(None, IDLE_SCREEN_NARROW, 1_000);
        assert_eq!(
            verdict,
            AgentStatus::Working,
            "a single capture cannot tell a gap from a finished turn"
        );
        assert!(sample.is_some(), "the first capture is remembered");

        // Two captures CLOSER than the minimum: still working.
        let (verdict, _) = fold(sample, IDLE_SCREEN_NARROW, 1_000 + PROMPT_STABLE_SECS - 1);
        assert_eq!(
            verdict,
            AgentStatus::Working,
            "the footer must hold for the whole minimum"
        );

        // Two captures with different content: the gap changed, so it was not
        // an idle pane — a prompt does not redraw between tools.
        let (verdict, moved) = fold(sample, IDLE_SCREEN_DEFAULT, 1_000 + PROMPT_STABLE_SECS);
        assert_eq!(
            verdict,
            AgentStatus::Working,
            "a footer that changed is not stable"
        );
        assert_ne!(
            moved.map(|s| s.first_seen_unix),
            Some(1_000),
            "a different screen restarts the clock"
        );

        // The second capture shows a turn: working, whatever the digest says.
        let (verdict, _) = fold(
            sample,
            WORKING_SCREEN_NARROW,
            1_000 + PROMPT_STABLE_SECS * 3,
        );
        assert_eq!(
            verdict,
            AgentStatus::Working,
            "a progress row means the turn is running"
        );

        // Same footer, far enough apart: the turn is over.
        let (verdict, _) = fold(sample, IDLE_SCREEN_NARROW, 1_000 + PROMPT_STABLE_SECS);
        assert_eq!(
            verdict,
            AgentStatus::Idle,
            "a footer that has not moved for the minimum IS a finished turn"
        );
    }

    #[test]
    fn the_record_age_comes_from_its_own_stamp() {
        let age_of = |iso: &str, now: i64| -> Option<u64> {
            let records: Value =
                serde_json::from_str(&format!(r#"{{"records":[{{"updated_at":"{iso}"}}]}}"#))
                    .unwrap();
            newest_record_age_secs(&records, now)
        };
        assert_eq!(age_of("1970-01-01T00:00:10Z", 100), Some(90));
        assert_eq!(
            age_of("1970-01-01T01:00:00+01:00", 100),
            Some(100),
            "an offset stamp normalises to UTC"
        );
        // The live fixture's absolute epoch is not hardcoded: two nows pin the
        // DELTA, and the 1970 cases above pin the parser's absolute answer.
        let records: Value = serde_json::from_str(STUCK_WORKING_RECORDS).unwrap();
        let a = newest_record_age_secs(&records, 2_000_000_000).expect("fixture stamp parses");
        let b = newest_record_age_secs(&records, 2_000_000_300).expect("fixture stamp parses");
        assert_eq!(b, a + 300, "300s later is 300s older");
        assert_eq!(
            newest_record_age_secs(&serde_json::json!({"records": []}), 100),
            None,
            "no record, no age"
        );
        assert_eq!(
            newest_record_age_secs(&serde_json::json!({"records":[{"state":"working"}]}), 100),
            None,
            "a record with no stamp never overrules"
        );
    }

    #[test]
    fn an_idle_record_needs_no_screen() {
        let records: Value = serde_json::from_str(IDLE_RECORDS).unwrap();
        assert_eq!(status_from_program_status(&records), AgentStatus::Idle);
        assert!(screen_shows_a_prompt(IDLE_SCREEN_NARROW));
    }
}
