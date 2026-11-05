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
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde_json::Value;
use crate::backend::{BackendError, BackendErrorCode, BackendEvent};

use crate::proc::{ProcError, run_bounded};

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

impl Rex {
    pub fn new(bin: PathBuf, wait: Duration) -> Self {
        Self { bin, wait }
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

#[cfg(test)]
pub(crate) fn reset_api_cache() {
    if let Some(cache) = API_LIST_CACHE.get() {
        *cache.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// `rex do -e <lua> [--args <json>]`: one-shot Lua, result as JSON.
///
/// The script must be handler- and timer-free (see
/// `integrations/rex/session-snapshot.lua`): `rex.on`/`rex.every` keep
/// `rex do` running past the call bound, which the edge reports as
/// `Interrupted`, never as a result. `--json` makes the return value
/// machine-readable; an empty stdout is `BadOutput`, never `Null`.
#[cfg_attr(not(test), allow(dead_code))]
pub fn do_args(session: Option<&str>, script: &str, args_json: Option<&str>) -> Vec<String> {
    let mut args = vec!["do".into()];
    if let Some(session) = session.filter(|s| !s.is_empty()) {
        args.push("-s".into());
        args.push(session.into());
    }
    args.push("-e".into());
    args.push(script.into());
    if let Some(payload) = args_json.filter(|s| !s.is_empty()) {
        args.push("--args".into());
        args.push(payload.into());
    }
    args.push("--json".into());
    args
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
/// This is a module inside `rex_cli.rs` because `src/backend/rex_map.rs` and
/// `src/backend/mod.rs` are exclusively leased by PurpleGorge for bd-i328.
pub mod rex_map {
    use serde_json::Value;
    use crate::backend::{AgentStatus, BackendEvent, PaneId};
    use crate::osc7501::{Board, ProgramState, parse_report};

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
        match crate::harness::Harness::from_kind(&view.foreground) {
            crate::harness::Harness::Other => {
                if SHELLS.contains(&view.foreground.as_str()) {
                    AgentStatus::Idle
                } else {
                    AgentStatus::Unknown
                }
            }
            _ => AgentStatus::Working,
        }
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
                let raw = if id.is_empty() {
                    format!("\u{1b}]7501;state={state}\u{1b}\\")
                } else {
                    format!("\u{1b}]7501;state={state}:id={id}\u{1b}\\")
                };
                if let Some(report) = parse_report(&raw) {
                    order.push(report.id.clone().unwrap_or_default());
                    board.apply(&raw);
                }
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
