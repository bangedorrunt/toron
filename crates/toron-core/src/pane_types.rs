// governed-by: ADR-0003, ADR-0031
//! Shared pane vocabulary: the data types the [`crate::plane_port::PanePlane`]
//! port names. Moved from the pane plane's `backend::types` (wave7-b) so the
//! plane crate depends on core, never the reverse. No backend logic here —
//! adapters, errors-as-values, and construction stay in `toron-plane`.

use serde_json::Value;

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Machine-readable backend error code. Backends map their protocol codes
/// onto these; unknown codes pass through as [`BackendErrorCode::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BackendErrorCode {
    /// Operation is unsupported on this backend.
    Unsupported,
    /// Operation timed out waiting for the backend.
    Timeout,
    /// The prompt landed but the program never went working in the start
    /// window (hc `started: false`; the old herdr SLA gate).
    AgentPromptStalled,
    /// A blocked program owns the pane (an approval/question dialog):
    /// nothing was sent. hc refuses `terminal.send` to a `blocked` program
    /// unless forced.
    AgentBlocked,
    /// A status precondition rejected the call (`status_mismatch`). hc reads
    /// never raise it; kept for backends that gate reads on idle.
    AgentNotIdle,
    /// Subscriber fell behind retained history; resubscribe + snapshot.
    EventsLost,
    /// Could not connect to the backend socket/daemon.
    Connect,
    /// Transport I/O failure.
    Io,
    /// Malformed response from the backend.
    Malformed,
    /// Request serialization failure.
    Serialize,
    /// Backend returned an opaque machine code not mapped above.
    Other(String),
}

impl BackendErrorCode {
    pub fn as_str(&self) -> &str {
        match self {
            BackendErrorCode::Unsupported => "unsupported",
            BackendErrorCode::Timeout => "timeout",
            BackendErrorCode::AgentPromptStalled => "agent_prompt_stalled",
            BackendErrorCode::AgentBlocked => "agent_blocked",
            BackendErrorCode::AgentNotIdle => "agent_not_idle",
            BackendErrorCode::EventsLost => "events_lost",
            BackendErrorCode::Connect => "connect",
            BackendErrorCode::Io => "io",
            BackendErrorCode::Malformed => "malformed",
            BackendErrorCode::Serialize => "serialize",
            BackendErrorCode::Other(s) => s,
        }
    }
}

impl FromStr for BackendErrorCode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "unsupported" => BackendErrorCode::Unsupported,
            "timeout" => BackendErrorCode::Timeout,
            "agent_prompt_stalled" => BackendErrorCode::AgentPromptStalled,
            "agent_blocked" => BackendErrorCode::AgentBlocked,
            "agent_not_idle" => BackendErrorCode::AgentNotIdle,
            "events_lost" => BackendErrorCode::EventsLost,
            "connect" => BackendErrorCode::Connect,
            "io" => BackendErrorCode::Io,
            "malformed" => BackendErrorCode::Malformed,
            "serialize" => BackendErrorCode::Serialize,
            other => BackendErrorCode::Other(other.to_string()),
        })
    }
}

/// Backend error: machine code + human message. Classify on `code`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub code: BackendErrorCode,
    pub message: String,
}

impl BackendError {
    pub fn new(code: BackendErrorCode, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: message.as_ref().to_string(),
        }
    }

    pub fn unsupported(what: impl AsRef<str>) -> Self {
        Self::new(BackendErrorCode::Unsupported, what.as_ref())
    }

    pub fn timeout(message: impl AsRef<str>) -> Self {
        Self::new(BackendErrorCode::Timeout, message.as_ref())
    }

    pub fn agent_not_idle(message: impl AsRef<str>) -> Self {
        Self::new(BackendErrorCode::AgentNotIdle, message.as_ref())
    }

    pub const fn code(&self) -> &BackendErrorCode {
        &self.code
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend error {}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for BackendError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_not_idle_roundtrips_through_wire_string() {
        let parsed: BackendErrorCode = "agent_not_idle".parse().expect("mapped");
        assert_eq!(parsed, BackendErrorCode::AgentNotIdle);
        assert_eq!(parsed.as_str(), "agent_not_idle");
        assert_eq!(BackendErrorCode::AgentNotIdle.as_str(), "agent_not_idle");
    }

    #[test]
    fn agent_blocked_roundtrips_through_wire_string() {
        let parsed: BackendErrorCode = "agent_blocked".parse().expect("mapped");
        assert_eq!(parsed, BackendErrorCode::AgentBlocked);
        assert_eq!(parsed.as_str(), "agent_blocked");
        assert_eq!(BackendErrorCode::AgentBlocked.as_str(), "agent_blocked");
    }

    #[test]
    fn agent_not_idle_error_carries_typed_code() {
        let err = BackendError::agent_not_idle("agent is working");
        assert_eq!(err.code(), &BackendErrorCode::AgentNotIdle);
        assert!(err.to_string().contains("agent_not_idle"), "{err}");
        match err.code() {
            BackendErrorCode::AgentNotIdle => {}
            other => panic!("expected AgentNotIdle, got {other:?}"),
        }
    }

    #[test]
    fn events_lost_roundtrips_through_wire_string() {
        let parsed: BackendErrorCode = "events_lost".parse().expect("mapped");
        assert_eq!(parsed, BackendErrorCode::EventsLost);
        assert_eq!(parsed.as_str(), "events_lost");
    }

    #[test]
    fn unknown_codes_stay_opaque() {
        let parsed: BackendErrorCode = "some_future_code".parse().expect("mapped");
        assert_eq!(
            parsed,
            BackendErrorCode::Other("some_future_code".to_string())
        );
    }
}

/// Agent lifecycle state (herdr protocol v20 wire values).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
}

impl AgentStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            AgentStatus::Idle => "idle",
            AgentStatus::Working => "working",
            AgentStatus::Blocked => "blocked",
            AgentStatus::Done => "done",
            AgentStatus::Unknown => "unknown",
        }
    }
}

impl FromStr for AgentStatus {
    type Err = BackendError;

    /// Lenient by design (ADR-0031 S1): unknown dialect strings map to
    /// `Unknown`, never `Err`. `Unknown` never proves completion, so a new
    /// backend word fails safe. Reducers should still prefer
    /// [`normalize_agent_status`] as the single named importer.
    ///
    /// Agents read [`AgentStatus::as_str`] for display; reducers call
    /// [`normalize_agent_status`] for input.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(normalize_agent_status(s))
    }
}

/// Canonical importer for raw backend status strings (ADR-0031 S1/A4).
///
/// The only function allowed to translate dialect into the five canonical
/// states. Unknown, empty, or future-dialect input maps to
/// [`AgentStatus::Unknown`], which never proves completion. Everything
/// downstream of this function sees only the canonical five.
///
/// `blocked` is an await-signal, not do-not-disturb; `done` is finished +
/// unseen, `idle` finished + seen (herdr seen-split, ADR-0031 A2).
pub fn normalize_agent_status(raw: &str) -> AgentStatus {
    match raw {
        "idle" => AgentStatus::Idle,
        "working" => AgentStatus::Working,
        "blocked" => AgentStatus::Blocked,
        "done" => AgentStatus::Done,
        "unknown" => AgentStatus::Unknown,
        _ => AgentStatus::Unknown,
    }
}
/// Token keys for the mail-identity stamp (ASCII alnum/`_`/`-`, ≤32 chars).
pub const TOKEN_PIN: &str = "toron_pin";
pub const TOKEN_PROJECT: &str = "toron_project";
pub const TOKEN_NPUB: &str = "toron_npub";
#[derive(Debug, Clone, Deserialize)]
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
/// Agent-session reference (`agent_session` on AgentInfo/PaneInfo). Rex
/// reports one; hc (tested 0.1.18) reports none, so the field stays `None` there.
#[derive(Debug, Clone, Deserialize)]
pub struct AgentSessionInfo {
    pub source: String,
    pub agent: String,
    pub kind: AgentSessionRefKind,
    pub value: String,
}

/// Agent-session reference kind (`id` | `path`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentSessionRefKind {
    Id,
    Path,
}

/// herdr pane scroll state (`scroll` on PaneInfo).
#[derive(Debug, Clone, Deserialize)]
pub struct PaneScrollInfo {
    pub offset_from_bottom: u64,
    pub max_offset_from_bottom: u64,
    pub viewport_rows: u64,
}

#[cfg(test)]
mod pane_types_tests {
    use super::*;

    #[test]
    fn canonical_five_normalize() {
        assert_eq!(normalize_agent_status("working"), AgentStatus::Working);
        assert_eq!(normalize_agent_status("blocked"), AgentStatus::Blocked);
        assert_eq!(normalize_agent_status("done"), AgentStatus::Done);
        assert_eq!(normalize_agent_status("idle"), AgentStatus::Idle);
        assert_eq!(normalize_agent_status("unknown"), AgentStatus::Unknown);
    }

    #[test]
    fn unknown_never_proves_completion() {
        for raw in ["", "IDDLE", "Idle", "superlogical:busy", "w1:p1", "done "] {
            assert_eq!(
                normalize_agent_status(raw),
                AgentStatus::Unknown,
                "raw {raw:?} must not classify as completion"
            );
        }
    }

    #[test]
    fn from_str_is_lenient_via_normalizer() {
        assert_eq!(
            "working".parse::<AgentStatus>().expect("parses"),
            AgentStatus::Working
        );
        assert_eq!(
            "future-dialect-word"
                .parse::<AgentStatus>()
                .expect("lenient"),
            AgentStatus::Unknown
        );
    }
}
