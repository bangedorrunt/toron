// governed-by: ADR-0003, ADR-0031
// bd-led.6.1: shared backend types — pane id, agent status, events.
// Wire contract mirrors herdr protocol v20 (AgentStatus lowercase JSON).
// ADR-0031 S1: PaneId is opaque (adapters construct via new(), cores only
// compare/clone/as_str); raw backend status strings enter only through
// normalize_herdr_status(), never FromStr/.parse() in reducers.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::error::BackendError;

/// Identifies a supervised pane/worktree in the backend.
///
/// Opaque (ADR-0031 S1): the inner workspace-qualified id (`w1:p1`) must
/// never enter receipts or bead reasons as a raw string. Construct at the
/// adapter edge via [`PaneId::new`]; cores only compare, clone, or
/// [`PaneId::as_str`]. No `.0` field access outside this module.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PaneId(String);

impl PaneId {
    /// Adapter-edge constructor. Accepts any string; no validation so a
    /// future Superlogical dialect changes method bodies, never reducers.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Empty id for missing `pane_id` in push payloads (same as before).
    pub const fn empty() -> Self {
        Self(String::new())
    }

    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for PaneId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for PaneId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl AsRef<str> for PaneId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PaneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
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
    /// [`normalize_herdr_status`] as the single named importer.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(normalize_herdr_status(s))
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
pub fn normalize_herdr_status(raw: &str) -> AgentStatus {
    match raw {
        "idle" => AgentStatus::Idle,
        "working" => AgentStatus::Working,
        "blocked" => AgentStatus::Blocked,
        "done" => AgentStatus::Done,
        "unknown" => AgentStatus::Unknown,
        _ => AgentStatus::Unknown,
    }
}

/// Push event emitted by a backend's `subscribe` stream (unified in toron-core
/// by ADR-0005 P1 — flywheel's `__poll_events` leaf consumes this shape).
///
/// Variants carry the fields the orchestration loop routes on: pane id, agent
/// name (herdr's `PaneAgentStatusChanged`/`PaneAgentDetected`), and status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendEvent {
    AgentStatusChanged {
        pane: PaneId,
        agent: Option<String>,
        status: AgentStatus,
    },
    AgentDetected {
        pane: PaneId,
        agent: Option<String>,
        status: AgentStatus,
    },
    PaneExited {
        pane: PaneId,
    },
    /// herdr closed the subscription after dropping retained events
    /// (`events_lost`). Callers must resubscribe and reconcile with a snapshot;
    /// do not treat this as a quiet EOF.
    EventsLost,
    /// Unmapped push payload (forwarded verbatim, never classified).
    Raw(String),
}

/// Landing data for a coded `agent.prompt` (ADR-0002, review-fix F6; unified
/// in toron-core by ADR-0005 P1).
///
/// Exactly one of the two is populated on the herdr backend (landed →
/// `status`, protocol error → `code`); other backends define their own
/// contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptLanding {
    pub status: Option<AgentStatus>,
    pub code: Option<String>,
}

/// Options for spawning a new agent pane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SpawnOptions {
    pub project: String,
    pub agent_name: String,
    pub program: String,
    pub cwd: Option<String>,
}

// ponytail: one test module, no helper abstractions. Upgrade to
// proptest strategies if dialect surface grows past herdr + superlogical.
#[cfg(test)]
mod backend_types_s1 {
    use super::*;

    #[test]
    fn canonical_five_normalize() {
        assert_eq!(normalize_herdr_status("working"), AgentStatus::Working);
        assert_eq!(normalize_herdr_status("blocked"), AgentStatus::Blocked);
        assert_eq!(normalize_herdr_status("done"), AgentStatus::Done);
        assert_eq!(normalize_herdr_status("idle"), AgentStatus::Idle);
        assert_eq!(normalize_herdr_status("unknown"), AgentStatus::Unknown);
    }

    #[test]
    fn unknown_never_proves_completion() {
        // Empty, future-dialect, wrong-case: all fail safe to Unknown.
        for raw in ["", "IDDLE", "Idle", "superlogical:busy", "w1:p1", "done "] {
            assert_eq!(
                normalize_herdr_status(raw),
                AgentStatus::Unknown,
                "raw {raw:?} must not classify as completion"
            );
        }
    }

    #[test]
    fn from_str_is_lenient_via_normalizer() {
        // Any .parse() path fails safe too; named importer preferred.
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

    #[test]
    fn pane_id_opaque_compare_clone_only() {
        // Adapters construct; cores compare/clone/as_str. No .0 access.
        let a = PaneId::new("w1:p1");
        let b = PaneId::from("w1:p1");
        let c: PaneId = "w1:p2".into();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.as_str(), "w1:p1");
        assert_eq!(a.clone().into_string(), "w1:p1");
        assert!(PaneId::empty().is_empty());
        // Serde stays transparent for the wire (no shape change).
        let json = serde_json::to_string(&a).expect("serializes");
        assert_eq!(json, "\"w1:p1\"");
        assert_eq!(serde_json::from_str::<PaneId>(&json).expect("parses"), a);
    }
}




/// Source id for toron mail-identity pane stamps (bd-6f57). Display-only
/// metadata — never steals lifecycle authority from the multiplexer.
pub const MAIL_IDENTITY_SOURCE: &str = "toron:mail-identity";
/// Token keys for the mail-identity stamp (ASCII alnum/`_`/`-`, ≤32 chars).
pub const TOKEN_PIN: &str = "toron_pin";
pub const TOKEN_PROJECT: &str = "toron_project";
pub const TOKEN_NPUB: &str = "toron_npub";

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
