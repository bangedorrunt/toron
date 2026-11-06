// governed-by: ADR-0003, ADR-0031

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
pub use toron_core::pane_types::{AgentStatus, TOKEN_NPUB, TOKEN_PIN, TOKEN_PROJECT};
// Moved vocabulary stays addressable under its old path so retargets are
// pure crate-prefix swaps (the canonical home is `toron_core::pane_types`).
pub use toron_core::pane_types::{
    AgentInfo, AgentSessionInfo, AgentSessionRefKind, PaneInfo, PaneScrollInfo,
    normalize_agent_status,
};

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

#[cfg(test)]
mod backend_types_s1 {
    use super::*;

    #[test]
    fn pane_id_opaque_compare_clone_only() {
        let a = PaneId::new("w1:p1");
        let b = PaneId::from("w1:p1");
        let c: PaneId = "w1:p2".into();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(a.as_str(), "w1:p1");
        assert_eq!(a.clone().into_string(), "w1:p1");
        assert!(PaneId::empty().is_empty());
        let json = serde_json::to_string(&a).expect("serializes");
        assert_eq!(json, "\"w1:p1\"");
        assert_eq!(serde_json::from_str::<PaneId>(&json).expect("parses"), a);
    }
}

/// Source id for toron mail-identity pane stamps (bd-6f57). Display-only
/// metadata — never steals lifecycle authority from the multiplexer.
pub const MAIL_IDENTITY_SOURCE: &str = "toron:mail-identity";

#[derive(Debug, Clone, Deserialize)]
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
