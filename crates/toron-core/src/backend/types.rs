// governed-by: ADR-0003
// bd-led.6.1: shared backend types — pane id, agent status, events.
// Wire contract mirrors herdr protocol v19 (AgentStatus lowercase JSON).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::error::BackendError;

/// Identifies a supervised pane/worktree in the backend.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PaneId(pub String);

impl PaneId {
    pub fn as_str(&self) -> &str {
        &self.0
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

/// Agent lifecycle state (herdr protocol v19 wire values).
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
    pub fn as_str(&self) -> &'static str {
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "idle" => Ok(AgentStatus::Idle),
            "working" => Ok(AgentStatus::Working),
            "blocked" => Ok(AgentStatus::Blocked),
            "done" => Ok(AgentStatus::Done),
            "unknown" => Ok(AgentStatus::Unknown),
            other => Err(BackendError::new(
                super::error::BackendErrorCode::Malformed,
                format!("unknown agent status: {other}"),
            )),
        }
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
