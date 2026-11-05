// governed-by: ADR-0003

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
