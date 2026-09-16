// governed-by: ADR-0003
// bd-led.6.1: typed backend errors with machine codes.
// Classification MUST use `code`, never `message` text (herdr protocol v20
// discipline — ADR-0003 "classify on code, never message text").

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Machine-readable backend error code. Backends map their protocol codes
/// onto these; unknown codes pass through as [`BackendErrorCode::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BackendErrorCode {
    /// Operation is unsupported on this backend (e.g. TmuxBackend before
    /// un-held; ADR-0003 G9).
    Unsupported,
    /// Operation timed out waiting for the backend.
    Timeout,
    /// herdr: agent prompt stalled (no response within SLA).
    AgentPromptStalled,
    /// herdr: agent prompt rejected because the agent is waiting at an
    /// approval/question dialog — nothing was sent (protocol v20, herdr
    /// 0.8.2: `agent.prompt` fails fast instead of typing into the dialog).
    AgentBlocked,
    /// herdr: agent.read/agent.prompt rejected because the agent is not idle
    /// (alt-screen transcript unavailable while working/blocked/unknown).
    AgentNotIdle,
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
        // The herdr wire code maps to the typed variant (ADR-0003: classify
        // on code, never message text), and the variant renders back the
        // same wire string for error display.
        let parsed: BackendErrorCode = "agent_not_idle".parse().expect("mapped");
        assert_eq!(parsed, BackendErrorCode::AgentNotIdle);
        assert_eq!(parsed.as_str(), "agent_not_idle");
        assert_eq!(BackendErrorCode::AgentNotIdle.as_str(), "agent_not_idle");
    }

    #[test]
    fn agent_blocked_roundtrips_through_wire_string() {
        // Protocol v20 (herdr 0.8.2): agent.prompt rejects blocked agents
        // with `agent_blocked` and sends nothing. Same discipline as
        // agent_not_idle — typed variant, wire-string round-trip.
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
        // Callers branch on the code, not message text.
        match err.code() {
            BackendErrorCode::AgentNotIdle => {}
            other => panic!("expected AgentNotIdle, got {other:?}"),
        }
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
