// governed-by: ADR-0003
// bd-led.6.1: typed backend errors with machine codes.
// Classification MUST use `code`, never `message` text (herdr protocol v18
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

    pub fn code(&self) -> &BackendErrorCode {
        &self.code
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "backend error {}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for BackendError {}
