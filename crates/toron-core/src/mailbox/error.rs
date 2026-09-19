// governed-by: ADR-0007
// R1 / D2 (ADR-0007): shared machine codes for the coordination fabric.
// Mirrors the `BackendError` spirit (backend/error.rs): classification MUST
// use `code`, never `message` text.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Machine-readable mail error code. Backends map their own error shapes
/// onto these; unknown codes pass through as [`MailErrorCode::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MailErrorCode {
    /// The referenced message/agent/project does not exist.
    NotFound,
    /// The operation was denied by a contact policy (or similar rule).
    PolicyDenied,
    /// Operation is unsupported on this backend.
    Unsupported,
    /// Operation timed out.
    Timeout,
    /// Transport / storage I/O failure.
    Io,
    /// Malformed response from the backend.
    Malformed,
    /// Request serialization failure.
    Serialize,
    /// Backend returned an opaque code not mapped above.
    Other(String),
}

impl MailErrorCode {
    pub fn as_str(&self) -> &str {
        match self {
            MailErrorCode::NotFound => "not_found",
            MailErrorCode::PolicyDenied => "policy_denied",
            MailErrorCode::Unsupported => "unsupported",
            MailErrorCode::Timeout => "timeout",
            MailErrorCode::Io => "io",
            MailErrorCode::Malformed => "malformed",
            MailErrorCode::Serialize => "serialize",
            MailErrorCode::Other(s) => s,
        }
    }
}

impl FromStr for MailErrorCode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "not_found" => MailErrorCode::NotFound,
            "policy_denied" => MailErrorCode::PolicyDenied,
            "unsupported" => MailErrorCode::Unsupported,
            "timeout" => MailErrorCode::Timeout,
            "io" => MailErrorCode::Io,
            "malformed" => MailErrorCode::Malformed,
            "serialize" => MailErrorCode::Serialize,
            other => MailErrorCode::Other(other.to_string()),
        })
    }
}

/// Typed mailbox edge (ADR-0034 G1): new fallible `MailSurface` methods
/// return this instead of `anyhow::Result`. Existing `anyhow` sites migrate
/// only when touched — no flag-day signature churn. Bridge with
/// [`MailSurfaceResult::map_mail_error`].
pub type MailboxResult<T> = Result<T, MailError>;

/// Mail error: machine code + human message. Classify on `code`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailError {
    pub code: MailErrorCode,
    pub message: String,
}

impl MailError {
    pub fn new(code: MailErrorCode, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: message.as_ref().to_string(),
        }
    }

    pub fn not_found(what: impl AsRef<str>) -> Self {
        Self::new(MailErrorCode::NotFound, what.as_ref())
    }

    pub fn policy_denied(what: impl AsRef<str>) -> Self {
        Self::new(MailErrorCode::PolicyDenied, what.as_ref())
    }

    pub fn unsupported(what: impl AsRef<str>) -> Self {
        Self::new(MailErrorCode::Unsupported, what.as_ref())
    }

    pub fn timeout(message: impl AsRef<str>) -> Self {
        Self::new(MailErrorCode::Timeout, message.as_ref())
    }

    pub fn other(message: impl AsRef<str>) -> Self {
        Self::new(
            MailErrorCode::Other(message.as_ref().to_string()),
            message.as_ref(),
        )
    }

    pub const fn code(&self) -> &MailErrorCode {
        &self.code
    }
}

/// Map an `anyhow` error from the underlying Mailbox onto a [`MailError`].
/// Code extraction is heuristic — the message text never becomes the
/// classifier, only the code does.
pub fn mail_error_from_anyhow(e: anyhow::Error) -> MailError {
    let msg = e.to_string();
    let lowered = msg.to_ascii_lowercase();
    let code = if lowered.contains("not found") || lowered.starts_with("no ") {
        MailErrorCode::NotFound
    } else if lowered.contains("policy") || lowered.contains("contact") {
        MailErrorCode::PolicyDenied
    } else if lowered.contains("unsupported") {
        MailErrorCode::Unsupported
    } else if lowered.contains("timeout") || lowered.contains("timed out") {
        MailErrorCode::Timeout
    } else if lowered.contains("malformed") || lowered.contains("parse") {
        MailErrorCode::Malformed
    } else {
        MailErrorCode::Other(msg.clone())
    };
    MailError::new(code, msg)
}

/// Extension trait: convert `anyhow::Result<T>` from the Mailbox into
/// `Result<T, MailError>` for the MailSurface layer.
pub trait MailSurfaceResult<T> {
    fn map_mail_error(self) -> Result<T, MailError>;
}

impl<T> MailSurfaceResult<T> for anyhow::Result<T> {
    fn map_mail_error(self) -> Result<T, MailError> {
        self.map_err(mail_error_from_anyhow)
    }
}

impl std::fmt::Display for MailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "mail error {}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for MailError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_string_round_trips() {
        for code in [
            MailErrorCode::NotFound,
            MailErrorCode::PolicyDenied,
            MailErrorCode::Unsupported,
            MailErrorCode::Timeout,
            MailErrorCode::Io,
            MailErrorCode::Malformed,
            MailErrorCode::Serialize,
        ] {
            let s = code.as_str();
            assert_eq!(s.parse::<MailErrorCode>().unwrap(), code, "round trip {s}");
        }
    }

    #[test]
    fn unknown_code_passes_through_other() {
        assert_eq!(
            "wat".parse::<MailErrorCode>().unwrap(),
            MailErrorCode::Other("wat".into())
        );
    }

    #[test]
    fn anyhow_maps_to_machine_codes() {
        let e = anyhow::anyhow!("agent GreenCastle not found");
        assert_eq!(mail_error_from_anyhow(e).code, MailErrorCode::NotFound);

        let e = anyhow::anyhow!("policy denied: block_all");
        assert_eq!(mail_error_from_anyhow(e).code, MailErrorCode::PolicyDenied);

        let e = anyhow::anyhow!("boom");
        assert_eq!(
            mail_error_from_anyhow(e).code,
            MailErrorCode::Other("boom".into())
        );
    }

    #[test]
    fn mailbox_result_edge_round_trips() {
        // G1: the typed edge carries codes end-to-end (anyhow bridge in,
        // typed result out) without per-site signature churn.
        let via_anyhow: anyhow::Result<u32> = Err(anyhow::anyhow!("agent X not found"));
        let typed: MailboxResult<u32> = via_anyhow.map_mail_error();
        let err = typed.expect_err("maps to typed error");
        assert_eq!(err.code, MailErrorCode::NotFound);
        let ok: MailboxResult<u32> = Ok(7);
        assert_eq!(ok.unwrap(), 7);
    }

    #[test]
    fn helpers_build_coded_errors() {
        assert_eq!(MailError::not_found("msg").code, MailErrorCode::NotFound);
        assert_eq!(MailError::unsupported("x").code, MailErrorCode::Unsupported);
        let e = MailError::other("x");
        assert_eq!(e.code, MailErrorCode::Other("x".into()));
        assert_eq!(e.message, "x");
    }
}
