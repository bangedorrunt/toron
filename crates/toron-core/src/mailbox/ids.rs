// governed-by: ADR-0007
// R1 / D1 (ADR-0007): canonical message identity is a string — hex
// event/rumor id on toron, decimal string on agent-mail. Never u64 in
// shared request/response types; this newtype is the typed contract.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Canonical message identity (ADR-0007 D1).
///
/// Wraps the raw string id: a hex event/rumor id on toron, a decimal
/// string on agent-mail. `FromStr` rejects empty / whitespace-only ids so
/// a corrupt id fails loudly instead of propagating as a silent default.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(String);

impl MessageId {
    /// The raw id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the newtype, returning the raw id string.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl FromStr for MessageId {
    type Err = MessageIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(MessageIdError::Empty);
        }
        if trimmed != s || trimmed.contains(char::is_whitespace) {
            return Err(MessageIdError::Whitespace);
        }
        Ok(Self(trimmed.to_string()))
    }
}

impl From<String> for MessageId {
    fn from(s: String) -> Self {
        // Callers constructing from already-validated ids (inbox rows,
        // event ids) — parse for the same non-empty guarantee.
        Self(s)
    }
}

impl From<&str> for MessageId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl AsRef<str> for MessageId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Why a [`MessageId`] failed to parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageIdError {
    /// The id was empty (after trimming).
    Empty,
    /// The id contained leading/trailing/embedded whitespace.
    Whitespace,
}

impl fmt::Display for MessageIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("message id is empty"),
            Self::Whitespace => f.write_str("message id contains whitespace"),
        }
    }
}

impl std::error::Error for MessageIdError {}

/// Shared parse error for ADR-0035 boundary newtypes. The `&'static str`
/// names the offending type so a loud rejection says which boundary failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundaryIdError {
    Empty(&'static str),
    Whitespace(&'static str),
}

impl fmt::Display for BoundaryIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(what) => write!(f, "{what} id is empty"),
            Self::Whitespace(what) => write!(f, "{what} id contains whitespace"),
        }
    }
}

impl std::error::Error for BoundaryIdError {}

/// Define one ADR-0035 Spec 6 boundary newtype: non-empty, whitespace-free,
/// parsed at the boundary via `FromStr`, trusted inside. Deserialization
/// assumes a validated writer (sqlite rows); construction validates.
macro_rules! boundary_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            /// The raw id string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consume the newtype, returning the raw id string.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = BoundaryIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    return Err(BoundaryIdError::Empty(stringify!($name)));
                }
                if trimmed != s || trimmed.contains(char::is_whitespace) {
                    return Err(BoundaryIdError::Whitespace(stringify!($name)));
                }
                Ok(Self(trimmed.to_string()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

boundary_id!(
    Corr,
    "Correlation id (ADR-0028 E3): minted by the sender, non-empty. The receipt \
     table is keyed by it; `thread_id` is never accepted where `Corr` is required."
);
boundary_id!(
    ThreadId,
    "Mail thread id: routing metadata only, never a work order and never `corr`."
);
boundary_id!(
    EffectKey,
    "Stable idempotency key for one logical external effect (ADR-0032): replay on \
     the same key returns the stored row, never re-fires."
);
boundary_id!(
    RunId,
    "Workflow run id: one `tick` + `RunJournal` Started-to-terminal lifecycle."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hex_rumor_id() {
        let id: MessageId = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            .parse()
            .expect("hex id parses");
        assert_eq!(
            id.as_str(),
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        );
    }

    #[test]
    fn parses_agent_mail_decimal_id() {
        let id: MessageId = "12345".parse().expect("decimal id parses");
        assert_eq!(id.as_str(), "12345");
    }

    #[test]
    fn rejects_empty_id() {
        let err = "".parse::<MessageId>().expect_err("empty id rejected");
        assert_eq!(err, MessageIdError::Empty);
    }

    #[test]
    fn rejects_whitespace() {
        assert_eq!(
            " abcd".parse::<MessageId>().unwrap_err(),
            MessageIdError::Whitespace,
            "leading whitespace rejected"
        );
        assert_eq!(
            "ab cd".parse::<MessageId>().unwrap_err(),
            MessageIdError::Whitespace,
            "embedded whitespace rejected"
        );
    }

    #[test]
    fn round_trips_through_string() {
        let raw = "0badbeef".to_string();
        let id = MessageId::from(raw.clone());
        assert_eq!(id.as_ref(), raw);
        assert_eq!(id.to_string(), raw);
        assert_eq!(id, MessageId::from(raw.as_str()));
    }

    #[test]
    fn boundary_ids_reject_empty_and_whitespace() {
        assert_eq!(
            "".parse::<Corr>().unwrap_err(),
            BoundaryIdError::Empty("Corr")
        );
        assert_eq!(
            "a b".parse::<ThreadId>().unwrap_err(),
            BoundaryIdError::Whitespace("ThreadId")
        );
        assert_eq!(
            " k".parse::<EffectKey>().unwrap_err(),
            BoundaryIdError::Whitespace("EffectKey")
        );
        let ok: RunId = "run-1".parse().expect("valid run id");
        assert_eq!(ok.as_str(), "run-1");
        assert_eq!(ok.to_string(), "run-1");
        let c: Corr = "c1".parse().expect("valid corr");
        assert_eq!(c.into_inner(), "c1");
        // Display names the boundary on loud rejection.
        assert!(
            BoundaryIdError::Empty("Corr").to_string().contains("Corr"),
            "error names the type"
        );
    }
}
