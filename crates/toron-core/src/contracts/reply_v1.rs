// governed-by: ADR-0041
//! `reply@1`: an answer to one message (Phase 4.3). Class [`crate::envelope::Class::Reply`].
//!
//! `env.causation` is required: a reply names the message it answers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `reply@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReplyV1 {
    /// The answer.
    pub text: String,
}

impl ReplyV1 {
    /// A reply answers a message and must say something.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        if env.causation.is_none() {
            return Err(Reject::bad_envelope(
                "reply@1 needs envelope.causation (the message it answers)",
            ));
        }
        if self.text.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "reply@1".into(),
                error: "text must be non-empty".into(),
            });
        }
        Ok(())
    }
}
