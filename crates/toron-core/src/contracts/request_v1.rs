// governed-by: ADR-0041
//! `request@1`: ask for something (Phase 4.3). Class [`crate::envelope::Class::Request`].
//!
//! The body is one object. Unknown body keys are [`crate::delivery::Reject::BadBody`].
//! The envelope already denies unknown fields.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `request@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RequestV1 {
    /// What the sender is asking for. Required and non-empty.
    pub goal: String,
}

impl RequestV1 {
    /// A request must name a goal.
    pub fn validate(&self, _env: &Envelope) -> Result<(), Reject> {
        if self.goal.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "request@1".into(),
                error: "goal must be non-empty".into(),
            });
        }
        Ok(())
    }
}
