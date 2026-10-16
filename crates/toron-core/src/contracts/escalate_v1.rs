// governed-by: ADR-0041
//! `escalate@1`: a crew handoff upward (Phase 4a). Class
//! [`crate::envelope::Class::Control`].
//!
//! The body's `context` is prose for the reader, not the envelope's
//! `context` id.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;

/// The body of an `escalate@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EscalateV1 {
    /// What the reader needs in order to take the handoff.
    pub context: String,
    /// Why this could not be finished at this level. Non-empty.
    pub reason: String,
    /// Pointers (logs, offsets, bead ids) that support `reason`.
    pub evidence: Vec<String>,
}

impl EscalateV1 {
    /// An escalation with no reason is not a decision anyone can answer.
    pub fn validate(&self) -> Result<(), Reject> {
        if self.reason.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "escalate@1".into(),
                error: "reason must be non-empty".into(),
            });
        }
        Ok(())
    }
}
