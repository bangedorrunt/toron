// governed-by: ADR-0041
//! `not_ready@1`: the recipient cannot take the work yet (Phase 4.3).
//! Class [`crate::envelope::Class::Receipt`].

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `not_ready@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotReadyV1 {
    /// Why the recipient is not ready.
    pub reason: String,
}

impl NotReadyV1 {
    /// A not-ready receipt must say why.
    pub fn validate(&self, _env: &Envelope) -> Result<(), Reject> {
        if self.reason.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "not_ready@1".into(),
                error: "reason must be non-empty".into(),
            });
        }
        Ok(())
    }
}
