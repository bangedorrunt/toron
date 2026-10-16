// governed-by: ADR-0041
//! `goal@1`: the objective a context is working (Phase 4a). Class
//! [`crate::envelope::Class::Control`].
//!
//! `join`, `budget`, and `deadline` are not this slice. A later slice accepts
//! `join = all`; this one refuses those fields as unknown.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;

/// The body of a `goal@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GoalV1 {
    /// What the work is for. Non-empty.
    pub objective: String,
    /// Checks that show the objective is met.
    pub acceptance: Vec<String>,
    /// Limits the work stays inside.
    pub constraints: Vec<String>,
}

impl GoalV1 {
    /// The objective is the whole point of the fact; blank is not one.
    pub fn validate(&self) -> Result<(), Reject> {
        if self.objective.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "goal@1".into(),
                error: "objective must be non-empty".into(),
            });
        }
        Ok(())
    }
}
