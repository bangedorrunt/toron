// governed-by: ADR-0041
//! `task@1`: one unit of work for one recipient (redesign §7). Replaces the
//! prose brief that used to be typed into a pane.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `task@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskV1 {
    /// One-line title.
    pub title: String,
    /// What to do and why.
    pub brief: String,
    /// Paths the assignee may touch (the reservation scope).
    #[serde(default)]
    pub scope_paths: Vec<String>,
    /// Acceptance checks, each a command or a test name.
    #[serde(default)]
    pub acceptance: Vec<String>,
    /// The bead this task works, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead: Option<String>,
}

impl TaskV1 {
    /// A task must name a unit of work and carry a non-empty title/brief.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        if env.task.is_none() {
            return Err(Reject::bad_envelope(
                "task@1 needs envelope.task (the TaskId)",
            ));
        }
        if self.title.trim().is_empty() || self.brief.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "task@1".into(),
                error: "title and brief must be non-empty".into(),
            });
        }
        Ok(())
    }
}
