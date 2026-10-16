// governed-by: ADR-0041
//! `verdict@1`: a verifier's verdict on a task (redesign §6.1; Phase 4.1).
//!
//! PROVISIONAL SHAPE. Phase 4.1 needs a Control-class contract that the
//! authority table can refuse a crew for, so this lands the minimum: the task
//! and `gen` ride on the envelope (`task`, `gen`), the body is the outcome.
//! Phase 4.3 owns the final shape and the effect (wiring the verdict into the
//! delivery state machine's `Verdict` event); until then an appended verdict
//! is a log fact only and moves no task.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `verdict@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct VerdictV1 {
    /// True = the evidence proves the work.
    pub pass: bool,
    /// What the verdict rests on (artifact refs, test names, commits).
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

impl VerdictV1 {
    /// A verdict is about a task.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        if env.task.is_none() {
            return Err(Reject::bad_envelope(
                "verdict@1 needs envelope.task (the TaskId it judges)",
            ));
        }
        Ok(())
    }
}
