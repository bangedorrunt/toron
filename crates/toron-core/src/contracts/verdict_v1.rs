// governed-by: ADR-0041
//! `verdict@1`: a verifier's verdict on a task (redesign §6.1; Phase 4.1).
//!
//! The wire shape is fixed: `task` and `gen` ride on the envelope, the body is
//! `{pass, evidence_refs}`. Phase 4.3 did not change it. An appended verdict
//! is a log fact only and moves no task until a later phase wires the effect.

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
