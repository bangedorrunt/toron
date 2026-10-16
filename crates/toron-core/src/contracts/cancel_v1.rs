// governed-by: ADR-0041
//! `cancel@1`: stop a task (Phase 4.3). Class [`crate::envelope::Class::Control`].
//!
//! The body may be an empty object. `env.task` is required.
//!
//! Envelope `generation` is `#[serde(default, rename = "gen")]`, so a missing
//! `gen` and `gen: 0` are the same value. Generation 0 is legal. This contract
//! does not try to tell them apart; it requires `task`, not a distinguishable gen.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of a `cancel@1` envelope. Empty is valid.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CancelV1 {}

impl CancelV1 {
    /// A cancel names the task it stops. Generation 0 is allowed.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        if env.task.is_none() {
            return Err(Reject::bad_envelope(
                "cancel@1 needs envelope.task (the TaskId it stops)",
            ));
        }
        Ok(())
    }
}
