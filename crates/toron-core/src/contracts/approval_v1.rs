// governed-by: ADR-0041
//! `approval@1`: a captain's decision on a run (Phase 4.3). Class
//! [`crate::envelope::Class::Control`].
//!
//! The run id is `env.task`. There is no separate envelope field for it.
//! `decision` may be omitted (it defaults to empty). Who may send one stays
//! the authority table: captain true, supervisor false.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// The body of an `approval@1` envelope.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovalV1 {
    /// The captain's decision. Omitted on the wire means empty.
    #[serde(default)]
    pub decision: String,
}

impl ApprovalV1 {
    /// `env.task` is the run id this approval decides.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        if env.task.is_none() {
            return Err(Reject::bad_envelope(
                "approval@1 needs envelope.task (the run id)",
            ));
        }
        Ok(())
    }
}
