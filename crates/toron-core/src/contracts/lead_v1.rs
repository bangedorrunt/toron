// governed-by: ADR-0041
//! `lead@1`: who holds the lead lease, and at which `sup_epoch` (Phase 4a.3).
//! Class [`crate::envelope::Class::Control`].
//!
//! The first fact for a context is operator-signed. A later fact is operator
//! or captain, and only if its `sup_epoch` is strictly above the one on
//! record. A steer names the same epoch and is sent by the `holder` key: a
//! steer from any other epoch is [`crate::delivery::Reject::StaleSupEpoch`],
//! one from another key at the current epoch is
//! [`crate::delivery::Reject::NotLeadHolder`], and with no lead row only the
//! operator may steer; anyone else is [`crate::delivery::Reject::NoLead`] (F7).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;

/// The body of a `lead@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LeadV1 {
    /// Who holds the lead (an npub, not checked as one here: the fact is a name
    /// the log records, and the authority check is on the signer). A steer is
    /// accepted only when its `from` equals this string exactly, so a holder
    /// that is not the steering key's npub fences every steer out.
    pub holder: String,
    /// The epoch steers must name while this fact is current.
    pub sup_epoch: u64,
}

impl LeadV1 {
    /// A lead with no holder names nobody.
    pub fn validate(&self) -> Result<(), Reject> {
        if self.holder.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "lead@1".into(),
                error: "holder must be non-empty".into(),
            });
        }
        Ok(())
    }
}
