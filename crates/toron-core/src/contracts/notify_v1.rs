// governed-by: ADR-0041
//! `notify@1`: information with no obligation; terminal at Delivered.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;

/// The body of a `notify@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NotifyV1 {
    /// The notice.
    pub text: String,
}

impl NotifyV1 {
    /// A notice must say something.
    pub fn validate(&self) -> Result<(), Reject> {
        if self.text.trim().is_empty() {
            return Err(Reject::BadBody {
                contract: "notify@1".into(),
                error: "text is empty".into(),
            });
        }
        Ok(())
    }
}
