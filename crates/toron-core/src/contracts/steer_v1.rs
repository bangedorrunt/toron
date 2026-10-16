// governed-by: ADR-0041
//! `steer@1`: change a task in flight (Phase 4a). Class
//! [`crate::envelope::Class::Control`].
//!
//! * `op` is one of `redirect | pause | resume | cancel`. Anything else is
//!   [`Reject::BadBody`], not a loose accept.
//! * `priority` is `normal` or `urgent`.
//! * `redirect` carries `delta`, a JSON object of at most 8 keys drawn only
//!   from `goal`, `acceptance`, `scope`, `priority`, `budget`. Any other op
//!   omits `delta` (absent or null).
//! * `env.task` is the task being steered. `env.generation` (wire `gen`) is
//!   the task generation this steer was issued at.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::delivery::Reject;
use crate::envelope::Envelope;

/// Keys a `redirect` delta may name. An unknown key is [`Reject::BadBody`].
const DELTA_KEYS: &[&str] = &["goal", "acceptance", "scope", "priority", "budget"];
/// A redirect delta is a small patch, not a document.
const DELTA_MAX_KEYS: usize = 8;

/// The body of a `steer@1` envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SteerV1 {
    /// `redirect`, `pause`, `resume`, or `cancel`.
    pub op: String,
    /// `normal` or `urgent`.
    pub priority: String,
    /// The redirect patch. Absent for every other op.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta: Option<serde_json::Value>,
    /// When the recipient should have acked, microseconds, if the sender set one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_by: Option<u64>,
    /// The lead epoch this steer speaks for. Fenced once a lead fact exists.
    pub sup_epoch: u64,
}

impl SteerV1 {
    /// `op`, `priority`, `delta`, and `env.task`. Generation is the task gen
    /// already on the envelope; this does not invent a second field for it.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        let bad = |error: &str| Reject::BadBody {
            contract: "steer@1".into(),
            error: error.into(),
        };
        if env.task.is_none() {
            return Err(Reject::bad_envelope(
                "steer@1 needs envelope.task (the TaskId it steers)",
            ));
        }
        match self.op.as_str() {
            "redirect" | "pause" | "resume" | "cancel" => {}
            _ => return Err(bad("unknown op")),
        }
        if !matches!(self.priority.as_str(), "normal" | "urgent") {
            return Err(bad("priority must be normal or urgent"));
        }
        match self.op.as_str() {
            "redirect" => validate_delta(self.delta.as_ref(), bad)?,
            _ if self.delta.is_some() => {
                return Err(bad("delta is only valid on redirect"));
            }
            _ => {}
        }
        Ok(())
    }
}

fn validate_delta(
    delta: Option<&serde_json::Value>,
    bad: impl Fn(&str) -> Reject,
) -> Result<(), Reject> {
    let Some(delta) = delta else {
        return Err(bad("redirect requires delta"));
    };
    let Some(obj) = delta.as_object() else {
        return Err(bad("redirect delta must be an object"));
    };
    if obj.len() > DELTA_MAX_KEYS {
        return Err(bad("redirect delta has more than 8 keys"));
    }
    for key in obj.keys() {
        if !DELTA_KEYS.contains(&key.as_str()) {
            return Err(bad("redirect delta has an unknown key"));
        }
    }
    Ok(())
}
