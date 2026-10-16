// governed-by: ADR-0041
//! `role@1`: the role fact `role{npub, role, context, gen}` (redesign §5,
//! §6.1; Phase 4.1). A Control envelope, appended like any other, whose
//! effect is that the store records `npub` as holding `role` in the
//! envelope's `context` at `gen`.
//!
//! * `context` is the envelope's `context` (not repeated in the body).
//! * `gen` is the fence for this `(context, npub)`: a fact is accepted only
//!   if its `gen` is above the one on record, so a stale issuer cannot undo a
//!   newer grant. The Rust field is `generation`; the wire name is `gen`.
//! * The envelope is addressed to the subject (`to == npub`), so the subject
//!   finds its role in its own log.
//! * Who may append one, and the bootstrap, are the store's rule
//!   ([`crate::authority::Authority::check_grant`]): the first fact of a
//!   context is `captain`, signed by the operator key.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::authority::{ROLE_CONTRACT, Role};
use crate::delivery::Reject;
use crate::envelope::{
    Class, ContextId, ContractId, ENVELOPE_V, Envelope, MsgId, Recipient, is_npub_hex,
};

/// The body of a `role@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RoleV1 {
    /// The agent the role is for (64 lowercase hex).
    pub npub: String,
    /// The role it now holds in the envelope's context.
    pub role: Role,
    /// Fence for this `(context, npub)`; strictly increasing.
    #[serde(rename = "gen")]
    pub generation: u32,
}

impl RoleV1 {
    /// The subject is an npub and is the envelope's recipient.
    pub fn validate(&self, env: &Envelope) -> Result<(), Reject> {
        let bad = |error: &str| Reject::BadBody {
            contract: "role@1".into(),
            error: error.into(),
        };
        if !is_npub_hex(&self.npub) {
            return Err(bad("npub is not 64 lowercase hex"));
        }
        if env.to != Recipient::Npub(self.npub.clone()) {
            return Err(bad("a role fact is addressed to its subject (to == npub)"));
        }
        Ok(())
    }

    /// The envelope that carries this fact: from the producer of `msg`, to
    /// the subject, class Control, contract `role@1`, in `context`.
    pub fn into_envelope(self, msg: MsgId, context: ContextId) -> Envelope {
        let body = serde_json::to_value(&self).expect("role body serializes");
        Envelope {
            v: ENVELOPE_V,
            from: msg.producer.clone(),
            msg,
            to: Recipient::Npub(self.npub),
            class: Class::Control,
            contract: ContractId::new(ROLE_CONTRACT, 1),
            context,
            task: None,
            generation: 0,
            causation: None,
            deadline_ts_us: None,
            lease_ms: None,
            body,
        }
    }
}
