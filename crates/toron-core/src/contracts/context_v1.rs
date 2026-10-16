// governed-by: ADR-0041
//! `context@1`: the topology fact `context{topology, gen}` (redesign §6.2,
//! Phase 4.2). A Control envelope. The store records `topology` for the
//! envelope's `context` at `gen`.
//!
//! * `gen` fences this context: a fact is accepted only above the one on
//!   record. The Rust field is `generation`; the wire name is `gen`.
//! * Who may append one is [`crate::authority::may_set_context`]: the operator,
//!   or the context's captain or supervisor. The first fact has no captain yet,
//!   so it is operator-signed.
//! * `supervisor` is a legal value and is stored. Projecting it is `NotYet`
//!   until Phase 4a; sends in that context are refused, not run as a swarm.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::authority::CONTEXT_CONTRACT;
use crate::envelope::{Class, ContextId, ContractId, ENVELOPE_V, Envelope, MsgId, Recipient};
use crate::topology::Topology;

/// The body of a `context@1` envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextV1 {
    /// The choreography this context runs.
    pub topology: Topology,
    /// Fence for this context; strictly increasing.
    #[serde(rename = "gen")]
    pub generation: u32,
}

impl ContextV1 {
    /// The envelope that carries this fact. `to` is an npub (the issuer's
    /// own, typically): a context has no subject the way a role fact does.
    pub fn into_envelope(self, msg: MsgId, context: ContextId, to: impl Into<String>) -> Envelope {
        let body = serde_json::to_value(&self).expect("context body serializes");
        Envelope {
            v: ENVELOPE_V,
            from: msg.producer.clone(),
            msg,
            to: Recipient::Npub(to.into()),
            class: Class::Control,
            contract: ContractId::new(CONTEXT_CONTRACT, 1),
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
