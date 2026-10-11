// governed-by: ADR-0040, ADR-0041
//! Envelope v1 (stack-comms redesign §2.1): the one shape every comms
//! message has, whatever its contract.
//!
//! * [`MsgId`] is the Law L16 producer triple `(producer, epoch, seq)` and
//!   the idempotency key: the inbox log appends one triple once.
//! * `from` and `to` are **npubs** (64 lowercase hex) or a group id, never a
//!   pin name. Names are resolved once at the CLI edge (ADR-0037).
//! * `contract` names the body's type (`task@1`, `notify@1`, …); an unknown
//!   major is a typed [`Reject::UnsupportedContract`], never a guess.
//!
//! This module is plain data plus validation. It names nothing from the
//! kernel; the log ([`crate::inbox`]) carries an envelope as the L16 payload.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::contracts;
use crate::delivery::Reject;

/// Envelope schema major this build reads and writes.
pub const ENVELOPE_V: u32 = 1;

/// The L16 producer triple; the message's identity and idempotency key.
#[derive(
    Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct MsgId {
    /// The producing npub.
    pub producer: String,
    /// The producer's epoch; a restarted sender starts a higher one.
    pub epoch: u64,
    /// Sequence within the epoch, gap-free from 0.
    pub seq: u64,
}

impl MsgId {
    /// Stable text key `producer:epoch:seq` (the `msg_key` column, and the
    /// `Durability::Durable` key of the send effect).
    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.producer, self.epoch, self.seq)
    }

    /// Parse a key produced by [`MsgId::key`].
    pub fn parse_key(key: &str) -> Option<Self> {
        let mut parts = key.rsplitn(3, ':');
        let seq = parts.next()?.parse().ok()?;
        let epoch = parts.next()?.parse().ok()?;
        let producer = parts.next()?.to_string();
        Some(Self {
            producer,
            epoch,
            seq,
        })
    }
}

impl fmt::Display for MsgId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key())
    }
}

/// Who a message is for. Never a pin name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Recipient {
    /// One agent, by npub (64 lowercase hex).
    Npub(String),
    /// A group stream (Notify only in this slice, redesign §12).
    Group(String),
}

impl Recipient {
    /// The stream key this recipient reads (`npub` or `group:<id>`).
    pub fn stream(&self) -> String {
        match self {
            Self::Npub(n) => n.clone(),
            Self::Group(g) => format!("group:{g}"),
        }
    }
}

/// Message class (redesign §3.1 "which classes stop where").
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// Terminal at Delivered.
    Notify,
    /// Terminal at Acknowledged.
    Request,
    /// Terminal at Acknowledged; links `causation`.
    Reply,
    /// Runs to Proved.
    Task,
    /// Terminal at Acknowledged; supersedes or cancels its target.
    Control,
    /// Terminal at Delivered.
    Receipt,
}

/// A contract name and major, written `name@major` (e.g. `task@1`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContractId {
    /// Contract name (`task`, `notify`, …).
    pub name: String,
    /// Breaking-change major.
    pub major: u32,
}

impl ContractId {
    /// `name@major`.
    pub fn new(name: impl Into<String>, major: u32) -> Self {
        Self {
            name: name.into(),
            major,
        }
    }

    /// Parse `name@major`.
    pub fn parse(s: &str) -> Option<Self> {
        let (name, major) = s.split_once('@')?;
        if name.is_empty() {
            return None;
        }
        Some(Self::new(name, major.parse().ok()?))
    }
}

impl fmt::Display for ContractId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.major)
    }
}

/// A2A `contextId`: the session / swarm / run a message belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct ContextId(pub String);

/// A2A `taskId`: the stable unit of work.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct TaskId(pub String);

/// Envelope v1 (redesign §2.1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// Envelope schema major ([`ENVELOPE_V`]).
    pub v: u32,
    /// Identity and idempotency key.
    pub msg: MsgId,
    /// Sender npub; must equal `msg.producer`.
    pub from: String,
    /// Recipient npub or group. Never a pin name.
    pub to: Recipient,
    /// Message class.
    pub class: Class,
    /// Body contract.
    pub contract: ContractId,
    /// Context (session / swarm / run).
    pub context: ContextId,
    /// Unit of work, when the message is about one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    /// Assignment generation (fencing token).
    #[serde(default, rename = "gen")]
    pub generation: u32,
    /// The message this one answers or derives from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation: Option<MsgId>,
    /// Absolute deadline (µs since the Unix epoch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ts_us: Option<i64>,
    /// Claim lease length for a Task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_ms: Option<u32>,
    /// Body, typed by `contract`.
    pub body: serde_json::Value,
}

/// True for a 64-character lowercase-hex npub (toron's stored pubkey form).
pub fn is_npub_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Envelope {
    /// Validate everything that does not need the log: schema major,
    /// addressing (npubs, never names), sender = producer, contract known
    /// and consistent with the class, body well-typed for the contract.
    pub fn validate(&self) -> Result<(), Reject> {
        if self.v != ENVELOPE_V {
            return Err(Reject::UnsupportedEnvelope { v: self.v });
        }
        if !is_npub_hex(&self.msg.producer) {
            return Err(Reject::BadAddress {
                field: "msg.producer",
                value: self.msg.producer.clone(),
            });
        }
        if self.from != self.msg.producer {
            return Err(Reject::SenderIsNotProducer {
                from: self.from.clone(),
                producer: self.msg.producer.clone(),
            });
        }
        match &self.to {
            Recipient::Npub(n) if !is_npub_hex(n) => {
                return Err(Reject::BadAddress {
                    field: "to",
                    value: n.clone(),
                });
            }
            Recipient::Group(g) if g.trim().is_empty() => {
                return Err(Reject::BadAddress {
                    field: "to",
                    value: g.clone(),
                });
            }
            Recipient::Group(_) if self.class != Class::Notify => {
                return Err(Reject::GroupNotifyOnly { class: self.class });
            }
            _ => {}
        }
        if self.context.0.trim().is_empty() {
            return Err(Reject::BadEnvelope("context is empty".into()));
        }
        contracts::check(self)?;
        Ok(())
    }

    /// Parse and validate one envelope from JSON.
    pub fn from_json(s: &str) -> Result<Self, Reject> {
        let env: Self = serde_json::from_str(s).map_err(|e| Reject::BadEnvelope(e.to_string()))?;
        env.validate()?;
        Ok(env)
    }

    /// The JSON Schema of the envelope (for docs and external producers).
    pub fn json_schema() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(Envelope)).expect("schema serializes")
    }
}
