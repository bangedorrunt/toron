// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Nostr layer: event build/sign/verify + frozen kind registry.
//!
//! Pure — no I/O. Every bus publish goes through [`event::build_event`].
//!
//! rust-nostr 0.45 split protocol types into the `nostr` crate (no longer
//! nested under `nostr_sdk::nostr`). Types are re-exported here so the rest
//! of toron-core can `use crate::nostr::{Event, Keys, ...}` without colliding
//! with the external `::nostr` crate path.

pub use ::nostr::event::{
    Event, EventBuilder, EventId, FinalizeEvent, Kind, Signature, Tag, Tags, UnsignedEvent,
};
pub use ::nostr::filter::{Filter, MatchEventOptions, SingleLetterTag};
pub use ::nostr::key::{Keys, PublicKey, SecretKey};
pub use ::nostr::message::{ClientMessage, RelayMessage, SubscriptionId};
pub use ::nostr::types::Timestamp;

pub mod event;
pub mod gift_wrap;
pub mod identity;
pub mod kinds;
pub mod presence;
pub mod proof;

#[cfg(test)]
pub mod proptests;
