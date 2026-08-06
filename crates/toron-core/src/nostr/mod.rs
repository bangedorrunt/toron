// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Nostr layer: event build/sign/verify + frozen kind registry.
//!
//! Pure — no I/O. Every bus publish goes through [`event::build_event`].

pub mod event;
pub mod gift_wrap;
pub mod identity;
pub mod kinds;

#[cfg(test)]
pub mod proptests;
