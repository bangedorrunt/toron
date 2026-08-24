// governed-by: ADR-0003
#![forbid(unsafe_code)]

pub mod archive;
pub mod backend;
pub mod bus;
pub mod config;
pub mod index;
pub mod keys;
pub mod mailbox;
pub mod nostr;

/// Client/relay pool. rust-nostr 0.45 no longer nests protocol types under
/// `nostr_sdk::nostr`; use `nostr_sdk::prelude::*` or the `nostr` crate.
pub use nostr_sdk;
