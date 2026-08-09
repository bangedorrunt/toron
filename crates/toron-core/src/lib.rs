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

/// Re-export of the pinned nostr-sdk (single-version coupling for embedders:
/// toron-core owns the nostr stack; flywheel/toron-cli name event/tag/filter
/// types through this path instead of adding their own nostr-sdk dep).
pub use nostr_sdk;
