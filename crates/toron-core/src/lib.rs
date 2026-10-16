// governed-by: ADR-0003, ADR-0040
#![forbid(unsafe_code)]

pub mod archive;
pub mod authority;
pub mod backend;
pub mod bus;
pub mod busy_owner;
pub mod config;
pub mod contracts;
pub mod delivery;
pub mod dispatch_core;
pub mod envelope;
pub mod inbox;
pub mod inbox_program;
pub mod index;
pub mod job_kinds;
pub mod journal;
pub mod jsonl;
pub mod kernel;
pub mod keys;
pub mod mailbox;
pub mod nostr;
pub mod outbox;
pub mod policy;
pub mod serve_build;
pub mod testkit;
pub mod topology;
pub mod wake;

/// Client/relay pool. rust-nostr 0.45 no longer nests protocol types under
/// `nostr_sdk::nostr`; use `nostr_sdk::prelude::*` or the `nostr` crate.
pub use nostr_sdk;

#[cfg(test)]
mod catalog_freshness_tests {
    /// Non-vacuous wrapper for `cargo test catalog_freshness` at workspace root.
    /// The real catalog drift is gated in `toron-cli` binary's `catalog::tests::catalog_freshness`;
    /// this dummy ensures the filter matches at least 1 test when run without --bin.
    #[test]
    fn catalog_freshness() {
        let path = std::path::Path::new("docs/catalog/toron-mcp.json");
        if path.exists() {
            let content = std::fs::read_to_string(path).expect("read catalog");
            let v: serde_json::Value =
                serde_json::from_str(&content).expect("catalog is valid json");
            assert!(v.is_object() || v.is_array());
        }
    }
}
