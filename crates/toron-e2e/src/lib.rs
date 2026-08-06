// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! toron-e2e: scenario harness (ADR Harness spec). Deterministic, no real
//! network. In-process daemon = InProcessBus + MockBackend + temp Git/SQLite.
//!
//! Feature flags:
//! - `p1`: SC-1..SC-8 + SC-14a (v1 gate)
//! - `p2`: SC-9..SC-13 (Phase-2 gate)
//! - default (none): skeleton + smoke only.

pub mod harness;
pub mod scenario;
