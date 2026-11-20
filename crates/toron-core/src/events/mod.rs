// governed-by: ADR-0039
#![forbid(unsafe_code)]

//! MCP Events catalogue (ADR-0039 D1/D2, plan U1).
//!
//! The catalogue is derived only from facts already on the bus. The classifier
//! is pure: no bus, clock or I/O, so a wrong mapping is a failed unit test,
//! not a live-only surprise.

pub mod catalogue;
pub mod dispatcher;
pub mod kernel;
pub mod principal;
pub mod store;
pub mod stream;

pub use catalogue::{CatalogueEvent, EventName, Fact, catalogue, classify};
