// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! toron-mcp: MCP server surface (rmcp) for the toron mailbox.
//!
//! Two transports from one [`server::ToronServer`] handler:
//! - stdio (`toron serve --mcp-stdio`)
//! - Streamable HTTP mounted on axum at `/mcp` (`toron serve --mcp-http <addr>`)
//!
//! Protocol: 2026-07-28 preferred, 2025-11-25 fallback (rmcp auto-negotiates).
//! `bd-led.7.2` wires the 38 AM-Rust tools; `bd-led.7.3` exposes the 25 resources.

pub mod blob_serve;
pub mod resources;
pub mod responses;
pub mod server;
pub mod tools;
pub mod workflow_host;

pub use resources::RESOURCES;
pub use server::{ToronServer, demo_mailbox, router, serve_http, serve_stdio};
pub use tools::{ToJson, ToolServer};
