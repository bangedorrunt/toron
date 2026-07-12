pub mod core;
pub mod discovery;
pub mod transport;

pub use core::{HubCore, HubRole};
pub use discovery::{DiscoveryManager, HubInfo};
pub use transport::{Transport, WsTransport, QuicTransport};
// governed-by: ADR-0001
