use crate::protocol::{Message, Intent};
use anyhow::Result;
use std::sync::Arc;

/// A trait for adapters that bridge existing coding agents into Toron.
#[async_trait::async_trait]
pub trait AgentAdapter: Send + Sync {
    /// The name of the agent being bridged (e.g., "gemini-cli").
    fn agent_name(&self) -> &str;
    
    /// Called when a message is received from the Hub for this agent.
    async fn on_hub_message(&self, msg: Message) -> Result<()>;
    
    /// Starts the adapter loop.
    async fn start(&self, cx: &asupersync::Cx) -> asupersync::Outcome<(), anyhow::Error>;
}

pub mod mcp;
pub mod sidecar;
