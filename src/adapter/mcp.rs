use crate::protocol::{Message, Intent};
use crate::adapter::AgentAdapter;
use anyhow::Result;
use serde_json::json;

/// MCP (Model Context Protocol) adapter.
/// Allows agents like Gemini CLI or Claude to talk to Toron as an MCP server.
pub struct McpAdapter {
    agent_name: String,
}

impl McpAdapter {
    pub fn new(agent_name: impl Into<String>) -> Self {
        Self { agent_name: agent_name.into() }
    }
}

#[async_trait::async_trait]
impl AgentAdapter for McpAdapter {
    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    async fn on_hub_message(&self, msg: Message) -> Result<()> {
        // In MCP, we'd emit a notification or event that the agent can see.
        log::info!("[MCP Adapter] Forwarding hub message to {}: {:?}", self.agent_name, msg.id);
        Ok(())
    }

    async fn start(&self, cx: &asupersync::Cx) -> asupersync::Outcome<(), anyhow::Error> {
        log::info!("Starting MCP Adapter for {}...", self.agent_name);
        
        // MCP logic: listen on stdin/stdout and expose Toron Hub as tools/resources
        loop {
            let _ = cx.checkpoint();
            asupersync::time::sleep(cx.now(), std::time::Duration::from_secs(1)).await;
        }
    }
}
