use crate::protocol::{Message, Intent};
use crate::adapter::AgentAdapter;
use anyhow::Result;
use std::process::Stdio;

/// Sidecar adapter.
/// Spawns the agent as a child process and communicates via stdio or environment.
pub struct SidecarAdapter {
    agent_name: String,
    command: String,
}

impl SidecarAdapter {
    pub fn new(agent_name: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            agent_name: agent_name.into(),
            command: command.into(),
        }
    }
}

#[async_trait::async_trait]
impl AgentAdapter for SidecarAdapter {
    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    async fn on_hub_message(&self, msg: Message) -> Result<()> {
        log::info!("[Sidecar Adapter] Pushing hub message to {} process: {:?}", self.agent_name, msg.id);
        // We'd write to the child's stdin here
        Ok(())
    }

    async fn start(&self, cx: &asupersync::Cx) -> asupersync::Outcome<(), anyhow::Error> {
        log::info!("Starting Sidecar Adapter for {} (Cmd: {})...", self.agent_name, self.command);
        
        // Spawn the agent process
        // Note: asupersync::process::Command integration pending, using std for now
        loop {
            let _ = cx.checkpoint();
            asupersync::time::sleep(cx.now(), std::time::Duration::from_secs(1)).await;
        }
    }
}
