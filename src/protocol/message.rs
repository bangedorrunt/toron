use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents the intent of an agent's message.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Intent {
    /// Agent is joining a topic.
    Join,
    /// Agent is leaving a topic.
    Leave,
    /// Agent is proposing a new action or thought.
    Propose,
    /// Agent is providing information or executing a command.
    Inform,
    /// Agent is requesting clarification or asking a question.
    Query,
    /// Agent is confirming a proposal or action.
    Acknowledge,
    /// A system-level error or rejection.
    Error,
}

/// The core semantic message exchanged between agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Unique identifier for the message.
    pub id: Uuid,
    /// The topic or context this message belongs to.
    pub topic: String,
    /// Identifier of the sender agent.
    pub sender: String,
    /// The semantic intent of the message.
    pub intent: Intent,
    /// Free-form payload, typically JSON or Markdown.
    pub payload: serde_json::Value,
    /// Timestamp in milliseconds since UNIX epoch.
    pub timestamp_ms: u64,
    /// ID of the message this is replying to, if any.
    pub reply_to: Option<Uuid>,
}

impl Message {
    /// Creates a new semantic message.
    pub fn new(
        topic: impl Into<String>,
        sender: impl Into<String>,
        intent: Intent,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic: topic.into(),
            sender: sender.into(),
            intent,
            payload,
            timestamp_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            reply_to: None,
        }
    }

    /// Sets the message this one is replying to.
    pub fn with_reply_to(mut self, reply_to: Uuid) -> Self {
        self.reply_to = Some(reply_to);
        self
    }
}
// governed-by: ADR-0001
