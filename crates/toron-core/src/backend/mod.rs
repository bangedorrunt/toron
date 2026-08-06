// governed-by: ADR-0003
#![forbid(unsafe_code)]

// bd-led.6.1: `Backend` trait — supervisor abstraction for agent panes.
// herdr is the default backend (ported in bd-led.6.2); tmux returns
// Unsupported until un-held (ADR-0003 G9).

pub mod error;
pub mod herdr;
pub mod mock;
pub mod types;

pub use error::{BackendError, BackendErrorCode};
pub use types::{AgentStatus, BackendEvent, PaneId, SpawnOptions};

use tokio::sync::mpsc;

/// In-flight backend event stream. Backends push `BackendEvent`s here.
pub type EventStream = mpsc::Receiver<BackendEvent>;

/// Supervisor backend: spawn/prompt/wait/read/send_keys/events.
/// Errors are typed with machine codes — classify on `BackendError::code`,
/// never message text.
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    /// Spawn an agent pane in the backend and return its pane id.
    async fn spawn(&self, opts: &SpawnOptions) -> Result<PaneId, BackendError>;

    /// Send a prompt/input line to the pane.
    async fn prompt(&self, pane: &PaneId, input: &str) -> Result<(), BackendError>;

    /// Wait for the agent to reach a terminal-ish state; returns current status.
    async fn wait(&self, pane: &PaneId) -> Result<AgentStatus, BackendError>;

    /// Read buffered output from the pane.
    async fn read(&self, pane: &PaneId) -> Result<String, BackendError>;

    /// Inject raw key/control sequences.
    async fn send_keys(&self, pane: &PaneId, keys: &str) -> Result<(), BackendError>;

    /// Subscribe to backend push events.
    async fn events(&self) -> Result<EventStream, BackendError>;
}
