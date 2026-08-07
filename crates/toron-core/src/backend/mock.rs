// governed-by: ADR-0003
#![forbid(unsafe_code)]

// bd-led.6.1: in-memory `Backend` for unit/mailbox tests — no herdr socket.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::Backend;
use super::error::BackendError;
use super::types::{AgentStatus, BackendEvent, PaneId, SpawnOptions};

#[derive(Debug, Clone, Default)]
struct MockState {
    spawned: Vec<(SpawnOptions, PaneId)>,
    prompts: Vec<(PaneId, String)>,
    keys: Vec<(PaneId, String)>,
    statuses: Vec<AgentStatus>,
    tx: Option<mpsc::Sender<BackendEvent>>,
}

/// Deterministic in-memory backend. Every call records its args into
/// `state` so tests can assert the mailbox drove the backend correctly.
#[derive(Debug, Clone, Default)]
pub struct MockBackend {
    state: Arc<Mutex<MockState>>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState::default())),
        }
    }

    pub fn spawned(&self) -> Vec<(SpawnOptions, PaneId)> {
        self.state.lock().spawned.clone()
    }

    pub fn prompts(&self) -> Vec<(PaneId, String)> {
        self.state.lock().prompts.clone()
    }

    pub fn keys(&self) -> Vec<(PaneId, String)> {
        self.state.lock().keys.clone()
    }

    /// Set the status the next `wait` returns.
    pub fn push_status(&self, status: AgentStatus) {
        self.state.lock().statuses.push(status);
    }

    /// Emit a push event to subscribers.
    pub async fn emit(&self, event: BackendEvent) {
        let tx = self.state.lock().tx.clone();
        if let Some(tx) = tx {
            let _ = tx.send(event).await;
        }
    }
}

impl Backend for MockBackend {
    async fn spawn(&self, opts: &SpawnOptions) -> Result<PaneId, BackendError> {
        let pane = PaneId(format!("pane-{}", self.state.lock().spawned.len()));
        self.state.lock().spawned.push((opts.clone(), pane.clone()));
        Ok(pane)
    }

    async fn prompt(&self, pane: &PaneId, input: &str) -> Result<(), BackendError> {
        self.state
            .lock()
            .prompts
            .push((pane.clone(), input.to_string()));
        Ok(())
    }

    async fn wait(&self, _pane: &PaneId) -> Result<AgentStatus, BackendError> {
        let mut state = self.state.lock();
        if state.statuses.is_empty() {
            return Ok(AgentStatus::Unknown);
        }
        Ok(state.statuses.remove(0))
    }

    async fn read(&self, _pane: &PaneId) -> Result<String, BackendError> {
        Ok(String::new())
    }

    async fn send_keys(&self, pane: &PaneId, keys: &str) -> Result<(), BackendError> {
        self.state
            .lock()
            .keys
            .push((pane.clone(), keys.to_string()));
        Ok(())
    }

    async fn events(&self) -> Result<mpsc::Receiver<BackendEvent>, BackendError> {
        let (tx, rx) = mpsc::channel(64);
        self.state.lock().tx = Some(tx);
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn trait_generic_compilable_and_spawns() {
        // Native-AFIT proof: Backend methods are callable through a generic
        // bound (static dispatch). Trait-object dispatch is intentionally not
        // asserted — AFIT traits are not dyn-compatible (measured, nightly
        // 1.97); `Bus`/`ProjectionSink` keep async-trait for their `dyn` uses.
        async fn drive<B: Backend>(backend: &B, opts: &SpawnOptions) -> String {
            backend
                .spawn(opts)
                .await
                .expect("spawn")
                .as_str()
                .to_string()
        }
        let backend = MockBackend::new();
        let opts = SpawnOptions {
            project: "demo".into(),
            agent_name: "PiAlpha".into(),
            program: "pi".into(),
            cwd: None,
        };
        assert_eq!(drive(&backend, &opts).await, "pane-0");
    }

    #[tokio::test]
    async fn mock_records_prompts_and_keys() {
        let backend = MockBackend::new();
        let opts = SpawnOptions::default();
        let pane = backend.spawn(&opts).await.expect("spawn");
        backend.prompt(&pane, "hello").await.expect("prompt");
        backend.send_keys(&pane, "ctrl-c").await.expect("keys");

        assert_eq!(backend.prompts(), vec![(pane.clone(), "hello".to_string())]);
        assert_eq!(backend.keys(), vec![(pane, "ctrl-c".to_string())]);
    }

    #[tokio::test]
    async fn mock_wait_returns_pushed_status_then_unknown() {
        let backend = MockBackend::new();
        backend.push_status(AgentStatus::Done);
        let pane = PaneId("pane-0".into());
        assert_eq!(backend.wait(&pane).await.unwrap(), AgentStatus::Done);
        assert_eq!(backend.wait(&pane).await.unwrap(), AgentStatus::Unknown);
    }

    #[tokio::test]
    async fn mock_events_delivers_pushes() {
        let backend = MockBackend::new();
        let mut rx = backend.events().await.expect("events");
        backend
            .emit(BackendEvent::AgentStatusChanged {
                pane: PaneId("pane-0".into()),
                status: AgentStatus::Working,
            })
            .await;
        let evt = rx.recv().await.expect("event");
        assert_eq!(
            evt,
            BackendEvent::AgentStatusChanged {
                pane: PaneId("pane-0".into()),
                status: AgentStatus::Working,
            }
        );
    }

    #[tokio::test]
    async fn tmux_unsupported_marker_roundtrips_code() {
        use crate::backend::error::BackendErrorCode;
        let err = BackendError::unsupported("TmuxBackend on hold (ADR G9)");
        assert_eq!(err.code(), &BackendErrorCode::Unsupported);
        let s = err.code().as_str();
        assert_eq!(s, "unsupported");
        assert_eq!(
            s.parse::<BackendErrorCode>().unwrap(),
            BackendErrorCode::Unsupported
        );
    }

    #[tokio::test]
    async fn unknown_machine_code_passthrough() {
        use crate::backend::error::BackendErrorCode;
        let s = "agent_prompt_stalled";
        assert_eq!(
            s.parse::<BackendErrorCode>().unwrap(),
            BackendErrorCode::AgentPromptStalled
        );
        let opaque = "custom_code_9".parse::<BackendErrorCode>().unwrap();
        assert_eq!(opaque.as_str(), "custom_code_9");
    }
}
