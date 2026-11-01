// governed-by: ADR-0003, ADR-0005
#![forbid(unsafe_code)]

// bd-led.6.1: in-memory `Backend` for unit/mailbox tests — no herdr socket.
// ADR-0005 P1: reworked to the unified sync `Backend` trait surface.

use std::sync::Arc;

use parking_lot::Mutex;
use serde_json::{Value, json};

use super::error::BackendError;
use super::types::{AgentInfo, PaneInfo, Subscription, WaitOptions, WorkspaceInfo};
use super::types::{AgentStatus, BackendEvent, PaneId, PromptLanding, SpawnOptions};

#[derive(Debug, Clone)]
struct MockState {
    spawned: Vec<(SpawnOptions, PaneId)>,
    prompts: Vec<(String, String)>,
    keys: Vec<(String, String)>,
    statuses: Vec<AgentStatus>,
    events: std::sync::mpsc::Sender<BackendEvent>,
}

impl Default for MockState {
    fn default() -> Self {
        Self {
            spawned: Vec::new(),
            prompts: Vec::new(),
            keys: Vec::new(),
            statuses: Vec::new(),
            events: std::sync::mpsc::channel::<BackendEvent>().0,
        }
    }
}

/// Deterministic in-memory backend. Every call records its args into `state`
/// so tests can assert the mailbox drove the backend correctly.
#[derive(Debug, Clone, Default)]
pub struct MockBackend {
    state: Arc<Mutex<MockState>>,
}

impl MockBackend {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MockState {
                events: std::sync::mpsc::channel::<BackendEvent>().0,
                ..Default::default()
            })),
        }
    }

    pub fn spawned(&self) -> Vec<(SpawnOptions, PaneId)> {
        self.state.lock().spawned.clone()
    }

    pub fn prompts(&self) -> Vec<(String, String)> {
        self.state.lock().prompts.clone()
    }

    pub fn keys(&self) -> Vec<(String, String)> {
        self.state.lock().keys.clone()
    }

    /// Set the status the next `agent_wait` returns.
    pub fn push_status(&self, status: AgentStatus) {
        self.state.lock().statuses.push(status);
    }

    /// Emit a push event to subscribers.
    pub fn emit(&self, event: BackendEvent) {
        let tx = self.state.lock().events.clone();
        let _ = tx.send(event);
    }
}

impl super::Backend for MockBackend {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        Ok(vec![WorkspaceInfo {
            workspace_id: "w0".into(),
            number: 0,
            label: "demo".into(),
            focused: false,
            pane_count: 0,
            tab_count: 0,
            active_tab_id: String::new(),
            agent_status: AgentStatus::Unknown,
            cwd: None,
        }])
    }

    fn workspace_create(&self, label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        Ok(WorkspaceInfo {
            workspace_id: "w0".into(),
            number: 0,
            label: label.unwrap_or("demo").into(),
            focused: false,
            pane_count: 0,
            tab_count: 0,
            active_tab_id: String::new(),
            agent_status: AgentStatus::Unknown,
            cwd: None,
        })
    }

    fn workspace_close(&self, _workspace_id: &str, _close_group: bool) -> Result<(), BackendError> {
        Ok(())
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        direction: &str,
        _cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        Ok(PaneInfo {
            pane_id: format!("{parent_pane_id}-{direction}"),
            terminal_id: String::new(),
            workspace_id: String::new(),
            tab_id: String::new(),
            focused: false,
            agent_status: AgentStatus::Unknown,
            revision: 0,
            title: None,
            cwd: None,
            agent: None,
            agent_session: None,
            display_agent: None,
            foreground_cwd: None,
            label: None,
            scroll: None,
            state_labels: serde_json::Map::new(),
            terminal_title: None,
            terminal_title_stripped: None,
            tokens: serde_json::Map::new(),
        })
    }

    fn pane_list(&self, _workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        Ok(vec![])
    }

    fn pane_close(&self, _pane_id: &str) -> Result<(), BackendError> {
        Ok(())
    }

    fn pane_run(&self, _pane_id: &str, _command: &str) -> Result<(), BackendError> {
        Ok(())
    }

    fn pane_wait_output(
        &self,
        _pane_id: &str,
        _needle: &str,
        _timeout_ms: u64,
    ) -> Result<Value, BackendError> {
        Ok(json!({ "matched_line": null, "read": "" }))
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        Ok(vec![])
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        _args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        self.state.lock().spawned.push((
            SpawnOptions {
                project: "demo".into(),
                agent_name: name.into(),
                program: kind.into(),
                cwd: None,
            },
            PaneId::new(pane_id),
        ));
        Ok(AgentInfo {
            agent: Some(name.into()),
            agent_status: AgentStatus::Working,
            cwd: None,
            display_agent: None,
            focused: false,
            foreground_cwd: None,
            interactive_ready: false,
            launch_pending: false,
            name: Some(name.into()),
            pane_id: pane_id.into(),
            revision: 0,
            state_change_seq: 0,
            completion_seq: None,
            state_labels: serde_json::Map::new(),
            tab_id: String::new(),
            terminal_id: String::new(),
            terminal_title: None,
            workspace_id: "w0".into(),
        })
    }

    fn agent_rename(&self, target: &str, new_name: Option<&str>) -> Result<(), BackendError> {
        if let Some(n) = new_name {
            self.state.lock().prompts.push((target.into(), n.into()));
        }
        Ok(())
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        _wait: Option<&WaitOptions>,
    ) -> Result<Value, BackendError> {
        self.state.lock().prompts.push((target.into(), text.into()));
        Ok(json!({ "status": "working" }))
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        _wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        self.state.lock().prompts.push((target.into(), text.into()));
        Ok(PromptLanding {
            status: Some(AgentStatus::Working),
            code: None,
        })
    }

    fn agent_wait(
        &self,
        _target: &str,
        _until: &[AgentStatus],
        _timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        let mut state = self.state.lock();
        let status = if state.statuses.is_empty() {
            AgentStatus::Unknown
        } else {
            state.statuses.remove(0)
        };
        Ok(AgentInfo {
            agent: None,
            agent_status: status,
            cwd: None,
            display_agent: None,
            focused: false,
            foreground_cwd: None,
            interactive_ready: false,
            launch_pending: false,
            name: None,
            pane_id: "pane-0".into(),
            revision: 0,
            state_change_seq: 0,
            completion_seq: None,
            state_labels: serde_json::Map::new(),
            tab_id: String::new(),
            terminal_id: String::new(),
            terminal_title: None,
            workspace_id: "w0".into(),
        })
    }

    fn agent_read(&self, _target: &str, _source: &str, _lines: u32) -> Result<Value, BackendError> {
        Ok(json!({ "text": "" }))
    }

    fn agent_send_keys(&self, target: &str, keys: &[&str]) -> Result<(), BackendError> {
        self.state.lock().keys.push((target.into(), keys.join("")));
        Ok(())
    }

    fn subscribe(
        &self,
        _filters: &[Subscription],
    ) -> Result<Box<dyn super::EventStream>, BackendError> {
        let (tx, rx) = std::sync::mpsc::channel::<BackendEvent>();
        self.state.lock().events = tx;
        Ok(Box::new(MockEventStream(rx)))
    }
}

struct MockEventStream(std::sync::mpsc::Receiver<BackendEvent>);

impl super::EventStream for MockEventStream {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError> {
        Ok(self.0.recv().ok())
    }

    fn set_read_timeout(
        &mut self,
        _timeout: Option<std::time::Duration>,
    ) -> Result<(), BackendError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::backend::error::BackendErrorCode;

    #[test]
    fn mock_records_prompts_and_keys() {
        let backend = MockBackend::new();
        let pane = backend
            .agent_start("PiAlpha", "pi", "pane-0", &[])
            .expect("start");
        backend
            .prompt(&pane.pane_id, "hello", None)
            .expect("prompt");
        backend
            .agent_send_keys(&pane.pane_id, &["ctrl-c"])
            .expect("keys");

        assert_eq!(
            backend.prompts(),
            vec![("pane-0".to_string(), "hello".to_string())]
        );
        assert_eq!(
            backend.keys(),
            vec![("pane-0".to_string(), "ctrl-c".to_string())]
        );
    }

    #[test]
    fn mock_wait_returns_pushed_status_then_unknown() {
        let backend = MockBackend::new();
        backend.push_status(AgentStatus::Done);
        let info = backend
            .agent_wait("pane-0", &[AgentStatus::Done], None)
            .unwrap();
        assert_eq!(info.agent_status, AgentStatus::Done);
        let info2 = backend
            .agent_wait("pane-0", &[AgentStatus::Done], None)
            .unwrap();
        assert_eq!(info2.agent_status, AgentStatus::Unknown);
    }

    #[test]
    fn mock_events_delivers_pushes() {
        let backend = MockBackend::new();
        let mut rx = backend.subscribe(&[]).expect("subscribe");
        backend.emit(BackendEvent::AgentStatusChanged {
            pane: PaneId::new("pane-0"),
            agent: Some("PiAlpha".into()),
            status: AgentStatus::Working,
        });
        let evt = rx.read_event().expect("event");
        assert_eq!(
            evt,
            Some(BackendEvent::AgentStatusChanged {
                pane: PaneId::new("pane-0"),
                agent: Some("PiAlpha".into()),
                status: AgentStatus::Working,
            })
        );
    }

    #[test]
    fn unknown_machine_code_passthrough() {
        let s = "agent_prompt_stalled";
        assert_eq!(
            s.parse::<BackendErrorCode>().unwrap(),
            BackendErrorCode::AgentPromptStalled
        );
        let opaque = "custom_code_9".parse::<BackendErrorCode>().unwrap();
        assert_eq!(opaque.as_str(), "custom_code_9");
    }
}
