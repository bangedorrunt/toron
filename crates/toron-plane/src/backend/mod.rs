// governed-by: ADR-0003, ADR-0005
#![forbid(unsafe_code)]

// 1. **Object-safety: YES (dyn-capable).** flywheel's factory returns
// 6. **Error type: this crate's `BackendError`** (machine-code typed,

pub mod error;
pub mod fallback;
pub mod hc;
pub mod mock;
pub mod plane;
pub mod rex;
pub mod rex_cli;
pub mod rex_schema;
pub mod superlogical;
pub mod types;
pub mod wait;

pub use fallback::FallbackBackend;
pub use hc::{
    Client as HcClient, HC_GENERATION, HcBackend, ID_PREFIX, MIN_HC_VERSION, TESTED_HC_VERSION,
};
pub use plane::{Plane, PlaneChoice, PlaneEnv, ResolvedPlane, RexCli};
pub use toron_core::pane_types::{
    AgentInfo, AgentSessionInfo, AgentSessionRefKind, AgentStatus, BackendError, BackendErrorCode,
    PaneInfo, PaneScrollInfo, TOKEN_NPUB, TOKEN_PIN, TOKEN_PROJECT,
};
pub use types::{
    BackendEvent, PaneId, PromptLanding, SpawnOptions, Subscription, WaitOptions, WorkspaceInfo,
};
pub use wait::{ACTIVITY_GATE_MS, PromptHost, PromptTarget, WaitOutcome, prompt_wait};

use std::time::Duration;

use serde_json::Value;

/// Sync push-event stream yielded by [`Backend::subscribe`] (ADR-0005 P1).
///
/// `read_event` blocks until a push arrives (or the stream closes →
/// `Ok(None)`); `set_read_timeout` bounds each blocking read (default: no-op
/// — a backend without per-read timeouts leaves them unbounded).
pub trait EventStream: Send {
    fn read_event(&mut self) -> Result<Option<BackendEvent>, BackendError>;
    fn set_read_timeout(&mut self, _timeout: Option<Duration>) -> Result<(), BackendError> {
        Ok(())
    }
}

/// Unified supervisor backend (ADR-0005 P1) — the ONE backend trait, by
/// design. Superset of flywheel's former `MultiplexerBackend` (18 methods:
/// workspace/pane/agent/subscribe) and this crate's former async `Backend`
/// (spawn/prompt/wait/read/send_keys/events — mapped onto the richer shapes:
/// spawn→agent_start, prompt→prompt/prompt_coded, wait→agent_wait,
/// read→agent_read, send_keys→agent_send_keys, events→subscribe).
///
/// - **SYNC + `&self` + object-safe** (`Box<dyn Backend>`): every real call
///   site (flywheel CLI commands + hidden subprocess leaves) is sync; impls
///   use interior mutability for their stateful clients. No async fns, no
///   generic methods — dyn-capable.
/// - **Unsupported-open (ADR-0002 §6):** every method except `name()` has a
///   default body returning `BackendError` with `BackendErrorCode::Unsupported`
///   naming the backend + method. Future backends inherit `Unsupported`.
/// - **Errors are machine-code typed** (`BackendError::code`, classify on
///   code — ADR-0003). flywheel's local `BackendError{Unsupported, Other}`
///   was deleted; pattern-matches migrate to `err.code()`.
pub trait Backend: Send + Sync {
    /// Stable backend identifier ("hc" | "superlogical" | ...).
    fn name(&self) -> &'static str;

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, BackendError> {
        Err(unsupported(self.name(), "workspace_list"))
    }
    fn workspace_create(&self, _label: Option<&str>) -> Result<WorkspaceInfo, BackendError> {
        Err(unsupported(self.name(), "workspace_create"))
    }
    fn workspace_close(&self, _workspace_id: &str, _close_group: bool) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "workspace_close"))
    }

    fn pane_split(
        &self,
        _parent_pane_id: &str,
        _direction: &str,
        _cwd: Option<&str>,
    ) -> Result<PaneInfo, BackendError> {
        Err(unsupported(self.name(), "pane_split"))
    }
    fn pane_list(&self, _workspace_id: Option<&str>) -> Result<Vec<PaneInfo>, BackendError> {
        Err(unsupported(self.name(), "pane_list"))
    }
    fn pane_close(&self, _pane_id: &str) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "pane_close"))
    }
    fn pane_run(&self, _pane_id: &str, _command: &str) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "pane_run"))
    }
    fn pane_wait_output(
        &self,
        _pane_id: &str,
        _needle: &str,
        _timeout_ms: u64,
    ) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "pane_wait_output"))
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, BackendError> {
        Err(unsupported(self.name(), "agent_list"))
    }
    fn agent_start(
        &self,
        _name: &str,
        _kind: &str,
        _pane_id: &str,
        _args: &[String],
    ) -> Result<AgentInfo, BackendError> {
        Err(unsupported(self.name(), "agent_start"))
    }
    fn agent_rename(&self, _target: &str, _new_name: Option<&str>) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "agent_rename"))
    }
    fn prompt(
        &self,
        _target: &str,
        _text: &str,
        _wait: Option<&WaitOptions>,
    ) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "prompt"))
    }
    /// Coded `agent.prompt` — landing data (status XOR protocol code), so a
    /// backend-agnostic caller can classify like the herdr client
    /// (ADR-0002 review-fix F6).
    fn prompt_coded(
        &self,
        _target: &str,
        _text: &str,
        _wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, BackendError> {
        Err(unsupported(self.name(), "prompt_coded"))
    }
    fn agent_wait(
        &self,
        _target: &str,
        _until: &[AgentStatus],
        _timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, BackendError> {
        Err(unsupported(self.name(), "agent_wait"))
    }
    fn agent_read(&self, _target: &str, _source: &str, _lines: u32) -> Result<Value, BackendError> {
        Err(unsupported(self.name(), "agent_read"))
    }
    fn agent_send_keys(&self, _target: &str, _keys: &[&str]) -> Result<(), BackendError> {
        Err(unsupported(self.name(), "agent_send_keys"))
    }

    fn subscribe(&self, _filters: &[Subscription]) -> Result<Box<dyn EventStream>, BackendError> {
        Err(unsupported(self.name(), "subscribe"))
    }
}

/// Default-body helper: an Unsupported machine-code error naming the backend
/// + method. Free fn (not a trait method) so `Backend` stays dyn-compatible.
fn unsupported(backend: &'static str, method: &'static str) -> BackendError {
    BackendError::unsupported(format!("{backend} does not implement {method}"))
}
