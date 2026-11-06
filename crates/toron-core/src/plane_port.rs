//! Narrow pane-plane port: the only facts the mail/comms kernel may take from
//! the pane plane (wave6-b).
//!
//! The kernel used to reach `crate::backend` directly (construct a backend per
//! call in messaging/identity/surface, import vocabulary types in osc7501).
//! That reach is why the plane could never become its own crate without a
//! cycle. This module is the single seam:
//!
//! - [`PanePlane`] names exactly the two facts the kernel uses (`pane_list`,
//!   `agent_list`) over plain data types. The `Backend` trait itself never
//!   crosses into the kernel.
//! - Vocabulary types (`AgentInfo`, `PaneInfo`, token consts, `AgentStatus`)
//!   are re-exported here so kernel modules name `crate::plane_port`, never
//!   `crate::backend`.
//! - [`LivePanePlane`] adapts the real backend (`plane::connect_default()`);
//!   [`MockPanePlane`] (tests) feeds canned panes/agents with no plane.
//!
//! Injection: [`crate::mailbox::Mailbox`] owns
//! `Option<Arc<dyn PanePlane>>` (default = live at construction, `None` when
//! no plane is reachable), overridable via `with_plane` for tests/embedders.

use crate::backend::Backend;
pub use crate::backend::types::{
    AgentInfo, AgentStatus, PaneInfo, TOKEN_NPUB, TOKEN_PIN, TOKEN_PROJECT,
};

/// The pane facts the kernel needs. Object-safe; `Send + Sync` so a mailbox
/// can share one across threads.
pub trait PanePlane: Send + Sync {
    fn pane_list(&self) -> anyhow::Result<Vec<PaneInfo>>;
    fn agent_list(&self) -> anyhow::Result<Vec<AgentInfo>>;
}

/// Production adapter: one constructed backend behind the narrow port.
pub struct LivePanePlane {
    inner: Box<dyn Backend>,
}

impl LivePanePlane {
    /// Connect like every former call site did (`plane::connect_default()`).
    /// `Err` (no plane reachable) is a value the caller degrades on, never a
    /// panic — same posture as the pre-port `let Ok(backend)` sites.
    pub fn connect_default() -> anyhow::Result<Self> {
        let inner = crate::backend::plane::connect_default()?;
        Ok(Self { inner })
    }

    /// Wrap an already-constructed backend (tests, embedders holding one).
    pub fn wrap(inner: Box<dyn Backend>) -> Self {
        Self { inner }
    }
}

impl PanePlane for LivePanePlane {
    fn pane_list(&self) -> anyhow::Result<Vec<PaneInfo>> {
        self.inner.pane_list(None).map_err(anyhow::Error::new)
    }

    fn agent_list(&self) -> anyhow::Result<Vec<AgentInfo>> {
        self.inner.agent_list().map_err(anyhow::Error::new)
    }
}

/// Canned panes/agents for kernel tests. No backend, no env, no I/O.
#[cfg(test)]
pub struct MockPanePlane {
    /// What `pane_list` returns (`Err` when this is `Err`).
    pub panes: Result<Vec<PaneInfo>, String>,
    /// What `agent_list` returns (`Err` when this is `Err`).
    pub agents: Result<Vec<AgentInfo>, String>,
}

#[cfg(test)]
impl MockPanePlane {
    pub fn new(panes: Vec<PaneInfo>, agents: Vec<AgentInfo>) -> Self {
        Self {
            panes: Ok(panes),
            agents: Ok(agents),
        }
    }

    /// A plane with no reachable backend: every list fails.
    pub fn unreachable() -> Self {
        Self {
            panes: Err("no plane".to_string()),
            agents: Err("no plane".to_string()),
        }
    }
}

#[cfg(test)]
impl PanePlane for MockPanePlane {
    fn pane_list(&self) -> anyhow::Result<Vec<PaneInfo>> {
        self.panes.clone().map_err(|e| anyhow::anyhow!(e))
    }

    fn agent_list(&self) -> anyhow::Result<Vec<AgentInfo>> {
        self.agents.clone().map_err(|e| anyhow::anyhow!(e))
    }
}

#[cfg(test)]
mod port_tests {
    use super::*;

    fn pane(id: &str, pin: Option<&str>) -> PaneInfo {
        let mut tokens = serde_json::Map::new();
        if let Some(pin) = pin {
            tokens.insert(TOKEN_PIN.into(), serde_json::Value::String(pin.into()));
        }
        PaneInfo {
            pane_id: id.into(),
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
            tokens,
        }
    }

    #[test]
    fn mock_plane_serves_canned_panes_and_agents() {
        let plane = MockPanePlane::new(vec![pane("b:1", Some("PaleHarbor"))], vec![]);
        let panes = plane.pane_list().expect("mock panes");
        assert_eq!(panes.len(), 1);
        assert_eq!(panes[0].pane_id, "b:1");
        assert!(plane.agent_list().expect("mock agents").is_empty());
    }

    #[test]
    fn unreachable_plane_fails_both_lists() {
        let plane = MockPanePlane::unreachable();
        assert!(plane.pane_list().is_err());
        assert!(plane.agent_list().is_err());
    }

    #[test]
    fn real_backend_is_wired_behind_the_port() {
        fn is_port<T: PanePlane>() {}
        is_port::<LivePanePlane>();
    }
}
