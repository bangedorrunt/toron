// governed-by: ADR-0003, ADR-0045
#![forbid(unsafe_code)]

//! Multiplexer pane plane (split wave7-b): backends, OSC 7501 fold input,
//! and the live [`toron_core::plane_port::PanePlane`] adapter. Depends on
//! `toron-core` (shared pane vocabulary + `proc`), never the reverse — core
//! names only `toron_core::pane_types` and the port trait.

pub mod backend;
pub mod osc7501;

use anyhow::Context;
use backend::Backend;
use toron_core::pane_types::{AgentInfo, PaneInfo};
use toron_core::plane_port::PanePlane;

/// Production adapter: one constructed backend behind the narrow port.
pub struct LivePanePlane {
    inner: Box<dyn Backend>,
}

impl LivePanePlane {
    /// Connect like the old per-call sites did (`plane::connect_default()`).
    /// `Err` (no plane reachable) is a value the caller degrades on, never a
    /// panic — same posture as the pre-port `let Ok(backend)` sites.
    pub fn connect_default() -> anyhow::Result<Self> {
        let inner = backend::plane::connect_default().context("pane plane unavailable")?;
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

#[cfg(test)]
mod live_port_tests {
    use super::*;

    /// The real backend is wired behind the port (compile-time proof; the
    /// live round-trip stays in the existing ignored backend tests, which
    /// need a real pane plane and must not run in CI).
    #[test]
    fn live_backend_implements_port_from_the_new_crate() {
        fn is_port<T: PanePlane>() {}
        is_port::<LivePanePlane>();
    }
}
