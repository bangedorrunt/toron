// governed-by: ADR-0003
#![forbid(unsafe_code)]

// bd-led.6.3: TmuxBackend — tmux is on hold (ADR-0003 G9); the API surface is
// reserved without false promises. Every method returns a stable
// `Unsupported` machine-code error. Pane-identity paths are documented for
// the future tmux implementation (per-pane identity files).

use tokio::sync::mpsc;

use super::Backend;
use super::error::BackendError;
use super::types::{AgentStatus, BackendEvent, PaneId, SpawnOptions};

/// tmux backend — Unsupported until un-held (ADR-0003 G9).
///
/// Pane-identity layout (reserved; implemented when tmux un-holds):
/// per-pane identity files at
/// `~/.config/<tool>/agent-mail/identity.<pane_id>` mirroring AM-Rust's
/// `~/.claude/agent-mail/identity.<pane>` convention, resolved via the tmux
/// `display-message` pane index. `cleanup_pane_identities` prunes files for
/// panes absent from `list-panes`.
#[derive(Debug, Clone, Default)]
pub struct TmuxBackend;

impl TmuxBackend {
    fn unsupported(method: &str) -> BackendError {
        BackendError::unsupported(format!("TmuxBackend::{method} on hold (ADR-0003 G9)"))
    }
}

impl Backend for TmuxBackend {
    async fn spawn(&self, _opts: &SpawnOptions) -> Result<PaneId, BackendError> {
        Err(Self::unsupported("spawn"))
    }

    async fn prompt(&self, _pane: &PaneId, _input: &str) -> Result<(), BackendError> {
        Err(Self::unsupported("prompt"))
    }

    async fn wait(&self, _pane: &PaneId) -> Result<AgentStatus, BackendError> {
        Err(Self::unsupported("wait"))
    }

    async fn read(&self, _pane: &PaneId) -> Result<String, BackendError> {
        Err(Self::unsupported("read"))
    }

    async fn send_keys(&self, _pane: &PaneId, _keys: &str) -> Result<(), BackendError> {
        Err(Self::unsupported("send_keys"))
    }

    async fn events(&self) -> Result<mpsc::Receiver<BackendEvent>, BackendError> {
        Err(Self::unsupported("events"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::error::BackendErrorCode;

    #[tokio::test]
    async fn every_method_yields_stable_unsupported_code() {
        let backend = TmuxBackend;
        let opts = SpawnOptions::default();
        let pane = PaneId("w0:p0".into());

        let assert_unsupported = |err: BackendError| {
            assert_eq!(err.code(), &BackendErrorCode::Unsupported, "stable code");
        };

        assert_unsupported(backend.spawn(&opts).await.expect_err("spawn unsupported"));
        assert_unsupported(
            backend
                .prompt(&pane, "hi")
                .await
                .expect_err("prompt unsupported"),
        );
        assert_unsupported(backend.wait(&pane).await.expect_err("wait unsupported"));
        assert_unsupported(backend.read(&pane).await.expect_err("read unsupported"));
        assert_unsupported(
            backend
                .send_keys(&pane, "ctrl-c")
                .await
                .expect_err("send_keys unsupported"),
        );
        assert_unsupported(backend.events().await.expect_err("events unsupported"));
    }

    #[tokio::test]
    async fn unsupported_message_is_deterministic() {
        let backend = TmuxBackend;
        let err = backend.spawn(&SpawnOptions::default()).await.unwrap_err();
        assert!(err.message.contains("TmuxBackend::spawn"));
    }
}
