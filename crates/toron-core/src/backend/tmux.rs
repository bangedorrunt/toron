// governed-by: ADR-0003, ADR-0005
#![forbid(unsafe_code)]

// bd-led.6.3: TmuxBackend — tmux is on hold (ADR-0003 G9); the API surface is
// reserved without false promises. Every method returns a stable
// `Unsupported` machine-code error. Pane-identity paths are documented for
// the future tmux implementation (per-pane identity files).
// ADR-0005 P1: reworked to the unified sync `Backend` trait — `name()` is the
// only override; every other method inherits the Unsupported-open default.

use super::Backend;

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

impl Backend for TmuxBackend {
    fn name(&self) -> &'static str {
        "tmux"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::error::{BackendError, BackendErrorCode};
    #[test]
    fn every_method_yields_stable_unsupported_code() {
        let backend = TmuxBackend;

        let assert_unsupported = |err: BackendError| {
            assert_eq!(err.code(), &BackendErrorCode::Unsupported, "stable code");
        };

        assert_unsupported(backend.workspace_list().expect_err("unsupported"));
        assert_unsupported(backend.agent_list().expect_err("unsupported"));
        assert_unsupported(
            backend
                .subscribe(&[])
                .err()
                .expect("subscribe unsupported"),
        );
        // A default-body Unsupported names the backend + method.
        let err = backend.pane_list(None).unwrap_err();
        assert!(err.message.contains("tmux"));
        assert!(err.message.contains("pane_list"));
    }
}
