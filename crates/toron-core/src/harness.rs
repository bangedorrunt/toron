//! Harness kind detection for a pane's foreground process.
//!
//! Pure: maps an hc agent kind string (`pi`, `opencode`, ...) to a
//! [`Harness`] variant. Mirrors flywheel's pane_write::Harness.

/// The harness a pane runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Harness {
    Pi,
    Opencode,
    Claude,
    Grok,
    Codex,
    Other,
}

impl Harness {
    pub fn from_kind(kind: &str) -> Self {
        match kind.trim().to_ascii_lowercase().as_str() {
            "pi" => Self::Pi,
            "opencode" | "oc" => Self::Opencode,
            "claude" | "claude-code" | "cc" => Self::Claude,
            "grok" | "gk" => Self::Grok,
            "codex" | "cx" => Self::Codex,
            _ => Self::Other,
        }
    }
}
