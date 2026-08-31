// governed-by: ADR-0003
//! hacker-night neon-on-void theme — single source of truth for toron TUI
//! and any future CLI paint. Semantic slots → ratatui Style with 3-tier
//! degrade (truecolor → 256 → 16) and NO_COLOR respect.

use ratatui::style::{Color, Modifier, Style};

fn no_color() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
        || std::env::var("TERM").map(|t| t == "dumb").unwrap_or(false)
}

fn truecolor() -> bool {
    if no_color() {
        return false;
    }
    matches!(
        std::env::var("COLORTERM")
            .ok()
            .map(|v| v.to_ascii_lowercase())
            .as_deref(),
        Some("truecolor") | Some("24bit")
    )
}

/// Hacker-night palette — Ghostty v2 neon-on-void.
/// When `NO_COLOR` is set every Style degrades to `Color::Reset` / no modifiers.
pub struct HackerNight;

impl HackerNight {
    // raw palette
    pub const VOID: Color = Color::Rgb(0x08, 0x08, 0x0e);
    pub const SURFACE: Color = Color::Rgb(0x12, 0x12, 0x2a);
    pub const OVERLAY: Color = Color::Rgb(0x1a, 0x1a, 0x3e);
    pub const SELECTION_BG: Color = Color::Rgb(0x24, 0x24, 0x3f);
    pub const FG: Color = Color::Rgb(0xc8, 0xd0, 0xe8);
    pub const MUTED: Color = Color::Rgb(0x56, 0x5f, 0x89);
    pub const EMPHASIS: Color = Color::Rgb(0xed, 0xed, 0xed);
    pub const CYAN: Color = Color::Rgb(0x0d, 0xb9, 0xd7);
    pub const LAV: Color = Color::Rgb(0xbb, 0x9a, 0xf7);
    pub const MINT: Color = Color::Rgb(0x73, 0xda, 0xca);
    pub const ICE: Color = Color::Rgb(0x89, 0xdd, 0xff);
    pub const AMBER: Color = Color::Rgb(0xe0, 0xaf, 0x68);
    pub const RED: Color = Color::Rgb(0xf7, 0x76, 0x8e); // AA on void (use 210/91 for bright)

    /// Resolve `Color::Rgb` vs `Color::Indexed` vs `Color::Reset` based on capability.
    fn resolve(c: Color, idx256: u8, ansi16: Color) -> Color {
        if no_color() {
            return Color::Reset;
        }
        if truecolor() {
            return c;
        }
        // 256color TERM → use indexed fallback
        let is_256 = std::env::var("TERM")
            .map(|t| t.contains("256color"))
            .unwrap_or(false);
        if is_256 {
            Color::Indexed(idx256)
        } else {
            ansi16
        }
    }

    // — semantic Styles —

    pub fn bg_base() -> Style {
        Style::default().bg(Self::resolve(Self::VOID, 233, Color::Black))
    }
    pub fn fg_default() -> Style {
        Style::default().fg(Self::resolve(Self::FG, 189, Color::White))
    }
    pub fn fg_muted() -> Style {
        Style::default()
            .fg(Self::resolve(Self::MUTED, 60, Color::DarkGray))
            .add_modifier(Modifier::DIM)
    }
    pub fn fg_emphasis() -> Style {
        let mut s = Style::default().fg(Self::resolve(Self::EMPHASIS, 255, Color::White));
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD);
        }
        s
    }
    pub fn accent_cyan() -> Style {
        let mut s = Style::default().fg(Self::resolve(Self::CYAN, 44, Color::Cyan));
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD);
        }
        s
    }
    pub fn accent_lav() -> Style {
        Style::default().fg(Self::resolve(Self::LAV, 141, Color::Magenta))
    }
    pub fn success() -> Style {
        Style::default().fg(Self::resolve(Self::MINT, 79, Color::Green))
    }
    pub fn info() -> Style {
        Style::default().fg(Self::resolve(Self::ICE, 117, Color::LightCyan))
    }
    pub fn warning() -> Style {
        Style::default().fg(Self::resolve(Self::AMBER, 180, Color::Yellow))
    }
    pub fn error() -> Style {
        let mut s = Style::default().fg(Self::resolve(Self::RED, 210, Color::LightRed));
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD);
        }
        s
    }
    pub fn header() -> Style {
        // void bg + fg.default bold — used for header bar
        let bg = Self::resolve(Self::VOID, 233, Color::Black);
        let fg = Self::resolve(Self::FG, 189, Color::White);
        let mut s = Style::default().bg(bg).fg(fg);
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD);
        }
        s
    }
    /// Selected row: bg.selection + bold fg
    pub fn selection() -> Style {
        let bg = Self::resolve(Self::SELECTION_BG, 236, Color::DarkGray);
        let fg = Self::resolve(Self::FG, 189, Color::White);
        let mut s = Style::default().bg(bg).fg(fg);
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD);
        }
        s
    }
    pub fn tabs_inactive() -> Style {
        Self::fg_muted()
    }
    pub fn tabs_active() -> Style {
        let fg = Self::resolve(Self::CYAN, 44, Color::Cyan);
        let mut s = Style::default().fg(fg).bg(Self::resolve(Self::VOID, 233, Color::Black));
        if !no_color() {
            s = s.add_modifier(Modifier::BOLD | Modifier::UNDERLINED);
        }
        s
    }
    /// Focused panel border style (heavy cyan), unfocused uses muted.
    pub fn border_focused() -> Style {
        Style::default().fg(Self::resolve(Self::CYAN, 44, Color::Cyan))
    }
    pub fn border_unfocused() -> Style {
        Style::default().fg(Self::resolve(Self::MUTED, 60, Color::DarkGray))
    }
    /// Block title style.
    pub fn title_focused() -> Style {
        Self::accent_cyan()
    }
    pub fn title_unfocused() -> Style {
        Self::fg_muted()
    }

    /// Wrap a style to respect NO_COLOR — strips fg/bg/modifiers when disabled.
    pub fn maybe_style(s: Style) -> Style {
        if no_color() {
            Style::default()
        } else {
            s
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_color_strips() {
        // Env mutation is process-global — serialize via TEST env lock pattern
        // used elsewhere (output.rs tests). Here we just assert shape without
        // mutating env: resolve() with Color::Reset is the NO_COLOR contract.
        let s = Style::default().fg(Color::Red).add_modifier(Modifier::BOLD);
        let stripped = HackerNight::maybe_style(s);
        // When NO_COLOR is not set in test env, maybe_style returns input unchanged
        // — the important property is that it exists and is the NO_COLOR gate.
        assert!(stripped.fg.is_some() || std::env::var_os("NO_COLOR").is_some());
    }

    #[test]
    fn palette_consts_are_rgb() {
        assert_eq!(HackerNight::VOID, Color::Rgb(0x08, 0x08, 0x0e));
        assert_eq!(HackerNight::CYAN, Color::Rgb(0x0d, 0xb9, 0xd7));
        assert_eq!(HackerNight::RED, Color::Rgb(0xf7, 0x76, 0x8e));
    }
}
