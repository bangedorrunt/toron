// governed-by: ADR-0003
//! Shared Swarm Board view helpers.

use ratatui::style::Style;
use ratatui::text::Span;

use crate::theme::HackerNight;

use super::super::model::DiagnosticSeverity;

pub fn age_of(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3_600 {
        format!("{}m", secs / 60)
    } else if secs < 86_400 {
        format!("{}h", secs / 3_600)
    } else {
        format!("{}d", secs / 86_400)
    }
}

pub fn hhmm(unix: u64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(unix as i64, 0).single() {
        Some(dt) => dt.format("%m-%d %H:%M").to_string(),
        None => "--:--".to_string(),
    }
}

pub fn stamp(now: u64, then: u64, relative: bool) -> String {
    if relative {
        format!("{} ago", age_of(now.saturating_sub(then)))
    } else {
        hhmm(then)
    }
}

pub fn truncate(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    let count = s.chars().count();
    if count <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

pub fn health_pill(label: &str, ok: bool, warn: bool) -> Span<'static> {
    let (shape, style) = if !ok {
        ("✗", HackerNight::error())
    } else if warn {
        ("!", HackerNight::warning())
    } else {
        ("✓", HackerNight::success())
    };
    Span::styled(format!("[{label} {shape}]"), style)
}

pub fn severity_style(sev: DiagnosticSeverity) -> Style {
    match sev {
        DiagnosticSeverity::P0 => HackerNight::error(),
        DiagnosticSeverity::P1 => HackerNight::warning(),
        DiagnosticSeverity::P2 => HackerNight::info(),
        DiagnosticSeverity::P3 => HackerNight::fg_muted(),
    }
}
