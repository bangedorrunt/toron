// governed-by: ADR-0003
//! Settings & probe cadence summary (right half of `4`/`s` Charts inspector).

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::{
    FLYWHEEL_PROBE_INTERVAL_SECS, IDLE_PAUSE_SECS, Model, TORON_PROBE_INTERVAL_SECS,
};

pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let viewer = model.settings.agent.as_deref().unwrap_or("(read-only)");
    let state = if !model.focused {
        "paused (unfocused)"
    } else if model.now_unix.saturating_sub(model.last_input_unix) >= IDLE_PAUSE_SECS {
        "paused (idle 5m)"
    } else {
        "live"
    };
    let lines = vec![
        Line::from(Span::styled("Cadence & Store", HackerNight::fg_emphasis())),
        Line::from(Span::styled(
            format!("  project: {}  ·  as: {viewer}", model.settings.project),
            HackerNight::fg_default(),
        )),
        Line::from(Span::styled(
            format!("  source: {}", model.settings.source.kind),
            HackerNight::fg_muted(),
        )),
        Line::from(Span::styled(
            format!(
                "  probes: {state} · :18080 {TORON_PROBE_INTERVAL_SECS}s · snapshot {FLYWHEEL_PROBE_INTERVAL_SECS}s · tick {}ms",
                model.settings.refresh_ms
            ),
            HackerNight::fg_muted(),
        )),
        Line::from(Span::styled(
            format!(
                "  receipts: owed {} · acked {} · resulted {} · degraded {}",
                model.receipts.owed,
                model.receipts.acked,
                model.receipts.resulted,
                model.receipts.degraded
            ),
            HackerNight::fg_muted(),
        )),
    ];
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}
