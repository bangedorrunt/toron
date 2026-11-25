// governed-by: ADR-0003
//! `2`/`d` Diagnostics & Health and `4`/`s` Charts & Cadence inspectors.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Sparkline, Wrap};

use crate::theme::HackerNight;

use super::super::model::Model;
use super::settings;
use super::widgets;

pub fn render_diagnostics(f: &mut Frame, area: Rect, model: &Model) {
    let diags = model.diagnostics();
    let fw_label = match &model.flywheel {
        Some(fw) => format!(
            "flywheel: {} (dispatcher: {}, pins: {})",
            fw.project, fw.dispatcher, fw.pin_count
        ),
        None => "flywheel: unavailable (not on PATH or skipped)".to_string(),
    };
    let mut lines = vec![Line::from(vec![
        Span::styled(
            format!("Diagnostics ({}) ", diags.len()),
            HackerNight::fg_emphasis(),
        ),
        Span::styled(format!("· {fw_label}"), HackerNight::fg_muted()),
    ])];

    if diags.is_empty() {
        lines.push(Line::from(Span::styled(
            "  ✓ all pure toron + flywheel checks passed",
            HackerNight::success(),
        )));
    } else {
        for (idx, d) in diags.iter().enumerate() {
            let sel = idx == model.selected_diag;
            let prefix = if sel { "> " } else { "  " };
            lines.push(Line::from(vec![
                Span::styled(
                    format!("{prefix}[{}] ", d.severity.label()),
                    widgets::severity_style(d.severity),
                ),
                Span::styled(format!("{} ", d.id), HackerNight::fg_emphasis()),
                Span::styled(
                    format!("({}) — {}", d.source, d.summary),
                    HackerNight::fg_default(),
                ),
            ]));
            if sel {
                lines.push(Line::from(Span::styled(
                    format!("    evidence: {}  |  remedy: {}", d.evidence, d.remedy_cmd),
                    HackerNight::fg_muted(),
                )));
            }
        }
    }

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: true }), area);
}

pub fn render_charts(f: &mut Frame, area: Rect, model: &Model) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

    let buckets = model.arrivals();
    let spark_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(cols[0]);
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            format!(
                "Board arrivals ({}h, {} posts)",
                model.settings.chart_window_hours,
                model.posts.len()
            ),
            HackerNight::fg_emphasis(),
        ))),
        spark_rows[0],
    );
    f.render_widget(
        Sparkline::default()
            .data(&buckets)
            .style(HackerNight::accent_cyan()),
        spark_rows[1],
    );

    settings::render(f, cols[1], model);
}
