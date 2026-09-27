// governed-by: ADR-0003
//! Settings tab — who this session is, where its data comes from, and the
//! handful of things that are tunable at runtime. Read-only facts are shown
//! as facts (with the flag that changes them), never as fake inputs.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::theme::HackerNight;

use super::super::model::Model;
use super::{Pane, widgets};

/// Render the settings tab.
pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(8),
        ])
        .split(area);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
        .split(rows[0]);
    let identity = if model.settings.agent.is_some() {
        format!(
            "{} @ {}",
            model.settings.agent.as_deref().unwrap_or("-"),
            model.settings.project
        )
    } else {
        format!("no pin @ {}", model.settings.project)
    };
    widgets::stat_card(
        f,
        cols[0],
        "identity",
        &identity,
        model
            .settings
            .npub
            .as_deref()
            .map(crate::tui::host::short_npub)
            .as_deref()
            .unwrap_or("pass --as or export TORON_AGENT to read an inbox"),
        model.settings.agent.is_some(),
    );
    widgets::stat_card(
        f,
        cols[1],
        "store",
        &model.settings.source.kind,
        &model.settings.source.detail,
        true,
    );

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
        .split(rows[1]);
    widgets::stat_card(
        f,
        cols[0],
        "refresh",
        &format!("{}ms", model.settings.refresh_ms),
        "[ slower  ·  ] faster  (250ms … 10s)",
        true,
    );
    widgets::stat_card(
        f,
        cols[1],
        "chart window",
        &format!("{}h", model.dashboard.window_hours),
        "- / + on the Dashboard tab",
        true,
    );

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Ratio(1, 2), Constraint::Ratio(1, 2)])
        .split(rows[2]);
    render_tunables(f, left[0], model);
    render_source(f, left[1], model);
    render_reference(f, rows[2], model);
}

/// Live toggles, each naming the key that flips it.
fn render_tunables(f: &mut Frame, area: Rect, model: &Model) {
    let flag = |on: bool| if on { "on " } else { "off" };
    let lines = vec![
        Line::from(vec![
            Span::styled("[d] ", HackerNight::accent_cyan()),
            Span::styled(format!("{:<12} ", "dense rows"), HackerNight::fg_muted()),
            Span::styled(
                flag(model.settings.dense).to_string(),
                HackerNight::fg_default(),
            ),
        ]),
        Line::from(vec![
            Span::styled("[t] ", HackerNight::accent_cyan()),
            Span::styled(format!("{:<12} ", "relative time"), HackerNight::fg_muted()),
            Span::styled(
                if model.settings.relative_time {
                    "age (2m)"
                } else {
                    "clock (14:03)"
                }
                .to_string(),
                HackerNight::fg_default(),
            ),
        ]),
        Line::from(vec![
            Span::styled("[c] ", HackerNight::accent_cyan()),
            Span::styled(
                format!("{:<12} ", "closed channels"),
                HackerNight::fg_muted(),
            ),
            Span::styled(
                flag(model.settings.show_closed_channels).to_string(),
                HackerNight::fg_default(),
            ),
        ]),
        Line::from(vec![
            Span::styled("[[] ", HackerNight::accent_cyan()),
            Span::styled(format!("{:<12} ", "cadence"), HackerNight::fg_muted()),
            Span::styled(
                format!("{}ms", model.settings.refresh_ms),
                HackerNight::fg_default(),
            ),
        ]),
    ];
    f.render_widget(
        Paragraph::new(lines).block(widgets::block_unfocused("tunables (live)")),
        area,
    );
}

/// The environment this session reads.
fn render_source(f: &mut Frame, area: Rect, model: &Model) {
    let mut lines = vec![
        Line::from(vec![
            Span::styled("data dir   ", HackerNight::fg_muted()),
            Span::styled(
                model.settings.source.data_dir.clone(),
                HackerNight::fg_default(),
            ),
        ]),
        Line::from(vec![
            Span::styled("selector   ", HackerNight::fg_muted()),
            Span::styled(
                if model.settings.source.daemon_loopback {
                    "daemon on this machine owns the same durable root"
                } else {
                    "embedded against the durable root (no daemon configured)"
                }
                .to_string(),
                HackerNight::fg_default(),
            ),
        ]),
    ];
    lines.push(Line::from(vec![
        Span::styled("change     ", HackerNight::fg_muted()),
        Span::styled(
            "--project / --as at launch, or $TORON_PROJECT / $TORON_AGENT",
            HackerNight::fg_default(),
        ),
    ]));
    f.render_widget(
        Paragraph::new(lines).block(widgets::block_unfocused("source")),
        area,
    );
}

/// The full key reference (help tier 3 of the skill's three tiers).
fn render_reference(f: &mut Frame, area: Rect, model: &Model) {
    let lines: Vec<Line> = super::super::keymap::help_lines()
        .into_iter()
        .map(|l| Line::from(Span::styled(l.to_string(), HackerNight::fg_muted())))
        .collect();
    let focus = matches!(model.pane, Pane::List);
    f.render_widget(
        Paragraph::new(lines).block(widgets::pane_block("keys", focus)),
        area,
    );
}
