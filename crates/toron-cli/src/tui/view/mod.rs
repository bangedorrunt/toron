// governed-by: ADR-0003
//! Swarm Board frame renderer: header + health pills, compact roster,
//! `#seq` posts stream, switchable bottom inspector, and 2-line footer.

pub mod agents;
pub mod channels;
pub mod compose;
pub mod dashboard;
pub mod mail;
pub mod settings;
pub mod widgets;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::keymap;
use super::model::{self, Inspector, LayoutMode, MIN_H, MIN_W, Mode, Model, StatusKind};

/// Render one Swarm Board frame.
pub fn render(f: &mut Frame, model: &Model) {
    let area = f.area();
    let mode = model::layout_mode(area.width, area.height);
    if mode == LayoutMode::TooSmall {
        render_resize_card(f, area);
        return;
    }

    let roster_h = agents::desired_height(model);
    let inspector_h = (area.height / 3).clamp(5, 12);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(roster_h),
            Constraint::Min(4),
            Constraint::Length(inspector_h),
            Constraint::Length(2),
        ])
        .split(area);

    render_header(f, rows[0], model, mode == LayoutMode::Compact);
    agents::render(f, rows[1], model);
    channels::render_posts(f, rows[2], model);
    render_inspector(f, rows[3], model);
    render_footer(f, rows[4], model, mode == LayoutMode::Compact);

    if model.mode == Mode::Help {
        render_help(f, area);
    } else if model.mode == Mode::Compose {
        compose::render(f, area, model);
    }
}

fn render_header(f: &mut Frame, area: Rect, model: &Model, compact: bool) {
    let follow_lbl = if model.follow { "on" } else { "off" };
    let diags = model.diagnostics();
    let blocking = model.has_blocking_diagnostics();

    let mut spans = vec![
        Span::styled(
            format!(" ◆ Swarm board \"{}\" ", model.settings.project),
            HackerNight::fg_emphasis(),
        ),
        Span::styled(
            format!("· {} posts · [follow: {follow_lbl}] ", model.posts.len()),
            HackerNight::fg_muted(),
        ),
    ];

    if !compact {
        let toron_ok =
            model.toron_health.wake_liveness_ok && !model.toron_health.wake_never_stamped;
        let toron_warn = model.toron_health.wake_publish_failures > 0
            || model.toron_health.stuck_queued_sends > 0;
        spans.push(widgets::health_pill("toron", toron_ok, toron_warn));
        spans.push(Span::raw(" "));
        let wake_lbl =
            if model.toron_health.wake_liveness_ok && !model.toron_health.wake_never_stamped {
                "wake Live"
            } else {
                "wake Deaf"
            };
        spans.push(widgets::health_pill(
            wake_lbl,
            model.toron_health.wake_liveness_ok && !model.toron_health.wake_never_stamped,
            false,
        ));
        if let Some(fw) = &model.flywheel {
            spans.push(Span::raw(" "));
            let fw_ok = !fw.diagnostics.iter().any(|d| d.severity.is_blocking());
            spans.push(widgets::health_pill(
                "flywheel",
                fw_ok,
                !fw.degraded.is_empty() || !fw.diagnostics.is_empty(),
            ));
        }
        spans.push(Span::raw(" "));
        spans.push(widgets::health_pill(
            &format!("diag {}", diags.len()),
            !blocking,
            !diags.is_empty(),
        ));
    }

    f.render_widget(
        Paragraph::new(Line::from(spans)).style(HackerNight::header()),
        area,
    );
}

fn render_inspector(f: &mut Frame, area: Rect, model: &Model) {
    match model.inspector {
        Inspector::Message => channels::render_message_inspector(f, area, model),
        Inspector::Diagnostics => dashboard::render_diagnostics(f, area, model),
        Inspector::Leases => mail::render_leases(f, area, model),
        Inspector::Charts => dashboard::render_charts(f, area, model),
    }
}

fn render_footer(f: &mut Frame, area: Rect, model: &Model, compact: bool) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);

    let diag_count = model.diagnostics().len();
    let status = if model.mode == Mode::Filter {
        Line::from(vec![
            Span::styled(" /", HackerNight::accent_cyan()),
            Span::styled(model.filter.clone(), HackerNight::fg_default()),
            Span::styled("_", HackerNight::accent_cyan()),
        ])
    } else if !model.status.text.is_empty() {
        let (shape, style) = match model.status.kind {
            StatusKind::Error => ("✗", HackerNight::error()),
            StatusKind::Ok => ("✓", HackerNight::success()),
            StatusKind::Info => ("·", HackerNight::fg_muted()),
        };
        Line::from(vec![
            Span::styled(format!(" {shape} "), style),
            Span::styled(model.status.text.clone(), style),
        ])
    } else {
        Line::from(Span::styled(
            format!(
                " ✓ {} · inspector: {} · diag: {} (d) · sealed: {} ({} unread)",
                if model.focused { "live" } else { "paused" },
                model.inspector.title(),
                diag_count,
                model.sealed_inbox.total,
                model.sealed_inbox.unread
            ),
            HackerNight::fg_muted(),
        ))
    };
    f.render_widget(Paragraph::new(status), rows[0]);

    let hints = if compact {
        "[j/k] post  [1-4] view  [d] diag  [c] post  [?] help  [q] quit"
    } else {
        "[j/k] post  [J/K] scroll  [f] follow  [/] filter  [R] retired  [1] msg  [2/d] diag  [3/l] leases  [4/s] charts  [c] post  [?] help  [q] quit"
    };
    f.render_widget(
        Paragraph::new(Line::from(Span::styled(hints, HackerNight::fg_muted()))),
        rows[1],
    );
}

fn render_resize_card(f: &mut Frame, area: Rect) {
    let msg = format!(
        "terminal too small ({}×{}) — need at least {}×{}\n\nresize to continue",
        area.width, area.height, MIN_W, MIN_H
    );
    let w = 36u16.min(area.width.max(1));
    let h = 5u16.min(area.height.max(1));
    let card = Rect {
        x: area.x + area.width.saturating_sub(w) / 2,
        y: area.y + area.height.saturating_sub(h) / 2,
        width: w,
        height: h,
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" resize ")
        .title_style(HackerNight::title_focused());
    f.render_widget(Clear, card);
    f.render_widget(
        Paragraph::new(msg)
            .style(HackerNight::warning())
            .block(block)
            .centered()
            .wrap(Wrap { trim: true }),
        card,
    );
}

fn render_help(f: &mut Frame, area: Rect) {
    let lines = keymap::help_lines();
    let w = (area.width * 3 / 4).clamp(48, 82).min(area.width);
    let h = (lines.len() as u16 + 2).min(area.height);
    let card = Rect {
        x: area.x + area.width.saturating_sub(w) / 2,
        y: area.y + area.height.saturating_sub(h) / 2,
        width: w,
        height: h,
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" help  [?/Esc/q to close] ")
        .title_style(HackerNight::title_focused())
        .style(
            ratatui::style::Style::default()
                .bg(HackerNight::OVERLAY)
                .fg(HackerNight::FG),
        );
    let text: Vec<Line> = lines
        .into_iter()
        .map(|l| Line::from(Span::styled(l, HackerNight::fg_default())))
        .collect();
    f.render_widget(Clear, card);
    f.render_widget(
        Paragraph::new(text).block(block).wrap(Wrap { trim: false }),
        card,
    );
}
