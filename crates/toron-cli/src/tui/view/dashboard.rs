// governed-by: ADR-0003
//! Dashboard tab — one screen of operator truth. Every number is derived from
//! the same data the other tabs show (plus the receipt store), so the summary
//! can never disagree with the detail.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::Model;
use super::widgets;

/// Render the dashboard.
pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let health_h = (model.dashboard.health.len() as u16 + 2).clamp(3, 10);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(health_h),
        ])
        .split(area);

    render_cards(f, rows[0], rows[1], model);
    render_charts(f, rows[2], model);
    render_health(f, rows[3], model);
}

/// Stat cards: mail, fleet, receipts.
fn render_cards(f: &mut Frame, row1: Rect, row2: Rect, model: &Model) {
    let unread = model.mail.messages.iter().filter(|m| !m.read).count();
    let ack = model
        .mail
        .messages
        .iter()
        .filter(|m| m.ack_pending())
        .count();
    let high = model.mail.messages.iter().filter(|m| m.important()).count();
    let cards1 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
        ])
        .split(row1);
    widgets::stat_card(
        f,
        cards1[0],
        "inbox",
        &charts_compact(model.mail.messages.len() as u64),
        &format!("{unread} unread"),
        true,
    );
    widgets::stat_card(
        f,
        cards1[1],
        "needs ack",
        &charts_compact(ack as u64),
        "ack-required, unacked",
        ack == 0,
    );
    widgets::stat_card(
        f,
        cards1[2],
        "important",
        &charts_compact(high as u64),
        "high / urgent",
        true,
    );
    widgets::stat_card(
        f,
        cards1[3],
        "threads",
        &charts_compact(model.thread_count() as u64),
        "conversations",
        true,
    );

    let online = model.agents.online();
    let total = model.agents.rows.len();
    let r = model.dashboard.receipts;
    let cards2 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
            Constraint::Ratio(1, 4),
        ])
        .split(row2);
    widgets::stat_card(
        f,
        cards2[0],
        "agents online",
        &format!("{online}/{total}"),
        "presence within TTL",
        online > 0 || total == 0,
    );
    widgets::stat_card(
        f,
        cards2[1],
        "channels",
        &charts_compact(model.channels.channels.len() as u64),
        &format!(
            "{} joined",
            model.channels.channels.iter().filter(|c| c.joined).count()
        ),
        true,
    );
    widgets::stat_card(
        f,
        cards2[2],
        "receipts owed",
        &charts_compact(r.owed as u64),
        &format!("{} acked · {} resulted", r.acked, r.resulted),
        r.owed == 0,
    );
    widgets::stat_card(
        f,
        cards2[3],
        "posts indexed",
        &charts_compact(model.channels.channels.iter().map(|c| c.posts as u64).sum()),
        "kind-9 on the local bus",
        true,
    );
}

/// Charts: arrivals over the window, posts per channel, presence split.
fn render_charts(f: &mut Frame, area: Rect, model: &Model) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(2, 5),
            Constraint::Ratio(3, 10),
            Constraint::Ratio(3, 10),
        ])
        .split(area);

    let arrivals = model.arrivals();
    let window = model.dashboard.window_hours;
    let total: u64 = arrivals.iter().sum();
    widgets::sparkline(
        f,
        cols[0],
        &format!("mail arrivals — last {window}h ({total})"),
        &arrivals,
        &format!("-{window}h"),
        "now",
    );

    render_bars(
        f,
        cols[1],
        "posts per channel",
        &model.channel_bars(),
        "no channel posts indexed",
    );
    render_bars(
        f,
        cols[2],
        "fleet by state",
        &model.agent_state_counts(),
        "no registered agents",
    );
}

/// A labelled bar list (one block per row).
fn render_bars(f: &mut Frame, area: Rect, title: &str, rows: &[(String, u64)], empty: &str) {
    let inner_w = area.width.saturating_sub(4).max(4);
    let label_w = ((inner_w as usize) / 3).clamp(6, 16);
    let bar_w = (inner_w as usize).saturating_sub(label_w + 6).max(4) as u16;
    let block = widgets::block_unfocused(title);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if rows.is_empty() {
        f.render_widget(
            Paragraph::new(Span::styled(empty.to_string(), HackerNight::fg_muted()))
                .wrap(Wrap { trim: true }),
            inner,
        );
        return;
    }
    let values: Vec<u64> = rows.iter().map(|(_, n)| *n).collect();
    let widths = super::super::charts::bar_widths(&values, bar_w);
    let shown: Vec<Line> = rows
        .iter()
        .zip(widths)
        .take(inner.height as usize)
        .map(|((label, n), w)| widgets::bar_line(label, w, *n, label_w))
        .collect();
    f.render_widget(Paragraph::new(shown), inner);
}

/// Health lines: environment facts, with a shape for the reader.
fn render_health(f: &mut Frame, area: Rect, model: &Model) {
    let mut lines: Vec<Line> = Vec::new();
    if model.dashboard.health.is_empty() {
        lines.push(Line::from(Span::styled(
            "health: loading…",
            HackerNight::fg_muted(),
        )));
    }
    for row in &model.dashboard.health {
        let (shape, style) = if row.ok {
            ("✓", HackerNight::success())
        } else {
            ("!", HackerNight::warning())
        };
        lines.push(Line::from(vec![
            Span::styled(format!(" {shape} "), style),
            Span::styled(format!("{:<10}", row.label), HackerNight::fg_muted()),
            Span::styled(row.value.clone(), HackerNight::fg_default()),
        ]));
    }
    f.render_widget(
        Paragraph::new(lines)
            .block(widgets::block_unfocused("health"))
            .wrap(Wrap { trim: true }),
        area,
    );
}

/// Compact count for cards (`1.2k`).
fn charts_compact(n: u64) -> String {
    super::super::charts::compact(n)
}
