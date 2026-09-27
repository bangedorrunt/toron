// governed-by: ADR-0003
//! Shared view widgets. Semantic theme slots only (`crate::theme`), shapes
//! never color alone (tui-design skill §4), and no widget decides anything —
//! every number here came out of the model.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, ListState, Paragraph, Sparkline, Wrap};

use crate::theme::HackerNight;

use super::super::model::{self, AgentRow};

/// A focused pane block.
pub fn block_focused(title: &str) -> Block<'_> {
    Block::bordered()
        .title(format!(" {title} "))
        .title_style(HackerNight::title_focused())
        .border_style(HackerNight::border_focused())
}

/// An unfocused pane block.
pub fn block_unfocused(title: &str) -> Block<'_> {
    Block::bordered()
        .title(format!(" {title} "))
        .title_style(HackerNight::title_unfocused())
        .border_style(HackerNight::border_unfocused())
}

/// Pick the block for a pane's focus state.
pub fn pane_block(title: &str, focused: bool) -> Block<'_> {
    if focused {
        block_focused(title)
    } else {
        block_unfocused(title)
    }
}

/// List state for a directory cursor, clamped against the length that
/// directory actually rendered. Each pane clamps against ITS OWN length:
/// clamping the folder sidebar against the message count highlighted the wrong
/// row whenever the index held fewer messages than there are folders.
pub fn dir_state(len: usize, selected: usize) -> ListState {
    let mut state = ListState::default();
    if len > 0 {
        state.select(Some(selected.min(len - 1)));
    }
    state
}

/// A pane with a single dim paragraph (empty state, error, hint).
pub fn message_pane<'a>(
    text: &'a str,
    title: &'a str,
    focused: bool,
    style: Style,
) -> Paragraph<'a> {
    Paragraph::new(text)
        .style(style)
        .block(pane_block(title, focused))
        .wrap(Wrap { trim: true })
}

/// Absolute `HH:MM` local clock for a unix second.
pub fn hhmm(unix: u64) -> String {
    use chrono::{Local, TimeZone};
    match Local.timestamp_opt(unix as i64, 0).single() {
        Some(dt) => dt.format("%H:%M").to_string(),
        None => String::from("--:--"),
    }
}

/// Age of a timestamp, coarse (`12s`, `5m`, `3h`, `2d`).
pub fn age(now: u64, then: u64) -> String {
    age_of(now.saturating_sub(then))
}

/// A duration in seconds, coarse (`12s`, `5m`, `3h`, `2d`).
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

/// Timestamp as the operator asked: age or clock.
pub fn stamp(now: u64, then: u64, relative: bool) -> String {
    if relative { age(now, then) } else { hhmm(then) }
}

/// The presence marker for an agent row: shape first, color second.
pub fn agent_badge(row: &AgentRow) -> Span<'static> {
    let style = match (row.online, row.state) {
        (false, _) => HackerNight::fg_muted(),
        (true, toron_core::nostr::presence::PresenceState::Blocked) => HackerNight::warning(),
        (true, toron_core::nostr::presence::PresenceState::Working) => HackerNight::success(),
        (true, _) => HackerNight::info(),
    };
    Span::styled(row.marker().to_string(), style)
}

/// A stat card: label, big value, optional sub-line. Rendered as a bordered
/// block so a wall of numbers still reads as separate facts (§1 widget grid).
pub fn stat_card(f: &mut Frame, area: Rect, label: &str, value: &str, sub: &str, ok: bool) {
    let value_style = if ok {
        HackerNight::accent_cyan()
    } else {
        HackerNight::warning()
    };
    let text = vec![
        Line::from(Span::styled(value.to_string(), value_style)),
        Line::from(Span::styled(sub.to_string(), HackerNight::fg_muted())),
    ];
    let p = Paragraph::new(text)
        .style(HackerNight::fg_default())
        .block(block_unfocused(label))
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

/// A horizontal bar with a label and a count, in text: `label █████ 12`.
pub fn bar_line(label: &str, width: u16, count: u64, label_w: usize) -> Line<'static> {
    let bar: String = std::iter::repeat_n('█', width as usize).collect();
    let pad = label_w.saturating_sub(label.chars().count());
    Line::from(vec![
        Span::styled(label.to_string(), HackerNight::fg_default()),
        Span::styled(" ".repeat(pad + 1), HackerNight::fg_muted()),
        Span::styled(bar, HackerNight::accent_lav()),
        Span::styled(format!(" {count}"), HackerNight::fg_muted()),
    ])
}

/// Render a sparkline of hourly counts with its bounds labelled.
pub fn sparkline(f: &mut Frame, area: Rect, title: &str, data: &[u64], left: &str, right: &str) {
    let block = block_unfocused(title);
    let inner = block.inner(area);
    f.render_widget(block, area);
    if inner.height == 0 {
        return;
    }
    let data_row = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: inner.height.saturating_sub(1).max(1),
    };
    f.render_widget(
        Sparkline::default()
            .data(data.to_vec())
            .style(HackerNight::accent_cyan()),
        data_row,
    );
    if inner.height >= 2 {
        let label_row = Rect {
            x: inner.x,
            y: inner.y + inner.height - 1,
            width: inner.width,
            height: 1,
        };
        let gap =
            (inner.width as usize).saturating_sub(left.chars().count() + right.chars().count());
        f.render_widget(
            Paragraph::new(Line::from(vec![
                Span::styled(left.to_string(), HackerNight::fg_muted()),
                Span::styled(" ".repeat(gap), HackerNight::fg_muted()),
                Span::styled(right.to_string(), HackerNight::fg_muted()),
            ])),
            label_row,
        );
    }
}

/// The `layout` label used in the footer (responsive feedback).
/// Short form of an id or npub for a title or a status line (`abc12345…`).
/// One spelling for every surface: two shorteners would disagree about where
/// the ellipsis lands.
pub fn short_id(s: &str) -> String {
    if s.chars().count() <= 12 {
        return s.to_string();
    }
    let head: String = s.chars().take(12).collect();
    format!("{head}…")
}

pub fn layout_label(mode: model::LayoutMode) -> &'static str {
    match mode {
        model::LayoutMode::Tri => "3-pane",
        model::LayoutMode::Dual => "2-pane",
        model::LayoutMode::Stacked => "1-pane",
        model::LayoutMode::TooSmall => "too small",
    }
}
