// governed-by: ADR-0003
//! `#seq` Posts stream and `1` Message inspector.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::Model;
use super::widgets;

pub fn render_posts(f: &mut Frame, area: Rect, model: &Model) {
    let visible = model.visible_posts();
    let follow_lbl = if model.follow { "on" } else { "off" };
    let filter_lbl = if model.filter.trim().is_empty() {
        "none".to_string()
    } else {
        model.filter.clone()
    };

    let mut items = Vec::new();
    items.push(ListItem::new(Line::from(vec![
        Span::styled("Posts", HackerNight::fg_emphasis()),
        Span::styled(
            format!(
                " · {} total · [follow: {follow_lbl}] · [filter: {filter_lbl}]",
                visible.len()
            ),
            HackerNight::fg_muted(),
        ),
    ])));

    if visible.is_empty() {
        items.push(ListItem::new(Line::from(Span::styled(
            "  no #seq posts yet — press [c] to post to the board",
            HackerNight::fg_muted(),
        ))));
    } else {
        let max_rows = area.height.saturating_sub(1) as usize;
        let selected = model.selected_post.min(visible.len().saturating_sub(1));
        let start = if selected >= max_rows {
            selected + 1 - max_rows
        } else {
            0
        };
        let end = (start + max_rows).min(visible.len());
        let preview_w = area.width.saturating_sub(36) as usize;
        for (idx, post) in visible[start..end].iter().enumerate() {
            let abs_idx = start + idx;
            let is_sel = abs_idx == selected;
            let cursor = if is_sel { "> " } else { "  " };
            let row_style = if is_sel {
                HackerNight::selection()
            } else {
                HackerNight::fg_default()
            };
            let stamp = widgets::stamp(
                model.now_unix,
                post.created_at,
                model.settings.relative_time,
            );
            let line = Line::from(vec![
                Span::styled(
                    format!("{cursor}#{:<3} ", post.seq),
                    HackerNight::accent_cyan(),
                ),
                Span::styled(format!("{:<11} ", stamp), HackerNight::fg_muted()),
                Span::styled(
                    format!("{:<14} ", widgets::truncate(&post.author, 14)),
                    HackerNight::accent_lav(),
                ),
                Span::styled(widgets::truncate(&post.preview(), preview_w), row_style),
            ]);
            items.push(ListItem::new(line));
        }
    }

    f.render_widget(List::new(items), area);
}

pub fn render_message_inspector(f: &mut Frame, area: Rect, model: &Model) {
    let Some(post) = model.selected_board_post() else {
        f.render_widget(
            Paragraph::new(Line::from(Span::styled(
                "Message · (no post selected)",
                HackerNight::fg_muted(),
            ))),
            area,
        );
        return;
    };

    let body_lines: Vec<&str> = post.body.lines().collect();
    let total = body_lines.len().max(1);
    let body_h = area.height.saturating_sub(1) as usize;
    let start = model.msg_scroll.min(total.saturating_sub(1));
    let end = (start + body_h).min(total);
    let stamp = widgets::stamp(
        model.now_unix,
        post.created_at,
        model.settings.relative_time,
    );

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(
            format!("Message · #{} {} · {stamp}", post.seq, post.author),
            HackerNight::fg_emphasis(),
        ),
        Span::styled(
            format!(" · lines {}-{} of {total}", start + 1, end.max(start + 1)),
            HackerNight::fg_muted(),
        ),
    ]));
    for raw in &body_lines[start..end] {
        lines.push(highlight_line(raw));
    }
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn highlight_line(raw: &str) -> Line<'static> {
    let trimmed = raw.trim_start();
    if trimmed.starts_with('#') {
        return Line::from(Span::styled(raw.to_string(), HackerNight::accent_cyan()));
    }
    let mut spans = Vec::new();
    for (idx, word) in raw.split(' ').enumerate() {
        if idx > 0 {
            spans.push(Span::raw(" "));
        }
        if word.starts_with('@') && word.len() > 1 {
            spans.push(Span::styled(word.to_string(), HackerNight::accent_lav()));
        } else if word.starts_with('`') && word.ends_with('`') && word.len() > 2 {
            spans.push(Span::styled(word.to_string(), HackerNight::info()));
        } else {
            spans.push(Span::styled(word.to_string(), HackerNight::fg_default()));
        }
    }
    Line::from(spans)
}
