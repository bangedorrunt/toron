// governed-by: ADR-0003
//! Channels tab — NIP-29 rooms (kind 39000/39002 lifecycle, kind 9 posts).
//! The sidebar is the room list, the list is the room's posts, the reader is
//! the full post. Author pins come from the loaded roster (a pure join), so a
//! post never shows a raw npub when the roster knows better.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::{LayoutMode, Model, Pane};
use super::{split, widgets};

/// Render the channels tab for the current layout mode.
pub fn render(f: &mut Frame, area: Rect, model: &Model, mode: LayoutMode) {
    // Phone width: one pane at a time, and the room list stays reachable with
    // `h` (it is the only way to change rooms).
    if mode.single() {
        return match model.pane {
            Pane::Sidebar => render_room_list(f, area, model, true),
            Pane::Reader => render_post_reader(f, area, model, true),
            Pane::List => render_posts(f, area, model, true, true),
        };
    }
    let panes = split(area, mode, true);
    if let Some(sb) = panes.sidebar {
        render_room_list(f, sb, model, model.pane == Pane::Sidebar);
    }
    render_posts(f, panes.list, model, model.pane == Pane::List, false);
    render_post_reader(f, panes.reader, model, model.pane == Pane::Reader);
}

/// The room list.
fn render_room_list(f: &mut Frame, area: Rect, model: &Model, focused: bool) {
    let title = if model.settings.show_closed_channels {
        "channels (+closed)"
    } else {
        "channels"
    };
    if model.channels.channels.is_empty() {
        // The command comes from the model, so it is the real one and carries
        // this project and pin (the old string was hand-written prose).
        let text = if model.channels.loaded {
            format!(
                "no channels in this project yet.\n\n  open the first one:\n    {}",
                model.onboarding().channel
            )
        } else {
            "loading channels…".to_string()
        };
        f.render_widget(
            widgets::message_pane(&text, title, focused, HackerNight::fg_muted()),
            area,
        );
        return;
    }
    let items: Vec<ListItem> = model
        .channels
        .channels
        .iter()
        .map(|c| {
            let mut spans = vec![Span::styled(
                format!("#{}", if c.name.is_empty() { &c.id } else { &c.name }),
                if c.closed {
                    HackerNight::fg_muted()
                } else {
                    HackerNight::fg_default()
                },
            )];
            if c.private {
                spans.push(Span::styled(" p", HackerNight::accent_lav()));
            }
            if c.closed {
                spans.push(Span::styled(" closed", HackerNight::warning()));
            }
            if !c.joined {
                spans.push(Span::styled(" ·not joined", HackerNight::fg_muted()));
            }
            spans.push(Span::styled(
                format!("  {}m {}p", c.members, c.posts),
                HackerNight::fg_muted(),
            ));
            ListItem::new(Line::from(spans))
        })
        .collect();
    let list = List::new(items)
        .block(widgets::pane_block(title, focused))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(
        list,
        area,
        &mut widgets::dir_state(model.channels.channels.len(), model.channels.selected),
    );
}

/// The post list of the selected room.
fn render_posts(f: &mut Frame, area: Rect, model: &Model, focused: bool, narrow: bool) {
    let Some(channel) = model.channels.current() else {
        f.render_widget(
            widgets::message_pane(
                "select a channel (j/k) to read its posts",
                "posts",
                focused,
                HackerNight::fg_muted(),
            ),
            area,
        );
        return;
    };
    let posts = model.channels.visible_posts(&model.input);
    let title = format!(
        "{} — {} post(s){}",
        super::super::model::channel_label(channel),
        posts.len(),
        if channel.closed { " (closed)" } else { "" }
    );
    if posts.is_empty() {
        f.render_widget(
            widgets::message_pane(
                "no posts yet — kind-9 channel posts land here as they arrive",
                &title,
                focused,
                HackerNight::fg_muted(),
            ),
            area,
        );
        return;
    }
    let items: Vec<ListItem> = posts
        .iter()
        .map(|p| {
            let author = model
                .agent_name_for(&p.author)
                .map(str::to_string)
                .unwrap_or_else(|| short_npub(&p.author));
            let at = widgets::stamp(model.now_unix, p.at, model.settings.relative_time);
            if p.is_reaction() {
                return ListItem::new(Line::from(vec![
                    Span::styled("↩ ", HackerNight::accent_lav()),
                    Span::styled(author, HackerNight::fg_muted()),
                    Span::styled(" reacted", HackerNight::fg_muted()),
                    Span::styled(format!("  {at}"), HackerNight::fg_muted()),
                ]));
            }
            let head = p.content.lines().next().unwrap_or_default();
            let mut spans = vec![
                Span::styled(
                    format!("{:<10}", truncate(&author, 10)),
                    HackerNight::accent_lav(),
                ),
                Span::styled(" ", HackerNight::fg_default()),
                Span::styled(truncate(head, 80), HackerNight::fg_default()),
            ];
            if !p.mentions.is_empty() {
                spans.push(Span::styled(
                    format!(" @{}", p.mentions.len()),
                    HackerNight::info(),
                ));
            }
            // Phone width: the stamp is the first thing to go — the body text
            // is the point of the row.
            if !narrow {
                spans.push(Span::styled(format!("  {at}"), HackerNight::fg_muted()));
            }
            ListItem::new(Line::from(spans))
        })
        .collect();
    let list = List::new(items)
        .block(widgets::pane_block(&title, focused))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(
        list,
        area,
        &mut widgets::dir_state(posts.len(), model.channels.selected_post),
    );
}

/// The full post, with the fields the list has no room for.
fn render_post_reader(f: &mut Frame, area: Rect, model: &Model, focused: bool) {
    let posts = model.channels.visible_posts(&model.input);
    let Some(post) = posts.get(model.channels.selected_post) else {
        f.render_widget(
            widgets::message_pane(
                "Enter opens the highlighted post here.\nMentions, reactions and the event id show up in this pane.",
                "reader",
                focused,
                HackerNight::fg_muted(),
            ),
            area,
        );
        return;
    };
    let author = model
        .agent_name_for(&post.author)
        .map(str::to_string)
        .unwrap_or_else(|| post.author.clone());
    let mut lines = vec![
        Line::from(vec![
            Span::styled(author, HackerNight::accent_lav()),
            Span::styled(
                format!(
                    "  kind {}  {}",
                    post.kind,
                    widgets::stamp(model.now_unix, post.at, model.settings.relative_time)
                ),
                HackerNight::fg_muted(),
            ),
        ]),
        Line::from(Span::styled(
            format!("event {}", post.event_id),
            HackerNight::fg_muted(),
        )),
        Line::from(""),
    ];
    for l in post.content.lines() {
        lines.push(Line::from(Span::styled(
            l.to_string(),
            HackerNight::fg_default(),
        )));
    }
    if !post.mentions.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("mentions: ", HackerNight::fg_muted()),
            Span::styled(post.mentions.join(", "), HackerNight::info()),
        ]));
    }
    if let Some(target) = &post.react_to {
        lines.push(Line::from(vec![
            Span::styled("reaction to: ", HackerNight::fg_muted()),
            Span::styled(target.clone(), HackerNight::fg_muted()),
        ]));
    }
    let title = format!(
        "reader — {}",
        model.channels.current().map_or("post", |c| c.id.as_str())
    );
    f.render_widget(
        Paragraph::new(lines)
            .style(HackerNight::fg_default())
            .block(widgets::pane_block(&title, focused))
            .scroll((model.channels.scroll.min(u16::MAX as usize) as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Truncate to `n` chars, ellipsis included.
fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let head: String = s.chars().take(n.saturating_sub(1)).collect();
    format!("{head}…")
}

/// Short npub when the roster cannot resolve the author.
fn short_npub(npub: &str) -> String {
    super::super::host::short_npub(npub)
}
