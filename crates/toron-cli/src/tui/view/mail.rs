// governed-by: ADR-0003
//! Mail tab — Gmail's reading order: folders, then the list, then the
//! conversation. Everything is derived from [`Model::mail_rows`] so the list
//! and the reader can never disagree about what is selected.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::{Folder, LayoutMode, Model, Pane};
use super::{split, widgets};

/// Render the mail tab for the current layout mode.
pub fn render(f: &mut Frame, area: Rect, model: &Model, mode: LayoutMode) {
    // Phone width: one pane at a time owns the frame (`h`/`l` and Enter move
    // between them), and the list rows go one line.
    if mode.single() {
        return match model.pane {
            Pane::Sidebar => render_folders(f, area, model, true),
            Pane::Reader => render_reader(f, area, model, true),
            Pane::List => render_list(f, area, model, true, true),
        };
    }
    let panes = split(area, mode, true);
    if let Some(sb) = panes.sidebar {
        render_folders(f, sb, model, model.pane == Pane::Sidebar);
    }
    render_list(f, panes.list, model, model.pane == Pane::List, false);
    render_reader(f, panes.reader, model, model.pane == Pane::Reader);
}

/// The folder sidebar, with per-folder counts.
fn render_folders(f: &mut Frame, area: Rect, model: &Model, focused: bool) {
    let counts = model.folder_counts();
    let items: Vec<ListItem> = Folder::ALL
        .iter()
        .map(|folder| {
            let n = counts
                .iter()
                .find(|(f, _)| f == folder)
                .map(|(_, n)| *n)
                .unwrap_or(0);
            let count = if n == 0 {
                String::new()
            } else {
                format!(" {n}")
            };
            let style = if n == 0 {
                HackerNight::fg_muted()
            } else {
                HackerNight::fg_default()
            };
            ListItem::new(Line::from(vec![
                Span::styled(folder.title().to_string(), style),
                Span::styled(count, HackerNight::accent_cyan()),
            ]))
        })
        .collect();
    let list = List::new(items)
        .block(widgets::pane_block("folders", focused))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, area, &mut folder_state(model));
}

/// The message/thread list.
fn render_list(f: &mut Frame, area: Rect, model: &Model, focused: bool, narrow: bool) {
    let rows = model.mail_rows();
    let title = match &model.mail.search {
        Some(hits) => format!("search — {} hit(s)", hits.len()),
        None => format!("{} — {}", model.mail.folder.title(), rows.len()),
    };
    if rows.is_empty() {
        // An empty pane is the moment to name the next command, with this
        // project and pin already in it. A FAILED fetch is settled, not
        // pending, so it shows the refusal and the command that fixes it
        // rather than a spinner that never stops.
        let on = model.onboarding();
        let failed = model.mail.error.as_deref();
        let text = if let Some(err) = failed {
            format!(
                "{err}\n\n  the inbox did not load. retry with r, or register this pin and reopen:
    {}",
                on.agent
            )
        } else if !model.mail.loaded {
            "loading the index…".to_string()
        } else if model.settings.agent.is_none() {
            format!(
                "no acting pin, so there is no inbox to read.\n\n  register one:\n    {}\n\nthen start the console with --as (or export AGENT_NAME).",
                on.agent
            )
        } else if model.mail.search.is_some() {
            format!(
                "no hits for {:?} — Esc clears the search",
                model.input.trim()
            )
        } else {
            format!(
                "nothing in this folder yet.\n\n  send the first message:\n    {}\n\n  or open a room first:\n    {}",
                on.mail, on.channel
            )
        };
        let style = if failed.is_some() {
            HackerNight::error()
        } else if model.settings.agent.is_none() && model.mail.loaded {
            HackerNight::warning()
        } else {
            HackerNight::fg_muted()
        };
        f.render_widget(widgets::message_pane(&text, &title, focused, style), area);
        return;
    }
    let now = model.now_unix;
    let relative = model.settings.relative_time;
    let dense = model.settings.dense;
    let items: Vec<ListItem> = rows
        .iter()
        .map(|row| {
            let subject_style = if row.unread {
                HackerNight::fg_emphasis()
            } else {
                HackerNight::fg_default()
            };
            if narrow {
                // Phone width: one line per message. A 10-col sender pad plus
                // a stamp would leave the subject nothing at 45 cols.
                let mut spans = vec![Span::styled(
                    if row.unread { "● " } else { "  " },
                    if row.unread {
                        HackerNight::accent_cyan()
                    } else {
                        HackerNight::fg_muted()
                    },
                )];
                spans.push(Span::styled(
                    format!("{}: ", truncate(&row.from, 8)),
                    HackerNight::accent_lav(),
                ));
                spans.push(Span::styled(row.subject.clone(), subject_style));
                if row.ack_pending {
                    spans.push(Span::styled(" ◉", HackerNight::warning()));
                }
                return ListItem::new(Line::from(spans));
            }
            let mut spans = vec![
                Span::styled(
                    format!("{:<10}", truncate(&row.from, 10)),
                    HackerNight::accent_lav(),
                ),
                Span::styled(" ", HackerNight::fg_default()),
                Span::styled(row.subject.clone(), subject_style),
                Span::styled(" ", HackerNight::fg_default()),
            ];
            if row.ack_pending {
                spans.push(Span::styled("◉ack ", HackerNight::warning()));
            }
            if row.important {
                spans.push(Span::styled("! ", HackerNight::error()));
            }
            if row.unread {
                spans.push(Span::styled("● ", HackerNight::accent_cyan()));
            }
            if row.thread {
                spans.push(Span::styled(
                    format!("[{}] ", row.meta),
                    HackerNight::fg_muted(),
                ));
            }
            spans.push(Span::styled(
                widgets::stamp(now, row.at, relative),
                HackerNight::fg_muted(),
            ));
            if !dense && !row.meta.is_empty() && !row.thread {
                spans.push(Span::styled(
                    format!("  {}", row.meta),
                    HackerNight::fg_muted(),
                ));
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
        &mut widgets::dir_state(rows.len(), model.mail.selected),
    );
}

/// The reader: the open conversation, oldest message first.
fn render_reader(f: &mut Frame, area: Rect, model: &Model, focused: bool) {
    let Some(open) = &model.mail.open else {
        f.render_widget(
            widgets::message_pane(
                "Enter opens the highlighted conversation here (l from the list).\nh/l moves between panes; Esc backs out.",
                "reader",
                focused,
                HackerNight::fg_muted(),
            ),
            area,
        );
        return;
    };
    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(open.subject.clone(), HackerNight::fg_emphasis()),
        Span::styled(
            format!("   thread {}", widgets::short_id(&open.thread_id)),
            HackerNight::fg_muted(),
        ),
    ]));
    lines.push(Line::from(Span::styled(
        format!("{} message(s)", open.messages.len()),
        HackerNight::fg_muted(),
    )));
    lines.push(Line::from(""));
    for msg in &open.messages {
        let mut meta = vec![
            Span::styled(msg.sender.clone(), HackerNight::accent_lav()),
            Span::styled(
                format!(
                    "  {}  {}",
                    widgets::stamp(model.now_unix, msg.at, model.settings.relative_time),
                    widgets::short_id(&msg.id)
                ),
                HackerNight::fg_muted(),
            ),
        ];
        if msg.ack_pending() {
            meta.push(Span::styled("  ◉ack", HackerNight::warning()));
        }
        if msg.important() {
            meta.push(Span::styled("  !", HackerNight::error()));
        }
        lines.push(Line::from(meta));
        match msg.body.as_deref() {
            Some(body) if !body.trim().is_empty() => {
                for l in body.lines() {
                    lines.push(Line::from(Span::styled(
                        l.to_string(),
                        HackerNight::fg_default(),
                    )));
                }
            }
            _ => lines.push(Line::from(Span::styled(
                "(no body in the index)",
                HackerNight::fg_muted(),
            ))),
        }
        lines.push(Line::from(""));
    }
    let title = format!("reader — {}", open.subject);
    let p = Paragraph::new(lines)
        .style(HackerNight::fg_default())
        .block(widgets::pane_block(&title, focused))
        .scroll((model.mail.scroll.min(u16::MAX as usize) as u16, 0))
        .wrap(Wrap { trim: false });
    f.render_widget(p, area);
}

/// The folder index for the sidebar cursor (folders are a fixed enum).
fn folder_index(model: &Model) -> usize {
    Folder::ALL
        .iter()
        .position(|f| *f == model.mail.folder)
        .unwrap_or(0)
}

/// The sidebar's list state: the folder's index, clamped against the FOLDER
/// count. Clamping it against the message count highlighted the wrong folder
/// whenever the index held fewer messages than there are folders.
pub(crate) fn folder_state(model: &Model) -> ListState {
    widgets::dir_state(Folder::ALL.len(), folder_index(model))
}

/// Truncate a cell to `n` chars (never mid-char).
fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let head: String = s.chars().take(n.saturating_sub(1)).collect();
    format!("{head}…")
}
