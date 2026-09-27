// governed-by: ADR-0003
//! Agents tab — the registered roster joined with the live presence
//! projection, refreshed every tick. Online/offline is a shape plus a word,
//! never color alone (tui-design skill §4).

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, Paragraph, Wrap};

use crate::theme::HackerNight;

use super::super::model::{AgentRow, LayoutMode, Model, Pane};
use super::{split, widgets};

/// Render the agents tab for the current layout mode. This tab has no sidebar
/// of its own, so it asks `split` for the full width.
pub fn render(f: &mut Frame, area: Rect, model: &Model, mode: LayoutMode) {
    let rows = model.agents.visible_rows(&model.input);
    if mode.single() {
        return match model.pane {
            Pane::Reader => render_detail(f, area, model, &rows, true),
            _ => render_list(f, area, model, &rows, true, true),
        };
    }
    let panes = split(area, mode, false);
    render_list(f, panes.list, model, &rows, model.pane == Pane::List, false);
    render_detail(f, panes.reader, model, &rows, model.pane == Pane::Reader);
}

/// The roster list: marker, pin, state, harness, age.
fn render_list(
    f: &mut Frame,
    area: Rect,
    model: &Model,
    rows: &[AgentRow],
    focused: bool,
    narrow: bool,
) {
    let online = rows.iter().filter(|r| r.online).count();
    let title = format!("agents — {online}/{} online", rows.len());
    if rows.is_empty() {
        let text = if !model.agents.loaded {
            "loading the roster…".to_string()
        } else if model.input.trim().is_empty() {
            "no registered agents in this project".to_string()
        } else {
            format!("no agent matches {:?}", model.input.trim())
        };
        f.render_widget(
            widgets::message_pane(&text, &title, focused, HackerNight::fg_muted()),
            area,
        );
        return;
    }
    let items: Vec<ListItem> = rows
        .iter()
        .map(|row| {
            let name_style = if row.is_self {
                HackerNight::accent_cyan()
            } else if row.online {
                HackerNight::fg_emphasis()
            } else {
                HackerNight::fg_muted()
            };
            let mut spans = vec![
                widgets::agent_badge(row),
                Span::styled(" ", HackerNight::fg_default()),
                Span::styled(format!("{:<16}", truncate(&row.name, 16)), name_style),
                Span::styled(format!("{:<9}", row.state_label()), state_style(row)),
            ];
            // Phone width: pin + state are the row; the harness and the age
            // are in the detail card (`Enter`).
            if !narrow {
                let harness = match (&row.program, &row.model) {
                    (Some(p), Some(m)) => format!("{p}/{m}"),
                    (Some(p), None) | (None, Some(p)) => p.clone(),
                    (None, None) => String::new(),
                };
                if !harness.is_empty() {
                    spans.push(Span::styled(
                        format!("{:<22}", truncate(&harness, 22)),
                        HackerNight::fg_muted(),
                    ));
                }
                if let Some(age) = row.age_secs {
                    spans.push(Span::styled(
                        format!("{} ago", widgets::age_of(age)),
                        HackerNight::fg_muted(),
                    ));
                }
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
        &mut widgets::dir_state(rows.len(), model.agents.selected),
    );
}

/// The detail card for the highlighted agent.
fn render_detail(f: &mut Frame, area: Rect, model: &Model, rows: &[AgentRow], focused: bool) {
    let Some(row) = rows.get(model.agents.selected) else {
        f.render_widget(
            widgets::message_pane(
                "the roster detail shows here: npub, harness, task and the last heartbeat.",
                "agent",
                focused,
                HackerNight::fg_muted(),
            ),
            area,
        );
        return;
    };
    let mut lines = vec![
        Line::from(vec![
            widgets::agent_badge(row),
            Span::styled(format!(" {}", row.name), HackerNight::fg_emphasis()),
            if row.is_self {
                Span::styled("  (this session)", HackerNight::accent_cyan())
            } else {
                Span::styled("", HackerNight::fg_muted())
            },
        ]),
        Line::from(Span::styled(
            format!("state     {}", row.state_label()),
            state_style(row),
        )),
        Line::from(Span::styled(
            format!("npub      {}", row.npub),
            HackerNight::fg_muted(),
        )),
    ];
    for (label, value) in [
        ("harness", row.program.clone()),
        ("model", row.model.clone()),
        ("task", row.task.clone()),
        ("pane", row.pane_id.clone()),
        ("status", row.status.clone()),
    ] {
        if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
            lines.push(Line::from(vec![
                Span::styled(format!("{label:<9} "), HackerNight::fg_muted()),
                Span::styled(value, HackerNight::fg_default()),
            ]));
        }
    }
    match row.age_secs {
        Some(age) => lines.push(Line::from(vec![
            Span::styled("heartbeat ", HackerNight::fg_muted()),
            Span::styled(
                format!("{} ago", widgets::age_of(age)),
                HackerNight::fg_default(),
            ),
        ])),
        None => lines.push(Line::from(Span::styled(
            "heartbeat none — no kind-20001 within the TTL (offline)",
            HackerNight::fg_muted(),
        ))),
    }
    f.render_widget(
        Paragraph::new(lines)
            .style(HackerNight::fg_default())
            .block(widgets::pane_block("agent", focused))
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Presence word styling (shape already carries the meaning).
fn state_style(row: &AgentRow) -> ratatui::style::Style {
    match (row.online, row.state) {
        (false, _) => HackerNight::fg_muted(),
        (true, toron_core::nostr::presence::PresenceState::Blocked) => HackerNight::warning(),
        (true, toron_core::nostr::presence::PresenceState::Working) => HackerNight::success(),
        (true, _) => HackerNight::info(),
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let head: String = s.chars().take(n.saturating_sub(1)).collect();
    format!("{head}…")
}
