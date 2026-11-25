// governed-by: ADR-0003
//! Compact Swarm Board roster renderer.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

use crate::theme::HackerNight;

use super::super::model::{Model, RosterRow};
use super::widgets;

fn is_active_seat(model: &Model, r: &RosterRow) -> bool {
    !r.retired && (r.online || model.lease_count_for(&r.name) > 0)
}

pub fn desired_height(model: &Model) -> u16 {
    let active = model
        .roster
        .iter()
        .filter(|r| is_active_seat(model, r))
        .count();
    let collapsed = model.roster.len().saturating_sub(active);
    let rows = if model.show_retired {
        model.roster.len()
    } else {
        active + usize::from(collapsed > 0)
    };
    (rows as u16 + 1).clamp(2, 7)
}

pub fn render(f: &mut Frame, area: Rect, model: &Model) {
    let active: Vec<&RosterRow> = model
        .roster
        .iter()
        .filter(|r| is_active_seat(model, r))
        .collect();
    let collapsed: Vec<&RosterRow> = model
        .roster
        .iter()
        .filter(|r| !is_active_seat(model, r))
        .collect();
    let online = active.iter().filter(|r| r.online).count();

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("Roster", HackerNight::fg_emphasis()),
        Span::styled(
            format!(
                " · {} seats ({} online, {} offline/retired)",
                model.roster.len(),
                online,
                collapsed.len()
            ),
            HackerNight::fg_muted(),
        ),
    ]));

    if model.roster.is_empty() {
        lines.push(Line::from(Span::styled(
            format!(
                "  no registered seats yet — run `toron agent bootstrap --project {} --as <pin>`",
                model.settings.project
            ),
            HackerNight::fg_muted(),
        )));
    } else {
        for r in &active {
            lines.push(roster_line(model, r));
        }
        if !collapsed.is_empty() {
            if model.show_retired {
                for r in &collapsed {
                    lines.push(roster_line(model, r));
                }
            } else {
                let names: Vec<&str> = collapsed.iter().map(|r| r.name.as_str()).take(6).collect();
                let suffix = if collapsed.len() > 6 { ", …" } else { "" };
                lines.push(Line::from(Span::styled(
                    format!(
                        "  • offline/retired ({}): {}{}  [R] expand",
                        collapsed.len(),
                        names.join(", "),
                        suffix
                    ),
                    HackerNight::fg_muted(),
                )));
            }
        }
    }

    f.render_widget(Paragraph::new(lines), area);
}

fn roster_line(model: &Model, r: &RosterRow) -> Line<'static> {
    let marker_style = if r.retired {
        HackerNight::fg_muted()
    } else if r.online {
        HackerNight::success()
    } else {
        HackerNight::info()
    };
    let age = r
        .age_secs
        .map(|s| format!("{} ago", widgets::age_of(s)))
        .unwrap_or_else(|| "—".to_string());
    let prog = r.program.as_deref().unwrap_or("agent");
    let leases = model.lease_count_for(&r.name).max(r.lease_count);
    Line::from(vec![
        Span::styled(format!("  {} ", r.marker()), marker_style),
        Span::styled(format!("{:<16}", r.name), HackerNight::fg_default()),
        Span::styled(format!(" {:<10}", r.status), HackerNight::fg_muted()),
        Span::styled(format!(" {:<10}", prog), HackerNight::accent_lav()),
        Span::styled(
            format!(" {:<8} {} lease(s)", age, leases),
            HackerNight::fg_muted(),
        ),
    ])
}
