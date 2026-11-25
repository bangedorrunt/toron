// governed-by: ADR-0031
//! Key -> [`Msg`] translation for the Swarm Board.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use super::model::{Inspector, Mode, Model};
use super::msg::Msg;

pub const REFRESH_STEP_MS: i64 = 250;

pub fn translate(model: &Model, key: KeyEvent) -> Option<Msg> {
    if key.kind == KeyEventKind::Release {
        return None;
    }
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    if ctrl && matches!(key.code, KeyCode::Char('c')) {
        return Some(Msg::Quit);
    }
    match model.mode {
        Mode::Help => match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') | KeyCode::Enter => {
                Some(Msg::ToggleHelp)
            }
            _ => None,
        },
        Mode::Filter => match key.code {
            KeyCode::Esc => Some(Msg::CancelFilter),
            KeyCode::Enter => Some(Msg::CommitFilter),
            KeyCode::Backspace => Some(Msg::FilterBackspace),
            KeyCode::Char(c) if !ctrl => Some(Msg::FilterChar(c)),
            _ => None,
        },
        Mode::Compose => match key.code {
            KeyCode::Esc => Some(Msg::ComposeCancel),
            KeyCode::Enter => Some(Msg::ComposeEnter),
            KeyCode::Backspace => Some(Msg::ComposeBackspace),
            KeyCode::Char(c) if !ctrl => Some(Msg::ComposeChar(c)),
            _ => None,
        },
        Mode::Normal => normal_mode(model, key),
    }
}

fn normal_mode(model: &Model, key: KeyEvent) -> Option<Msg> {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => Some(Msg::Quit),
        KeyCode::Char('?') => Some(Msg::ToggleHelp),
        KeyCode::Tab => Some(Msg::CycleInspector(1)),
        KeyCode::BackTab => Some(Msg::CycleInspector(-1)),

        KeyCode::Char('1') => Some(Msg::SelectInspector(Inspector::Message)),
        KeyCode::Char('2') | KeyCode::Char('d') => {
            Some(Msg::SelectInspector(Inspector::Diagnostics))
        }
        KeyCode::Char('3') | KeyCode::Char('l') => Some(Msg::SelectInspector(Inspector::Leases)),
        KeyCode::Char('4') | KeyCode::Char('s') => Some(Msg::SelectInspector(Inspector::Charts)),

        KeyCode::Char('j') | KeyCode::Down => Some(if model.inspector == Inspector::Diagnostics {
            Msg::MoveDiag(1)
        } else {
            Msg::MovePost(1)
        }),
        KeyCode::Char('k') | KeyCode::Up => Some(if model.inspector == Inspector::Diagnostics {
            Msg::MoveDiag(-1)
        } else {
            Msg::MovePost(-1)
        }),
        KeyCode::Char('J') | KeyCode::PageDown => Some(Msg::ScrollMessage(5)),
        KeyCode::Char('K') | KeyCode::PageUp => Some(Msg::ScrollMessage(-5)),
        KeyCode::Char('g') => Some(Msg::GotoPost(true)),
        KeyCode::Char('G') => Some(Msg::GotoPost(false)),

        KeyCode::Char('f') => Some(Msg::ToggleFollow),
        KeyCode::Char('R') => Some(Msg::ToggleRetired),
        KeyCode::Char('y') => Some(Msg::CopyRemedy),
        KeyCode::Char('/') => Some(Msg::OpenFilter),
        KeyCode::Char('r') => Some(Msg::Refresh),
        KeyCode::Char('c') => Some(Msg::OpenCompose),
        KeyCode::Char('t') => Some(Msg::ToggleRelativeTime),
        KeyCode::Char('[') => Some(Msg::StepRefresh(-REFRESH_STEP_MS)),
        KeyCode::Char(']') => Some(Msg::StepRefresh(REFRESH_STEP_MS)),
        _ => None,
    }
}

pub fn help_lines() -> Vec<&'static str> {
    vec![
        "toron tui / dashboard / ui — Swarm Board keys",
        "",
        "  j / k or ↑ / ↓     select post (or diagnostic in Diagnostics view)",
        "  J / K or PgUp/Dn   scroll message body",
        "  g / G              jump to oldest / newest post",
        "  f                  toggle [follow: on|off]",
        "  R                  expand/collapse retired seats",
        "  /                  filter posts (Esc clears, Enter keeps)",
        "",
        "  1                  Inspector: Message body",
        "  2 / d              Inspector: Diagnostics & Health",
        "  3 / l              Inspector: Leases & Locks",
        "  4 / s              Inspector: Charts & Cadence",
        "  y                  show remedy command for selected diagnostic",
        "",
        "  c                  post to #seq board (Esc cancels, Enter sends)",
        "  r                  refresh now",
        "  t                  toggle relative / clock timestamps",
        "  [ / ]              step tick interval",
        "  ?                  toggle this help",
        "  q / Esc / Ctrl-C   quit",
    ]
}
