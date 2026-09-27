// governed-by: ADR-0031
//! Key → [`Msg`] translation. Pure: same model, same key, same message.
//!
//! Layers follow the tui-design skill §3:
//!   L0 universal — arrows, Enter, Esc, q      (footer, always visible)
//!   L1 vim       — hjkl, gg/G, /, ?, :        (footer)
//!   L2 actions   — a(ck), m(ark read), r(efresh), d(ense), t(ime), c(losed)
//!   L3 power     — [ / ] cadence, - / + chart window   (`?` help lists them)
//!
//! Modes: `Insert` owns printable keys (the filter box); `Help` is a focus
//! trap and swallows everything except its close keys.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::model::{Mode, Model, Pane, Tab};
use super::msg::Msg;

/// Refresh cadence step for `[` / `]` (ms).
pub const REFRESH_STEP_MS: i64 = 250;

/// Translate one key press for the current model state.
pub fn translate(model: &Model, key: KeyEvent) -> Option<Msg> {
    if key.kind == crossterm::event::KeyEventKind::Release {
        return None;
    }
    match model.mode {
        Mode::Help => match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') | KeyCode::Enter => {
                Some(Msg::ToggleHelp)
            }
            _ => None,
        },
        Mode::Insert => insert_mode(key),
        Mode::Normal => normal_mode(model, key),
    }
}

/// Typing in the filter box. Only `Esc`/`Enter` leave it.
fn insert_mode(key: KeyEvent) -> Option<Msg> {
    match key.code {
        KeyCode::Esc => Some(Msg::CancelInsert),
        KeyCode::Enter => Some(Msg::SubmitInput),
        KeyCode::Backspace => Some(Msg::InputBackspace),
        KeyCode::Char(c) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            Some(Msg::InputChar(c))
        }
        _ => None,
    }
}

/// Navigation and actions.
fn normal_mode(model: &Model, key: KeyEvent) -> Option<Msg> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    // Ctrl-C never reaches the shell inside a raw-mode alternate screen, so
    // the conventional meaning ("leave now") is bound to it deliberately —
    // the one documented deviation from the skill's never-bind list.
    if ctrl && matches!(key.code, KeyCode::Char('c')) {
        return Some(Msg::Quit);
    }
    match key.code {
        KeyCode::Char(c) if c.is_ascii_digit() => Tab::from_digit(c).map(Msg::SelectTab),
        KeyCode::Char('q') => Some(Msg::Quit),
        KeyCode::Char('?') => Some(Msg::ToggleHelp),
        KeyCode::Esc => Some(if model.pane == Pane::Reader {
            Msg::CloseReader
        } else {
            Msg::Quit
        }),
        KeyCode::Tab => Some(Msg::CycleTab(1)),
        KeyCode::BackTab => Some(Msg::CycleTab(-1)),

        KeyCode::Char('j') | KeyCode::Down => Some(Msg::MoveCursor(1)),
        KeyCode::Char('k') | KeyCode::Up => Some(Msg::MoveCursor(-1)),
        KeyCode::Char(' ') | KeyCode::PageDown => Some(Msg::MoveCursor(10)),
        KeyCode::Char('g') => Some(Msg::GotoCursor(true)),
        KeyCode::Char('G') => Some(Msg::GotoCursor(false)),

        // h/l walk the panes; l drills into the reader from the list, h backs
        // out of the reader (panic button: never a dead end).
        KeyCode::Char('h') | KeyCode::Left => Some(if model.pane == Pane::Reader {
            Msg::CloseReader
        } else {
            Msg::FocusPane(-1)
        }),
        KeyCode::Char('l') | KeyCode::Right => {
            Some(if model.pane == Pane::List && model.tab.has_reader() {
                Msg::OpenSelection
            } else {
                Msg::FocusPane(1)
            })
        }
        KeyCode::Enter => Some(Msg::OpenSelection),

        KeyCode::Char('/') => Some(Msg::OpenInsert),
        KeyCode::Char('r') => Some(Msg::Refresh),
        KeyCode::Char('a') if model.tab == Tab::Mail => Some(Msg::AckSelected),
        KeyCode::Char('m') if model.tab == Tab::Mail => Some(Msg::ReadSelected),

        KeyCode::Char('[') => Some(Msg::StepRefresh(-REFRESH_STEP_MS)),
        KeyCode::Char(']') => Some(Msg::StepRefresh(REFRESH_STEP_MS)),
        KeyCode::Char('-') if model.tab == Tab::Dashboard => Some(Msg::StepWindow(-1)),
        KeyCode::Char('+') | KeyCode::Char('=') if model.tab == Tab::Dashboard => {
            Some(Msg::StepWindow(1))
        }
        KeyCode::Char('d') => Some(Msg::ToggleDense),
        KeyCode::Char('t') => Some(Msg::ToggleRelativeTime),
        KeyCode::Char('c') if model.tab == Tab::Channels => Some(Msg::ToggleClosedChannels),
        _ => None,
    }
}

/// The keybinding overlay text (help tier 2 of the skill's three tiers).
pub fn help_lines() -> Vec<&'static str> {
    vec![
        "toron tui — keys",
        "",
        "  1..5 / Tab / Shift-Tab   tabs: mail, channels, agents, dashboard, settings",
        "",
        "  j k / ↑ ↓                move            gg / G   top / bottom",
        "  h l / ← →                panes (l opens the reader, h backs out)",
        "  space / PgDn             page down       Enter    open",
        "  /                        filter (Enter searches the index on Mail)",
        "  Esc                      clear filter / back / quit",
        "",
        "  a  acknowledge selected message       m  mark read",
        "  r  refresh now                        d  dense rows",
        "  t  relative / clock time              c  show closed channels",
        "  [ ]  refresh cadence                  - +  dashboard chart window",
        "",
        "  q  quit              ?  this help",
        "",
        "  presence is live: the Agents tab re-reads the roster every tick.",
        "  NO_COLOR=1 / TERM=dumb disables color; below 80x14 a resize card shows.",
    ]
}
