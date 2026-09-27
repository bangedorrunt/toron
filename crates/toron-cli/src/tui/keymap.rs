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

use super::compose::CaretMove;
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
        Mode::Compose => compose_mode(key),
        Mode::Normal => normal_mode(model, key),
    }
}

/// Typing in the compose form. Every printable key goes to the focused field;
/// the form's own verbs are `Tab` (next stop) and `Enter` (advance, insert a
/// line, toggle, or send — the fold reads the focused stop).
///
/// There is no send chord on purpose: `Ctrl-S` is XOFF where flow control is
/// still honoured and `Ctrl-Enter` is not distinguishable from `Enter` on
/// several terminals, so a chord would be a submit key that sometimes does
/// nothing. `Enter` on the `send` stop is always available.
fn compose_mode(key: KeyEvent) -> Option<Msg> {
    let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
    // Ctrl-C keeps its one meaning from normal mode: leave the console now.
    if ctrl && matches!(key.code, KeyCode::Char('c')) {
        return Some(Msg::Quit);
    }
    match key.code {
        KeyCode::Esc => Some(Msg::ComposeCancel),
        KeyCode::Tab => Some(Msg::ComposeFocus(1)),
        KeyCode::BackTab => Some(Msg::ComposeFocus(-1)),
        KeyCode::Enter => Some(Msg::ComposeEnter),
        KeyCode::Backspace => Some(Msg::ComposeBackspace),
        KeyCode::Delete => Some(Msg::ComposeDelete),
        KeyCode::Left => Some(Msg::ComposeMove(CaretMove::Left)),
        KeyCode::Right => Some(Msg::ComposeMove(CaretMove::Right)),
        KeyCode::Up => Some(Msg::ComposeMove(CaretMove::Up)),
        KeyCode::Down => Some(Msg::ComposeMove(CaretMove::Down)),
        KeyCode::Home => Some(Msg::ComposeMove(CaretMove::Home)),
        KeyCode::End => Some(Msg::ComposeMove(CaretMove::End)),
        KeyCode::Char(c) if !ctrl => Some(Msg::ComposeInsert(c)),
        _ => None,
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
        // `c` composes everywhere; the closed-channel filter moved to `C` so
        // the console's primary action is not tab-scoped.
        KeyCode::Char('c') => Some(Msg::OpenCompose),

        KeyCode::Char('[') => Some(Msg::StepRefresh(-REFRESH_STEP_MS)),
        KeyCode::Char(']') => Some(Msg::StepRefresh(REFRESH_STEP_MS)),
        KeyCode::Char('-') if model.tab == Tab::Dashboard => Some(Msg::StepWindow(-1)),
        KeyCode::Char('+') | KeyCode::Char('=') if model.tab == Tab::Dashboard => {
            Some(Msg::StepWindow(1))
        }
        KeyCode::Char('d') => Some(Msg::ToggleDense),
        KeyCode::Char('t') => Some(Msg::ToggleRelativeTime),
        KeyCode::Char('C') if model.tab == Tab::Channels => Some(Msg::ToggleClosedChannels),
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
        "  j k / ↑ ↓                move            g / G    top / bottom",
        "  h l / ← →                panes (l opens the reader, h backs out)",
        "  space / PgDn             page down       Enter    open",
        "  /                        filter (Enter searches the index on Mail)",
        "  Esc                      clear filter / back / quit",
        "",
        "  c  compose / reply (Tab moves the form, Enter advances)",
        "  a  acknowledge selected message       m  mark read",
        "  r  refresh now                        d  dense rows",
        "  t  relative / clock time              C  show closed channels",
        "  [ ]  refresh cadence                  - +  dashboard chart window",
        "",
        "  q  quit              ?  this help",
        "",
        "  compose: c opens the form on any tab. Tab / Shift-Tab walk To, Subject,",
        "  Body, Wake, Importance, Ack, send. Enter advances, inserts the body's newline,",
        "  toggles, or sends on the send stop. Esc drops the draft; Ctrl-C quits.",
        "",
        "  presence is live: the Agents tab re-reads the roster every tick.",
        "  40-79 cols (phone width): one pane at a time, h / l switches, Enter opens.",
        "  NO_COLOR=1 / TERM=dumb disables color; below 40x12 a resize card shows.",
    ]
}
