// governed-by: ADR-0003
//! Frame rendering: header, tab bar, the responsive body, footer + status,
//! and the modal help. Pure draw — the model is never mutated here.

pub mod agents;
pub mod channels;
pub mod compose;
pub mod dashboard;
pub mod mail;
pub mod settings;
pub mod widgets;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap};

use crate::theme::HackerNight;

use super::keymap;
use super::model::{self, LayoutMode, MIN_H, MIN_W, Mode, Model, Pane, StatusKind, Tab};

/// The pane rectangles for one frame (sidebar only exists in the 3-pane mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Panes {
    /// Folder/channel sidebar, when the width allows it.
    pub sidebar: Option<Rect>,
    /// The list pane.
    pub list: Rect,
    /// The reader pane (equals [`Self::list`] when stacked).
    pub reader: Rect,
}

/// Split the body area for a layout mode. Sidebar ~20% (clamped), list ~38%
/// of the remainder, reader takes the rest — proportional, never absolute
/// (§2).
///
/// `sidebar` is the calling tab's own answer to "do I have a folder/room
/// column?": the Agents tab does not, so it gets the full width back instead
/// of an empty gutter. One pane at a time (`Stacked`) gives the focused pane
/// the whole frame — that is how a 45-column phone terminal works.
pub fn split(area: Rect, mode: LayoutMode, sidebar: bool) -> Panes {
    if mode.single() || mode == LayoutMode::TooSmall {
        return Panes {
            sidebar: None,
            list: area,
            reader: area,
        };
    }
    let sidebar_w = match (sidebar, mode) {
        (false, _) => 0,
        (true, LayoutMode::Tri) => (area.width / 5).clamp(18, 28),
        (true, _) => (area.width / 6).clamp(14, 20),
    };
    let inner = Rect {
        x: area.x + sidebar_w,
        width: area.width.saturating_sub(sidebar_w),
        ..area
    };
    let list_w = match mode {
        LayoutMode::Tri => ((inner.width as u32 * 38 / 100) as u16)
            .max(28)
            .min(inner.width),
        _ => ((inner.width as u32 * 45 / 100) as u16)
            .max(24)
            .min(inner.width.saturating_sub(20).max(1)),
    };
    Panes {
        sidebar: (sidebar_w > 0).then_some(Rect {
            width: sidebar_w,
            ..area
        }),
        list: Rect {
            width: list_w,
            ..inner
        },
        reader: Rect {
            x: inner.x + list_w,
            width: inner.width.saturating_sub(list_w),
            ..inner
        },
    }
}

/// Render one frame.
pub fn render(f: &mut Frame, model: &Model) {
    let area = f.area();
    let mode = model::layout_mode(area.width, area.height);
    if mode == LayoutMode::TooSmall {
        render_resize_card(f, area);
        return;
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(2),
        ])
        .split(area);

    render_header(f, rows[0], model, mode.single());
    render_tabs(f, rows[1], model, mode.single());
    match model.tab {
        Tab::Mail => mail::render(f, rows[2], model, mode),
        Tab::Channels => channels::render(f, rows[2], model, mode),
        Tab::Agents => agents::render(f, rows[2], model, mode),
        Tab::Dashboard => dashboard::render(f, rows[2], model),
        Tab::Settings => settings::render(f, rows[2], model),
    }
    render_footer(f, rows[3], model, mode.single());

    if model.mode == Mode::Help {
        render_help(f, area);
    }
    if model.mode == Mode::Compose {
        compose::render(f, area, model);
    }
}

/// Header: identity, store, liveness. The spinner is tick-driven, so a
/// frozen clock is visible as a frozen spinner.
fn render_header(f: &mut Frame, area: Rect, model: &Model, narrow: bool) {
    let dot = match model.status.kind {
        StatusKind::Error => Span::styled("✗", HackerNight::error()),
        StatusKind::Ok => Span::styled("✓", HackerNight::success()),
        StatusKind::Info => Span::styled("◆", HackerNight::accent_cyan()),
    };
    let who = match &model.settings.agent {
        Some(pin) => format!("{}/{}", model.settings.project, pin),
        None => format!("{}/(no pin)", model.settings.project),
    };
    let busy = if model.inflight > 0 {
        format!("{} {} in flight", spinner(model.tick), model.inflight)
    } else {
        format!("{} live", spinner(model.tick))
    };
    let line = if narrow {
        Line::from(vec![
            Span::styled(" toron ", HackerNight::fg_emphasis()),
            dot,
            Span::styled(
                format!(" {} ", model.tab.title()),
                HackerNight::accent_cyan(),
            ),
            Span::styled(who, HackerNight::fg_default()),
            Span::styled(format!(" {busy}"), HackerNight::accent_lav()),
        ])
    } else {
        Line::from(vec![
            Span::styled(" toron ", HackerNight::fg_emphasis()),
            dot,
            Span::styled(format!(" {who}"), HackerNight::fg_default()),
            Span::styled(
                format!("  ·  {:<18}", model.settings.source.kind),
                HackerNight::fg_muted(),
            ),
            Span::styled(busy, HackerNight::accent_lav()),
            Span::styled(format!("  ·  tick {}", model.tick), HackerNight::fg_muted()),
        ])
    };
    f.render_widget(Paragraph::new(line).style(HackerNight::header()), area);
}

/// Tab bar with per-tab counts (unread mail, rooms, live agents). At phone
/// width the labels collapse to `1M 2C …` — five full titles are 60 columns.
fn render_tabs(f: &mut Frame, area: Rect, model: &Model, narrow: bool) {
    let counts: Vec<String> = Tab::ALL
        .iter()
        .map(|tab| {
            if narrow {
                return format!(
                    "{}{}",
                    tab.index() + 1,
                    tab.title().chars().next().unwrap_or('?')
                );
            }
            let n = match tab {
                Tab::Mail => model.mail.messages.iter().filter(|m| m.unread()).count(),
                Tab::Channels => model.channels.channels.len(),
                Tab::Agents => model.agents.online(),
                Tab::Dashboard | Tab::Settings => 0,
            };
            format!("{} {} {}", tab.index() + 1, tab.title(), badge(n))
                .trim_end()
                .to_string()
        })
        .collect();
    let tabs = Tabs::new(counts)
        .select(model.tab.index())
        .style(HackerNight::tabs_inactive())
        .highlight_style(HackerNight::tabs_active())
        .divider(if narrow { " " } else { "  " });
    f.render_widget(tabs, area);
}

/// `N` when non-zero, blank otherwise (no `(0)` noise).
fn badge(n: usize) -> String {
    if n == 0 {
        String::new()
    } else {
        format!("({n})")
    }
}

/// Footer: status line + context-sensitive key hints (skill §3 tiers 1–2).
fn render_footer(f: &mut Frame, area: Rect, model: &Model, narrow: bool) {
    let frame = f.area();
    let layout = widgets::layout_label(model::layout_mode(frame.width, frame.height));
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(area);

    let status_style = match model.status.kind {
        StatusKind::Error => HackerNight::error(),
        StatusKind::Ok => HackerNight::success(),
        StatusKind::Info => HackerNight::fg_muted(),
    };
    let status = if model.status.text.is_empty() {
        let text = if narrow {
            format!(" {}ms  ·  {layout}", model.settings.refresh_ms)
        } else {
            format!(
                " refresh {}ms  ·  {layout}  ·  {}",
                model.settings.refresh_ms, model.settings.source.detail,
            )
        };
        Line::from(Span::styled(text, HackerNight::fg_muted()))
    } else {
        let shape = match model.status.kind {
            StatusKind::Error => "✗",
            StatusKind::Ok => "✓",
            StatusKind::Info => "·",
        };
        Line::from(vec![
            Span::styled(format!(" {shape} "), status_style),
            Span::styled(model.status.text.clone(), status_style),
        ])
    };
    f.render_widget(Paragraph::new(status), rows[0]);

    let hints = hint_line(model, narrow);
    f.render_widget(
        Paragraph::new(hints).style(HackerNight::fg_muted()),
        rows[1],
    );
}

/// Phone-width hints: the stable keys only. The focused pane's title says
/// where you are, and `?` lists the rest.
const NARROW_HINTS: &str = "[?] help  [q] quit  [1-5] tab";

/// The footer hint text as a plain string, so a test can assert the console
/// documents its own keys. The renderer calls `hint_line` directly.
#[cfg(test)]
pub(crate) fn hint_line_for_test(model: &Model, narrow: bool) -> String {
    hint_line(model, narrow)
        .spans
        .iter()
        .map(|s| s.content.to_string())
        .collect()
}

/// Context-sensitive hints: only what is actionable right now.
fn hint_line(model: &Model, narrow: bool) -> Line<'static> {
    let text = match model.mode {
        Mode::Help => "[Esc/?/q] close help",
        Mode::Insert => "[Enter] apply  [Esc] cancel  [type] filter",
        Mode::Compose => "[Tab] next  [Enter] advance/toggle/send  [Esc] cancel  [^C] quit",
        Mode::Normal if narrow => NARROW_HINTS,
        Mode::Normal => match (model.tab, model.pane) {
            (Tab::Mail, Pane::Reader) => {
                "[j/k] scroll  [c] reply  [a] ack  [m] read  [h/Esc] back  [?] help  [q] quit"
            }
            (Tab::Mail, Pane::Sidebar) => {
                "[j/k] folder  [l] list  [1-5] tab  [r] refresh  [?] help  [q] quit"
            }
            (Tab::Mail, Pane::List) => {
                "[j/k] move  [Enter/l] open  [c] compose  [a] ack  [/] filter  [?] help  [q] quit"
            }
            (Tab::Channels, Pane::Sidebar) => {
                "[j/k] channel  [l] posts  [c] compose  [C] closed  [r] refresh  [?] help"
            }
            (Tab::Channels, _) => {
                "[j/k] post  [Enter] open  [c] compose  [C] closed  [/] filter  [?] help"
            }
            (Tab::Agents, _) => {
                "[j/k] agent  [Enter] detail  [c] message  [/] filter  [?] help  [q] quit"
            }
            (Tab::Dashboard, _) => "[r] refresh  [-/+] window  [?] help  [1-5] tab  [q] quit",
            (Tab::Settings, _) => "[[] slower  []] faster  [d] dense  [t] time  [?] help  [q] quit",
        },
    };
    Line::from(Span::styled(text.to_string(), HackerNight::fg_muted()))
}

const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Spinner frame for the tick counter.
pub const fn spinner(tick: u64) -> &'static str {
    FRAMES[(tick as usize) % FRAMES.len()]
}

/// The minimum-size gate (§2): say what is wrong and what to do.
fn render_resize_card(f: &mut Frame, area: Rect) {
    let msg = format!(
        "terminal too small ({}×{}) — need at least {}×{}\n\nresize to continue; the fleet keeps ticking",
        area.width, area.height, MIN_W, MIN_H
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" resize ")
        .title_style(HackerNight::title_focused());
    let w = (msg.len() as u16 / 2 + 8)
        .clamp(30, area.width.saturating_sub(2).max(30))
        .min(area.width.max(1));
    let h = 6u16.min(area.height.max(1));
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    let card = Rect {
        x,
        y,
        width: w,
        height: h,
    };
    f.render_widget(Clear, card);
    f.render_widget(
        Paragraph::new(msg)
            .style(HackerNight::warning())
            .block(block)
            .centered()
            .wrap(Wrap { trim: true }),
        card,
    );
}

/// Modal keybinding overlay (focus trap: the background gets no keys).
fn render_help(f: &mut Frame, area: Rect) {
    let lines = keymap::help_lines();
    let w = (area.width * 3 / 4).clamp(52, 84).min(area.width);
    let h = (lines.len() as u16 + 2).min(area.height);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    let card = Rect {
        x,
        y,
        width: w,
        height: h,
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" help  [?/Esc/q to close] ")
        .title_style(HackerNight::title_focused())
        .style(
            ratatui::style::Style::default()
                .bg(HackerNight::OVERLAY)
                .fg(HackerNight::FG),
        );
    let text: Vec<Line> = lines
        .into_iter()
        .map(|l| Line::from(Span::styled(l.to_string(), HackerNight::fg_default())))
        .collect();
    f.render_widget(Clear, card);
    f.render_widget(
        Paragraph::new(text)
            .block(block)
            .style(
                ratatui::style::Style::default()
                    .bg(HackerNight::OVERLAY)
                    .fg(HackerNight::FG),
            )
            .wrap(Wrap { trim: false }),
        card,
    );
}
