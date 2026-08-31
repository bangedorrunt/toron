// governed-by: ADR-0003

//! `toron tui` (bd-esh.4): the standalone-daemon operator dashboard.
//! hacker-night neon-on-void theme — semantic slots via `crate::theme`.

use std::io::IsTerminal;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs};
use ratatui::{DefaultTerminal, Frame};
use toron_core::mailbox::{
    FetchInbox, InboxMessage, ListAgentsOptions, Mailbox, SearchMessage, SearchMessages, WhoIs,
};

use crate::cli::TuiArgs;
use crate::theme::HackerNight;

/// The demo mailbox's canonical project + the dashboard's operator agent.
const PROJECT: &str = "demo";
const AGENT: &str = "TestAgent";

/// Tab labels, indexed by [`TuiState::tab`].
const TABS: [&str; 3] = ["Inbox", "Agents", "Search"];

/// Minimum usable terminal size — below this we show a resize card (skill §2).
const MIN_W: u16 = 80;
const MIN_H: u16 = 12;

/// Interactive dashboard state: per-tab data + navigation + last error + help.
#[derive(Debug, Default)]
pub struct TuiState {
    pub tab: usize,
    pub inbox: Vec<InboxMessage>,
    pub agents: Vec<WhoIs>,
    pub results: Vec<SearchMessage>,
    pub search_input: String,
    pub selected: usize,
    pub error: Option<String>,
    pub show_help: bool,
    pub tick: u64,
}

/// Entry point: gate on real TTYs, build the demo mailbox, run the loop.
#[tokio::main]
pub async fn run_tui(args: &TuiArgs) -> anyhow::Result<()> {
    if !std::io::stdout().is_terminal() || !std::io::stdin().is_terminal() {
        anyhow::bail!("toron tui requires a TTY (refusing to run on a pipe)");
    }

    let mailbox = toron_mcp::demo_mailbox().await?;

    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mailbox, args).await;
    ratatui::restore();
    result
}

/// Event loop: `poll(refresh_ms)` for keys; on timeout refresh data.
async fn run_loop(
    terminal: &mut DefaultTerminal,
    mailbox: &Mailbox,
    args: &TuiArgs,
) -> anyhow::Result<()> {
    let mut state = TuiState::default();
    refresh(&mut state, mailbox).await;

    loop {
        if event::poll(Duration::from_millis(args.refresh_ms))? {
            let ev = event::read()?;
            if handle_event(&mut state, ev, mailbox).await {
                break;
            }
        } else {
            state.tick = state.tick.wrapping_add(1);
            refresh(&mut state, mailbox).await;
        }
        terminal.draw(|f| ui(f, &mut state))?;
    }
    Ok(())
}

/// Route one key event. Returns `true` when the operator wants to quit.
async fn handle_event(state: &mut TuiState, ev: Event, mailbox: &Mailbox) -> bool {
    let Event::Key(key) = ev else {
        return false;
    };
    if key.kind != KeyEventKind::Press {
        return false;
    }
    // help overlay consumes Esc / ? / q before normal routing
    if state.show_help {
        match key.code {
            KeyCode::Esc | KeyCode::Char('?') | KeyCode::Char('q') => {
                state.show_help = false;
            }
            _ => {}
        }
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            // search tab: Esc clears input first, second Esc quits
            if state.tab == 2 && !state.search_input.is_empty() {
                state.search_input.clear();
                state.results.clear();
                state.error = None;
                return false;
            }
            true
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => true,
        KeyCode::Char('?') => {
            state.show_help = true;
            false
        }
        // 'q' quits everywhere except while typing search (but allow q as quit
        // when search input is empty — skill §3 progressive disclosure).
        KeyCode::Char('q') if state.tab != 2 || state.search_input.is_empty() => true,
        KeyCode::Char('/') if state.tab != 2 => {
            state.tab = 2;
            state.selected = 0;
            false
        }
        KeyCode::Tab => {
            state.tab = (state.tab + 1) % TABS.len();
            state.selected = 0;
            false
        }
        KeyCode::BackTab => {
            state.tab = (state.tab + TABS.len() - 1) % TABS.len();
            state.selected = 0;
            false
        }
        // vim motions: j/k, g/G, n/N (where applicable), plus arrows
        KeyCode::Char('j') | KeyCode::Down => {
            move_selection(state, 1);
            false
        }
        KeyCode::Char('k') | KeyCode::Up => {
            move_selection(state, -1);
            false
        }
        KeyCode::Char('g') => {
            goto_selection(state, 0);
            false
        }
        KeyCode::Char('G') => {
            goto_selection(state, usize::MAX);
            false
        }
        // Search tab input handling — char push is below so j/k don't double-fire
        KeyCode::Char(c) if state.tab == 2 => {
            // already handled j/k/G via explicit arms above, but g falls through
            // to here when tab==2 — treat g as input unless it's uppercase G
            // (handled). Push char for search.
            // We already matched ? and / — remaining chars are input.
            // Special: 'j'/'k' when search tab should also be input if the user
            // is typing — but we chose motion priority (vim-like). To type j/k
            // in search, the current behavior requires the motion; tradeoff
            // noted. For now keep motion priority.
            state.search_input.push(c);
            false
        }
        KeyCode::Backspace if state.tab == 2 => {
            state.search_input.pop();
            false
        }
        KeyCode::Enter if state.tab == 2 => {
            run_search(state, mailbox).await;
            false
        }
        _ => false,
    }
}

/// Clamp `state.selected` into `[0, len)` for the active tab's list.
fn move_selection(state: &mut TuiState, delta: i64) {
    let len = match state.tab {
        0 => state.inbox.len(),
        1 => state.agents.len(),
        _ => state.results.len(),
    };
    if len == 0 {
        state.selected = 0;
        return;
    }
    state.selected = (state.selected as i64 + delta).clamp(0, len as i64 - 1) as usize;
}

fn goto_selection(state: &mut TuiState, target: usize) {
    let len = match state.tab {
        0 => state.inbox.len(),
        1 => state.agents.len(),
        _ => state.results.len(),
    };
    if len == 0 {
        state.selected = 0;
        return;
    }
    state.selected = target.min(len - 1);
}

/// Re-fetch inbox + agents each tick (the demo index is cheap).
async fn refresh(state: &mut TuiState, mailbox: &Mailbox) {
    let inbox = mailbox
        .fetch_inbox(&FetchInbox {
            project_slug: PROJECT.into(),
            agent_name: AGENT.into(),
            include_bodies: false,
            ..Default::default()
        })
        .await;
    let agents = mailbox
        .list_agents(PROJECT, ListAgentsOptions::default())
        .await;

    state.error = match (inbox, agents) {
        (Ok(inbox), Ok(agents)) => {
            state.inbox = inbox;
            state.agents = agents;
            None
        }
        (Ok(inbox), Err(e)) => {
            state.inbox = inbox;
            Some(format!("agents: {e:#}"))
        }
        (Err(e), Ok(agents)) => {
            state.agents = agents;
            Some(format!("inbox: {e:#}"))
        }
        (Err(e), Err(_)) => Some(format!("inbox: {e:#}")),
    };
}

/// Run an FTS5 search over the demo project's indexed messages.
async fn run_search(state: &mut TuiState, mailbox: &Mailbox) {
    let query = state.search_input.trim().to_string();
    if query.is_empty() {
        state.error = Some("enter a query to search".into());
        state.results.clear();
        return;
    }
    match mailbox
        .search_messages(&SearchMessages {
            project_key: PROJECT.into(),
            query,
            ..Default::default()
        })
        .await
    {
        Ok(results) => {
            state.results = results;
            state.error = None;
        }
        Err(e) => state.error = Some(format!("search: {e:#}")),
    }
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

/// Render one frame: header, tab bar, active view, footer + optional help.
fn ui(f: &mut Frame, state: &mut TuiState) {
    let area = f.area();
    // responsive gate (skill §2)
    if area.width < MIN_W || area.height < MIN_H {
        render_resize_card(f, area);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);

    // header — hacker-night void + muted + neon accents
    let live = spinner(state.tick);
    let header = Paragraph::new(format!(
        "toron ◆ operator dashboard  ·  {}/{}  ·  demo/TestAgent  ·  {} live",
        PROJECT, AGENT, live
    ))
    .style(HackerNight::header());
    f.render_widget(header, chunks[0]);

    let tabs = Tabs::new(TABS)
        .select(state.tab)
        .style(HackerNight::tabs_inactive())
        .highlight_style(HackerNight::tabs_active())
        .divider("  ");
    f.render_widget(tabs, chunks[1]);

    match state.tab {
        0 => render_inbox(f, chunks[2], state),
        1 => render_agents(f, chunks[2], state),
        _ => render_search(f, chunks[2], state),
    }

    let footer = Paragraph::new(footer_hint(state).to_string()).style(HackerNight::fg_muted());
    f.render_widget(footer, chunks[3]);

    if state.show_help {
        render_help_overlay(f, area);
    }
}

fn render_resize_card(f: &mut Frame, area: Rect) {
    let msg = format!(
        "terminal too small ({}×{}) — need at least {}×{} — resize to continue",
        area.width, area.height, MIN_W, MIN_H
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" resize ")
        .title_style(HackerNight::title_focused());
    let w = (msg.len() as u16 + 6)
        .min(area.width.saturating_sub(2))
        .max(30);
    let p = Paragraph::new(msg.clone())
        .style(HackerNight::warning())
        .block(block)
        .centered();
    let h = 5u16;
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    f.render_widget(
        Clear,
        Rect {
            x,
            y,
            width: w,
            height: h,
        },
    );
    f.render_widget(
        p,
        Rect {
            x,
            y,
            width: w,
            height: h,
        },
    );
}

fn block_focused(title: &str) -> Block<'_> {
    Block::bordered()
        .title(format!(" {title} "))
        .title_style(HackerNight::title_focused())
        .border_style(HackerNight::border_focused())
}

fn block_unfocused(title: &str) -> Block<'_> {
    Block::bordered()
        .title(format!(" {title} "))
        .title_style(HackerNight::title_unfocused())
        .border_style(HackerNight::border_unfocused())
}

fn render_inbox(f: &mut Frame, area: Rect, state: &mut TuiState) {
    if let Some(err) = &state.error {
        render_error(f, area, err);
        return;
    }
    if state.inbox.is_empty() {
        f.render_widget(
            Paragraph::new("no mail")
                .style(HackerNight::fg_muted())
                .block(block_unfocused("Inbox")),
            area,
        );
        return;
    }
    let items: Vec<ListItem> = state
        .inbox
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let is_sel = i == state.selected;
            let ack = if m.ack_required { " ◉ ack" } else { "" };
            let importance_badge = match m.importance.as_str() {
                "high" | "urgent" => format!(" [{}] ", m.importance.to_uppercase()),
                _ => format!(" ({}) ", m.importance),
            };
            // sender bold when selected, muted importance
            if is_sel {
                ListItem::new(format!(
                    "{} — {}{}{}",
                    m.sender, m.subject, ack, importance_badge
                ))
                .style(HackerNight::fg_default())
            } else {
                ListItem::new(format!(
                    "{} — {}{}{}",
                    m.sender, m.subject, ack, importance_badge
                ))
                .style(Style::default().fg(HackerNight::MUTED))
            }
        })
        .collect();
    let list = List::new(items)
        .block(block_focused("Inbox"))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, area, &mut selected_state(state, state.inbox.len()));
}

fn render_agents(f: &mut Frame, area: Rect, state: &mut TuiState) {
    if let Some(err) = &state.error {
        render_error(f, area, err);
        return;
    }
    if state.agents.is_empty() {
        f.render_widget(
            Paragraph::new("no agents")
                .style(HackerNight::fg_muted())
                .block(block_unfocused("Agents")),
            area,
        );
        return;
    }
    let items: Vec<ListItem> = state
        .agents
        .iter()
        .map(|a| {
            let role = match (&a.program, &a.model) {
                (Some(p), Some(m)) => format!("{p} / {m}"),
                (Some(p), None) | (None, Some(p)) => p.clone(),
                (None, None) => String::new(),
            };
            if role.is_empty() {
                ListItem::new(a.name.clone()).style(HackerNight::fg_default())
            } else {
                ListItem::new(format!("{}  {}", a.name, role)).style(HackerNight::fg_default())
            }
        })
        .collect();
    let list = List::new(items)
        .block(block_focused("Agents"))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(list, area, &mut selected_state(state, state.agents.len()));
}

fn render_search(f: &mut Frame, area: Rect, state: &mut TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(1)])
        .split(area);

    let input_style = if state.show_help {
        HackerNight::fg_muted()
    } else {
        HackerNight::fg_default()
    };
    let input = Paragraph::new(state.search_input.as_str())
        .style(input_style)
        .block(block_focused("query  (Enter: search  ·  Esc: clear)"));
    f.render_widget(input, chunks[0]);

    if let Some(err) = &state.error {
        render_error(f, chunks[1], err);
        return;
    }
    if state.search_input.trim().is_empty() {
        f.render_widget(
            Paragraph::new("type a query and press Enter to search the index  ·  / focuses search")
                .style(HackerNight::fg_muted())
                .block(block_unfocused("Results")),
            chunks[1],
        );
        return;
    }
    if state.results.is_empty() {
        f.render_widget(
            Paragraph::new("no results")
                .style(HackerNight::fg_muted())
                .block(block_unfocused("Results")),
            chunks[1],
        );
        return;
    }
    let items: Vec<ListItem> = state
        .results
        .iter()
        .map(|m| {
            let ack = if m.ack_required { " ◉" } else { "" };
            ListItem::new(format!("{} — {}{}", m.from, m.subject, ack))
                .style(HackerNight::fg_default())
        })
        .collect();
    let list = List::new(items)
        .block(block_focused("Results"))
        .highlight_style(HackerNight::selection())
        .highlight_symbol("▶ ");
    f.render_stateful_widget(
        list,
        chunks[1],
        &mut selected_state(state, state.results.len()),
    );
}

/// A `ListState` with `state.selected` clamped to the visible list length.
fn selected_state(state: &TuiState, len: usize) -> ListState {
    let mut list_state = ListState::default();
    let selected = if len == 0 {
        0
    } else {
        state.selected.min(len - 1)
    };
    list_state.select(Some(selected));
    list_state
}

fn render_error(f: &mut Frame, area: Rect, err: &str) {
    // never color alone — prefix with ✗ (skill §4 accessibility)
    let text = format!("✗ {err}");
    let paragraph = Paragraph::new(text)
        .style(HackerNight::error())
        .block(block_focused("error"))
        .wrap(ratatui::widgets::Wrap { trim: true });
    f.render_widget(paragraph, area);
}

fn footer_hint(state: &TuiState) -> String {
    match state.tab {
        0 => format!(
            "[q] quit  [Tab] tab  [↑↓/jk] nav  [g/G] top/bottom  [/] search  [?] help  {}",
            spinner(state.tick)
        ),
        1 => format!(
            "[q] quit  [Tab] tab  [↑↓/jk] nav  [g/G] top/bottom  [?] help  {}",
            spinner(state.tick)
        ),
        _ => "[Enter] search  [Esc] clear  [q] quit  [Tab] tab  [↑↓/jk] nav  [?] help".to_string(),
    }
}

fn spinner(tick: u64) -> &'static str {
    const FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
    FRAMES[(tick as usize) % FRAMES.len()]
}

fn render_help_overlay(f: &mut Frame, area: Rect) {
    let w = (area.width * 3 / 4).clamp(48, 72);
    let h = 16u16;
    let x = area.x + (area.width.saturating_sub(w)) / 2;
    let y = area.y + (area.height.saturating_sub(h)) / 2;
    let help_area = Rect {
        x,
        y,
        width: w,
        height: h,
    };

    let help_text = [
        "toron tui — hacker-night  ·  keyboard",
        "",
        "  q / Esc / Ctrl-C     quit (Esc clears search first)",
        "  Tab / Shift-Tab       next / prev tab",
        "  ↑↓ / j k              navigate list",
        "  g / G                 top / bottom",
        "  /                     focus Search tab",
        "  Enter                 run search (Search tab)",
        "  ? / Esc               toggle / close this help",
        "",
        "  NO_COLOR=1 or TERM=dumb disables color.",
        "  Resize below 80×12 shows a resize card.",
    ]
    .join("\n");

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(HackerNight::border_focused())
        .title(" help  [?/Esc/q to close] ")
        .title_style(HackerNight::title_focused())
        .style(
            Style::default()
                .bg(HackerNight::OVERLAY)
                .fg(HackerNight::FG),
        );

    let p = Paragraph::new(help_text)
        .style(
            Style::default()
                .bg(HackerNight::OVERLAY)
                .fg(HackerNight::FG),
        )
        .block(block)
        .wrap(ratatui::widgets::Wrap { trim: true });

    f.render_widget(Clear, help_area);
    f.render_widget(p, help_area);
}
