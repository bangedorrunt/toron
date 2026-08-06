// governed-by: ADR-0003

//! `toron tui` (bd-esh.4): the standalone-daemon operator dashboard.
//!
//! Operator-critical subset first (ADR-0003 open-Q): **Inbox / Agents /
//! Search** tabs over the demo mailbox. Threads/reservations land later. The
//! integrated mode's primary UI is flywheel's loop, so this is the
//! standalone-daemon surface.

use std::io::IsTerminal;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, List, ListItem, ListState, Paragraph, Tabs};
use ratatui::{DefaultTerminal, Frame};
use toron_core::mailbox::{
    FetchInbox, InboxMessage, ListAgentsOptions, Mailbox, SearchMessage, SearchMessages, WhoIs,
};

use crate::cli::TuiArgs;

/// The demo mailbox's canonical project + the dashboard's operator agent.
const PROJECT: &str = "demo";
const AGENT: &str = "TestAgent";

/// Tab labels, indexed by [`TuiState::tab`].
const TABS: [&str; 3] = ["Inbox", "Agents", "Search"];

/// Interactive dashboard state: per-tab data + navigation + last error.
#[derive(Debug, Default)]
pub struct TuiState {
    pub tab: usize,
    pub inbox: Vec<InboxMessage>,
    pub agents: Vec<WhoIs>,
    pub results: Vec<SearchMessage>,
    pub search_input: String,
    pub selected: usize,
    pub error: Option<String>,
}

/// Entry point: gate on real TTYs, build the demo mailbox, run the loop.
///
/// The TTY gate is critical: raw-mode dashboards need a key source, so on a
/// pipe `crossterm::event::poll` would block forever (and corrupt the piped
/// stream). CI and scripts get a fast, clear exit instead. Both stdout and
/// stdin are gated — a TUI needs an output TTY *and* an input TTY.
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

/// Event loop: `poll(refresh_ms)` for keys; on timeout refresh data. Returns
/// `Ok(())` when the operator quits (q / Esc / Ctrl-C).
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
    match key.code {
        KeyCode::Esc => true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => true,
        // 'q' quits everywhere except while typing a search query.
        KeyCode::Char('q') if state.tab != 2 => true,
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
        KeyCode::Up => {
            move_selection(state, -1);
            false
        }
        KeyCode::Down => {
            move_selection(state, 1);
            false
        }
        // Search tab input handling.
        KeyCode::Char(c) if state.tab == 2 => {
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

/// Re-fetch inbox + agents each tick (the demo index is cheap). Search only
/// re-runs on Enter.
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

/// Render one frame: header, tab bar, active view, footer.
fn ui(f: &mut Frame, state: &mut TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(f.area());

    let header = Paragraph::new(format!(
        "toron tui — operator dashboard    view: {} / {}    q: quit / Tab: next",
        state.tab + 1,
        TABS.len()
    ));
    f.render_widget(header, chunks[0]);

    let tabs = Tabs::new(TABS)
        .select(state.tab)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED));
    f.render_widget(tabs, chunks[1]);

    match state.tab {
        0 => render_inbox(f, chunks[2], state),
        1 => render_agents(f, chunks[2], state),
        _ => render_search(f, chunks[2], state),
    }

    let footer = Paragraph::new(footer_hint(state).to_string());
    f.render_widget(footer, chunks[3]);
}

fn render_inbox(f: &mut Frame, area: ratatui::layout::Rect, state: &mut TuiState) {
    if let Some(err) = &state.error {
        render_error(f, area, err);
        return;
    }
    if state.inbox.is_empty() {
        f.render_widget(Paragraph::new("no mail"), area);
        return;
    }
    let items: Vec<ListItem> = state
        .inbox
        .iter()
        .map(|m| {
            let ack = if m.ack_required { " [ack]" } else { "" };
            ListItem::new(format!(
                "{} — {}{} ({})",
                m.sender, m.subject, ack, m.importance
            ))
        })
        .collect();
    let list = List::new(items)
        .block(Block::bordered().title("Inbox"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");
    f.render_stateful_widget(list, area, &mut selected_state(state, state.inbox.len()));
}

fn render_agents(f: &mut Frame, area: ratatui::layout::Rect, state: &mut TuiState) {
    if let Some(err) = &state.error {
        render_error(f, area, err);
        return;
    }
    if state.agents.is_empty() {
        f.render_widget(Paragraph::new("no agents"), area);
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
                ListItem::new(a.name.clone())
            } else {
                ListItem::new(format!("{} ({role})", a.name))
            }
        })
        .collect();
    let list = List::new(items)
        .block(Block::bordered().title("Agents"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");
    f.render_stateful_widget(list, area, &mut selected_state(state, state.agents.len()));
}

fn render_search(f: &mut Frame, area: ratatui::layout::Rect, state: &mut TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(area);

    let input = Paragraph::new(state.search_input.as_str())
        .block(Block::bordered().title("query (Enter: search)"));
    f.render_widget(input, chunks[0]);

    if let Some(err) = &state.error {
        render_error(f, chunks[1], err);
        return;
    }
    if state.search_input.trim().is_empty() {
        f.render_widget(
            Paragraph::new("type a query and press Enter to search the index"),
            chunks[1],
        );
        return;
    }
    if state.results.is_empty() {
        f.render_widget(Paragraph::new("no results"), chunks[1]);
        return;
    }
    let items: Vec<ListItem> = state
        .results
        .iter()
        .map(|m| {
            let ack = if m.ack_required { " [ack]" } else { "" };
            ListItem::new(format!("{} — {}{}", m.from, m.subject, ack))
        })
        .collect();
    let list = List::new(items)
        .block(Block::bordered().title("Results"))
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");
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

fn render_error(f: &mut Frame, area: ratatui::layout::Rect, err: &str) {
    let paragraph = Paragraph::new(err).style(Style::default().fg(Color::Red));
    f.render_widget(paragraph, area);
}

fn footer_hint(state: &TuiState) -> &'static str {
    match state.tab {
        0 => "inbox: refreshed every tick — q: quit / Tab: next",
        1 => "agents: refreshed every tick — q: quit / Tab: next",
        _ => "search: type + Enter to run — q: quit / Tab: next",
    }
}
