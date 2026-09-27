// governed-by: ADR-0031
//! The fold: `tick(&Model, Msg) -> (Model, Vec<Effect>)`.
//!
//! Pure. No awaits, no clock, no I/O, no second reducer for the same model
//! (ADR-0031 §7). The runtime executes the returned effects and feeds their
//! outcomes back as messages; the same message log always produces the same
//! model and the same effect list.
//!
//! Cadence: a tick refreshes the active tab, and the Agents tab refreshes every
//! tick because presence *is* its content. Channels and the dashboard ride a
//! slower cadence (bus replay + receipt counts are the expensive reads), and a
//! tick with effects still in flight only advances the spinner — a slow store
//! must never queue a second wave of reads.

use super::model::{
    Folder, ListRow, MailRow, Mode, Model, OpenThread, Pane, StatusKind, Tab, thread_id_of,
};
use super::msg::{Effect, Msg};

/// Ticks between the slow-cadence refreshes (bus replay + receipt counts).
pub const SLOW_CADENCE: u64 = 3;

/// Fold one message into the model, returning the effects to execute.
pub fn tick(model: &Model, msg: Msg) -> (Model, Vec<Effect>) {
    let mut m = model.clone();
    let mut effects = Vec::new();
    match msg {
        Msg::Init => effects.extend(initial_refresh(&m)),

        Msg::Tick { now_unix } => {
            m.now_unix = now_unix;
            m.tick = m.tick.wrapping_add(1);
            effects.extend(tick_refresh(&m));
        }

        Msg::InboxLoaded(rows) => {
            m.mail.messages = rows;
            m.mail.loaded = true;
            m.clamp_cursor();
        }
        Msg::ThreadLoaded { thread_id, rows } => {
            // A stale thread (the operator moved on) never clobbers the reader.
            let want = m.mail.open.as_ref().map(|o| o.thread_id.as_str());
            if want.is_none() || want == Some(thread_id.as_str()) {
                // Narrow the body-carrying page to this thread. A top-level
                // message IS its own thread and carries no thread tag, so the
                // page is the only way to see it — and when the page no longer
                // holds it (age, limit), fall back to the rows already on
                // screen rather than opening an empty reader.
                let mut messages: Vec<MailRow> = rows
                    .iter()
                    .filter(|r| thread_id_of(r) == thread_id)
                    .cloned()
                    .collect();
                if messages.is_empty() {
                    messages = m
                        .mail
                        .messages
                        .iter()
                        .filter(|r| thread_id_of(r) == thread_id)
                        .cloned()
                        .collect();
                }
                // Reading order (oldest first), then the title comes from the
                // newest message — the subject the operator last saw.
                messages.sort_by(|a, b| a.at.cmp(&b.at).then_with(|| a.id.cmp(&b.id)));
                let subject = messages
                    .iter()
                    .rev()
                    .find(|r| !r.subject.is_empty())
                    .map(|r| r.subject.clone())
                    .unwrap_or_default();
                m.mail.open = Some(OpenThread {
                    thread_id,
                    subject,
                    messages,
                });
            }
        }
        Msg::ChannelsLoaded(rows) => {
            m.channels.channels = rows;
            m.channels.loaded = true;
            m.clamp_cursor();
        }
        Msg::ChannelPostsLoaded { channel_id, rows } => {
            // Only the selected channel's posts are ever drawn.
            let current = m.channels.current().map(|c| c.id.clone());
            if current.as_deref() == Some(channel_id.as_str()) || current.is_none() {
                m.channels.posts_for = Some(channel_id);
                m.channels.posts = rows;
                m.channels.selected_post = 0;
            }
        }
        Msg::AgentsLoaded(rows) => {
            m.agents.rows = rows;
            m.agents.loaded = true;
            m.clamp_cursor();
        }
        Msg::SearchLoaded(rows) => {
            let hits = rows.len();
            m.mail.search = Some(rows);
            m.mail.selected = 0;
            m.set_status(StatusKind::Ok, format!("search: {hits} hit(s)"));
            m.clamp_cursor();
        }
        Msg::ReceiptsLoaded(counts) => {
            m.dashboard.receipts = counts;
            m.dashboard.loaded = true;
        }
        Msg::HealthLoaded(lines) => {
            m.dashboard.health = lines;
            m.dashboard.loaded = true;
        }
        Msg::Inflight(n) => m.inflight = n,
        Msg::Failed { op, message } => {
            // Typed failure: show it and keep the last good data on screen.
            m.set_status(StatusKind::Error, format!("{}: {message}", op.label()));
        }
        Msg::ReceiptWritten { corr, verb } => {
            m.set_status(StatusKind::Ok, format!("{verb}: {corr}"));
        }

        // --- intents ---------------------------------------------------
        Msg::Quit => {
            m.should_quit = true;
            effects.push(Effect::Quit);
        }
        Msg::ToggleHelp => {
            m.mode = match m.mode {
                Mode::Help => Mode::Normal,
                _ => Mode::Help,
            };
        }
        Msg::SelectTab(tab) => {
            if tab != m.tab {
                m.goto_tab(tab);
                effects.extend(tab_enter_refresh(&m));
            }
        }
        Msg::CycleTab(delta) => {
            m.goto_tab(m.tab.step(delta));
            effects.extend(tab_enter_refresh(&m));
        }
        Msg::FocusPane(delta) => {
            m.pane = m.pane.step(m.tab, delta);
            m.clamp_cursor();
        }
        Msg::MoveCursor(delta) => match (m.tab, m.pane) {
            // The Mail sidebar is the folder axis, not a numeric list.
            (Tab::Mail, Pane::Sidebar) => {
                let folder = m.mail.folder.step(delta);
                if m.set_folder(folder) {
                    effects.extend(reader_refresh(&m));
                }
            }
            (_, Pane::Reader) => m.scroll_reader(delta),
            (Tab::Channels, Pane::Sidebar) => {
                let before = m.channels.selected;
                m.move_cursor(delta);
                if m.channels.selected != before {
                    // A different room: the post pane and reader start over.
                    m.channels.selected_post = 0;
                    m.channels.scroll = 0;
                    effects.extend(reader_refresh(&m));
                }
            }
            _ => m.move_cursor(delta),
        },
        Msg::GotoCursor(top) => match (m.tab, m.pane) {
            (Tab::Mail, Pane::Sidebar) => {
                let folder = if top {
                    Folder::Inbox
                } else {
                    Folder::Important
                };
                if m.set_folder(folder) {
                    effects.extend(reader_refresh(&m));
                }
            }
            (_, Pane::Reader) => m.scroll_reader(if top { isize::MIN } else { isize::MAX }),
            _ => m.goto_cursor(if top { 0 } else { usize::MAX }),
        },
        Msg::OpenSelection => {
            m.pane = Pane::Reader;
            effects.extend(reader_refresh(&m));
        }
        Msg::CloseReader => {
            m.pane = Pane::List;
            m.set_reader_scroll(0);
        }
        Msg::OpenInsert => {
            if m.tab.filters() {
                m.mode = Mode::Insert;
            }
        }
        Msg::CancelInsert => {
            m.mode = Mode::Normal;
            m.input.clear();
            m.mail.selected = 0;
            if m.mail.search.take().is_some() {
                m.set_status(StatusKind::Info, "search cleared");
            }
            m.clamp_cursor();
        }
        Msg::InputChar(c) => {
            if m.mode == Mode::Insert {
                m.input.push(c);
                m.clamp_cursor();
            }
        }
        Msg::InputBackspace => {
            if m.mode == Mode::Insert {
                m.input.pop();
                m.clamp_cursor();
            }
        }
        Msg::SubmitInput => {
            m.mode = Mode::Normal;
            let query = m.input.trim().to_string();
            if m.tab == Tab::Mail && !query.is_empty() {
                effects.push(Effect::Search { query });
            }
        }
        Msg::Refresh => {
            m.set_status(StatusKind::Info, "refreshing");
            effects.extend(tab_enter_refresh(&m));
        }
        Msg::AckSelected => match selected_mail_id(&m) {
            Some(id) => effects.push(Effect::AckMessage { message_id: id }),
            None => m.set_status(StatusKind::Error, "no message selected"),
        },
        Msg::ReadSelected => match selected_mail_id(&m) {
            Some(id) => effects.push(Effect::ReadMessage { message_id: id }),
            None => m.set_status(StatusKind::Error, "no message selected"),
        },
        Msg::StepRefresh(step) => {
            m.settings.refresh_ms = m.settings.step_refresh(step);
            let ms = m.settings.refresh_ms;
            m.set_status(StatusKind::Info, format!("refresh: {ms}ms"));
        }
        Msg::StepWindow(delta) => {
            m.dashboard.window_hours = m.dashboard.step_window(delta);
            let h = m.dashboard.window_hours;
            m.set_status(StatusKind::Info, format!("chart window: {h}h"));
        }
        Msg::ToggleDense => {
            m.settings.dense = !m.settings.dense;
            let text = if m.settings.dense {
                "rows: dense"
            } else {
                "rows: comfy"
            };
            m.set_status(StatusKind::Info, text);
        }
        Msg::ToggleRelativeTime => {
            m.settings.relative_time = !m.settings.relative_time;
            let text = if m.settings.relative_time {
                "time: relative"
            } else {
                "time: clock"
            };
            m.set_status(StatusKind::Info, text);
        }
        Msg::ToggleClosedChannels => {
            m.settings.show_closed_channels = !m.settings.show_closed_channels;
            effects.push(Effect::RefreshChannels);
        }
    }
    (m, effects)
}

/// Fold a whole log — the replay entry point the determinism test uses
/// (ADR-0031 §9). Effects accumulate in order. Only the tests replay today,
/// so it is test-gated rather than shipped as dead surface.
#[cfg(test)]
pub fn replay(model: &Model, log: &[Msg]) -> (Model, Vec<Effect>) {
    let mut m = model.clone();
    let mut all = Vec::new();
    for msg in log {
        let (next, mut effects) = tick(&m, msg.clone());
        m = next;
        all.append(&mut effects);
    }
    (m, all)
}

/// Startup fetches: the active tab, plus the roster (which carries presence,
/// the one thing the operator expects live from the first frame).
fn initial_refresh(m: &Model) -> Vec<Effect> {
    let mut effects = vec![Effect::RefreshAgents];
    if m.tab == Tab::Dashboard {
        effects.push(Effect::RefreshChannels);
        effects.push(Effect::RefreshReceipts);
        effects.push(Effect::RefreshHealth);
        if m.settings.agent.is_some() {
            effects.push(Effect::RefreshInbox);
        }
    } else {
        effects.extend(tab_enter_refresh(m));
    }
    effects
}

/// What entering (or manually refreshing) a tab must fetch.
pub fn tab_enter_refresh(m: &Model) -> Vec<Effect> {
    let mut effects = Vec::new();
    match m.tab {
        Tab::Mail => {
            if m.settings.agent.is_some() {
                effects.push(Effect::RefreshInbox);
            }
        }
        Tab::Channels => {
            effects.push(Effect::RefreshChannels);
            if let Some(ch) = m.channels.current() {
                effects.push(Effect::RefreshChannelPosts {
                    channel_id: ch.id.clone(),
                });
            }
        }
        Tab::Agents => effects.push(Effect::RefreshAgents),
        Tab::Dashboard => {
            effects.push(Effect::RefreshReceipts);
            effects.push(Effect::RefreshHealth);
            effects.push(Effect::RefreshAgents);
            effects.push(Effect::RefreshChannels);
            if m.settings.agent.is_some() {
                effects.push(Effect::RefreshInbox);
            }
        }
        Tab::Settings => effects.push(Effect::RefreshHealth),
    }
    effects
}

/// Per-tick refresh, bounded: nothing is dispatched while a previous wave is
/// still in flight.
fn tick_refresh(m: &Model) -> Vec<Effect> {
    if m.inflight > 0 {
        return Vec::new();
    }
    let slow = m.tick % SLOW_CADENCE == 0;
    match m.tab {
        Tab::Mail => {
            if m.settings.agent.is_some() {
                vec![Effect::RefreshInbox]
            } else {
                Vec::new()
            }
        }
        Tab::Channels => {
            if !slow {
                return Vec::new();
            }
            let mut effects = vec![Effect::RefreshChannels];
            if let Some(ch) = m.channels.current() {
                effects.push(Effect::RefreshChannelPosts {
                    channel_id: ch.id.clone(),
                });
            }
            effects
        }
        // Presence is the Agents tab's whole point: refresh every tick.
        Tab::Agents => vec![Effect::RefreshAgents],
        Tab::Dashboard => {
            if !slow {
                return Vec::new();
            }
            vec![Effect::RefreshReceipts, Effect::RefreshHealth]
        }
        Tab::Settings => Vec::new(),
    }
}

/// Reader-gated fetch: what the reader pane is about to draw.
fn reader_refresh(m: &Model) -> Vec<Effect> {
    match m.tab {
        Tab::Mail => match selected_thread(m) {
            Some(thread_id) => vec![Effect::RefreshThread { thread_id }],
            None => Vec::new(),
        },
        Tab::Channels => match m.channels.current() {
            Some(ch) => vec![Effect::RefreshChannelPosts {
                channel_id: ch.id.clone(),
            }],
            None => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// The thread the Mail reader should open: the selected row's thread.
pub fn selected_thread(m: &Model) -> Option<String> {
    selected_mail_row(m).map(|r| r.thread_id.clone())
}

/// The selected mail row, from the folder list or the search hits.
pub fn selected_mail_row(m: &Model) -> Option<ListRow> {
    m.mail_rows().get(m.mail.selected).cloned()
}

/// The message id an ack/read targets: the selected row's own id, or — for a
/// thread row — its newest message.
pub fn selected_mail_id(m: &Model) -> Option<String> {
    let row = selected_mail_row(m)?;
    if !row.thread {
        return Some(row.key);
    }
    let thread = row.thread_id;
    m.mail
        .messages
        .iter()
        .filter(|msg| thread_id_of(msg) == thread)
        .max_by_key(|msg| msg.at)
        .map(|msg| msg.id.clone())
}

impl Model {
    /// Scroll the reader pane by `delta` rows (negative = up). The view clamps
    /// the offset to the rendered content; the fold only clamps at zero so a
    /// log replay stays deterministic.
    fn scroll_reader(&mut self, delta: isize) {
        let cur = self.reader_scroll() as i64;
        let next = cur.saturating_add(delta as i64).max(0);
        self.set_reader_scroll(next as usize);
    }

    /// Current reader scroll offset for the active tab.
    pub fn reader_scroll(&self) -> usize {
        match self.tab {
            Tab::Mail => self.mail.scroll,
            Tab::Channels => self.channels.scroll,
            _ => 0,
        }
    }

    /// Write the reader scroll offset for the active tab.
    pub fn set_reader_scroll(&mut self, value: usize) {
        match self.tab {
            Tab::Mail => self.mail.scroll = value,
            Tab::Channels => self.channels.scroll = value,
            _ => {}
        }
    }
}
