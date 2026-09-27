// governed-by: ADR-0003
// governed-by: ADR-0031
//! TUI model — the pure half of the Elm fold (ADR-0031 §7, "Elm-everywhere").
//!
//! Everything here is data. No clock, no environment, no I/O: wall time enters
//! as a `Msg::Tick { now_unix }` payload and is stored in [`Model::now_unix`],
//! and the view turns it into text. Lists are *derived* from the fetched rows
//! ([`MailState::rows`]), so the same model always renders the same rows.
//!
//! Gmail-shaped reading order: tabs across the top, then sidebar → list →
//! reader. The sidebar is the folder (or channel) axis, the list is the
//! current selection, the reader is the open conversation. Tabs that do not
//! need a reader (dashboard, settings) use the full width.

use toron_core::nostr::presence::PresenceState;

/// Minimum usable terminal width. The floor is a phone-width PTY — an iPhone
/// SSH client reports ~40-50 columns — so the console keeps working there in
/// one-pane mode instead of refusing. Below this the resize card shows (§2).
pub const MIN_W: u16 = 40;
/// Minimum usable terminal height (five rows of chrome, seven of content).
pub const MIN_H: u16 = 12;

/// Width at which the reader pane gets its own column (three-pane Gmail).
pub const TRI_PANE_W: u16 = 108;
/// Width at which the sidebar, list and reader share the frame (two-pane).
pub const DUAL_PANE_W: u16 = 80;

/// Top-level tabs. Order is the display order and the `1..5` key order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tab {
    Mail,
    Channels,
    Agents,
    Dashboard,
    Settings,
}

impl Tab {
    /// Every tab, in display order.
    pub const ALL: [Tab; 5] = [
        Tab::Mail,
        Tab::Channels,
        Tab::Agents,
        Tab::Dashboard,
        Tab::Settings,
    ];

    /// Position in [`Tab::ALL`] (`1..=5` in the tab bar).
    pub fn index(self) -> usize {
        Self::ALL.iter().position(|t| *t == self).unwrap_or(0)
    }

    /// Tab-bar label.
    pub fn title(self) -> &'static str {
        match self {
            Tab::Mail => "Mail",
            Tab::Channels => "Channels",
            Tab::Agents => "Agents",
            Tab::Dashboard => "Dashboard",
            Tab::Settings => "Settings",
        }
    }

    /// Keyboard mnemonic: the digits 1..5 select tabs directly.
    pub fn from_digit(c: char) -> Option<Tab> {
        let idx = c.to_digit(10)? as usize;
        (1..=Tab::ALL.len())
            .contains(&idx)
            .then(|| Tab::ALL[idx - 1])
    }

    /// Parse the `--tab` flag value.
    pub fn parse(s: &str) -> Option<Tab> {
        match s.trim().to_ascii_lowercase().as_str() {
            "mail" | "inbox" => Some(Tab::Mail),
            "channels" | "channel" => Some(Tab::Channels),
            "agents" | "agent" => Some(Tab::Agents),
            "dashboard" | "stats" => Some(Tab::Dashboard),
            "settings" | "config" => Some(Tab::Settings),
            _ => None,
        }
    }

    /// Tab cycling (Tab / Shift-Tab).
    pub fn step(self, delta: isize) -> Tab {
        let n = Self::ALL.len() as isize;
        let i = (self.index() as isize + delta).rem_euclid(n);
        Self::ALL[i as usize]
    }

    /// True when the tab renders sidebar + list + reader.
    pub fn has_reader(self) -> bool {
        matches!(self, Tab::Mail | Tab::Channels | Tab::Agents)
    }

    /// True when the tab renders a filter box (`/` opens insert mode).
    pub fn filters(self) -> bool {
        matches!(self, Tab::Mail | Tab::Channels | Tab::Agents)
    }
}

/// Input mode. `Insert` is the in-place filter/search box; `Help` is the
/// modal keybinding overlay (a focus trap — background keys are ignored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Normal,
    Insert,
    Help,
}

/// Which pane owns the keyboard. `Tab` moves focus forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pane {
    Sidebar,
    List,
    Reader,
}

impl Pane {
    /// Panes visible on a tab, left to right. Truthful per tab: the Agents tab
    /// draws no sidebar, so it must not offer one as a focus target (`h` would
    /// land on a pane nothing renders).
    pub fn order(tab: Tab) -> &'static [Pane] {
        match tab {
            Tab::Mail | Tab::Channels => &[Pane::Sidebar, Pane::List, Pane::Reader],
            Tab::Agents => &[Pane::List, Pane::Reader],
            _ => &[Pane::List],
        }
    }

    /// Move focus within a tab's visible panes, clamped at both ends (the
    /// leftmost pane is `h`, not a wrap into the reader).
    pub fn step(self, tab: Tab, delta: isize) -> Pane {
        let order = Pane::order(tab);
        let i = order.iter().position(|p| *p == self).unwrap_or(0) as isize;
        let next = (i + delta).clamp(0, order.len() as isize - 1);
        order[next as usize]
    }
}

/// Responsive breakpoint (§2 "Breakpoint modes" + "Minimum size gate").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutMode {
    /// Three columns: sidebar, list, reader — from [`TRI_PANE_W`].
    Tri,
    /// Two columns: the sidebar, plus list and reader sharing the frame —
    /// from [`DUAL_PANE_W`].
    Dual,
    /// One pane at a time, switched with `h`/`l`: phone-width terminals
    /// ([`MIN_W`]..[`DUAL_PANE_W`] columns) where three columns cannot fit.
    /// The rows narrow with it (one line per message, no stamp).
    Stacked,
    /// Below [`MIN_W`]×[`MIN_H`]: resize card, no data panes.
    TooSmall,
}

impl LayoutMode {
    /// True in the one-pane (phone-width) mode: chrome and rows compact.
    pub fn single(self) -> bool {
        matches!(self, LayoutMode::Stacked)
    }
}

/// Pick the layout for a frame size. Pure — the view calls it every frame.
pub fn layout_mode(width: u16, height: u16) -> LayoutMode {
    if width < MIN_W || height < MIN_H {
        return LayoutMode::TooSmall;
    }
    if width >= TRI_PANE_W {
        LayoutMode::Tri
    } else if width >= DUAL_PANE_W {
        LayoutMode::Dual
    } else {
        LayoutMode::Stacked
    }
}

/// Mail folder axis (the sidebar of the Mail tab).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Folder {
    Inbox,
    Unread,
    Threads,
    NeedsAck,
    Important,
}

impl Folder {
    /// Every folder, in sidebar order.
    pub const ALL: [Folder; 5] = [
        Folder::Inbox,
        Folder::Unread,
        Folder::Threads,
        Folder::NeedsAck,
        Folder::Important,
    ];

    /// Sidebar label.
    pub fn title(self) -> &'static str {
        match self {
            Folder::Inbox => "Inbox",
            Folder::Unread => "Unread",
            Folder::Threads => "Threads",
            Folder::NeedsAck => "Needs ack",
            Folder::Important => "Important",
        }
    }

    /// Parse the `--folder` flag value.
    pub fn parse(s: &str) -> Option<Folder> {
        match s.trim().to_ascii_lowercase().as_str() {
            "inbox" | "all" => Some(Folder::Inbox),
            "unread" | "new" => Some(Folder::Unread),
            "threads" | "thread" => Some(Folder::Threads),
            "ack" | "needs-ack" | "needsack" => Some(Folder::NeedsAck),
            "important" | "high" => Some(Folder::Important),
            _ => None,
        }
    }

    /// `j`/`k` movement within the sidebar.
    pub fn step(self, delta: isize) -> Folder {
        let i = (self as isize + delta).rem_euclid(Self::ALL.len() as isize);
        Self::ALL[i as usize]
    }
}

/// One mail message as the index returns it. `thread_id` is always the
/// message's own id when it is not part of a thread (the mailbox contract).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MailRow {
    /// Message id (hex).
    pub id: String,
    /// Thread id (`corr`-adjacent, never the sender's corr).
    pub thread_id: String,
    /// Sender pin.
    pub sender: String,
    /// Subject (NIP-14).
    pub subject: String,
    /// Importance tag as stored (`normal`/`high`/`urgent`).
    pub importance: String,
    /// Receiver asked for an acknowledgement.
    pub ack_required: bool,
    /// The acknowledgement already landed (`acked` receipt exists).
    pub acked: bool,
    /// The recipient opened it (`read_ts_us` in the projection).
    pub read: bool,
    /// Created-at, unix seconds (µs precision is not needed in the UI).
    pub at: u64,
    /// Body, when the reader fetched it.
    pub body: Option<String>,
    /// The projection carried this row's read/ack state. A search hit does
    /// not (its result carries no corr and no read stamp), so it must say
    /// "unknown" instead of inventing an unread dot or an owed ack.
    pub state_known: bool,
}

impl MailRow {
    /// High or urgent importance (the `Important` folder + the row badge).
    pub fn important(&self) -> bool {
        matches!(
            self.importance.trim().to_ascii_lowercase().as_str(),
            "high" | "urgent"
        )
    }

    /// Still owes an acknowledgement. Unknown state is never owed.
    pub fn ack_pending(&self) -> bool {
        self.state_known && self.ack_required && !self.acked
    }

    /// Never opened. Unknown state is never claimed as unread.
    pub fn unread(&self) -> bool {
        self.state_known && !self.read
    }
}

/// One thread: the derived grouping of the mail rows (`Folder::Threads`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadRow {
    /// Thread id.
    pub id: String,
    /// Newest message's subject.
    pub subject: String,
    /// Message count.
    pub count: usize,
    /// Newest `at` in the thread.
    pub at: u64,
    /// Distinct senders, newest first.
    pub participants: Vec<String>,
    /// How many messages still owe an ack.
    pub ack_pending: usize,
}

/// One display row of the list pane. Threads and messages share it so the
/// view, the selection clamp, and the reader-open decision all read one shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRow {
    /// Stable key: message id, or thread id when the row is a thread.
    pub key: String,
    /// Thread to open in the reader.
    pub thread_id: String,
    /// Subject.
    pub subject: String,
    /// Sender (message row) or participants (thread row).
    pub from: String,
    /// Metadata line: message count / age / flags.
    pub meta: String,
    /// Newest `at` on the row.
    pub at: u64,
    /// Row is a thread aggregate.
    pub thread: bool,
    /// Owes an acknowledgement.
    pub ack_pending: bool,
    /// High/urgent importance.
    pub important: bool,
    /// Never opened (`●` in the list).
    pub unread: bool,
}

/// The open conversation in the reader pane.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OpenThread {
    /// Thread id.
    pub thread_id: String,
    /// Subject line of the conversation.
    pub subject: String,
    /// Messages oldest-first (reading order).
    pub messages: Vec<MailRow>,
}

/// Mail tab state. `messages` is the fetched index; every list is derived.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MailState {
    /// Sidebar folder.
    pub folder: Folder,
    /// Rows from the last successful inbox fetch, newest first.
    pub messages: Vec<MailRow>,
    /// Cursor in the visible list.
    pub selected: usize,
    /// Open conversation, when the reader has content.
    pub open: Option<OpenThread>,
    /// Server-side search hits (`Enter` in the filter box). `None` = the
    /// folder list is showing; `Some` = search results replace it.
    pub search: Option<Vec<MailRow>>,
    /// The last fetch landed at least once (drives the empty-state text).
    pub loaded: bool,
    /// Reader scroll offset (the view clamps it to the rendered content).
    pub scroll: usize,
}

impl Default for Folder {
    fn default() -> Self {
        Folder::Inbox
    }
}

impl MailState {
    /// The rows the list pane shows, derived from the folder + filter +
    /// search state. Newest first.
    pub fn rows(&self) -> Vec<ListRow> {
        let source: Vec<MailRow> = match &self.search {
            Some(hits) => hits.clone(),
            None => match self.folder {
                Folder::Unread => self.messages.iter().filter(|m| !m.read).cloned().collect(),
                Folder::NeedsAck => self
                    .messages
                    .iter()
                    .filter(|m| m.ack_pending())
                    .cloned()
                    .collect(),
                Folder::Important => self
                    .messages
                    .iter()
                    .filter(|m| m.important())
                    .cloned()
                    .collect(),
                Folder::Inbox => self.messages.clone(),
                Folder::Threads => return thread_rows(&self.messages),
            },
        };
        let mut rows: Vec<ListRow> = source
            .iter()
            .map(|m| ListRow {
                key: m.id.clone(),
                thread_id: thread_id_of(m),
                subject: m.subject.clone(),
                from: m.sender.clone(),
                meta: String::new(),
                at: m.at,
                thread: false,
                ack_pending: m.ack_pending(),
                important: m.important(),
                unread: m.unread(),
            })
            .collect();
        rows.sort_by(|a, b| b.at.cmp(&a.at).then_with(|| a.key.cmp(&b.key)));
        rows
    }

    /// Apply a local (client-side) filter to the derived rows. Pure; the
    /// filter text is the model's input box.
    pub fn filtered_rows(&self, filter: &str) -> Vec<ListRow> {
        let mut rows = self.rows();
        filter_rows(&mut rows, filter);
        rows
    }
}

/// Group messages into thread rows, newest thread first.
pub fn thread_rows(messages: &[MailRow]) -> Vec<ListRow> {
    let mut threads: Vec<ThreadRow> = Vec::new();
    for m in messages {
        let tid = thread_id_of(m);
        match threads.iter_mut().find(|t| t.id == tid) {
            Some(t) => {
                t.count += 1;
                if m.at >= t.at {
                    t.at = m.at;
                    if !m.subject.is_empty() {
                        t.subject = m.subject.clone();
                    }
                }
                if !t.participants.contains(&m.sender) {
                    t.participants.push(m.sender.clone());
                }
                if m.ack_pending() {
                    t.ack_pending += 1;
                }
            }
            None => threads.push(ThreadRow {
                id: tid,
                subject: m.subject.clone(),
                count: 1,
                at: m.at,
                participants: vec![m.sender.clone()],
                ack_pending: usize::from(m.ack_pending()),
            }),
        }
    }
    threads.sort_by(|a, b| b.at.cmp(&a.at).then_with(|| a.id.cmp(&b.id)));
    threads
        .into_iter()
        .map(|t| {
            let others = t.participants.len().saturating_sub(1);
            // A thread is unread when any of its messages is.
            let unread = messages
                .iter()
                .any(|m| thread_id_of(m) == t.id && m.unread());
            ListRow {
                key: t.id.clone(),
                thread_id: t.id,
                subject: t.subject,
                from: t.participants.first().cloned().unwrap_or_default(),
                meta: if others == 0 {
                    format!("{} msg", t.count)
                } else {
                    format!("{} msgs · +{others}", t.count)
                },
                at: t.at,
                thread: true,
                ack_pending: t.ack_pending > 0,
                important: false,
                unread,
            }
        })
        .collect()
}

/// A message's thread: its own id when it is not threaded (never empty, so
/// the reader always has a key to fetch by).
pub fn thread_id_of(m: &MailRow) -> String {
    if m.thread_id.trim().is_empty() {
        m.id.clone()
    } else {
        m.thread_id.clone()
    }
}

/// Case-insensitive substring filter over subject/from/meta. Pure.
pub fn filter_rows(rows: &mut Vec<ListRow>, filter: &str) {
    let needle = filter.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return;
    }
    rows.retain(|r| {
        r.subject.to_ascii_lowercase().contains(&needle)
            || r.from.to_ascii_lowercase().contains(&needle)
            || r.meta.to_ascii_lowercase().contains(&needle)
    });
}

// ---------------------------------------------------------------------------
// Channels
// ---------------------------------------------------------------------------

/// One channel row (kind 39000/39002 lifecycle projection).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChannelRow {
    /// Channel id (the `h`/`d` tag).
    pub id: String,
    /// Display name.
    pub name: String,
    /// Private channels are members-only.
    pub private: bool,
    /// Retired channel (posts refused, history until purge).
    pub closed: bool,
    /// Member count.
    pub members: usize,
    /// The caller is a member.
    pub joined: bool,
    /// Posts observed on the local bus.
    pub posts: usize,
    /// Newest post `at`, when any.
    pub last_at: Option<u64>,
}

/// One channel post (kind 9; kind 7 reactions carry `react_to`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PostRow {
    /// Event id.
    pub event_id: String,
    /// Author npub hex. The view resolves the pin from the loaded roster —
    /// the host never guesses a second name mapping.
    pub author: String,
    /// Body.
    pub content: String,
    /// Created-at, unix seconds.
    pub at: u64,
    /// Event kind (9 post, 7 reaction).
    pub kind: u16,
    /// Reaction target, when kind 7.
    pub react_to: Option<String>,
    /// `@mention` targets (p tags).
    pub mentions: Vec<String>,
}

impl PostRow {
    /// True for a reaction rather than a post.
    pub fn is_reaction(&self) -> bool {
        self.kind == 7
    }
}

/// Channels tab state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChannelsState {
    /// Channels visible to the caller, name-sorted.
    pub channels: Vec<ChannelRow>,
    /// Sidebar cursor.
    pub selected: usize,
    /// Cursor in the post list.
    pub selected_post: usize,
    /// Posts of the selected channel, newest first.
    pub posts: Vec<PostRow>,
    /// The selected channel's id the posts belong to (guards stale reads).
    pub posts_for: Option<String>,
    /// A fetch has landed (drives empty-state text).
    pub loaded: bool,
    /// Reader scroll offset (the view clamps it).
    pub scroll: usize,
}

impl ChannelsState {
    /// The channel the sidebar cursor points at.
    pub fn current(&self) -> Option<&ChannelRow> {
        self.channels.get(self.selected)
    }

    /// Posts for the selected channel, local-filtered, newest first.
    pub fn visible_posts(&self, filter: &str) -> Vec<PostRow> {
        let mut rows = self.posts.clone();
        rows.sort_by(|a, b| b.at.cmp(&a.at).then_with(|| a.event_id.cmp(&b.event_id)));
        let needle = filter.trim().to_ascii_lowercase();
        if !needle.is_empty() {
            rows.retain(|p| {
                p.content.to_ascii_lowercase().contains(&needle)
                    || p.author.to_ascii_lowercase().contains(&needle)
            });
        }
        rows
    }
}

// ---------------------------------------------------------------------------
// Agents
// ---------------------------------------------------------------------------

/// One agent row: the registered roster joined with the presence projection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AgentRow {
    /// Canonical pin.
    pub name: String,
    /// npub hex.
    pub npub: String,
    /// Harness/program from the 30013 profile.
    pub program: Option<String>,
    /// Model from the 30013 profile.
    pub model: Option<String>,
    /// Task from the 30013 profile.
    pub task: Option<String>,
    /// Live pane (advisory, from whois when a backend is reachable).
    pub pane_id: Option<String>,
    /// Heartbeat inside the TTL: online.
    pub online: bool,
    /// Last heartbeat state (Unknown when never seen).
    pub state: PresenceState,
    /// Seconds since the last heartbeat, when one exists.
    pub age_secs: Option<u64>,
    /// Heartbeat status line.
    pub status: Option<String>,
    /// The local tour of duty: this row is the TUI's own identity.
    pub is_self: bool,
}

impl AgentRow {
    /// `online` marker with a shape, never color alone (§4): `●` vs `○`.
    pub fn marker(&self) -> &'static str {
        if self.online { "●" } else { "○" }
    }

    /// Human presence word for the detail pane and the dashboard buckets.
    pub fn state_label(&self) -> &'static str {
        presence_label(self.online, self.state)
    }
}

/// Canonical presence word: offline wins, then the five herdr-vocabulary
/// states (never a sixth spelling — ADR-0031 A4).
pub fn presence_label(online: bool, state: PresenceState) -> &'static str {
    if !online {
        return "offline";
    }
    match state {
        PresenceState::Idle => "idle",
        PresenceState::Working => "working",
        PresenceState::Blocked => "blocked",
        PresenceState::Done => "done",
        PresenceState::Unknown => "unknown",
    }
}

/// Agents tab state.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentsState {
    /// Roster joined with presence, name-sorted.
    pub rows: Vec<AgentRow>,
    /// Cursor.
    pub selected: usize,
    /// Detail-card scroll offset (the view clamps it to the rendered card;
    /// a long task or status line has to be readable somewhere).
    pub scroll: usize,
    /// A fetch has landed.
    pub loaded: bool,
}

impl AgentsState {
    /// Rows matching the local filter (pin / npub / program / model / task).
    pub fn visible_rows(&self, filter: &str) -> Vec<AgentRow> {
        let mut rows = self.rows.clone();
        let needle = filter.trim().to_ascii_lowercase();
        if !needle.is_empty() {
            let matches = |v: &Option<String>| {
                v.as_deref()
                    .is_some_and(|s| s.to_ascii_lowercase().contains(&needle))
            };
            rows.retain(|a| {
                a.name.to_ascii_lowercase().contains(&needle)
                    || a.npub.to_ascii_lowercase().contains(&needle)
                    || matches(&a.program)
                    || matches(&a.model)
                    || matches(&a.task)
            });
        }
        rows
    }

    /// Online count (dashboard).
    pub fn online(&self) -> usize {
        self.rows.iter().filter(|a| a.online).count()
    }
}

// ---------------------------------------------------------------------------
// Dashboard
// ---------------------------------------------------------------------------

/// Receipt transition counts (durable store).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReceiptCounts {
    /// Owed.
    pub owed: usize,
    /// Acked.
    pub acked: usize,
    /// Resulted.
    pub resulted: usize,
    /// Bookkeeping failed after a durable enqueue (ADR-0010 D8) — surfaced on
    /// purpose: a degraded receipt is a fact the operator has to see.
    pub degraded: usize,
}

/// One dashboard health line (`label: value`, plus a pass/warn shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthRow {
    /// Line label.
    pub label: String,
    /// Line value.
    pub value: String,
    /// False renders the warn shape (`!`) instead of the ok shape (`✓`).
    pub ok: bool,
}

/// Dashboard tab state. Every number other than receipts/health is derived
/// from the other tabs' loaded data, so the dashboard never disagrees with
/// what the operator can see.
#[derive(Debug, Clone, PartialEq)]
pub struct DashboardState {
    /// Receipt counts from the durable store.
    pub receipts: ReceiptCounts,
    /// Receipts (or health) have been fetched at least once.
    pub loaded: bool,
    /// Health lines from the host (source, data dir, daemon, keys).
    pub health: Vec<HealthRow>,
    /// Window of the arrival chart, in hours (live-tunable).
    pub window_hours: u64,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            receipts: ReceiptCounts::default(),
            loaded: false,
            health: Vec::new(),
            window_hours: 24,
        }
    }
}

impl DashboardState {
    /// Cycle the chart window (6 → 12 → 24 → 48 → 6).
    pub fn step_window(&self, delta: isize) -> u64 {
        const WINDOWS: [u64; 4] = [6, 12, 24, 48];
        let i = WINDOWS
            .iter()
            .position(|w| *w == self.window_hours)
            .unwrap_or(2) as isize;
        WINDOWS[(i + delta).rem_euclid(WINDOWS.len() as isize) as usize]
    }
}

// ---------------------------------------------------------------------------
// Settings
// ---------------------------------------------------------------------------

/// Where the data came from (the mailbox selector's verdict).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceInfo {
    /// `daemon` / `embedded` / `pinned`.
    pub kind: String,
    /// Endpoint or root path, in one line.
    pub detail: String,
    /// The durable data dir the store lives in.
    pub data_dir: String,
    /// True when a daemon on this machine owns the same store.
    pub daemon_loopback: bool,
}

/// Settings tab state. Identity and source are read-only facts (they come
/// from the flags/env at start); the tunables are live model edits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsState {
    /// Project slug.
    pub project: String,
    /// Acting pin, when the operator has one.
    pub agent: Option<String>,
    /// Resolved npub of the acting pin.
    pub npub: Option<String>,
    /// Data source.
    pub source: SourceInfo,
    /// Refresh cadence in ms (live-tunable; the runtime rebuilds its timer).
    pub refresh_ms: u64,
    /// Compact rows (one line per row) vs the two-line default.
    pub dense: bool,
    /// Timestamps rendered as age (`2m`) vs clock time (`14:03`).
    pub relative_time: bool,
    /// Show closed (retired) channels in the Channels tab (`--all`).
    pub show_closed_channels: bool,
}

impl Default for SettingsState {
    fn default() -> Self {
        Self {
            project: String::new(),
            agent: None,
            npub: None,
            source: SourceInfo::default(),
            refresh_ms: 1000,
            dense: false,
            relative_time: true,
            show_closed_channels: false,
        }
    }
}

impl SettingsState {
    /// Refresh cadence bounds: 250ms .. 10s (an unbounded cadence either
    /// hammers the store or stops being a live view).
    pub const MIN_REFRESH_MS: u64 = 250;
    /// Upper bound.
    pub const MAX_REFRESH_MS: u64 = 10_000;

    /// Step the refresh cadence by ±`step` ms, clamped.
    pub fn step_refresh(&self, step: i64) -> u64 {
        let next = self.refresh_ms as i64 + step;
        next.clamp(Self::MIN_REFRESH_MS as i64, Self::MAX_REFRESH_MS as i64) as u64
    }
}

// ---------------------------------------------------------------------------
// Status line
// ---------------------------------------------------------------------------

/// Status-line severity (vim one-liner feedback).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    /// Plain information.
    Info,
    /// The action succeeded.
    Ok,
    /// The action failed (typed failure, never a panic).
    Error,
}

/// The one-line status message under the footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Status {
    /// Message text.
    pub text: String,
    /// Severity (rendered with a shape prefix, never color alone).
    pub kind: StatusKind,
}

impl Default for Status {
    fn default() -> Self {
        Self {
            text: String::new(),
            kind: StatusKind::Info,
        }
    }
}

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// The whole UI state. One model, one fold (`update::tick`).
#[derive(Debug, Clone, PartialEq)]
pub struct Model {
    /// Active tab.
    pub tab: Tab,
    /// Input mode.
    pub mode: Mode,
    /// Focused pane.
    pub pane: Pane,
    /// Mail tab.
    pub mail: MailState,
    /// Channels tab.
    pub channels: ChannelsState,
    /// Agents tab.
    pub agents: AgentsState,
    /// Dashboard tab.
    pub dashboard: DashboardState,
    /// Settings tab.
    pub settings: SettingsState,
    /// Live filter box (shared vocabulary: `/` opens it on any list tab).
    pub input: String,
    /// Wall-clock payload from the last tick, unix seconds.
    pub now_unix: u64,
    /// Tick counter (spinner + the slower-cadence gate).
    pub tick: u64,
    /// Effects in flight (bounded: a slow store never queues a second wave).
    pub inflight: usize,
    /// Status line.
    pub status: Status,
    /// Set by the quit path; the runtime draws one last frame and exits.
    pub should_quit: bool,
}

impl Model {
    /// Seed a model from resolved identity + source facts. Starts on
    /// `folder`/`tab` as asked by the flags, with an empty understood state.
    pub fn seed(tab: Tab, folder: Folder, settings: SettingsState) -> Self {
        Self {
            tab,
            mode: Mode::Normal,
            pane: Pane::List,
            mail: MailState {
                folder,
                ..Default::default()
            },
            channels: ChannelsState::default(),
            agents: AgentsState::default(),
            dashboard: DashboardState::default(),
            settings,
            input: String::new(),
            now_unix: 0,
            tick: 0,
            inflight: 0,
            status: Status::default(),
            should_quit: false,
        }
    }

    /// Rows the list pane currently shows (mail tab). A server search IS the
    /// filter: re-running the local predicate over its hits would hide every
    /// hit that matched in the body (which the local pass cannot see) while
    /// the title still counted it.
    pub fn mail_rows(&self) -> Vec<ListRow> {
        if self.mail.search.is_some() {
            return self.mail.rows();
        }
        self.mail.filtered_rows(&self.input)
    }

    /// Cursor length for the focused pane of the active tab. The Mail sidebar
    /// holds the folder enum (four fixed rows) and is moved through
    /// `SelectFolder` semantics, not a numeric cursor.
    pub fn cursor_len(&self) -> usize {
        match self.tab {
            Tab::Mail => self.mail_rows().len(),
            Tab::Channels => match self.pane {
                Pane::Sidebar => self.channels.channels.len(),
                _ => self.channels.visible_posts(&self.input).len(),
            },
            Tab::Agents => self.agents.visible_rows(&self.input).len(),
            _ => 0,
        }
    }

    /// Cursor position for the focused pane.
    pub fn cursor(&self) -> usize {
        match self.tab {
            Tab::Mail => self.mail.selected,
            Tab::Channels => match self.pane {
                Pane::Sidebar => self.channels.selected,
                _ => self.channels.selected_post,
            },
            Tab::Agents => self.agents.selected,
            _ => 0,
        }
    }

    /// Clamp the focused cursor into `[0, len)`; empty lists sit at 0.
    pub fn clamp_cursor(&mut self) {
        let len = self.cursor_len();
        let cur = self.cursor();
        let next = if len == 0 { 0 } else { cur.min(len - 1) };
        self.set_cursor(next);
    }

    /// Move the focused cursor by `delta`, clamped.
    pub fn move_cursor(&mut self, delta: isize) {
        let len = self.cursor_len();
        if len == 0 {
            self.set_cursor(0);
            return;
        }
        let next = (self.cursor() as isize + delta).clamp(0, len as isize - 1);
        self.set_cursor(next as usize);
    }

    /// Jump the focused cursor to `target` (clamped to the last row).
    pub fn goto_cursor(&mut self, target: usize) {
        let len = self.cursor_len();
        let next = if len == 0 { 0 } else { target.min(len - 1) };
        self.set_cursor(next);
    }

    /// Write the focused cursor back into its owning state.
    pub fn set_cursor(&mut self, value: usize) {
        match self.tab {
            Tab::Mail => self.mail.selected = value,
            Tab::Channels => match self.pane {
                Pane::Sidebar => self.channels.selected = value,
                _ => self.channels.selected_post = value,
            },
            Tab::Agents => self.agents.selected = value,
            _ => {}
        }
    }

    /// Change the mail folder with the same reset semantics as clicking it:
    /// the list cursor resets, the reader closes, and any search drops.
    pub fn set_folder(&mut self, folder: Folder) -> bool {
        if folder == self.mail.folder {
            return false;
        }
        self.mail.folder = folder;
        self.mail.selected = 0;
        self.mail.open = None;
        self.mail.search = None;
        self.mail.scroll = 0;
        self.clamp_cursor();
        true
    }

    /// Switch tabs, resetting focus to a pane that exists on the new tab.
    pub fn goto_tab(&mut self, tab: Tab) {
        self.tab = tab;
        self.mode = Mode::Normal;
        self.pane = Pane::List;
        self.input.clear();
        self.clamp_cursor();
    }

    /// Set the status line (one-liner feedback). It stays until the next
    /// action replaces it — no timer, so nothing here reads a clock.
    pub fn set_status(&mut self, kind: StatusKind, text: impl Into<String>) {
        self.status = Status {
            text: text.into(),
            kind,
        };
    }

    /// Pin for an npub, from the loaded roster. Pure join: the host never
    /// keeps a second npub→name map.
    pub fn agent_name_for(&self, npub: &str) -> Option<&str> {
        self.agents
            .rows
            .iter()
            .find(|a| a.npub == npub)
            .map(|a| a.name.as_str())
    }

    /// Sidebar badge counts, one per folder, derived from the index.
    pub fn folder_counts(&self) -> Vec<(Folder, usize)> {
        Folder::ALL
            .iter()
            .map(|f| {
                let n = match f {
                    Folder::Inbox => self.mail.messages.len(),
                    Folder::Unread => self.mail.messages.iter().filter(|m| !m.read).count(),
                    Folder::Threads => thread_rows(&self.mail.messages).len(),
                    Folder::NeedsAck => self
                        .mail
                        .messages
                        .iter()
                        .filter(|m| m.ack_pending())
                        .count(),
                    Folder::Important => {
                        self.mail.messages.iter().filter(|m| m.important()).count()
                    }
                };
                (*f, n)
            })
            .collect()
    }

    /// Mail arrivals per hour over the dashboard window, oldest first.
    pub fn arrivals(&self) -> Vec<u64> {
        let ts: Vec<u64> = self.mail.messages.iter().map(|m| m.at).collect();
        crate::tui::charts::hour_buckets(&ts, self.now_unix, self.dashboard.window_hours)
    }

    /// Posts per channel, largest first (the dashboard's activity bars).
    pub fn channel_bars(&self) -> Vec<(String, u64)> {
        let mut bars: Vec<(String, u64)> = self
            .channels
            .channels
            .iter()
            .map(|c| (channel_label(c), c.posts as u64))
            .collect();
        bars.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        bars.truncate(8);
        bars
    }

    /// Roster split by presence (offline counted once, by `online`).
    pub fn agent_state_counts(&self) -> Vec<(String, u64)> {
        let mut counts: Vec<(String, u64)> = Vec::new();
        for row in &self.agents.rows {
            let label = row.state_label().to_string();
            match counts.iter_mut().find(|(l, _)| *l == label) {
                Some((_, n)) => *n += 1,
                None => counts.push((label, 1)),
            }
        }
        counts.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        counts
    }

    /// Threads in the mail index (dashboard stat).
    pub fn thread_count(&self) -> usize {
        thread_rows(&self.mail.messages).len()
    }
}

/// Short channel label for charts and titles (`#general`).
pub fn channel_label(c: &ChannelRow) -> String {
    let name = if c.name.trim().is_empty() {
        c.id.as_str()
    } else {
        c.name.as_str()
    };
    format!("#{name}")
}
