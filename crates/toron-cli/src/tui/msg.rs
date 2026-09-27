// governed-by: ADR-0031
//! Messages in, effects out (ADR-0031 §7).
//!
//! Every state change enters `update::tick` as a [`Msg`]: user intents the
//! keymap produced, and data payloads the runtime fetched. The fold returns
//! [`Effect`] values that the runtime executes. Payloads are already-fetched
//! rows with wall time as a number, so the fold never awaits, never reads the
//! clock, and never touches the network.

use super::model::{AgentRow, ChannelRow, HealthRow, MailRow, PostRow, ReceiptCounts, Tab};

/// Which fetch an error came from (one vocabulary for status-line text and
/// the typed-failure arm).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Inbox fetch.
    Inbox,
    /// Thread fetch for the reader.
    Thread,
    /// Channel lifecycle list.
    Channels,
    /// Channel posts.
    ChannelPosts,
    /// Agent roster.
    Agents,
    /// Full-text search.
    Search,
    /// Receipt counts.
    Receipts,
    /// Health probes.
    Health,
    /// A write (ack/read receipt).
    Receipt,
}

impl Op {
    /// Human label for the status line.
    pub fn label(self) -> &'static str {
        match self {
            Op::Inbox => "inbox",
            Op::Thread => "thread",
            Op::Channels => "channels",
            Op::ChannelPosts => "posts",
            Op::Agents => "agents",
            Op::Search => "search",
            Op::Receipts => "receipts",
            Op::Health => "health",
            Op::Receipt => "ack",
        }
    }
}

/// One message into the fold.
#[derive(Debug, Clone, PartialEq)]
pub enum Msg {
    // --- data (fetched by the runtime, never by the fold) ----------------
    /// Start: the runtime kicks the first fetches.
    Init,
    /// A refresh interval elapsed. Wall time rides as a payload.
    Tick {
        /// Unix seconds at the tick.
        now_unix: u64,
    },
    /// Inbox rows landed.
    InboxLoaded(Vec<MailRow>),
    /// Thread rows for the reader landed.
    ThreadLoaded {
        /// The thread these rows belong to.
        thread_id: String,
        /// Messages, oldest first.
        rows: Vec<MailRow>,
    },
    /// Channel lifecycle rows landed.
    ChannelsLoaded(Vec<ChannelRow>),
    /// Posts of one channel landed.
    ChannelPostsLoaded {
        /// Channel the posts belong to.
        channel_id: String,
        /// Posts, newest first.
        rows: Vec<PostRow>,
    },
    /// Agent roster (joined with presence) landed.
    AgentsLoaded(Vec<AgentRow>),
    /// Search hits landed.
    SearchLoaded(Vec<MailRow>),
    /// Receipt counts landed.
    ReceiptsLoaded(ReceiptCounts),
    /// Health lines landed.
    HealthLoaded(Vec<HealthRow>),
    /// Effects in flight changed (the runtime owns the count; the model
    /// displays it and uses it to avoid queueing a second wave).
    Inflight(usize),
    /// A fetch or write failed. Typed: the model shows it and stays usable.
    Failed {
        /// Which operation failed.
        op: Op,
        /// Operator-facing message.
        message: String,
    },
    /// A write landed (ack/read receipt).
    ReceiptWritten {
        /// Correlation/message id the receipt was written for.
        corr: String,
        /// Verb that ran (`ack` / `read`).
        verb: &'static str,
    },

    // --- intents (keys, decoded by `keymap`) -----------------------------
    /// Leave the alternate screen and exit.
    Quit,
    /// Open/close the keybinding overlay.
    ToggleHelp,
    /// Select a tab by name.
    SelectTab(Tab),
    /// Tab / Shift-Tab.
    CycleTab(isize),
    /// h / l — move focus one pane.
    FocusPane(isize),
    /// j / k — move the list cursor (or scroll the reader). On the Mail
    /// sidebar this walks the folder axis, which is how folders are chosen.
    MoveCursor(isize),
    /// g / G — jump to the top / bottom of the list (or reader).
    GotoCursor(bool),
    /// Enter — open the selected row in the reader.
    OpenSelection,
    /// Esc in normal mode (or h at the list edge) — back to the list.
    CloseReader,
    /// `/` — focus the filter box.
    OpenInsert,
    /// Esc while typing — leave the box, clear the filter and any search.
    CancelInsert,
    /// A printable key while typing.
    InputChar(char),
    /// Backspace while typing.
    InputBackspace,
    /// Enter while typing — commit (Mail runs a server-side search).
    SubmitInput,
    /// `r` — refresh the active tab now.
    Refresh,
    /// `a` — acknowledge the selected message.
    AckSelected,
    /// `m` — mark the selected message read.
    ReadSelected,
    /// `[` / `]` — refresh cadence down / up.
    StepRefresh(i64),
    /// `-` / `+` — chart window down / up.
    StepWindow(isize),
    /// `d` — toggle dense rows.
    ToggleDense,
    /// `t` — toggle relative vs clock timestamps.
    ToggleRelativeTime,
    /// `c` — show/hide closed channels.
    ToggleClosedChannels,
}

impl Msg {
    /// True for messages that answer an effect. The runtime's in-flight
    /// accounting counts exactly these, so a slow store never queues a second
    /// wave of reads (the fold's `tick_refresh` gate reads the count).
    pub fn is_result(&self) -> bool {
        matches!(
            self,
            Msg::InboxLoaded(_)
                | Msg::ThreadLoaded { .. }
                | Msg::ChannelsLoaded(_)
                | Msg::ChannelPostsLoaded { .. }
                | Msg::AgentsLoaded(_)
                | Msg::SearchLoaded(_)
                | Msg::ReceiptsLoaded(_)
                | Msg::HealthLoaded(_)
                | Msg::ReceiptWritten { .. }
                | Msg::Failed { .. }
        )
    }
}

/// One effect the runtime executes. Data only — no closures, so a replayed
/// message log yields the same effect list byte for byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Fetch the acting agent's inbox.
    RefreshInbox,
    /// Fetch one thread with bodies for the reader.
    RefreshThread {
        /// Thread id.
        thread_id: String,
    },
    /// Fetch the channel lifecycle projection.
    RefreshChannels,
    /// Fetch one channel's posts.
    RefreshChannelPosts {
        /// Channel id.
        channel_id: String,
    },
    /// Fetch the agent roster + presence projection.
    RefreshAgents,
    /// Fetch a full-text search page.
    Search {
        /// Query string (already trimmed; empty is never sent).
        query: String,
    },
    /// Fetch receipt counts.
    RefreshReceipts,
    /// Fetch the health/status lines.
    RefreshHealth,
    /// Acknowledge a message (signed receipt).
    AckMessage {
        /// Message id.
        message_id: String,
    },
    /// Mark a message read (signed receipt).
    ReadMessage {
        /// Message id.
        message_id: String,
    },
    /// Leave the alternate screen and exit.
    Quit,
}
