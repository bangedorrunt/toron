// governed-by: ADR-0031
//! Messages in, effects out (ADR-0031 §7).
//!
//! Every state change enters `update::tick` as a [`Msg`]: user intents the
//! keymap produced, and data payloads the runtime fetched. The fold returns
//! [`Effect`] values that the runtime executes. Payloads are already-fetched
//! rows with wall time as a number, so the fold never awaits, never reads the
//! clock, and never touches the network.

use super::compose::{CaretMove, ComposedSend};
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
    /// Sending a composed message.
    Send,
}

impl Op {
    /// Human label for the status line.
    pub const fn label(self) -> &'static str {
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
            Op::Send => "send",
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
    /// Effects in flight changed. The runtime derives the count from its live
    /// task handles rather than accumulating results (a task that panics
    /// never answers, and an accumulator would sit above zero forever); the
    /// fold uses it to avoid queueing a second wave.
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
    /// A composed message landed. Carries the recipient list back so the fold
    /// can say who was actually addressed (the doorbell's prompt hop needs a
    /// live pane, and the operator has to know which they got).
    MailSent {
        /// Recipients as typed.
        to: String,
        /// The message id (hex) the send produced.
        message_id: String,
        /// Thread the send joined, when it was a reply.
        thread: Option<String>,
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

    // --- the compose form ------------------------------------------------
    /// `c` — open the send form. The fold decides whether it is a REPLY or a
    /// new message from what is on screen, so the key means one thing.
    OpenCompose,
    /// `Enter` in the form: advance, insert a newline, toggle, or send. The
    /// fold reads the focused stop, so the meaning lives in one place.
    ComposeEnter,
    /// A printable key in the form.
    ComposeInsert(char),
    /// `Backspace` in the form.
    ComposeBackspace,
    /// `Delete` in the form.
    ComposeDelete,
    /// A caret move in the focused field.
    ComposeMove(CaretMove),
    /// `Tab` / `Shift-Tab` — the next / previous form stop.
    ComposeFocus(isize),
    /// `Esc` — leave the form (the draft is dropped; the status says so).
    ComposeCancel,
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
    /// Send one composed message. This is the mailbox's own send path, so the
    /// metadata-only wake ring (one kind-9 per recipient) rides it for free.
    SendMail(ComposedSend),
    /// Leave the alternate screen and exit.
    Quit,
}
