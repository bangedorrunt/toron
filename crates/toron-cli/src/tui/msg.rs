// governed-by: ADR-0031
//! Swarm Board messages and effects (Elm-everywhere).

use super::model::{
    BoardPost, FlywheelPanel, Inspector, LeaseRow, ReceiptCounts, RosterRow, SealedInboxCounts,
    ToronHealthSignals,
};

/// Operation identifier for typed failure reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Board,
    Roster,
    Leases,
    Receipts,
    SealedCounts,
    Health,
    Flywheel,
    Post,
}

impl Op {
    pub const fn label(self) -> &'static str {
        match self {
            Op::Board => "board",
            Op::Roster => "roster",
            Op::Leases => "leases",
            Op::Receipts => "receipts",
            Op::SealedCounts => "sealed",
            Op::Health => "health",
            Op::Flywheel => "flywheel",
            Op::Post => "post",
        }
    }
}

/// Pure input messages folded by [`super::update::tick`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Msg {
    Init,
    Tick {
        now_unix: u64,
    },
    FocusChanged(bool),
    BusChanged,
    BoardLoaded(Vec<BoardPost>),
    RosterLoaded(Vec<RosterRow>),
    LeasesLoaded(Vec<LeaseRow>),
    ReceiptsLoaded(ReceiptCounts),
    SealedCountsLoaded(SealedInboxCounts),
    ToronHealthLoaded {
        signals: ToronHealthSignals,
        now_unix: u64,
    },
    FlywheelSnapshotLoaded {
        panel: Option<FlywheelPanel>,
        now_unix: u64,
    },
    BoardPosted(BoardPost),
    Inflight(usize),
    Failed {
        op: Op,
        message: String,
    },

    Quit,
    ToggleHelp,
    SelectInspector(Inspector),
    CycleInspector(isize),
    MovePost(isize),
    GotoPost(bool),
    ScrollMessage(isize),
    ToggleFollow,
    ToggleRetired,
    MoveDiag(isize),
    CopyRemedy,
    OpenFilter,
    CancelFilter,
    FilterChar(char),
    FilterBackspace,
    CommitFilter,
    Refresh,
    StepRefresh(i64),
    ToggleRelativeTime,

    OpenCompose,
    ComposeChar(char),
    ComposeBackspace,
    ComposeEnter,
    ComposeCancel,
}

/// Effects returned by [`super::update::tick`] and executed by [`super::host::TuiHost`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    LoadBoard,
    LoadRoster,
    LoadLeases,
    LoadReceipts,
    LoadSealedCounts,
    ProbeToronHealth,
    ProbeFlywheelSnapshot,
    PostBoard { body: String },
    Quit,
}
