//! OSC 7501, the program status protocol.
//!
//! A program writes one report on its pty. The terminal stores a record.
//! hc (herdr-core) and Rex both speak this, so a pin whose harness exposes
//! no agent row can still be working, blocked, or failed. The bytes are the spec at
//! <https://www.superlogical.com/rex/docs/build/program-status>.
//!
//! Herdr's / hc's finished-and-seen `idle` is not OSC `idle` (at rest,
//! waiting for the next instruction). This module does not import OSC
//! `idle` as finished-and-seen idle, and it does not import OSC `error`
//! as `done`. Fold boards with [`fold`] and a [`FoldPolicy`].

use std::collections::BTreeMap;

use crate::plane_port::AgentStatus;

const MAX_SEQUENCE: usize = 4096;
const MAX_KEY: usize = 16;
const MAX_MSG_ENCODED: usize = 2732;
const MAX_MSG_DECODED: usize = 2048;
const MAX_TITLE_ENCODED: usize = 256;
const MAX_TITLE_DECODED: usize = 192;
const MAX_APP: usize = 32;
const MAX_ID: usize = 128;
const MAX_SEGMENT: usize = 32;
const MAX_DEPTH: usize = 8;

/// What the program says it is doing. `Clear` removes records. It is not
/// a state the terminal stores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgramState {
    Idle,
    Working,
    Done,
    Blocked,
    Error,
    Clear,
}

/// Why a `blocked` record is waiting. Absent for every other state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockKind {
    Permission,
    Question,
    Auth,
}

/// One accepted report. A key the report omitted is absent. Unknown keys
/// are dropped. A repeated key keeps the last value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub state: ProgramState,
    pub id: Option<String>,
    pub kind: Option<BlockKind>,
    pub progress: Option<u8>,
    pub app: Option<String>,
    pub title: Option<String>,
    pub msg: Option<String>,
}

/// What a fold may believe. OSC `idle` is `AtPrompt`, not herdr's
/// finished-and-seen idle. OSC `error` is `Failed`, never `done`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoldHint {
    AtPrompt,
    Running,
    UnseenDone,
    NeedsUser,
    Failed,
    Cleared,
}

impl Report {
    pub fn fold_hint(&self) -> FoldHint {
        match self.state {
            ProgramState::Idle => FoldHint::AtPrompt,
            ProgramState::Working => FoldHint::Running,
            ProgramState::Done => FoldHint::UnseenDone,
            ProgramState::Blocked => FoldHint::NeedsUser,
            ProgramState::Error => FoldHint::Failed,
            ProgramState::Clear => FoldHint::Cleared,
        }
    }

    /// The hc status this report may update. `None` means "do not
    /// touch the hc status": OSC idle is not finished-and-seen, and
    /// OSC error must not count as done.
    pub fn osc_status(&self) -> Option<AgentStatus> {
        match self.state {
            ProgramState::Working => Some(AgentStatus::Working),
            ProgramState::Blocked => Some(AgentStatus::Blocked),
            ProgramState::Done => Some(AgentStatus::Done),
            ProgramState::Idle | ProgramState::Error | ProgramState::Clear => None,
        }
    }
}

/// Records on one terminal. The empty id is the root record.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Board {
    records: BTreeMap<String, Report>,
}

impl Board {
    pub fn get(&self, id: &str) -> Option<&Report> {
        self.records.get(id)
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Apply one report. A discarded or ignored report leaves the board
    /// unchanged. `clear` drops the addressed record and every child.
    pub fn apply(&mut self, raw: &str) {
        let Some(report) = parse_report(raw) else {
            return;
        };
        let id = report.id.clone().unwrap_or_default();
        if report.state == ProgramState::Clear {
            if id.is_empty() {
                self.records.clear();
            } else {
                let prefix = format!("{id}/");
                self.records
                    .retain(|k, _| k != &id && !k.starts_with(&prefix));
            }
            return;
        }
        self.records.insert(id, report);
    }

    /// Build a board from an hc `Terminal.status` root plus `Terminal.records`.
    /// Pure: no I/O. Unknown/absent states are skipped. `error` is kept on
    /// the board so [`fold`] can map it to [`AgentStatus::Unknown`].
    pub fn insert_hc(&mut self, id: &str, view: &HcStatusView<'_>) {
        if let Some(report) = report_from_hc(view, if id.is_empty() { None } else { Some(id) }) {
            self.records.insert(id.to_string(), report);
        }
    }

    /// Every stored report, root first then ids in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Report)> {
        self.records.iter().map(|(k, v)| (k.as_str(), v))
    }
}

/// Borrowed view of one hc Status object (root or record). Callers pass
/// wire fields without allocating a Report until the board accepts them.
#[derive(Clone, Copy, Debug)]
pub struct HcStatusView<'a> {
    pub state: &'a str,
    pub kind: Option<&'a str>,
    pub progress: Option<u8>,
    pub app: Option<&'a str>,
    pub title: Option<&'a str>,
    pub msg: Option<&'a str>,
}

fn report_from_hc(view: &HcStatusView<'_>, id: Option<&str>) -> Option<Report> {
    let state = parse_state(view.state)?;
    if state == ProgramState::Clear {
        return None;
    }
    let kind = if state == ProgramState::Blocked {
        view.kind.and_then(parse_kind)
    } else {
        None
    };
    let progress = if matches!(state, ProgramState::Working | ProgramState::Blocked) {
        view.progress.filter(|n| *n <= 100)
    } else {
        None
    };
    Some(Report {
        state,
        id: id.filter(|s| !s.is_empty()).map(str::to_string),
        kind,
        progress,
        app: view.app.filter(|s| app_ok(s)).map(str::to_string),
        title: view.title.map(str::to_string),
        msg: view.msg.map(str::to_string),
    })
}

/// How [`fold`] picks one [`AgentStatus`] from a board of OSC records.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum FoldPolicy {
    /// Blocked beats working beats done beats idle. OSC `error` → Unknown.
    /// Default for toron/flywheel wake urgency.
    #[default]
    Attention,
    /// The root record speaks for the whole terminal. Children never override
    /// a present root. Matches flywheel `rex_map` root-wins. With no root,
    /// falls back to Attention over the remaining records.
    RootWins,
}

/// Fold one terminal's OSC board into a single [`AgentStatus`]. Pure: no I/O,
/// no clock, no heartbeat. OSC `error` never becomes `done` or `idle`
/// (module fence); it maps to [`AgentStatus::Unknown`].
pub fn fold(board: &Board, policy: FoldPolicy) -> AgentStatus {
    match policy {
        FoldPolicy::RootWins => {
            if let Some(root) = board.get("") {
                return status_of(root.state);
            }
            fold_attention(board)
        }
        FoldPolicy::Attention => fold_attention(board),
    }
}

fn fold_attention(board: &Board) -> AgentStatus {
    if board.is_empty() {
        return AgentStatus::Unknown;
    }
    let mut saw_working = false;
    let mut saw_done = false;
    let mut saw_idle = false;
    let mut saw_error = false;
    for (_id, report) in board.iter() {
        match report.state {
            ProgramState::Blocked => return AgentStatus::Blocked,
            ProgramState::Working => saw_working = true,
            ProgramState::Done => saw_done = true,
            ProgramState::Idle => saw_idle = true,
            ProgramState::Error => saw_error = true,
            ProgramState::Clear => {}
        }
    }
    if saw_working {
        return AgentStatus::Working;
    }
    if saw_error {
        return AgentStatus::Unknown;
    }
    if saw_done {
        return AgentStatus::Done;
    }
    if saw_idle {
        return AgentStatus::Idle;
    }
    AgentStatus::Unknown
}

fn status_of(state: ProgramState) -> AgentStatus {
    match state {
        ProgramState::Idle => AgentStatus::Idle,
        ProgramState::Working => AgentStatus::Working,
        ProgramState::Blocked => AgentStatus::Blocked,
        ProgramState::Done => AgentStatus::Done,
        ProgramState::Error | ProgramState::Clear => AgentStatus::Unknown,
    }
}

/// Parse one OSC 7501 report. `None` means ignored or discarded: the
/// caller must not change a stored record.
pub fn parse_report(raw: &str) -> Option<Report> {
    let body = osc_body(raw)?;
    if body == "?" || body.starts_with("?:") {
        return None;
    }
    if raw.len() > MAX_SEQUENCE {
        return None;
    }
    let mut state = None;
    let mut id = None;
    let mut kind = None;
    let mut progress = None;
    let mut app = None;
    let mut title = None;
    let mut msg = None;
    let mut saw_limit = false;
    for piece in body.split(':') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let Some((key, value)) = piece.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || key.len() > MAX_KEY || !key.bytes().all(|b| b.is_ascii_lowercase()) {
            if key.len() > MAX_KEY {
                saw_limit = true;
            }
            continue;
        }
        if !value_ok(value) {
            continue;
        }
        match key {
            "state" => state = parse_state(value),
            "id" => id = Some(value.to_string()),
            "kind" => kind = parse_kind(value),
            "progress" => progress = value.parse::<u8>().ok().filter(|n| *n <= 100),
            "app" => {
                if value.len() > MAX_APP {
                    saw_limit = true;
                } else if app_ok(value) {
                    app = Some(value.to_string());
                }
            }
            "title" => match decode_text(value, MAX_TITLE_ENCODED, MAX_TITLE_DECODED) {
                Decode::Ok(text) => title = Some(text),
                Decode::Discard => return None,
            },
            "msg" => match decode_text(value, MAX_MSG_ENCODED, MAX_MSG_DECODED) {
                Decode::Ok(text) => msg = Some(text),
                Decode::Discard => return None,
            },
            _ => {}
        }
    }
    if saw_limit {
        return None;
    }
    let state = state?;
    if let Some(ref ident) = id
        && !id_ok(ident)
    {
        return None;
    }
    let kind = if state == ProgramState::Blocked {
        kind
    } else {
        None
    };
    let progress = if matches!(state, ProgramState::Working | ProgramState::Blocked) {
        progress
    } else {
        None
    };
    Some(Report {
        state,
        id,
        kind,
        progress,
        app,
        title,
        msg,
    })
}

fn osc_body(raw: &str) -> Option<&str> {
    let raw = raw.trim();
    let rest = raw
        .strip_prefix("\u{1b}]")
        .or_else(|| raw.strip_prefix("ESC]"))
        .unwrap_or(raw);
    let rest = rest.trim_start();
    let rest = rest.strip_prefix("7501")?.trim_start();
    let rest = rest.strip_prefix(';')?.trim();
    let rest = rest
        .strip_suffix("\u{1b}\\")
        .or_else(|| rest.strip_suffix('\u{7}'))
        .or_else(|| rest.strip_suffix("ESC\\"))
        .unwrap_or(rest);
    Some(rest.trim())
}

fn parse_state(value: &str) -> Option<ProgramState> {
    Some(match value {
        "idle" => ProgramState::Idle,
        "working" => ProgramState::Working,
        "done" => ProgramState::Done,
        "blocked" => ProgramState::Blocked,
        "error" => ProgramState::Error,
        "clear" => ProgramState::Clear,
        _ => return None,
    })
}

fn parse_kind(value: &str) -> Option<BlockKind> {
    Some(match value {
        "permission" => BlockKind::Permission,
        "question" => BlockKind::Question,
        "auth" => BlockKind::Auth,
        _ => return None,
    })
}

fn value_ok(value: &str) -> bool {
    value.bytes().all(|b| {
        b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b',' | b'+' | b'/' | b'=' | b'-')
    })
}

fn app_ok(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'+' | b'-'))
}

fn id_ok(id: &str) -> bool {
    if id.is_empty() || id.len() > MAX_ID || id.split('/').count() > MAX_DEPTH {
        return false;
    }
    id.split('/').all(|seg| {
        !seg.is_empty()
            && seg.len() <= MAX_SEGMENT
            && seg
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'+' | b'-'))
    })
}

enum Decode {
    Ok(String),
    Discard,
}

fn decode_text(encoded: &str, max_encoded: usize, max_decoded: usize) -> Decode {
    if encoded.len() > max_encoded {
        return Decode::Discard;
    }
    let Some(bytes) = b64(encoded) else {
        return Decode::Discard;
    };
    if bytes.len() > max_decoded {
        return Decode::Discard;
    }
    let Ok(text) = String::from_utf8(bytes) else {
        return Decode::Discard;
    };
    if text.chars().any(is_control) {
        return Decode::Discard;
    }
    Decode::Ok(text)
}

fn is_control(c: char) -> bool {
    let u = c as u32;
    u <= 0x1f || u == 0x7f || (0x80..=0x9f).contains(&u)
}

fn b64(input: &str) -> Option<Vec<u8>> {
    fn val(b: u8) -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let cleaned: Vec<u8> = input.bytes().filter(|b| *b != b'=').collect();
    if cleaned.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(cleaned.len() * 3 / 4);
    let mut i = 0;
    while i < cleaned.len() {
        let remain = cleaned.len() - i;
        let take = remain.min(4);
        let mut n = 0u32;
        for j in 0..take {
            n = (n << 6) | u32::from(val(cleaned[i + j])?);
        }
        let shift = (4 - take) * 6;
        n <<= shift;
        let produce = take - 1;
        for k in 0..produce {
            out.push((n >> (16 - 8 * k)) as u8);
        }
        i += take;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn osc(body: &str) -> String {
        format!("\u{1b}]7501;{body}\u{1b}\\")
    }

    #[test]
    fn a_blocked_permission_decodes_the_message_and_is_not_done() {
        let raw = osc(
            "state=blocked:kind=permission:app=terraform:msg=QXBwbHkgMyB0byBhZGQsIDEgdG8gY2hhbmdlLCAwIHRvIGRlc3Ryb3k/",
        );
        let report = parse_report(&raw).expect("report");
        assert_eq!(report.state, ProgramState::Blocked);
        assert_eq!(report.kind, Some(BlockKind::Permission));
        assert_eq!(report.app.as_deref(), Some("terraform"));
        assert_eq!(
            report.msg.as_deref(),
            Some("Apply 3 to add, 1 to change, 0 to destroy?")
        );
        assert_eq!(report.fold_hint(), FoldHint::NeedsUser);
        assert_eq!(report.osc_status(), Some(AgentStatus::Blocked));
    }

    #[test]
    fn error_is_not_done_and_idle_is_not_herdr_idle() {
        let err = parse_report(&osc("state=error:app=freebuff")).unwrap();
        assert_eq!(err.fold_hint(), FoldHint::Failed);
        assert_eq!(err.osc_status(), None);
        let idle = parse_report(&osc("state=idle:app=claude-code")).unwrap();
        assert_eq!(idle.fold_hint(), FoldHint::AtPrompt);
        assert_eq!(idle.osc_status(), None);
        let done = parse_report(&osc("state=done")).unwrap();
        assert_eq!(done.osc_status(), Some(AgentStatus::Done));
    }

    #[test]
    fn a_child_can_be_blocked_while_the_root_is_working() {
        let mut board = Board::default();
        board.apply(&osc("state=working:app=deploy"));
        board.apply(&osc("state=blocked:id=eu-west:kind=permission"));
        assert_eq!(board.get("").unwrap().state, ProgramState::Working);
        assert_eq!(board.get("eu-west").unwrap().state, ProgramState::Blocked);
        board.apply(&osc("state=clear:id=eu-west"));
        assert!(board.get("eu-west").is_none());
        assert_eq!(board.get("").unwrap().state, ProgramState::Working);
    }

    #[test]
    fn attention_prefers_blocked_child_over_working_root() {
        let mut board = Board::default();
        board.apply(&osc("state=working:app=deploy"));
        board.apply(&osc("state=blocked:id=eu-west:kind=permission"));
        assert_eq!(fold(&board, FoldPolicy::Attention), AgentStatus::Blocked);
        assert_eq!(fold(&board, FoldPolicy::RootWins), AgentStatus::Working);
    }

    #[test]
    fn attention_maps_error_to_unknown_never_done() {
        let mut board = Board::default();
        board.apply(&osc("state=error:app=freebuff"));
        assert_eq!(fold(&board, FoldPolicy::Attention), AgentStatus::Unknown);
        assert_eq!(fold(&board, FoldPolicy::RootWins), AgentStatus::Unknown);
    }

    /// bd-xgea: `error` is not masked by a `done` sibling, in either
    /// direction. A failed root with a finished child is still a failure,
    /// and a failed child is not hidden by a finished root.
    #[test]
    fn attention_error_is_not_masked_by_a_done_sibling() {
        let mut root_failed = Board::default();
        root_failed.apply(&osc("state=error:app=freebuff"));
        root_failed.apply(&osc("state=done:id=child"));
        assert_eq!(
            fold(&root_failed, FoldPolicy::Attention),
            AgentStatus::Unknown,
            "a done child must not turn a failed root into Done"
        );

        let mut child_failed = Board::default();
        child_failed.apply(&osc("state=done:app=deploy"));
        child_failed.apply(&osc("state=error:id=worker"));
        assert_eq!(
            fold(&child_failed, FoldPolicy::Attention),
            AgentStatus::Unknown,
            "a failed child must not be hidden by a done root"
        );

        child_failed.apply(&osc("state=working:id=live"));
        assert_eq!(
            fold(&child_failed, FoldPolicy::Attention),
            AgentStatus::Working
        );
    }

    #[test]
    fn attention_orders_working_over_done_over_idle() {
        let mut board = Board::default();
        board.apply(&osc("state=idle"));
        board.apply(&osc("state=done:id=a"));
        assert_eq!(fold(&board, FoldPolicy::Attention), AgentStatus::Done);
        board.apply(&osc("state=working:id=b"));
        assert_eq!(fold(&board, FoldPolicy::Attention), AgentStatus::Working);
    }

    #[test]
    fn empty_board_is_unknown() {
        assert_eq!(
            fold(&Board::default(), FoldPolicy::Attention),
            AgentStatus::Unknown
        );
        assert_eq!(
            fold(&Board::default(), FoldPolicy::RootWins),
            AgentStatus::Unknown
        );
    }

    #[test]
    fn hc_status_view_builds_the_same_board() {
        let mut board = Board::default();
        board.insert_hc(
            "",
            &HcStatusView {
                state: "working",
                kind: None,
                progress: Some(40),
                app: Some("deploy"),
                title: None,
                msg: None,
            },
        );
        board.insert_hc(
            "child",
            &HcStatusView {
                state: "blocked",
                kind: Some("question"),
                progress: None,
                app: None,
                title: None,
                msg: None,
            },
        );
        assert_eq!(fold(&board, FoldPolicy::Attention), AgentStatus::Blocked);
        assert_eq!(fold(&board, FoldPolicy::RootWins), AgentStatus::Working);
    }

    #[test]
    fn a_report_with_a_control_character_or_no_state_changes_nothing() {
        let mut board = Board::default();
        board.apply(&osc("state=working:app=brew"));
        board.apply(&osc("state=nope"));
        board.apply(&osc("kind=permission"));
        let newline = b64_encode("hello\n");
        board.apply(&osc(&format!("state=working:msg={newline}")));
        assert_eq!(board.len(), 1);
        assert_eq!(board.get("").unwrap().app.as_deref(), Some("brew"));
        assert!(board.get("").unwrap().msg.is_none());
    }

    fn b64_encode(text: &str) -> String {
        const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let bytes = text.as_bytes();
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let mut n = 0u32;
            for (i, b) in chunk.iter().enumerate() {
                n |= u32::from(*b) << (16 - 8 * i);
            }
            for i in 0..=chunk.len() {
                out.push(T[((n >> (18 - 6 * i)) & 0x3f) as usize] as char);
            }
        }
        out
    }
}
