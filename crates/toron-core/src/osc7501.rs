//! OSC 7501, the program status protocol.
//!
//! A program writes one report on its pty. The terminal stores a record.
//! Herdr and Rex both speak this, so a pin with no herdr agent row can
//! still be working, blocked, or failed. The bytes are the spec at
//! <https://www.superlogical.com/rex/docs/build/program-status>.
//!
//! Herdr's `AgentStatus::Idle` means finished and already seen. OSC `idle`
//! means the program is at rest waiting for the next instruction. Those
//! are not the same fact, so this module does not import OSC `idle` as
//! herdr idle, and it does not import OSC `error` as herdr `done`.

use std::collections::BTreeMap;

use crate::backend::AgentStatus;

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

    /// The herdr status this report may update. `None` means "do not
    /// touch the herdr status": OSC idle is not finished-and-seen, and
    /// OSC error must not count as done.
    pub fn herdr_status(&self) -> Option<AgentStatus> {
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
                self.records.retain(|k, _| k != &id && !k.starts_with(&prefix));
            }
            return;
        }
        self.records.insert(id, report);
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
    if let Some(ref ident) = id {
        if !id_ok(ident) {
            return None;
        }
    }
    let kind = if state == ProgramState::Blocked { kind } else { None };
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
    value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b',' | b'+' | b'/' | b'=' | b'-'))
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
        assert_eq!(report.herdr_status(), Some(AgentStatus::Blocked));
    }

    #[test]
    fn error_is_not_done_and_idle_is_not_herdr_idle() {
        let err = parse_report(&osc("state=error:app=freebuff")).unwrap();
        assert_eq!(err.fold_hint(), FoldHint::Failed);
        assert_eq!(err.herdr_status(), None);
        let idle = parse_report(&osc("state=idle:app=claude-code")).unwrap();
        assert_eq!(idle.fold_hint(), FoldHint::AtPrompt);
        assert_eq!(idle.herdr_status(), None);
        let done = parse_report(&osc("state=done")).unwrap();
        assert_eq!(done.herdr_status(), Some(AgentStatus::Done));
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
