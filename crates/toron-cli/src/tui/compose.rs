// governed-by: ADR-0003
//! Compose modal: the form stops, the caret, and the ONE wrapping function
//! that both the drawing and the caret placement read.
//!
//! Everything here is pure. The caret is a BYTE OFFSET into the focused
//! field's text, so nothing in the core needs the terminal width: [`wrap`]
//! takes the width as an argument, and the view calls it once for the text it
//! draws and once for the caret it places. Two callers, one function, so the
//! drawn caret cannot disagree with the drawn text about where the end of a
//! wrapped line is.
//!
//! The assignment kind is toron-core's [`Wake`], not a second copy: the value
//! that goes on the wire is the value the mailbox parses (ADR-0027 E3).

use toron_core::mailbox::send_tags::Wake;

/// One focus stop in the compose form, in `Tab` order.
///
/// `Send` is a stop rather than a key chord on purpose. Plain `Enter` can then
/// mean "advance", "toggle" or "send" depending only on where focus is, and no
/// action needs a control chord: `Ctrl-S` is XOFF on terminals that still
/// honour flow control, and `Ctrl-Enter` is not distinguishable from `Enter`
/// on several of them, so neither is a reliable way to send a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    /// Recipient list (pin, npub, or an alias like `captain`).
    To,
    /// Subject line (NIP-14).
    Subject,
    /// Multi-line body.
    Body,
    /// Assignment kind toggle (`tasked` / `standing`).
    Wake,
    /// Importance toggle (`normal` / `high` / `urgent`).
    Importance,
    /// Ask the recipient for an acknowledgement.
    Ack,
    /// The send stop itself.
    Send,
}

impl Field {
    /// Every stop, in `Tab` order.
    pub const ALL: [Field; 7] = [
        Field::To,
        Field::Subject,
        Field::Body,
        Field::Wake,
        Field::Importance,
        Field::Ack,
        Field::Send,
    ];

    /// Label rendered on the form's left edge.
    pub const fn label(self) -> &'static str {
        match self {
            Field::To => "To",
            Field::Subject => "Subject",
            Field::Body => "Body",
            Field::Wake => "Wake",
            Field::Importance => "Importance",
            Field::Ack => "Ack",
            Field::Send => "",
        }
    }
    /// Cycle one stop, wrapping. `Tab` past the last stop returns to the first
    /// so the form is a loop the operator can walk without thinking.
    pub fn cycle(self, delta: isize) -> Field {
        let n = Self::ALL.len() as isize;
        let i = Self::ALL.iter().position(|f| *f == self).unwrap_or(0) as isize;
        Self::ALL[(i + delta).rem_euclid(n) as usize]
    }
}

/// Importance tag the send carries. These three spellings are the wire values
/// the mailbox stores and `MailRow::important` reads, so the console cannot
/// invent a fourth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Importance {
    /// Ordinary mail.
    Normal,
    /// Deserves attention; lands in the `Important` folder.
    High,
    /// Interrupt-worthy; the caller should have a reason.
    Urgent,
}

impl Importance {
    /// Every level, in ascending urgency.
    pub const ALL: [Importance; 3] = [Importance::Normal, Importance::High, Importance::Urgent];

    /// Wire value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Importance::Normal => "normal",
            Importance::High => "high",
            Importance::Urgent => "urgent",
        }
    }

    /// Cycle by `delta`, wrapping.
    pub fn step(self, delta: isize) -> Importance {
        let n = Self::ALL.len() as isize;
        let i = Self::ALL.iter().position(|l| *l == self).unwrap_or(0) as isize;
        Self::ALL[(i + delta).rem_euclid(n) as usize]
    }
}

/// A caret move inside the focused text field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaretMove {
    /// One character left.
    Left,
    /// One character right.
    Right,
    /// Up one LOGICAL (newline-delimited) line. `Up`/`Down` walk paragraphs,
    /// not display rows: display rows depend on the terminal width, which the
    /// core deliberately does not know, and a composer is written in
    /// paragraphs. The caret keeps its column where the target line is long
    /// enough.
    Up,
    /// Down one logical line.
    Down,
    /// Start of the logical line.
    Home,
    /// End of the logical line (before its newline).
    End,
}

/// The compose form's state. Pure data; the fold owns every transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeState {
    /// Recipient list, as typed (comma-separated is allowed; the mailbox
    /// resolves every name and fails the whole send on an unknown one).
    pub to: String,
    /// Subject line.
    pub subject: String,
    /// Body, markdown, multi-line.
    pub body: String,
    /// Focused stop.
    pub field: Field,
    /// Caret as a byte offset into the focused field's text. Always on a char
    /// boundary; the editing helpers keep it that way.
    pub caret: usize,
    /// Assignment kind. `Tasked` is what a peer message about work is.
    pub wake: Wake,
    /// Importance tag.
    pub importance: Importance,
    /// Ask for an acknowledgement.
    pub ack: bool,
    /// `Some` when this composer is a reply: the thread the send stays in.
    pub thread: Option<String>,
    /// The last send's refusal, kept so the draft survives a failed send.
    pub error: Option<String>,
}

impl Default for ComposeState {
    fn default() -> Self {
        Self {
            to: String::new(),
            subject: String::new(),
            body: String::new(),
            field: Field::To,
            caret: 0,
            wake: Wake::Tasked,
            importance: Importance::Normal,
            ack: false,
            thread: None,
            error: None,
        }
    }
}

impl ComposeState {
    /// A blank composer, focused on the recipient.
    pub fn new() -> Self {
        Self::default()
    }

    /// A reply: pre-addressed, in-thread, focused on the body.
    ///
    /// The subject is copied verbatim. It is tempting to prepend `re: `, but
    /// threading is carried by `thread_id`, not the subject (the mailbox's
    /// reply path attaches the NIP-10 root), so a rewritten subject would be
    /// invented text with no effect on where the message lands.
    pub fn reply(thread: String, to: String, subject: String) -> Self {
        Self {
            to,
            subject,
            thread: Some(thread),
            field: Field::Body,
            ..Self::default()
        }
    }

    /// The text of the focused field.
    pub fn text(&self) -> &str {
        self.text_of(self.field)
    }

    /// The text of any text stop (`""` for the toggles and `Send`).
    pub fn text_of(&self, field: Field) -> &str {
        match field {
            Field::To => &self.to,
            Field::Subject => &self.subject,
            Field::Body => &self.body,
            _ => "",
        }
    }

    /// Mutable access to the focused field's text, when it has any.
    const fn text_mut(&mut self) -> Option<&mut String> {
        match self.field {
            Field::To => Some(&mut self.to),
            Field::Subject => Some(&mut self.subject),
            Field::Body => Some(&mut self.body),
            _ => None,
        }
    }

    /// Move focus to `field`, parking the caret at the end of its text.
    pub fn set_field(&mut self, field: Field) {
        self.field = field;
        self.caret = self.text_of(field).len();
    }

    /// Walk the form one stop (`Tab` = 1, `Shift-Tab` = -1), wrapping.
    pub fn step_field(&mut self, delta: isize) {
        self.set_field(self.field.cycle(delta));
    }

    /// Cycle a toggle stop. A no-op on the text stops and on `Send`, which are
    /// activated rather than toggled.
    pub fn cycle(&mut self, delta: isize) {
        match self.field {
            Field::Wake => {
                self.wake = match self.wake {
                    Wake::Tasked => Wake::Standing,
                    Wake::Standing => Wake::Tasked,
                }
            }
            Field::Importance => self.importance = self.importance.step(delta),
            Field::Ack => self.ack = !self.ack,
            _ => {}
        }
    }

    /// Insert one character at the caret.
    pub fn insert_char(&mut self, c: char) {
        let caret = self.caret;
        if let Some(text) = self.text_mut() {
            text.insert(caret, c);
            self.caret = caret + c.len_utf8();
        }
    }

    /// Insert a newline (the body's `Enter`).
    pub fn newline(&mut self) {
        self.insert_char('\n');
    }

    /// Delete the character before the caret.
    pub fn backspace(&mut self) {
        let caret = self.caret;
        if caret == 0 {
            return;
        }
        let Some(prev) = self
            .text()
            .get(..caret)
            .and_then(|head| head.chars().next_back())
        else {
            return;
        };
        let start = caret - prev.len_utf8();
        if let Some(text) = self.text_mut() {
            text.remove(start);
            self.caret = start;
        }
    }

    /// Delete the character AT the caret (`Delete`).
    pub fn delete_forward(&mut self) {
        let caret = self.caret;
        if caret >= self.text().len() || !self.text().is_char_boundary(caret) {
            return;
        }
        if let Some(text) = self.text_mut() {
            text.remove(caret);
        }
    }

    /// Move the caret. Never panics and never leaves a char boundary, whatever
    /// offset it is handed.
    pub fn move_caret(&mut self, m: CaretMove) {
        let text = self.text().to_string();
        self.caret = snap(&text, self.caret);
        let caret = self.caret;
        self.caret = match m {
            CaretMove::Left => text[..caret]
                .chars()
                .next_back()
                .map(|c| caret - c.len_utf8())
                .unwrap_or(0),
            CaretMove::Right => text[caret..]
                .chars()
                .next()
                .map(|c| caret + c.len_utf8())
                .unwrap_or(caret),
            CaretMove::Home => logical_line(&text, caret).0,
            CaretMove::End => logical_line(&text, caret).1,
            CaretMove::Up => {
                let (start, _) = logical_line(&text, caret);
                if start == 0 {
                    0
                } else {
                    let (prev_start, prev_end) = logical_line(&text, start - 1);
                    let col = text[start..caret].chars().count();
                    prev_start + byte_col(&text[prev_start..prev_end], col)
                }
            }
            CaretMove::Down => {
                let (start, end) = logical_line(&text, caret);
                if end >= text.len() {
                    text.len()
                } else {
                    let (next_start, next_end) = logical_line(&text, end + 1);
                    let col = text[start..caret].chars().count();
                    next_start + byte_col(&text[next_start..next_end], col)
                }
            }
        };
    }

    /// The message this form describes.
    ///
    /// A value rather than seven arguments threaded through the fold and the
    /// host: the form produces it, the effect carries it, and the host turns it
    /// into the mailbox's `SendMessage`. One shape, so the three cannot
    /// disagree about what was typed.
    pub fn to_send(&self) -> ComposedSend {
        ComposedSend {
            to: recipients(&self.to),
            subject: self.subject.trim().to_string(),
            body: self.body.clone(),
            thread: self.thread.clone(),
            wake: self.wake.as_str(),
            importance: self.importance.as_str(),
            ack: self.ack,
        }
    }

    /// Why the form cannot be sent yet, if it cannot. Kept separate from the
    /// send so the fold can put the reason on the form and keep the draft.
    pub fn validate(&self) -> Result<(), String> {
        if self.to.trim().is_empty() {
            return Err("no recipient — put a pin, npub or alias in To".into());
        }
        if self.subject.trim().is_empty() {
            return Err("no subject — the mailbox requires one".into());
        }
        if self.body.trim().is_empty() {
            return Err("empty body — write something to send".into());
        }
        Ok(())
    }
}

/// The recipients in a `To` field: comma-separated, blanks dropped. The
/// mailbox resolves each name and fails the whole send on an unknown one, so
/// the console does not pre-judge which spellings are legal.
pub fn recipients(to: &str) -> Vec<String> {
    to.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// One composed message, as the form describes it. Data only: the wire values
/// are the mailbox's own spellings, so nothing here has to be parsed again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposedSend {
    /// Recipients, already split (the mailbox wants a list, and re-deciding
    /// that at the host is how a second spelling of `--to` appears).
    pub to: Vec<String>,
    /// Subject line.
    pub subject: String,
    /// Body, markdown, multi-line.
    pub body: String,
    /// Thread to reply within, when replying.
    pub thread: Option<String>,
    /// Assignment kind, a [`Wake`] spelling.
    pub wake: &'static str,
    /// Importance tag (`normal` / `high` / `urgent`).
    pub importance: &'static str,
    /// Ask the recipient for an acknowledgement.
    pub ack: bool,
}

/// Snap a byte offset onto a char boundary at or before it.
fn snap(text: &str, mut at: usize) -> usize {
    at = at.min(text.len());
    while at > 0 && !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// Byte bounds of the logical (newline-delimited) line holding `at`.
///
/// `start` is the line's first byte and `end` the byte of its newline (or the
/// end of the text for the last line), so `text[start..end]` is exactly the
/// line and the caret can sit at `end` meaning "end of this line".
pub fn logical_line(text: &str, at: usize) -> (usize, usize) {
    let at = snap(text, at);
    let start = text[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let end = text[start..]
        .find('\n')
        .map(|i| start + i)
        .unwrap_or(text.len());
    (start, end)
}

/// Byte offset of character index `col` within `line`, clamped to its length.
fn byte_col(line: &str, col: usize) -> usize {
    line.char_indices()
        .nth(col)
        .map(|(i, _)| i)
        .unwrap_or(line.len())
}

/// One display line of a wrapped text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wrapped {
    /// The line's text. Never wider than the requested width, so the renderer
    /// is never allowed to wrap it a second time (a second wrap is what would
    /// put the caret on a row the text is not on).
    pub text: String,
    /// Byte offset in the source where this line starts.
    pub start: usize,
    /// Byte offset where it ends. Contiguous: line `k`'s `end` is line `k+1`'s
    /// `start`, which is what makes [`caret_pos`] exact instead of approximate.
    pub end: usize,
}

/// Wrap `text` into display lines at most `width` columns wide.
///
/// Word-wrapped where a space is available, hard-broken inside a word that is
/// longer than the width (a URL in a message must not spill out of the box).
/// The space a line breaks at stays on that line, which is why the offsets are
/// exactly contiguous; trailing whitespace is invisible anyway.
///
/// `width == 0` returns one line per logical line: with nothing measurable the
/// honest answer is "no wrapping", and the caller renders an empty field.
pub fn wrap(text: &str, width: usize) -> Vec<Wrapped> {
    let mut out = Vec::new();
    let mut base = 0usize;
    for line in text.split('\n') {
        let too_long = width > 0 && line.chars().count() > width;
        if !too_long {
            out.push(Wrapped {
                text: line.to_string(),
                start: base,
                end: base + line.len(),
            });
            base += line.len() + 1;
            continue;
        }
        let mut rest = line;
        let mut offset = base;
        while rest.chars().count() > width {
            let head: String = rest.chars().take(width).collect();
            let take = match head.rfind(' ') {
                Some(sp) if sp > 0 => head[..sp].chars().count() + 1,
                _ => width,
            };
            let chunk: String = rest.chars().take(take).collect();
            let len = chunk.len();
            out.push(Wrapped {
                text: chunk,
                start: offset,
                end: offset + len,
            });
            offset += len;
            rest = &rest[len..];
        }
        out.push(Wrapped {
            text: rest.to_string(),
            start: offset,
            end: offset + rest.len(),
        });
        base += line.len() + 1;
    }
    out
}

/// The visible slice of a SINGLE-LINE field, and the caret's column inside it.
///
/// A recipient list longer than the box scrolls so the caret stays visible;
/// without this the cursor would be parked outside the field the operator is
/// typing into. `col` is always less than `width` when `width > 0`.
pub fn single_line_window(text: &str, width: usize, caret: usize) -> (String, usize) {
    if width == 0 {
        return (String::new(), 0);
    }
    let caret = snap(text, caret);
    let caret_col = text[..caret].chars().count();
    let start = if caret_col >= width {
        caret_col - width + 1
    } else {
        0
    };
    let visible: String = text.chars().skip(start).take(width).collect();
    (visible, caret_col - start)
}

/// What a multi-line body box draws, and where the caret goes inside it.
///
/// One function produces both: the rendered rows and the caret coordinate, so a
/// body taller than its box cannot scroll the text while leaving the caret
/// behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyView {
    /// The wrapped rows to draw, top to bottom (already scrolled).
    pub lines: Vec<String>,
    /// Caret position `(row, column)` within the drawn rows.
    pub caret: (usize, usize),
}

/// Window a wrapped body to `height` rows, keeping the caret in view.
pub fn body_view(text: &str, width: usize, height: usize, caret: usize) -> BodyView {
    let wrapped = wrap(text, width);
    let (row, col) = caret_pos(text, width, caret);
    let start = if height > 0 && row >= height {
        row - height + 1
    } else {
        0
    };
    let end = if height == 0 {
        start
    } else {
        (start + height).min(wrapped.len())
    };
    BodyView {
        lines: wrapped[start..end].iter().map(|l| l.text.clone()).collect(),
        caret: (row - start, col),
    }
}

/// Where the caret sits on screen: `(row, column)` inside the wrapped text.
///
/// The row is the LAST wrapped line that starts at or before the caret, so a
/// caret landing exactly on a line boundary belongs to the start of the next
/// line (the newline was consumed) — which is where a terminal puts it too.
pub fn caret_pos(text: &str, width: usize, caret: usize) -> (usize, usize) {
    let caret = snap(text, caret);
    let lines = wrap(text, width);
    let mut row = 0usize;
    for (i, line) in lines.iter().enumerate() {
        if line.start <= caret {
            row = i;
        }
    }
    let start = lines.get(row).map(|l| l.start).unwrap_or(0);
    (row, text[start..caret].chars().count())
}
