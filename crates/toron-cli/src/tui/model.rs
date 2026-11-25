// governed-by: ADR-0003
// governed-by: ADR-0031
//! Pure Swarm Board model (Elm-everywhere).
//!
//! No I/O, no clock, no environment reads: wall time arrives via `Msg::Tick`,
//! and [`Model::projection`] produces the exact `toron-board.v1` JSON contract
//! and screen state. Sealed NIP-17 bodies are never stored here (counts only).

use serde::{Deserialize, Serialize};

pub const MIN_W: u16 = 40;
pub const MIN_H: u16 = 12;
pub const WIDE_W: u16 = 100;

pub const TORON_PROBE_INTERVAL_SECS: u64 = 10;
pub const FLYWHEEL_PROBE_INTERVAL_SECS: u64 = 30;
pub const IDLE_PAUSE_SECS: u64 = 300;
pub const BOARD_SCHEMA: &str = "toron-board.v1";

/// Responsive layout mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LayoutMode {
    Wide,
    Compact,
    TooSmall,
}

/// Pick the layout mode for a frame size.
pub const fn layout_mode(width: u16, height: u16) -> LayoutMode {
    if width < MIN_W || height < MIN_H {
        LayoutMode::TooSmall
    } else if width >= WIDE_W {
        LayoutMode::Wide
    } else {
        LayoutMode::Compact
    }
}

/// Bottom inspector mode (`1`..`4` / `d` / `l` / `s`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Inspector {
    #[default]
    Message,
    Diagnostics,
    Leases,
    Charts,
}

impl Inspector {
    pub const ALL: [Inspector; 4] = [
        Inspector::Message,
        Inspector::Diagnostics,
        Inspector::Leases,
        Inspector::Charts,
    ];

    pub const fn title(self) -> &'static str {
        match self {
            Inspector::Message => "Message",
            Inspector::Diagnostics => "Diagnostics",
            Inspector::Leases => "Leases",
            Inspector::Charts => "Charts",
        }
    }

    pub fn step(self, delta: isize) -> Inspector {
        let i = Self::ALL.iter().position(|x| *x == self).unwrap_or(0) as isize;
        let next = (i + delta).rem_euclid(Self::ALL.len() as isize);
        Self::ALL[next as usize]
    }
}

/// Input mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mode {
    #[default]
    Normal,
    Filter,
    Help,
    Compose,
}

/// Diagnostic severity (`P0` and `P1` fail `--check`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    P0,
    P1,
    P2,
    P3,
}

impl DiagnosticSeverity {
    pub const fn is_blocking(self) -> bool {
        matches!(self, Self::P0 | Self::P1)
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::P0 => "P0",
            Self::P1 => "P1",
            Self::P2 => "P2",
            Self::P3 => "P3",
        }
    }
}

/// One diagnostic finding from either `toron` or `flywheel`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub id: String,
    pub severity: DiagnosticSeverity,
    #[serde(default = "default_source")]
    pub source: String,
    pub summary: String,
    pub evidence: String,
    pub remedy_cmd: String,
}

fn default_source() -> String {
    "flywheel".to_string()
}

/// Signals probed for `toron` health.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToronHealthSignals {
    pub daemon_reachable: bool,
    pub daemon_latency_ms: Option<u64>,
    pub wake_liveness_ok: bool,
    pub wake_never_stamped: bool,
    pub wake_publish_failures: u64,
    pub stuck_queued_sends: usize,
}

impl Default for ToronHealthSignals {
    fn default() -> Self {
        Self {
            daemon_reachable: false,
            daemon_latency_ms: None,
            wake_liveness_ok: true,
            wake_never_stamped: false,
            wake_publish_failures: 0,
            stuck_queued_sends: 0,
        }
    }
}

/// Pure `toron` v1 diagnostic rules over [`ToronHealthSignals`].
pub fn diagnose_toron(signals: &ToronHealthSignals) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    if signals.wake_never_stamped {
        out.push(Diagnostic {
            id: "WAKE_NEVER_STAMPED".to_string(),
            severity: DiagnosticSeverity::P1,
            source: "toron".to_string(),
            summary: "Wake liveness has never been stamped".to_string(),
            evidence: "wake_never_stamped=true".to_string(),
            remedy_cmd: "toron daemon status --json".to_string(),
        });
    } else if !signals.wake_liveness_ok {
        out.push(Diagnostic {
            id: "WAKE_DEAF".to_string(),
            severity: DiagnosticSeverity::P0,
            source: "toron".to_string(),
            summary: "Wake listener is deaf or expired".to_string(),
            evidence: "wake_liveness_ok=false".to_string(),
            remedy_cmd: "toron daemon status --json".to_string(),
        });
    }
    if signals.wake_publish_failures > 0 {
        out.push(Diagnostic {
            id: "WAKE_PUBLISH_FAILURES".to_string(),
            severity: DiagnosticSeverity::P1,
            source: "toron".to_string(),
            summary: "Wake event publish failures detected".to_string(),
            evidence: format!("wake_publish_failures={}", signals.wake_publish_failures),
            remedy_cmd: "toron daemon status --json".to_string(),
        });
    }
    if signals.stuck_queued_sends > 0 {
        out.push(Diagnostic {
            id: "STUCK_QUEUED_SEND".to_string(),
            severity: DiagnosticSeverity::P1,
            source: "toron".to_string(),
            summary: "Queued sends are waiting in queued-sends.jsonl".to_string(),
            evidence: format!("stuck_queued_sends={}", signals.stuck_queued_sends),
            remedy_cmd: "toron mail flush".to_string(),
        });
    }
    out
}

/// Optional `flywheel snapshot` panel parsed from subprocess stdout.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FlywheelPanel {
    pub project: String,
    pub dispatcher: String,
    pub degraded: Vec<String>,
    pub pin_count: usize,
    pub diagnostics: Vec<Diagnostic>,
}

/// One `#seq` board post.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardPost {
    pub seq: u64,
    pub event_id: String,
    pub author: String,
    pub body: String,
    pub mentions: Vec<String>,
    pub created_at: u64,
}

impl BoardPost {
    pub fn preview(&self) -> String {
        self.body
            .lines()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("")
            .trim()
            .to_string()
    }
}

/// One agent seat on the roster.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RosterRow {
    pub name: String,
    pub npub: String,
    pub program: Option<String>,
    pub model: Option<String>,
    pub status: String,
    pub online: bool,
    pub retired: bool,
    pub age_secs: Option<u64>,
    pub lease_count: usize,
}

impl RosterRow {
    pub fn marker(&self) -> &'static str {
        if self.retired {
            "•"
        } else if self.online {
            "●"
        } else {
            "○"
        }
    }
}

/// One active file reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRow {
    pub id: String,
    pub holder: String,
    pub path: String,
    pub exclusive: bool,
    pub expires_at: u64,
}

/// Durable receipt transition counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptCounts {
    pub owed: usize,
    pub acked: usize,
    pub resulted: usize,
    pub degraded: usize,
}

/// Sealed NIP-17 inbox counts only — bodies are excluded from v1.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedInboxCounts {
    pub total: usize,
    pub unread: usize,
}

/// Source connection info.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SourceInfo {
    pub kind: String,
    pub detail: String,
    pub data_dir: String,
    pub daemon_loopback: bool,
}

/// Seeded identity and display settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsState {
    pub project: String,
    pub agent: Option<String>,
    pub npub: Option<String>,
    pub source: SourceInfo,
    pub refresh_ms: u64,
    pub relative_time: bool,
    pub chart_window_hours: u64,
}

impl Default for SettingsState {
    fn default() -> Self {
        Self {
            project: String::new(),
            agent: None,
            npub: None,
            source: SourceInfo::default(),
            refresh_ms: 1000,
            relative_time: true,
            chart_window_hours: 24,
        }
    }
}

impl SettingsState {
    pub const MIN_REFRESH_MS: u64 = 250;
    pub const MAX_REFRESH_MS: u64 = 10_000;

    pub fn step_refresh(&self, step: i64) -> u64 {
        (self.refresh_ms as i64 + step)
            .clamp(Self::MIN_REFRESH_MS as i64, Self::MAX_REFRESH_MS as i64) as u64
    }
}

/// Status-line severity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StatusKind {
    #[default]
    Info,
    Ok,
    Error,
}

/// Footer status banner.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Status {
    pub text: String,
    pub kind: StatusKind,
}

/// Single pure JSON projection shared with `--json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardProjection {
    pub schema: String,
    pub project: String,
    pub viewer: Option<String>,
    pub follow: bool,
    pub toron_health: ToronHealthSignals,
    pub flywheel: Option<FlywheelPanel>,
    pub roster: Vec<RosterRow>,
    pub posts: Vec<BoardPost>,
    pub leases: Vec<LeaseRow>,
    pub receipts: ReceiptCounts,
    pub sealed_inbox: SealedInboxCounts,
    pub diagnostics: Vec<Diagnostic>,
}

/// Complete Swarm Board UI state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub mode: Mode,
    pub inspector: Inspector,
    pub settings: SettingsState,
    pub posts: Vec<BoardPost>,
    pub roster: Vec<RosterRow>,
    pub leases: Vec<LeaseRow>,
    pub receipts: ReceiptCounts,
    pub sealed_inbox: SealedInboxCounts,
    pub toron_health: ToronHealthSignals,
    pub flywheel: Option<FlywheelPanel>,
    pub follow: bool,
    pub show_retired: bool,
    pub selected_post: usize,
    pub msg_scroll: usize,
    pub selected_diag: usize,
    pub filter: String,
    pub compose_draft: Option<String>,
    pub now_unix: u64,
    pub tick: u64,
    pub inflight: usize,
    pub focused: bool,
    pub last_input_unix: u64,
    pub last_toron_probe_unix: Option<u64>,
    pub last_flywheel_probe_unix: Option<u64>,
    pub toron_probe_inflight: bool,
    pub flywheel_probe_inflight: bool,
    pub status: Status,
    pub should_quit: bool,
}

impl Model {
    pub fn seed(settings: SettingsState) -> Self {
        Self {
            mode: Mode::Normal,
            inspector: Inspector::Message,
            settings,
            posts: Vec::new(),
            roster: Vec::new(),
            leases: Vec::new(),
            receipts: ReceiptCounts::default(),
            sealed_inbox: SealedInboxCounts::default(),
            toron_health: ToronHealthSignals::default(),
            flywheel: None,
            follow: true,
            show_retired: false,
            selected_post: 0,
            msg_scroll: 0,
            selected_diag: 0,
            filter: String::new(),
            compose_draft: None,
            now_unix: 0,
            tick: 0,
            inflight: 0,
            focused: true,
            last_input_unix: 0,
            last_toron_probe_unix: None,
            last_flywheel_probe_unix: None,
            toron_probe_inflight: false,
            flywheel_probe_inflight: false,
            status: Status::default(),
            should_quit: false,
        }
    }

    pub fn visible_posts(&self) -> Vec<&BoardPost> {
        let needle = self.filter.trim().to_ascii_lowercase();
        if needle.is_empty() {
            return self.posts.iter().collect();
        }
        self.posts
            .iter()
            .filter(|p| {
                p.author.to_ascii_lowercase().contains(&needle)
                    || p.body.to_ascii_lowercase().contains(&needle)
                    || p.mentions
                        .iter()
                        .any(|m| m.to_ascii_lowercase().contains(&needle))
            })
            .collect()
    }

    pub fn selected_board_post(&self) -> Option<&BoardPost> {
        let visible = self.visible_posts();
        visible.get(self.selected_post).copied()
    }

    pub fn clamp_selection(&mut self) {
        let post_len = self.visible_posts().len();
        self.selected_post = if post_len == 0 {
            0
        } else {
            self.selected_post.min(post_len - 1)
        };
        let diag_len = self.diagnostics().len();
        self.selected_diag = if diag_len == 0 {
            0
        } else {
            self.selected_diag.min(diag_len - 1)
        };
    }

    pub fn diagnostics(&self) -> Vec<Diagnostic> {
        let mut all = diagnose_toron(&self.toron_health);
        if let Some(fw) = &self.flywheel {
            all.extend(fw.diagnostics.iter().cloned());
        }
        all.sort_by(|a, b| a.severity.cmp(&b.severity).then_with(|| a.id.cmp(&b.id)));
        all
    }

    pub fn has_blocking_diagnostics(&self) -> bool {
        self.diagnostics().iter().any(|d| d.severity.is_blocking())
    }

    pub fn projection(&self) -> BoardProjection {
        BoardProjection {
            schema: BOARD_SCHEMA.to_string(),
            project: self.settings.project.clone(),
            viewer: self.settings.agent.clone(),
            follow: self.follow,
            toron_health: self.toron_health.clone(),
            flywheel: self.flywheel.clone(),
            roster: self.roster.clone(),
            posts: self.posts.clone(),
            leases: self.leases.clone(),
            receipts: self.receipts,
            sealed_inbox: self.sealed_inbox,
            diagnostics: self.diagnostics(),
        }
    }

    pub fn arrivals(&self) -> Vec<u64> {
        let ts: Vec<u64> = self.posts.iter().map(|p| p.created_at).collect();
        super::charts::hour_buckets(&ts, self.now_unix, self.settings.chart_window_hours)
    }

    pub fn set_status(&mut self, kind: StatusKind, text: impl Into<String>) {
        self.status = Status {
            text: text.into(),
            kind,
        };
    }
}
