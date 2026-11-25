// governed-by: ADR-0003
// governed-by: ADR-0031
//! Effectful edge for the Swarm Board: `LiveHost` and `FakeHost`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use toron_core::mailbox::{FetchInbox, ListAgentsOptions, Mailbox, ReceiptState};

use super::compose::{extract_mentions, validate_board_draft};
use super::model::{
    BoardPost, Diagnostic, FlywheelPanel, LeaseRow, ReceiptCounts, RosterRow, SealedInboxCounts,
    SettingsState, SourceInfo, ToronHealthSignals,
};
use super::msg::{Effect, Msg, Op};

pub const DEFAULT_EFFECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_HTTP_TIMEOUT: Duration = Duration::from_millis(250);
const FLYWHEEL_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(5);
const BOARD_LIMIT: usize = 200;

pub fn effect_timeout() -> Duration {
    std::env::var("TORON_TUI_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_EFFECT_TIMEOUT)
}

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub trait TuiHost: Clone + Send + Sync + 'static {
    fn run(&self, effect: Effect) -> impl Future<Output = Msg> + Send;
    fn now_unix(&self) -> u64;
    fn settings(&self) -> SettingsState;
}

#[derive(Clone)]
pub struct LiveHost {
    mailbox: Arc<Mailbox>,
    project: String,
    agent: Option<String>,
    settings: SettingsState,
}

impl LiveHost {
    pub async fn connect(args: &crate::cli::TuiArgs) -> anyhow::Result<Self> {
        let project = crate::plane::pane_project(args.project.as_deref()).ok_or_else(|| {
            anyhow::anyhow!(
                "pass --project or export TORON_PROJECT (no project in flags or pane env)"
            )
        })?;
        let agent = crate::plane::pane_agent(args.agent.as_deref());

        let source = crate::mailbox_source::MailboxSource::resolve().await?;
        let (mailbox, kind, detail, daemon_loopback) = match source {
            crate::mailbox_source::MailboxSource::Embedded(mb) => (
                mb,
                "embedded",
                "durable root (no daemon configured)".to_string(),
                false,
            ),
            crate::mailbox_source::MailboxSource::Daemon(dc) => {
                if !crate::mailbox_source::daemon_url_is_loopback(dc.endpoint()) {
                    anyhow::bail!(
                        "configured daemon is remote ({}); refusing remote store (bd-d41q)",
                        dc.endpoint()
                    );
                }
                let mb = Arc::new(toron_mcp::demo_mailbox().await?);
                (mb, "daemon (loopback)", dc.endpoint().to_string(), true)
            }
        };

        let npub = match &agent {
            Some(pin) => mailbox
                .whois(&project, pin)
                .await
                .ok()
                .map(|w| w.npub)
                .filter(|n| !n.is_empty()),
            None => None,
        };
        let data_dir = mailbox.data_dir().display().to_string();
        let settings = SettingsState {
            project: project.clone(),
            agent: agent.clone(),
            npub,
            source: SourceInfo {
                kind: kind.to_string(),
                detail,
                data_dir,
                daemon_loopback,
            },
            refresh_ms: args
                .refresh_ms
                .clamp(SettingsState::MIN_REFRESH_MS, SettingsState::MAX_REFRESH_MS),
            relative_time: true,
            chart_window_hours: 24,
        };
        Ok(Self {
            mailbox,
            project,
            agent,
            settings,
        })
    }

    pub fn mailbox(&self) -> &Arc<Mailbox> {
        &self.mailbox
    }

    fn require_agent(&self) -> anyhow::Result<&str> {
        self.agent
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no acting pin — pass --as or export AGENT_NAME"))
    }

    async fn fetch_board(&self) -> anyhow::Result<Vec<BoardPost>> {
        let mut posts: Vec<BoardPost> = self
            .mailbox
            .board_read(&self.project, "main", 0, BOARD_LIMIT)
            .await?
            .messages
            .into_iter()
            .map(|m| BoardPost {
                seq: m.seq,
                event_id: m.event_id,
                author: m.author,
                body: m.body,
                mentions: m.mentions,
                created_at: m.created_at.max(0) as u64,
            })
            .collect();

        let mut seen: std::collections::HashSet<String> =
            posts.iter().map(|p| p.event_id.clone()).collect();

        let canonical = self.mailbox.canonical_project(&self.project).await;
        if let Ok(mut sub) = self
            .mailbox
            .bus()
            .subscribe_trusted(
                toron_core::bus::Filter::new().kinds([toron_core::nostr::kinds::toron_kind(9)]),
            )
            .await
        {
            while let Some(toron_core::bus::SubscriptionEvent::Event(ev)) = sub.next().await {
                let tag = |name: &str| -> Option<String> {
                    ev.tags
                        .iter()
                        .find(|t| t.as_slice().first().map(String::as_str) == Some(name))
                        .and_then(|t| t.as_slice().get(1).cloned())
                };
                if tag("toron-kind").as_deref() != Some("board-post")
                    || tag("t").as_deref() != Some(canonical.as_str())
                {
                    continue;
                }
                let event_id = ev.id.to_hex();
                if !seen.insert(event_id.clone()) {
                    continue;
                }
                let ch = tag("h").unwrap_or_else(|| "main".to_string());
                let body = if ch == "main" {
                    ev.content.clone()
                } else {
                    format!("[#{ch}] {}", ev.content)
                };
                let mentions: Vec<String> = ev
                    .tags
                    .iter()
                    .filter(|t| t.as_slice().first().map(String::as_str) == Some("mention"))
                    .filter_map(|t| t.as_slice().get(1).cloned())
                    .collect();
                posts.push(BoardPost {
                    seq: tag("seq").and_then(|s| s.parse::<u64>().ok()).unwrap_or(0),
                    event_id,
                    author: tag("from").unwrap_or_else(|| "unknown".to_string()),
                    body,
                    mentions,
                    created_at: ev.created_at.as_secs(),
                });
            }
        }

        if let Ok(store) = crate::plane::rebuild_store(&self.mailbox, &self.project).await {
            let agents = self
                .mailbox
                .list_agents(&self.project, ListAgentsOptions::default())
                .await
                .unwrap_or_default();
            let by_pubkey: HashMap<String, String> =
                agents.into_iter().map(|a| (a.npub, a.name)).collect();

            for ch in store.list().into_iter().filter(|c| !c.private) {
                let Ok(events) =
                    crate::plane::collect_channel_events(&self.mailbox, &self.project, &ch.id, 0)
                        .await
                else {
                    continue;
                };
                for ev in events {
                    let event_id = ev.id.to_hex();
                    if !seen.insert(event_id.clone()) {
                        continue;
                    }
                    let author_hex = ev.pubkey.to_hex();
                    let author = by_pubkey
                        .get(&author_hex)
                        .cloned()
                        .unwrap_or_else(|| author_hex.chars().take(12).collect());
                    let label = if ch.name.is_empty() { &ch.id } else { &ch.name };
                    posts.push(BoardPost {
                        seq: 0,
                        event_id,
                        author,
                        body: format!("[#{label}] {}", ev.content),
                        mentions: Vec::new(),
                        created_at: ev.created_at.as_secs(),
                    });
                }
            }
        }

        posts.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.event_id.cmp(&b.event_id))
        });
        for (idx, post) in posts.iter_mut().enumerate() {
            if post.seq == 0 {
                post.seq = idx as u64 + 1;
            }
        }
        Ok(posts)
    }

    async fn fetch_roster(&self) -> anyhow::Result<Vec<RosterRow>> {
        let agents = self
            .mailbox
            .list_agents(&self.project, ListAgentsOptions::default())
            .await?;
        let now = now_unix();
        let presence = crate::plane::read_presence(&self.mailbox, &self.project, now);
        let mut by_npub: HashMap<String, (String, u64)> = HashMap::new();
        for row in presence {
            by_npub.insert(row.npub, (row.state, now.saturating_sub(row.ts)));
        }

        let mut rows: Vec<RosterRow> = agents
            .into_iter()
            .map(|w| {
                let live = by_npub.get(&w.npub);
                let status = live
                    .map(|(s, _)| s.clone())
                    .unwrap_or_else(|| "offline".to_string());
                let retired = status == "done" || w.pane_id.as_deref() == Some("retired");
                RosterRow {
                    lease_count: 0,
                    name: w.name,
                    npub: w.npub,
                    program: w.program,
                    model: w.model,
                    status,
                    online: live.is_some(),
                    retired,
                    age_secs: live.map(|(_, age)| *age),
                }
            })
            .collect();
        rows.sort_by(|a, b| {
            a.retired
                .cmp(&b.retired)
                .then_with(|| b.online.cmp(&a.online))
                .then_with(|| a.name.cmp(&b.name))
        });
        Ok(rows)
    }

    async fn fetch_leases(&self) -> anyhow::Result<Vec<LeaseRow>> {
        let rows = self
            .mailbox
            .list_file_reservations(&self.project, None, None)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| LeaseRow {
                id: r.id,
                holder: r.agent_name,
                path: r.path_pattern,
                exclusive: r.exclusive,
                expires_at: (r.expires_ts_us.unwrap_or(0).max(0) as u64) / 1_000_000,
            })
            .collect())
    }

    async fn fetch_receipts(&self) -> anyhow::Result<ReceiptCounts> {
        let store = self.mailbox.receipt_store()?;
        let rows = store.list(None).await?;
        let mut counts = ReceiptCounts::default();
        for r in rows {
            match r.state {
                ReceiptState::Owed => counts.owed += 1,
                ReceiptState::Acked => counts.acked += 1,
                ReceiptState::Resulted => counts.resulted += 1,
                ReceiptState::Degraded => counts.degraded += 1,
            }
        }
        Ok(counts)
    }

    async fn fetch_sealed_counts(&self) -> anyhow::Result<SealedInboxCounts> {
        let Some(agent) = self.agent.as_deref() else {
            return Ok(SealedInboxCounts::default());
        };
        let rows = self
            .mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: self.project.clone(),
                agent_name: agent.to_string(),
                limit: Some(200),
                include_bodies: false,
                ..Default::default()
            })
            .await?;
        let unread = rows.iter().filter(|r| r.read_ts_us.is_none()).count();
        Ok(SealedInboxCounts {
            total: rows.len(),
            unread,
        })
    }

    async fn probe_toron_health(&self) -> ToronHealthSignals {
        let stuck_queued_sends = toron_core::queued_send::pending()
            .map(|v| v.len())
            .unwrap_or(0);
        let wake_publish_failures = self.mailbox.wake_publish_failures();
        let skip_http = std::env::var("TORON_TUI_NO_HTTP_PROBE").ok().as_deref() == Some("1");
        let (daemon_reachable, daemon_latency_ms) =
            if self.settings.source.daemon_loopback && !skip_http {
                let endpoint = self.settings.source.detail.clone();
                tokio::task::spawn_blocking(move || probe_http_endpoint(&endpoint))
                    .await
                    .unwrap_or((false, None))
            } else {
                (!self.settings.source.daemon_loopback, None)
            };
        let (wake_liveness_ok, wake_never_stamped) =
            if self.settings.source.daemon_loopback && !skip_http {
                let root = toron_core::config::mailbox_root(self.mailbox.data_dir());
                let wake_sock_exists = toron_core::bus::wake::wake_sock_path(&root).exists();
                (
                    daemon_reachable && wake_sock_exists,
                    daemon_reachable && !wake_sock_exists,
                )
            } else {
                (true, false)
            };
        ToronHealthSignals {
            daemon_reachable,
            daemon_latency_ms,
            wake_liveness_ok,
            wake_never_stamped,
            wake_publish_failures,
            stuck_queued_sends,
        }
    }

    async fn probe_flywheel_snapshot(&self) -> Option<FlywheelPanel> {
        if std::env::var("TORON_TUI_NO_FLYWHEEL").ok().as_deref() == Some("1")
            || std::env::var("TORON_UNDER_TEST").is_ok()
        {
            return None;
        }
        let project = self.project.clone();
        let flywheel_bin =
            std::env::var("TORON_FLYWHEEL_BIN").unwrap_or_else(|_| "flywheel".to_string());
        let fut = tokio::process::Command::new(flywheel_bin)
            .args(["snapshot", &project])
            .output();
        let out = tokio::time::timeout(FLYWHEEL_SNAPSHOT_TIMEOUT, fut)
            .await
            .ok()?
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let val: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
        let diagnostics: Vec<Diagnostic> = val
            .get("diagnostics")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let degraded: Vec<String> = val
            .get("degraded")
            .cloned()
            .and_then(|v| serde_json::from_value(v).ok())
            .unwrap_or_default();
        let pin_count = val
            .get("pins")
            .and_then(|v| v.as_array())
            .map_or(0, Vec::len);
        Some(FlywheelPanel {
            project: val
                .get("project")
                .and_then(|v| v.as_str())
                .unwrap_or(&project)
                .to_string(),
            dispatcher: val
                .get("dispatcher")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            degraded,
            pin_count,
            diagnostics,
        })
    }

    async fn post_board(&self, body: &str) -> anyhow::Result<BoardPost> {
        let agent = self.require_agent()?;
        validate_board_draft(body)?;
        let mentions = extract_mentions(body);
        let msg = self
            .mailbox
            .board_post(&self.project, "main", agent, body, &mentions, None)
            .await?;
        Ok(BoardPost {
            seq: msg.seq,
            event_id: msg.event_id,
            author: msg.author,
            body: msg.body,
            mentions: msg.mentions,
            created_at: msg.created_at.max(0) as u64,
        })
    }
}

fn probe_http_endpoint(endpoint: &str) -> (bool, Option<u64>) {
    let config = ureq::config::Config::builder()
        .timeout_global(Some(HEALTH_HTTP_TIMEOUT))
        .http_status_as_error(false)
        .build();
    let agent = ureq::Agent::new_with_config(config);
    let started = std::time::Instant::now();
    match agent.get(endpoint).call() {
        Ok(_) => (true, Some(started.elapsed().as_millis() as u64)),
        Err(_) => (false, Some(started.elapsed().as_millis() as u64)),
    }
}

impl TuiHost for LiveHost {
    fn run(&self, effect: Effect) -> impl Future<Output = Msg> + Send {
        let this = self.clone();
        async move {
            match effect {
                Effect::LoadBoard => match this.fetch_board().await {
                    Ok(rows) => Msg::BoardLoaded(rows),
                    Err(e) => failure(Op::Board, e),
                },
                Effect::LoadRoster => match this.fetch_roster().await {
                    Ok(rows) => Msg::RosterLoaded(rows),
                    Err(e) => failure(Op::Roster, e),
                },
                Effect::LoadLeases => match this.fetch_leases().await {
                    Ok(rows) => Msg::LeasesLoaded(rows),
                    Err(e) => failure(Op::Leases, e),
                },
                Effect::LoadReceipts => match this.fetch_receipts().await {
                    Ok(counts) => Msg::ReceiptsLoaded(counts),
                    Err(e) => failure(Op::Receipts, e),
                },
                Effect::LoadSealedCounts => match this.fetch_sealed_counts().await {
                    Ok(counts) => Msg::SealedCountsLoaded(counts),
                    Err(e) => failure(Op::SealedCounts, e),
                },
                Effect::ProbeToronHealth => Msg::ToronHealthLoaded {
                    signals: this.probe_toron_health().await,
                    now_unix: now_unix(),
                },
                Effect::ProbeFlywheelSnapshot => Msg::FlywheelSnapshotLoaded {
                    panel: this.probe_flywheel_snapshot().await,
                    now_unix: now_unix(),
                },
                Effect::PostBoard { body } => match this.post_board(&body).await {
                    Ok(post) => Msg::BoardPosted(post),
                    Err(e) => failure(Op::Post, e),
                },
                Effect::Quit => Msg::Inflight(0),
            }
        }
    }

    fn now_unix(&self) -> u64 {
        now_unix()
    }

    fn settings(&self) -> SettingsState {
        self.settings.clone()
    }
}

pub const fn effect_op(effect: &Effect) -> Op {
    match effect {
        Effect::LoadBoard => Op::Board,
        Effect::LoadRoster => Op::Roster,
        Effect::LoadLeases => Op::Leases,
        Effect::LoadReceipts => Op::Receipts,
        Effect::LoadSealedCounts => Op::SealedCounts,
        Effect::ProbeToronHealth => Op::Health,
        Effect::ProbeFlywheelSnapshot => Op::Flywheel,
        Effect::PostBoard { .. } => Op::Post,
        Effect::Quit => Op::Health,
    }
}

fn failure(op: Op, e: anyhow::Error) -> Msg {
    Msg::Failed {
        op,
        message: format!("{e:#}"),
    }
}

#[cfg(test)]
pub mod fake {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{Msg, TuiHost, now_unix};
    use crate::tui::model::{
        BoardPost, ReceiptCounts, RosterRow, SealedInboxCounts, SettingsState, ToronHealthSignals,
    };
    use crate::tui::msg::{Effect, Op};

    #[derive(Debug, Clone, Default)]
    #[allow(dead_code)]
    pub enum Reply {
        #[default]
        Ok,
        Fail(String),
        Hang,
        After(Duration),
    }

    #[derive(Clone, Default)]
    pub struct FakeHost {
        inner: Arc<FakeInner>,
    }

    #[derive(Default)]
    struct FakeInner {
        reply: Mutex<Reply>,
        seen: Mutex<Vec<Effect>>,
        settings: Mutex<SettingsState>,
    }

    #[allow(dead_code)]
    impl FakeHost {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn with_reply(reply: Reply) -> Self {
            let host = Self::default();
            *host.inner.reply.lock().expect("reply lock") = reply;
            host
        }

        pub fn seen(&self) -> Vec<Effect> {
            self.inner.seen.lock().expect("seen lock").clone()
        }
    }

    impl TuiHost for FakeHost {
        fn run(&self, effect: Effect) -> impl Future<Output = Msg> + Send {
            let script = self.inner.reply.lock().expect("reply lock").clone();
            self.inner
                .seen
                .lock()
                .expect("seen lock")
                .push(effect.clone());
            async move {
                match script {
                    Reply::Ok => canned(&effect),
                    Reply::Fail(message) => Msg::Failed {
                        op: op_of(&effect),
                        message,
                    },
                    Reply::Hang => std::future::pending().await,
                    Reply::After(d) => {
                        tokio::time::sleep(d).await;
                        canned(&effect)
                    }
                }
            }
        }

        fn now_unix(&self) -> u64 {
            now_unix()
        }

        fn settings(&self) -> SettingsState {
            self.inner.settings.lock().expect("settings lock").clone()
        }
    }

    pub fn canned(effect: &Effect) -> Msg {
        match effect {
            Effect::LoadBoard => Msg::BoardLoaded(vec![BoardPost {
                seq: 1,
                event_id: "ev1".into(),
                author: "AmberFalcon".into(),
                body: "hello swarm".into(),
                mentions: vec![],
                created_at: 1_700_000_000,
            }]),
            Effect::LoadRoster => Msg::RosterLoaded(vec![RosterRow {
                name: "AmberFalcon".into(),
                npub: "npub1live".into(),
                online: true,
                status: "working".into(),
                ..Default::default()
            }]),
            Effect::LoadLeases => Msg::LeasesLoaded(Vec::new()),
            Effect::LoadReceipts => Msg::ReceiptsLoaded(ReceiptCounts::default()),
            Effect::LoadSealedCounts => Msg::SealedCountsLoaded(SealedInboxCounts::default()),
            Effect::ProbeToronHealth => Msg::ToronHealthLoaded {
                signals: ToronHealthSignals::default(),
                now_unix: 1_700_000_000,
            },
            Effect::ProbeFlywheelSnapshot => Msg::FlywheelSnapshotLoaded {
                panel: None,
                now_unix: 1_700_000_000,
            },
            Effect::PostBoard { body } => Msg::BoardPosted(BoardPost {
                seq: 2,
                event_id: "ev2".into(),
                author: "AmberFalcon".into(),
                body: body.clone(),
                mentions: vec![],
                created_at: 1_700_000_010,
            }),
            Effect::Quit => Msg::Inflight(0),
        }
    }

    pub fn op_of(effect: &Effect) -> Op {
        super::effect_op(effect)
    }
}
