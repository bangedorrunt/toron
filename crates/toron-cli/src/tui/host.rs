// governed-by: ADR-0003
// governed-by: ADR-0031
//! The effectful edge: one host trait, two implementations.
//!
//! The fold returns [`Effect`] values; the runtime hands them to a [`TuiHost`]
//! and feeds the [`Msg`] back. `LiveHost` reads the same mailbox the CLI's
//! plane commands read (daemon-loopback or the durable root — never a second
//! store, bd-d41q), and the fake host is what the interpreter tests script.
//!
//! Nothing here decides anything: every `run` arm is fetch → map → message.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use toron_core::mailbox::{
    FetchInbox, InboxMessage, ListAgentsOptions, Mailbox, ReceiptState, SearchMessage,
    SearchMessages, SendMessage,
};
use toron_core::nostr::presence::PresenceState;

use super::compose::ComposedSend;
use super::model::{
    AgentRow, ChannelRow, HealthRow, MailRow, PostRow, ReceiptCounts, SettingsState, SourceInfo,
};
use super::msg::{Effect, Msg, Op};

/// Per-effect ceiling. A wedged store must surface as a typed failure, not a
/// dashboard that quietly stops updating (bounded wait, effect law #10).
pub const DEFAULT_EFFECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The bounded wait for one effect, overridable for slow stores.
pub fn effect_timeout() -> Duration {
    std::env::var("TORON_TUI_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_EFFECT_TIMEOUT)
}

/// The data port the runtime drives. `run` never fails: a fetch that cannot
/// complete becomes `Msg::Failed`, which the fold renders and recovers from.
pub trait TuiHost: Clone + Send + Sync + 'static {
    /// Execute one effect and return the message it produced.
    fn run(&self, effect: Effect) -> impl Future<Output = Msg> + Send;
    /// Wall clock, as a payload for `Msg::Tick` (the fold never reads it).
    fn now_unix(&self) -> u64;
    /// The identity/source facts the model seeds from.
    fn settings(&self) -> SettingsState;
}

/// Unix seconds now.
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Inbox page size (the Mail list is a view, not an archive).
const INBOX_LIMIT: usize = 200;
/// Search page size.
const SEARCH_LIMIT: usize = 200;

/// The live host: the local mailbox, plus the resolved identity facts.
#[derive(Clone)]
pub struct LiveHost {
    mailbox: Arc<Mailbox>,
    project: String,
    agent: Option<String>,
    settings: SettingsState,
}

impl LiveHost {
    /// Resolve identity + the one mailbox, then probe the acting key.
    ///
    /// Project: `--project`, else `TORON_PROJECT` — otherwise bail loud (no
    /// silent `demo`). Pin: `--as`, else the pane env ladder; a missing pin is
    /// allowed (the roster and channels still work) and the Mail tab says so.
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
                        "the configured daemon is remote ({}): its store is not this machine's. \
                         Unset TORON_MCP_URL / [mail.toron] url to run against the local durable \
                         root. Refusing to show a second store the daemon never writes (bd-d41q).",
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
            dense: false,
            relative_time: true,
            show_closed_channels: args.all,
        };
        Ok(Self {
            mailbox,
            project,
            agent,
            settings,
        })
    }

    /// The durable data root (project roots hang off it).
    fn root(&self) -> String {
        toron_core::config::mailbox_root(self.mailbox.data_dir())
            .display()
            .to_string()
    }

    /// The acting pin, or a typed error naming the fix.
    fn require_agent(&self) -> anyhow::Result<&str> {
        self.agent
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no acting pin — pass --as or export AGENT_NAME"))
    }

    /// Acked corrs, from the durable receipt store (one list per inbox page).
    /// `Degraded` (a receipt that could not be maintained after a durable
    /// enqueue) counts as acked: the wrap landed, so the message is not owed.
    async fn acked_corrs(&self) -> anyhow::Result<std::collections::HashSet<String>> {
        let store = self.mailbox.receipt_store()?;
        let rows = store.list(None).await?;
        Ok(rows
            .into_iter()
            .filter(|r| {
                matches!(
                    r.state,
                    ReceiptState::Acked | ReceiptState::Resulted | ReceiptState::Degraded
                )
            })
            .map(|r| r.corr)
            .collect())
    }

    async fn fetch_inbox(&self) -> anyhow::Result<Vec<MailRow>> {
        let agent = self.require_agent()?;
        let rows = self
            .mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: self.project.clone(),
                agent_name: agent.to_string(),
                limit: Some(INBOX_LIMIT),
                include_bodies: false,
                ..Default::default()
            })
            .await?;
        let acked = self.acked_corrs().await.unwrap_or_default();
        Ok(rows.into_iter().map(|m| mail_row(m, &acked)).collect())
    }

    /// The body-carrying inbox page. The reader narrows it to one thread in
    /// the fold, and this is deliberately NOT a `thread_id` query: a top-level
    /// message carries no thread tag, so an exact thread filter would return
    /// nothing for exactly the common case (a single message with no replies).
    /// Projection unwraps every wrap either way, so the extra cost is the
    /// bodies already in hand, not a second pass.
    async fn fetch_page_with_bodies(&self) -> anyhow::Result<Vec<MailRow>> {
        let agent = self.require_agent()?;
        let rows = self
            .mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: self.project.clone(),
                agent_name: agent.to_string(),
                limit: Some(INBOX_LIMIT),
                include_bodies: true,
                ..Default::default()
            })
            .await?;
        let acked = self.acked_corrs().await.unwrap_or_default();
        Ok(rows.into_iter().map(|m| mail_row(m, &acked)).collect())
    }

    async fn fetch_channels(&self) -> anyhow::Result<Vec<ChannelRow>> {
        let store = crate::plane::rebuild_store(&self.mailbox, &self.project).await?;
        let activity = crate::plane::channel_activity(&self.mailbox, &self.project).await?;
        let me = self.pubkey();
        let mut rows: Vec<ChannelRow> = store
            .list()
            .into_iter()
            .filter(|c| {
                if c.closed && !self.settings.show_closed_channels {
                    return false;
                }
                !c.private || me.is_some_and(|pk| c.is_member(&pk))
            })
            .map(|c| {
                let (posts, last_at) = activity.get(&c.id).copied().unwrap_or((0, 0));
                ChannelRow {
                    joined: me.is_some_and(|pk| c.is_member(&pk)),
                    id: c.id,
                    name: c.name,
                    private: c.private,
                    closed: c.closed,
                    members: c.members.len(),
                    posts,
                    last_at: (last_at > 0).then_some(last_at),
                }
            })
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        Ok(rows)
    }

    async fn fetch_posts(&self, channel_id: &str) -> anyhow::Result<Vec<PostRow>> {
        let events =
            crate::plane::collect_channel_events(&self.mailbox, &self.project, channel_id, 0)
                .await?;
        Ok(events
            .into_iter()
            .map(|ev| PostRow {
                event_id: ev.id.to_hex(),
                author: ev.pubkey.to_hex(),
                content: ev.content.clone(),
                at: ev.created_at.as_secs(),
                kind: ev.kind.as_u16(),
                react_to: ev
                    .tags
                    .iter()
                    .find(|t| t.kind() == "e")
                    .and_then(|t| t.content())
                    .map(str::to_string),
                mentions: ev
                    .tags
                    .iter()
                    .filter(|t| t.kind() == "p")
                    .filter_map(|t| t.content())
                    .map(str::to_string)
                    .collect(),
            })
            .collect())
    }

    async fn fetch_agents(&self) -> anyhow::Result<Vec<AgentRow>> {
        let roster = self
            .mailbox
            .list_agents(&self.project, ListAgentsOptions::default())
            .await?;
        let now = now_unix();
        let presence = crate::plane::read_presence(&self.mailbox, &self.project, now);
        let mut by_npub: HashMap<String, (PresenceState, u64, Option<String>)> = HashMap::new();
        for row in presence {
            by_npub.insert(
                row.npub,
                (
                    parse_presence_state(&row.state),
                    now.saturating_sub(row.ts),
                    row.status,
                ),
            );
        }
        let mut rows: Vec<AgentRow> = roster
            .into_iter()
            .map(|w| {
                let live = by_npub.get(&w.npub);
                AgentRow {
                    is_self: self.agent.as_deref() == Some(w.name.as_str()),
                    name: w.name,
                    npub: w.npub,
                    program: w.program,
                    model: w.model,
                    task: w.task_description,
                    pane_id: w.pane_id,
                    online: live.is_some(),
                    state: live.map(|l| l.0).unwrap_or(PresenceState::Unknown),
                    age_secs: live.map(|l| l.1),
                    status: live.and_then(|l| l.2.clone()),
                }
            })
            .collect();
        rows.sort_by(|a, b| {
            b.online
                .cmp(&a.online)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.npub.cmp(&b.npub))
        });
        Ok(rows)
    }

    async fn fetch_search(&self, query: &str) -> anyhow::Result<Vec<MailRow>> {
        let hits = self
            .mailbox
            .search_messages(&SearchMessages {
                project_key: self.project.clone(),
                query: query.to_string(),
                limit: Some(SEARCH_LIMIT),
                include_body_md: false,
                ..Default::default()
            })
            .await?;
        Ok(hits.into_iter().map(search_row).collect())
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

    /// Health lines: facts about the environment this dashboard is reading,
    /// assembled once per dashboard tick (no probe is invented that does not
    /// exist — an unavailable backend says so).
    fn health(&self) -> Vec<HealthRow> {
        let mut out = vec![
            HealthRow {
                label: "source".into(),
                value: if self.settings.source.daemon_loopback {
                    format!("daemon (loopback) {}", self.settings.source.detail)
                } else {
                    "embedded (durable root)".into()
                },
                ok: true,
            },
            HealthRow {
                label: "store root".into(),
                value: self.root(),
                ok: true,
            },
            HealthRow {
                label: "project".into(),
                value: self.project.clone(),
                ok: true,
            },
            HealthRow {
                label: "identity".into(),
                value: match (&self.agent, &self.settings.npub) {
                    (Some(pin), Some(npub)) => {
                        format!("{pin} ({})", short_npub(npub))
                    }
                    (Some(pin), None) => format!("{pin} (not registered in this project)"),
                    (None, _) => "none — pass --as or export AGENT_NAME".into(),
                },
                ok: self.agent.is_some(),
            },
        ];
        out.push(HealthRow {
            label: "presence".into(),
            value: format!(
                "joined via {}/presence.jsonl (TTL {})",
                self.root(),
                std::env::var("TORON_PRESENCE_TTL_SECS").unwrap_or_else(|_| "60".into())
            ),
            ok: true,
        });
        out
    }

    /// The acting pin's public key, when it resolves.
    fn pubkey(&self) -> Option<nostr::prelude::PublicKey> {
        let npub = self.settings.npub.as_deref()?;
        nostr::prelude::PublicKey::from_hex(npub).ok()
    }
}

impl TuiHost for LiveHost {
    fn run(&self, effect: Effect) -> impl Future<Output = Msg> + Send {
        let this = self.clone();
        async move {
            match effect {
                Effect::RefreshInbox => match this.fetch_inbox().await {
                    Ok(rows) => Msg::InboxLoaded(rows),
                    Err(e) => failure(Op::Inbox, e),
                },
                Effect::RefreshThread { thread_id } => match this.fetch_page_with_bodies().await {
                    Ok(rows) => Msg::ThreadLoaded { thread_id, rows },
                    Err(e) => failure(Op::Thread, e),
                },
                Effect::RefreshChannels => match this.fetch_channels().await {
                    Ok(rows) => Msg::ChannelsLoaded(rows),
                    Err(e) => failure(Op::Channels, e),
                },
                Effect::RefreshChannelPosts { channel_id } => {
                    match this.fetch_posts(&channel_id).await {
                        Ok(rows) => Msg::ChannelPostsLoaded { channel_id, rows },
                        Err(e) => failure(Op::ChannelPosts, e),
                    }
                }
                Effect::RefreshAgents => match this.fetch_agents().await {
                    Ok(rows) => Msg::AgentsLoaded(rows),
                    Err(e) => failure(Op::Agents, e),
                },
                Effect::Search { query } => match this.fetch_search(&query).await {
                    Ok(rows) => Msg::SearchLoaded(rows),
                    Err(e) => failure(Op::Search, e),
                },
                Effect::RefreshReceipts => match this.fetch_receipts().await {
                    Ok(counts) => Msg::ReceiptsLoaded(counts),
                    Err(e) => failure(Op::Receipts, e),
                },
                Effect::RefreshHealth => Msg::HealthLoaded(this.health()),
                Effect::AckMessage { message_id } => match this.require_agent() {
                    Ok(agent) => match this
                        .mailbox
                        .acknowledge_message(&this.project, agent, &message_id)
                        .await
                    {
                        Ok(_) => Msg::ReceiptWritten {
                            corr: message_id,
                            verb: "ack",
                        },
                        Err(e) => failure(Op::Receipt, e),
                    },
                    Err(e) => failure(Op::Receipt, e),
                },
                Effect::ReadMessage { message_id } => match this.require_agent() {
                    Ok(agent) => match this
                        .mailbox
                        .mark_message_read(&this.project, agent, &message_id)
                        .await
                    {
                        Ok(_) => Msg::ReceiptWritten {
                            corr: message_id,
                            verb: "read",
                        },
                        Err(e) => failure(Op::Receipt, e),
                    },
                    Err(e) => failure(Op::Receipt, e),
                },
                Effect::SendMail(composed) => match this.require_agent() {
                    Ok(agent) => match composed_send(&this.project, agent, &composed) {
                        Ok(msg) => match this.mailbox.send_message(&msg).await {
                            Ok(result) => Msg::MailSent {
                                to: msg.to.join(", "),
                                message_id: result
                                    .deliveries
                                    .first()
                                    .map(|d| d.payload.event_id.clone())
                                    .unwrap_or_default(),
                                thread: msg.thread_id.clone(),
                            },
                            Err(e) => failure(Op::Send, e),
                        },
                        Err(e) => failure(Op::Send, e),
                    },
                    Err(e) => failure(Op::Send, e),
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

/// Turn one composed message into the mailbox's `SendMessage`.
///
/// The console does NOT wrap the message itself: this produces the same value
/// the `mail` verb does and hands it to `MailSurface::send_message`, so the
/// metadata-only kind-9 wake ring (one per attempted recipient, `mail-delivered`)
/// is emitted by the send path rather than remembered here. A free function
/// because the test hands this struct to a REAL mailbox and checks the ring.
pub fn composed_send(
    project: &str,
    sender: &str,
    composed: &ComposedSend,
) -> anyhow::Result<SendMessage> {
    let extra_tags = crate::robot::build_send_tags(
        None,
        Some(composed.wake.to_string()),
        None,
        None,
        None,
        None,
    )?;
    Ok(SendMessage {
        project_slug: project.to_string(),
        sender_name: sender.to_string(),
        to: composed.to.clone(),
        subject: composed.subject.clone(),
        body_md: composed.body.clone(),
        cc: Vec::new(),
        bcc: Vec::new(),
        importance: Some(composed.importance.to_string()),
        ack_required: composed.ack,
        thread_id: composed.thread.clone(),
        sender_token: None,
        overseer: false,
        extra_tags,
    })
}

/// Which operation an effect belongs to (the typed-failure label).
pub const fn effect_op(effect: &Effect) -> Op {
    match effect {
        Effect::RefreshInbox => Op::Inbox,
        Effect::RefreshThread { .. } => Op::Thread,
        Effect::RefreshChannels => Op::Channels,
        Effect::RefreshChannelPosts { .. } => Op::ChannelPosts,
        Effect::RefreshAgents => Op::Agents,
        Effect::Search { .. } => Op::Search,
        Effect::RefreshReceipts => Op::Receipts,
        Effect::RefreshHealth => Op::Health,
        Effect::AckMessage { .. } | Effect::ReadMessage { .. } => Op::Receipt,
        Effect::SendMail { .. } => Op::Send,
        Effect::Quit => Op::Health,
    }
}

/// Operator-facing failure message (the context chain, never a bare Debug).
fn failure(op: Op, e: anyhow::Error) -> Msg {
    Msg::Failed {
        op,
        message: format!("{e:#}"),
    }
}

/// Map one inbox row.
fn mail_row(m: InboxMessage, acked: &std::collections::HashSet<String>) -> MailRow {
    let corr = m.corr.as_ref().map(|c| c.as_str().to_string());
    let is_acked = corr.as_ref().is_some_and(|c| acked.contains(c));
    let body = (!m.body_md.trim().is_empty()).then(|| m.body_md.clone());
    MailRow {
        id: m.id,
        thread_id: m.thread_id.unwrap_or_default(),
        sender: m.sender,
        subject: m.subject,
        importance: m.importance,
        ack_required: m.ack_required,
        acked: is_acked,
        read: m.read_ts_us.is_some(),
        at: micros_to_secs(m.created_ts_us),
        body,
        state_known: true,
    }
}

/// Map one search hit. The result carries no corr and no read stamp, so the
/// read/ack state is UNKNOWN (`state_known: false`): the row shows no unread
/// dot and no owed ack instead of inventing either. Joining the receipt store
/// is impossible here — there is no corr on the row to join against.
fn search_row(m: SearchMessage) -> MailRow {
    MailRow {
        id: m.id,
        thread_id: m.thread_id.unwrap_or_default(),
        sender: m.from,
        subject: m.subject,
        importance: m.importance,
        ack_required: m.ack_required,
        acked: false,
        read: false,
        at: micros_to_secs(m.created_ts_us),
        body: m.body_md,
        state_known: false,
    }
}

/// µs epoch → seconds (the UI never needs sub-second precision).
fn micros_to_secs(us: i64) -> u64 {
    (us.max(0) as u64) / 1_000_000
}

/// The presence state strings, normalized at the adapter edge (ADR-0031 A4):
/// an unknown dialect word becomes `Unknown`, never a sixth state.
pub fn parse_presence_state(raw: &str) -> PresenceState {
    match raw.trim().to_ascii_lowercase().as_str() {
        "idle" => PresenceState::Idle,
        "working" => PresenceState::Working,
        "blocked" => PresenceState::Blocked,
        "done" => PresenceState::Done,
        _ => PresenceState::Unknown,
    }
}

/// Short npub for dense cards (`npub1abc…1234`).
pub fn short_npub(npub: &str) -> String {
    if npub.len() <= 14 {
        return npub.to_string();
    }
    let head: String = npub.chars().take(10).collect();
    let tail: String = npub
        .chars()
        .rev()
        .take(4)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

#[cfg(test)]
pub mod fake {
    //! A scripted host for the interpreter tests: no network, no store, no
    //! clock. `hang` never resolves — the runtime's timeout is what has to
    //! turn that into a typed failure.

    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{Msg, TuiHost, now_unix};
    use crate::tui::model::{ReceiptCounts, SettingsState};
    use crate::tui::msg::{Effect, Op};

    /// How a scripted host answers.
    #[derive(Debug, Clone, Default)]
    pub enum Reply {
        /// Answer with the canned message for the effect.
        #[default]
        Ok,
        /// Answer with a typed failure.
        Fail(String),
        /// Never answer (the runtime's timeout must fire).
        Hang,
        /// Answer after this long.
        After(Duration),
    }

    /// A host that records every effect it was asked to run.
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

    impl FakeHost {
        /// A host that answers `Ok` with canned rows.
        pub fn new() -> Self {
            Self::default()
        }

        /// A host that answers every effect with this reply.
        pub fn with_reply(reply: Reply) -> Self {
            let host = Self::default();
            *host.inner.reply.lock().expect("reply lock") = reply;
            host
        }

        /// Every effect this host has been asked to run, in order.
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

    /// The canned success message per effect (a fixed script, so the model it
    /// produces is deterministic).
    pub fn canned(effect: &Effect) -> Msg {
        match effect {
            Effect::RefreshInbox => Msg::InboxLoaded(vec![crate::tui::model::MailRow {
                id: "m1".into(),
                thread_id: "t1".into(),
                sender: "bee".into(),
                subject: "landed".into(),
                importance: "normal".into(),
                ack_required: false,
                acked: false,
                read: false,
                state_known: true,
                at: 1_700_000_000,
                body: None,
            }]),
            Effect::RefreshThread { thread_id } => Msg::ThreadLoaded {
                thread_id: thread_id.clone(),
                rows: vec![crate::tui::model::MailRow {
                    id: "m1".into(),
                    thread_id: thread_id.clone(),
                    sender: "bee".into(),
                    subject: "landed".into(),
                    importance: "normal".into(),
                    ack_required: false,
                    acked: false,
                    read: true,
                    state_known: true,
                    at: 1_700_000_000,
                    body: Some("body".into()),
                }],
            },
            Effect::RefreshChannels => Msg::ChannelsLoaded(vec![crate::tui::model::ChannelRow {
                id: "ch".into(),
                name: "general".into(),
                joined: true,
                members: 2,
                ..Default::default()
            }]),
            Effect::RefreshChannelPosts { channel_id } => Msg::ChannelPostsLoaded {
                channel_id: channel_id.clone(),
                rows: Vec::new(),
            },
            Effect::RefreshAgents => Msg::AgentsLoaded(vec![crate::tui::model::AgentRow {
                name: "aye".into(),
                npub: "npub1live".into(),
                online: true,
                state: toron_core::nostr::presence::PresenceState::Working,
                ..Default::default()
            }]),
            Effect::Search { .. } => Msg::SearchLoaded(Vec::new()),
            Effect::RefreshReceipts => Msg::ReceiptsLoaded(ReceiptCounts::default()),
            Effect::RefreshHealth => Msg::HealthLoaded(Vec::new()),
            Effect::AckMessage { message_id } | Effect::ReadMessage { message_id } => {
                Msg::ReceiptWritten {
                    corr: message_id.clone(),
                    verb: "ack",
                }
            }
            Effect::SendMail(composed) => Msg::MailSent {
                to: composed.to.join(", "),
                message_id: "e1".into(),
                thread: composed.thread.clone(),
            },
            Effect::Quit => Msg::Inflight(0),
        }
    }

    /// The op label of an effect (mirrors the live host's mapping).
    pub fn op_of(effect: &Effect) -> Op {
        super::effect_op(effect)
    }
}
