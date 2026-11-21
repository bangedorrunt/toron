// governed-by: ADR-0025
// governed-by: ADR-0003
// governed-by: ADR-0004 (MailOverseerArgs — Ship 5)

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// toron: a Nostr-native agent mailbox.
///
/// Two personas from one surface:
///   - Operators: daemon lifecycle, keys, projects, relays, backend, doctor.
///   - Agents/scripts (robot mode): the agent-mailbox tool surface (AM-Rust
///     lineage; the live spec is `toron catalog --json`, 38 parity + 3 additive).
///
/// Examples
/// --------
///   toron serve --mcp-stdio
///   toron serve --mcp-http 127.0.0.1:8000
///   toron status
///   toron status --json
#[derive(Debug, Parser)]
#[command(name = "toron", version, about)]
pub struct Cli {
    #[command(flatten)]
    pub globals: Globals,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Global flags (clig.dev: flags > env > project config > user config > system).
#[derive(Debug, Args, Clone, Default)]
pub struct Globals {
    /// Structured JSON output (all read commands).
    #[arg(long, global = true)]
    pub json: bool,

    /// Flat tabular text for grep/awk.
    #[arg(long, global = true)]
    pub plain: bool,

    /// Disable color (also: NO_COLOR, TERM=dumb, piped).
    #[arg(long, global = true)]
    pub no_color: bool,

    /// Less output.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Debug/verbose to stderr.
    #[arg(short, long, global = true)]
    pub debug: bool,

    /// Show changes without running.
    #[arg(short = 'n', long, global = true)]
    pub dry_run: bool,

    /// Override config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Output to file (commands that write artifacts).
    #[arg(short, long, global = true)]
    pub output: Option<PathBuf>,

    /// Emit a full robot state snapshot (JSON) and exit.
    #[arg(long, global = true)]
    pub robot_snapshot: bool,

    /// Emit a minimal robot status (JSON) and exit.
    #[arg(long, global = true)]
    pub robot_status: bool,

    /// Emit the robot capability/role map (JSON) and exit.
    #[arg(long, global = true)]
    pub robot_capabilities: bool,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the toron daemon (mailbox + MCP server).
    Serve(ServeArgs),

    /// Daemon lifecycle: start/stop/restart/status/install/uninstall.
    Daemon(DaemonArgs),

    /// Quick health/config overview (human by default, --json for machines).
    Status,

    /// Emit the MCP surface catalog (frozen 38 parity tools, 25 resources, CLI
    /// commands). The live surface adds 3 tools; `catalog --json` is the spec.
    /// The toron.dev catalog contract (ADR-0002 D3).
    Catalog,

    /// Show the effective configuration (flags > env > project > user > system).
    Config(ConfigArgs),

    /// Mail: send/reply/inbox/ack/read/search/thread-summarize (MCP parity JSON).
    #[command(subcommand)]
    Mail(MailCommand),

    /// Ack-SLA scan+publish: emit one `ack-sla-expired` wake per overdue
    /// ack-required post and print the expired ids. CLI-only (no new MCP
    /// tool, ADR-0020 wave 1 freeze).
    AckSla(AckSlaArgs),

    /// Events principals (ADR-0039): operator-only bearer token management.
    #[command(subcommand)]
    Events(EventsCommand),

    /// File reservations: acquire/conflicts/list/renew/release/release-by-reason/sweep/force-release.
    #[command(subcommand)]
    Reserve(ReserveCommand),

    /// Build slots: acquire/renew/release.
    #[command(subcommand)]
    Slot(SlotCommand),

    /// Contacts: request/respond/list/policy.
    #[command(subcommand)]
    Contact(ContactCommand),

    /// Pre-commit guard: install/uninstall.
    #[command(subcommand)]
    Guard(GuardCommand),

    /// Agent identity: one-step session bootstrap (register/upsert + reserve).
    #[command(subcommand)]
    Agent(AgentCommand),

    /// NIP-29 channels: create/join/leave/close/purge/list/members/post/read (39000/39002 + kind 9)
    /// (plane S; lifecycle open -> closed -> purged).
    #[command(subcommand)]
    Channel(ChannelCommand),

    /// Job lifecycle 43001-43006: list/show/claim/progress/result/error/cancel.
    #[command(subcommand)]
    Job(JobCommand),

    /// React (kind 7) to a channel post.
    React(ReactArgs),

    /// Presence (kind 20001): publish/list (plane N).
    #[command(subcommand)]
    Presence(PresenceCommand),

    /// Watch the shared plane (NIP-01 REQ): --once exits on first match.
    Watch(WatchArgs),

    /// Wait until a receipt appears (subscribe-until-receipt).
    Wait(WaitArgs),

    /// Deep health checks (hc/git/keys/engine store). --check exits 3 on any failure.
    Doctor(DoctorArgs),

    /// In-process operator counters (tool calls, errors, latency).
    Metrics,

    /// NIP-49 key management: generate + inspect (never prints the nsec).
    #[command(subcommand)]
    Key(KeyCommand),

    /// Project lifecycle: ensure (idempotent manifest) + list.
    #[command(subcommand)]
    Project(ProjectCommand),

    /// Relay federation config: list/add/remove/info (config-backed).
    #[command(subcommand)]
    Relay(RelayCommand),

    /// Backend selection: list/status/default.
    #[command(subcommand)]
    Backend(BackendCommand),

    /// Bench a mailbox scenario (startup/inbox/send/search/threads): live
    /// latency percentiles over an in-process bus, not a parity check.
    Bench(BenchArgs),

    /// Operator console (ratatui): mail, channels, agents, dashboard,
    /// settings — live against the project's one local store.
    Tui(TuiArgs),

    /// Workflow (ADR-0005 Spec 5): lint/validate/run/invoke/dry-run a single
    /// workflow file — the fabric (standalone) surface. Orchestration
    /// (catalogs/cron/crews) lives in flywheel.
    #[command(subcommand)]
    Workflow(WorkflowCommand),

    /// Receipts durable store (ADR-0027 E2): `receipt get` / `receipt list` / `receipt set`.
    #[command(subcommand)]
    Receipt(ReceiptCommand),

    /// Role binding via 30013 role= tags + alias (ADR-0027 E4).
    #[command(subcommand)]
    Role(RoleCommand),

    /// Run-state checkpoints (ADR-0035): read one boundary's model and its
    /// pending-write set, or render what a fork from it WOULD do. Read-only
    /// and CLI-only: nothing here seeds a live model from the log.
    #[command(subcommand)]
    RunState(RunStateCommand),

    /// Read your inbox past your cursor (commits cursor + Delivered in one
    /// transaction; --peek commits neither).
    Next(NextArgs),

    /// Accept or refuse a delivered message (Delivered -> Acknowledged|Rejected).
    Ack(AckArgs),

    /// Finish a task you hold, with evidence (Acknowledged -> Acted).
    Done(DoneArgs),

    /// Inbox plumbing: `append` (fixture door, one envelope straight to the
    /// log), `send` (journal first through the outbox loop, with recovery) and
    /// `task` (every envelope of a task you sent or received).
    #[command(subcommand)]
    Inbox(InboxCommand),
}

/// `toron next` (ADR-0041).
#[derive(Debug, clap::Args)]
pub struct NextArgs {
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Reader: agent name in the project, or a 64-hex npub.
    #[arg(long = "as", required = true)]
    pub who: String,
    /// Stream to read (defaults to the reader's own npub). Another npub's
    /// stream is refused (not_recipient).
    #[arg(long)]
    pub stream: Option<String>,
    /// Show without committing the cursor or Delivered.
    #[arg(long)]
    pub peek: bool,
    /// With `--peek`, exit 0 and print `urgent` when a `steer@1` priority
    /// field is `urgent`. Exit 1 when none are pending. The body is not
    /// printed. Without `--peek` this flag is a usage error.
    #[arg(long)]
    pub urgent: bool,
}

/// `toron ack` (ADR-0041).
#[derive(Debug, clap::Args)]
pub struct AckArgs {
    /// Message key `producer:epoch:seq` (from `toron next`).
    pub msg: String,
    /// accept | reject
    #[arg(value_parser = ["accept", "reject"])]
    pub verdict: String,
    /// Reason (required for reject).
    #[arg(long)]
    pub reason: Option<String>,
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Recipient: agent name or 64-hex npub.
    #[arg(long = "as", required = true)]
    pub who: String,
}

/// `toron done` (ADR-0041).
#[derive(Debug, clap::Args)]
pub struct DoneArgs {
    /// Task id carried on the envelope.
    pub task: String,
    /// Evidence (repeatable): paths, commit SHAs, test output refs.
    #[arg(long, required = true)]
    pub evidence: Vec<String>,
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Holder: agent name or 64-hex npub.
    #[arg(long = "as", required = true)]
    pub who: String,
}

/// `toron inbox` (ADR-0041).
#[derive(Debug, Subcommand)]
pub enum InboxCommand {
    /// Append one envelope JSON file to the inbox log (validated; same
    /// msg id twice replays the same offset).
    Append(InboxAppendArgs),
    /// Send through the outbox loop: journal the intent, then append; first
    /// reconciles any of this sender's intents a crashed run left behind.
    /// Without --file it only recovers.
    Send(InboxSendArgs),
    /// Every envelope of a task you sent or received, oldest first, with
    /// its delivery phase (read-only).
    Task(InboxTaskArgs),
    /// One stored envelope by its `producer:epoch:seq` key, even after its
    /// epoch has been fenced (read-only).
    Msg(InboxMsgArgs),
    /// The pin's committed pull cursor as rumor-time micros (read-only):
    /// the high-water every `next` and `since last` read advances, which the
    /// doorbell ring guard takes as the one notion of consumed (bd-yc6jf).
    /// Missing row = 0.
    Cursor(InboxCursorArgs),
}

#[derive(Debug, clap::Args)]
pub struct InboxMsgArgs {
    /// Message key `producer:epoch:seq`.
    pub key: String,
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Viewer: agent name or 64-hex npub. Only a message it sent or
    /// received is shown.
    #[arg(long = "as", required = true)]
    pub who: String,
}

#[derive(Debug, clap::Args)]
pub struct InboxCursorArgs {
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Reader: agent name whose committed cursor is served. A single
    /// rumor-time timestamp, not mail content.
    #[arg(long = "as", required = true)]
    pub who: String,
}

#[derive(Debug, clap::Args)]
pub struct InboxTaskArgs {
    /// Task id carried on the envelope.
    pub task: String,
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Viewer: agent name or 64-hex npub. Only messages it sent or
    /// received are shown.
    #[arg(long = "as", required = true)]
    pub who: String,
}

#[derive(Debug, clap::Args)]
pub struct InboxSendArgs {
    /// Envelope JSON file (`-` for stdin); omit to only recover.
    #[arg(long)]
    pub file: Option<String>,
    /// Project slug (used to resolve --as).
    #[arg(long, required = true)]
    pub project: String,
    /// Sender: agent name or 64-hex npub (must be the envelope's `from`).
    #[arg(long = "as", required = true)]
    pub who: String,
}

#[derive(Debug, clap::Args)]
pub struct InboxAppendArgs {
    /// Envelope JSON file (`-` for stdin).
    #[arg(long, required = true)]
    pub file: String,
}

/// `toron run-state` (ADR-0035, spec S8). Read-only half of the checkpoint
/// store: `replay` renders a boundary, `fork` renders a plan.
///
/// No `--project` on purpose. The checkpoint table is keyed by
/// `(thread_id, seq)` and carries no project column, so a `--project` filter
/// here would read as a scope it does not enforce. `thread` IS the unit.
#[derive(Debug, Subcommand)]
pub enum RunStateCommand {
    /// Print one boundary's model and the effect keys it owned. Exact: a
    /// boundary that does not exist is an error, never a nearby one.
    Replay(RunStateBoundaryArgs),

    /// Render what a fork from a boundary WOULD do. Dry by default and
    /// refusing to install: seeding a live model is the live host's call.
    Fork(RunStateBoundaryArgs),
}

#[derive(Debug, clap::Args)]
pub struct RunStateBoundaryArgs {
    /// Thread the boundaries belong to, as recorded by the recorder.
    #[arg(long, required = true)]
    pub thread: String,
    /// Boundary sequence number, as recorded by the recorder.
    #[arg(long, required = true)]
    pub seq: i64,
}

/// toron workflow (fabric) subcommands (ADR-0005 Spec 5).
#[derive(Debug, clap::Subcommand)]
pub enum WorkflowCommand {
    /// Static validation: schema + guard syntax + template refs + graph
    /// acyclicity. Exit 0 clean, 1 on any blocking issue, 2 on parse failure.
    Lint {
        /// Workflow YAML file.
        file: std::path::PathBuf,
    },
    /// Alias for `workflow lint`.
    Validate {
        /// Workflow YAML file.
        file: std::path::PathBuf,
    },
    /// Run a workflow from a trigger event payload.
    Run {
        /// Workflow YAML file.
        file: std::path::PathBuf,
        /// Trigger payload JSON (the `{{trigger.*}}` context).
        #[arg(long, default_value = "{}")]
        trigger: String,
    },
    /// Invoke a workflow on-demand with inputs (no trigger event).
    Invoke {
        /// Workflow YAML file.
        file: std::path::PathBuf,
        /// Inputs JSON (the `{{inputs.x}}` context).
        #[arg(long, default_value = "{}")]
        inputs: String,
    },
    /// Lint + show what would run without executing.
    DryRun {
        /// Workflow YAML file.
        file: std::path::PathBuf,
        /// Trigger payload JSON.
        #[arg(long, default_value = "{}")]
        trigger: String,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum ReceiptCommand {
    /// Get one receipt by corr (reads the durable sqlite store).
    Get(ReceiptGetArgs),
    /// List receipts, optionally filtered by state.
    List(ReceiptListArgs),
    /// Set receipt state to resulted (explicit verb, ADR-0027 E2.3).
    Set(ReceiptSetArgs),
}

#[derive(Debug, Args)]
pub struct ReceiptGetArgs {
    /// Correlation id.
    #[arg(long, required = true)]
    pub corr: String,
}

#[derive(Debug, Args)]
pub struct ReceiptListArgs {
    /// Filter by state: owed|acked|resulted.
    #[arg(long)]
    pub state: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReceiptSetArgs {
    /// Correlation id.
    #[arg(long, required = true)]
    pub corr: String,
    /// Target state — only `resulted` is accepted (explicit verb).
    #[arg(long, required = true)]
    pub state: String,
    /// Optional result path to persist with the receipt.
    #[arg(long)]
    pub result_path: Option<String>,
}

#[derive(Debug, clap::Subcommand)]
pub enum RoleCommand {
    /// Pin a role to a registered agent (alias write, 30013 role= tag).
    Pin(RolePinArgs),
    /// List role → pin aliases for a project.
    List(RoleListArgs),
    /// Clear a role alias.
    Clear(RoleClearArgs),
}

#[derive(Debug, Args)]
pub struct RolePinArgs {
    /// Role name (e.g. wheelhouse).
    pub role: String,
    /// Registered pin (adjective+noun) that holds the role.
    pub pin: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct RoleListArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct RoleClearArgs {
    /// Role name to clear.
    pub role: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    /// Print the annotated sample configuration to stdout.
    #[arg(long)]
    pub sample: bool,
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Serve the MCP endpoint over stdio (child-process / embedded mode).
    #[arg(long, conflicts_with = "mcp_http")]
    pub mcp_stdio: bool,

    /// Serve the MCP endpoint over Streamable HTTP at this address (e.g. 127.0.0.1:8000).
    #[arg(long)]
    pub mcp_http: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    /// Report daemon state (running/stopped + detection source).
    Status,
    /// Start the daemon (installs the autostart unit first if absent).
    Start,
    /// Stop the daemon.
    Stop,
    /// Restart the daemon (stop, then start).
    Restart,
    /// Install the launchd (macOS) / systemd (Linux) autostart unit +
    /// passphrase file, and load the unit.
    Install,
    /// Stop the daemon and remove the autostart unit.
    Uninstall,
}

#[derive(Debug, Args)]
pub struct DaemonArgs {
    /// MCP-over-HTTP listen address for the daemon (defaults to the
    /// flywheel `[mail.toron]` address).
    #[arg(long, default_value = crate::daemon::DEFAULT_ADDR)]
    pub addr: String,

    /// launchd/systemd unit label.
    #[arg(long, default_value = "com.toron.serve")]
    pub label: String,

    /// State dir for the pidfile + logs (default: `$XDG_STATE_HOME/toron`,
    /// fallback `~/.local/state/toron`).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Kill an UNMANAGED process that holds the daemon port (stop/restart
    /// only). Without it, stop/restart refuse when a stray `toron serve`
    /// owns the address — the launchd replacement would crash-loop on bind.
    #[arg(long)]
    pub force: bool,

    #[command(subcommand)]
    pub cmd: DaemonCommand,
}

/// Contact policy selector (parity: open|auto|contacts_only|block_all).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PolicyArg {
    Open,
    Auto,
    #[value(name = "contacts_only")]
    ContactsOnly,
    BlockAll,
}

#[derive(Debug, Subcommand)]
pub enum MailCommand {
    /// Send a NIP-17 gift-wrapped message to one or more recipients.
    Send(MailSendArgs),
    /// Operator Overseer override (ADR-0004 Ship 5): signs with the operator
    /// key, sets importance=high, tags `toron:overseer`, and SKIPS contact-
    /// policy enforcement at the projection (block_all cannot block the
    /// operator). Requires an operator key.
    Overseer(MailOverseerArgs),
    /// Reply within a thread (requires --thread).
    Reply(MailReplyArgs),
    /// Project the agent's inbox (rumor-time order).
    Inbox(MailInboxArgs),
    /// Acknowledge a message (private, idempotent).
    Ack(MailReceiptArgs),
    /// Mark a message read (private, idempotent).
    Read(MailReceiptArgs),
    /// FTS5 search over a project's indexed messages.
    Search(MailSearchArgs),
    /// Deterministic digest of a thread.
    ThreadSummarize(MailThreadSummarizeArgs),
    /// Resolve a name/alias/pin to its identity (npub + profile fields).
    /// bd-captain-alias-0g8a: `mail whois <project> captain` hits the
    /// orchestrator pin once the alias is set.
    Whois(MailWhoisArgs),
}

/// bd-captain-alias-0g8a: CLI noun for the shared whois resolver.
#[derive(Debug, Args)]
pub struct MailWhoisArgs {
    /// Project slug.
    pub project: String,
    /// Identity to resolve (canonical name, adj+noun pin, folded alias, or npub).
    pub name: String,
}

#[derive(Debug, Args)]
pub struct MailSendArgs {
    /// Sender agent name (must have a local key in the project).
    #[arg(long = "as", default_value = "agent")]
    pub from: String,
    /// Recipients (comma-separated).
    #[arg(long, value_delimiter = ',', required = true)]
    pub to: Vec<String>,
    /// Message subject (NIP-14).
    #[arg(long, required = true)]
    pub subject: String,
    /// Message body (markdown).
    #[arg(long, required = true)]
    pub body: String,
    /// Copy recipients (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub cc: Vec<String>,
    /// Blind-copy recipients (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub bcc: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Importance tag (e.g. normal|high|urgent).
    #[arg(long)]
    pub importance: Option<String>,
    /// Request an acknowledgement from recipients.
    #[arg(long)]
    pub ack: bool,
    /// Reply within this thread (original message id).
    #[arg(long)]
    pub thread: Option<String>,
    /// Sender token (verification).
    #[arg(long)]
    pub sender_token: Option<String>,
    /// Correlation id for the assignment (ADR-0027 E2: rides extra_tags corr).
    #[arg(long)]
    pub corr: Option<String>,
    /// ADR-0010 D9 (bd-l0fz): the work-ledger bead this send resolves — the
    /// structural done-hold. Stamps `bead=<id>` on the rumor; the flywheel
    /// dispatch guard matches THIS tag, never the `[{id}] done` subject.
    #[arg(long = "resolve-bead")]
    pub resolve_bead: Option<String>,
    /// Wake kind: standing|tasked (ADR-0027 E2/E3).
    #[arg(long)]
    pub wake: Option<String>,
    /// Priority: queue|interrupt (ADR-0027 E2/E3; interrupt is operator/user only).
    #[arg(long)]
    pub priority: Option<String>,
    /// Audience: human|agents (ADR-0027 E3; human fan-out >1 is hard error).
    #[arg(long)]
    pub audience: Option<String>,
    /// Result path for the assignment (ADR-0027 E2).
    #[arg(long = "result-path")]
    pub result_path: Option<String>,
    /// HumanDecision verdict: approve|deny (ADR-0028 E4; rides extra_tags verdict).
    #[arg(long)]
    pub verdict: Option<String>,
}

/// Operator Overseer override args (ADR-0004 Ship 5).
#[derive(Debug, Args)]
pub struct MailOverseerArgs {
    /// Recipients (comma-separated agent names).
    #[arg(long, value_delimiter = ',', required = true)]
    pub to: Vec<String>,
    /// Message body (markdown). The operator preamble is prepended.
    #[arg(long, required = true)]
    pub body: String,
    /// Message subject (NIP-14). Default: an operator-override marker.
    #[arg(long)]
    pub subject: Option<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct MailReplyArgs {
    /// Sender agent name (must have a local key in the project).
    #[arg(long = "as", default_value = "agent")]
    pub from: String,
    /// Recipients (comma-separated).
    #[arg(long, value_delimiter = ',', required = true)]
    pub to: Vec<String>,
    /// Message subject (NIP-14).
    #[arg(long, required = true)]
    pub subject: String,
    /// Message body (markdown).
    #[arg(long, required = true)]
    pub body: String,
    /// Copy recipients (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub cc: Vec<String>,
    /// Blind-copy recipients (comma-separated).
    #[arg(long, value_delimiter = ',')]
    pub bcc: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Importance tag (e.g. normal|high|urgent).
    #[arg(long)]
    pub importance: Option<String>,
    /// Request an acknowledgement from recipients.
    #[arg(long)]
    pub ack: bool,
    /// Thread to reply within (original message id; required).
    #[arg(long, required = true)]
    pub thread: String,
    /// Sender token (verification).
    #[arg(long)]
    pub sender_token: Option<String>,
    /// Correlation id (ADR-0027 E2).
    #[arg(long)]
    pub corr: Option<String>,
    /// Wake kind: standing|tasked.
    #[arg(long)]
    pub wake: Option<String>,
    /// Priority: queue|interrupt.
    #[arg(long)]
    pub priority: Option<String>,
    /// Audience: human|agents.
    #[arg(long)]
    pub audience: Option<String>,
    /// Result path.
    #[arg(long = "result-path")]
    pub result_path: Option<String>,
}

#[derive(Debug, Args)]
pub struct MailInboxArgs {
    /// Agent whose inbox to project.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Unread messages only (documented no-op: the inbox projection has no
    /// read state; poll with --since instead, per ADR-0018/bd-pin-public-plane-s-easy.3).
    #[arg(long)]
    pub unread: bool,
    /// Maximum messages to return.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Include message bodies.
    #[arg(long)]
    pub bodies: bool,
    /// Keep only messages in this thread (bd-harvest-thread-filter-yp9r).
    #[arg(long)]
    pub thread_id: Option<String>,
    /// Only messages strictly newer than this RFC3339 timestamp (e.g.
    /// 2026-08-18T00:00:00Z), a relative offset (30s, 15m, 6h, 7d),
    /// or the literal `last` (bd-since-cursor-70cf): the daemon-side high-water
    /// cursor, read then atomically bumped — crash-proof incremental polling.
    /// A malformed value is an error, never ignored.
    #[arg(long)]
    pub since: Option<String>,
}

#[derive(Debug, Args)]
pub struct MailReceiptArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent sending the receipt.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Message id.
    #[arg(long, required = true)]
    pub id: String,
}

#[derive(Debug, Args)]
pub struct MailSearchArgs {
    /// FTS5 MATCH query (porter/unicode61/prefix).
    #[arg(long, required = true)]
    pub query: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Maximum results (default 100, capped 1000).
    #[arg(long)]
    pub limit: Option<usize>,
    /// Ranking: relevance (bm25) or recency.
    #[arg(long)]
    pub ranking: Option<String>,
    /// Filter by sender name.
    #[arg(long)]
    pub sender: Option<String>,
    /// Filter by importance.
    #[arg(long)]
    pub importance: Option<String>,
    /// Filter by thread id.
    #[arg(long)]
    pub thread: Option<String>,
    /// Include messages created at/after this µs timestamp.
    #[arg(long)]
    pub since: Option<i64>,
    /// Include messages created at/before this µs timestamp.
    #[arg(long)]
    pub until: Option<i64>,
    /// Include message bodies.
    #[arg(long)]
    pub bodies: bool,
}

#[derive(Debug, Args)]
pub struct MailThreadSummarizeArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Thread id.
    #[arg(long, required = true)]
    pub thread: String,
    /// Include example messages (LLM summary).
    #[arg(long)]
    pub examples: bool,
    /// Use the LLM digest mode.
    #[arg(long)]
    pub llm_mode: bool,
    /// LLM model id (llm_mode only).
    #[arg(long)]
    pub llm_model: Option<String>,
}

/// Args for `toron ack-sla` (bd-0zcr.2 G1): scan+publish overdue ack wakes.
#[derive(Debug, Args)]
pub struct AckSlaArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Ack SLA in seconds (default 1800: posts older than this with
    /// ack_required + high/urgent importance and no receipt get a wake).
    #[arg(long)]
    pub sla_secs: Option<u64>,
}

#[derive(Debug, Subcommand)]
pub enum EventsCommand {
    /// Bearer tokens for events/* (CLI-only; no new MCP tool, ADR-0039 D6).
    #[command(subcommand)]
    Token(TokenCommand),
}

#[derive(Debug, Subcommand)]
pub enum TokenCommand {
    /// Mint a token; the plaintext is printed ONCE and never stored.
    Mint(TokenMintArgs),
    /// List token hashes and their principals (never plaintext).
    List,
    /// Revoke a token by hash; its next resolve fails.
    Revoke(TokenRevokeArgs),
}

#[derive(Debug, Args)]
pub struct TokenMintArgs {
    /// Mint an operator token (cross-scope, full payload, no expiry).
    #[arg(long)]
    pub operator: bool,
    /// Pin name for a per-pin token (requires --project).
    #[arg(long)]
    pub pin: Option<String>,
    /// Project slug for a per-pin token (requires --pin).
    #[arg(long)]
    pub project: Option<String>,
    /// Token lifetime in seconds (pin tokens only, ≤24h).
    #[arg(long)]
    pub ttl_secs: Option<i64>,
}

#[derive(Debug, Args)]
pub struct TokenRevokeArgs {
    /// SHA-256 hash of the token (from mint or list).
    #[arg(long, required = true)]
    pub hash: String,
}

#[derive(Debug, Subcommand)]
pub enum ReserveCommand {
    /// Acquire exclusive/shared leases on path globs.
    Acquire(ReserveAcquireArgs),
    /// Symmetric-glob conflict report for paths vs other agents' reservations.
    Conflicts(ReserveConflictsArgs),
    /// Show the active lease board (the state the strict guard evaluates).
    List(ReserveListArgs),
    /// Extend the TTLs of the holder's active file reservations.
    Renew(ReserveRenewArgs),
    /// Release the holder's active file reservations (idempotent).
    Release(ReserveReleaseArgs),
    /// Release every reservation whose reason is the given bead id
    /// (release-on-bead-close; operator-signed, no staleness heuristic).
    ReleaseByReason(ReserveReleaseByReasonArgs),
    /// Release every lease whose reason bead is CLOSED (batch of
    /// release-by-reason; the closed set is read from the repo tracker).
    Sweep(ReserveSweepArgs),
    /// Record ONE structured stand-down marker for (bead, holder): "bead
    /// <id> is being worked by <holder>, stop resuming it". Idempotent per
    /// (bead, holder); the loop reads `reserve yields` and skips the bead
    /// while the holder's lease is live.
    Yield(ReserveYieldArgs),
    /// Show the project's yield markers, each flagged with whether its holder
    /// is live right now, plus the beads a resume must currently skip.
    Yields(ReserveYieldsArgs),
    /// Operator force-release of a stale reservation.
    ForceRelease(ReserveForceReleaseArgs),
    /// Ask the holder to release or defer a held reservation (ADR-0011
    /// Spec 6, plane P — the ask is a sealed 1:1, not a room post).
    RequestRelease(ReserveRequestReleaseArgs),
    /// Holder answers a request-release (agreed | denied | deferred:<secs>).
    Respond(ReserveRespondArgs),
    /// Transfer a held lease to another agent (release + re-acquire, same
    /// path, exclusive + reason preserved). Never "commit as me" — the
    /// target commits as itself. bd-pin-public-plane-s-easy.13.
    Transfer(ReserveTransferArgs),
    /// Run one command under exclusive leases: acquire, run, ALWAYS release
    /// (success, failure, signal); exit with the command's own code.
    /// bd-reserve-with-lease-2lvt.
    WithLease(ReserveWithLeaseArgs),
}

#[derive(Debug, Args)]
pub struct ReserveAcquireArgs {
    /// Path globs to reserve (comma-separated; e.g. 'src/**').
    #[arg(long, value_delimiter = ',', num_args = 1.., required = true)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent acquiring the leases.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Lease TTL in seconds (default 180; a hold is a heartbeat, not a deed).
    #[arg(long)]
    pub ttl_secs: Option<u64>,
    /// Exclusive leases block other agents.
    #[arg(long)]
    pub exclusive: bool,
    /// Lease reason.
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReserveConflictsArgs {
    /// Path globs to check (comma-separated).
    #[arg(long, value_delimiter = ',', num_args = 1.., required = true)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent performing the check.
    #[arg(long = "as", required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct ReserveListArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Only leases held by this exact agent pin.
    #[arg(long)]
    pub holder: Option<String>,
    /// Only leases whose path glob overlaps this path (symmetric fnmatch).
    #[arg(long)]
    pub path: Option<String>,
    /// Repository root whose `.beads/issues.jsonl` backs the stale audit
    /// (default: current directory; no tracker file = no annotation).
    #[arg(long)]
    pub repo: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct ReserveRenewArgs {
    /// Path globs to renew (comma-separated; empty = renew all of the agent's).
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent holding the leases.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Seconds to extend each lease's TTL (default: the reservation TTL,
    /// 180 s). A heartbeat call is therefore `reserve renew --project P --as X`.
    #[arg(long, default_value_t = toron_core::mailbox::reservations::DEFAULT_RESERVATION_TTL_SECS)]
    pub extend_secs: u64,
}

#[derive(Debug, Args)]
pub struct ReserveReleaseArgs {
    /// Path globs to release (comma-separated; empty = release all of the agent's).
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent holding the leases.
    #[arg(long = "as", required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct ReserveReleaseByReasonArgs {
    /// Bead id whose matching (reason == bead id) leases are revoked.
    pub bead_id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Operator agent performing the revocation.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Skip the NIP-17 notification to the previous holders (default: notify).
    #[arg(long)]
    pub no_notify: bool,
}

#[derive(Debug, Args)]
pub struct ReserveSweepArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Operator agent signing the revocations.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Sweep leases whose reason names a CLOSED bead. Required: it is the
    /// only sweep source. Bare, it reads `<repo>/.beads/issues.jsonl`; it also
    /// takes an explicit tracker path, which is the form the skill documents.
    #[arg(long, num_args = 0..=1, default_missing_value = "", value_name = "ISSUES")]
    pub closed_beads: Option<String>,
    /// Repository root holding `.beads/` (default: current directory).
    #[arg(long)]
    pub repo: Option<std::path::PathBuf>,
    /// Skip the NIP-17 notification to the previous holders (default: notify).
    #[arg(long)]
    pub no_notify: bool,
}

#[derive(Debug, Args)]
pub struct ReserveYieldArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// The pin yielding the bead (the pane that was resumed and declined).
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Bead id being yielded.
    #[arg(long = "bead", required = true)]
    pub bead_id: String,
    /// The pin already working the bead (whose live lease keeps it suppressed).
    #[arg(long, required = true)]
    pub holder: String,
    /// Why the bead is being yielded (free text, stored verbatim).
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReserveYieldsArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct ReserveForceReleaseArgs {
    /// Path (or glob) of the stale reservation to revoke. The daemon resolves
    /// the active lease on this path itself (bd-cumj).
    #[arg(long, required = true)]
    pub path: String,
    /// Holder agent name to disambiguate (optional).
    #[arg(long)]
    pub holder: Option<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Requestor agent (the operator's tool user).
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Note attached to the revocation.
    #[arg(long)]
    pub note: Option<String>,
    /// Notify the previous holder via NIP-17.
    #[arg(long)]
    pub notify_previous: bool,
}

#[derive(Debug, Args)]
pub struct ReserveRequestReleaseArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// The requesting agent (crew A, the pin).
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// The holder being asked (crew B).
    #[arg(long, required = true)]
    pub target: String,
    /// Reservation id = the kind-30031 event id of the held lease.
    #[arg(long, required = true)]
    pub reservation_id: String,
    /// Ask: `release` or `defer:<secs>`.
    #[arg(long, default_value = "release")]
    pub ask: String,
    /// Note attached to the ask.
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReserveRespondArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// The responding holder (crew B, the pin).
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// The request's message id (from request-release output).
    #[arg(long, required = true)]
    pub request_id: String,
    /// Decision: `agreed` | `denied` | `deferred:<secs>`.
    #[arg(long, required = true)]
    pub decision: String,
    /// Note attached to the response.
    #[arg(long)]
    pub note: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReserveTransferArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// The current holder releasing the lease (the pin).
    #[arg(long = "as", required = true)]
    pub from: String,
    /// The target acquiring the lease (the pin).
    #[arg(long, required = true)]
    pub to: String,
    /// Path (or glob) whose lease moves.
    #[arg(long, required = true)]
    pub path: String,
    /// Explicit reason override (default: preserve the held lease's reason).
    #[arg(long)]
    pub reason: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReserveWithLeaseArgs {
    /// Path globs to hold for the command (comma-separated).
    #[arg(long, value_delimiter = ',', num_args = 1.., required = true)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent holding the leases (the pin).
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Lease reason (the bead id this scoped command belongs to). Required:
    /// a reason-less scoped lease is exactly the stale-lease litter this verb
    /// exists to end.
    #[arg(long, required = true)]
    pub reason: String,
    /// Seconds to keep polling while another pin holds the paths exclusively
    /// before refusing (0 = refuse on the first conflict).
    #[arg(long, default_value_t = 60)]
    pub wait_secs: u64,
    /// Lease TTL in seconds (default 180). The wrapper renews every half-TTL
    /// while the command runs and always releases after it, so the TTL bounds
    /// a CRASH, never a long command.
    #[arg(long)]
    pub ttl_secs: Option<u64>,
    /// The command to run while the leases are held (everything after `--`).
    #[arg(last = true, required = true, num_args = 1.., value_name = "CMD")]
    pub command: Vec<String>,
}

#[derive(Debug, Subcommand)]
pub enum AgentCommand {
    /// Register (upsert) an identity, then optionally reserve paths — the CLI
    /// onboarding path when `reserve acquire` fails on unknown identity.
    Bootstrap(AgentBootstrapArgs),
    /// bd-4a0u: purge the folded-slug leftovers `doctor` names — absorb the
    /// standalone `agents` row for every recorded fold alias whose canonical
    /// pin is registered (the second inbox stops existing). Idempotent; a row
    /// whose canonical target is unregistered is KEPT and reported.
    Prune(AgentPruneArgs),
    /// Rename an identity IN PLACE — the same key, the same npub, a new name.
    /// A name the pin convention forbids (a hyphenated slug) is unroutable:
    /// mail addressed to it bounces and its leases are unaddressable. This
    /// moves the identity onto a registered adjective+noun pin without
    /// minting a second key for the same words.
    Rename(AgentRenameArgs),
}

#[derive(Debug, Args)]
pub struct AgentRenameArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// The name to rename (the identity's CURRENT name).
    pub old_name: String,
    /// The new name: a registered adjective+noun pin.
    pub new_name: String,
}

#[derive(Debug, Args)]
pub struct AgentPruneArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Acting operator/pin, recorded in the report. No signing: this is a
    /// local roster cleanup (sqlite), never a signed event.
    #[arg(long = "as")]
    pub agent: Option<String>,
}

#[derive(Debug, Args)]
pub struct AgentBootstrapArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent pin to register.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Path globs to reserve (comma-separated; e.g. 'src/**'). Empty skips.
    #[arg(long, value_delimiter = ',')]
    pub paths: Vec<String>,
    /// Lease reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Exclusive leases block other agents.
    #[arg(long)]
    pub exclusive: bool,
    /// Lease TTL in seconds (default 180; a hold is a heartbeat, not a deed).
    #[arg(long)]
    pub ttl_secs: Option<u64>,
    /// Harness/program for the 30013 profile (optional).
    #[arg(long)]
    pub program: Option<String>,
    /// Model for the 30013 profile (optional).
    #[arg(long)]
    pub model: Option<String>,
    /// Task description for the 30013 profile (optional).
    #[arg(long)]
    pub task: Option<String>,
    /// Hc terminal to stamp with the mail identity (optional; defaults to
    /// $HC_TERMINAL_ID when set). Best-effort display-only metadata.
    #[arg(long = "pane-id")]
    pub pane_id: Option<String>,
    /// ADR-0037 W2: when the pin already holds a key in another project, the
    /// project whose key to reuse (required only when the pin holds more than
    /// one key). The CLI proves possession with that key file.
    #[arg(long = "key-from")]
    pub key_from: Option<String>,
}

#[derive(Debug, Subcommand)]
pub enum SlotCommand {
    /// Acquire an advisory build-slot lease, optionally exclusive.
    Acquire(SlotAcquireArgs),
    /// Extend an active build-slot lease's TTL.
    Renew(SlotRenewArgs),
    /// Release an active build-slot lease.
    Release(SlotReleaseArgs),
}

#[derive(Debug, Args)]
pub struct SlotAcquireArgs {
    /// Slot name (e.g. ci).
    #[arg(long, required = true)]
    pub slot: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent acquiring the slot.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Lease TTL in seconds (default 180; a hold is a heartbeat, not a deed).
    #[arg(long)]
    pub ttl_secs: Option<u64>,
    /// Exclusive lease (serializes against any holder).
    #[arg(long)]
    pub exclusive: bool,
}

#[derive(Debug, Args)]
pub struct SlotRenewArgs {
    /// Slot name (e.g. ci).
    #[arg(long, required = true)]
    pub slot: String,
    /// Seconds to extend the lease's TTL.
    #[arg(long, required = true)]
    pub extend_secs: u64,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent holding the slot.
    #[arg(long = "as", required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct SlotReleaseArgs {
    /// Slot name (e.g. ci).
    #[arg(long, required = true)]
    pub slot: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent holding the slot.
    #[arg(long = "as", required = true)]
    pub agent: String,
}

#[derive(Debug, Subcommand)]
pub enum ContactCommand {
    /// Request a contact link with another agent.
    Request(ContactRequestArgs),
    /// Approve or deny a pending contact request.
    Respond(ContactRespondArgs),
    /// List an agent's contact links.
    List(ContactListArgs),
    /// Set the agent's inbound contact policy.
    Policy(ContactPolicyArgs),
    /// Show or set the project's captain alias (bd-captain-alias-0g8a).
    /// With `--pin`, validates the pin is REGISTERED and folds `captain`
    /// onto it; without, prints the current fold.
    Captain(ContactCaptainArgs),
}

/// bd-captain-alias-0g8a: the routable captain address.
#[derive(Debug, Args)]
pub struct ContactCaptainArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Registered adj+noun pin to fold `captain` onto (omit to show current).
    #[arg(long, conflicts_with = "clear")]
    pub pin: Option<String>,
    /// Clear the captain alias for the project (bd-auqj).
    #[arg(long, conflicts_with = "pin")]
    pub clear: bool,
    /// Write the fold even in open identity mode, where it cannot route
    /// (bd-lsgm). Audited to stderr.
    #[arg(long, requires = "pin")]
    pub force: bool,
}

#[derive(Debug, Args)]
pub struct ContactRequestArgs {
    /// Peer agent name.
    #[arg(long, required = true)]
    pub to: String,
    /// Requesting agent name.
    #[arg(long = "as", default_value = "agent")]
    pub from: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Request reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Peer's project (defaults to --project).
    #[arg(long)]
    pub to_project: Option<String>,
}

#[derive(Debug, Args)]
pub struct ContactRespondArgs {
    /// Requesting agent name.
    #[arg(long = "as", required = true)]
    pub from: String,
    /// Responding agent name.
    #[arg(long, required = true)]
    pub to: String,
    /// Approve (true) or deny (false) the request.
    #[arg(long, required = true)]
    pub accept: bool,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Requester's project (defaults to --project).
    #[arg(long)]
    pub from_project: Option<String>,
}

#[derive(Debug, Args)]
pub struct ContactListArgs {
    /// Agent whose contact links to list.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct ContactPolicyArgs {
    /// Agent whose inbound policy to set.
    #[arg(long = "as", required = true)]
    pub agent: String,
    /// open|auto|contacts_only|block_all.
    #[arg(long, value_enum, required = true)]
    pub policy: PolicyArg,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Subcommand)]
pub enum GuardCommand {
    /// Install the toron pre-commit hook in a git repository.
    Install(GuardRepoArgs),
    /// Remove the toron-installed pre-commit hook (never clobbers a user hook).
    Uninstall(GuardRepoArgs),
    /// Check a commit's staged paths against active reservations. Exit 1
    /// (blocked) when a staged path is exclusively reserved by ANOTHER agent;
    /// exit 0 (allow) otherwise. Attribution: --as, else AGENT_NAME; an
    /// unattributed commit fails open (exit 0).
    ///
    /// `--strict` (bd-2dox sweep-guard): every staged path must be covered by
    /// the committer's OWN active reservation or the shared-safe bookkeeping
    /// list (.beads/issues.jsonl, .beads/beads.base.jsonl); an unattributed
    /// commit FAILS CLOSED (exit 1). Env TORON_GUARD_STRICT=0 disables strict
    /// for a session (operator escape hatch, logged); the installed hook
    /// exports TORON_GUARD_STRICT=1 by default.
    Check(GuardCheckArgs),
    /// Record a GitFact for HEAD to the flywheel fact journal (bd-jhes).
    /// Best-effort — ANY journal failure must NEVER block the commit (exit 0
    /// always). Called from the post-commit hook.
    Record(GuardRecordArgs),
}

#[derive(Debug, Args)]
pub struct GuardRepoArgs {
    /// Git repository path.
    #[arg(long, required = true)]
    pub repo: PathBuf,
    /// Project slug whose reservations the guard evaluates (baked into the hook).
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct GuardCheckArgs {
    /// Git repository path.
    #[arg(long, required = true)]
    pub repo: PathBuf,
    /// Project slug whose reservations are evaluated.
    #[arg(long, required = true)]
    pub project: String,
    /// Committing agent name (overrides AGENT_NAME).
    #[arg(long)]
    pub as_agent: Option<String>,
    /// Strict sweep-guard (bd-2dox): staged paths must be covered by the
    /// committer's OWN reservations (or shared-safe bookkeeping); blocks
    /// unattributed commits fail-closed. TORON_GUARD_STRICT=0 disables.
    #[arg(long)]
    pub strict: bool,
    /// Commit message file ($1 of a commit-msg hook). When set, ADDED lines
    /// of the staged diff are scanned for bd-* tokens absent from the
    /// message (bd-9mb8 bead-hunk attribution); violations block under
    /// strict mode, warn loudly when the strict hatch is open.
    #[arg(long)]
    pub msg_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct GuardRecordArgs {
    /// Git repository path.
    #[arg(long, required = true)]
    pub repo: PathBuf,
    /// Project slug (kept for symmetry with `guard check`; unused today).
    #[arg(long, required = true)]
    pub project: String,
    /// Committing agent name (overrides AGENT_NAME).
    #[arg(long)]
    pub as_agent: Option<String>,
    /// Override journal path (hermetic tests only; default is
    /// `$XDG_STATE_HOME/flywheel/journals/git-facts.jsonl` or the hard-coded
    /// fallback).
    #[arg(long)]
    pub journal: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum ChannelCommand {
    /// Create a channel (idempotent; creator is the first member).
    Create(ChannelCreateArgs),
    /// Join a channel (idempotent).
    Join(ChannelJoinArgs),
    /// Leave a channel (the creator cannot leave).
    Leave(ChannelJoinArgs),
    /// List channels visible to the caller (project-scoped).
    List(ChannelListArgs),
    /// List a channel's member npubs.
    Members(ChannelMembersArgs),
    /// Post a kind-9 message (membership-gated; `--mention` dispatches 43001).
    Post(ChannelPostArgs),
    /// Read channel history (posts + reactions; membership-gated).
    Read(ChannelReadArgs),
    /// Close (retire) a channel (bd-channel-close-purge-bsqb). Creator-only,
    /// idempotent. A closed channel refuses posts/joins and is hidden from
    /// `list` by default; its history survives until `purge`. Lifecycle:
    /// open -> closed -> purged.
    Close(ChannelCloseArgs),
    /// Purge a CLOSED channel's local bus events (operator-grade, for
    /// genuinely dead probe/testing channels). Caller must be the creator
    /// or the operator. Logged to {data_dir}/channel_purge.log.
    Purge(ChannelPurgeArgs),
}

#[derive(Debug, Args)]
pub struct ChannelCreateArgs {
    /// Channel id (the `h` tag).
    pub id: String,
    /// Display name.
    #[arg(long, required = true)]
    pub name: String,
    /// Private channel (global subscriptions never see its events).
    #[arg(long)]
    pub private: bool,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Creating agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

#[derive(Debug, Args)]
pub struct ChannelJoinArgs {
    /// Channel id.
    pub id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
    /// Only join if not already a member (idempotent bootstrap, bd-kbl1c).
    #[arg(long)]
    pub if_missing: bool,
}

#[derive(Debug, Args)]
pub struct ChannelListArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Optional reader identity (private channels appear only to members).
    #[arg(long = "as")]
    pub as_name: Option<String>,
    /// Include closed (retired) channels — hidden by default
    /// (bd-channel-close-purge-bsqb).
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, Args)]
pub struct ChannelMembersArgs {
    /// Channel id.
    pub id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct ChannelPostArgs {
    /// Channel id.
    pub id: String,
    /// Message body (markdown). Required unless --file is given.
    #[arg(long, required = false)]
    pub body: Option<String>,
    /// bd-channel-truncation-238e: post a LONG report from a file — the
    /// content is chunked at 700 chars into an (1/N)-marked thread so the
    /// channel carries what crews actually write instead of a silent
    /// truncation. Exactly one of --body / --file.
    #[arg(long)]
    pub file: Option<PathBuf>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Posting agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
    /// @mention targets (canonical names; p-tags + mention dispatcher;
    /// applied to the first chunk only). Each target must also appear as
    /// `@Name` in the body; a flag/body mismatch is a usage error (exit 2)
    /// and publishes nothing.
    #[arg(long, value_delimiter = ',')]
    pub mention: Vec<String>,
    /// Verify-after-send (bd-verify-after-send-fxtv): after publishing,
    /// poll the channel projection until the post's own event id is
    /// readable, up to this many seconds; exit nonzero on timeout.
    /// Delivery is claimed only after it is observed.
    #[arg(long, value_name = "SECS")]
    pub wait_ack: Option<u64>,
}

#[derive(Debug, Args)]
pub struct ChannelReadArgs {
    /// Channel id.
    pub id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Reading agent's canonical name (membership gate needs an identity).
    /// Read order is NEWEST-FIRST (descending created_at) by design — slice
    /// with `--limit` from the top; do not assume ascending order
    /// (bd-channel-truncation-238e rider).
    #[arg(long = "as", required = true)]
    pub as_name: String,
    /// Maximum messages to return.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Include messages at/after this timestamp: RFC3339 or a relative
    /// offset (30s, 15m, 6h, 7d). `last` is rejected here — a channel read
    /// tracks no high-water cursor.
    #[arg(long)]
    pub since: Option<String>,
    /// Only events authored by this agent (bd-delivery-visibility-g96y:
    /// "did the scout report?" as a single query).
    #[arg(long)]
    pub author: Option<String>,
}

#[derive(Debug, Args)]
pub struct ReactArgs {
    /// Target message event id (the kind-9 post being reacted to).
    #[arg(long, required = true)]
    pub to: String,
    /// Reaction emoji/shortcode (e.g. :+1:).
    #[arg(long, required = true)]
    pub emoji: String,
    /// Channel id (optional; resolved from the target event's `h` tag).
    #[arg(long)]
    pub channel: Option<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Reacting agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

/// bd-channel-close-purge-bsqb: close (retire) a channel. Creator-only.
#[derive(Debug, Args)]
pub struct ChannelCloseArgs {
    /// Channel id.
    pub id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Creator's canonical name (only the creator may close).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

/// bd-channel-close-purge-bsqb: purge a CLOSED channel's bus events.
/// Creator or operator only; audited.
#[derive(Debug, Args)]
pub struct ChannelPurgeArgs {
    /// Channel id (must be closed first — lifecycle: open -> closed -> purged).
    pub id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Caller's canonical name (creator or operator).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List job requests (43001) with their current state.
    List(JobListArgs),
    /// Show one job's request + state (43001-43006 stream).
    Show(JobShowArgs),
    /// Claim a job (43002, signed by `--as`; first claimer wins).
    Claim(JobClaimArgs),
    /// Post a progress update (43003, optional).
    Progress(JobProgressArgs),
    /// Close with a result (43004; ok|failed).
    Result(JobResultArgs),
    /// Close with an error (43006).
    Error(JobErrorArgs),
    /// Cancel a job (43005; requester or operator only).
    Cancel(JobCancelArgs),
}

/// Job state filter for `toron job list`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum JobStateArg {
    Open,
    Claimed,
    Done,
    Error,
}

#[derive(Debug, Args)]
pub struct JobListArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Filter by assigned agent name.
    #[arg(long)]
    pub assignee: Option<String>,
    /// Filter by lifecycle state.
    #[arg(long, value_enum)]
    pub state: Option<JobStateArg>,
    /// Caller identity (optional for reads).
    #[arg(long = "as")]
    pub as_name: Option<String>,
}

#[derive(Debug, Args)]
pub struct JobShowArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct JobClaimArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Claiming agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

#[derive(Debug, Args)]
pub struct JobProgressArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// Progress note.
    #[arg(long, required = true)]
    pub body: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Reporting agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

/// Job result status selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum JobStatusArg {
    Ok,
    Failed,
}

#[derive(Debug, Args)]
pub struct JobResultArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// ok | failed.
    #[arg(long, value_enum, required = true)]
    pub status: JobStatusArg,
    /// Outcome body (receipt, summary). Optional when --witness is given.
    #[arg(long, required_unless_present = "witness")]
    pub body: Option<String>,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Reporting agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
    /// Witness file: JSON object attached to the 43004 payload (D4.1).
    /// When present, --body becomes optional.
    #[arg(long)]
    pub witness: Option<std::path::PathBuf>,
}

#[derive(Debug, Args)]
pub struct JobErrorArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// Error message.
    #[arg(long, required = true)]
    pub message: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Reporting agent's canonical name (ADR-0018).
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

#[derive(Debug, Args)]
pub struct JobCancelArgs {
    /// Job request event id (43001).
    pub job_id: String,
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Cancelling agent's canonical name (ADR-0018); requester or operator only.
    #[arg(long = "as", required = true)]
    pub as_name: String,
}

#[derive(Debug, Subcommand)]
pub enum PresenceCommand {
    /// Publish a kind-20001 heartbeat (ephemeral; broker last-heartbeat-wins).
    Publish(PresencePublishArgs),
    /// List live presence for the project.
    List(PresenceListArgs),
}

/// Presence state selector (hc vocabulary: idle|working|blocked|done).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PresenceStateArg {
    Idle,
    Working,
    Blocked,
    Done,
}

#[derive(Debug, Args)]
pub struct PresencePublishArgs {
    /// idle | working | blocked | done.
    #[arg(long, value_enum, required = true)]
    pub state: PresenceStateArg,
    /// Optional status message.
    #[arg(long)]
    pub status: Option<String>,
    /// Project slug (when absent, defaults from the TORON_PROJECT env var).
    #[arg(long)]
    pub project: Option<String>,
    /// Publishing agent's canonical name (ADR-0018; when absent, defaults
    /// from AGENT_NAME).
    #[arg(long = "as")]
    pub as_name: Option<String>,
}

#[derive(Debug, Args)]
pub struct PresenceListArgs {
    /// Project slug (when absent, defaults from the TORON_PROJECT env var).
    #[arg(long)]
    pub project: Option<String>,
}

#[derive(Debug, Args, Clone)]
pub struct WatchArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Watcher identity (required only when watching kind 1059).
    #[arg(long = "as")]
    pub as_name: Option<String>,
    /// Comma-separated kind allowlist (default 9,7,39002,43001,20001,30013 —
    /// 39002 join/leave wakes the dispatcher on crew-composition changes,
    /// ADR-0023; 30013 role= bind, ADR-0027 E4).
    #[arg(long, default_value = "9,7,39002,43001,20001,30013")]
    pub kinds: String,
    /// Channel id (`h` tag filter).
    #[arg(long)]
    pub channel: Option<String>,
    /// Exit 0 on the first matching event.
    #[arg(long)]
    pub once: bool,
    /// Total watch budget in ms (required with --once).
    #[arg(long)]
    pub timeout_ms: Option<u64>,
    /// Live subscription (bus subscribe, yields WatchNdjson lines) — no snapshot poll.
    #[arg(long, conflicts_with = "once")]
    pub live: bool,
    /// Exit 4 after N seconds with no matching event (silence-bounded live
    /// watch; consumers respawn the child). Without it a half-open bus
    /// subscription delivers nothing AND never errors — a silent zombie.
    #[arg(long, conflicts_with = "once")]
    pub idle_exit: Option<u64>,
}

#[derive(Debug, Args)]
pub struct WaitArgs {
    /// Correlation id to wait for (receipts store).
    #[arg(long, required = true)]
    pub corr: String,
    /// Timeout in seconds (default 30, 0 = no timeout).
    #[arg(long)]
    pub timeout: Option<u64>,
    /// Project slug (required, no silent demo default).
    #[arg(long, required = true)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    /// CI-friendly: exit 3 if any check fails (0 if all pass).
    #[arg(long)]
    pub check: bool,

    /// Disaster-recovery subcommands.
    #[command(subcommand)]
    pub command: Option<DoctorCmd>,
}

/// Disaster-recovery subcommands for `toron doctor`.
#[derive(Debug, Subcommand)]
#[command(arg_required_else_help = false)]
pub enum DoctorCmd {
    /// Rebuild the SQLite index from the Git archive (disaster recovery,
    /// AM `am doctor reconstruct` parity). Forensic bundle + quarantine +
    /// atomic rebuild. Exit codes: 0 ok · 1 rebuild failed (quarantine
    /// preserved) · 3 diagnostic (nothing to do / cannot locate data).
    Reconstruct(DoctorReconstructArgs),
}

#[derive(Debug, Args)]
pub struct DoctorReconstructArgs {
    /// Preview only; make no changes. Reports what WOULD happen.
    #[arg(long)]
    pub dry_run: bool,

    /// Reconstruct only this project slug (default: all projects under the
    /// data dir that have an archive).
    #[arg(long)]
    pub project: Option<String>,

    /// Override the data directory (default: `TORON_DATA_DIR` env, then
    /// config `data_dir`, then the durable XDG root — ADR-0007 D3).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
}

/// Keypair role selector (drives the default output path suffix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RoleArg {
    Operator,
    Agent,
    Anchor,
}

#[derive(Debug, Subcommand)]
pub enum KeyCommand {
    /// Generate a keypair, write ncryptsec (0600), print the npub + path.
    Generate(KeyGenerateArgs),
    /// Load a key file and print ONLY the public key (never the nsec).
    Inspect(KeyInspectArgs),
}

#[derive(Debug, Args)]
pub struct KeyGenerateArgs {
    /// operator|agent|anchor.
    #[arg(long, value_enum, default_value_t = RoleArg::Operator)]
    pub role: RoleArg,
    /// Output path (default: operator_key, else the role's canonical path — ADR-0038).
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct KeyInspectArgs {
    /// Key file path (default: operator_key, else the canonical operator key).
    pub path: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    /// Idempotent project bootstrap (kind-30010 manifest via the operator key).
    Ensure(ProjectEnsureArgs),
    /// List known projects (demo data dir; registry query not wired).
    List,
}

#[derive(Debug, Args)]
pub struct ProjectEnsureArgs {
    /// Project slug.
    pub slug: String,
}

#[derive(Debug, Subcommand)]
pub enum RelayCommand {
    /// Print the configured federation relays.
    List,
    /// Propose appending a relay URL (in-memory; no config write-back).
    Add(RelayUrlArg),
    /// Propose removing a relay URL (in-memory; no config write-back).
    Remove(RelayUrlArg),
    /// Relay metadata lookup (NIP-11; network — not wired yet).
    Info(RelayUrlArg),
}

#[derive(Debug, Args)]
pub struct RelayUrlArg {
    /// Relay URL (e.g. wss://relay.example.com).
    pub url: String,
}

/// Backend selector (hc).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum BackendArg {
    Hc,
}

impl BackendArg {
    pub const fn as_str(&self) -> &'static str {
        match self {
            BackendArg::Hc => "hc",
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum BackendCommand {
    /// Probe backend availability (hc socket connect).
    List,
    /// Print the effective backend from config.
    Status,
    /// Choose the default backend (confirmation only; no persistence).
    Default(BackendDefaultArgs),
}

#[derive(Debug, Args)]
pub struct BackendDefaultArgs {
    /// hc.
    #[arg(value_enum)]
    pub backend: BackendArg,
}

#[derive(Debug, Args)]
pub struct BenchArgs {
    /// Scenario: startup|inbox|send|search|threads.
    #[arg(value_enum)]
    pub case: BenchCaseArg,
}

#[derive(Debug, Args)]
pub struct TuiArgs {
    /// Poll/refresh interval in milliseconds (live-tunable in the Settings tab).
    #[arg(long, default_value_t = 1000)]
    pub refresh_ms: u64,
    /// Project slug to open. Falls back to $TORON_PROJECT; with neither the
    /// TUI fails loud (no silent `demo` default — same rule as presence).
    #[arg(long)]
    pub project: Option<String>,
    /// Acting agent pin whose inbox the Mail tab projects. Falls back to
    /// $AGENT_NAME; without either the Mail tab reports a missing identity
    /// instead of guessing one.
    #[arg(long = "as")]
    pub agent: Option<String>,
    /// Open on this tab (mail|channels|agents|dashboard|settings).
    #[arg(long, default_value = "mail")]
    pub tab: String,
    /// Open on this mail folder (inbox|threads|ack|important).
    #[arg(long, default_value = "inbox")]
    pub folder: String,
    /// Include closed (retired) channels in the Channels tab.
    #[arg(long)]
    pub all: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum BenchCaseArg {
    Startup,
    Inbox,
    Send,
    Search,
    Threads,
}

impl BenchCaseArg {
    /// The AM-Rust parity case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Inbox => "inbox",
            Self::Send => "send",
            Self::Search => "search",
            Self::Threads => "threads",
        }
    }
}

#[cfg(test)]
mod flag_conformance_tests {
    use clap::CommandFactory;

    use super::Cli;

    /// Walk every subcommand (recursively) and enforce:
    /// - the acting agent is ALWAYS `--as` — `--from` and `--agent` are
    ///   banned long names anywhere on the surface;
    /// - every subcommand that has a `--project` argument REQUIRES it (no
    ///   silent `demo` default producing "no local key in project demo").
    #[test]
    fn flag_conformance_one_vocabulary_and_required_project() {
        let mut banned: Vec<String> = Vec::new();
        let mut bad_project: Vec<String> = Vec::new();
        let waived = [
            "toron doctor reconstruct",
            "toron presence publish",
            "toron presence list",
            "toron tui",
            // An operator token is project-less by design (ADR-0039 D6).
            "toron events token mint",
        ];
        let root = Cli::command();
        walk(&root, String::new(), &waived, &mut banned, &mut bad_project);
        assert!(
            banned.is_empty(),
            "banned sender flags found (sender must be --as): {banned:?}"
        );
        assert!(
            bad_project.is_empty(),
            "--project must be required (no silent demo default): {bad_project:?}"
        );
    }

    fn walk(
        cmd: &clap::Command,
        path: String,
        waived: &[&str],
        banned: &mut Vec<String>,
        bad_project: &mut Vec<String>,
    ) {
        let here = if path.is_empty() {
            cmd.get_name().to_string()
        } else {
            format!("{path} {}", cmd.get_name())
        };
        for arg in cmd.get_arguments() {
            let Some(long) = arg.get_long() else { continue };
            match long {
                "from" | "agent" => banned.push(format!("{here}: --{long}")),
                "project" => {
                    if waived.contains(&here.as_str()) {
                        continue;
                    }
                    if !arg.is_required_set() || !arg.get_default_values().is_empty() {
                        bad_project.push(format!(
                            "{here}: --project required={}, defaults={:?}",
                            arg.is_required_set(),
                            arg.get_default_values()
                        ));
                    }
                }
                _ => {}
            }
        }
        for sub in cmd.get_subcommands() {
            walk(sub, here.clone(), waived, banned, bad_project);
        }
    }

    /// bd-auqj: `toron contact captain --clear` clears the alias; `--pin` and
    /// `--clear` conflict, and `--project` remains required.
    #[test]
    fn captain_clear() {
        use crate::cli::{Cli, Command, ContactCommand};
        use clap::Parser;

        let cli = Cli::try_parse_from([
            "toron",
            "contact",
            "captain",
            "--project",
            "testproj",
            "--clear",
        ])
        .expect("parse contact captain --clear");
        match cli.command {
            Some(Command::Contact(ContactCommand::Captain(args))) => {
                assert!(args.clear, "--clear flag set");
                assert!(args.pin.is_none(), "--clear conflicts with --pin");
                assert_eq!(args.project, "testproj");
            }
            _ => panic!("expected contact captain with --clear"),
        }

        let err = Cli::try_parse_from([
            "toron",
            "contact",
            "captain",
            "--project",
            "testproj",
            "--pin",
            "SomePin",
            "--clear",
        ])
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("conflict") || msg.contains("cannot be used with"),
            "conflict error: {msg}"
        );

        let cli = Cli::try_parse_from(["toron", "contact", "captain", "--project", "testproj"])
            .expect("parse show");
        match cli.command {
            Some(Command::Contact(ContactCommand::Captain(args))) => {
                assert!(!args.clear);
                assert!(args.pin.is_none());
            }
            _ => panic!("expected show"),
        }
    }

    /// bd-i1o3: watch kinds must include 39002 (join/leave) next to 9,43001,30031.
    #[test]
    fn wake_watch_kinds_include_39002() {
        use clap::CommandFactory;
        let cmd = Cli::command();
        let watch = cmd.find_subcommand("watch").expect("watch subcommand");
        let kinds = watch
            .get_arguments()
            .find(|a| a.get_long() == Some("kinds"))
            .expect("kinds arg");
        let defaults: Vec<String> = kinds
            .get_default_values()
            .iter()
            .map(|v| v.to_string_lossy().to_string())
            .collect();
        let joined = defaults.join(",");
        assert!(
            joined.contains("39002"),
            "watch --kinds default must include 39002, got {joined}"
        );
        assert!(joined.contains('9'), "must still contain 9");
    }
}
