// governed-by: ADR-0003
// governed-by: ADR-0004 (MailOverseerArgs — Ship 5)

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// toron: a Nostr-native agent mailbox.
///
/// Two personas from one surface:
///   - Operators: daemon lifecycle, keys, projects, relays, backend, doctor.
///   - Agents/scripts (robot mode): the full 38-tool AM-Rust parity surface.
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

    /// Never prompt; fail with instruction if input is needed.
    #[arg(long, global = true)]
    pub no_input: bool,

    /// Override config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Output to file (commands that write artifacts).
    #[arg(short, long, global = true)]
    pub output: Option<PathBuf>,

    // --- robot mode (bd-led.8.4): agent-driver global flags (ADR-0003).
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

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the toron daemon (mailbox + MCP server).
    Serve(ServeArgs),

    /// Quick health/config overview (human by default, --json for machines).
    Status,

    /// Show the effective configuration (flags > env > project > user > system).
    Config(ConfigArgs),

    // --- robot parity surface (bd-led.8.3): the MCP tools as shell-out commands.
    /// Mail: send/reply/inbox/ack/read/search/thread-summarize (MCP parity JSON).
    #[command(subcommand)]
    Mail(MailCommand),

    /// File reservations: acquire/conflicts/renew/release/force-release.
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

    // --- operator surface (bd-led.8.2): daemon-persona commands.
    /// Deep health checks (herdr/git/keys/resonate). --check exits 3 on any failure.
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

    /// Bench a mailbox scenario (startup/inbox/send/search/threads). AM-Rust parity.
    Bench(BenchArgs),

    /// Interactive operator dashboard (ratatui; standalone-daemon TUI).
    Tui(TuiArgs),
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

// ---------------------------------------------------------------------------
// Robot parity surface (bd-led.8.3): the MCP tools as shell-out commands.
// Noun-verb per ADR. Every command prints the exact `ToJson` shape the
// matching MCP tool returns (see `robot.rs`).
// ---------------------------------------------------------------------------

/// Default project for robot commands whose `--project` is optional (the
/// demo mailbox's canonical project).
pub const DEFAULT_PROJECT: &str = "demo";

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
}

#[derive(Debug, Args)]
pub struct MailSendArgs {
    /// Sender agent name (must have a local key in the project).
    #[arg(long, default_value = "agent")]
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct MailReplyArgs {
    /// Sender agent name (must have a local key in the project).
    #[arg(long, default_value = "agent")]
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
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
}

#[derive(Debug, Args)]
pub struct MailInboxArgs {
    /// Agent whose inbox to project.
    #[arg(long, required = true)]
    pub agent: String,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Unread messages only.
    #[arg(long)]
    pub unread: bool,
    /// Maximum messages to return.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Include message bodies.
    #[arg(long)]
    pub bodies: bool,
}

#[derive(Debug, Args)]
pub struct MailReceiptArgs {
    /// Project slug.
    #[arg(long, required = true)]
    pub project: String,
    /// Agent sending the receipt.
    #[arg(long, required = true)]
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
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

#[derive(Debug, Subcommand)]
pub enum ReserveCommand {
    /// Acquire exclusive/shared leases on path globs.
    Acquire(ReserveAcquireArgs),
    /// Symmetric-glob conflict report for paths vs other agents' reservations.
    Conflicts(ReserveConflictsArgs),
    /// Extend the TTLs of the holder's active file reservations.
    Renew(ReserveRenewArgs),
    /// Release the holder's active file reservations (idempotent).
    Release(ReserveReleaseArgs),
    /// Operator force-release of a stale reservation.
    ForceRelease(ReserveForceReleaseArgs),
}

#[derive(Debug, Args)]
pub struct ReserveAcquireArgs {
    /// Path globs to reserve (comma-separated; e.g. 'src/**').
    #[arg(long, value_delimiter = ',', required = true)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent acquiring the leases.
    #[arg(long, required = true)]
    pub agent: String,
    /// Lease TTL in seconds (default envelope TTL).
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
    #[arg(long, value_delimiter = ',', required = true)]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent performing the check.
    #[arg(long, required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct ReserveRenewArgs {
    /// Path globs to renew (comma-separated; empty = renew all of the agent's).
    #[arg(long, value_delimiter = ',')]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent holding the leases.
    #[arg(long, required = true)]
    pub agent: String,
    /// Seconds to extend each lease's TTL.
    #[arg(long, required = true)]
    pub extend_secs: u64,
}

#[derive(Debug, Args)]
pub struct ReserveReleaseArgs {
    /// Path globs to release (comma-separated; empty = release all of the agent's).
    #[arg(long, value_delimiter = ',')]
    pub paths: Vec<String>,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent holding the leases.
    #[arg(long, required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct ReserveForceReleaseArgs {
    /// Reservation id (the lease event id).
    #[arg(long, required = true)]
    pub id: String,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Requestor agent (the operator's tool user).
    #[arg(long, required = true)]
    pub agent: String,
    /// Note attached to the revocation.
    #[arg(long)]
    pub note: Option<String>,
    /// Notify the previous holder via NIP-17.
    #[arg(long)]
    pub notify_previous: bool,
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent acquiring the slot.
    #[arg(long, required = true)]
    pub agent: String,
    /// Lease TTL in seconds (default envelope TTL).
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
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent holding the slot.
    #[arg(long, required = true)]
    pub agent: String,
}

#[derive(Debug, Args)]
pub struct SlotReleaseArgs {
    /// Slot name (e.g. ci).
    #[arg(long, required = true)]
    pub slot: String,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Agent holding the slot.
    #[arg(long, required = true)]
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
}

#[derive(Debug, Args)]
pub struct ContactRequestArgs {
    /// Peer agent name.
    #[arg(long, required = true)]
    pub to: String,
    /// Requesting agent name.
    #[arg(long, default_value = "agent")]
    pub from: String,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
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
    #[arg(long, required = true)]
    pub from: String,
    /// Responding agent name.
    #[arg(long, required = true)]
    pub to: String,
    /// Approve (true) or deny (false) the request.
    #[arg(long, required = true)]
    pub accept: bool,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
    /// Requester's project (defaults to --project).
    #[arg(long)]
    pub from_project: Option<String>,
}

#[derive(Debug, Args)]
pub struct ContactListArgs {
    /// Agent whose contact links to list.
    #[arg(long, required = true)]
    pub agent: String,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
}

#[derive(Debug, Args)]
pub struct ContactPolicyArgs {
    /// Agent whose inbound policy to set.
    #[arg(long, required = true)]
    pub agent: String,
    /// open|auto|contacts_only|block_all.
    #[arg(long, value_enum, required = true)]
    pub policy: PolicyArg,
    /// Project slug.
    #[arg(long, default_value = DEFAULT_PROJECT)]
    pub project: String,
}

#[derive(Debug, Subcommand)]
pub enum GuardCommand {
    /// Install the toron pre-commit hook in a git repository.
    Install(GuardRepoArgs),
    /// Remove the toron-installed pre-commit hook (never clobbers a user hook).
    Uninstall(GuardRepoArgs),
}

#[derive(Debug, Args)]
pub struct GuardRepoArgs {
    /// Git repository path.
    #[arg(long, required = true)]
    pub repo: PathBuf,
}

// ---------------------------------------------------------------------------
// Operator surface (bd-led.8.2): doctor/metrics/key/project/relay/backend.
// Passphrases NEVER arrive via flags — env `TORON_KEY_PASSPHRASE` only (dev
// default "toron"; production must set the env var).
// ---------------------------------------------------------------------------

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
    /// config `data_dir`).
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
    /// Output path (default: operator_key or ~/.config/toron/<role>_nsec).
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct KeyInspectArgs {
    /// Key file path (default: operator_key).
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

/// Backend selector (herdr | tmux).
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum BackendArg {
    Herdr,
    Tmux,
}

impl BackendArg {
    pub fn as_str(&self) -> &'static str {
        match self {
            BackendArg::Herdr => "herdr",
            BackendArg::Tmux => "tmux",
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum BackendCommand {
    /// Probe backend availability (herdr socket connect; tmux held).
    List,
    /// Print the effective backend from config.
    Status,
    /// Choose the default backend (confirmation only; no persistence).
    Default(BackendDefaultArgs),
}

#[derive(Debug, Args)]
pub struct BackendDefaultArgs {
    /// herdr|tmux.
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
    /// Poll/refresh interval in milliseconds.
    #[arg(long, default_value_t = 1000)]
    pub refresh_ms: u64,
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
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Startup => "startup",
            Self::Inbox => "inbox",
            Self::Send => "send",
            Self::Search => "search",
            Self::Threads => "threads",
        }
    }
}
