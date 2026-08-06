// governed-by: ADR-0003

//! Operator surface (bd-led.8.2): doctor/metrics/key/project/relay/backend.
//!
//! The daemon-persona commands. Read ops emit JSON on stdout (clig.dev:
//! stdout = data, stderr = messages). Exit-code discipline: 0 ok · 1 runtime ·
//! 2 usage (clap) · 3 doctor `--check` fails-closed.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde_json::json;

use crate::cli::{
    BackendCommand, Command, ContactCommand, DoctorArgs, GuardCommand, KeyCommand, KeyGenerateArgs,
    KeyInspectArgs, MailCommand, ProjectCommand, ProjectEnsureArgs, RelayCommand, RelayUrlArg,
    ReserveCommand, SlotCommand,
};
use crate::config::Config;
use crate::output::{render_json, OutputFormat};
use toron_mcp::ToJson;

/// argon2 log-n for operator keys (spec: 12 — dev-speed, still strong).
const KEY_LOG_N: u8 = 12;

/// Dev-default passphrase when `TORON_KEY_PASSPHRASE` is unset. Production
/// MUST set the env var; the flag form is intentionally rejected.
fn key_password() -> String {
    std::env::var("TORON_KEY_PASSPHRASE").unwrap_or_else(|_| "toron".to_string())
}

/// Print a JSON value to stdout as pretty JSON.
fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// doctor
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct CheckResult {
    name: &'static str,
    ok: bool,
    detail: String,
}

fn check_herdr() -> CheckResult {
    let path = toron_core::backend::herdr::default_socket_path();
    match toron_core::backend::herdr::HerdrBackend::connect(None) {
        Ok(_) => CheckResult {
            name: "herdr",
            ok: true,
            detail: format!("herdr socket reachable at {}", path.display()),
        },
        Err(e) => CheckResult {
            name: "herdr",
            ok: false,
            detail: format!("herdr not present ({}): {e}", path.display()),
        },
    }
}

fn check_git() -> CheckResult {
    match std::process::Command::new("git").arg("--version").output() {
        Ok(out) if out.status.success() => CheckResult {
            name: "git",
            ok: true,
            detail: String::from_utf8_lossy(&out.stdout).trim().to_string(),
        },
        Ok(_) => CheckResult {
            name: "git",
            ok: false,
            detail: "git present but `git --version` failed".to_string(),
        },
        Err(e) => CheckResult {
            name: "git",
            ok: false,
            detail: format!("git not found: {e}"),
        },
    }
}

fn check_keys(cfg: &Config) -> CheckResult {
    match &cfg.operator_key {
        Some(p) if p.is_file() => CheckResult {
            name: "keys",
            ok: true,
            detail: format!("operator key present at {}", p.display()),
        },
        Some(p) => CheckResult {
            name: "keys",
            ok: false,
            detail: format!("operator key missing at {}", p.display()),
        },
        None => CheckResult {
            name: "keys",
            ok: false,
            detail: "operator_key not configured (set operator_key or TORON_OPERATOR_KEY)"
                .to_string(),
        },
    }
}

fn check_resonate() -> CheckResult {
    CheckResult {
        name: "resonate",
        ok: true,
        detail: "not configured (no running server expected)".to_string(),
    }
}

/// Deep health checks. `--check` exits 3 when any check fails (fails-closed,
/// CI-friendly); 0 when all pass. Runtime failures still exit 1.
pub fn run_doctor(
    cfg: &Config,
    args: &DoctorArgs,
    format: OutputFormat,
) -> anyhow::Result<ExitCode> {
    let checks = vec![
        check_herdr(),
        check_git(),
        check_keys(cfg),
        check_resonate(),
    ];
    let value = json!({ "checks": serde_json::to_value(&checks)? });

    match format {
        OutputFormat::Json | OutputFormat::Plain => render_json(&value, format),
        OutputFormat::Human => {
            println!("toron doctor");
            for c in &checks {
                let status = if c.ok { "ok  " } else { "FAIL" };
                println!("  {status} {} — {}", c.name, c.detail);
            }
        }
    }

    if args.check && checks.iter().any(|c| !c.ok) {
        Ok(ExitCode::from(3))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

// ---------------------------------------------------------------------------
// metrics (in-process counters, file-backed so separate processes aggregate)
// ---------------------------------------------------------------------------

/// Per-process counter state, persisted to a JSON file so an operator can
/// observe cumulative tool usage across invocations. The file is only written
/// on `record_*`; `snapshot` reads the in-process cache loaded at first use.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsState {
    /// Successful tool calls keyed by tool name (e.g. "mail.inbox").
    pub call_counts: BTreeMap<String, u64>,
    /// Failed tool calls (recorded with the tool name they errored on).
    pub errors: u64,
    /// Per-call latency in microseconds (a small in-process histogram).
    pub latencies_us: Vec<u64>,
}

impl MetricsState {
    fn record(&mut self, name: &str, error: bool, duration: Duration) {
        if error {
            self.errors += 1;
        } else {
            *self.call_counts.entry(name.to_string()).or_insert(0) += 1;
        }
        self.latencies_us.push(duration.as_micros() as u64);
    }

    fn total_calls(&self) -> u64 {
        self.call_counts.values().sum::<u64>() + self.errors
    }
}

static STATE: OnceLock<Mutex<MetricsState>> = OnceLock::new();

/// Metrics file location: `TORON_METRICS_FILE` env override, else
/// `$TMPDIR/toron-cli-metrics.json`. Tests isolate via the env var.
fn metrics_file_path() -> PathBuf {
    std::env::var_os("TORON_METRICS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("toron-cli-metrics.json"))
}

fn state() -> &'static Mutex<MetricsState> {
    STATE.get_or_init(|| {
        let mut loaded = MetricsState::default();
        if let Ok(raw) = std::fs::read_to_string(metrics_file_path()) {
            if let Ok(parsed) = serde_json::from_str::<MetricsState>(&raw) {
                loaded = parsed;
            }
        }
        Mutex::new(loaded)
    })
}

fn persist() {
    let path = metrics_file_path();
    let raw = match state().lock() {
        Ok(g) => serde_json::to_string(&*g).unwrap_or_else(|_| "{}".to_string()),
        Err(p) => serde_json::to_string(&*p.into_inner()).unwrap_or_else(|_| "{}".to_string()),
    };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, raw).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// Record a successful robot tool call (tool name + duration).
pub fn record_tool_call(name: &str, duration: Duration) {
    {
        let mut guard = match state().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.record(name, false, duration);
    }
    persist();
}

/// Record a failed robot tool call (tool name + duration).
pub fn record_tool_error(name: &str, duration: Duration) {
    {
        let mut guard = match state().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.record(name, true, duration);
    }
    persist();
}

/// JSON snapshot: `{tool_calls, errors, total_calls, latency_us}`.
pub fn snapshot() -> serde_json::Value {
    let guard = match state().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    let mut tool_calls = serde_json::Map::new();
    for (name, count) in &guard.call_counts {
        tool_calls.insert(name.clone(), json!(count));
    }
    let total = guard.total_calls();
    let lat_count = guard.latencies_us.len();
    let lat_sum: u64 = guard.latencies_us.iter().sum();
    json!({
        "tool_calls": tool_calls,
        "errors": guard.errors,
        "total_calls": total,
        "latency_us": { "count": lat_count, "sum": lat_sum },
    })
}

pub fn run_metrics() -> anyhow::Result<()> {
    print_json(&snapshot())
}

/// The MCP-tool-style name for a robot command (e.g. "mail.inbox"), used as
/// the metrics key. Non-robot commands never reach `run_robot`.
pub fn robot_tool_name(command: &Command) -> &'static str {
    match command {
        Command::Mail(MailCommand::Send(_)) => "mail.send",
        Command::Mail(MailCommand::Reply(_)) => "mail.reply",
        Command::Mail(MailCommand::Inbox(_)) => "mail.inbox",
        Command::Mail(MailCommand::Ack(_)) => "mail.ack",
        Command::Mail(MailCommand::Read(_)) => "mail.read",
        Command::Mail(MailCommand::Search(_)) => "mail.search",
        Command::Mail(MailCommand::ThreadSummarize(_)) => "mail.thread_summarize",
        Command::Reserve(ReserveCommand::Acquire(_)) => "reserve.acquire",
        Command::Reserve(ReserveCommand::Conflicts(_)) => "reserve.conflicts",
        Command::Reserve(ReserveCommand::Renew(_)) => "reserve.renew",
        Command::Reserve(ReserveCommand::Release(_)) => "reserve.release",
        Command::Reserve(ReserveCommand::ForceRelease(_)) => "reserve.force_release",
        Command::Slot(SlotCommand::Acquire(_)) => "slot.acquire",
        Command::Slot(SlotCommand::Renew(_)) => "slot.renew",
        Command::Slot(SlotCommand::Release(_)) => "slot.release",
        Command::Contact(ContactCommand::Request(_)) => "contact.request",
        Command::Contact(ContactCommand::Respond(_)) => "contact.respond",
        Command::Contact(ContactCommand::List(_)) => "contact.list",
        Command::Contact(ContactCommand::Policy(_)) => "contact.policy",
        Command::Guard(GuardCommand::Install(_)) => "guard.install",
        Command::Guard(GuardCommand::Uninstall(_)) => "guard.uninstall",
        _ => "unknown",
    }
}

// ---------------------------------------------------------------------------
// key
// ---------------------------------------------------------------------------

/// Default key path: `~/.config/toron/<role>_nsec` (no HOME → relative).
fn default_key_path(role: &str) -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".config")
        .join("toron")
        .join(format!("{role}_nsec"))
}

pub fn run_key(cfg: &Config, cmd: &KeyCommand) -> anyhow::Result<()> {
    match cmd {
        KeyCommand::Generate(args) => run_key_generate(cfg, args),
        KeyCommand::Inspect(args) => run_key_inspect(cfg, args),
    }
}

fn run_key_generate(cfg: &Config, args: &KeyGenerateArgs) -> anyhow::Result<()> {
    let path = match &args.out {
        Some(p) => p.clone(),
        None => match args.role {
            crate::cli::RoleArg::Operator => cfg
                .operator_key
                .clone()
                .unwrap_or_else(|| default_key_path("operator")),
            crate::cli::RoleArg::Agent => default_key_path("agent"),
            crate::cli::RoleArg::Anchor => default_key_path("anchor"),
        },
    };
    let keys = toron_core::keys::generate();
    let password = key_password();
    toron_core::keys::write_encrypted_key(&path, &keys, &password, KEY_LOG_N)?;
    print_json(&json!({
        "npub": keys.public_key().to_hex(),
        "path": path.display().to_string(),
    }))
}

fn run_key_inspect(cfg: &Config, args: &KeyInspectArgs) -> anyhow::Result<()> {
    let path = match &args.path {
        Some(p) => p.clone(),
        None => cfg
            .operator_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no key path given and operator_key not configured"))?,
    };
    let password = key_password();
    // Load/decrypt failure bubbles as a runtime error (exit 1); only the
    // public key is ever printed — never the nsec.
    let keys = toron_core::keys::load_encrypted_key(&path, &password)?;
    print_json(&json!({ "npub": keys.public_key().to_hex() }))
}

// ---------------------------------------------------------------------------
// project
// ---------------------------------------------------------------------------

fn run_project_list() -> anyhow::Result<()> {
    let demo_dir = std::env::temp_dir().join("toron-mcp-demo");
    let mut projects: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&demo_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                projects.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    projects.sort();
    let note = if demo_dir.is_dir() {
        format!("local listing of demo data dir ({})", demo_dir.display())
    } else {
        "registry not wired".to_string()
    };
    print_json(&json!({ "projects": projects, "note": note }))
}

/// `project ensure` needs the demo mailbox (kind-30010 manifest signing), so it
/// is async via `#[tokio::main]` like the robot commands.
#[tokio::main]
async fn run_project_ensure(args: &ProjectEnsureArgs) -> anyhow::Result<()> {
    let mailbox = toron_mcp::demo_mailbox().await?;
    let state = mailbox.ensure_project(&args.slug).await?;
    print_json(&state.to_json())
}

pub fn run_project(cmd: &ProjectCommand) -> anyhow::Result<()> {
    match cmd {
        ProjectCommand::Ensure(args) => run_project_ensure(args),
        ProjectCommand::List => run_project_list(),
    }
}

// ---------------------------------------------------------------------------
// relay
// ---------------------------------------------------------------------------

pub fn run_relay(cfg: &Config, cmd: &RelayCommand) -> anyhow::Result<()> {
    match cmd {
        RelayCommand::List => print_json(&json!({ "relays": cfg.relays })),
        RelayCommand::Add(RelayUrlArg { url }) => {
            let mut relays = cfg.relays.clone();
            if !relays.iter().any(|r| r == url) {
                relays.push(url.clone());
            }
            print_json(&json!({
                "relays": relays,
                "note": "persist via .toron.toml (config write-back not wired)",
            }))
        }
        RelayCommand::Remove(RelayUrlArg { url }) => {
            let relays: Vec<String> = cfg.relays.iter().filter(|r| *r != url).cloned().collect();
            print_json(&json!({
                "relays": relays,
                "note": "persist via .toron.toml (config write-back not wired)",
            }))
        }
        RelayCommand::Info(RelayUrlArg { url }) => print_json(&json!({
            "url": url,
            "note": "NIP-11 fetch not wired (bd-led.3.4+)",
        })),
    }
}

// ---------------------------------------------------------------------------
// backend
// ---------------------------------------------------------------------------

pub fn run_backend(cfg: &Config, cmd: &BackendCommand) -> anyhow::Result<()> {
    match cmd {
        BackendCommand::List => {
            let herdr = toron_core::backend::herdr::HerdrBackend::connect(None).is_ok();
            print_json(&json!([
                { "name": "herdr", "available": herdr },
                { "name": "tmux", "available": false },
            ]))
        }
        BackendCommand::Status => print_json(&json!({ "backend": cfg.effective_backend() })),
        BackendCommand::Default(args) => print_json(&json!({
            "backend": args.backend.as_str(),
            "effective": cfg.effective_backend(),
            "note": "no persistence; set TORON_BACKEND or .toron.toml",
        })),
    }
}
